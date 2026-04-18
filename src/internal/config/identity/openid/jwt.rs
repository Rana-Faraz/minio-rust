use std::collections::HashMap;
use std::env;
use std::fmt;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Sha256, Sha384, Sha512};

use crate::internal::arn::{self, Arn};
use crate::internal::auth;
use crate::internal::config::{ENV_MINIO_STS_DURATION, MAX_EXPIRATION, MIN_EXPIRATION};

use super::jwks::{Jwks, PublicKey};

pub const AUD_CLAIM: &str = "aud";
pub const AZP_CLAIM: &str = "azp";
pub const VENDOR: &str = "vendor";
pub const KEYCLOAK_REALM: &str = "keycloak_realm";
pub const KEYCLOAK_ADMIN_URL: &str = "keycloak_admin_url";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<auth::Error> for Error {
    fn from(value: auth::Error) -> Self {
        Self::new(value.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DiscoveryDoc {
    #[serde(default)]
    pub issuer: String,
    #[serde(default)]
    pub auth_endpoint: String,
    #[serde(default)]
    pub token_endpoint: String,
    #[serde(default)]
    pub end_session_endpoint: String,
    #[serde(default)]
    pub user_info_endpoint: String,
    #[serde(default)]
    pub revocation_endpoint: String,
    #[serde(default)]
    pub jwks_uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeycloakProvider {
    pub admin_url: String,
    pub realm: String,
    pub token_endpoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Provider {
    Keycloak(KeycloakProvider),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProviderCfg {
    pub discovery_doc: DiscoveryDoc,
    pub client_id: String,
    pub client_secret: String,
    pub claim_userinfo: bool,
    pub jwks_url: Option<String>,
    pub provider: Option<Provider>,
}

impl ProviderCfg {
    pub fn initialize_provider<F>(&mut self, cfg_get: F) -> Result<(), Error>
    where
        F: Fn(&str) -> String,
    {
        let vendor = cfg_get(VENDOR);
        if vendor.is_empty() {
            return Ok(());
        }

        match vendor.as_str() {
            "keycloak" => {
                let admin_url = cfg_get(KEYCLOAK_ADMIN_URL);
                if admin_url.is_empty() {
                    return Err(Error::new("Admin URL cannot be empty"));
                }

                let realm = match cfg_get(KEYCLOAK_REALM) {
                    value if value.is_empty() => "master".to_owned(),
                    value => value,
                };

                if self.discovery_doc.token_endpoint.is_empty() {
                    return Err(Error::new("missing OpenID token endpoint"));
                }

                self.provider = Some(Provider::Keycloak(KeycloakProvider {
                    admin_url,
                    realm,
                    token_endpoint: self.discovery_doc.token_endpoint.clone(),
                }));
                Ok(())
            }
            _ => Err(Error::new("Unsupported vendor keycloak")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyMaterial {
    Hmac(Vec<u8>),
    Public(PublicKey),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublicKeys {
    pk_map: HashMap<String, KeyMaterial>,
}

impl PublicKeys {
    pub fn parse_and_add<R: Read>(&mut self, reader: R) -> Result<(), Error> {
        let jwks: Jwks = serde_json::from_reader(reader)?;
        for key in jwks.keys {
            let public_key = key
                .decode_public_key()
                .map_err(|err| Error::new(err.to_string()))?;
            self.add_public(&key.kid, public_key);
        }
        Ok(())
    }

    pub fn add_hmac(&mut self, key_id: &str, key: impl Into<Vec<u8>>) {
        self.pk_map
            .insert(key_id.to_owned(), KeyMaterial::Hmac(key.into()));
    }

    pub fn add_public(&mut self, key_id: &str, key: PublicKey) {
        self.pk_map
            .insert(key_id.to_owned(), KeyMaterial::Public(key));
    }

    pub fn get(&self, kid: &str) -> Option<&KeyMaterial> {
        self.pk_map.get(kid)
    }

    pub fn len(&self) -> usize {
        self.pk_map.len()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub enabled: bool,
    pub pub_keys: PublicKeys,
    pub arn_provider_cfgs_map: HashMap<Arn, ProviderCfg>,
    pub provider_cfgs: HashMap<String, ProviderCfg>,
}

impl Config {
    pub fn validate(
        &self,
        arn: &Arn,
        token: &str,
        _access_token: &str,
        dsecs: &str,
        claims: &mut Map<String, Value>,
    ) -> Result<(), Error> {
        let provider = self
            .arn_provider_cfgs_map
            .get(arn)
            .ok_or_else(|| Error::new(format!("Role {arn} does not exist")))?;

        let (header, mut payload, signing_input, signature) = split_token(token)?;
        let algorithm = header
            .get("alg")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("missing alg"))?;
        let kid = header
            .get("kid")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("Invalid kid value"))?;

        let key = self
            .pub_keys
            .get(kid)
            .ok_or_else(|| Error::new(format!("No public key found for kid {kid}")))?;

        match key {
            KeyMaterial::Hmac(secret) => {
                verify_hmac(algorithm, secret, signing_input.as_bytes(), &signature)?;
            }
            KeyMaterial::Public(_) => {
                return Err(Error::new(format!(
                    "signing method ({algorithm}) is unavailable for this Rust port"
                )));
            }
        }

        validate_time_claims(&payload)?;
        update_claims_expiry_in_map(dsecs, &mut payload)?;
        validate_audience(&payload, &provider.client_id)?;

        claims.clear();
        claims.extend(payload);
        Ok(())
    }
}

pub fn dummy_role_arn() -> Arn {
    arn::new_iam_role_arn("dummy-internal", "").expect("dummy role ARN should always parse")
}

pub fn get_default_expiration(dsecs: &str) -> Result<Duration, Error> {
    let timeout = env::var(ENV_MINIO_STS_DURATION).unwrap_or_default();
    let mut duration = match parse_go_duration(&timeout) {
        Ok(value) => value,
        Err(_) => Duration::from_secs(3600),
    };

    if timeout.is_empty() && !dsecs.is_empty() {
        let expiry_secs = dsecs
            .parse::<i64>()
            .map_err(|_| Error::new(auth::Error::InvalidDuration.to_string()))?;
        if !(MIN_EXPIRATION..=MAX_EXPIRATION).contains(&expiry_secs) {
            return Err(Error::new(auth::Error::InvalidDuration.to_string()));
        }
        duration = Duration::from_secs(expiry_secs as u64);
    } else if timeout.is_empty() && dsecs.is_empty() {
        return Ok(Duration::from_secs(3600));
    }

    let seconds = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    if !(MIN_EXPIRATION..=MAX_EXPIRATION).contains(&seconds) {
        return Err(Error::new(auth::Error::InvalidDuration.to_string()));
    }

    Ok(duration)
}

pub fn update_claims_expiry<T>(dsecs: &str, exp: T) -> Result<i64, Error>
where
    T: Into<auth::ExpValue>,
{
    let parsed = auth::exp_to_int64(exp)?;
    if dsecs.is_empty() {
        return Ok(parsed);
    }

    let duration = get_default_expiration(dsecs)?;
    let expiry = SystemTime::now()
        .checked_add(duration)
        .ok_or_else(|| Error::new(auth::Error::InvalidDuration.to_string()))?
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new(auth::Error::InvalidDuration.to_string()))?
        .as_secs() as i64;
    Ok(expiry)
}

fn update_claims_expiry_in_map(dsecs: &str, claims: &mut Map<String, Value>) -> Result<(), Error> {
    let exp = claims
        .get("exp")
        .cloned()
        .ok_or_else(|| Error::new("token expired"))?;

    let updated_exp = match exp {
        Value::String(value) => update_claims_expiry(dsecs, value)?,
        Value::Number(value) => {
            if let Some(parsed) = value.as_i64() {
                update_claims_expiry(dsecs, parsed)?
            } else if let Some(parsed) = value.as_u64() {
                update_claims_expiry(dsecs, parsed)?
            } else if let Some(parsed) = value.as_f64() {
                update_claims_expiry(dsecs, parsed)?
            } else {
                return Err(Error::new(auth::Error::InvalidDuration.to_string()));
            }
        }
        _ => return Err(Error::new(auth::Error::InvalidDuration.to_string())),
    };

    claims.insert("exp".to_owned(), Value::Number(updated_exp.into()));
    Ok(())
}

fn validate_audience(claims: &Map<String, Value>, client_id: &str) -> Result<(), Error> {
    if claim_contains_value(claims.get(AUD_CLAIM), client_id) {
        return Ok(());
    }

    if claim_contains_value(claims.get(AZP_CLAIM), client_id) {
        return Ok(());
    }

    Err(Error::new(
        "STS JWT Token has `azp` claim invalid, `azp` must match configured OpenID Client ID",
    ))
}

fn claim_contains_value(value: Option<&Value>, expected: &str) -> bool {
    match value {
        Some(Value::String(candidate)) => candidate == expected,
        Some(Value::Array(values)) => values.iter().any(|value| value.as_str() == Some(expected)),
        _ => false,
    }
}

fn validate_time_claims(claims: &Map<String, Value>) -> Result<(), Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::new("system clock before unix epoch"))?
        .as_secs() as i64;

    if let Some(exp) = claims.get("exp") {
        let exp = json_value_to_i64(exp)?;
        if now > exp {
            return Err(Error::new("token expired"));
        }
    }

    if let Some(nbf) = claims.get("nbf") {
        let nbf = json_value_to_i64(nbf)?;
        if now < nbf {
            return Err(Error::new("token is not valid yet"));
        }
    }

    Ok(())
}

fn json_value_to_i64(value: &Value) -> Result<i64, Error> {
    match value {
        Value::String(value) => auth::exp_to_int64(value.clone()).map_err(Error::from),
        Value::Number(value) => {
            if let Some(parsed) = value.as_i64() {
                auth::exp_to_int64(parsed).map_err(Error::from)
            } else if let Some(parsed) = value.as_u64() {
                auth::exp_to_int64(parsed).map_err(Error::from)
            } else if let Some(parsed) = value.as_f64() {
                auth::exp_to_int64(parsed).map_err(Error::from)
            } else {
                Err(Error::new(auth::Error::InvalidDuration.to_string()))
            }
        }
        _ => Err(Error::new(auth::Error::InvalidDuration.to_string())),
    }
}

fn split_token(
    token: &str,
) -> Result<(Map<String, Value>, Map<String, Value>, String, Vec<u8>), Error> {
    let parts: Vec<_> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::new("signature is invalid"));
    }

    let header_bytes = URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|error| Error::new(error.to_string()))?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|error| Error::new(error.to_string()))?;
    let signature = URL_SAFE_NO_PAD
        .decode(parts[2])
        .map_err(|error| Error::new(error.to_string()))?;

    let header: Map<String, Value> =
        serde_json::from_slice(&header_bytes).map_err(|error| Error::new(error.to_string()))?;
    let payload: Map<String, Value> =
        serde_json::from_slice(&payload_bytes).map_err(|error| Error::new(error.to_string()))?;

    Ok((
        header,
        payload,
        format!("{}.{}", parts[0], parts[1]),
        signature,
    ))
}

fn verify_hmac(
    algorithm: &str,
    key: &[u8],
    signing_input: &[u8],
    signature: &[u8],
) -> Result<(), Error> {
    match algorithm {
        "HS256" => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key)
                .map_err(|error| Error::new(error.to_string()))?;
            mac.update(signing_input);
            mac.verify_slice(signature)
                .map_err(|_| Error::new("signature is invalid"))?;
        }
        "HS384" => {
            let mut mac = Hmac::<Sha384>::new_from_slice(key)
                .map_err(|error| Error::new(error.to_string()))?;
            mac.update(signing_input);
            mac.verify_slice(signature)
                .map_err(|_| Error::new("signature is invalid"))?;
        }
        "HS512" => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key)
                .map_err(|error| Error::new(error.to_string()))?;
            mac.update(signing_input);
            mac.verify_slice(signature)
                .map_err(|_| Error::new("signature is invalid"))?;
        }
        other => {
            return Err(Error::new(format!(
                "signing method ({other}) is unavailable."
            )));
        }
    }
    Ok(())
}

fn parse_go_duration(input: &str) -> Result<Duration, Error> {
    if input.is_empty() {
        return Err(Error::new("empty duration"));
    }

    let (digits, unit) = input.split_at(input.len().saturating_sub(1));
    let value = digits
        .parse::<u64>()
        .map_err(|_| Error::new("invalid duration"))?;
    match unit {
        "s" => Ok(Duration::from_secs(value)),
        "m" => Ok(Duration::from_secs(value * 60)),
        "h" => Ok(Duration::from_secs(value * 3600)),
        _ => Err(Error::new("invalid duration")),
    }
}
