use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::{Sha256, Sha384, Sha512};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationErrorKind {
    Malformed,
    Unverifiable,
    SignatureInvalid,
    ClaimsInvalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    pub kind: ValidationErrorKind,
    pub message: String,
}

impl ValidationError {
    fn malformed(message: impl Into<String>) -> Self {
        Self {
            kind: ValidationErrorKind::Malformed,
            message: message.into(),
        }
    }

    fn unverifiable(message: impl Into<String>) -> Self {
        Self {
            kind: ValidationErrorKind::Unverifiable,
            message: message.into(),
        }
    }

    fn signature_invalid() -> Self {
        Self {
            kind: ValidationErrorKind::SignatureInvalid,
            message: "signature is invalid".to_owned(),
        }
    }

    fn claims_invalid(message: impl Into<String>) -> Self {
        Self {
            kind: ValidationErrorKind::ClaimsInvalid,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ValidationError {}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MapClaims {
    pub access_key: String,
    pub map_claims: HashMap<String, Value>,
}

impl MapClaims {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, key: impl Into<String>, val: Value) {
        self.map_claims.insert(key.into(), val);
    }

    pub fn set_expiry(&mut self, unix_time: i64) {
        self.map_claims
            .insert("exp".to_owned(), Value::Number(unix_time.into()));
    }

    pub fn set_access_key(&mut self, access_key: impl Into<String>) {
        let access_key = access_key.into();
        self.map_claims
            .insert("sub".to_owned(), Value::String(access_key.clone()));
        self.map_claims
            .insert("accessKey".to_owned(), Value::String(access_key.clone()));
        self.access_key = access_key;
    }

    pub fn lookup(&self, key: &str) -> Option<String> {
        self.map_claims
            .get(key)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    }

    pub fn valid(&self) -> Result<(), ValidationError> {
        validate_time_claims(&self.map_claims)?;
        if self.access_key.is_empty() {
            return Err(ValidationError::claims_invalid("accessKey/sub missing"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct StandardClaims {
    pub access_key: String,
    pub audience: String,
    pub expires_at: i64,
    pub issued_at: i64,
    pub issuer: String,
    pub not_before: i64,
    pub subject: String,
}

impl StandardClaims {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_access_key(&mut self, access_key: impl Into<String>) {
        let access_key = access_key.into();
        self.subject = access_key.clone();
        self.access_key = access_key;
    }

    pub fn set_expiry(&mut self, unix_time: i64) {
        self.expires_at = unix_time;
    }

    pub fn valid(&self) -> Result<(), ValidationError> {
        let now = now_unix();
        if self.expires_at != 0 && now > self.expires_at {
            return Err(ValidationError::claims_invalid("token is expired"));
        }
        if self.not_before != 0 && now < self.not_before {
            return Err(ValidationError::claims_invalid("token is not valid yet"));
        }
        if self.access_key.is_empty() && self.subject.is_empty() {
            return Err(ValidationError::claims_invalid("accessKey/sub missing"));
        }
        Ok(())
    }
}

pub fn parse_with_claims(
    token: &str,
    claims: &mut MapClaims,
    key_fn: Option<fn(&MapClaims) -> Result<Vec<u8>, ValidationError>>,
) -> Result<(), ValidationError> {
    let key_fn = key_fn.ok_or_else(|| ValidationError::unverifiable("no Keyfunc was provided."))?;
    let algorithm = parse_unverified_map_claims(token, claims)?;
    claims.access_key = claims
        .lookup("accessKey")
        .or_else(|| claims.lookup("sub"))
        .ok_or_else(|| ValidationError::claims_invalid("accessKey/sub missing"))?;

    let key = key_fn(claims)?;
    verify_signature(token, &key, &algorithm)?;
    claims.valid()
}

pub fn parse_with_claims_and_key(
    token: &str,
    claims: &mut MapClaims,
    key: &[u8],
) -> Result<(), ValidationError> {
    if key.is_empty() {
        return Err(ValidationError::unverifiable("no key was provided."));
    }
    let algorithm = parse_unverified_map_claims(token, claims)?;
    claims.access_key = claims
        .lookup("accessKey")
        .or_else(|| claims.lookup("sub"))
        .ok_or_else(|| ValidationError::claims_invalid("accessKey/sub missing"))?;
    verify_signature(token, key, &algorithm)?;
    claims.valid()
}

pub fn parse_with_standard_claims(
    token: &str,
    claims: &mut StandardClaims,
    key: &[u8],
) -> Result<(), ValidationError> {
    if key.is_empty() {
        return Err(ValidationError::unverifiable("no key was provided."));
    }
    let algorithm = parse_unverified_standard_claims(token, claims)?;
    verify_signature(token, key, &algorithm)?;
    if claims.access_key.is_empty() && claims.subject.is_empty() {
        return Err(ValidationError::claims_invalid("accessKey/sub missing"));
    }
    claims.valid()
}

fn parse_unverified_map_claims(
    token: &str,
    claims: &mut MapClaims,
) -> Result<String, ValidationError> {
    let (header, payload, _) = split_token(token)?;
    let algorithm = header
        .get("alg")
        .and_then(Value::as_str)
        .ok_or_else(|| ValidationError::malformed("missing alg"))?
        .to_owned();

    claims.map_claims = payload
        .as_object()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .collect();

    if !matches!(algorithm.as_str(), "HS256" | "HS384" | "HS512") {
        return Err(ValidationError::unverifiable(format!(
            "signing method ({algorithm}) is unavailable."
        )));
    }
    Ok(algorithm)
}

fn parse_unverified_standard_claims(
    token: &str,
    claims: &mut StandardClaims,
) -> Result<String, ValidationError> {
    let (header, payload, _) = split_token(token)?;
    let algorithm = header
        .get("alg")
        .and_then(Value::as_str)
        .ok_or_else(|| ValidationError::malformed("missing alg"))?
        .to_owned();

    claims.access_key = get_string(&payload, "accessKey").unwrap_or_default();
    claims.audience = get_string(&payload, "aud").unwrap_or_default();
    claims.expires_at = get_i64(&payload, "exp").unwrap_or_default();
    claims.issued_at = get_i64(&payload, "iat").unwrap_or_default();
    claims.issuer = get_string(&payload, "iss").unwrap_or_default();
    claims.not_before = get_i64(&payload, "nbf").unwrap_or_default();
    claims.subject = get_string(&payload, "sub").unwrap_or_default();

    if !matches!(algorithm.as_str(), "HS256" | "HS384" | "HS512") {
        return Err(ValidationError::unverifiable(format!(
            "signing method ({algorithm}) is unavailable."
        )));
    }
    Ok(algorithm)
}

fn split_token(token: &str) -> Result<(Map<String, Value>, Value, String), ValidationError> {
    let parts: Vec<_> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(ValidationError::signature_invalid());
    }

    let header_bytes = URL_SAFE_NO_PAD
        .decode(parts[0])
        .map_err(|error| ValidationError::malformed(error.to_string()))?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|error| ValidationError::malformed(error.to_string()))?;

    let header: Map<String, Value> = serde_json::from_slice(&header_bytes)
        .map_err(|error| ValidationError::malformed(error.to_string()))?;
    let payload: Value = serde_json::from_slice(&payload_bytes)
        .map_err(|error| ValidationError::malformed(error.to_string()))?;

    Ok((header, payload, parts[2].to_owned()))
}

fn verify_signature(token: &str, key: &[u8], algorithm: &str) -> Result<(), ValidationError> {
    let index = token
        .rfind('.')
        .ok_or_else(ValidationError::signature_invalid)?;
    let signature = URL_SAFE_NO_PAD
        .decode(&token[index + 1..])
        .map_err(|error| ValidationError::malformed(error.to_string()))?;

    let signed = token[..index].as_bytes();
    let expected = match algorithm {
        "HS256" => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key)
                .map_err(|error| ValidationError::unverifiable(error.to_string()))?;
            mac.update(signed);
            mac.finalize().into_bytes().to_vec()
        }
        "HS384" => {
            let mut mac = <Hmac<Sha384> as Mac>::new_from_slice(key)
                .map_err(|error| ValidationError::unverifiable(error.to_string()))?;
            mac.update(signed);
            mac.finalize().into_bytes().to_vec()
        }
        "HS512" => {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key)
                .map_err(|error| ValidationError::unverifiable(error.to_string()))?;
            mac.update(signed);
            mac.finalize().into_bytes().to_vec()
        }
        _ => {
            return Err(ValidationError::unverifiable(format!(
                "signing method ({algorithm}) is unavailable."
            )))
        }
    };

    if expected == signature {
        Ok(())
    } else {
        Err(ValidationError::signature_invalid())
    }
}

fn validate_time_claims(claims: &HashMap<String, Value>) -> Result<(), ValidationError> {
    let now = now_unix();
    if let Some(exp) = claims.get("exp").and_then(Value::as_i64) {
        if now > exp {
            return Err(ValidationError::claims_invalid("token is expired"));
        }
    }
    if let Some(nbf) = claims.get("nbf").and_then(Value::as_i64) {
        if now < nbf {
            return Err(ValidationError::claims_invalid("token is not valid yet"));
        }
    }
    Ok(())
}

fn get_string(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn get_i64(payload: &Value, key: &str) -> Option<i64> {
    payload.get(key).and_then(Value::as_i64)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as i64
}
