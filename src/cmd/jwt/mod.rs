use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha512;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cmd::TestRequest;
use crate::internal::jwt::{parse_with_claims_and_key, MapClaims, ValidationError};

pub const DEFAULT_JWT_EXPIRY_SECS: i64 = 24 * 60 * 60;
pub const ERR_NO_AUTH_TOKEN: &str = "no auth token";
pub const ERR_AUTHENTICATION: &str = "authentication failed";

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_secs() as i64
}

fn sign_hs512(payload: &serde_json::Value, secret_key: &str) -> Result<String, String> {
    let header = json!({
        "alg": "HS512",
        "typ": "JWT",
    });
    let header_json = serde_json::to_vec(&header).map_err(|err| err.to_string())?;
    let payload_json = serde_json::to_vec(payload).map_err(|err| err.to_string())?;
    let header_segment = URL_SAFE_NO_PAD.encode(header_json);
    let payload_segment = URL_SAFE_NO_PAD.encode(payload_json);
    let signing_input = format!("{header_segment}.{payload_segment}");

    let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(secret_key.as_bytes())
        .map_err(|err| err.to_string())?;
    mac.update(signing_input.as_bytes());
    let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());

    Ok(format!("{signing_input}.{signature}"))
}

pub fn get_token_string(access_key: &str, secret_key: &str) -> Result<String, String> {
    let payload = json!({
        "accessKey": access_key,
        "sub": access_key,
        "exp": now_unix() + DEFAULT_JWT_EXPIRY_SECS,
    });
    sign_hs512(&payload, secret_key)
}

pub fn authenticate_node(access_key: &str, secret_key: &str) -> Result<String, String> {
    get_token_string(access_key, secret_key)
}

#[derive(Debug, Clone)]
pub struct CachedAuthToken {
    access_key: String,
    secret_key: String,
    token: Option<String>,
}

impl CachedAuthToken {
    pub fn token(&mut self) -> Result<String, String> {
        if let Some(token) = &self.token {
            return Ok(token.clone());
        }
        let token = authenticate_node(&self.access_key, &self.secret_key)?;
        self.token = Some(token.clone());
        Ok(token)
    }
}

pub fn new_cached_auth_token(access_key: &str, secret_key: &str) -> CachedAuthToken {
    CachedAuthToken {
        access_key: access_key.to_owned(),
        secret_key: secret_key.to_owned(),
        token: None,
    }
}

pub fn metrics_request_authenticate(
    request: &TestRequest,
    secret_key: &str,
) -> Result<MapClaims, &'static str> {
    let token = request.header("Authorization").ok_or(ERR_NO_AUTH_TOKEN)?;
    if token.trim().is_empty() {
        return Err(ERR_NO_AUTH_TOKEN);
    }

    let mut claims = MapClaims::new();
    parse_with_claims_and_key(token, &mut claims, secret_key.as_bytes())
        .map_err(map_validation_error)?;
    Ok(claims)
}

fn map_validation_error(error: ValidationError) -> &'static str {
    match error.kind {
        crate::internal::jwt::ValidationErrorKind::Malformed
        | crate::internal::jwt::ValidationErrorKind::Unverifiable
        | crate::internal::jwt::ValidationErrorKind::SignatureInvalid
        | crate::internal::jwt::ValidationErrorKind::ClaimsInvalid => ERR_AUTHENTICATION,
    }
}
