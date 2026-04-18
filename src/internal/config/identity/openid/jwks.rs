use std::fmt;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwks {
    #[serde(default)]
    pub keys: Vec<JwksKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwksKey {
    pub kty: String,
    #[serde(default)]
    pub use_: String,
    #[serde(default)]
    pub kid: String,
    #[serde(default)]
    pub alg: String,
    #[serde(default)]
    pub crv: String,
    #[serde(default)]
    pub x: String,
    #[serde(default)]
    pub y: String,
    #[serde(default)]
    pub d: String,
    #[serde(default)]
    pub n: String,
    #[serde(default)]
    pub e: String,
    #[serde(default)]
    pub k: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicKey {
    Rsa {
        n: Vec<u8>,
        e: u64,
    },
    Ec {
        curve: String,
        x: Vec<u8>,
        y: Vec<u8>,
    },
    Ed25519(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwksError(String);

impl JwksError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for JwksError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for JwksError {}

impl JwksKey {
    pub fn decode_public_key(&self) -> Result<PublicKey, JwksError> {
        match self.kty.as_str() {
            "RSA" => {
                if self.n.is_empty() || self.e.is_empty() {
                    return Err(JwksError::new("malformed JWK RSA key"));
                }
                let ebuf = URL_SAFE_NO_PAD
                    .decode(&self.e)
                    .map_err(|_| JwksError::new("malformed JWK RSA key"))?;
                let nbuf = URL_SAFE_NO_PAD
                    .decode(&self.n)
                    .map_err(|_| JwksError::new("malformed JWK RSA key"))?;

                let exponent = ebuf
                    .iter()
                    .fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));

                Ok(PublicKey::Rsa {
                    n: nbuf,
                    e: exponent,
                })
            }
            "EC" => {
                if self.crv.is_empty() || self.x.is_empty() || self.y.is_empty() {
                    return Err(JwksError::new("malformed JWK EC key"));
                }
                match self.crv.as_str() {
                    "P-224" | "P-256" | "P-384" | "P-521" => {}
                    other => return Err(JwksError::new(format!("Unknown curve type: {other}"))),
                }

                let x = URL_SAFE_NO_PAD
                    .decode(&self.x)
                    .map_err(|_| JwksError::new("malformed JWK EC key"))?;
                let y = URL_SAFE_NO_PAD
                    .decode(&self.y)
                    .map_err(|_| JwksError::new("malformed JWK EC key"))?;

                Ok(PublicKey::Ec {
                    curve: self.crv.clone(),
                    x,
                    y,
                })
            }
            _ => {
                if self.alg == "EdDSA" && self.crv == "Ed25519" && !self.x.is_empty() {
                    let public = URL_SAFE_NO_PAD
                        .decode(&self.x)
                        .map_err(|_| JwksError::new("malformed JWK EC key"))?;
                    Ok(PublicKey::Ed25519(public))
                } else {
                    Err(JwksError::new(format!("Unknown JWK key type {}", self.kty)))
                }
            }
        }
    }
}
