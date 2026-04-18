use std::collections::{BTreeMap, HashMap};
use std::fmt;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce as AesNonce};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chacha20::cipher::consts::U10;
use chacha20::hchacha;
use chacha20poly1305::{ChaCha20Poly1305, Key as ChaChaKey, Nonce as ChaChaNonce};
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde_json::{Map, Value};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub const ENV_KMS_ENDPOINT: &str = "MINIO_KMS_SERVER";
pub const ENV_KMS_ENCLAVE: &str = "MINIO_KMS_ENCLAVE";
pub const ENV_KMS_DEFAULT_KEY: &str = "MINIO_KMS_SSE_KEY";
pub const ENV_KMS_API_KEY: &str = "MINIO_KMS_API_KEY";
pub const ENV_KES_ENDPOINT: &str = "MINIO_KMS_KES_ENDPOINT";
pub const ENV_KES_DEFAULT_KEY: &str = "MINIO_KMS_KES_KEY_NAME";
pub const ENV_KES_API_KEY: &str = "MINIO_KMS_KES_API_KEY";
pub const ENV_KES_CLIENT_KEY: &str = "MINIO_KMS_KES_KEY_FILE";
pub const ENV_KES_CLIENT_CERT: &str = "MINIO_KMS_KES_CERT_FILE";
pub const ENV_KES_SERVER_CA: &str = "MINIO_KMS_KES_CAPATH";
pub const ENV_KES_CLIENT_PASSWORD: &str = "MINIO_KMS_KES_KEY_PASSWORD";
pub const ENV_KMS_SECRET_KEY: &str = "MINIO_KMS_SECRET_KEY";
pub const ENV_KMS_SECRET_KEY_FILE: &str = "MINIO_KMS_SECRET_KEY_FILE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub message: String,
}

impl Error {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Context(pub BTreeMap<String, String>);

impl Context {
    pub fn marshal_text(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(&self.0).map_err(|err| Error::new(err.to_string()))
    }
}

impl From<HashMap<String, String>> for Context {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value.into_iter().collect())
    }
}

impl<const N: usize> From<[(&str, &str); N]> for Context {
    fn from(value: [(&str, &str); N]) -> Self {
        Self(
            value
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Dek {
    pub key_id: String,
    pub version: i32,
    pub plaintext: Option<Vec<u8>>,
    pub ciphertext: Vec<u8>,
}

impl Dek {
    pub fn marshal_text(&self) -> Result<Vec<u8>, Error> {
        let mut json = Map::new();
        json.insert("keyid".to_owned(), Value::String(self.key_id.clone()));
        if self.version != 0 {
            json.insert(
                "version".to_owned(),
                Value::Number(serde_json::Number::from(self.version as u64)),
            );
        }
        json.insert(
            "ciphertext".to_owned(),
            Value::String(BASE64_STANDARD.encode(&self.ciphertext)),
        );
        serde_json::to_vec(&Value::Object(json)).map_err(|err| Error::new(err.to_string()))
    }

    pub fn unmarshal_text(&mut self, text: &[u8]) -> Result<(), Error> {
        let value: Value =
            serde_json::from_slice(text).map_err(|err| Error::new(err.to_string()))?;
        let object = value
            .as_object()
            .ok_or_else(|| Error::new("kms: invalid DEK JSON"))?;

        self.key_id = object
            .get("keyid")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.version = object
            .get("version")
            .and_then(Value::as_u64)
            .unwrap_or_default() as i32;
        self.ciphertext = object
            .get("ciphertext")
            .and_then(Value::as_str)
            .map(|s| BASE64_STANDARD.decode(s))
            .transpose()
            .map_err(|err| Error::new(err.to_string()))?
            .unwrap_or_default();
        self.plaintext = None;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GenerateKeyRequest {
    pub name: String,
    pub associated_data: Context,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecryptRequest {
    pub name: String,
    pub version: i32,
    pub ciphertext: Vec<u8>,
    pub associated_data: Context,
}

#[derive(Debug, Clone)]
pub struct BuiltinKms {
    key_id: String,
    key: [u8; 32],
}

impl BuiltinKms {
    pub fn generate_key(&self, request: &GenerateKeyRequest) -> Result<Dek, Error> {
        if request.name != self.key_id {
            return Err(err_key_not_found());
        }

        let associated_data = request.associated_data.marshal_text()?;
        let mut random = [0u8; 28];
        rand::thread_rng().fill_bytes(&mut random);
        let (iv, nonce) = random.split_at(16);

        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.key).expect("32-byte hmac key");
        mac.update(iv);
        let sealing_key = mac.finalize().into_bytes();
        let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("32-byte aes key");

        let mut plaintext = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut plaintext);
        let mut ciphertext = cipher
            .encrypt(
                AesNonce::from_slice(nonce),
                aes_gcm::aead::Payload {
                    msg: &plaintext,
                    aad: &associated_data,
                },
            )
            .map_err(|_| err_decrypt())?;
        ciphertext.extend_from_slice(&random);

        Ok(Dek {
            key_id: request.name.clone(),
            version: 0,
            plaintext: Some(plaintext),
            ciphertext,
        })
    }

    pub fn decrypt(&self, request: &DecryptRequest) -> Result<Vec<u8>, Error> {
        if request.name != self.key_id {
            return Err(err_key_not_found());
        }

        let (ciphertext, kind) = parse_ciphertext(&request.ciphertext)?;
        if ciphertext.len() < 28 {
            return Err(err_decrypt());
        }

        let split_at = ciphertext.len() - 28;
        let (sealed, random) = ciphertext.split_at(split_at);
        let (iv, nonce) = random.split_at(16);
        let associated_data = request.associated_data.marshal_text()?;

        match kind {
            CipherKind::Aes256 => {
                let mut mac =
                    <HmacSha256 as Mac>::new_from_slice(&self.key).expect("32-byte hmac key");
                mac.update(iv);
                let sealing_key = mac.finalize().into_bytes();
                let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("32-byte aes key");
                cipher
                    .decrypt(
                        AesNonce::from_slice(nonce),
                        aes_gcm::aead::Payload {
                            msg: sealed,
                            aad: &associated_data,
                        },
                    )
                    .map_err(|_| err_decrypt())
            }
            CipherKind::ChaCha20 => {
                let sealing_key = hchacha::<U10>(&self.key.into(), iv.into());
                let cipher = ChaCha20Poly1305::new(ChaChaKey::from_slice(&sealing_key));
                cipher
                    .decrypt(
                        ChaChaNonce::from_slice(nonce),
                        chacha20poly1305::aead::Payload {
                            msg: sealed,
                            aad: &associated_data,
                        },
                    )
                    .map_err(|_| err_decrypt())
            }
        }
    }
}

pub fn parse_secret_key(value: &str) -> Result<BuiltinKms, Error> {
    let (key_id, b64_key) = value
        .split_once(':')
        .ok_or_else(|| Error::new("kms: invalid secret key format"))?;
    let key = BASE64_STANDARD
        .decode(b64_key)
        .map_err(|err| Error::new(err.to_string()))?;
    new_builtin(key_id, &key)
}

pub fn new_builtin(key_id: &str, key: &[u8]) -> Result<BuiltinKms, Error> {
    let key: [u8; 32] = key
        .try_into()
        .map_err(|_| Error::new(format!("kms: invalid key length {}", key.len())))?;
    Ok(BuiltinKms {
        key_id: key_id.to_owned(),
        key,
    })
}

pub fn is_present() -> Result<bool, Error> {
    let env = std::env::vars().collect::<HashMap<_, _>>();
    is_present_in(&env)
}

pub fn is_present_in(env: &HashMap<String, String>) -> Result<bool, Error> {
    let has_any = |keys: &[&str]| keys.iter().any(|key| env.contains_key(*key));

    let kms_present = has_any(&[
        ENV_KMS_ENDPOINT,
        ENV_KMS_ENCLAVE,
        ENV_KMS_API_KEY,
        ENV_KMS_DEFAULT_KEY,
    ]);
    let kes_present = has_any(&[
        ENV_KES_ENDPOINT,
        ENV_KES_DEFAULT_KEY,
        ENV_KES_API_KEY,
        ENV_KES_CLIENT_KEY,
        ENV_KES_CLIENT_CERT,
        ENV_KES_CLIENT_PASSWORD,
        ENV_KES_SERVER_CA,
    ]);

    let secret_key_present = env
        .get(ENV_KMS_SECRET_KEY)
        .map(|value| !value.is_empty())
        .unwrap_or(false);
    let secret_key_file_present = env
        .get(ENV_KMS_SECRET_KEY_FILE)
        .map(|value| !value.is_empty() && std::path::Path::new(value).exists())
        .unwrap_or(false);
    let static_key_present = secret_key_present || secret_key_file_present;

    match (kms_present, kes_present, static_key_present) {
        (true, true, _) => {
            return Err(Error::new(
                "kms: configuration for MinIO KMS and MinIO KES is present",
            ))
        }
        (true, _, true) => {
            return Err(Error::new(
                "kms: configuration for MinIO KMS and static KMS key is present",
            ))
        }
        (_, true, true) => {
            return Err(Error::new(
                "kms: configuration for MinIO KES and static KMS key is present",
            ))
        }
        _ => {}
    }

    if kms_present {
        for key in [
            ENV_KMS_ENDPOINT,
            ENV_KMS_ENCLAVE,
            ENV_KMS_DEFAULT_KEY,
            ENV_KMS_API_KEY,
        ] {
            if !env.contains_key(key) {
                return Err(Error::new(format!(
                    "kms: incomplete configuration for MinIO KMS: missing '{key}'"
                )));
            }
        }
        return Ok(true);
    }

    if static_key_present {
        if secret_key_present && secret_key_file_present {
            return Err(Error::new(format!(
                "kms: invalid configuration for static KMS key: '{ENV_KMS_SECRET_KEY}' and '{ENV_KMS_SECRET_KEY_FILE}' are present"
            )));
        }
        return Ok(true);
    }

    if kes_present {
        for key in [ENV_KES_ENDPOINT, ENV_KES_DEFAULT_KEY] {
            if !env.contains_key(key) {
                return Err(Error::new(format!(
                    "kms: incomplete configuration for MinIO KES: missing '{key}'"
                )));
            }
        }

        let has_client_cert_auth = has_any(&[
            ENV_KES_CLIENT_KEY,
            ENV_KES_CLIENT_CERT,
            ENV_KES_CLIENT_PASSWORD,
        ]);
        if has_client_cert_auth {
            if env.contains_key(ENV_KES_API_KEY) {
                return Err(Error::new(format!(
                    "kms: invalid configuration for MinIO KES: '{ENV_KES_API_KEY}' and client certificate is present"
                )));
            }
            if !env.contains_key(ENV_KES_CLIENT_CERT) {
                return Err(Error::new(format!(
                    "kms: incomplete configuration for MinIO KES: missing '{ENV_KES_CLIENT_CERT}'"
                )));
            }
            if !env.contains_key(ENV_KES_CLIENT_KEY) {
                return Err(Error::new(format!(
                    "kms: incomplete configuration for MinIO KES: missing '{ENV_KES_CLIENT_KEY}'"
                )));
            }
            return Ok(true);
        }
        if !env.contains_key(ENV_KES_API_KEY) {
            return Err(Error::new(
                "kms: incomplete configuration for MinIO KES: missing authentication method",
            ));
        }
        return Ok(true);
    }

    Ok(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CipherKind {
    Aes256,
    ChaCha20,
}

fn parse_ciphertext(ciphertext: &[u8]) -> Result<(Vec<u8>, CipherKind), Error> {
    if ciphertext.is_empty() {
        return Ok((Vec::new(), CipherKind::Aes256));
    }

    if ciphertext.first() == Some(&b'{') && ciphertext.last() == Some(&b'}') {
        let value: Value = serde_json::from_slice(ciphertext).map_err(|_| err_decrypt())?;
        let object = value.as_object().ok_or_else(err_decrypt)?;

        let algorithm = object
            .get("aead")
            .and_then(Value::as_str)
            .ok_or_else(err_decrypt)?;
        let iv = decode_b64_field(object, "iv")?;
        let nonce = decode_b64_field(object, "nonce")?;
        let bytes = decode_b64_field(object, "bytes")?;
        if iv.len() != 16 || nonce.len() != 12 {
            return Err(err_decrypt());
        }

        let kind = match algorithm {
            "AES-256-GCM-HMAC-SHA-256" => CipherKind::Aes256,
            "ChaCha20Poly1305" => CipherKind::ChaCha20,
            _ => return Err(err_decrypt()),
        };

        let mut converted = bytes;
        converted.extend_from_slice(&iv);
        converted.extend_from_slice(&nonce);
        return Ok((converted, kind));
    }

    Ok((ciphertext.to_vec(), CipherKind::Aes256))
}

fn decode_b64_field(object: &Map<String, Value>, key: &str) -> Result<Vec<u8>, Error> {
    let encoded = object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(err_decrypt)?;
    BASE64_STANDARD.decode(encoded).map_err(|_| err_decrypt())
}

fn err_key_not_found() -> Error {
    Error::new("key with given key ID does not exist")
}

fn err_decrypt() -> Error {
    Error::new("failed to decrypt ciphertext")
}
