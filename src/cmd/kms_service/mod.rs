use super::*;

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::Utc;
use rand::RngCore;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::sync::{Arc, OnceLock};
use url::Url;

use crate::internal::bucket::encryption::{BucketSseConfig, AWS_KMS};
use crate::internal::config::crypto;
use crate::internal::kms::{self, BuiltinKms, Context};
use rustls::pki_types::PrivateKeyDer;
use rustls::{ClientConfig, RootCertStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum KmsServiceBackend {
    #[default]
    Disabled,
    StaticKey,
    MinioKms,
    Kes,
}
impl_msg_codec!(KmsServiceBackend);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum KmsKeySource {
    #[default]
    ServiceDefault,
    BucketConfig,
    RequestHeader,
}
impl_msg_codec!(KmsKeySource);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KmsKeyChoice {
    pub key_id: String,
    pub source: KmsKeySource,
}
impl_msg_codec!(KmsKeyChoice);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KmsServiceStatus {
    pub configured: bool,
    pub backend: KmsServiceBackend,
    pub endpoint: String,
    pub enclave: String,
    pub default_key: String,
    pub auth_mode: String,
    pub config_encryption_supported: bool,
}
impl_msg_codec!(KmsServiceStatus);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KmsKeyStatus {
    pub key_id: String,
    pub backend: KmsServiceBackend,
    pub exists: bool,
    pub validation_succeeded: bool,
    pub create_supported: bool,
    pub error: String,
}
impl_msg_codec!(KmsKeyStatus);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KmsServiceMetrics {
    pub online: bool,
    pub configured: bool,
    pub backend: KmsServiceBackend,
    pub default_key: String,
}
impl_msg_codec!(KmsServiceMetrics);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct KmsGenerateKeyRequestBody {
    pub key_id: String,
    pub associated_data: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct KmsDekJson {
    pub key_id: String,
    pub version: i32,
    pub plaintext: Option<String>,
    pub ciphertext: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct KmsDecryptKeyRequestBody {
    pub key_id: String,
    pub version: i32,
    pub ciphertext: String,
    pub associated_data: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct KmsDecryptKeyResponse {
    pub plaintext: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ConfigEncryptedObject {
    key_id: String,
    kms_key: Vec<u8>,
    nonce: Vec<u8>,
}

const CONFIG_CIPHERTEXT_VERSION: u8 = 1;
const CONFIG_MAX_METADATA_SIZE: usize = 1 << 20;

#[derive(Debug, Clone)]
pub struct KmsServiceFacade {
    status: KmsServiceStatus,
    builtin: Option<BuiltinKms>,
    remote_api_key: Option<String>,
    remote_cert_authority: Option<String>,
    remote_client_cert: Option<String>,
    remote_client_key: Option<String>,
    remote_client_key_password: Option<String>,
}

impl KmsServiceFacade {
    pub fn from_env_map(env: &HashMap<String, String>) -> Result<Self, String> {
        let configured = kms::is_present_in(env).map_err(|err| err.to_string())?;
        let backend = detect_backend(env);
        let default_key = default_key_for_env(env, backend)?;
        let auth_mode = auth_mode_for_env(env, backend);
        let endpoint = endpoint_for_env(env, backend);
        let enclave = env.get(kms::ENV_KMS_ENCLAVE).cloned().unwrap_or_default();
        let builtin = build_builtin_from_env(env, backend)?;

        Ok(Self {
            status: KmsServiceStatus {
                configured,
                backend,
                endpoint,
                enclave,
                default_key,
                auth_mode,
                config_encryption_supported: configured,
            },
            builtin,
            remote_api_key: remote_api_key_for_env(env, backend),
            remote_cert_authority: remote_cert_authority_for_env(env, backend),
            remote_client_cert: remote_client_cert_for_env(env, backend),
            remote_client_key: remote_client_key_for_env(env, backend),
            remote_client_key_password: remote_client_key_password_for_env(env, backend),
        })
    }

    pub fn from_current_env() -> Result<Self, String> {
        let env = std::env::vars().collect::<HashMap<_, _>>();
        Self::from_env_map(&env)
    }

    pub fn status(&self) -> &KmsServiceStatus {
        &self.status
    }

    pub fn api_key_auth_token(&self) -> Option<&str> {
        self.remote_api_key.as_deref()
    }

    pub fn default_key(&self) -> Option<&str> {
        if self.status.default_key.is_empty() {
            None
        } else {
            Some(self.status.default_key.as_str())
        }
    }

    pub fn resolve_bucket_default_key(
        &self,
        bucket_config: Option<&BucketSseConfig>,
    ) -> Option<KmsKeyChoice> {
        if let Some(config) = bucket_config {
            if config.algo() == AWS_KMS {
                let key_id = config.key_id();
                if !key_id.is_empty() {
                    return Some(KmsKeyChoice {
                        key_id,
                        source: KmsKeySource::BucketConfig,
                    });
                }
            }
        }

        self.default_key().map(|key_id| KmsKeyChoice {
            key_id: key_id.to_string(),
            source: KmsKeySource::ServiceDefault,
        })
    }

    pub fn resolve_object_default_key(
        &self,
        bucket_config: Option<&BucketSseConfig>,
        headers: &BTreeMap<String, String>,
    ) -> Option<KmsKeyChoice> {
        if let Some(key_id) = headers
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .and_then(|value| normalize_kms_key_id(value))
        {
            return Some(KmsKeyChoice {
                key_id,
                source: KmsKeySource::RequestHeader,
            });
        }

        let kms_requested = headers
            .get("x-amz-server-side-encryption")
            .is_some_and(|value| value.eq_ignore_ascii_case(AWS_KMS));
        if kms_requested {
            return self.resolve_bucket_default_key(bucket_config);
        }

        self.resolve_bucket_default_key(bucket_config)
    }

    pub fn encrypt_config_bytes(
        &self,
        plaintext: &[u8],
        context: Context,
    ) -> Result<Vec<u8>, String> {
        match self.status.backend {
            KmsServiceBackend::Disabled => {
                Err("kms config encryption is unavailable for this backend".to_string())
            }
            KmsServiceBackend::StaticKey => {
                let builtin = self.builtin.as_ref().ok_or_else(|| {
                    "kms config encryption is unavailable for this backend".to_string()
                })?;
                crypto::encrypt_bytes(builtin, plaintext, context).map_err(|err| err.to_string())
            }
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes => {
                self.remote_encrypt_config_bytes(plaintext, context)
            }
        }
    }

    pub fn decrypt_config_bytes(
        &self,
        ciphertext: &[u8],
        context: Context,
    ) -> Result<Vec<u8>, String> {
        match self.status.backend {
            KmsServiceBackend::Disabled => {
                Err("kms config encryption is unavailable for this backend".to_string())
            }
            KmsServiceBackend::StaticKey => {
                let builtin = self.builtin.as_ref().ok_or_else(|| {
                    "kms config encryption is unavailable for this backend".to_string()
                })?;
                crypto::decrypt_bytes(builtin, ciphertext, context).map_err(|err| err.to_string())
            }
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes => {
                self.remote_decrypt_config_bytes(ciphertext, context)
            }
        }
    }

    pub fn generate_data_key(
        &self,
        key_id: Option<&str>,
        context: Context,
    ) -> Result<kms::Dek, String> {
        let resolved = key_id
            .and_then(normalize_kms_key_id)
            .or_else(|| self.default_key().map(ToString::to_string))
            .unwrap_or_default();
        if resolved.is_empty() {
            return Err("kms key id is required".to_string());
        }
        match self.status.backend {
            KmsServiceBackend::Disabled => Err("kms is not configured".to_string()),
            KmsServiceBackend::StaticKey => self
                .builtin
                .as_ref()
                .ok_or_else(|| "kms is not configured".to_string())?
                .generate_key(&kms::GenerateKeyRequest {
                    name: resolved,
                    associated_data: context,
                })
                .map_err(|err| err.to_string()),
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes => {
                self.remote_generate_key(&resolved, &context)
            }
        }
    }

    pub fn decrypt_data_key(
        &self,
        key_id: &str,
        version: i32,
        ciphertext: &[u8],
        context: Context,
    ) -> Result<Vec<u8>, String> {
        let resolved =
            normalize_kms_key_id(key_id).ok_or_else(|| "kms key id is required".to_string())?;
        match self.status.backend {
            KmsServiceBackend::Disabled => Err("kms is not configured".to_string()),
            KmsServiceBackend::StaticKey => self
                .builtin
                .as_ref()
                .ok_or_else(|| "kms is not configured".to_string())?
                .decrypt(&kms::DecryptRequest {
                    name: resolved,
                    version,
                    ciphertext: ciphertext.to_vec(),
                    associated_data: context,
                })
                .map_err(|err| err.to_string()),
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes => {
                self.remote_decrypt_key(&resolved, version, ciphertext, &context)
            }
        }
    }

    pub fn key_status(&self, key_id: Option<&str>) -> Result<KmsKeyStatus, String> {
        let resolved = key_id
            .and_then(normalize_kms_key_id)
            .or_else(|| self.default_key().map(ToString::to_string))
            .unwrap_or_default();
        if resolved.is_empty() {
            return Err("kms key id is required".to_string());
        }

        let mut status = KmsKeyStatus {
            key_id: resolved.clone(),
            backend: self.status.backend,
            create_supported: !matches!(self.status.backend, KmsServiceBackend::Disabled),
            ..KmsKeyStatus::default()
        };

        match self.status.backend {
            KmsServiceBackend::Disabled => {
                status.error = "kms is not configured".to_string();
            }
            KmsServiceBackend::StaticKey => {
                let exists = self.default_key() == Some(resolved.as_str());
                status.exists = exists;
                status.create_supported = false;
                if !exists {
                    status.error = "key not found".to_string();
                } else if let Some(builtin) = &self.builtin {
                    let context = Context::from([("MinIO admin API", "KMSKeyStatusHandler")]);
                    let dek = builtin
                        .generate_key(&kms::GenerateKeyRequest {
                            name: resolved.clone(),
                            associated_data: context.clone(),
                        })
                        .map_err(|err| err.to_string())?;
                    builtin
                        .decrypt(&kms::DecryptRequest {
                            name: resolved,
                            version: dek.version,
                            ciphertext: dek.ciphertext,
                            associated_data: context,
                        })
                        .map_err(|err| err.to_string())?;
                    status.validation_succeeded = true;
                }
            }
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes => {
                return self.remote_key_status(&resolved);
            }
        }

        Ok(status)
    }

    pub fn create_key(&self, key_id: &str) -> Result<KmsKeyStatus, String> {
        let key_id =
            normalize_kms_key_id(key_id).ok_or_else(|| "kms key id is required".to_string())?;
        match self.status.backend {
            KmsServiceBackend::Disabled => Err("kms is not configured".to_string()),
            KmsServiceBackend::StaticKey => {
                if self.default_key() == Some(key_id.as_str()) {
                    self.key_status(Some(&key_id))
                } else {
                    Err("static kms backend cannot create additional keys".to_string())
                }
            }
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes => self.remote_create_key(&key_id),
        }
    }

    pub fn metrics(&self) -> KmsServiceMetrics {
        if matches!(
            self.status.backend,
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes
        ) {
            if let Ok(metrics) = self.remote_metrics() {
                return metrics;
            }
        }
        KmsServiceMetrics {
            online: self.status.configured,
            configured: self.status.configured,
            backend: self.status.backend,
            default_key: self.status.default_key.clone(),
        }
    }

    pub fn list_keys(&self, pattern: Option<&str>) -> Vec<KmsKeyStatus> {
        if matches!(
            self.status.backend,
            KmsServiceBackend::MinioKms | KmsServiceBackend::Kes
        ) {
            if let Ok(keys) = self.remote_list_keys(pattern) {
                return keys;
            }
        }
        let Some(default_key) = self.default_key() else {
            return Vec::new();
        };
        if !matches_pattern(default_key, pattern.unwrap_or_default()) {
            return Vec::new();
        }
        self.key_status(Some(default_key)).into_iter().collect()
    }

    fn remote_key_status(&self, key_id: &str) -> Result<KmsKeyStatus, String> {
        self.remote_json(
            "GET",
            "/minio/kms/v1/key/status",
            &[("key-id", key_id)],
            None,
        )
    }

    fn remote_generate_key(&self, key_id: &str, context: &Context) -> Result<kms::Dek, String> {
        let body = serde_json::to_string(&KmsGenerateKeyRequestBody {
            key_id: key_id.to_string(),
            associated_data: context.0.clone(),
        })
        .map_err(|err| err.to_string())?;
        let response: KmsDekJson =
            self.remote_json("POST", "/minio/kms/v1/key/generate", &[], Some(&body))?;
        Ok(kms::Dek {
            key_id: response.key_id,
            version: response.version,
            plaintext: response
                .plaintext
                .map(|value| BASE64_STANDARD.decode(value).map_err(|err| err.to_string()))
                .transpose()?,
            ciphertext: BASE64_STANDARD
                .decode(response.ciphertext)
                .map_err(|err| err.to_string())?,
        })
    }

    fn remote_decrypt_key(
        &self,
        key_id: &str,
        version: i32,
        ciphertext: &[u8],
        context: &Context,
    ) -> Result<Vec<u8>, String> {
        let body = serde_json::to_string(&KmsDecryptKeyRequestBody {
            key_id: key_id.to_string(),
            version,
            ciphertext: BASE64_STANDARD.encode(ciphertext),
            associated_data: context.0.clone(),
        })
        .map_err(|err| err.to_string())?;
        let response: KmsDecryptKeyResponse =
            self.remote_json("POST", "/minio/kms/v1/key/decrypt", &[], Some(&body))?;
        BASE64_STANDARD
            .decode(response.plaintext)
            .map_err(|err| err.to_string())
    }

    fn remote_create_key(&self, key_id: &str) -> Result<KmsKeyStatus, String> {
        self.remote_json(
            "POST",
            "/minio/kms/v1/key/create",
            &[("key-id", key_id)],
            None,
        )
    }

    fn remote_metrics(&self) -> Result<KmsServiceMetrics, String> {
        self.remote_json("GET", "/minio/kms/v1/metrics", &[], None)
    }

    fn remote_list_keys(&self, pattern: Option<&str>) -> Result<Vec<KmsKeyStatus>, String> {
        let params = pattern
            .map(|pattern| vec![("pattern", pattern)])
            .unwrap_or_default();
        self.remote_json("GET", "/minio/kms/v1/key/list", &params, None)
    }

    fn remote_encrypt_config_bytes(
        &self,
        plaintext: &[u8],
        context: Context,
    ) -> Result<Vec<u8>, String> {
        let key_id = self
            .default_key()
            .map(ToString::to_string)
            .unwrap_or_else(|| "my-key".to_string());
        let dek = self.generate_data_key(Some(&key_id), context)?;
        let plaintext_key = dek
            .plaintext
            .clone()
            .ok_or_else(|| "kms returned no plaintext key".to_string())?;

        let cipher = Aes256Gcm::new_from_slice(&plaintext_key)
            .map_err(|_| "failed to initialize cipher".to_string())?;
        let mut nonce = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: b"",
                },
            )
            .map_err(|_| "failed to encrypt plaintext".to_string())?;

        let metadata = serde_json::to_vec(&ConfigEncryptedObject {
            key_id: dek.key_id,
            kms_key: dek.ciphertext,
            nonce: nonce.to_vec(),
        })
        .map_err(|err| err.to_string())?;
        if metadata.len() > CONFIG_MAX_METADATA_SIZE {
            return Err("config: encryption metadata is too large".to_string());
        }

        let mut output = Vec::with_capacity(1 + 4 + metadata.len() + ciphertext.len());
        output.push(CONFIG_CIPHERTEXT_VERSION);
        output.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
        output.extend_from_slice(&metadata);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    fn remote_decrypt_config_bytes(
        &self,
        ciphertext: &[u8],
        context: Context,
    ) -> Result<Vec<u8>, String> {
        if ciphertext.len() < 5 {
            return Err("failed to read config ciphertext header".to_string());
        }
        if ciphertext[0] != CONFIG_CIPHERTEXT_VERSION {
            return Err(format!(
                "config: unknown ciphertext version {}",
                ciphertext[0]
            ));
        }
        let metadata_size =
            u32::from_le_bytes(ciphertext[1..5].try_into().expect("header slice")) as usize;
        if metadata_size > CONFIG_MAX_METADATA_SIZE {
            return Err("config: encryption metadata is too large".to_string());
        }
        if ciphertext.len() < 5 + metadata_size {
            return Err("failed to read config metadata".to_string());
        }
        let metadata: ConfigEncryptedObject =
            serde_json::from_slice(&ciphertext[5..5 + metadata_size])
                .map_err(|err| err.to_string())?;
        let plaintext_key =
            self.decrypt_data_key(&metadata.key_id, 0, &metadata.kms_key, context)?;
        if metadata.nonce.len() != 12 {
            return Err("config: invalid nonce".to_string());
        }
        let cipher = Aes256Gcm::new_from_slice(&plaintext_key)
            .map_err(|_| "failed to initialize cipher".to_string())?;
        cipher
            .decrypt(
                Nonce::from_slice(&metadata.nonce),
                Payload {
                    msg: &ciphertext[5 + metadata_size..],
                    aad: b"",
                },
            )
            .map_err(|_| "failed to decrypt ciphertext".to_string())
    }

    fn remote_json<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        params: &[(&str, &str)],
        body: Option<&str>,
    ) -> Result<T, String> {
        let body_bytes = body.unwrap_or_default().as_bytes();
        let (url, signed_headers, auth_header) =
            self.remote_url(method, path, params, body_bytes)?;
        let mut request = self.remote_agent()?.request(method, &url);
        if let Some(headers) = signed_headers {
            for (key, value) in headers {
                request = request.set(&key, &value);
            }
        } else if let Some(header) = auth_header {
            request = request.set("Authorization", &header);
        } else if let Some(api_key) = &self.remote_api_key {
            request = request.set("Authorization", &format!("Bearer {api_key}"));
        }
        let response = match body {
            Some(body) => request.send_string(body),
            None => request.call(),
        }
        .map_err(|err| err.to_string())?;
        let text = response.into_string().map_err(|err| err.to_string())?;
        serde_json::from_str::<T>(&text).map_err(|err| err.to_string())
    }

    fn remote_agent(&self) -> Result<ureq::Agent, String> {
        let builder = ureq::builder();
        if let Some(tls_config) = self.remote_tls_config()? {
            Ok(builder.tls_config(tls_config).build())
        } else {
            Ok(builder.build())
        }
    }

    fn remote_tls_config(&self) -> Result<Option<Arc<ClientConfig>>, String> {
        if self.status.backend != KmsServiceBackend::Kes {
            return Ok(None);
        }
        let has_client_auth = self
            .remote_client_cert
            .as_ref()
            .is_some_and(|value| !value.is_empty())
            || self
                .remote_client_key
                .as_ref()
                .is_some_and(|value| !value.is_empty());
        if !has_client_auth {
            return Ok(None);
        }
        if self
            .remote_client_key_password
            .as_ref()
            .is_some_and(|value| !value.is_empty())
        {
            return Err(
                "encrypted KES client private keys are not supported in this Rust port".to_string(),
            );
        }
        let client_cert = self
            .remote_client_cert
            .as_ref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "missing KES client certificate path".to_string())?;
        let client_key = self
            .remote_client_key
            .as_ref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "missing KES client key path".to_string())?;

        ensure_rustls_provider();
        let mut roots = RootCertStore::empty();
        if let Some(cert_authority) = self
            .remote_cert_authority
            .as_ref()
            .filter(|value| !value.is_empty())
        {
            for certificate in load_certificates(cert_authority)? {
                roots.add(certificate).map_err(|err| err.to_string())?;
            }
        }

        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                load_certificates(client_cert)?,
                load_private_key(client_key)?,
            )
            .map_err(|err| err.to_string())?;
        Ok(Some(Arc::new(client_config)))
    }

    fn remote_url(
        &self,
        method: &str,
        path: &str,
        params: &[(&str, &str)],
        body: &[u8],
    ) -> Result<(String, Option<Vec<(String, String)>>, Option<String>), String> {
        let mut url = Url::parse(&self.status.endpoint).map_err(|err| err.to_string())?;
        url.set_path(path);
        {
            let mut query = url.query_pairs_mut();
            query.clear();
            for (key, value) in params {
                query.append_pair(key, value);
            }
        }
        let signed_headers = if !url.username().is_empty() {
            let access_key = url.username().to_string();
            let secret_key = url.password().unwrap_or_default().to_string();
            url.set_username("")
                .map_err(|_| "invalid endpoint username".to_string())?;
            url.set_password(None)
                .map_err(|_| "invalid endpoint password".to_string())?;
            let mut request =
                new_test_request(method, url.as_str(), body.len() as i64, Some(body))?;
            sign_request_v4_standard(
                &mut request,
                &access_key,
                &secret_key,
                GLOBAL_MINIO_DEFAULT_REGION,
                Utc::now(),
            )?;
            Some(vec![
                (
                    "authorization".to_string(),
                    request
                        .header("authorization")
                        .ok_or_else(|| "missing signed authorization header".to_string())?
                        .to_string(),
                ),
                (
                    "x-amz-date".to_string(),
                    request
                        .header("x-amz-date")
                        .ok_or_else(|| "missing x-amz-date header".to_string())?
                        .to_string(),
                ),
                (
                    "x-amz-content-sha256".to_string(),
                    request
                        .header("x-amz-content-sha256")
                        .ok_or_else(|| "missing x-amz-content-sha256 header".to_string())?
                        .to_string(),
                ),
            ])
        } else {
            None
        };
        Ok((url.to_string(), signed_headers, None))
    }
}

fn detect_backend(env: &HashMap<String, String>) -> KmsServiceBackend {
    if has_static_key(env) {
        return KmsServiceBackend::StaticKey;
    }
    if has_kms_env(env) {
        return KmsServiceBackend::MinioKms;
    }
    if has_kes_env(env) {
        return KmsServiceBackend::Kes;
    }
    KmsServiceBackend::Disabled
}

fn has_static_key(env: &HashMap<String, String>) -> bool {
    env.get(kms::ENV_KMS_SECRET_KEY)
        .is_some_and(|value| !value.is_empty())
        || env
            .get(kms::ENV_KMS_SECRET_KEY_FILE)
            .is_some_and(|value| !value.is_empty() && Path::new(value).exists())
}

fn has_kms_env(env: &HashMap<String, String>) -> bool {
    [
        kms::ENV_KMS_ENDPOINT,
        kms::ENV_KMS_ENCLAVE,
        kms::ENV_KMS_API_KEY,
        kms::ENV_KMS_DEFAULT_KEY,
    ]
    .iter()
    .any(|key| env.contains_key(*key))
}

fn has_kes_env(env: &HashMap<String, String>) -> bool {
    [
        kms::ENV_KES_ENDPOINT,
        kms::ENV_KES_DEFAULT_KEY,
        kms::ENV_KES_API_KEY,
        kms::ENV_KES_CLIENT_KEY,
        kms::ENV_KES_CLIENT_CERT,
        kms::ENV_KES_CLIENT_PASSWORD,
        kms::ENV_KES_SERVER_CA,
    ]
    .iter()
    .any(|key| env.contains_key(*key))
}

fn default_key_for_env(
    env: &HashMap<String, String>,
    backend: KmsServiceBackend,
) -> Result<String, String> {
    match backend {
        KmsServiceBackend::Disabled => Ok(String::new()),
        KmsServiceBackend::StaticKey => static_key_material(env).map(|(key_id, _)| key_id),
        KmsServiceBackend::MinioKms => Ok(env
            .get(kms::ENV_KMS_DEFAULT_KEY)
            .cloned()
            .unwrap_or_default()),
        KmsServiceBackend::Kes => Ok(env
            .get(kms::ENV_KES_DEFAULT_KEY)
            .cloned()
            .unwrap_or_default()),
    }
}

fn endpoint_for_env(env: &HashMap<String, String>, backend: KmsServiceBackend) -> String {
    match backend {
        KmsServiceBackend::MinioKms => env.get(kms::ENV_KMS_ENDPOINT).cloned().unwrap_or_default(),
        KmsServiceBackend::Kes => env.get(kms::ENV_KES_ENDPOINT).cloned().unwrap_or_default(),
        _ => String::new(),
    }
}

fn auth_mode_for_env(env: &HashMap<String, String>, backend: KmsServiceBackend) -> String {
    match backend {
        KmsServiceBackend::Disabled => "disabled".to_string(),
        KmsServiceBackend::StaticKey => "static-key".to_string(),
        KmsServiceBackend::MinioKms => {
            if env.contains_key(kms::ENV_KMS_API_KEY) {
                "api-key".to_string()
            } else {
                "unknown".to_string()
            }
        }
        KmsServiceBackend::Kes => {
            if env.contains_key(kms::ENV_KES_API_KEY) {
                "api-key".to_string()
            } else if env.contains_key(kms::ENV_KES_CLIENT_CERT)
                || env.contains_key(kms::ENV_KES_CLIENT_KEY)
            {
                "client-cert".to_string()
            } else {
                "unknown".to_string()
            }
        }
    }
}

fn remote_api_key_for_env(
    env: &HashMap<String, String>,
    backend: KmsServiceBackend,
) -> Option<String> {
    match backend {
        KmsServiceBackend::MinioKms => env.get(kms::ENV_KMS_API_KEY).cloned(),
        KmsServiceBackend::Kes => env.get(kms::ENV_KES_API_KEY).cloned(),
        _ => None,
    }
}

fn remote_cert_authority_for_env(
    env: &HashMap<String, String>,
    backend: KmsServiceBackend,
) -> Option<String> {
    match backend {
        KmsServiceBackend::Kes => env.get(kms::ENV_KES_SERVER_CA).cloned(),
        _ => None,
    }
}

fn remote_client_cert_for_env(
    env: &HashMap<String, String>,
    backend: KmsServiceBackend,
) -> Option<String> {
    match backend {
        KmsServiceBackend::Kes => env.get(kms::ENV_KES_CLIENT_CERT).cloned(),
        _ => None,
    }
}

fn remote_client_key_for_env(
    env: &HashMap<String, String>,
    backend: KmsServiceBackend,
) -> Option<String> {
    match backend {
        KmsServiceBackend::Kes => env.get(kms::ENV_KES_CLIENT_KEY).cloned(),
        _ => None,
    }
}

fn remote_client_key_password_for_env(
    env: &HashMap<String, String>,
    backend: KmsServiceBackend,
) -> Option<String> {
    match backend {
        KmsServiceBackend::Kes => env.get(kms::ENV_KES_CLIENT_PASSWORD).cloned(),
        _ => None,
    }
}

fn build_builtin_from_env(
    env: &HashMap<String, String>,
    backend: KmsServiceBackend,
) -> Result<Option<BuiltinKms>, String> {
    if backend != KmsServiceBackend::StaticKey {
        return Ok(None);
    }
    let (_, material) = static_key_material(env)?;
    kms::parse_secret_key(&material)
        .map(Some)
        .map_err(|err| err.to_string())
}

fn static_key_material(env: &HashMap<String, String>) -> Result<(String, String), String> {
    if let Some(value) = env.get(kms::ENV_KMS_SECRET_KEY) {
        let trimmed = value.trim().to_string();
        let key_id = extract_key_id(&trimmed)?;
        return Ok((key_id, trimmed));
    }
    if let Some(path) = env.get(kms::ENV_KMS_SECRET_KEY_FILE) {
        let material = fs::read_to_string(path).map_err(|err| err.to_string())?;
        let trimmed = material.trim().to_string();
        let key_id = extract_key_id(&trimmed)?;
        return Ok((key_id, trimmed));
    }
    Err("kms: static key material is unavailable".to_string())
}

fn extract_key_id(material: &str) -> Result<String, String> {
    material
        .split_once(':')
        .map(|(key_id, _)| key_id.to_string())
        .ok_or_else(|| "kms: invalid secret key format".to_string())
}

fn normalize_kms_key_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.trim_start_matches("arn:aws:kms:").to_string())
}

fn ensure_rustls_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_certificates(
    path: &str,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, String> {
    let file = File::open(path).map_err(|err| err.to_string())?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path).map_err(|err| err.to_string())?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "private key should exist".to_string())
}

fn matches_pattern(value: &str, pattern: &str) -> bool {
    if pattern.is_empty() || pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return value.contains(pattern);
    }
    let parts = pattern.split('*').collect::<Vec<_>>();
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');
    let mut cursor = 0usize;
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if index == 0 && anchored_start {
            if !value[cursor..].starts_with(part) {
                return false;
            }
            cursor += part.len();
            continue;
        }
        let Some(found) = value[cursor..].find(part) else {
            return false;
        };
        cursor += found + part.len();
    }
    if anchored_end {
        parts
            .iter()
            .rev()
            .find(|part| !part.is_empty())
            .is_none_or(|last| value.ends_with(last))
    } else {
        true
    }
}
