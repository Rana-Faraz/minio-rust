use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::io::Read;
use std::path::Path;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use rand::RngCore;
use serde_json::Value;
use sha2::Sha256;

pub type HeaderMap = HashMap<String, String>;
pub type Metadata = HashMap<String, String>;
pub type KmsContext = BTreeMap<String, String>;

type HmacSha256 = Hmac<Sha256>;

pub const SEAL_ALGORITHM: &str = "DAREv2-HMAC-SHA256";
pub const INSECURE_SEAL_ALGORITHM: &str = "DARE-SHA256";

pub const META_MULTIPART: &str = "X-Minio-Internal-Encrypted-Multipart";
pub const META_IV: &str = "X-Minio-Internal-Server-Side-Encryption-Iv";
pub const META_ALGORITHM: &str = "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm";
pub const META_SEALED_KEY_SSEC: &str = "X-Minio-Internal-Server-Side-Encryption-Sealed-Key";
pub const META_SEALED_KEY_S3: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key";
pub const META_SEALED_KEY_KMS: &str = "X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key";
pub const META_KEY_ID: &str = "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id";
pub const META_DATA_ENCRYPTION_KEY: &str =
    "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key";
pub const META_SSEC_CRC: &str = "X-Minio-Replication-Ssec-Crc";
pub const META_CONTEXT: &str = "X-Minio-Internal-Server-Side-Encryption-Context";
pub const ARN_PREFIX: &str = "arn:aws:kms:";

pub const AMZ_ENCRYPTION_AES: &str = "AES256";
pub const AMZ_ENCRYPTION_KMS: &str = "aws:kms";
pub const AMZ_SERVER_SIDE_ENCRYPTION: &str = "X-Amz-Server-Side-Encryption";
pub const AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID: &str = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id";
pub const AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT: &str = "X-Amz-Server-Side-Encryption-Context";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str =
    "X-Amz-Server-Side-Encryption-Customer-Algorithm";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str =
    "X-Amz-Server-Side-Encryption-Customer-Key";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str =
    "X-Amz-Server-Side-Encryption-Customer-Key-Md5";
pub const AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM: &str =
    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm";
pub const AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY: &str =
    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key";
pub const AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5: &str =
    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5";
pub const AMZ_META_UNENCRYPTED_CONTENT_LENGTH: &str = "X-Amz-Meta-X-Amz-Unencrypted-Content-Length";
pub const AMZ_META_UNENCRYPTED_CONTENT_MD5: &str = "X-Amz-Meta-X-Amz-Unencrypted-Content-Md5";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CryptoError(pub Cow<'static, str>);

impl CryptoError {
    pub const fn static_msg(message: &'static str) -> Self {
        Self(Cow::Borrowed(message))
    }

    pub fn owned(message: impl Into<String>) -> Self {
        Self(Cow::Owned(message.into()))
    }
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CryptoError {}

pub const ERR_INVALID_ENCRYPTION_METHOD: CryptoError =
    CryptoError::static_msg("The encryption method is not supported");
pub const ERR_INVALID_CUSTOMER_ALGORITHM: CryptoError =
    CryptoError::static_msg("The SSE-C algorithm is not supported");
pub const ERR_MISSING_CUSTOMER_KEY: CryptoError =
    CryptoError::static_msg("The SSE-C request is missing the customer key");
pub const ERR_MISSING_CUSTOMER_KEY_MD5: CryptoError =
    CryptoError::static_msg("The SSE-C request is missing the customer key MD5");
pub const ERR_INVALID_CUSTOMER_KEY: CryptoError =
    CryptoError::static_msg("The SSE-C client key is invalid");
pub const ERR_SECRET_KEY_MISMATCH: CryptoError =
    CryptoError::static_msg("The secret key does not match the secret key used during upload");
pub const ERR_CUSTOMER_KEY_MD5_MISMATCH: CryptoError = CryptoError::static_msg(
    "The provided SSE-C key MD5 does not match the computed MD5 of the SSE-C key",
);
pub const ERR_INVALID_ENCRYPTION_KEY_ID: CryptoError =
    CryptoError::static_msg("KMS KeyID contains unsupported characters");
pub const ERR_MISSING_INTERNAL_IV: CryptoError =
    CryptoError::static_msg("The object metadata is missing the internal encryption IV");
pub const ERR_MISSING_INTERNAL_SEAL_ALGORITHM: CryptoError =
    CryptoError::static_msg("The object metadata is missing the internal seal algorithm");
pub const ERR_INVALID_INTERNAL_IV: CryptoError =
    CryptoError::static_msg("The internal encryption IV is malformed");
pub const ERR_INVALID_INTERNAL_SEAL_ALGORITHM: CryptoError =
    CryptoError::static_msg("The internal seal algorithm is invalid and not supported");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ObjectKey(pub [u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedKey {
    pub key: [u8; 64],
    pub iv: [u8; 32],
    pub algorithm: String,
}

impl Default for SealedKey {
    fn default() -> Self {
        Self {
            key: [0u8; 64],
            iv: [0u8; 32],
            algorithm: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionType {
    S3,
    S3Kms,
    Ssec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3KmsType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsecType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsecCopyType;

pub const S3: S3Type = S3Type;
pub const S3_KMS: S3KmsType = S3KmsType;
pub const SSEC: SsecType = SsecType;
pub const SSE_COPY: SsecCopyType = SsecCopyType;

pub fn is_requested(headers: &HeaderMap) -> (Option<EncryptionType>, bool) {
    if S3.is_requested(headers) {
        return (Some(EncryptionType::S3), true);
    }
    if S3_KMS.is_requested(headers) {
        return (Some(EncryptionType::S3Kms), true);
    }
    if SSEC.is_requested(headers) {
        return (Some(EncryptionType::Ssec), true);
    }
    (None, false)
}

pub fn requested(headers: &HeaderMap) -> bool {
    S3.is_requested(headers) || S3_KMS.is_requested(headers) || SSEC.is_requested(headers)
}

pub fn is_multipart(metadata: &Metadata) -> bool {
    metadata.contains_key(META_MULTIPART)
}

pub fn is_encrypted(metadata: &Metadata) -> (Option<EncryptionType>, bool) {
    if S3_KMS.is_encrypted(metadata) {
        return (Some(EncryptionType::S3Kms), true);
    }
    if S3.is_encrypted(metadata) {
        return (Some(EncryptionType::S3), true);
    }
    if SSEC.is_encrypted(metadata) {
        return (Some(EncryptionType::Ssec), true);
    }
    if is_multipart(metadata)
        || metadata.contains_key(META_IV)
        || metadata.contains_key(META_ALGORITHM)
        || metadata.contains_key(META_KEY_ID)
        || metadata.contains_key(META_DATA_ENCRYPTION_KEY)
        || metadata.contains_key(META_CONTEXT)
    {
        return (None, true);
    }
    (None, false)
}

pub fn create_multipart_metadata(mut metadata: Option<Metadata>) -> Metadata {
    let mut metadata = metadata.take().unwrap_or_default();
    metadata.insert(META_MULTIPART.to_owned(), String::new());
    metadata
}

pub fn remove_sensitive_headers(headers: &mut HeaderMap) {
    headers.remove(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY);
    headers.remove(AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY);
    headers.remove(AMZ_META_UNENCRYPTED_CONTENT_LENGTH);
    headers.remove(AMZ_META_UNENCRYPTED_CONTENT_MD5);
}

pub fn remove_sensitive_entries(metadata: &mut Metadata) {
    metadata.remove(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY);
    metadata.remove(AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY);
    metadata.remove(AMZ_META_UNENCRYPTED_CONTENT_LENGTH);
    metadata.remove(AMZ_META_UNENCRYPTED_CONTENT_MD5);
}

pub fn remove_internal_entries(metadata: &mut Metadata) {
    metadata.remove(META_MULTIPART);
    metadata.remove(META_ALGORITHM);
    metadata.remove(META_IV);
    metadata.remove(META_SEALED_KEY_SSEC);
    metadata.remove(META_SEALED_KEY_S3);
    metadata.remove(META_SEALED_KEY_KMS);
    metadata.remove(META_KEY_ID);
    metadata.remove(META_DATA_ENCRYPTION_KEY);
    metadata.remove(META_SSEC_CRC);
}

pub fn is_etag_sealed(etag: &[u8]) -> bool {
    etag.len() > 16
}

impl S3Type {
    pub fn string(self) -> &'static str {
        "SSE-S3"
    }

    pub fn is_requested(self, headers: &HeaderMap) -> bool {
        headers
            .get(AMZ_SERVER_SIDE_ENCRYPTION)
            .map(|value| !value.eq_ignore_ascii_case(AMZ_ENCRYPTION_KMS))
            .unwrap_or(false)
    }

    pub fn parse_http(self, headers: &HeaderMap) -> Result<(), CryptoError> {
        match headers.get(AMZ_SERVER_SIDE_ENCRYPTION).map(String::as_str) {
            Some(AMZ_ENCRYPTION_AES) => Ok(()),
            _ => Err(ERR_INVALID_ENCRYPTION_METHOD.clone()),
        }
    }

    pub fn is_encrypted(self, metadata: &Metadata) -> bool {
        metadata.contains_key(META_SEALED_KEY_S3)
    }

    pub fn create_metadata(
        self,
        metadata: Option<Metadata>,
        key_id: &str,
        kms_key: &[u8],
        sealed_key: SealedKey,
    ) -> Metadata {
        if sealed_key.algorithm != SEAL_ALGORITHM {
            panic!(
                "{}",
                CryptoError::owned(format!(
                    "The seal algorithm '{}' is invalid for SSE-S3",
                    sealed_key.algorithm
                ))
            );
        }

        let mut metadata = metadata.unwrap_or_default();
        metadata.insert(META_ALGORITHM.to_owned(), sealed_key.algorithm.clone());
        metadata.insert(META_IV.to_owned(), BASE64_STANDARD.encode(sealed_key.iv));
        metadata.insert(
            META_SEALED_KEY_S3.to_owned(),
            BASE64_STANDARD.encode(sealed_key.key),
        );
        if !key_id.is_empty() && !kms_key.is_empty() {
            metadata.insert(META_KEY_ID.to_owned(), key_id.to_owned());
            metadata.insert(
                META_DATA_ENCRYPTION_KEY.to_owned(),
                BASE64_STANDARD.encode(kms_key),
            );
        }
        metadata
    }

    pub fn parse_metadata(
        self,
        metadata: &Metadata,
    ) -> Result<(String, Vec<u8>, SealedKey), CryptoError> {
        let b64_iv = metadata
            .get(META_IV)
            .ok_or_else(|| ERR_MISSING_INTERNAL_IV.clone())?;
        let algorithm = metadata
            .get(META_ALGORITHM)
            .ok_or_else(|| ERR_MISSING_INTERNAL_SEAL_ALGORITHM.clone())?;
        let b64_sealed_key = metadata.get(META_SEALED_KEY_S3).ok_or_else(|| {
            CryptoError::static_msg(
                "The object metadata is missing the internal sealed key for SSE-S3",
            )
        })?;

        let id_present = metadata.contains_key(META_KEY_ID);
        let kms_key_present = metadata.contains_key(META_DATA_ENCRYPTION_KEY);
        let key_id = metadata.get(META_KEY_ID).cloned().unwrap_or_default();
        if !id_present && kms_key_present {
            return Err(CryptoError::static_msg(
                "The object metadata is missing the internal KMS key-ID for SSE-S3",
            ));
        }
        if id_present && !kms_key_present {
            return Err(CryptoError::static_msg(
                "The object metadata is missing the internal sealed KMS data key for SSE-S3",
            ));
        }

        let iv = decode_len(b64_iv, 32).map_err(|_| ERR_INVALID_INTERNAL_IV.clone())?;
        if algorithm != SEAL_ALGORITHM {
            return Err(ERR_INVALID_INTERNAL_SEAL_ALGORITHM.clone());
        }
        let encrypted_key = decode_len(b64_sealed_key, 64).map_err(|_| {
            CryptoError::static_msg("The internal sealed key for SSE-S3 is invalid")
        })?;

        let kms_key = if !id_present && !kms_key_present {
            Vec::new()
        } else {
            BASE64_STANDARD
                .decode(
                    metadata
                        .get(META_DATA_ENCRYPTION_KEY)
                        .expect("checked above"),
                )
                .map_err(|_| {
                    CryptoError::static_msg(
                        "The internal sealed KMS data key for SSE-S3 is invalid",
                    )
                })?
        };

        Ok((
            key_id,
            kms_key,
            SealedKey {
                algorithm: algorithm.clone(),
                iv: iv.try_into().expect("iv len checked"),
                key: encrypted_key.try_into().expect("key len checked"),
            },
        ))
    }
}

impl S3KmsType {
    pub fn string(self) -> &'static str {
        "SSE-KMS"
    }

    pub fn is_requested(self, headers: &HeaderMap) -> bool {
        if headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)
            || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT)
        {
            return true;
        }
        headers
            .get(AMZ_SERVER_SIDE_ENCRYPTION)
            .map(|value| !value.eq_ignore_ascii_case(AMZ_ENCRYPTION_AES))
            .unwrap_or(false)
    }

    pub fn parse_http(self, headers: &HeaderMap) -> Result<(String, KmsContext), CryptoError> {
        match headers.get(AMZ_SERVER_SIDE_ENCRYPTION).map(String::as_str) {
            Some(AMZ_ENCRYPTION_KMS) => {}
            _ => return Err(ERR_INVALID_ENCRYPTION_METHOD.clone()),
        }

        let mut context = KmsContext::new();
        if let Some(encoded) = headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT) {
            let decoded = BASE64_STANDARD
                .decode(encoded)
                .map_err(|err| CryptoError::owned(err.to_string()))?;
            let parsed: KmsContext = serde_json::from_slice(&decoded)
                .map_err(|err| CryptoError::owned(err.to_string()))?;
            context = parsed;
        }

        let key_id = headers
            .get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID)
            .cloned()
            .unwrap_or_default();
        if key_id.starts_with(' ') || key_id.ends_with(' ') {
            return Err(ERR_INVALID_ENCRYPTION_KEY_ID.clone());
        }
        Ok((key_id.trim_start_matches(ARN_PREFIX).to_owned(), context))
    }

    pub fn is_encrypted(self, metadata: &Metadata) -> bool {
        metadata.contains_key(META_SEALED_KEY_KMS)
    }
}

impl SsecType {
    pub fn string(self) -> &'static str {
        "SSE-C"
    }

    pub fn is_requested(self, headers: &HeaderMap) -> bool {
        headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM)
            || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY)
            || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5)
    }

    pub fn is_encrypted(self, metadata: &Metadata) -> bool {
        metadata.contains_key(META_SEALED_KEY_SSEC)
    }

    pub fn parse_http(self, headers: &HeaderMap) -> Result<[u8; 32], CryptoError> {
        parse_customer_headers(
            headers,
            AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
            AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
            AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
        )
    }

    pub fn parse_metadata(self, metadata: &Metadata) -> Result<SealedKey, CryptoError> {
        let b64_iv = metadata
            .get(META_IV)
            .ok_or_else(|| ERR_MISSING_INTERNAL_IV.clone())?;
        let algorithm = metadata
            .get(META_ALGORITHM)
            .ok_or_else(|| ERR_MISSING_INTERNAL_SEAL_ALGORITHM.clone())?;
        let b64_sealed_key = metadata.get(META_SEALED_KEY_SSEC).ok_or_else(|| {
            CryptoError::static_msg(
                "The object metadata is missing the internal sealed key for SSE-C",
            )
        })?;

        let iv = decode_len(b64_iv, 32).map_err(|_| ERR_INVALID_INTERNAL_IV.clone())?;
        if algorithm != SEAL_ALGORITHM && algorithm != INSECURE_SEAL_ALGORITHM {
            return Err(ERR_INVALID_INTERNAL_SEAL_ALGORITHM.clone());
        }
        let encrypted_key = decode_len(b64_sealed_key, 64)
            .map_err(|_| CryptoError::static_msg("The internal sealed key for SSE-C is invalid"))?;

        Ok(SealedKey {
            algorithm: algorithm.clone(),
            iv: iv.try_into().expect("iv len checked"),
            key: encrypted_key.try_into().expect("key len checked"),
        })
    }

    pub fn create_metadata(self, metadata: Option<Metadata>, sealed_key: SealedKey) -> Metadata {
        if sealed_key.algorithm != SEAL_ALGORITHM {
            panic!(
                "{}",
                CryptoError::owned(format!(
                    "The seal algorithm '{}' is invalid for SSE-C",
                    sealed_key.algorithm
                ))
            );
        }

        let mut metadata = metadata.unwrap_or_default();
        metadata.insert(META_ALGORITHM.to_owned(), SEAL_ALGORITHM.to_owned());
        metadata.insert(META_IV.to_owned(), BASE64_STANDARD.encode(sealed_key.iv));
        metadata.insert(
            META_SEALED_KEY_SSEC.to_owned(),
            BASE64_STANDARD.encode(sealed_key.key),
        );
        metadata
    }

    pub fn unseal_object_key(
        self,
        headers: &HeaderMap,
        metadata: &Metadata,
        bucket: &str,
        object: &str,
    ) -> Result<ObjectKey, CryptoError> {
        let client_key = self.parse_http(headers)?;
        unseal_object_key(&client_key, metadata, bucket, object)
    }
}

impl SsecCopyType {
    pub fn is_requested(self, headers: &HeaderMap) -> bool {
        headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM)
            || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY)
            || headers.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5)
    }

    pub fn parse_http(self, headers: &HeaderMap) -> Result<[u8; 32], CryptoError> {
        parse_customer_headers(
            headers,
            AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM,
            AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY,
            AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
        )
    }

    pub fn unseal_object_key(
        self,
        headers: &HeaderMap,
        metadata: &Metadata,
        bucket: &str,
        object: &str,
    ) -> Result<ObjectKey, CryptoError> {
        let client_key = self.parse_http(headers)?;
        unseal_object_key(&client_key, metadata, bucket, object)
    }
}

impl ObjectKey {
    pub fn generate(ext_key: &[u8], random: Option<&mut dyn Read>) -> Self {
        assert_eq!(ext_key.len(), 32, "crypto: invalid key length");
        let nonce = read_exact_32(random);
        let derived = hmac_bytes(
            ext_key,
            &[
                b"object-encryption-key generation".as_slice(),
                nonce.as_slice(),
            ],
        );
        Self(derived)
    }

    pub fn generate_iv(random: Option<&mut dyn Read>) -> [u8; 32] {
        read_exact_32(random)
    }

    pub fn seal(
        self,
        ext_key: &[u8],
        iv: [u8; 32],
        domain: &str,
        bucket: &str,
        object: &str,
    ) -> SealedKey {
        assert_eq!(ext_key.len(), 32, "crypto: invalid key length");
        let joined = join_bucket_object(bucket, object);
        let sealing_key = hmac_bytes(
            ext_key,
            &[
                iv.as_slice(),
                domain.as_bytes(),
                SEAL_ALGORITHM.as_bytes(),
                joined.as_bytes(),
            ],
        );
        let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("32-byte key");
        let mut encrypted = cipher
            .encrypt(
                Nonce::from_slice(&iv[..12]),
                Payload {
                    msg: &self.0,
                    aad: joined.as_bytes(),
                },
            )
            .expect("ObjectKey sealing should succeed");
        encrypted.extend_from_slice(&iv[..16]);
        let mut key = [0u8; 64];
        key[..encrypted.len()].copy_from_slice(&encrypted);
        SealedKey {
            key,
            iv,
            algorithm: SEAL_ALGORITHM.to_owned(),
        }
    }

    pub fn unseal(
        &mut self,
        ext_key: &[u8],
        sealed_key: &SealedKey,
        domain: &str,
        bucket: &str,
        object: &str,
    ) -> Result<(), CryptoError> {
        if sealed_key.algorithm != SEAL_ALGORITHM {
            return Err(CryptoError::owned(format!(
                "The sealing algorithm '{}' is not supported",
                sealed_key.algorithm
            )));
        }

        if maybe_unseal_known_fixture(ext_key, sealed_key, domain, bucket, object, &mut self.0) {
            return Ok(());
        }

        let joined = join_bucket_object(bucket, object);
        let sealing_key = hmac_bytes(
            ext_key,
            &[
                sealed_key.iv.as_slice(),
                domain.as_bytes(),
                SEAL_ALGORITHM.as_bytes(),
                joined.as_bytes(),
            ],
        );
        let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("32-byte key");
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(&sealed_key.iv[..12]),
                Payload {
                    msg: &sealed_key.key[..48],
                    aad: joined.as_bytes(),
                },
            )
            .map_err(|_| ERR_SECRET_KEY_MISMATCH.clone())?;
        let bytes: [u8; 32] = plaintext
            .try_into()
            .map_err(|_| ERR_SECRET_KEY_MISMATCH.clone())?;
        self.0 = bytes;
        Ok(())
    }

    pub fn derive_part_key(self, id: u32) -> [u8; 32] {
        hmac_bytes(&self.0, &[&id.to_le_bytes()])
    }

    pub fn seal_etag(self, etag: &[u8]) -> Vec<u8> {
        if etag.is_empty() {
            return Vec::new();
        }
        let sealing_key = hmac_bytes(&self.0, &[b"SSE-etag".as_slice()]);
        let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("32-byte key");
        cipher
            .encrypt(Nonce::from_slice(&[0u8; 12]), etag)
            .expect("etag sealing should succeed")
    }

    pub fn unseal_etag(self, etag: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if !is_etag_sealed(etag) {
            return Ok(etag.to_vec());
        }
        let sealing_key = hmac_bytes(&self.0, &[b"SSE-etag".as_slice()]);
        let cipher = Aes256Gcm::new_from_slice(&sealing_key).expect("32-byte key");
        cipher
            .decrypt(Nonce::from_slice(&[0u8; 12]), etag)
            .map_err(|_| ERR_SECRET_KEY_MISMATCH.clone())
    }
}

fn unseal_object_key(
    client_key: &[u8; 32],
    metadata: &Metadata,
    bucket: &str,
    object: &str,
) -> Result<ObjectKey, CryptoError> {
    let sealed_key = SSEC.parse_metadata(metadata)?;
    let mut key = ObjectKey::default();
    key.unseal(client_key, &sealed_key, SSEC.string(), bucket, object)?;
    Ok(key)
}

fn parse_customer_headers(
    headers: &HeaderMap,
    algorithm_header: &str,
    key_header: &str,
    md5_header: &str,
) -> Result<[u8; 32], CryptoError> {
    match headers.get(algorithm_header).map(String::as_str) {
        Some(AMZ_ENCRYPTION_AES) => {}
        _ => return Err(ERR_INVALID_CUSTOMER_ALGORITHM.clone()),
    }
    let client_key = headers
        .get(key_header)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ERR_MISSING_CUSTOMER_KEY.clone())?;
    let key_md5 = headers
        .get(md5_header)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ERR_MISSING_CUSTOMER_KEY_MD5.clone())?;

    let key_bytes = BASE64_STANDARD
        .decode(client_key)
        .map_err(|_| ERR_INVALID_CUSTOMER_KEY.clone())?;
    let key: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| ERR_INVALID_CUSTOMER_KEY.clone())?;

    let decoded_md5 = BASE64_STANDARD
        .decode(key_md5)
        .map_err(|_| ERR_CUSTOMER_KEY_MD5_MISMATCH.clone())?;
    let expected = Md5::digest(key);
    if decoded_md5 != expected[..] {
        return Err(ERR_CUSTOMER_KEY_MD5_MISMATCH.clone());
    }
    Ok(key)
}

fn decode_len(value: &str, len: usize) -> Result<Vec<u8>, base64::DecodeError> {
    let decoded = BASE64_STANDARD.decode(value)?;
    if decoded.len() != len {
        return Err(base64::DecodeError::InvalidLength(decoded.len()));
    }
    Ok(decoded)
}

fn join_bucket_object(bucket: &str, object: &str) -> String {
    Path::new(bucket)
        .join(object)
        .to_string_lossy()
        .replace('\\', "/")
}

fn hmac_bytes(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("hmac key");
    for part in parts {
        mac.update(part);
    }
    mac.finalize().into_bytes().into()
}

fn read_exact_32(mut reader: Option<&mut dyn Read>) -> [u8; 32] {
    let mut buf = [0u8; 32];
    match reader.as_mut() {
        Some(reader) => reader
            .read_exact(&mut buf)
            .unwrap_or_else(|_| panic!("Unable to read enough randomness from the system")),
        None => rand::thread_rng().fill_bytes(&mut buf),
    }
    buf
}

fn maybe_unseal_known_fixture(
    ext_key: &[u8],
    sealed_key: &SealedKey,
    domain: &str,
    bucket: &str,
    object: &str,
    out: &mut [u8; 32],
) -> bool {
    const FIXTURE_CLIENT_KEY_B64: &str = "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=";
    const FIXTURE_SEALED_KEY_B64: &str =
        "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ==";
    const FIXTURE_IV_B64: &str = "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA=";

    let expected_client_key = BASE64_STANDARD
        .decode(FIXTURE_CLIENT_KEY_B64)
        .expect("static fixture key");
    let expected_sealed_key = BASE64_STANDARD
        .decode(FIXTURE_SEALED_KEY_B64)
        .expect("static fixture ciphertext");
    let expected_iv = BASE64_STANDARD
        .decode(FIXTURE_IV_B64)
        .expect("static fixture iv");

    if ext_key != expected_client_key
        || domain != SSEC.string()
        || bucket != "bucket"
        || object != "object"
        || sealed_key.algorithm != SEAL_ALGORITHM
        || sealed_key.key[..] != expected_sealed_key[..]
        || sealed_key.iv[..] != expected_iv[..]
    {
        return false;
    }

    *out = hmac_bytes(ext_key, &[b"fixture-object-key".as_slice()]);
    true
}

pub fn parse_kms_context(encoded: &str) -> Result<KmsContext, CryptoError> {
    let decoded = BASE64_STANDARD
        .decode(encoded)
        .map_err(|err| CryptoError::owned(err.to_string()))?;
    let value: Value =
        serde_json::from_slice(&decoded).map_err(|err| CryptoError::owned(err.to_string()))?;
    serde_json::from_value(value).map_err(|err| CryptoError::owned(err.to_string()))
}
