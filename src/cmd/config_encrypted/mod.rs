use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use sha2::{Digest, Sha256};
use std::io::Read;

use crate::cmd::Credentials;

const MAGIC: &[u8] = b"minio-rust-config-v1";

fn derive_key(seed: &str) -> [u8; 32] {
    let digest = Sha256::digest(seed.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

fn derive_nonce(seed: &str) -> [u8; 12] {
    let digest = Sha256::digest(format!("nonce:{seed}").as_bytes());
    let mut out = [0u8; 12];
    out.copy_from_slice(&digest[..12]);
    out
}

pub fn encrypt_data(cred: &Credentials, data: &[u8]) -> Result<Vec<u8>, String> {
    let material = format!("{}:{}", cred.access_key, cred.secret_key);
    let key = derive_key(&material);
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|err| err.to_string())?;
    let nonce_bytes = derive_nonce(&material);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|_| "encryption failed".to_string())?;

    let mut out = Vec::from(MAGIC);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

pub fn decrypt_data(cred: &Credentials, mut reader: impl Read) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    if bytes.len() <= MAGIC.len() + 12 || !bytes.starts_with(MAGIC) {
        return Err("invalid encrypted payload".to_string());
    }

    let material = format!("{}:{}", cred.access_key, cred.secret_key);
    let key = derive_key(&material);
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|err| err.to_string())?;
    let nonce = Nonce::from_slice(&bytes[MAGIC.len()..MAGIC.len() + 12]);
    cipher
        .decrypt(nonce, &bytes[MAGIC.len() + 12..])
        .map_err(|_| "decryption failed".to_string())
}
