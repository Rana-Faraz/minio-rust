use std::fmt;
use std::io::{Cursor, Read};

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::internal::kms::{BuiltinKms, Context, DecryptRequest, GenerateKeyRequest};

const MAX_METADATA_SIZE: usize = 1 << 20;
const VERSION: u8 = 1;

#[derive(Debug)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncryptedObject {
    key_id: String,
    kms_key: Vec<u8>,
    nonce: Vec<u8>,
}

pub fn encrypt_bytes(
    kms: &BuiltinKms,
    plaintext: &[u8],
    context: Context,
) -> Result<Vec<u8>, Error> {
    let encrypted = encrypt(kms, Cursor::new(plaintext), context)?;
    read_all(encrypted)
}

pub fn decrypt_bytes(
    kms: &BuiltinKms,
    ciphertext: &[u8],
    context: Context,
) -> Result<Vec<u8>, Error> {
    let decrypted = decrypt(kms, Cursor::new(ciphertext), context)?;
    read_all(decrypted)
}

pub fn encrypt(
    kms: &BuiltinKms,
    mut plaintext: impl Read,
    context: Context,
) -> Result<Cursor<Vec<u8>>, Error> {
    let request = GenerateKeyRequest {
        name: "my-key".to_owned(),
        associated_data: context,
    };
    let dek = kms
        .generate_key(&request)
        .map_err(|err| Error::new(err.to_string()))?;
    let plaintext_key = dek
        .plaintext
        .clone()
        .ok_or_else(|| Error::new("kms returned no plaintext key"))?;

    let mut source = Vec::new();
    plaintext
        .read_to_end(&mut source)
        .map_err(|err| Error::new(err.to_string()))?;

    let cipher = Aes256Gcm::new_from_slice(&plaintext_key)
        .map_err(|_| Error::new("failed to initialize cipher"))?;
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);

    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &source,
                aad: b"",
            },
        )
        .map_err(|_| Error::new("failed to encrypt plaintext"))?;

    let metadata = serde_json::to_vec(&EncryptedObject {
        key_id: dek.key_id,
        kms_key: dek.ciphertext,
        nonce: nonce.to_vec(),
    })
    .map_err(|err| Error::new(err.to_string()))?;

    if metadata.len() > MAX_METADATA_SIZE {
        return Err(Error::new("config: encryption metadata is too large"));
    }

    let mut output = Vec::with_capacity(1 + 4 + metadata.len() + ciphertext.len());
    output.push(VERSION);
    output.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    output.extend_from_slice(&metadata);
    output.extend_from_slice(&ciphertext);
    Ok(Cursor::new(output))
}

pub fn decrypt(
    kms: &BuiltinKms,
    mut ciphertext: impl Read,
    context: Context,
) -> Result<Cursor<Vec<u8>>, Error> {
    let mut header = [0u8; 5];
    ciphertext
        .read_exact(&mut header)
        .map_err(|err| Error::new(err.to_string()))?;

    if header[0] != VERSION {
        return Err(Error::new(format!(
            "config: unknown ciphertext version {}",
            header[0]
        )));
    }

    let metadata_size = u32::from_le_bytes(header[1..5].try_into().expect("header slice")) as usize;
    if metadata_size > MAX_METADATA_SIZE {
        return Err(Error::new("config: encryption metadata is too large"));
    }

    let mut metadata_bytes = vec![0u8; metadata_size];
    ciphertext
        .read_exact(&mut metadata_bytes)
        .map_err(|err| Error::new(err.to_string()))?;
    let metadata: EncryptedObject =
        serde_json::from_slice(&metadata_bytes).map_err(|err| Error::new(err.to_string()))?;

    let plaintext_key = kms
        .decrypt(&DecryptRequest {
            name: metadata.key_id,
            version: 0,
            ciphertext: metadata.kms_key,
            associated_data: context,
        })
        .map_err(|err| Error::new(err.to_string()))?;

    if metadata.nonce.len() != 12 {
        return Err(Error::new("config: invalid nonce"));
    }

    let mut sealed = Vec::new();
    ciphertext
        .read_to_end(&mut sealed)
        .map_err(|err| Error::new(err.to_string()))?;

    let cipher = Aes256Gcm::new_from_slice(&plaintext_key)
        .map_err(|_| Error::new("failed to initialize cipher"))?;
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&metadata.nonce),
            Payload {
                msg: &sealed,
                aad: b"",
            },
        )
        .map_err(|_| Error::new("failed to decrypt ciphertext"))?;

    Ok(Cursor::new(plaintext))
}

fn read_all(mut reader: impl Read) -> Result<Vec<u8>, Error> {
    let mut data = Vec::new();
    reader
        .read_to_end(&mut data)
        .map_err(|err| Error::new(err.to_string()))?;
    Ok(data)
}
