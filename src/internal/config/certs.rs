use std::env;
use std::fmt;
use std::io::Cursor;
use std::path::Path;
use std::process::Command;

use rustls_pemfile::{read_one, Item};
use x509_parser::prelude::parse_x509_certificate;

pub const ENV_CERT_PASSWORD: &str = "MINIO_CERT_PASSWD";

#[derive(Debug)]
pub struct ParsedCertificate(pub Vec<u8>);

#[derive(Debug)]
pub struct LoadedKeyPair {
    pub certificates: Vec<ParsedCertificate>,
    pub private_key_pem: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertError(String);

impl CertError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for CertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CertError {}

pub fn parse_public_cert_file(
    cert_file: impl AsRef<Path>,
) -> Result<Vec<ParsedCertificate>, CertError> {
    let cert_file = cert_file.as_ref();
    let data = std::fs::read(cert_file).map_err(|err| CertError::new(err.to_string()))?;
    let trimmed = trim_ascii_whitespace(&data);

    if trimmed.is_empty() {
        return Err(CertError::new(format!(
            "Empty public certificate file {}",
            cert_file.display()
        )));
    }

    let mut reader = Cursor::new(trimmed);
    let mut certs = Vec::new();

    loop {
        match read_one(&mut reader).map_err(|err| CertError::new(err.to_string()))? {
            Some(Item::X509Certificate(cert)) => {
                parse_x509_certificate(cert.as_ref()).map_err(|err| {
                    CertError::new(format!(
                        "Failed to parse `{}`: {}",
                        cert_file.display(),
                        err
                    ))
                })?;
                certs.push(ParsedCertificate(cert.as_ref().to_vec()));
            }
            Some(_) => {
                return Err(CertError::new(format!(
                    "Could not read PEM block from file {}",
                    cert_file.display()
                )));
            }
            None => break,
        }
    }

    if certs.is_empty() {
        return Err(CertError::new(format!(
            "Could not read PEM block from file {}",
            cert_file.display()
        )));
    }

    Ok(certs)
}

pub fn load_x509_key_pair(
    cert_file: impl AsRef<Path>,
    key_file: impl AsRef<Path>,
) -> Result<LoadedKeyPair, CertError> {
    let cert_file = cert_file.as_ref();
    let key_file = key_file.as_ref();

    let certs = parse_public_cert_file(cert_file)?;
    let key_pem = prepare_private_key_pem(key_file)?;

    validate_private_key(&key_pem)?;
    validate_matching_public_key(cert_file, key_file)?;

    Ok(LoadedKeyPair {
        certificates: certs,
        private_key_pem: key_pem,
    })
}

fn prepare_private_key_pem(key_file: &Path) -> Result<Vec<u8>, CertError> {
    let key_pem = std::fs::read(key_file)
        .map_err(|err| CertError::new(format!("Unable to read the private key: {err}")))?;

    if key_pem
        .windows("ENCRYPTED".len())
        .any(|window| window == b"ENCRYPTED")
    {
        let password = env::var(ENV_CERT_PASSWORD).map_err(|_| {
            CertError::new(
                "TLS private key is password protected, but MINIO_CERT_PASSWD is not set",
            )
        })?;
        let output = Command::new("openssl")
            .arg("rsa")
            .arg("-in")
            .arg(key_file)
            .arg("-passin")
            .arg("env:MINIO_CERT_PASSWD")
            .env(ENV_CERT_PASSWORD, password)
            .output()
            .map_err(|err| CertError::new(format!("failed to execute openssl: {err}")))?;
        if !output.status.success() {
            return Err(CertError::new(
                String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            ));
        }
        return Ok(output.stdout);
    }

    Ok(key_pem)
}

fn validate_private_key(key_pem: &[u8]) -> Result<(), CertError> {
    let mut reader = Cursor::new(key_pem);
    match read_one(&mut reader).map_err(|err| CertError::new(err.to_string()))? {
        Some(Item::Pkcs1Key(_)) | Some(Item::Pkcs8Key(_)) | Some(Item::Sec1Key(_)) => Ok(()),
        Some(_) => Err(CertError::new("The private key is not readable")),
        None => Err(CertError::new("The private key is not readable")),
    }
}

fn validate_matching_public_key(cert_file: &Path, key_file: &Path) -> Result<(), CertError> {
    let cert_pubkey = Command::new("openssl")
        .arg("x509")
        .arg("-in")
        .arg(cert_file)
        .arg("-noout")
        .arg("-pubkey")
        .output()
        .map_err(|err| CertError::new(format!("failed to execute openssl: {err}")))?;
    if !cert_pubkey.status.success() {
        return Err(CertError::new(
            String::from_utf8_lossy(&cert_pubkey.stderr)
                .trim()
                .to_owned(),
        ));
    }

    let mut key_command = Command::new("openssl");
    key_command
        .arg("pkey")
        .arg("-in")
        .arg(key_file)
        .arg("-pubout");
    if let Ok(password) = env::var(ENV_CERT_PASSWORD) {
        key_command.arg("-passin").arg("env:MINIO_CERT_PASSWD");
        key_command.env(ENV_CERT_PASSWORD, password);
    }
    let key_pubkey = key_command
        .output()
        .map_err(|err| CertError::new(format!("failed to execute openssl: {err}")))?;
    if !key_pubkey.status.success() {
        return Err(CertError::new(
            String::from_utf8_lossy(&key_pubkey.stderr)
                .trim()
                .to_owned(),
        ));
    }

    if cert_pubkey.stdout != key_pubkey.stdout {
        return Err(CertError::new("certificate and private key do not match"));
    }

    Ok(())
}

fn trim_ascii_whitespace(bytes: &[u8]) -> Vec<u8> {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)
        .unwrap_or(start);
    bytes[start..end].to_vec()
}
