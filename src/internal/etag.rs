use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read};

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chacha20poly1305::ChaCha20Poly1305;
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use sha2::Sha256;

const DARE_VERSION_20: u8 = 0x20;
const AES_256_GCM: u8 = 0;
const CHACHA20_POLY1305: u8 = 1;
const HEADER_SIZE: usize = 16;
const TAG_SIZE: usize = 16;
const MAX_PAYLOAD_SIZE: usize = 1 << 16;
const HMAC_CONTEXT: &[u8] = b"SSE-etag";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ETag(Vec<u8>);

impl ETag {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn is_encrypted(&self) -> bool {
        self.0.len() >= 32
    }

    pub fn is_multipart(&self) -> bool {
        self.0.len() > 16 && !self.is_encrypted() && self.0.contains(&b'-')
    }

    pub fn parts(&self) -> usize {
        if !self.is_multipart() {
            return 1;
        }
        let index = self
            .0
            .iter()
            .position(|byte| *byte == b'-')
            .expect("multipart etag must contain '-'");
        std::str::from_utf8(&self.0[index + 1..])
            .expect("multipart etag suffix must be utf-8")
            .parse()
            .expect("multipart etag suffix must be numeric")
    }

    pub fn format(&self) -> ETag {
        if !self.is_encrypted() {
            return self.clone();
        }
        ETag::new(self.0[self.0.len() - 16..].to_vec())
    }
}

impl fmt::Display for ETag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_multipart() {
            write!(
                f,
                "{}{}",
                hex::encode(&self.0[..16]),
                String::from_utf8_lossy(&self.0[16..])
            )
        } else {
            f.write_str(&hex::encode(&self.0))
        }
    }
}

pub trait Tagger {
    fn etag(&self) -> ETag;
}

impl Tagger for ETag {
    fn etag(&self) -> ETag {
        self.clone()
    }
}

pub struct WrapReader<R> {
    wrapped: R,
    tag: Option<ETag>,
}

impl<R> WrapReader<R> {
    pub fn new(wrapped: R, tag: Option<ETag>) -> Self {
        Self { wrapped, tag }
    }
}

impl<R: Read> Read for WrapReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.wrapped.read(buf)
    }
}

impl<R> Tagger for WrapReader<R> {
    fn etag(&self) -> ETag {
        self.tag.clone().unwrap_or_default()
    }
}

pub fn wrap<R>(wrapped: R, content: Option<ETag>) -> WrapReader<R> {
    WrapReader::new(wrapped, content)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyError {
    pub expected: ETag,
    pub computed: ETag,
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "etag: expected ETag {:?} does not match computed ETag {:?}",
            self.expected.to_string(),
            self.computed.to_string()
        )
    }
}

impl std::error::Error for VerifyError {}

pub struct Reader<R> {
    src: R,
    md5: Md5State,
    checksum: ETag,
    read_n: i64,
}

enum Md5State {
    Md5(Md5),
    Fixed(Vec<u8>),
}

impl<R: Read> Reader<R> {
    pub fn new(src: R, etag: ETag, force_md5: Option<Vec<u8>>) -> Self {
        let md5 = match force_md5 {
            Some(bytes) if !bytes.is_empty() => Md5State::Fixed(bytes),
            _ => Md5State::Md5(Md5::new()),
        };
        Self {
            src,
            md5,
            checksum: etag,
            read_n: 0,
        }
    }

    pub fn etag(&self) -> ETag {
        match &self.md5 {
            Md5State::Md5(hasher) => ETag::new(hasher.clone().finalize().to_vec()),
            Md5State::Fixed(bytes) => ETag::new(bytes.clone()),
        }
    }
}

impl<R: Read> Read for Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.src.read(buf)?;
        self.read_n += n as i64;

        match &mut self.md5 {
            Md5State::Md5(hasher) => hasher.update(&buf[..n]),
            Md5State::Fixed(_) => {}
        }

        if n == 0 && !self.checksum.as_bytes().is_empty() {
            let etag = self.etag();
            if !equal(&etag, &self.checksum) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    VerifyError {
                        expected: self.checksum.clone(),
                        computed: etag,
                    },
                ));
            }
        }

        Ok(n)
    }
}

pub type HeaderMap = HashMap<String, Vec<String>>;

pub fn parse(s: &str) -> Result<ETag, String> {
    parse_internal(s, false)
}

pub fn equal(a: &ETag, b: &ETag) -> bool {
    a.as_bytes() == b.as_bytes()
}

pub fn from_content_md5(headers: &HeaderMap) -> Result<ETag, String> {
    let Some(values) = headers.get("Content-Md5") else {
        return Ok(ETag::default());
    };
    if values.first().is_none() || values[0].is_empty() {
        return Err("etag: content-md5 is set but contains no value".to_owned());
    }
    let bytes = BASE64_STANDARD
        .decode(&values[0])
        .map_err(|error| error.to_string())?;
    if bytes.len() != 16 {
        return Err("etag: invalid content-md5".to_owned());
    }
    Ok(ETag::new(bytes))
}

pub fn multipart(etags: &[ETag]) -> ETag {
    if etags.is_empty() {
        return ETag::default();
    }

    let mut hasher = Md5::new();
    let mut count = 0_i64;
    for etag in etags {
        if !etag.is_multipart() && !etag.is_encrypted() {
            hasher.update(etag.as_bytes());
            count += 1;
        }
    }

    let mut bytes = hasher.finalize().to_vec();
    bytes.push(b'-');
    bytes.extend_from_slice(count.to_string().as_bytes());
    ETag::new(bytes)
}

pub fn decrypt(key: &[u8], etag: &ETag) -> Result<ETag, String> {
    if !etag.is_encrypted() {
        return Ok(etag.clone());
    }

    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).map_err(|error| error.to_string())?;
    mac.update(HMAC_CONTEXT);
    let decryption_key = mac.finalize().into_bytes();
    let bytes = decrypt_dare_v20(etag.as_bytes(), &decryption_key)?;
    Ok(ETag::new(bytes))
}

fn parse_internal(mut s: &str, strict: bool) -> Result<ETag, String> {
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        s = &s[1..s.len() - 1];
    }

    let dash_index = s.find('-');
    match dash_index {
        None => {
            let bytes = hex::decode(s).map_err(|error| error.to_string())?;
            if strict && bytes.len() != 16 {
                return Err(format!("etag: invalid length {}", bytes.len()));
            }
            Ok(ETag::new(bytes))
        }
        Some(index) => {
            let prefix = &s[..index];
            let suffix = &s[index..];
            if prefix.len() != 32 {
                return Err(format!("etag: invalid prefix length {}", prefix.len()));
            }
            if suffix.len() <= 1 {
                return Err("etag: suffix is not a part number".to_owned());
            }
            let mut bytes = hex::decode(prefix).map_err(|error| error.to_string())?;
            let part_number: usize = suffix[1..]
                .parse()
                .map_err(|error: std::num::ParseIntError| error.to_string())?;
            if strict && (part_number == 0 || part_number > 10000) {
                return Err(format!("etag: invalid part number {}", part_number));
            }
            bytes.extend_from_slice(suffix.as_bytes());
            Ok(ETag::new(bytes))
        }
    }
}

fn decrypt_dare_v20(src: &[u8], key: &[u8]) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    let mut remaining = src;
    let mut seq_num = 0_u32;
    let mut ref_header: Option<[u8; HEADER_SIZE]> = None;
    let mut finalized = false;

    while !remaining.is_empty() {
        if finalized {
            return Err("sio: unexpected data after final package".to_owned());
        }
        if remaining.len() <= HEADER_SIZE + TAG_SIZE {
            return Err("sio: invalid payload size".to_owned());
        }

        let header = &remaining[..HEADER_SIZE];
        if header[0] != DARE_VERSION_20 {
            return Err("sio: unsupported version".to_owned());
        }

        let cipher_id = header[1];
        let length = u16::from_le_bytes([header[2], header[3]]) as usize + 1;
        let package_len = HEADER_SIZE + length + TAG_SIZE;
        if remaining.len() < package_len {
            return Err("sio: invalid payload size".to_owned());
        }
        if !is_final(header) && length != MAX_PAYLOAD_SIZE {
            return Err("sio: invalid payload size".to_owned());
        }

        let package = &remaining[..package_len];
        let stored_ref = ref_header.get_or_insert_with(|| {
            let mut bytes = [0_u8; HEADER_SIZE];
            bytes.copy_from_slice(header);
            bytes
        });

        if stored_ref[1] != cipher_id {
            return Err("sio: unsupported cipher suite".to_owned());
        }

        let mut expected_nonce = stored_ref[4..HEADER_SIZE].to_vec();
        if is_final(header) {
            finalized = true;
            expected_nonce[0] |= 0x80;
        }
        if header[4..HEADER_SIZE] != expected_nonce[..] {
            return Err("sio: header nonce mismatch".to_owned());
        }

        let mut nonce = [0_u8; 12];
        nonce.copy_from_slice(&header[4..HEADER_SIZE]);
        let tail = u32::from_le_bytes(nonce[8..12].try_into().expect("nonce tail is 4 bytes"));
        nonce[8..12].copy_from_slice(&(tail ^ seq_num).to_le_bytes());

        let plaintext = match cipher_id {
            AES_256_GCM => {
                let cipher = Aes256Gcm::new_from_slice(key).map_err(|error| error.to_string())?;
                cipher
                    .decrypt(
                        (&nonce).into(),
                        Payload {
                            msg: &package[HEADER_SIZE..],
                            aad: &header[..4],
                        },
                    )
                    .map_err(|_| "sio: authentication failed".to_owned())?
            }
            CHACHA20_POLY1305 => {
                let cipher =
                    ChaCha20Poly1305::new_from_slice(key).map_err(|error| error.to_string())?;
                cipher
                    .decrypt(
                        (&nonce).into(),
                        Payload {
                            msg: &package[HEADER_SIZE..],
                            aad: &header[..4],
                        },
                    )
                    .map_err(|_| "sio: authentication failed".to_owned())?
            }
            _ => return Err("sio: unsupported cipher suite".to_owned()),
        };

        output.extend_from_slice(&plaintext);
        remaining = &remaining[package_len..];
        seq_num += 1;
    }

    if !finalized {
        return Err("sio: unexpected EOF".to_owned());
    }

    Ok(output)
}

fn is_final(header: &[u8]) -> bool {
    header[4] & 0x80 == 0x80
}
