use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read};
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use crc::{Algorithm, Crc, CRC_32_ISCSI, CRC_32_ISO_HDLC};
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::Sha256;

use crate::internal::ioutil;

pub const AMZ_CHECKSUM_CRC32: &str = "x-amz-checksum-crc32";
pub const AMZ_CHECKSUM_CRC32C: &str = "x-amz-checksum-crc32c";
pub const AMZ_CHECKSUM_SHA1: &str = "x-amz-checksum-sha1";
pub const AMZ_CHECKSUM_SHA256: &str = "x-amz-checksum-sha256";
pub const AMZ_CHECKSUM_CRC64NVME: &str = "x-amz-checksum-crc64nvme";
pub const AMZ_CHECKSUM_ALGO: &str = "x-amz-checksum-algorithm";
pub const AMZ_CHECKSUM_TYPE: &str = "x-amz-checksum-type";
pub const AMZ_CHECKSUM_TYPE_FULL_OBJECT: &str = "FULL_OBJECT";
pub const AMZ_CHECKSUM_TYPE_COMPOSITE: &str = "COMPOSITE";

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);
const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const CRC64_NVME_ALGORITHM: Algorithm<u64> = Algorithm {
    width: 64,
    poly: 0xad93d23594c93659,
    init: 0xffff_ffff_ffff_ffff,
    refin: true,
    refout: true,
    xorout: 0xffff_ffff_ffff_ffff,
    check: 0xae8b_1486_0a79_9888,
    residue: 0xf310_303b_2b6f_6e42,
};
const CRC64NVME: Crc<u64> = Crc::<u64>::new(&CRC64_NVME_ALGORITHM);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ChecksumType(u32);

impl ChecksumType {
    pub const TRAILING: Self = Self(1 << 0);
    pub const SHA256: Self = Self(1 << 1);
    pub const SHA1: Self = Self(1 << 2);
    pub const CRC32: Self = Self(1 << 3);
    pub const CRC32C: Self = Self(1 << 4);
    pub const INVALID: Self = Self(1 << 5);
    pub const MULTIPART: Self = Self(1 << 6);
    pub const INCLUDES_MULTIPART: Self = Self(1 << 7);
    pub const CRC64NVME: Self = Self(1 << 8);
    pub const FULL_OBJECT: Self = Self(1 << 9);
    pub const NONE: Self = Self(0);

    const BASE_MASK: Self =
        Self(Self::SHA256.0 | Self::SHA1.0 | Self::CRC32.0 | Self::CRC32C.0 | Self::CRC64NVME.0);

    pub fn is(self, other: Self) -> bool {
        if other == Self::NONE {
            return self == Self::NONE;
        }
        self & other == other
    }

    pub fn base(self) -> Self {
        self & Self::BASE_MASK
    }

    pub fn key(self) -> &'static str {
        match self.base() {
            Self::CRC32 => AMZ_CHECKSUM_CRC32,
            Self::CRC32C => AMZ_CHECKSUM_CRC32C,
            Self::SHA1 => AMZ_CHECKSUM_SHA1,
            Self::SHA256 => AMZ_CHECKSUM_SHA256,
            Self::CRC64NVME => AMZ_CHECKSUM_CRC64NVME,
            _ => "",
        }
    }

    pub fn raw_byte_len(self) -> usize {
        match self.base() {
            Self::CRC32 | Self::CRC32C => 4,
            Self::SHA1 => 20,
            Self::SHA256 => 32,
            Self::CRC64NVME => 8,
            _ => 0,
        }
    }

    pub fn is_set(self) -> bool {
        !self.is(Self::INVALID) && !self.base().is(Self::NONE)
    }

    pub fn trailing(self) -> bool {
        self.is(Self::TRAILING)
    }

    pub fn full_object_requested(self) -> bool {
        self.is(Self::FULL_OBJECT) || self.base().is(Self::CRC64NVME)
    }

    pub fn is_multipart_composite(self) -> bool {
        self.is(Self::MULTIPART) && !self.full_object_requested()
    }

    pub fn obj_type(self) -> &'static str {
        if self.full_object_requested() {
            return AMZ_CHECKSUM_TYPE_FULL_OBJECT;
        }
        if self.is_multipart_composite() {
            return AMZ_CHECKSUM_TYPE_COMPOSITE;
        }
        if !self.is(Self::MULTIPART) && self.is_set() {
            return AMZ_CHECKSUM_TYPE_FULL_OBJECT;
        }
        ""
    }

    pub fn can_merge(self) -> bool {
        self.base().is(Self::CRC64NVME)
            || self.base().is(Self::CRC32)
            || self.base().is(Self::CRC32C)
    }

    pub fn string(self) -> &'static str {
        match self.base() {
            Self::CRC32 => "CRC32",
            Self::CRC32C => "CRC32C",
            Self::SHA1 => "SHA1",
            Self::SHA256 => "SHA256",
            Self::CRC64NVME => "CRC64NVME",
            Self::NONE => "",
            _ => "invalid",
        }
    }

    pub fn string_full(self) -> String {
        let mut out = vec![self.string().to_owned()];
        if self.is(Self::MULTIPART) {
            out.push("MULTIPART".to_owned());
        }
        if self.is(Self::INCLUDES_MULTIPART) {
            out.push("INCLUDESMP".to_owned());
        }
        if self.is(Self::TRAILING) {
            out.push("TRAILING".to_owned());
        }
        if self.is(Self::FULL_OBJECT) {
            out.push("FULLOBJ".to_owned());
        }
        out.join("|")
    }

    pub fn compute_raw(self, data: &[u8]) -> Option<Vec<u8>> {
        match self.base() {
            Self::CRC32 => Some(CRC32.checksum(data).to_be_bytes().to_vec()),
            Self::CRC32C => Some(CRC32C.checksum(data).to_be_bytes().to_vec()),
            Self::SHA1 => Some(sha1_digest(data)),
            Self::SHA256 => Some(sha256_digest(data)),
            Self::CRC64NVME => Some(CRC64NVME.checksum(data).to_be_bytes().to_vec()),
            _ => None,
        }
    }
}

impl BitOr for ChecksumType {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for ChecksumType {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

impl BitAnd for ChecksumType {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0)
    }
}

impl BitAndAssign for ChecksumType {
    fn bitand_assign(&mut self, rhs: Self) {
        self.0 &= rhs.0;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadDigest {
    pub expected_md5: String,
    pub calculated_md5: String,
}

impl fmt::Display for BadDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bad digest: Expected {} does not match calculated {}",
            self.expected_md5, self.calculated_md5
        )
    }
}

impl std::error::Error for BadDigest {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SHA256Mismatch {
    pub expected_sha256: String,
    pub calculated_sha256: String,
}

impl fmt::Display for SHA256Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bad sha256: Expected {} does not match calculated {}",
            self.expected_sha256, self.calculated_sha256
        )
    }
}

impl std::error::Error for SHA256Mismatch {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeMismatch {
    pub want: i64,
    pub got: i64,
}

impl fmt::Display for SizeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Size mismatch: got {}, want {}", self.got, self.want)
    }
}

impl std::error::Error for SizeMismatch {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumMismatch {
    pub want: String,
    pub got: String,
}

impl fmt::Display for ChecksumMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bad checksum: Want {} does not match calculated {}",
            self.want, self.got
        )
    }
}

impl std::error::Error for ChecksumMismatch {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HashError {
    BadDigest(BadDigest),
    SHA256Mismatch(SHA256Mismatch),
    SizeMismatch(SizeMismatch),
}

impl fmt::Display for HashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadDigest(error) => error.fmt(f),
            Self::SHA256Mismatch(error) => error.fmt(f),
            Self::SizeMismatch(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for HashError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    pub type_: ChecksumType,
    pub encoded: String,
    pub raw: Vec<u8>,
    pub want_parts: usize,
}

impl Checksum {
    pub fn valid(&self) -> bool {
        if self.type_ == ChecksumType::INVALID {
            return false;
        }
        if self.encoded.is_empty() || self.type_.trailing() {
            return self.type_.is(ChecksumType::NONE) || self.type_.trailing();
        }
        self.type_.raw_byte_len() == self.raw.len()
    }

    pub fn matches(&self, content: &[u8], parts: usize) -> Result<(), ChecksumMismatch> {
        if self.encoded.is_empty() {
            return Ok(());
        }
        let sum = self
            .type_
            .compute_raw(content)
            .expect("checksum type should be computable");
        if self.want_parts > 0 && self.want_parts != parts {
            return Err(ChecksumMismatch {
                want: format!("{}-{}", self.encoded, self.want_parts),
                got: format!("{}-{}", BASE64_STANDARD.encode(&sum), parts),
            });
        }
        if sum != self.raw {
            return Err(ChecksumMismatch {
                want: self.encoded.clone(),
                got: BASE64_STANDARD.encode(&sum),
            });
        }
        Ok(())
    }

    pub fn as_map(&self) -> HashMap<String, String> {
        if !self.valid() {
            return HashMap::new();
        }
        HashMap::from([
            (self.type_.string().to_owned(), self.encoded.clone()),
            (
                AMZ_CHECKSUM_TYPE.to_owned(),
                self.type_.obj_type().to_owned(),
            ),
        ])
    }

    pub fn equal(&self, other: &Self) -> bool {
        self == other
    }

    pub fn append_to(&self, mut out: Vec<u8>, mut parts: &[u8]) -> Vec<u8> {
        if !self.valid() {
            return out;
        }

        let mut typ = self.type_;
        if typ.trailing() {
            typ = ChecksumType(typ.0 ^ ChecksumType::TRAILING.0);
        }

        if self.raw.len() != typ.raw_byte_len() {
            return out;
        }

        encode_uvarint(typ.0 as u64, &mut out);
        out.extend_from_slice(&self.raw);

        if typ.is(ChecksumType::MULTIPART) {
            let mut checksums = 0usize;
            if self.want_parts > 0 && !typ.is(ChecksumType::INCLUDES_MULTIPART) {
                checksums = self.want_parts;
            }

            if typ.raw_byte_len() == 0 || !parts.len().is_multiple_of(typ.raw_byte_len()) {
                checksums = 0;
                parts = &[];
            } else if !parts.is_empty() {
                checksums = parts.len() / typ.raw_byte_len();
            }

            if !typ.is(ChecksumType::INCLUDES_MULTIPART) {
                parts = &[];
            }

            encode_uvarint(checksums as u64, &mut out);
            out.extend_from_slice(parts);
        }

        out
    }
}

pub fn checksum_string_to_type(alg: &str) -> ChecksumType {
    match alg.to_ascii_uppercase().as_str() {
        "CRC32" => ChecksumType::CRC32,
        "CRC32C" => ChecksumType::CRC32C,
        "SHA1" => ChecksumType::SHA1,
        "SHA256" => ChecksumType::SHA256,
        "CRC64NVME" => ChecksumType::CRC64NVME,
        "" => ChecksumType::NONE,
        _ => ChecksumType::INVALID,
    }
}

pub fn new_checksum_type(alg: &str, obj_type: &str) -> ChecksumType {
    let full = match obj_type {
        "" | AMZ_CHECKSUM_TYPE_COMPOSITE => ChecksumType::NONE,
        AMZ_CHECKSUM_TYPE_FULL_OBJECT => ChecksumType::FULL_OBJECT,
        _ => return ChecksumType::INVALID,
    };

    match alg.to_ascii_uppercase().as_str() {
        "CRC32" => ChecksumType::CRC32 | full,
        "CRC32C" => ChecksumType::CRC32C | full,
        "SHA1" => {
            if full != ChecksumType::NONE {
                ChecksumType::INVALID
            } else {
                ChecksumType::SHA1
            }
        }
        "SHA256" => {
            if full != ChecksumType::NONE {
                ChecksumType::INVALID
            } else {
                ChecksumType::SHA256
            }
        }
        "CRC64NVME" => ChecksumType::CRC64NVME,
        "" => {
            if full != ChecksumType::NONE {
                ChecksumType::INVALID
            } else {
                ChecksumType::NONE
            }
        }
        _ => ChecksumType::INVALID,
    }
}

pub fn new_checksum_with_type(alg: ChecksumType, value: &str) -> Option<Checksum> {
    if !alg.is_set() {
        return None;
    }

    let mut want_parts = 0usize;
    let mut encoded = value.to_owned();
    let mut typ = alg;
    if let Some((head, tail)) = value.rsplit_once('-') {
        encoded = head.to_owned();
        want_parts = tail.parse().ok()?;
        typ |= ChecksumType::MULTIPART;
    }

    let raw = if encoded.is_empty() {
        Vec::new()
    } else {
        BASE64_STANDARD.decode(encoded.as_bytes()).ok()?
    };

    let checksum = Checksum {
        type_: typ,
        encoded,
        raw,
        want_parts,
    };
    checksum.valid().then_some(checksum)
}

pub fn new_checksum_from_data(typ: ChecksumType, data: &[u8]) -> Option<Checksum> {
    if !typ.is_set() {
        return None;
    }
    let raw = typ.compute_raw(data)?;
    let checksum = Checksum {
        type_: typ,
        encoded: BASE64_STANDARD.encode(&raw),
        raw,
        want_parts: 0,
    };
    checksum.valid().then_some(checksum)
}

pub fn checksum_from_bytes(mut bytes: &[u8]) -> Option<Checksum> {
    if bytes.is_empty() {
        return None;
    }
    let typ_raw = decode_uvarint(&mut bytes)? as u32;
    let typ = ChecksumType(typ_raw);
    let length = typ.raw_byte_len();
    if length == 0 || bytes.len() < length {
        return None;
    }
    let raw = bytes[..length].to_vec();
    bytes = &bytes[length..];

    let mut checksum = Checksum {
        type_: typ,
        encoded: BASE64_STANDARD.encode(&raw),
        raw,
        want_parts: 0,
    };

    if typ.is(ChecksumType::MULTIPART) {
        let parts = decode_uvarint(&mut bytes)? as usize;
        checksum.want_parts = parts;
        if typ.is(ChecksumType::INCLUDES_MULTIPART) {
            let need = parts.checked_mul(length)?;
            if bytes.len() < need {
                return None;
            }
        }
    }

    checksum.valid().then_some(checksum)
}

pub fn read_part_checksums(mut bytes: &[u8]) -> Vec<HashMap<String, String>> {
    let mut result = Vec::new();

    while !bytes.is_empty() {
        let Some(raw_type) = decode_uvarint(&mut bytes) else {
            break;
        };
        let typ = ChecksumType(raw_type as u32);
        let length = typ.raw_byte_len();
        if length == 0 || bytes.len() < length {
            break;
        }
        bytes = &bytes[length..];
        let Some(parts) = decode_uvarint(&mut bytes).map(|v| v as usize) else {
            break;
        };
        if !typ.is(ChecksumType::INCLUDES_MULTIPART) {
            continue;
        }
        if result.is_empty() {
            result = vec![HashMap::new(); parts];
        }
        for slot in result.iter_mut().take(parts) {
            if bytes.len() < length {
                return result;
            }
            let encoded = BASE64_STANDARD.encode(&bytes[..length]);
            slot.insert(typ.string().to_owned(), encoded);
            bytes = &bytes[length..];
        }
    }

    result
}

pub fn add_checksum_header(
    headers: &mut HashMap<String, String>,
    checksums: &HashMap<String, String>,
) {
    for (key, value) in checksums {
        if key == AMZ_CHECKSUM_TYPE {
            headers.insert(AMZ_CHECKSUM_TYPE.to_owned(), value.clone());
            continue;
        }
        let typ = checksum_string_to_type(key);
        if let Some(checksum) = new_checksum_with_type(typ, value) {
            if checksum.valid() {
                headers.insert(checksum.type_.key().to_owned(), value.clone());
            }
        }
    }
}

pub fn get_content_checksum(
    headers: &HashMap<String, String>,
) -> Result<Option<Checksum>, &'static str> {
    if let Some(alg) = headers.get(AMZ_CHECKSUM_ALGO) {
        let mut typ = new_checksum_type(
            alg,
            headers
                .get(AMZ_CHECKSUM_TYPE)
                .map(String::as_str)
                .unwrap_or(""),
        );
        if headers.get(AMZ_CHECKSUM_TYPE).map(String::as_str) == Some(AMZ_CHECKSUM_TYPE_FULL_OBJECT)
        {
            if !typ.can_merge() {
                return Err("invalid checksum");
            }
            typ |= ChecksumType::FULL_OBJECT;
        }
        if !typ.is_set() {
            return Err("invalid checksum");
        }
        let Some(value) = headers.get(typ.key()) else {
            return Ok(None);
        };
        return new_checksum_with_type(typ, value)
            .map(Some)
            .ok_or("invalid checksum");
    }

    let mut found: Option<(ChecksumType, String)> = None;
    for typ in [
        ChecksumType::SHA256,
        ChecksumType::SHA1,
        ChecksumType::CRC32,
        ChecksumType::CRC64NVME,
        ChecksumType::CRC32C,
    ] {
        if let Some(value) = headers.get(typ.key()) {
            if found.is_some() {
                return Err("invalid checksum");
            }
            let mut effective = typ;
            if headers.get(AMZ_CHECKSUM_TYPE).map(String::as_str)
                == Some(AMZ_CHECKSUM_TYPE_FULL_OBJECT)
            {
                if !typ.can_merge() {
                    return Err("invalid checksum");
                }
                effective |= ChecksumType::FULL_OBJECT;
            }
            found = Some((effective, value.clone()));
        }
    }

    match found {
        None => Ok(None),
        Some((typ, value)) => new_checksum_with_type(typ, &value)
            .map(Some)
            .ok_or("invalid checksum"),
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReaderOptions {
    pub md5_hex: String,
    pub sha256_hex: String,
    pub size: i64,
    pub actual_size: i64,
}

pub struct Reader<R> {
    src: R,
    bytes_read: i64,
    size: i64,
    actual_size: i64,
    expected_md5: Option<Vec<u8>>,
    expected_sha256: Option<Vec<u8>>,
    md5: Md5,
    sha256: Sha256,
    finished: bool,
}

impl<R: Read> Reader<R> {
    pub fn new(
        src: R,
        size: i64,
        md5_hex: &str,
        sha256_hex: &str,
        actual_size: i64,
    ) -> Result<Self, HashError> {
        Self::with_options(
            src,
            ReaderOptions {
                md5_hex: md5_hex.to_owned(),
                sha256_hex: sha256_hex.to_owned(),
                size,
                actual_size,
            },
        )
    }

    pub fn with_options(src: R, options: ReaderOptions) -> Result<Self, HashError> {
        let expected_md5 = parse_optional_hex_md5(&options.md5_hex)?;
        let expected_sha256 = parse_optional_hex_sha256(&options.sha256_hex)?;
        Ok(Self {
            src,
            bytes_read: 0,
            size: options.size,
            actual_size: options.actual_size,
            expected_md5,
            expected_sha256,
            md5: Md5::new(),
            sha256: Sha256::new(),
            finished: false,
        })
    }

    pub fn merge(
        inner: Reader<R>,
        size: i64,
        md5_hex: &str,
        sha256_hex: &str,
        actual_size: i64,
    ) -> Result<Reader<Self>, HashError> {
        if inner.bytes_read > 0 {
            return Err(HashError::SizeMismatch(SizeMismatch {
                want: 0,
                got: inner.bytes_read,
            }));
        }

        let new_md5 = parse_optional_hex_md5(md5_hex)?;
        let new_sha256 = parse_optional_hex_sha256(sha256_hex)?;

        if let (Some(existing), Some(next)) = (inner.expected_md5.as_ref(), new_md5.as_ref()) {
            if existing != next {
                return Err(HashError::BadDigest(BadDigest {
                    expected_md5: hex::encode(existing),
                    calculated_md5: md5_hex.to_owned(),
                }));
            }
        }

        if let (Some(existing), Some(next)) = (inner.expected_sha256.as_ref(), new_sha256.as_ref())
        {
            if existing != next {
                return Err(HashError::SHA256Mismatch(SHA256Mismatch {
                    expected_sha256: hex::encode(existing),
                    calculated_sha256: sha256_hex.to_owned(),
                }));
            }
        }

        if inner.size >= 0 && size >= 0 && inner.size != size {
            return Err(HashError::SizeMismatch(SizeMismatch {
                want: inner.size,
                got: size,
            }));
        }

        let merged_size = if inner.size < 0 && size >= 0 {
            size
        } else {
            inner.size
        };
        let merged_actual = if inner.actual_size <= 0 && actual_size >= 0 {
            actual_size
        } else {
            inner.actual_size
        };
        let inner_md5_hex = inner.md5_hex_string();
        let inner_sha256_hex = inner.sha256_hex_string();

        Reader::with_options(
            inner,
            ReaderOptions {
                md5_hex: new_md5.as_ref().map(hex::encode).unwrap_or(inner_md5_hex),
                sha256_hex: new_sha256
                    .as_ref()
                    .map(hex::encode)
                    .unwrap_or(inner_sha256_hex),
                size: merged_size,
                actual_size: merged_actual,
            },
        )
    }

    pub fn size(&self) -> i64 {
        self.size
    }

    pub fn actual_size(&self) -> i64 {
        self.actual_size
    }

    pub fn md5_current(&self) -> Vec<u8> {
        self.md5.clone().finalize().to_vec()
    }

    pub fn sha256(&self) -> Vec<u8> {
        self.expected_sha256.clone().unwrap_or_default()
    }

    pub fn sha256_hex_string(&self) -> String {
        self.expected_sha256
            .as_ref()
            .map(hex::encode)
            .unwrap_or_default()
    }

    pub fn md5_hex_string(&self) -> String {
        self.expected_md5
            .as_ref()
            .map(hex::encode)
            .unwrap_or_default()
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;

        let actual_md5 = self.md5.clone().finalize().to_vec();
        if let Some(expected_md5) = &self.expected_md5 {
            if expected_md5 != &actual_md5 {
                return Err(io::Error::other(BadDigest {
                    expected_md5: hex::encode(expected_md5),
                    calculated_md5: hex::encode(actual_md5),
                }));
            }
        }

        let actual_sha256 = self.sha256.clone().finalize().to_vec();
        if let Some(expected_sha256) = &self.expected_sha256 {
            if expected_sha256 != &actual_sha256 {
                return Err(io::Error::other(SHA256Mismatch {
                    expected_sha256: hex::encode(expected_sha256),
                    calculated_sha256: hex::encode(actual_sha256),
                }));
            }
        }

        Ok(())
    }
}

impl<R: Read> Read for Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.src.read(buf)?;
        if self.size >= 0 && self.bytes_read + n as i64 > self.size {
            return Err(io::Error::other(ioutil::ERR_OVERREAD));
        }
        self.bytes_read += n as i64;
        self.md5.update(&buf[..n]);
        self.sha256.update(&buf[..n]);

        if n == 0 {
            self.finish()?;
        }

        Ok(n)
    }
}

fn parse_optional_hex_md5(value: &str) -> Result<Option<Vec<u8>>, HashError> {
    if value.is_empty() {
        return Ok(None);
    }
    hex::decode(value).map(Some).map_err(|_| {
        HashError::BadDigest(BadDigest {
            expected_md5: value.to_owned(),
            calculated_md5: String::new(),
        })
    })
}

fn parse_optional_hex_sha256(value: &str) -> Result<Option<Vec<u8>>, HashError> {
    if value.is_empty() {
        return Ok(None);
    }
    hex::decode(value).map(Some).map_err(|_| {
        HashError::SHA256Mismatch(SHA256Mismatch {
            expected_sha256: value.to_owned(),
            calculated_sha256: String::new(),
        })
    })
}

fn sha1_digest(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn sha256_digest(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn encode_uvarint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_uvarint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        value |= u64::from(byte & 0x7f) << shift;
        index += 1;
        if byte < 0x80 {
            *bytes = &bytes[index..];
            return Some(value);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}
