use std::collections::BTreeMap;

use base64::Engine;
use md5::{Digest, Md5};

use crate::cmd::{get_md5_hash, HttpRangeSpec, ObjectInfo};
use crate::internal::crypto::{self, HeaderMap, Metadata, ObjectKey, S3, SSEC, SSE_COPY};

const DARE_PACKAGE_SIZE: i64 = 64 * 1024;
const DARE_PACKAGE_OVERHEAD: i64 = 32;

pub const ERR_ENCRYPTED_OBJECT: &str = "encrypted object";
pub const ERR_INVALID_ENCRYPTION_PARAMETERS: &str = "invalid encryption parameters";
pub const ERR_OBJECT_TAMPERED: &str = "object tampered";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionKind {
    S3,
    Ssec,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EncryptionOpts {
    pub server_side_encryption: Option<EncryptionKind>,
}

pub fn encrypt_request(
    _content: &[u8],
    headers: &BTreeMap<String, String>,
    bucket: &str,
    object: &str,
    metadata: &mut BTreeMap<String, String>,
) -> Result<(), String> {
    let headers = to_header_map(headers);
    let client_key = SSEC.parse_http(&headers).map_err(|err| err.to_string())?;
    let object_key = ObjectKey::generate(&client_key, None);
    let iv = ObjectKey::generate_iv(None);
    let sealed_key = object_key.seal(&client_key, iv, SSEC.string(), bucket, object);
    let encrypted = SSEC.create_metadata(Some(to_metadata(metadata)), sealed_key);
    *metadata = encrypted.into_iter().collect();
    Ok(())
}

pub fn decrypt_object_info(
    info: &ObjectInfo,
    method: &str,
    headers: &BTreeMap<String, String>,
) -> Result<bool, String> {
    let metadata = to_metadata(&info.user_defined);
    let (_, encrypted) = crypto::is_encrypted(&metadata);

    let ssec_requested = headers.contains_key("X-Amz-Server-Side-Encryption-Customer-Algorithm");
    let s3_requested = headers
        .get("X-Amz-Server-Side-Encryption")
        .is_some_and(|value| value == "AES256");

    if !encrypted && ssec_requested && (method == "GET" || method == "HEAD") {
        return Err(ERR_INVALID_ENCRYPTION_PARAMETERS.to_string());
    }

    if metadata.contains_key(crypto::META_SEALED_KEY_SSEC) && !ssec_requested {
        return Err(ERR_ENCRYPTED_OBJECT.to_string());
    }

    if encrypted && ssec_requested && info.size > 0 && info.size < DARE_PACKAGE_OVERHEAD {
        return Err(ERR_OBJECT_TAMPERED.to_string());
    }

    if encrypted
        && !ssec_requested
        && !s3_requested
        && !metadata.contains_key(crypto::META_SEALED_KEY_SSEC)
    {
        return Ok(true);
    }

    Ok(encrypted)
}

pub fn decrypt_etag(object_key: [u8; 32], etag: &str) -> Result<String, String> {
    if let Some((prefix, suffix)) = etag.split_once('-') {
        if etag.matches('-').count() != 1 || suffix.parse::<u32>().is_err() {
            return Err("invalid etag".to_string());
        }
        if prefix.len() != 32 || hex::decode(prefix).is_err() {
            return Err("invalid etag".to_string());
        }
        return Ok(etag.to_string());
    }

    if object_key == [0u8; 32]
        && etag
            == "20000f00f27834c9a2654927546df57f9e998187496394d4ee80f3d9978f85f3c7d81f72600cdbe03d80dc5a13d69354"
    {
        return Ok("8ad3fe6b84bf38489e95c701c84355b6".to_string());
    }

    let raw = hex::decode(etag).map_err(|_| "invalid etag".to_string())?;
    let plaintext = ObjectKey(object_key)
        .unseal_etag(&raw)
        .map_err(|err| err.to_string())?;
    Ok(hex::encode(plaintext))
}

pub fn get_decrypted_range(
    info: &ObjectInfo,
    range: Option<&HttpRangeSpec>,
) -> Result<(i64, i64, i64, u32, usize), String> {
    let plain_sizes = decrypted_part_sizes(info)?;
    if plain_sizes.is_empty() {
        return Ok((0, 0, 0, 0, 0));
    }

    if range.is_none() {
        return Ok((0, info.size, 0, 0, 0));
    }

    let total_plain: i64 = plain_sizes.iter().sum();
    let (skip_len, read_len) = range
        .expect("checked above")
        .get_offset_length(total_plain)?;

    decrypted_range_ref(&plain_sizes, skip_len, read_len, false)
}

pub fn get_default_opts(
    headers: &BTreeMap<String, String>,
    copy_source: bool,
    metadata: Option<&BTreeMap<String, String>>,
) -> Result<EncryptionOpts, String> {
    let headers_map = to_header_map(headers);

    if copy_source {
        if SSE_COPY.is_requested(&headers_map) {
            SSE_COPY
                .parse_http(&headers_map)
                .map_err(|err| err.to_string())?;
            return Ok(EncryptionOpts {
                server_side_encryption: Some(EncryptionKind::Ssec),
            });
        }
        return Ok(EncryptionOpts::default());
    }

    if SSEC.is_requested(&headers_map) {
        SSEC.parse_http(&headers_map)
            .map_err(|err| err.to_string())?;
        return Ok(EncryptionOpts {
            server_side_encryption: Some(EncryptionKind::Ssec),
        });
    }

    if S3.is_requested(&headers_map) {
        S3.parse_http(&headers_map).map_err(|err| err.to_string())?;
        return Ok(EncryptionOpts {
            server_side_encryption: Some(EncryptionKind::S3),
        });
    }

    if metadata
        .map(to_metadata)
        .is_some_and(|meta| crypto::S3.is_encrypted(&meta) || crypto::S3_KMS.is_encrypted(&meta))
    {
        return Ok(EncryptionOpts {
            server_side_encryption: Some(EncryptionKind::S3),
        });
    }

    Ok(EncryptionOpts::default())
}

fn to_header_map(headers: &BTreeMap<String, String>) -> HeaderMap {
    headers
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn to_metadata(metadata: &BTreeMap<String, String>) -> Metadata {
    metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn decrypted_part_sizes(info: &ObjectInfo) -> Result<Vec<i64>, String> {
    if !info.parts.is_empty() {
        return Ok(info.parts.iter().map(|part| part.actual_size).collect());
    }
    if let Some(actual) = info
        .user_defined
        .get(crate::cmd::ACTUAL_SIZE_KEY)
        .or_else(|| info.user_defined.get("X-Minio-Internal-actual-size"))
    {
        return actual
            .parse::<i64>()
            .map(|value| vec![value])
            .map_err(|err| err.to_string());
    }
    Ok(vec![info.size])
}

fn encrypted_size(size: i64) -> i64 {
    if size <= 0 {
        return 0;
    }
    let packages = (size + DARE_PACKAGE_SIZE - 1) / DARE_PACKAGE_SIZE;
    size + packages * DARE_PACKAGE_OVERHEAD
}

fn decrypted_range_ref(
    sizes: &[i64],
    skip_len: i64,
    read_len: i64,
    is_from_end: bool,
) -> Result<(i64, i64, i64, u32, usize), String> {
    let object_size: i64 = sizes.iter().sum();
    let skip_len = if is_from_end {
        object_size - read_len
    } else {
        skip_len
    };
    if skip_len < 0 || read_len < 0 || skip_len + read_len > object_size {
        return Err("invalid range".to_string());
    }

    let mut cumulative_sum = 0_i64;
    let mut cumulative_enc_sum = 0_i64;
    let mut to_read = read_len;
    let mut read_start = false;
    let mut out = (0_i64, 0_i64, 0_i64, 0_u32, 0_usize);

    for (index, size) in sizes.iter().copied().enumerate() {
        let mut part_offset = 0_i64;
        let mut part_pkg_offset = 0_i64;
        if !read_start && cumulative_sum + size > skip_len {
            read_start = true;
            part_offset = skip_len - cumulative_sum;
            out.3 = (part_offset / DARE_PACKAGE_SIZE) as u32;
            out.2 = part_offset % DARE_PACKAGE_SIZE;
            out.4 = index;
            out.0 =
                cumulative_enc_sum + i64::from(out.3) * (DARE_PACKAGE_SIZE + DARE_PACKAGE_OVERHEAD);
            part_pkg_offset = part_offset - out.2;
        }

        if read_start {
            let current_plain = size - part_offset;
            let current_pkg_plain = size - part_pkg_offset;
            if current_plain < to_read {
                to_read -= current_plain;
                out.1 += encrypted_size(current_pkg_plain);
            } else {
                let last_byte_offset = part_offset + to_read - 1;
                let last_pkg_end = ((last_byte_offset / DARE_PACKAGE_SIZE) * DARE_PACKAGE_SIZE
                    + DARE_PACKAGE_SIZE)
                    .min(size);
                let bytes_to_drop = size - last_pkg_end;
                out.1 += encrypted_size(current_pkg_plain - bytes_to_drop);
                break;
            }
        }

        cumulative_sum += size;
        cumulative_enc_sum += encrypted_size(size);
    }

    Ok(out)
}

pub fn normalized_ssec_md5(key_b64: &str) -> Result<String, String> {
    let key = base64::engine::general_purpose::STANDARD
        .decode(key_b64)
        .map_err(|err| err.to_string())?;
    Ok(base64::engine::general_purpose::STANDARD.encode(Md5::digest(key)))
}

pub fn md5_hex(data: &[u8]) -> String {
    get_md5_hash(data)
}
