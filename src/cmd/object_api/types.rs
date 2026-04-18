use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CompletePart {
    pub etag: String,
    pub part_number: i32,
}
impl_msg_codec!(CompletePart);

pub fn get_complete_multipart_md5(parts: &[CompletePart]) -> String {
    let mut input = Vec::new();
    for part in parts {
        if let Ok(decoded) = hex::decode(&part.etag) {
            input.extend(decoded);
        } else {
            input.extend(part.etag.as_bytes());
        }
    }
    let digest = Md5::digest(&input);
    format!("{:x}-{}", digest, parts.len())
}

pub fn remove_standard_storage_class(
    metadata: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut cleaned = metadata.clone();
    if cleaned
        .get("x-amz-storage-class")
        .is_some_and(|value| value == "STANDARD")
    {
        cleaned.remove("x-amz-storage-class");
    }
    cleaned
}

pub fn clean_metadata(metadata: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut cleaned = metadata.clone();
    cleaned.remove("etag");
    cleaned.remove("md5Sum");
    remove_standard_storage_class(&cleaned)
}

pub fn clean_metadata_keys(
    metadata: &BTreeMap<String, String>,
    keys: &[&str],
) -> BTreeMap<String, String> {
    let mut cleaned = metadata.clone();
    for key in keys {
        cleaned.remove(*key);
    }
    cleaned
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub etag: String,
    pub content_type: String,
    pub is_dir: bool,
    pub delete_marker: bool,
    pub version_id: String,
    pub is_latest: bool,
    pub mod_time: i64,
    pub user_defined: BTreeMap<String, String>,
    pub parts: Vec<ObjectPartInfo>,
    pub size: i64,
    pub actual_size: Option<i64>,
}
impl_msg_codec!(ObjectInfo);

impl ObjectInfo {
    pub fn is_remote(&self) -> bool {
        if self
            .user_defined
            .get(TRANSITION_STATUS_KEY)
            .is_none_or(|value| value != "complete")
        {
            return false;
        }

        !self
            .user_defined
            .get(AMZ_RESTORE_HEADER)
            .and_then(|header| parse_restore_obj_status(header).ok())
            .is_some_and(|status| status.on_disk())
    }

    pub fn is_compressed(&self) -> bool {
        self.user_defined.contains_key(COMPRESSION_KEY)
    }

    pub fn is_compressed_ok(&self) -> Result<bool, String> {
        let Some(scheme) = self.user_defined.get(COMPRESSION_KEY) else {
            return Ok(false);
        };
        match scheme.as_str() {
            COMPRESSION_ALGORITHM_V1 | COMPRESSION_ALGORITHM_V2 => Ok(true),
            other => Err(format!("unknown compression scheme: {other}")),
        }
    }

    pub fn get_actual_size(&self) -> Result<i64, String> {
        if let Some(actual_size) = self.actual_size {
            return Ok(actual_size);
        }
        if self.is_compressed() {
            if let Some(value) = self.user_defined.get(ACTUAL_SIZE_KEY) {
                return value.parse::<i64>().map_err(|err| err.to_string());
            }
            let actual_size: i64 = self.parts.iter().map(|part| part.actual_size).sum();
            if actual_size == 0 && actual_size != self.size {
                return Ok(-1);
            }
            return Ok(actual_size);
        }
        Ok(self.size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CompressConfig {
    pub enabled: bool,
    pub allow_encrypted: bool,
    pub extensions: Vec<String>,
    pub mime_types: Vec<String>,
}
impl_msg_codec!(CompressConfig);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MakeBucketOptions {
    pub versioning_enabled: bool,
}
impl_msg_codec!(MakeBucketOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BucketOptions {}
impl_msg_codec!(BucketOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BucketInfo {
    pub name: String,
}
impl_msg_codec!(BucketInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListObjectsInfo {
    pub objects: Vec<ObjectInfo>,
    pub prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_marker: String,
    pub next_continuation_token: String,
}
impl_msg_codec!(ListObjectsInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListObjectVersionsInfo {
    pub objects: Vec<ObjectInfo>,
    pub prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_marker: String,
    pub next_version_id_marker: String,
}
impl_msg_codec!(ListObjectVersionsInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObjectOptions {
    pub user_defined: BTreeMap<String, String>,
    pub versioned: bool,
    pub version_suspended: bool,
    pub version_id: String,
    pub mtime: Option<i64>,
}
impl_msg_codec!(ObjectOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PutObjReader {
    pub data: Vec<u8>,
    pub declared_size: i64,
    pub expected_md5: String,
    pub expected_sha256: String,
}
impl_msg_codec!(PutObjReader);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NewMultipartUploadResult {
    pub upload_id: String,
}
impl_msg_codec!(NewMultipartUploadResult);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MultipartPartInfo {
    pub etag: String,
    pub part_number: i32,
    pub size: i64,
}
impl_msg_codec!(MultipartPartInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MultipartInfo {
    pub object: String,
    pub upload_id: String,
}
impl_msg_codec!(MultipartInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListMultipartsInfo {
    pub key_marker: String,
    pub upload_id_marker: String,
    pub next_key_marker: String,
    pub next_upload_id_marker: String,
    pub max_uploads: i32,
    pub is_truncated: bool,
    pub prefix: String,
    pub delimiter: String,
    pub uploads: Vec<MultipartInfo>,
}
impl_msg_codec!(ListMultipartsInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PartInfo {
    pub part_number: i32,
    pub size: i64,
    pub etag: String,
}
impl_msg_codec!(PartInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListPartsInfo {
    pub bucket: String,
    pub object: String,
    pub upload_id: String,
    pub part_number_marker: i32,
    pub next_part_number_marker: i32,
    pub max_parts: i32,
    pub is_truncated: bool,
    pub parts: Vec<PartInfo>,
}
impl_msg_codec!(ListPartsInfo);
