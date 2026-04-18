use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StatInfo {
    pub size: i64,
    pub mod_time: i64,
    pub name: String,
    pub dir: bool,
    pub mode: u32,
}
impl_msg_codec!(StatInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChecksumInfo {
    pub part_number: i32,
    pub algorithm: u32,
    pub hash: Option<Vec<u8>>,
}
impl_msg_codec!(ChecksumInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChecksumInfoJson {
    pub name: String,
    pub algorithm: String,
    pub hash: String,
}
impl_msg_codec!(ChecksumInfoJson);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaMinio {
    pub release: String,
}
impl_msg_codec!(XlMetaMinio);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaV1Object {
    pub version: String,
    pub format: String,
    pub stat: StatInfo,
    pub erasure: ErasureInfo,
    pub minio: XlMetaMinio,
    pub meta: Option<BTreeMap<String, String>>,
    pub parts: Option<Vec<ObjectPartInfo>>,
    pub version_id: String,
    pub data_dir: String,
}
impl_msg_codec!(XlMetaV1Object);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaDataDirDecoder {
    pub data_dir: Option<Vec<u8>>,
}
impl_msg_codec!(XlMetaDataDirDecoder);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaV2DeleteMarker {
    pub version_id: Option<Vec<u8>>,
    pub mod_time: i64,
    pub meta_sys: Option<BTreeMap<String, Vec<u8>>>,
}
impl_msg_codec!(XlMetaV2DeleteMarker);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaV2Object {
    pub version_id: Option<Vec<u8>>,
    pub data_dir: Option<Vec<u8>>,
    pub erasure_algorithm: u8,
    pub erasure_m: i32,
    pub erasure_n: i32,
    pub erasure_block_size: i64,
    pub erasure_index: i32,
    pub erasure_dist: Option<Vec<u8>>,
    pub bitrot_checksum_algo: u8,
    pub part_numbers: Option<Vec<i32>>,
    pub part_etags: Option<Vec<String>>,
    pub part_sizes: Option<Vec<i64>>,
    pub part_actual_sizes: Option<Vec<i64>>,
    pub part_indices: Option<Vec<Vec<u8>>>,
    pub size: i64,
    pub mod_time: i64,
    pub meta_sys: Option<BTreeMap<String, Vec<u8>>>,
    pub meta_user: Option<BTreeMap<String, String>>,
}
impl_msg_codec!(XlMetaV2Object);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaV2Version {
    pub type_id: u8,
    pub object_v1: Option<XlMetaV1Object>,
    pub object_v2: Option<XlMetaV2Object>,
    pub delete_marker: Option<XlMetaV2DeleteMarker>,
    pub written_by_version: u64,
}
impl_msg_codec!(XlMetaV2Version);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaV2VersionHeader {
    pub version_id: Option<Vec<u8>>,
    pub mod_time: i64,
    pub signature: Option<Vec<u8>>,
    pub type_id: u8,
    pub flags: u8,
    pub ec_n: u8,
    pub ec_m: u8,
}
impl_msg_codec!(XlMetaV2VersionHeader);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaV2StoredVersion {
    pub header: XlMetaV2VersionHeader,
    pub version: XlMetaV2Version,
    pub uses_data_dir: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaV2 {
    pub versions: Vec<XlMetaV2StoredVersion>,
    pub data: XlMetaInlineData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaCacheEntry {
    pub name: String,
    pub metadata: Vec<u8>,
}
