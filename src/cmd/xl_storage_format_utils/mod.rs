use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::cmd::{FileInfoVersions, XlMetaV2};

pub fn hash_deterministic_string(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn get_file_info_versions(
    meta: &XlMetaV2,
    volume: &str,
    path: &str,
    incl_free_vers: bool,
) -> Result<FileInfoVersions, String> {
    meta.list_versions(volume, path, incl_free_vers)
}
