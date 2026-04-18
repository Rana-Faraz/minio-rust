use super::*;

pub const RESERVED_METADATA_PREFIX_LOWER: &str = "x-minio-internal-";
pub const TRANSITION_STATUS_KEY: &str = "x-minio-internal-transition-status";
pub const AMZ_RESTORE_HEADER: &str = "x-amz-restore";
pub const HEALING_KEY: &str = "x-minio-internal-healing";
pub const REPLICATION_TIMESTAMP_KEY: &str = "x-minio-internal-replication-timestamp";
pub const REPLICA_TIMESTAMP_KEY: &str = "x-minio-internal-replica-timestamp";
pub const TIER_FREE_VERSION_ID_KEY: &str = "x-minio-internal-tier-free-versionID";
pub const TIER_FREE_MARKER_KEY: &str = "x-minio-internal-tier-free-marker";
pub const TIER_SKIP_FVID_KEY: &str = "x-minio-internal-tier-skip-fvid";
pub const XL_META_OBJECT_TYPE: u8 = 1;
pub const XL_META_DELETE_MARKER_TYPE: u8 = 2;
pub const XL_META_VERSION_100: &str = "1.0.0";
pub const XL_META_VERSION_101: &str = "1.0.1";
pub const XL_META_FORMAT: &str = "xl";
pub const ERR_PART_SIZE_ZERO: &str = "part size cannot be zero";
pub const ERR_PART_SIZE_INDEX: &str = "part index must be positive";
pub const ERR_INVALID_RANGE: &str = "InvalidRange";

pub fn is_xlmeta_format_valid(version: &str, format: &str) -> bool {
    (version == XL_META_VERSION_100 || version == XL_META_VERSION_101) && format == XL_META_FORMAT
}

pub fn is_xlmeta_erasure_info_valid(data: i32, parity: i32) -> bool {
    data >= parity && data > 0 && parity >= 0
}

pub fn calculate_part_size_from_idx(
    total_size: i64,
    part_size: i64,
    part_index: i32,
) -> Result<i64, String> {
    if total_size < -1 {
        return Err(ERR_INVALID_ARGUMENT.to_string());
    }
    if part_size == 0 {
        return Err(ERR_PART_SIZE_ZERO.to_string());
    }
    if part_index <= 0 {
        return Err(ERR_PART_SIZE_INDEX.to_string());
    }
    if total_size <= 0 {
        return Ok(0);
    }

    let completed_parts = i64::from(part_index - 1);
    let start = completed_parts.saturating_mul(part_size);
    if start >= total_size {
        return Ok(0);
    }

    Ok((total_size - start).min(part_size))
}
