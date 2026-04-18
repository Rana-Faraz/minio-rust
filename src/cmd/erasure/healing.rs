use std::collections::BTreeMap;

use crate::cmd::*;

pub const CHECK_PART_UNKNOWN: i32 = 0;
pub const CHECK_PART_SUCCESS: i32 = 1;
pub const CHECK_PART_DISK_NOT_FOUND: i32 = 2;
pub const CHECK_PART_VOLUME_NOT_FOUND: i32 = 3;
pub const CHECK_PART_FILE_NOT_FOUND: i32 = 4;
pub const CHECK_PART_FILE_CORRUPT: i32 = 5;

pub fn new_file_info(object: &str, data_blocks: i32, parity_blocks: i32) -> FileInfo {
    let total_blocks = data_blocks + parity_blocks;
    FileInfo {
        name: object.to_string(),
        erasure: ErasureInfo {
            algorithm: "reedsolomon".to_string(),
            data_blocks,
            parity_blocks,
            block_size: BLOCK_SIZE_V2,
            index: 0,
            distribution: Some((1..=total_blocks).collect()),
        },
        ..Default::default()
    }
}

fn dangling_meta_errs_count(errs: &[Option<&str>]) -> (usize, usize) {
    let mut not_found_count = 0;
    let mut non_actionable_count = 0;
    for err in errs {
        match err {
            None => {}
            Some(value) if *value == ERR_FILE_NOT_FOUND || *value == ERR_FILE_VERSION_NOT_FOUND => {
                not_found_count += 1;
            }
            Some(_) => non_actionable_count += 1,
        }
    }
    (not_found_count, non_actionable_count)
}

fn dangling_part_errs_count(results: &[i32]) -> (usize, usize) {
    let mut not_found_count = 0;
    let mut non_actionable_count = 0;
    for result in results {
        match *result {
            CHECK_PART_SUCCESS => {}
            CHECK_PART_FILE_NOT_FOUND => not_found_count += 1,
            _ => non_actionable_count += 1,
        }
    }
    (not_found_count, non_actionable_count)
}

pub fn is_object_dangling(
    meta_arr: &[FileInfo],
    errs: &[Option<&str>],
    data_errs_by_part: &BTreeMap<i32, Vec<i32>>,
) -> (FileInfo, bool) {
    let (not_found_meta_errs, non_actionable_meta_errs) = dangling_meta_errs_count(errs);

    let mut not_found_parts_errs = 0;
    let mut non_actionable_parts_errs = 0;
    for data_errs in data_errs_by_part.values() {
        let (not_found, non_actionable) = dangling_part_errs_count(data_errs);
        if not_found > not_found_parts_errs {
            not_found_parts_errs = not_found;
            non_actionable_parts_errs = non_actionable;
        }
    }

    let valid_meta = meta_arr
        .iter()
        .find(|meta| meta.is_valid())
        .cloned()
        .unwrap_or_default();

    if !valid_meta.is_valid() {
        let data_blocks = meta_arr.len().div_ceil(2);
        if not_found_parts_errs > data_blocks {
            return (valid_meta, true);
        }
        return (valid_meta, false);
    }

    if non_actionable_meta_errs > 0 || non_actionable_parts_errs > 0 {
        return (valid_meta, false);
    }

    if valid_meta.deleted {
        let data_blocks = errs.len().div_ceil(2);
        return (valid_meta, not_found_meta_errs > data_blocks);
    }

    if not_found_meta_errs > 0 && not_found_meta_errs > valid_meta.erasure.parity_blocks as usize {
        return (valid_meta, true);
    }

    if !valid_meta.is_remote()
        && not_found_parts_errs > 0
        && not_found_parts_errs > valid_meta.erasure.parity_blocks as usize
    {
        return (valid_meta, true);
    }

    (valid_meta, false)
}
