use std::collections::BTreeMap;
use std::io::Cursor;

use minio_rust::cmd::{
    calculate_part_size_from_idx, is_xlmeta_erasure_info_valid, is_xlmeta_format_valid,
    read_xl_meta_no_data, ErasureInfo, FileInfo, ObjectPartInfo, StatInfo, XlMetaMinio,
    XlMetaV1Object, XlMetaV2, ERR_INVALID_ARGUMENT, ERR_PART_SIZE_INDEX, ERR_PART_SIZE_ZERO,
    XL_META_FORMAT, XL_META_VERSION_100, XL_META_VERSION_101,
};

pub const SOURCE_FILE: &str = "cmd/xl-storage-format_test.go";

fn sample_xlmeta_v1(total_parts: usize) -> XlMetaV1Object {
    XlMetaV1Object {
        version: XL_META_VERSION_101.to_string(),
        format: XL_META_FORMAT.to_string(),
        stat: StatInfo {
            size: 20,
            mod_time: 1_700_000_000,
            name: "object".to_string(),
            dir: false,
            mode: 0,
        },
        erasure: ErasureInfo {
            algorithm: "klauspost/reedsolomon/vandermonde".to_string(),
            data_blocks: 5,
            parity_blocks: 5,
            block_size: 10 * 1024 * 1024,
            index: 1,
            distribution: Some(vec![9, 10, 1, 2, 3, 4, 5, 6, 7, 8]),
        },
        minio: XlMetaMinio {
            release: "test".to_string(),
        },
        meta: Some(BTreeMap::from([
            ("testKey1".to_string(), "val1".to_string()),
            ("testKey2".to_string(), "val2".to_string()),
        ])),
        parts: Some(
            (0..total_parts)
                .map(|idx| ObjectPartInfo {
                    number: (idx + 1) as i32,
                    size: 64 * 1024 * 1024,
                    etag: String::new(),
                    actual_size: 64 * 1024 * 1024,
                })
                .collect(),
        ),
        version_id: String::new(),
        data_dir: "legacy".to_string(),
    }
}

fn sample_file_info(version_id: &str, data_dir: &str, mod_time: i64) -> FileInfo {
    FileInfo {
        volume: "volume".to_string(),
        name: "object-name".to_string(),
        version_id: version_id.to_string(),
        is_latest: true,
        deleted: false,
        transition_status: "PENDING".to_string(),
        transitioned_obj_name: String::new(),
        transition_tier: String::new(),
        transition_version_id: String::new(),
        expire_restored: false,
        data_dir: data_dir.to_string(),
        xlv1: false,
        mod_time,
        size: 1_234_456,
        mode: 0,
        written_by_version: 0,
        metadata: Some(BTreeMap::from([
            ("content-md5".to_string(), format!("md5-{version_id}")),
            (
                "x-amz-bucket-replication-status".to_string(),
                "PENDING".to_string(),
            ),
            ("content-type".to_string(), "application/json".to_string()),
        ])),
        parts: Some(vec![
            ObjectPartInfo {
                number: 1,
                size: 1_234_345,
                actual_size: 1_234_345,
                etag: "etag-1".to_string(),
            },
            ObjectPartInfo {
                number: 2,
                size: 1_234_345,
                actual_size: 1_234_345,
                etag: "etag-2".to_string(),
            },
        ]),
        erasure: ErasureInfo {
            algorithm: "reedsolomon".to_string(),
            data_blocks: 4,
            parity_blocks: 2,
            block_size: 10_000,
            index: 1,
            distribution: Some(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        },
        mark_deleted: false,
        replication_state: Default::default(),
        data: None,
        num_versions: 0,
        successor_mod_time: 0,
        fresh: false,
        idx: 0,
        checksum: None,
        versioned: true,
    }
}

fn build_xl_meta_v2(size: usize) -> (XlMetaV2, Vec<String>, Vec<u8>) {
    let mut xl = XlMetaV2::default();
    let mut ids = Vec::with_capacity(size);
    for idx in 0..size {
        let version_id = format!("version-{idx}");
        let data_dir = format!("data-dir-{idx}");
        let file_info = sample_file_info(&version_id, &data_dir, 1_700_000_000 - idx as i64);
        xl.add_version(file_info).expect("add version");
        ids.push(version_id);
    }
    let enc = xl.append_to(None).expect("append");
    (xl, ids, enc)
}

#[test]
fn test_is_xlmeta_format_valid_line_34() {
    let cases = [
        ("123", "fs", false),
        ("123", XL_META_FORMAT, false),
        (XL_META_VERSION_100, "test", false),
        (XL_META_VERSION_101, "hello", false),
        (XL_META_VERSION_100, XL_META_FORMAT, true),
        (XL_META_VERSION_101, XL_META_FORMAT, true),
    ];

    for (version, format, want) in cases {
        assert_eq!(is_xlmeta_format_valid(version, format), want);
    }
}

#[test]
fn test_is_xlmeta_erasure_info_valid_line_55() {
    let cases = [
        (5, 6, false),
        (5, 5, true),
        (0, 5, false),
        (-1, 5, false),
        (5, -1, false),
        (5, 0, true),
        (5, 4, true),
    ];

    for (data, parity, want) in cases {
        assert_eq!(is_xlmeta_erasure_info_valid(data, parity), want);
    }
}

#[test]
fn test_get_xlmeta_v1_jsoniter1_line_233() {
    let sample = sample_xlmeta_v1(1);
    let bytes = serde_json::to_vec(&sample).expect("serialize");
    let decoded: XlMetaV1Object = serde_json::from_slice(&bytes).expect("deserialize");
    assert_eq!(decoded, sample);
}

#[test]
fn test_get_xlmeta_v1_jsoniter10_line_251() {
    let sample = sample_xlmeta_v1(10);
    let bytes = serde_json::to_vec(&sample).expect("serialize");
    let decoded: XlMetaV1Object = serde_json::from_slice(&bytes).expect("deserialize");
    assert_eq!(decoded, sample);
}

#[test]
fn test_get_part_size_from_idx_line_269() {
    let success_cases = [
        (0, 10, 1, 0),
        (4 * 1024 * 1024, 2 * 1024 * 1024, 1, 2 * 1024 * 1024),
        (4 * 1024 * 1024, 2 * 1024 * 1024, 2, 2 * 1024 * 1024),
        (4 * 1024 * 1024, 2 * 1024 * 1024, 3, 0),
        (5 * 1024 * 1024, 2 * 1024 * 1024, 1, 2 * 1024 * 1024),
        (5 * 1024 * 1024, 2 * 1024 * 1024, 2, 2 * 1024 * 1024),
        (5 * 1024 * 1024, 2 * 1024 * 1024, 3, 1024 * 1024),
        (5 * 1024 * 1024, 2 * 1024 * 1024, 4, 0),
    ];

    for (total_size, part_size, part_index, expected) in success_cases {
        assert_eq!(
            calculate_part_size_from_idx(total_size, part_size, part_index).expect("part size"),
            expected
        );
    }

    let failure_cases = [
        (10, 0, 1, ERR_PART_SIZE_ZERO),
        (10, 1, 0, ERR_PART_SIZE_INDEX),
        (-2, 10, 1, ERR_INVALID_ARGUMENT),
    ];

    for (total_size, part_size, part_index, expected_err) in failure_cases {
        assert_eq!(
            calculate_part_size_from_idx(total_size, part_size, part_index).unwrap_err(),
            expected_err
        );
    }
}

#[test]
fn benchmark_xl_meta_v2_shallow_line_325() {
    for size in [1, 10, 100] {
        let (_, _, enc) = build_xl_meta_v2(size);
        let mut loaded = XlMetaV2::default();
        loaded.load(&enc).expect("load");
        assert_eq!(loaded.versions.len(), size);
    }
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_fmt_sprint_size_line_378() {
    for size in [1, 10, 100] {
        let (_, _, enc) = build_xl_meta_v2(size);
        assert!(
            !enc.is_empty(),
            "encoded metadata should not be empty for {size}"
        );
    }
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_update_object_version_line_396() {
    let (_, ids, enc) = build_xl_meta_v2(10);
    let mut loaded = XlMetaV2::default();
    loaded.load(&enc).expect("load");

    let mut updated = sample_file_info(&ids[3], "data-dir-updated", 1_700_000_500);
    updated.size = 9_999;
    loaded.update_object_version(updated).expect("update");

    let listed = loaded.list_versions("volume", "path", true).expect("list");
    let versions = listed.versions.expect("versions");
    assert_eq!(versions.len(), 10);
    assert_eq!(versions[0].version_id, ids[3]);
    assert_eq!(versions[0].size, 9_999);
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_delete_version_line_423() {
    let (_, ids, enc) = build_xl_meta_v2(10);
    let mut loaded = XlMetaV2::default();
    loaded.load(&enc).expect("load");

    let deleted = loaded
        .delete_version(&sample_file_info(&ids[4], "data-dir-4", 0))
        .expect("delete");
    assert_eq!(deleted, "data-dir-4");
    assert_eq!(loaded.versions.len(), 9);
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_add_version_line_448() {
    let (_, _, enc) = build_xl_meta_v2(10);
    let mut loaded = XlMetaV2::default();
    loaded.load(&enc).expect("load");

    loaded
        .add_version(sample_file_info(
            "version-new",
            "data-dir-new",
            1_800_000_000,
        ))
        .expect("add");
    assert_eq!(loaded.versions.len(), 11);
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_to_file_info_line_475() {
    let (_, ids, enc) = build_xl_meta_v2(10);
    let mut loaded = XlMetaV2::default();
    loaded.load(&enc).expect("load");

    let info = loaded
        .to_file_info("volume", "path", &ids[5], false, true)
        .expect("to_file_info");
    assert_eq!(info.version_id, ids[5]);
    assert_eq!(info.parts.as_ref().map(Vec::len), Some(2));
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_list_versions_line_493() {
    let (_, _, enc) = build_xl_meta_v2(10);
    let mut loaded = XlMetaV2::default();
    loaded.load(&enc).expect("load");

    let listed = loaded.list_versions("volume", "path", true).expect("list");
    assert_eq!(listed.versions.as_ref().map(Vec::len), Some(10));
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_to_file_info_new_line_511() {
    let (_, ids, enc) = build_xl_meta_v2(10);
    let loaded = read_xl_meta_no_data(Cursor::new(enc.clone()), enc.len() as i64).expect("read");

    let info = loaded
        .to_file_info("volume", "path", &ids[1], false, true)
        .expect("to_file_info");
    assert_eq!(info.version_id, ids[1]);
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_list_versions_new_line_526() {
    let (_, _, enc) = build_xl_meta_v2(10);
    let loaded = read_xl_meta_no_data(Cursor::new(enc.clone()), enc.len() as i64).expect("read");

    let listed = loaded.list_versions("volume", "path", true).expect("list");
    assert_eq!(listed.versions.as_ref().map(Vec::len), Some(10));
}
