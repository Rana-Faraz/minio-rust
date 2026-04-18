use minio_rust::cmd::{
    get_file_info_versions, hash_deterministic_string, ErasureInfo, FileInfo, XlMetaV2,
};

pub const SOURCE_FILE: &str = "cmd/xl-storage-format-utils_test.go";

#[test]
fn test_hash_deterministic_string_line_30() {
    let first = hash_deterministic_string("minio");
    let second = hash_deterministic_string("minio");
    assert_eq!(first, second);
    assert_ne!(first, hash_deterministic_string("minio-rust"));
}

#[test]
fn subtest_test_hash_deterministic_string_tt_name_line_67() {
    let cases = [
        ("", hash_deterministic_string("")),
        ("bucket/object", hash_deterministic_string("bucket/object")),
        (
            "bucket/object/part.1",
            hash_deterministic_string("bucket/object/part.1"),
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(hash_deterministic_string(input), expected);
    }
}

#[test]
fn test_get_file_info_versions_line_115() {
    let mut meta = XlMetaV2::default();
    meta.add_version(FileInfo {
        volume: "bucket".to_string(),
        name: "object".to_string(),
        version_id: "v1".to_string(),
        mod_time: 10,
        size: 5,
        data_dir: "data-v1".to_string(),
        erasure: ErasureInfo {
            data_blocks: 2,
            parity_blocks: 2,
            block_size: 1024,
            ..ErasureInfo::default()
        },
        ..FileInfo::default()
    })
    .expect("add version 1");
    meta.add_version(FileInfo {
        volume: "bucket".to_string(),
        name: "object".to_string(),
        version_id: "v2".to_string(),
        mod_time: 20,
        size: 7,
        data_dir: "data-v2".to_string(),
        erasure: ErasureInfo {
            data_blocks: 2,
            parity_blocks: 2,
            block_size: 1024,
            ..ErasureInfo::default()
        },
        ..FileInfo::default()
    })
    .expect("add version 2");

    let versions = get_file_info_versions(&meta, "bucket", "object", false).expect("versions");
    assert_eq!(versions.volume, "bucket");
    assert_eq!(versions.name, "object");
    assert_eq!(versions.latest_mod_time, 20);
    let listed = versions.versions.expect("listed versions");
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].version_id, "v2");
    assert!(listed[0].is_latest);
    assert_eq!(listed[1].version_id, "v1");
    assert!(!listed[1].is_latest);
    assert_eq!(listed[1].successor_mod_time, 20);
}
