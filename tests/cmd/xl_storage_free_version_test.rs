use minio_rust::cmd::{ErasureInfo, FileInfo, XlMetaV2};

pub const SOURCE_FILE: &str = "cmd/xl-storage-free-version_test.go";

#[test]
fn test_free_version_line_45() {
    let mut file_info = FileInfo::default();
    assert_eq!(file_info.tier_free_version_id(), "");
    assert!(!file_info.tier_free_version());

    file_info.set_tier_free_version_id("vid-123");
    file_info.set_tier_free_version();

    assert_eq!(file_info.tier_free_version_id(), "vid-123");
    assert!(file_info.tier_free_version());

    let mut meta = XlMetaV2::default();
    meta.add_version(FileInfo {
        volume: "bucket".to_string(),
        name: "object".to_string(),
        version_id: "vid-123".to_string(),
        mod_time: 10,
        size: 1,
        data_dir: "data-vid-123".to_string(),
        erasure: ErasureInfo {
            data_blocks: 2,
            parity_blocks: 2,
            block_size: 1024,
            ..ErasureInfo::default()
        },
        metadata: file_info.metadata.clone(),
        ..FileInfo::default()
    })
    .expect("add version");

    let versions = meta
        .list_versions("bucket", "object", true)
        .expect("list versions");
    let listed = versions.versions.expect("versions");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].tier_free_version_id(), "vid-123");
    assert!(listed[0].tier_free_version());
    assert!(versions.free_versions.is_none());
}

#[test]
fn test_skip_free_version_line_231() {
    let mut file_info = FileInfo::default();
    assert!(!file_info.skip_tier_free_version());

    file_info.set_skip_tier_free_version();
    assert!(file_info.skip_tier_free_version());

    file_info.set_tier_free_version_id("vid-skip");
    assert_eq!(file_info.tier_free_version_id(), "vid-skip");
    assert!(file_info.skip_tier_free_version());
}
