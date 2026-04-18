use minio_rust::internal::disk;

pub const SOURCE_FILE: &str = "internal/disk/disk_test.go";

#[test]
fn test_free() {
    let temp_dir = tempfile::tempdir().expect("tempdir must be created");
    let info = disk::get_info(temp_dir.path(), true).expect("disk info should be readable");

    assert_ne!(info.fs_type, "UNKNOWN");
    assert!(!info.fs_type.is_empty());
}
