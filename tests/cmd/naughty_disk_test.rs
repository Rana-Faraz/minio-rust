// Rust test snapshot derived from cmd/naughty-disk_test.go.

use std::collections::BTreeMap;

use minio_rust::cmd::{LocalXlStorage, NaughtyDisk, ERR_FAULTY_DISK};
use tempfile::tempdir;

pub const SOURCE_FILE: &str = "cmd/naughty-disk_test.go";

#[test]
fn test_naughty_disk_programmed_errors() {
    let temp_dir = tempdir().expect("tempdir");
    let storage = LocalXlStorage::new(temp_dir.path().to_str().expect("path")).expect("storage");
    storage.make_vol("bucket").expect("make vol");

    let mut programmed = BTreeMap::new();
    programmed.insert(1, ERR_FAULTY_DISK.to_string());
    let disk = NaughtyDisk::new(storage, programmed, None);

    let err = disk
        .append_file("bucket", "object.txt", b"first")
        .expect_err("first call should fail");
    assert_eq!(err, ERR_FAULTY_DISK);
    assert_eq!(disk.call_nr(), 1);

    disk.append_file("bucket", "object.txt", b"second")
        .expect("second call should succeed");
    let data = disk.read_all("bucket", "object.txt").expect("read_all");
    assert_eq!(data, b"second");
}

#[test]
fn test_naughty_disk_default_error() {
    let temp_dir = tempdir().expect("tempdir");
    let storage = LocalXlStorage::new(temp_dir.path().to_str().expect("path")).expect("storage");
    let disk = NaughtyDisk::new(storage, BTreeMap::new(), Some(ERR_FAULTY_DISK.to_string()));

    let err = disk.make_vol("bucket").expect_err("default error");
    assert_eq!(err, ERR_FAULTY_DISK);
}
