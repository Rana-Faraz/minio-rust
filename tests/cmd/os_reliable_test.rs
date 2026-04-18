use std::fs;

use minio_rust::cmd::{os_mkdir_all, os_rename_all};
use tempfile::tempdir;

pub const SOURCE_FILE: &str = "cmd/os-reliable_test.go";

#[test]
fn test_osmkdir_all_line_25() {
    let dir = tempdir().expect("tempdir");
    let nested = dir.path().join("a").join("b").join("c");

    os_mkdir_all(&nested).expect("mkdir");
    os_mkdir_all(&nested).expect("mkdir should be idempotent");

    assert!(nested.is_dir());
}

#[test]
fn test_osrename_all_line_46() {
    let dir = tempdir().expect("tempdir");
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("nested").join("deep").join("dst.txt");
    fs::write(&src, b"hello").expect("write source");

    os_rename_all(&src, &dst).expect("rename");

    assert!(!src.exists());
    assert_eq!(fs::read(&dst).expect("read destination"), b"hello");
}
