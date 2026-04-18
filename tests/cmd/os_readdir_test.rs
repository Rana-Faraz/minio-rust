use std::fs;

use minio_rust::cmd::{read_dir, read_dir_n, ERR_FILE_NOT_FOUND};
use tempfile::tempdir;

pub const SOURCE_FILE: &str = "cmd/os-readdir_test.go";

#[test]
fn test_read_dir_fail_line_31() {
    let dir = tempdir().expect("tempdir");
    let missing = dir.path().join("missing");
    let err = read_dir(&missing).expect_err("missing directory must fail");
    assert_eq!(err, ERR_FILE_NOT_FOUND);
}

#[test]
fn test_read_dir_line_185() {
    let dir = tempdir().expect("tempdir");
    fs::write(dir.path().join("zeta.txt"), b"zeta").expect("write file");
    fs::write(dir.path().join("alpha.txt"), b"alpha").expect("write file");
    fs::create_dir(dir.path().join("nested")).expect("create dir");

    let entries = read_dir(dir.path()).expect("read dir");
    assert_eq!(entries, vec!["alpha.txt", "nested/", "zeta.txt"]);
}

#[test]
fn test_read_dir_n_line_214() {
    let dir = tempdir().expect("tempdir");
    for name in ["delta", "alpha", "charlie", "bravo"] {
        fs::write(dir.path().join(name), name.as_bytes()).expect("write file");
    }

    let entries = read_dir_n(dir.path(), 2).expect("read dir");
    assert_eq!(entries, vec!["alpha", "bravo"]);

    let all_entries = read_dir_n(dir.path(), 32).expect("read full dir");
    assert_eq!(all_entries, vec!["alpha", "bravo", "charlie", "delta"]);
}
