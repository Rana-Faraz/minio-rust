use minio_rust::cmd::{apply_umask_to_file_mode, apply_umask_to_volume_mode};

pub const SOURCE_FILE: &str = "cmd/xl-storage_unix_test.go";

#[test]
fn test_is_valid_umask_vol_line_39() {
    assert_eq!(apply_umask_to_volume_mode(0o777, 0o022), 0o755);
    assert_eq!(apply_umask_to_volume_mode(0o770, 0o027), 0o750);
}

#[test]
fn test_is_valid_umask_file_line_77() {
    assert_eq!(apply_umask_to_file_mode(0o666, 0o022), 0o644);
    assert_eq!(apply_umask_to_file_mode(0o640, 0o027), 0o640);
}
