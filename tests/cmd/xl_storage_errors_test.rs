use std::io::ErrorKind;

use minio_rust::cmd::{
    map_storage_error_kind, map_storage_error_message, ERR_DISK_NOT_FOUND, ERR_FILE_ACCESS_DENIED,
    ERR_FILE_NAME_TOO_LONG, ERR_FILE_NOT_FOUND, ERR_IS_NOT_REGULAR, ERR_PATH_NOT_FOUND,
};

pub const SOURCE_FILE: &str = "cmd/xl-storage-errors_test.go";

#[test]
fn test_sys_errors_line_27() {
    assert_eq!(
        map_storage_error_kind(ErrorKind::NotFound),
        ERR_FILE_NOT_FOUND
    );
    assert_eq!(
        map_storage_error_kind(ErrorKind::PermissionDenied),
        ERR_FILE_ACCESS_DENIED
    );
    assert_eq!(
        map_storage_error_kind(ErrorKind::IsADirectory),
        ERR_IS_NOT_REGULAR
    );

    assert_eq!(
        map_storage_error_message("file name too long"),
        ERR_FILE_NAME_TOO_LONG
    );
    assert_eq!(
        map_storage_error_message("permission denied"),
        ERR_FILE_ACCESS_DENIED
    );
    assert_eq!(
        map_storage_error_message("path not found"),
        ERR_PATH_NOT_FOUND
    );
    assert_eq!(
        map_storage_error_message("weird disk failure"),
        ERR_DISK_NOT_FOUND
    );
}
