use std::io::ErrorKind;

use crate::cmd::{
    ERR_DISK_NOT_FOUND, ERR_FILE_ACCESS_DENIED, ERR_FILE_NAME_TOO_LONG, ERR_FILE_NOT_FOUND,
    ERR_IS_NOT_REGULAR, ERR_PATH_NOT_FOUND,
};

pub fn map_storage_error_kind(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::NotFound => ERR_FILE_NOT_FOUND,
        ErrorKind::PermissionDenied => ERR_FILE_ACCESS_DENIED,
        ErrorKind::IsADirectory => ERR_IS_NOT_REGULAR,
        ErrorKind::DirectoryNotEmpty => ERR_PATH_NOT_FOUND,
        _ => ERR_DISK_NOT_FOUND,
    }
}

pub fn map_storage_error_message(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("path not found") {
        ERR_PATH_NOT_FOUND
    } else if lower.contains("file name too long") || lower.contains("filename too long") {
        ERR_FILE_NAME_TOO_LONG
    } else if lower.contains("not found") || lower.contains("no such file") {
        ERR_FILE_NOT_FOUND
    } else if lower.contains("permission denied") {
        ERR_FILE_ACCESS_DENIED
    } else if lower.contains("not a regular file") || lower.contains("is a directory") {
        ERR_IS_NOT_REGULAR
    } else {
        ERR_DISK_NOT_FOUND
    }
}
