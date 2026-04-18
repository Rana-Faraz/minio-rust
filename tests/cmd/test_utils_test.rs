// Rust test snapshot derived from cmd/test-utils_test.go.

use minio_rust::cmd::{to_api_error, to_object_err, to_storage_err, ApiErrorCode, BadDigest};

pub const SOURCE_FILE: &str = "cmd/test-utils_test.go";

#[test]
fn test_main_test_main_line_73() {
    let object_err: Option<String> = to_object_err(Some("object failure".to_string()));
    let storage_err: Option<String> = to_storage_err(Some("storage failure".to_string()));
    let digest = BadDigest;

    assert_eq!(object_err.as_deref(), Some("object failure"));
    assert_eq!(storage_err.as_deref(), Some("storage failure"));
    assert_eq!(to_api_error(Some(&digest)), ApiErrorCode::BadDigest);
}

#[test]
fn test_to_err_is_nil_line_2270() {
    let object_err: Option<String> = to_object_err(None);
    let storage_err: Option<String> = to_storage_err(None);

    assert!(object_err.is_none());
    assert!(storage_err.is_none());
    assert_eq!(to_api_error(None), ApiErrorCode::None);
}
