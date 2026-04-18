use std::collections::BTreeMap;

use minio_rust::cmd::{extract_metadata_headers, get_resource, is_valid_location_constraint};

pub const SOURCE_FILE: &str = "cmd/handler-utils_test.go";

#[test]
fn test_is_valid_location_constraint_line_35() {
    assert!(is_valid_location_constraint("", "us-east-1"));
    assert!(is_valid_location_constraint("us-east-1", "us-east-1"));
    assert!(!is_valid_location_constraint("US-EAST-1", "us-east-1"));
    assert!(!is_valid_location_constraint("eu-west-1", "us-east-1"));
}

#[test]
fn test_extract_metadata_headers_line_99() {
    let headers = BTreeMap::from([
        (
            "Content-Type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("X-Amz-Meta-Name".to_string(), "photo.jpg".to_string()),
        ("x-amz-meta-extension".to_string(), "jpg".to_string()),
        ("X-Minio-Internal-test".to_string(), "skip".to_string()),
    ]);

    assert_eq!(
        extract_metadata_headers(&headers),
        BTreeMap::from([
            ("x-amz-meta-extension".to_string(), "jpg".to_string()),
            ("x-amz-meta-name".to_string(), "photo.jpg".to_string()),
        ])
    );
}

#[test]
fn test_get_resource_line_180() {
    assert_eq!(
        get_resource("/bucket/object", "localhost:9000", &[]),
        "/bucket/object"
    );
    assert_eq!(
        get_resource(
            "/object",
            "bucket.s3.example.com",
            &[String::from("s3.example.com")]
        ),
        "/bucket/object"
    );
    assert_eq!(
        get_resource(
            "object",
            "photos.s3.example.com:9000",
            &[String::from("s3.example.com")]
        ),
        "/photos/object"
    );
}
