use std::collections::BTreeMap;
use std::path::PathBuf;

use tempfile::TempDir;

use minio_rust::cmd::{LocalObjectLayer, MakeBucketOptions, PostPolicyHandlers, MINIO_META_BUCKET};

pub const SOURCE_FILE: &str = "cmd/post-policy_test.go";

fn new_object_layer(disk_count: usize) -> (LocalObjectLayer, Vec<TempDir>) {
    let temp_dirs: Vec<TempDir> = (0..disk_count)
        .map(|_| tempfile::tempdir().expect("create tempdir"))
        .collect();
    let disks: Vec<PathBuf> = temp_dirs
        .iter()
        .map(|dir| dir.path().to_path_buf())
        .collect();
    (LocalObjectLayer::new(disks), temp_dirs)
}

#[test]
fn test_post_policy_reserved_bucket_exploit_line_124() {
    let (layer, _dirs) = new_object_layer(4);
    let handlers = PostPolicyHandlers::new(layer);
    let mut fields = BTreeMap::new();
    fields.insert("key".to_string(), "exploit.txt".to_string());

    let response = handlers.post_policy_bucket_handler(
        MINIO_META_BUCKET,
        r#"{"expiration":"2026-12-30T12:00:00.000Z","conditions":[{"bucket":".minio.sys"}]}"#,
        &fields,
        b"exploit",
    );
    assert_eq!(response.status, 403);
}

#[test]
fn test_post_policy_bucket_handler_line_183() {
    let (layer, _dirs) = new_object_layer(4);
    layer
        .make_bucket("uploads", MakeBucketOptions::default())
        .expect("make bucket");
    let handlers = PostPolicyHandlers::new(layer);
    let mut fields = BTreeMap::new();
    fields.insert("key".to_string(), "photos/image.jpg".to_string());
    fields.insert(
        "x-amz-algorithm".to_string(),
        "AWS4-HMAC-SHA256".to_string(),
    );

    let response = handlers.post_policy_bucket_handler(
        "uploads",
        r#"{
            "expiration":"2026-12-30T12:00:00.000Z",
            "conditions":[
                {"bucket":"uploads"},
                ["starts-with","$key","photos/"],
                ["eq","$x-amz-algorithm","AWS4-HMAC-SHA256"],
                ["content-length-range",1,20]
            ]
        }"#,
        &fields,
        b"image-bytes",
    );
    assert_eq!(response.status, 204);
}

#[test]
fn test_post_policy_bucket_handler_redirect_line_510() {
    let (layer, _dirs) = new_object_layer(4);
    layer
        .make_bucket("uploads", MakeBucketOptions::default())
        .expect("make bucket");
    let handlers = PostPolicyHandlers::new(layer);
    let mut fields = BTreeMap::new();
    fields.insert("key".to_string(), "photos/image.jpg".to_string());
    fields.insert(
        "success_action_redirect".to_string(),
        "https://example.test/complete".to_string(),
    );

    let response = handlers.post_policy_bucket_handler(
        "uploads",
        r#"{
            "expiration":"2026-12-30T12:00:00.000Z",
            "conditions":[
                {"bucket":"uploads"},
                ["starts-with","$key","photos/"],
                ["content-length-range",1,20]
            ]
        }"#,
        &fields,
        b"image-bytes",
    );
    assert_eq!(response.status, 303);
    assert_eq!(
        response.headers.get("location").map(String::as_str),
        Some("https://example.test/complete?bucket=uploads&key=photos%2Fimage.jpg")
    );
}
