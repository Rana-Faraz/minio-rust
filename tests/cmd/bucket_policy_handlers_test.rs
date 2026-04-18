use std::path::PathBuf;

use tempfile::TempDir;

use minio_rust::cmd::{BucketPolicyHandlers, HandlerCredentials, LocalObjectLayer, RequestAuth};

pub const SOURCE_FILE: &str = "cmd/bucket-policy-handlers_test.go";

fn new_handlers(disk_count: usize) -> (BucketPolicyHandlers, Vec<TempDir>, HandlerCredentials) {
    let temp_dirs: Vec<TempDir> = (0..disk_count)
        .map(|_| tempfile::tempdir().expect("create tempdir"))
        .collect();
    let disks: Vec<PathBuf> = temp_dirs
        .iter()
        .map(|dir| dir.path().to_path_buf())
        .collect();
    let credentials = HandlerCredentials::new("minioadmin", "minioadmin");
    (
        BucketPolicyHandlers::new(LocalObjectLayer::new(disks), credentials.clone()),
        temp_dirs,
        credentials,
    )
}

fn auth_v4(credentials: &HandlerCredentials) -> RequestAuth {
    RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key)
}

#[test]
fn test_create_bucket_line_109() {
    let (handlers, _dirs, _credentials) = new_handlers(4);
    handlers
        .create_bucket("policybucket")
        .expect("create bucket");
    assert!(handlers
        .layer()
        .bucket_exists("policybucket")
        .expect("bucket exists"));
}

#[test]
fn test_put_bucket_policy_handler_line_156() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    handlers
        .create_bucket("policybucket")
        .expect("create bucket");
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::policybucket/*"]}]}"#;

    let response = handlers.put_bucket_policy("policybucket", &auth_v4(&credentials), policy);
    assert_eq!(response.status, 204);

    let bad = handlers.put_bucket_policy("policybucket", &RequestAuth::anonymous(), policy);
    assert_eq!(bad.status, 403);
}

#[test]
fn test_get_bucket_policy_handler_line_375() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    handlers
        .create_bucket("policybucket")
        .expect("create bucket");
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::policybucket/*"]}]}"#;
    handlers.put_bucket_policy("policybucket", &auth_v4(&credentials), policy);

    let response = handlers.get_bucket_policy("policybucket", &auth_v4(&credentials));
    assert_eq!(response.status, 200);
    assert_eq!(std::str::from_utf8(&response.body).expect("json"), policy);
}

#[test]
fn test_delete_bucket_policy_handler_line_579() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    handlers
        .create_bucket("policybucket")
        .expect("create bucket");
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::policybucket/*"]}]}"#;
    handlers.put_bucket_policy("policybucket", &auth_v4(&credentials), policy);

    let delete = handlers.delete_bucket_policy("policybucket", &auth_v4(&credentials));
    assert_eq!(delete.status, 204);

    let missing = handlers.get_bucket_policy("policybucket", &auth_v4(&credentials));
    assert_eq!(missing.status, 404);
}
