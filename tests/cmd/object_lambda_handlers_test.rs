use std::path::PathBuf;

use tempfile::TempDir;

use minio_rust::cmd::LocalObjectLayer;
use minio_rust::cmd::{
    HandlerCredentials, MakeBucketOptions, ObjectLambdaHandlers, ObjectOptions, PutObjReader,
    RequestAuth,
};

pub const SOURCE_FILE: &str = "cmd/object-lambda-handlers_test.go";

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
fn test_get_object_lambda_handler_line_41() {
    let (layer, _dirs) = new_object_layer(4);
    layer
        .make_bucket("lambda-bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "lambda-bucket",
            "greeting.txt",
            &PutObjReader {
                data: b"hello lambda".to_vec(),
                declared_size: 12,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put object");

    let handlers =
        ObjectLambdaHandlers::new(layer, HandlerCredentials::new("minioadmin", "minioadmin"));
    let auth = RequestAuth::signed_v4("minioadmin", "minioadmin");

    let plain = handlers.get_object_lambda("lambda-bucket", "greeting.txt", &auth, None);
    assert_eq!(plain.status, 200);
    assert_eq!(plain.body, b"hello lambda");

    let uppercase =
        handlers.get_object_lambda("lambda-bucket", "greeting.txt", &auth, Some("uppercase"));
    assert_eq!(uppercase.status, 200);
    assert_eq!(uppercase.body, b"HELLO LAMBDA");
}

#[test]
fn subtest_test_get_object_lambda_handler_tc_name_line_73() {
    let (layer, _dirs) = new_object_layer(4);
    layer
        .make_bucket("lambda-bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "lambda-bucket",
            "greeting.txt",
            &PutObjReader {
                data: b"hello".to_vec(),
                declared_size: 5,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put object");

    let handlers =
        ObjectLambdaHandlers::new(layer, HandlerCredentials::new("minioadmin", "minioadmin"));
    let auth = RequestAuth::signed_v4("minioadmin", "minioadmin");

    let cases = [
        ("reverse", 200u16, b"olleh".to_vec()),
        (
            "unknown",
            400u16,
            b"unsupported object lambda transform".to_vec(),
        ),
    ];

    for (transform, status, body) in cases {
        let response =
            handlers.get_object_lambda("lambda-bucket", "greeting.txt", &auth, Some(transform));
        assert_eq!(response.status, status, "{transform}");
        assert_eq!(response.body, body, "{transform}");
    }

    let not_found =
        handlers.get_object_lambda("lambda-bucket", "missing.txt", &auth, Some("identity"));
    assert_eq!(not_found.status, 404);

    let denied = handlers.get_object_lambda(
        "lambda-bucket",
        "greeting.txt",
        &RequestAuth::anonymous(),
        Some("identity"),
    );
    assert_eq!(denied.status, 403);
}
