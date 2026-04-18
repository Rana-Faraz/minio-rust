use std::path::PathBuf;

use roxmltree::Document;
use tempfile::TempDir;

use minio_rust::cmd::{
    HandlerCredentials, LocalObjectLayer, MakeBucketOptions, ObjectApiHandlers, ObjectOptions,
    PutObjReader, RequestAuth,
};

pub const SOURCE_FILE: &str = "cmd/bucket-handlers_test.go";

fn new_handlers(disk_count: usize) -> (ObjectApiHandlers, Vec<TempDir>, HandlerCredentials) {
    let temp_dirs: Vec<TempDir> = (0..disk_count)
        .map(|_| tempfile::tempdir().expect("create tempdir"))
        .collect();
    let disks: Vec<PathBuf> = temp_dirs
        .iter()
        .map(|dir| dir.path().to_path_buf())
        .collect();
    let credentials = HandlerCredentials::new("minioadmin", "minioadmin");
    (
        ObjectApiHandlers::new(LocalObjectLayer::new(disks), credentials.clone()),
        temp_dirs,
        credentials,
    )
}

fn must_make_bucket(layer: &LocalObjectLayer, bucket: &str) {
    layer
        .make_bucket(bucket, MakeBucketOptions::default())
        .expect("make bucket");
}

fn put_object(layer: &LocalObjectLayer, bucket: &str, object: &str, data: &[u8]) {
    layer
        .put_object(
            bucket,
            object,
            &PutObjReader {
                data: data.to_vec(),
                declared_size: data.len() as i64,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put object");
}

fn auth_v4(credentials: &HandlerCredentials) -> RequestAuth {
    RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key)
}

fn auth_v2(credentials: &HandlerCredentials) -> RequestAuth {
    RequestAuth::signed_v2(&credentials.access_key, &credentials.secret_key)
}

fn invalid_auth() -> RequestAuth {
    RequestAuth::signed_v4("abcd", "abcd")
}

fn xml_tag_text(body: &[u8], tag: &str) -> String {
    let xml = std::str::from_utf8(body).expect("xml utf8");
    let doc = Document::parse(xml).expect("parse xml");
    doc.descendants()
        .find(|node| node.has_tag_name(tag))
        .and_then(|node| node.text())
        .unwrap_or_default()
        .to_string()
}

fn xml_tag_values(body: &[u8], tag: &str) -> Vec<String> {
    let xml = std::str::from_utf8(body).expect("xml utf8");
    let doc = Document::parse(xml).expect("parse xml");
    doc.descendants()
        .filter(|node| node.has_tag_name(tag))
        .filter_map(|node| node.text())
        .map(ToString::to_string)
        .collect()
}

#[test]
fn test_remove_bucket_handler_line_34() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "remove-bucket";
    must_make_bucket(layer, bucket);
    put_object(layer, bucket, "test-object", b"");

    let response = handlers.remove_bucket(bucket, &auth_v4(&credentials));
    assert_eq!(response.status, 409);
    assert_eq!(xml_tag_text(&response.body, "Code"), "BucketNotEmpty");

    let response_v2 = handlers.remove_bucket(bucket, &auth_v2(&credentials));
    assert_eq!(response_v2.status, 409);

    layer
        .delete_object(bucket, "test-object", ObjectOptions::default())
        .expect("delete object");
    let removed = handlers.remove_bucket(bucket, &auth_v4(&credentials));
    assert_eq!(removed.status, 204);
}

#[test]
fn test_get_bucket_location_handler_line_80() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "location-bucket";
    must_make_bucket(layer, bucket);

    let response = handlers.get_bucket_location(bucket, &auth_v4(&credentials));
    assert_eq!(response.status, 200);
    assert_eq!(xml_tag_text(&response.body, "LocationConstraint"), "");

    let response_v2 = handlers.get_bucket_location(bucket, &auth_v2(&credentials));
    assert_eq!(response_v2.status, 200);

    let invalid = handlers.get_bucket_location(bucket, &invalid_auth());
    assert_eq!(invalid.status, 403);
    assert_eq!(xml_tag_text(&invalid.body, "Code"), "InvalidAccessKeyId");
}

#[test]
fn test_head_bucket_handler_line_221() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "head-bucket";
    must_make_bucket(layer, bucket);

    assert_eq!(
        handlers.head_bucket(bucket, &auth_v4(&credentials)).status,
        200
    );
    assert_eq!(
        handlers
            .head_bucket("missing-bucket", &auth_v4(&credentials))
            .status,
        404
    );
    assert_eq!(handlers.head_bucket(bucket, &invalid_auth()).status, 403);
    assert_eq!(
        handlers.head_bucket(bucket, &auth_v2(&credentials)).status,
        200
    );
}

#[test]
fn test_list_multipart_uploads_handler_line_322() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "multipart-bucket";
    must_make_bucket(layer, bucket);

    let upload = layer
        .new_multipart_upload(bucket, "asia/europe/object", ObjectOptions::default())
        .expect("new multipart");
    layer
        .put_object_part(
            bucket,
            "asia/europe/object",
            &upload.upload_id,
            1,
            &PutObjReader {
                data: b"hello".to_vec(),
                declared_size: 5,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put part");

    let invalid_bucket =
        handlers.list_multipart_uploads(".test", "", "", "", "", "0", &auth_v4(&credentials));
    assert_eq!(invalid_bucket.status, 400);

    let missing_bucket = handlers.list_multipart_uploads(
        "volatile-bucket-1",
        "",
        "",
        "",
        "",
        "0",
        &auth_v4(&credentials),
    );
    assert_eq!(missing_bucket.status, 404);

    let unsupported_delimiter =
        handlers.list_multipart_uploads(bucket, "", "", "", "-", "0", &auth_v4(&credentials));
    assert_eq!(unsupported_delimiter.status, 200);
    assert!(xml_tag_values(&unsupported_delimiter.body, "Key").is_empty());

    let invalid_combo = handlers.list_multipart_uploads(
        bucket,
        "asia",
        "europe-object",
        "",
        "",
        "0",
        &auth_v4(&credentials),
    );
    assert_eq!(invalid_combo.status, 501);

    let negative_max =
        handlers.list_multipart_uploads(bucket, "", "", "", "", "-1", &auth_v4(&credentials));
    assert_eq!(negative_max.status, 400);

    let success =
        handlers.list_multipart_uploads(bucket, "", "", "", "/", "100", &auth_v4(&credentials));
    assert_eq!(success.status, 200);
    assert_eq!(
        xml_tag_values(&success.body, "Key"),
        vec!["asia/europe/object".to_string()]
    );

    let success_v2 =
        handlers.list_multipart_uploads(bucket, "", "", "", "", "100", &auth_v2(&credentials));
    assert_eq!(success_v2.status, 200);

    let invalid_creds =
        handlers.list_multipart_uploads(bucket, "", "", "", "", "100", &invalid_auth());
    assert_eq!(invalid_creds.status, 403);
}

#[test]
fn test_list_buckets_handler_line_558() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    must_make_bucket(layer, "alpha-bucket");
    must_make_bucket(layer, "zeta-bucket");

    let response = handlers.list_buckets(&auth_v4(&credentials));
    assert_eq!(response.status, 200);
    let names = xml_tag_values(&response.body, "Name");
    assert_eq!(
        names,
        vec!["alpha-bucket".to_string(), "zeta-bucket".to_string()]
    );

    let response_v2 = handlers.list_buckets(&auth_v2(&credentials));
    assert_eq!(response_v2.status, 200);

    let invalid = handlers.list_buckets(&invalid_auth());
    assert_eq!(invalid.status, 403);
    assert_eq!(xml_tag_text(&invalid.body, "Code"), "InvalidAccessKeyId");
}

#[test]
fn test_apidelete_multiple_objects_handler_line_649() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    let layer = handlers.layer().expect("layer");
    let bucket = "delete-many-bucket";
    must_make_bucket(layer, bucket);

    for name in [
        "test-object-0",
        "test-object-1",
        "test-object-2",
        "public/object",
    ] {
        put_object(layer, bucket, name, b"hello");
    }

    let response = handlers.delete_multiple_objects(
        bucket,
        &[
            "test-object-0".to_string(),
            "test-object-1".to_string(),
            "missing-object".to_string(),
            "bad//name".to_string(),
        ],
        false,
        &auth_v4(&credentials),
    );
    assert_eq!(response.status, 200);
    assert_eq!(
        xml_tag_values(&response.body, "Key"),
        vec![
            "test-object-0".to_string(),
            "test-object-1".to_string(),
            "missing-object".to_string(),
            "bad//name".to_string(),
        ]
    );
    assert_eq!(
        xml_tag_values(&response.body, "Code"),
        vec!["InvalidObjectName".to_string()]
    );

    let quiet = handlers.delete_multiple_objects(
        bucket,
        &["test-object-2".to_string(), "public/object".to_string()],
        true,
        &auth_v2(&credentials),
    );
    assert_eq!(quiet.status, 200);
    assert!(xml_tag_values(&quiet.body, "Deleted").is_empty());

    let invalid =
        handlers.delete_multiple_objects(bucket, &["anything".to_string()], false, &invalid_auth());
    assert_eq!(invalid.status, 403);
    assert_eq!(xml_tag_text(&invalid.body, "Code"), "InvalidAccessKeyId");

    let remaining = layer
        .list_objects(bucket, "", "", "", 100)
        .expect("list objects after delete");
    assert!(remaining.objects.is_empty());
}
