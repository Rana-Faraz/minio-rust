use std::path::PathBuf;

use roxmltree::Document;
use tempfile::TempDir;

use minio_rust::cmd::{
    BucketLifecycleHandlers, HandlerCredentials, LocalObjectLayer, MakeBucketOptions, RequestAuth,
};

pub const SOURCE_FILE: &str = "cmd/bucket-lifecycle-handlers_test.go";

fn new_handlers(disk_count: usize) -> (BucketLifecycleHandlers, Vec<TempDir>, HandlerCredentials) {
    let temp_dirs: Vec<TempDir> = (0..disk_count)
        .map(|_| tempfile::tempdir().expect("create tempdir"))
        .collect();
    let disks: Vec<PathBuf> = temp_dirs
        .iter()
        .map(|dir| dir.path().to_path_buf())
        .collect();
    let credentials = HandlerCredentials::new("minioadmin", "minioadmin");
    (
        BucketLifecycleHandlers::new(LocalObjectLayer::new(disks), credentials.clone()),
        temp_dirs,
        credentials,
    )
}

fn auth_v4(credentials: &HandlerCredentials) -> RequestAuth {
    RequestAuth::signed_v4(&credentials.access_key, &credentials.secret_key)
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

#[test]
fn test_bucket_lifecycle_wrong_credentials_line_31() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    handlers
        .layer()
        .make_bucket("bucketlife", MakeBucketOptions::default())
        .expect("make bucket");
    let xml = r#"<LifecycleConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#;

    let response = handlers.put_bucket_lifecycle("bucketlife", &invalid_auth(), xml);
    assert_eq!(response.status, 403);
    assert_eq!(xml_tag_text(&response.body, "Code"), "AccessDenied");

    let ok = handlers.put_bucket_lifecycle("bucketlife", &auth_v4(&credentials), xml);
    assert_eq!(ok.status, 200);
}

#[test]
fn test_bucket_lifecycle_line_147() {
    let (handlers, _dirs, credentials) = new_handlers(4);
    handlers
        .layer()
        .make_bucket("bucketlife", MakeBucketOptions::default())
        .expect("make bucket");
    let xml = r#"<LifecycleConfiguration><Rule><ID>rule1</ID><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>"#;

    let put = handlers.put_bucket_lifecycle("bucketlife", &auth_v4(&credentials), xml);
    assert_eq!(put.status, 200);

    let get = handlers.get_bucket_lifecycle("bucketlife", &auth_v4(&credentials));
    assert_eq!(get.status, 200);
    assert_eq!(std::str::from_utf8(&get.body).expect("xml"), xml);

    let delete = handlers.delete_bucket_lifecycle("bucketlife", &auth_v4(&credentials));
    assert_eq!(delete.status, 204);

    let missing = handlers.get_bucket_lifecycle("bucketlife", &auth_v4(&credentials));
    assert_eq!(missing.status, 404);
    assert_eq!(
        xml_tag_text(&missing.body, "Code"),
        "NoSuchLifecycleConfiguration"
    );
}
