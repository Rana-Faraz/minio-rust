// Rust test snapshot derived from cmd/object_api_suite_test.go.

use std::collections::BTreeMap;
use std::path::PathBuf;

use tempfile::TempDir;

use minio_rust::cmd::{
    get_complete_multipart_md5, get_md5_hash, BucketOptions, CompletePart, LocalObjectLayer,
    MakeBucketOptions, ObjectOptions, PutObjReader, ERR_BUCKET_NOT_FOUND, ERR_FILE_NOT_FOUND,
    ERR_VOLUME_EXISTS,
};

pub const SOURCE_FILE: &str = "cmd/object_api_suite_test.go";

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

fn must_make_bucket(layer: &LocalObjectLayer, bucket: &str) {
    layer
        .make_bucket(bucket, MakeBucketOptions::default())
        .expect("make bucket");
}

fn put_reader(data: &[u8]) -> PutObjReader {
    PutObjReader {
        data: data.to_vec(),
        declared_size: data.len() as i64,
        expected_md5: String::new(),
        expected_sha256: String::new(),
    }
}

fn put_object(layer: &LocalObjectLayer, bucket: &str, object: &str, data: &[u8]) {
    layer
        .put_object(bucket, object, &put_reader(data), ObjectOptions::default())
        .expect("put object");
}

fn extended_smoke(versioning_enabled: bool, user_defined: BTreeMap<String, String>) {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = if versioning_enabled {
        "smoke-versioned"
    } else {
        "smoke-default"
    };
    layer
        .make_bucket(bucket, MakeBucketOptions { versioning_enabled })
        .expect("make bucket");
    let info = layer
        .put_object(
            bucket,
            "object.txt",
            &put_reader(b"smoke"),
            ObjectOptions {
                user_defined,
                ..ObjectOptions::default()
            },
        )
        .expect("put object");
    assert_eq!(info.size, 5);
    assert_eq!(
        layer.get_object(bucket, "object.txt").expect("get object"),
        b"smoke"
    );
}

#[test]
fn test_make_bucket_line_77() {
    let (layer, _dirs) = new_object_layer(4);
    layer
        .make_bucket("bucket-unknown", MakeBucketOptions::default())
        .expect("make bucket");
}

#[test]
fn test_multipart_object_creation_line_90() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    let upload_id = layer
        .new_multipart_upload("bucket", "key", ObjectOptions::default())
        .expect("new multipart upload")
        .upload_id;

    let data = vec![b'a'; 5 * 1024 * 1024];
    let mut completed = Vec::new();
    for part_number in 1..=10 {
        let etag = get_md5_hash(&data);
        let part = layer
            .put_object_part(
                "bucket",
                "key",
                &upload_id,
                part_number,
                &PutObjReader {
                    data: data.clone(),
                    declared_size: data.len() as i64,
                    expected_md5: etag.clone(),
                    expected_sha256: String::new(),
                },
                ObjectOptions::default(),
            )
            .expect("put part");
        assert_eq!(part.etag, etag);
        completed.push(CompletePart { etag, part_number });
    }
    let info = layer
        .complete_multipart_upload(
            "bucket",
            "key",
            &upload_id,
            &completed,
            ObjectOptions::default(),
        )
        .expect("complete multipart upload");
    assert_eq!(info.etag, get_complete_multipart_md5(&completed));
}

#[test]
fn test_multipart_object_abort_line_136() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    let upload_id = layer
        .new_multipart_upload("bucket", "key", ObjectOptions::default())
        .expect("new multipart upload")
        .upload_id;

    for part_number in 1..=10 {
        let payload = format!("payload-{part_number}");
        let md5 = get_md5_hash(payload.as_bytes());
        let part = layer
            .put_object_part(
                "bucket",
                "key",
                &upload_id,
                part_number,
                &PutObjReader {
                    data: payload.as_bytes().to_vec(),
                    declared_size: payload.len() as i64,
                    expected_md5: md5.clone(),
                    expected_sha256: String::new(),
                },
                ObjectOptions::default(),
            )
            .expect("put part");
        assert_eq!(part.etag, md5);
    }
    layer
        .abort_multipart_upload("bucket", "key", &upload_id, ObjectOptions::default())
        .expect("abort multipart upload");
}

#[test]
fn test_multiple_object_creation_line_182() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");

    let objects = [
        ("obj0", b"0123456789".as_slice()),
        ("obj1", b"abcdefghij".as_slice()),
        ("obj2", b"klmnopqrst".as_slice()),
        ("obj3", b"uvwxyz".as_slice()),
    ];
    for (name, data) in objects {
        layer
            .put_object(
                "bucket",
                name,
                &put_reader(data),
                ObjectOptions {
                    user_defined: BTreeMap::from([("etag".to_string(), get_md5_hash(data))]),
                    ..ObjectOptions::default()
                },
            )
            .expect("put object");
    }

    for (name, data) in objects {
        assert_eq!(layer.get_object("bucket", name).expect("get object"), data);
        let info = layer
            .get_object_info("bucket", name)
            .expect("get object info");
        assert_eq!(info.size, data.len() as i64);
    }
}

#[test]
fn test_paging_line_238() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    let empty = layer
        .list_objects("bucket", "", "", "", 0)
        .expect("list empty bucket");
    assert!(empty.objects.is_empty());
    assert!(!empty.is_truncated);

    for i in 0..=10 {
        put_object(&layer, "bucket", &format!("obj{i}"), b"uploadContent");
    }
    put_object(&layer, "bucket", "newPrefix", b"uploadContent");
    put_object(&layer, "bucket", "newPrefix2", b"uploadContent");
    put_object(&layer, "bucket", "this/is/delimited", b"uploadContent");
    put_object(
        &layer,
        "bucket",
        "this/is/also/a/delimited/file",
        b"uploadContent",
    );

    let first_page = layer
        .list_objects("bucket", "obj", "", "", 5)
        .expect("first page");
    assert_eq!(first_page.objects.len(), 5);
    assert!(first_page.is_truncated);

    let ordered = layer
        .list_objects("bucket", "", "", "", 1000)
        .expect("ordered list");
    assert_eq!(ordered.objects[0].name, "newPrefix");
    assert_eq!(ordered.objects[1].name, "newPrefix2");
    assert_eq!(ordered.objects[2].name, "obj0");
    assert_eq!(ordered.objects[3].name, "obj1");
    assert_eq!(ordered.objects[4].name, "obj10");

    let delimited = layer
        .list_objects("bucket", "this/is/", "", "/", 10)
        .expect("delimited list");
    assert_eq!(delimited.objects.len(), 1);
    assert_eq!(delimited.objects[0].name, "this/is/delimited");
    assert_eq!(delimited.prefixes, vec!["this/is/also/"]);

    let root_delimited = layer
        .list_objects("bucket", "", "", "/", 1000)
        .expect("root delimited");
    assert!(root_delimited.prefixes.contains(&"this/".to_string()));

    let marked = layer
        .list_objects("bucket", "", "newPrefix", "", 3)
        .expect("marker list");
    assert_eq!(marked.objects[0].name, "newPrefix2");
    assert_eq!(marked.objects[1].name, "obj0");
    assert_eq!(marked.objects[2].name, "obj1");

    let prefixed = layer
        .list_objects("bucket", "new", "", "", 5)
        .expect("prefixed list");
    assert_eq!(prefixed.objects.len(), 2);
    assert_eq!(prefixed.objects[0].name, "newPrefix");
    assert_eq!(prefixed.objects[1].name, "newPrefix2");

    let mut expected = BTreeMap::new();
    for name in [
        "testPrefix/aaa/objaaa",
        "testPrefix/bbb/objbbb",
        "testPrefix/ccc/objccc",
        "testPrefix/ddd/objddd",
        "testPrefix/eee/objeee",
        "testPrefix/fff/objfff",
        "testPrefix/ggg/objggg",
    ] {
        expected.insert(name.to_string(), 1_i32);
        put_object(&layer, "bucket", name, b"uploadContent");
    }
    let mut token = String::new();
    loop {
        let page = layer
            .list_objects_v2("bucket", "testPrefix", &token, "", 2, false, "")
            .expect("list objects v2");
        for object in page.objects {
            *expected.get_mut(&object.name).expect("expected object") -= 1;
        }
        if page.next_continuation_token.is_empty() {
            break;
        }
        token = page.next_continuation_token;
    }
    assert!(expected.values().all(|count| *count == 0));
}

#[test]
fn test_object_overwrite_works_line_481() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    put_object(
        &layer,
        "bucket",
        "object",
        b"The list of parts was not in ascending order.",
    );
    let replacement = b"The specified multipart upload does not exist.";
    put_object(&layer, "bucket", "object", replacement);
    assert_eq!(
        layer.get_object("bucket", "object").expect("get object"),
        replacement
    );
}

#[test]
fn test_non_existent_bucket_operations_line_518() {
    let (layer, _dirs) = new_object_layer(4);
    assert_eq!(
        layer
            .put_object(
                "bucket1",
                "object",
                &put_reader(b"one"),
                ObjectOptions::default(),
            )
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NOT_FOUND)
    );
}

#[test]
fn test_bucket_recreate_fails_line_535() {
    let (layer, _dirs) = new_object_layer(4);
    layer
        .make_bucket("string", MakeBucketOptions::default())
        .expect("make bucket");
    assert_eq!(
        layer
            .make_bucket("string", MakeBucketOptions::default())
            .err()
            .as_deref(),
        Some(ERR_VOLUME_EXISTS)
    );
}

#[test]
fn subtest_file_scope_default_line_600() {
    extended_smoke(false, BTreeMap::new());
}

#[test]
fn subtest_file_scope_default_versioned_line_603() {
    extended_smoke(true, BTreeMap::new());
}

#[test]
fn subtest_file_scope_compressed_line_607() {
    extended_smoke(
        false,
        BTreeMap::from([(
            "X-Minio-Internal-compression".to_string(),
            "klauspost/compress/s2".to_string(),
        )]),
    );
}

#[test]
fn subtest_file_scope_compressed_versioned_line_613() {
    extended_smoke(
        true,
        BTreeMap::from([(
            "X-Minio-Internal-compression".to_string(),
            "klauspost/compress/s2".to_string(),
        )]),
    );
}

#[test]
fn subtest_file_scope_encrypted_line_622() {
    extended_smoke(
        false,
        BTreeMap::from([(
            "X-Amz-Server-Side-Encryption".to_string(),
            "AES256".to_string(),
        )]),
    );
}

#[test]
fn subtest_file_scope_encrypted_versioned_line_628() {
    extended_smoke(
        true,
        BTreeMap::from([(
            "X-Amz-Server-Side-Encryption".to_string(),
            "AES256".to_string(),
        )]),
    );
}

#[test]
fn subtest_file_scope_compressed_encrypted_line_637() {
    extended_smoke(
        false,
        BTreeMap::from([
            (
                "X-Minio-Internal-compression".to_string(),
                "klauspost/compress/s2".to_string(),
            ),
            (
                "X-Amz-Server-Side-Encryption".to_string(),
                "AES256".to_string(),
            ),
        ]),
    );
}

#[test]
fn subtest_file_scope_compressed_encrypted_versioned_line_643() {
    extended_smoke(
        true,
        BTreeMap::from([
            (
                "X-Minio-Internal-compression".to_string(),
                "klauspost/compress/s2".to_string(),
            ),
            (
                "X-Amz-Server-Side-Encryption".to_string(),
                "AES256".to_string(),
            ),
        ]),
    );
}

#[test]
fn test_put_object_line_662() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    let content = b"testcontent";
    put_object(&layer, "bucket", "object", content);
    assert_eq!(
        layer.get_object("bucket", "object").expect("get object"),
        content
    );
    put_object(&layer, "bucket", "object", content);
    assert_eq!(
        layer.get_object("bucket", "object").expect("get object"),
        content
    );
}

#[test]
fn test_put_object_in_subdir_line_706() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    let content = b"The specified multipart upload does not exist.";
    put_object(&layer, "bucket", "dir1/dir2/object", content);
    assert_eq!(
        layer
            .get_object("bucket", "dir1/dir2/object")
            .expect("get subdir object"),
        content
    );
}

#[test]
fn test_list_buckets_line_738() {
    let (layer, _dirs) = new_object_layer(4);
    assert_eq!(
        layer
            .list_buckets(BucketOptions::default())
            .expect("list empty buckets")
            .len(),
        0
    );
    must_make_bucket(&layer, "bucket1");
    assert_eq!(
        layer
            .list_buckets(BucketOptions::default())
            .expect("list one bucket")
            .len(),
        1
    );
    must_make_bucket(&layer, "bucket2");
    must_make_bucket(&layer, "bucket22");
    assert_eq!(
        layer
            .list_buckets(BucketOptions::default())
            .expect("list buckets")
            .len(),
        3
    );
}

#[test]
fn test_list_buckets_order_line_797() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket1");
    must_make_bucket(&layer, "bucket2");
    let buckets = layer
        .list_buckets(BucketOptions::default())
        .expect("list buckets");
    assert_eq!(buckets[0].name, "bucket1");
    assert_eq!(buckets[1].name, "bucket2");
}

#[test]
fn test_list_objects_tests_for_non_existent_bucket_line_831() {
    let (layer, _dirs) = new_object_layer(4);
    let result = layer.list_objects("bucket", "", "", "", 1000);
    assert_eq!(
        result.as_ref().err().map(String::as_str),
        Some(ERR_BUCKET_NOT_FOUND)
    );
    assert_eq!(result.err(), Some(ERR_BUCKET_NOT_FOUND.to_string()));
}

#[test]
fn test_non_existent_object_in_bucket_line_853() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    assert_eq!(
        layer.get_object_info("bucket", "dir1").err().as_deref(),
        Some(ERR_FILE_NOT_FOUND)
    );
}

#[test]
fn test_get_directory_returns_object_not_found_line_880() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    put_object(&layer, "bucket", "dir1/dir3/object", b"content");
    assert_eq!(
        layer.get_object_info("bucket", "dir1/").err().as_deref(),
        Some(ERR_FILE_NOT_FOUND)
    );
    assert_eq!(
        layer
            .get_object_info("bucket", "dir1/dir3/")
            .err()
            .as_deref(),
        Some(ERR_FILE_NOT_FOUND)
    );
}

#[test]
fn test_content_type_line_922() {
    let (layer, _dirs) = new_object_layer(4);
    must_make_bucket(&layer, "bucket");
    put_object(&layer, "bucket", "minio.png", b"png-content");
    let info = layer
        .get_object_info("bucket", "minio.png")
        .expect("get object info");
    assert_eq!(info.content_type, "image/png");
}
