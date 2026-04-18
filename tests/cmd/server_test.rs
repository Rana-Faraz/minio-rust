// Rust test snapshot derived from cmd/server_test.go.

use tempfile::tempdir;

use minio_rust::cmd::{
    new_object_layer, HandlerCredentials, LocalObjectLayer, MakeBucketOptions, ObjectApiHandlers,
    ObjectOptions, PutObjReader, RequestAuth,
};

pub const SOURCE_FILE: &str = "cmd/server_test.go";

fn make_layer(disk_count: usize) -> LocalObjectLayer {
    let mut disks = Vec::new();
    let temp_root = tempdir().expect("temp root");
    for index in 0..disk_count {
        let disk = temp_root.path().join(format!("disk-{index}"));
        std::fs::create_dir_all(&disk).expect("create disk");
        disks.push(disk);
    }
    let layer = new_object_layer(disks).expect("new object layer");
    std::mem::forget(temp_root);
    layer
}

#[test]
fn test_server_suite_line_134() {
    let layer = make_layer(4);
    layer
        .make_bucket("server-bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "server-bucket",
            "dir/hello.txt",
            &PutObjReader {
                data: b"hello from rust".to_vec(),
                declared_size: 15,
                ..PutObjReader::default()
            },
            ObjectOptions::default(),
        )
        .expect("put object");

    let list_objects = layer
        .list_objects("server-bucket", "", "", "/", 1000)
        .expect("list objects");
    assert_eq!(list_objects.prefixes, vec!["dir/".to_string()]);

    let handlers =
        ObjectApiHandlers::new(layer, HandlerCredentials::new("minioadmin", "minioadmin"));
    let auth = RequestAuth::signed_v4("minioadmin", "minioadmin");

    let list_buckets = handlers.list_buckets(&auth);
    assert_eq!(list_buckets.status, 200);
    assert!(String::from_utf8(list_buckets.body)
        .expect("utf8 list buckets")
        .contains("server-bucket"));

    let head_bucket = handlers.head_bucket("server-bucket", &auth);
    assert_eq!(head_bucket.status, 200);

    let head_object =
        handlers.head_object("server-bucket", "dir/hello.txt", &auth, &Default::default());
    assert_eq!(head_object.status, 200);
    assert_eq!(
        head_object
            .headers
            .get("content-length")
            .map(String::as_str),
        Some("15")
    );

    let get_object = handlers.get_object("server-bucket", "dir/hello.txt", &auth, None);
    assert_eq!(get_object.status, 200);
    assert_eq!(get_object.body, b"hello from rust");
}

#[test]
fn subtest_test_server_suite_fmt_sprintf_test_d_line_149() {
    for (name, disk_count, auth) in [
        (
            "single-disk signed-v4",
            1usize,
            RequestAuth::signed_v4("minioadmin", "minioadmin"),
        ),
        (
            "multi-disk signed-v2",
            4usize,
            RequestAuth::signed_v2("minioadmin", "minioadmin"),
        ),
    ] {
        let layer = make_layer(disk_count);
        layer
            .make_bucket("suite-bucket", MakeBucketOptions::default())
            .expect(name);
        layer
            .put_object(
                "suite-bucket",
                "object.txt",
                &PutObjReader {
                    data: name.as_bytes().to_vec(),
                    declared_size: name.len() as i64,
                    ..PutObjReader::default()
                },
                ObjectOptions::default(),
            )
            .expect(name);

        let handlers =
            ObjectApiHandlers::new(layer, HandlerCredentials::new("minioadmin", "minioadmin"));

        assert_eq!(handlers.list_buckets(&auth).status, 200, "{name}");
        assert_eq!(
            handlers.head_bucket("suite-bucket", &auth).status,
            200,
            "{name}"
        );
        assert_eq!(
            handlers
                .get_object("suite-bucket", "object.txt", &auth, None)
                .body,
            name.as_bytes(),
            "{name}"
        );
    }

    let uninitialized =
        ObjectApiHandlers::without_layer(HandlerCredentials::new("minioadmin", "minioadmin"));
    let response = uninitialized.list_buckets(&RequestAuth::signed_v4("minioadmin", "minioadmin"));
    assert_eq!(response.status, 503);
}
