// Rust test snapshot derived from cmd/object-api-deleteobject_test.go.

use std::path::PathBuf;

use tempfile::TempDir;

use minio_rust::cmd::{LocalObjectLayer, MakeBucketOptions, ObjectOptions, PutObjReader};

pub const SOURCE_FILE: &str = "cmd/object-api-deleteobject_test.go";

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

#[test]
fn test_delete_object_line_29() {
    let (layer, _dirs) = new_object_layer(4);
    let cases = [
        (
            "bucket1",
            vec![
                ("object0", b"content".as_slice()),
                ("object1", b"content".as_slice()),
            ],
            "object0",
            vec!["object1"],
        ),
        (
            "bucket2",
            vec![
                ("object0", b"content".as_slice()),
                ("dir/object1", b"content".as_slice()),
            ],
            "dir/object1",
            vec!["object0"],
        ),
        (
            "bucket3",
            vec![
                ("dir/object1", b"content".as_slice()),
                ("dir/object2", b"content".as_slice()),
            ],
            "dir/object1",
            vec!["dir/object2"],
        ),
        (
            "bucket4",
            vec![
                ("object0", b"content".as_slice()),
                ("dir/object1", b"content".as_slice()),
            ],
            "dir/",
            vec!["dir/object1", "object0"],
        ),
        (
            "bucket5",
            vec![("object0", b"content".as_slice()), ("dir/", b"".as_slice())],
            "dir/",
            vec!["object0"],
        ),
    ];

    for (bucket, uploads, path_to_delete, expected_after) in cases {
        must_make_bucket(&layer, bucket);
        for (name, data) in uploads {
            put_object(&layer, bucket, name, data);
        }

        let _ = layer.delete_object(bucket, path_to_delete, ObjectOptions::default());
        let result = layer
            .list_objects(bucket, "", "", "", 1000)
            .expect("list objects after delete");

        let names: Vec<String> = result
            .objects
            .into_iter()
            .map(|object| object.name)
            .collect();
        assert_eq!(
            names, expected_after,
            "bucket {bucket}, delete {path_to_delete}"
        );
    }
}
