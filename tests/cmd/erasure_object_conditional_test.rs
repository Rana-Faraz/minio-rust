use std::fs;
use std::path::PathBuf;

use crate::support::workspace_tempdir;
use tempfile::TempDir;

use minio_rust::cmd::{
    LocalObjectLayer, MakeBucketOptions, ObjectOptions, PutObjReader, ERR_ERASURE_READ_QUORUM,
};

pub const SOURCE_FILE: &str = "cmd/erasure-object-conditional_test.go";

fn new_erasure_layer(disks: usize) -> (LocalObjectLayer, Vec<TempDir>) {
    let roots = (0..disks)
        .map(|_| workspace_tempdir("erasure-object-conditional"))
        .collect::<Vec<_>>();
    let layer = LocalObjectLayer::new(
        roots
            .iter()
            .map(|dir| dir.path().to_path_buf())
            .collect::<Vec<_>>(),
    );
    (layer, roots)
}

fn put_reader(data: &[u8]) -> PutObjReader {
    PutObjReader {
        data: data.to_vec(),
        declared_size: data.len() as i64,
        expected_md5: String::new(),
        expected_sha256: String::new(),
    }
}

fn object_path(root: &TempDir, bucket: &str, object: &str) -> PathBuf {
    root.path().join(bucket).join(object)
}

fn take_disks_offline(roots: &[TempDir], indexes: &[usize]) -> Vec<(PathBuf, PathBuf)> {
    let mut moved = Vec::new();
    for index in indexes {
        let original = roots[*index].path().to_path_buf();
        let backup = original.with_extension(format!("offline-{index}"));
        if backup.exists() {
            fs::remove_dir_all(&backup).expect("remove stale offline dir");
        }
        fs::rename(&original, &backup).expect("take disk offline");
        moved.push((original, backup));
    }
    moved
}

fn restore_disks(moved: Vec<(PathBuf, PathBuf)>) {
    for (original, backup) in moved {
        fs::rename(&backup, &original).expect("restore disk");
    }
}

fn induce_read_quorum_failure(
    roots: &[TempDir],
    bucket: &str,
    object: &str,
) -> Vec<(PathBuf, PathBuf)> {
    let moved = take_disks_offline(roots, &[0, 1, 2, 3, 4, 5, 6]);
    fs::write(object_path(&roots[7], bucket, object), b"corrupt-copy").expect("corrupt object");
    moved
}

#[test]
fn test_put_object_conditional_with_read_quorum_failure_line_36() {
    let (layer, roots) = new_erasure_layer(16);
    let bucket = "test-bucket";
    let object = "test-object";
    layer
        .make_bucket(bucket, MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            bucket,
            object,
            &put_reader(b"initial-value"),
            ObjectOptions::default(),
        )
        .expect("put object");
    let existing = layer.get_object_info(bucket, object).expect("object info");

    let moved = induce_read_quorum_failure(&roots, bucket, object);

    let err = layer
        .put_object(
            bucket,
            object,
            &put_reader(b"replacement"),
            ObjectOptions {
                user_defined: [("if-none-match".to_string(), "*".to_string())]
                    .into_iter()
                    .collect(),
                ..ObjectOptions::default()
            },
        )
        .expect_err("if-none-match should fail");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);

    let err = layer
        .put_object(
            bucket,
            object,
            &put_reader(b"replacement"),
            ObjectOptions {
                user_defined: [("if-match".to_string(), "wrong-etag-12345".to_string())]
                    .into_iter()
                    .collect(),
                ..ObjectOptions::default()
            },
        )
        .expect_err("wrong if-match should fail");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);

    let err = layer
        .put_object(
            bucket,
            object,
            &put_reader(b"replacement"),
            ObjectOptions {
                user_defined: [("if-match".to_string(), existing.etag.clone())]
                    .into_iter()
                    .collect(),
                ..ObjectOptions::default()
            },
        )
        .expect_err("correct if-match should still fail on quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);

    restore_disks(moved);
}

#[test]
fn subtest_test_put_object_conditional_with_read_quorum_failure_if_none_match_with_read_quorum_failure_line_85(
) {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("test-bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "test-bucket",
            "test-object",
            &put_reader(b"initial-value"),
            ObjectOptions::default(),
        )
        .expect("put object");
    let moved = induce_read_quorum_failure(&roots, "test-bucket", "test-object");

    let err = layer
        .put_object(
            "test-bucket",
            "test-object",
            &put_reader(b"replacement"),
            ObjectOptions {
                user_defined: [("if-none-match".to_string(), "*".to_string())]
                    .into_iter()
                    .collect(),
                ..ObjectOptions::default()
            },
        )
        .expect_err("expected read quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);

    restore_disks(moved);
}

#[test]
fn subtest_test_put_object_conditional_with_read_quorum_failure_if_match_with_read_quorum_failure_line_107(
) {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("test-bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "test-bucket",
            "test-object",
            &put_reader(b"initial-value"),
            ObjectOptions::default(),
        )
        .expect("put object");
    let etag = layer
        .get_object_info("test-bucket", "test-object")
        .expect("object info")
        .etag;
    let moved = induce_read_quorum_failure(&roots, "test-bucket", "test-object");

    let err = layer
        .put_object(
            "test-bucket",
            "test-object",
            &put_reader(b"replacement"),
            ObjectOptions {
                user_defined: [("if-match".to_string(), etag)].into_iter().collect(),
                ..ObjectOptions::default()
            },
        )
        .expect_err("expected read quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);

    restore_disks(moved);
}

#[test]
fn subtest_test_put_object_conditional_with_read_quorum_failure_if_match_wrong_etag_with_read_quorum_failure_line_129(
) {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("test-bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "test-bucket",
            "test-object",
            &put_reader(b"initial-value"),
            ObjectOptions::default(),
        )
        .expect("put object");
    let moved = induce_read_quorum_failure(&roots, "test-bucket", "test-object");

    let err = layer
        .put_object(
            "test-bucket",
            "test-object",
            &put_reader(b"replacement"),
            ObjectOptions {
                user_defined: [("if-match".to_string(), "wrong-etag-12345".to_string())]
                    .into_iter()
                    .collect(),
                ..ObjectOptions::default()
            },
        )
        .expect_err("expected read quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);

    restore_disks(moved);
}
