use std::fs;
use std::path::PathBuf;

use crate::support::workspace_tempdir;
use tempfile::TempDir;

use minio_rust::cmd::{
    get_md5_hash, CompletePart, LocalObjectLayer, MakeBucketOptions, NewMultipartUploadResult,
    ObjectOptions, PutObjReader, ERR_ERASURE_READ_QUORUM,
};

pub const SOURCE_FILE: &str = "cmd/erasure-multipart-conditional_test.go";

fn new_erasure_layer(disks: usize) -> (LocalObjectLayer, Vec<TempDir>) {
    let roots = (0..disks)
        .map(|_| workspace_tempdir("erasure-multipart-conditional"))
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
fn test_new_multipart_upload_conditional_with_read_quorum_failure_line_37() {
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
        .new_multipart_upload(
            bucket,
            object,
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
        .new_multipart_upload(
            bucket,
            object,
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
        .new_multipart_upload(
            bucket,
            object,
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
fn subtest_test_new_multipart_upload_conditional_with_read_quorum_failure_if_none_match_with_read_quorum_failure_line_86(
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
        .new_multipart_upload(
            "test-bucket",
            "test-object",
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
fn subtest_test_new_multipart_upload_conditional_with_read_quorum_failure_if_match_with_wrong_etag_and_read_quorum_failure_line_106(
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
        .new_multipart_upload(
            "test-bucket",
            "test-object",
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

#[test]
fn subtest_test_new_multipart_upload_conditional_with_read_quorum_failure_if_match_with_correct_etag_and_read_quorum_failure_line_128(
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
        .expect("info")
        .etag;
    let moved = induce_read_quorum_failure(&roots, "test-bucket", "test-object");
    let err = layer
        .new_multipart_upload(
            "test-bucket",
            "test-object",
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
fn test_complete_multipart_upload_conditional_with_read_quorum_failure_line_151() {
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

    let NewMultipartUploadResult { upload_id } = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload");
    let part_data = vec![b'a'; 5 * 1024 * 1024];
    let md5 = get_md5_hash(&part_data);
    layer
        .put_object_part(
            bucket,
            object,
            &upload_id,
            1,
            &PutObjReader {
                data: part_data,
                declared_size: 5 * 1024 * 1024,
                expected_md5: md5.clone(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put part");

    let moved = induce_read_quorum_failure(&roots, bucket, object);

    let err = layer
        .complete_multipart_upload(
            bucket,
            object,
            &upload_id,
            &[CompletePart {
                part_number: 1,
                etag: md5,
            }],
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
fn subtest_test_complete_multipart_upload_conditional_with_read_quorum_failure_complete_multipart_with_if_none_match_and_read_quorum_failure_line_207(
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
    let NewMultipartUploadResult { upload_id } = layer
        .new_multipart_upload("test-bucket", "test-object", ObjectOptions::default())
        .expect("new multipart upload");
    let part_data = vec![b'a'; 5 * 1024 * 1024];
    let md5 = get_md5_hash(&part_data);
    layer
        .put_object_part(
            "test-bucket",
            "test-object",
            &upload_id,
            1,
            &PutObjReader {
                data: part_data,
                declared_size: 5 * 1024 * 1024,
                expected_md5: md5.clone(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put part");
    let moved = induce_read_quorum_failure(&roots, "test-bucket", "test-object");
    let err = layer
        .complete_multipart_upload(
            "test-bucket",
            "test-object",
            &upload_id,
            &[CompletePart {
                part_number: 1,
                etag: md5,
            }],
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
