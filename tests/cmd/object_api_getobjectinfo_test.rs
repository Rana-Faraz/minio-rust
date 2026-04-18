// Rust test snapshot derived from cmd/object-api-getobjectinfo_test.go.

use std::path::PathBuf;

use tempfile::TempDir;

use minio_rust::cmd::{
    LocalObjectLayer, MakeBucketOptions, ObjectOptions, PutObjReader, ERR_BUCKET_NAME_INVALID,
    ERR_BUCKET_NOT_FOUND, ERR_FILE_NOT_FOUND, ERR_OBJECT_NAME_INVALID,
};

pub const SOURCE_FILE: &str = "cmd/object-api-getobjectinfo_test.go";

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
fn test_get_object_info_line_27() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = "test-getobjectinfo";
    layer
        .make_bucket(bucket, MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            bucket,
            "Asia/asiapics.jpg",
            &PutObjReader {
                data: b"asiapics".to_vec(),
                declared_size: 8,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put object");
    layer
        .put_object(
            bucket,
            "Asia/empty-dir/",
            &PutObjReader {
                data: Vec::new(),
                declared_size: 0,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions::default(),
        )
        .expect("put empty dir");

    let cases = [
        (".test", "", Some(ERR_BUCKET_NAME_INVALID), None, None, None),
        ("---", "", Some(ERR_BUCKET_NAME_INVALID), None, None, None),
        ("ad", "", Some(ERR_BUCKET_NAME_INVALID), None, None, None),
        (
            "abcdefgh",
            "abc",
            Some(ERR_BUCKET_NOT_FOUND),
            None,
            None,
            None,
        ),
        (
            "ijklmnop",
            "efg",
            Some(ERR_BUCKET_NOT_FOUND),
            None,
            None,
            None,
        ),
        (bucket, "", Some(ERR_OBJECT_NAME_INVALID), None, None, None),
        (bucket, "Africa", Some(ERR_FILE_NOT_FOUND), None, None, None),
        (
            bucket,
            "Antartica",
            Some(ERR_FILE_NOT_FOUND),
            None,
            None,
            None,
        ),
        (
            bucket,
            "Asia/myfile",
            Some(ERR_FILE_NOT_FOUND),
            None,
            None,
            None,
        ),
        (
            bucket,
            "Asia/asiapics.jpg",
            None,
            Some("test-getobjectinfo"),
            Some("Asia/asiapics.jpg"),
            Some(("image/jpeg", false)),
        ),
        (
            bucket,
            "Asia/empty-dir/",
            None,
            Some("test-getobjectinfo"),
            Some("Asia/empty-dir/"),
            Some(("application/octet-stream", true)),
        ),
    ];

    for (
        idx,
        (bucket_name, object_name, expected_err, expected_bucket, expected_name, expected_attrs),
    ) in cases.into_iter().enumerate()
    {
        let result = layer.get_object_info(bucket_name, object_name);
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "case {idx}"
        );
        if expected_err.is_none() {
            let info = result.expect("get object info");
            assert_eq!(
                Some(info.bucket.as_str()),
                expected_bucket,
                "case {idx} bucket"
            );
            assert_eq!(Some(info.name.as_str()), expected_name, "case {idx} name");
            let (content_type, is_dir) = expected_attrs.expect("expected attrs");
            assert_eq!(info.content_type, content_type, "case {idx} content type");
            assert_eq!(info.is_dir, is_dir, "case {idx} dir flag");
        }
    }
}
