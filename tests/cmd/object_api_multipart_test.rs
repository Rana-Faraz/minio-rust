// Rust test snapshot derived from cmd/object-api-multipart_test.go.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

use minio_rust::cmd::{
    get_md5_hash, CompletePart, ListMultipartsInfo, ListPartsInfo, LocalObjectLayer,
    MakeBucketOptions, MultipartInfo, NewMultipartUploadResult, ObjectOptions, PartInfo,
    PutObjReader, ERR_BAD_DIGEST, ERR_BUCKET_NAME_INVALID, ERR_BUCKET_NOT_FOUND,
    ERR_INCOMPLETE_BODY, ERR_INVALID_PART, ERR_INVALID_UPLOAD_ID, ERR_OBJECT_NAME_INVALID,
    ERR_OVERREAD, ERR_PART_TOO_SMALL, ERR_SHA256_MISMATCH, MINIO_META_MULTIPART_BUCKET,
};

pub const SOURCE_FILE: &str = "cmd/object-api-multipart_test.go";

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

fn put_reader(data: &[u8], declared_size: i64, md5: &str, sha256: &str) -> PutObjReader {
    PutObjReader {
        data: data.to_vec(),
        declared_size,
        expected_md5: md5.to_string(),
        expected_sha256: sha256.to_string(),
    }
}

fn multipart_part_path(
    disk: &Path,
    bucket: &str,
    object: &str,
    upload_id: &str,
    part_number: i32,
) -> PathBuf {
    disk.join(MINIO_META_MULTIPART_BUCKET)
        .join(bucket)
        .join(object)
        .join(upload_id)
        .join(format!("part.{part_number}"))
}

fn upload_part(
    layer: &LocalObjectLayer,
    bucket: &str,
    object: &str,
    upload_id: &str,
    part_number: i32,
    data: &[u8],
) -> String {
    let md5 = get_md5_hash(data);
    let info = layer
        .put_object_part(
            bucket,
            object,
            upload_id,
            part_number,
            &put_reader(data, data.len() as i64, &md5, ""),
            ObjectOptions::default(),
        )
        .expect("put object part");
    assert_eq!(info.etag, md5);
    md5
}

fn benchmark_put_object_part_smoke(disk_count: usize, size: usize) {
    let (layer, _dirs) = new_object_layer(disk_count);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);
    let NewMultipartUploadResult { upload_id } = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload");
    // This is benchmark-shaped smoke coverage, not a real throughput benchmark.
    // Cap the payload so the full test suite stays stable under parallel load.
    let smoke_size = size.min(5 * 1024 * 1024);
    let data = vec![b'a'; smoke_size];
    for part_number in 1..=2 {
        let md5 = get_md5_hash(&data);
        layer
            .put_object_part(
                bucket,
                object,
                &upload_id,
                part_number,
                &put_reader(&data, smoke_size as i64, &md5, ""),
                ObjectOptions::default(),
            )
            .expect("benchmark smoke part upload");
    }
}

#[test]
fn test_object_new_multipart_upload_line_37() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = "minio-bucket";

    assert_eq!(
        layer
            .new_multipart_upload("--", "minio-object", ObjectOptions::default())
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NAME_INVALID)
    );
    assert_eq!(
        layer
            .new_multipart_upload(bucket, "minio-object", ObjectOptions::default())
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NOT_FOUND)
    );

    must_make_bucket(&layer, bucket);
    let NewMultipartUploadResult { upload_id } = layer
        .new_multipart_upload(bucket, "\\", ObjectOptions::default())
        .expect("new multipart upload");
    layer
        .abort_multipart_upload(bucket, "\\", &upload_id, ObjectOptions::default())
        .expect("abort multipart upload");
}

#[test]
fn test_object_abort_multipart_upload_line_88() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);

    let NewMultipartUploadResult { upload_id } = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload");

    assert_eq!(
        layer
            .abort_multipart_upload("--", object, &upload_id, ObjectOptions::default())
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NAME_INVALID)
    );
    assert_eq!(
        layer
            .abort_multipart_upload("foo", object, &upload_id, ObjectOptions::default())
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NOT_FOUND)
    );
    assert!(layer
        .abort_multipart_upload(bucket, object, "foo-foo", ObjectOptions::default())
        .err()
        .is_some_and(|err| err.contains(ERR_INVALID_UPLOAD_ID)));

    layer
        .abort_multipart_upload(bucket, object, &upload_id, ObjectOptions::default())
        .expect("abort multipart upload");
    assert!(layer
        .abort_multipart_upload(bucket, object, &upload_id, ObjectOptions::default())
        .err()
        .is_some_and(|err| err.contains(ERR_INVALID_UPLOAD_ID)));
}

#[test]
fn test_object_apiis_upload_idexists_line_144() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);
    let _upload = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload");

    assert!(layer
        .abort_multipart_upload(bucket, object, "abc", ObjectOptions::default())
        .err()
        .is_some_and(|err| err.contains(ERR_INVALID_UPLOAD_ID)));
}

#[test]
fn test_object_apiput_object_part_line_175() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);
    must_make_bucket(&layer, "unused-bucket");

    let NewMultipartUploadResult { upload_id } = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload");
    must_make_bucket(&layer, "abc");
    let NewMultipartUploadResult {
        upload_id: other_upload_id,
    } = layer
        .new_multipart_upload("abc", "def", ObjectOptions::default())
        .expect("other multipart upload");
    fs::remove_dir_all(layer.disk_paths()[0].join("abc")).expect("remove bucket on one disk");
    for disk in layer.disk_paths().iter().skip(1) {
        let path = disk.join("abc");
        if path.exists() {
            fs::remove_dir_all(path).expect("remove bucket");
        }
    }

    let cases: Vec<(
        &str,
        &str,
        &str,
        i32,
        Vec<u8>,
        String,
        String,
        i64,
        Option<&str>,
    )> = vec![
        (
            ".test",
            "obj",
            &upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            "------",
            "obj",
            &upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            "$this-is-not-valid-too",
            "obj",
            &upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            "a",
            "obj",
            &upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            bucket,
            "",
            &upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_OBJECT_NAME_INVALID),
        ),
        (
            "abc",
            "def",
            &other_upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NOT_FOUND),
        ),
        (
            "unused-bucket",
            "def",
            "xyz",
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_INVALID_UPLOAD_ID),
        ),
        (
            bucket,
            "def",
            "xyz",
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_INVALID_UPLOAD_ID),
        ),
        (
            bucket,
            object,
            "xyz",
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_INVALID_UPLOAD_ID),
        ),
        (
            "unused-bucket",
            object,
            &upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_INVALID_UPLOAD_ID),
        ),
        (
            bucket,
            "none-object",
            &upload_id,
            1,
            Vec::new(),
            String::new(),
            String::new(),
            0,
            Some(ERR_INVALID_UPLOAD_ID),
        ),
        (
            bucket,
            object,
            &upload_id,
            1,
            Vec::new(),
            "d41d8cd98f00b204e9800998ecf8427f".to_string(),
            String::new(),
            0,
            Some(ERR_BAD_DIGEST),
        ),
        (
            bucket,
            object,
            &upload_id,
            1,
            b"abcd".to_vec(),
            "e2fc714c4727ee9395f324cd2e7f331f".to_string(),
            "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580".to_string(),
            4,
            Some(ERR_SHA256_MISMATCH),
        ),
        (
            bucket,
            object,
            &upload_id,
            1,
            b"abcd".to_vec(),
            "e2fc714c4727ee9395f324cd2e7f3335".to_string(),
            String::new(),
            5,
            Some(ERR_BAD_DIGEST),
        ),
        (
            bucket,
            object,
            &upload_id,
            1,
            b"abcd".to_vec(),
            "900150983cd24fb0d6963f7d28e17f73".to_string(),
            String::new(),
            3,
            Some(ERR_OVERREAD),
        ),
        (
            bucket,
            object,
            &upload_id,
            1,
            b"abcd".to_vec(),
            "e2fc714c4727ee9395f324cd2e7f331f".to_string(),
            "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589".to_string(),
            4,
            None,
        ),
        (
            bucket,
            object,
            &upload_id,
            2,
            b"efgh".to_vec(),
            "1f7690ebdd9b4caf8fab49ca1757bf27".to_string(),
            String::new(),
            4,
            None,
        ),
        (
            bucket,
            object,
            &upload_id,
            3,
            b"ijkl".to_vec(),
            "09a0877d04abf8759f99adec02baf579".to_string(),
            String::new(),
            4,
            None,
        ),
        (
            bucket,
            object,
            &upload_id,
            4,
            b"mnop".to_vec(),
            "e132e96a5ddad6da8b07bba6f6131fef".to_string(),
            String::new(),
            4,
            None,
        ),
        (
            bucket,
            object,
            &upload_id,
            5,
            Vec::new(),
            String::new(),
            String::new(),
            1,
            Some(ERR_INCOMPLETE_BODY),
        ),
    ];

    for (
        idx,
        (
            bucket_name,
            object_name,
            upload_id,
            part_number,
            input,
            md5,
            sha256,
            declared_size,
            expected_err,
        ),
    ) in cases.into_iter().enumerate()
    {
        let result = layer.put_object_part(
            bucket_name,
            object_name,
            upload_id,
            part_number,
            &put_reader(&input, declared_size, &md5, &sha256),
            ObjectOptions::default(),
        );
        assert_eq!(
            result.as_ref().err().map(|err| {
                if err.contains(ERR_INVALID_UPLOAD_ID) {
                    ERR_INVALID_UPLOAD_ID
                } else {
                    err.as_str()
                }
            }),
            expected_err,
            "case {idx}"
        );
        if expected_err.is_none() {
            assert_eq!(result.expect("put part").etag, md5, "case {idx}");
        }
    }
}

#[test]
fn test_list_multipart_uploads_line_335() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket_one = "minio-bucket";
    let bucket_two = "minio-2-bucket";
    let bucket_three = "minio-3-bucket";
    let object = "minio-object-1.txt";

    must_make_bucket(&layer, bucket_one);
    must_make_bucket(&layer, bucket_two);
    must_make_bucket(&layer, bucket_three);

    let upload_one = layer
        .new_multipart_upload(bucket_one, object, ObjectOptions::default())
        .expect("upload one")
        .upload_id;
    let upload_two = layer
        .new_multipart_upload(bucket_two, object, ObjectOptions::default())
        .expect("upload two")
        .upload_id;
    let upload_three = layer
        .new_multipart_upload(bucket_two, object, ObjectOptions::default())
        .expect("upload three")
        .upload_id;
    let upload_four = layer
        .new_multipart_upload(bucket_two, object, ObjectOptions::default())
        .expect("upload four")
        .upload_id;

    let objects = ["minio-object.txt", "neymar.jpeg", "parrot.png"];
    let upload_ids: Vec<String> = objects
        .into_iter()
        .map(|name| {
            layer
                .new_multipart_upload(bucket_three, name, ObjectOptions::default())
                .expect("bucket three upload")
                .upload_id
        })
        .collect();

    assert_eq!(
        layer
            .list_multipart_uploads(".test", "", "", "", "", 0)
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NAME_INVALID)
    );
    assert_eq!(
        layer
            .list_multipart_uploads("volatile-bucket", "", "", "", "", 0)
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NOT_FOUND)
    );
    assert!(layer
        .list_multipart_uploads(bucket_one, "asia", "", "abc", "", 0)
        .err()
        .is_some());
    assert!(layer
        .list_multipart_uploads(bucket_one, "asia", "asia/europe", "abc=", "", 0)
        .err()
        .is_some());

    let unsupported = layer
        .list_multipart_uploads(bucket_one, "", "", "", "*", 0)
        .expect("unsupported delimiter");
    assert_eq!(unsupported.delimiter, "*");
    assert!(unsupported.uploads.is_empty());

    let listed = layer
        .list_multipart_uploads(bucket_one, "", "", "", "", 100)
        .expect("list uploads");
    assert_eq!(
        listed,
        ListMultipartsInfo {
            max_uploads: 100,
            uploads: vec![MultipartInfo {
                object: object.to_string(),
                upload_id: upload_one.clone(),
            }],
            ..ListMultipartsInfo::default()
        }
    );

    let mut bucket_two_ids = [
        upload_two.clone(),
        upload_three.clone(),
        upload_four.clone(),
    ];
    bucket_two_ids.sort();
    let multi = layer
        .list_multipart_uploads(bucket_two, "", "", "", "", 2)
        .expect("list truncated uploads");
    assert!(multi.is_truncated);
    assert_eq!(multi.uploads.len(), 2);
    assert_eq!(multi.next_key_marker, object);
    assert_eq!(multi.next_upload_id_marker, bucket_two_ids[1]);
    assert_eq!(
        multi.uploads,
        vec![
            MultipartInfo {
                object: object.to_string(),
                upload_id: bucket_two_ids[0].clone(),
            },
            MultipartInfo {
                object: object.to_string(),
                upload_id: bucket_two_ids[1].clone(),
            },
        ]
    );

    let resumed = layer
        .list_multipart_uploads(bucket_two, "", object, &bucket_two_ids[1], "", 100)
        .expect("resume uploads");
    assert_eq!(
        resumed.uploads,
        vec![MultipartInfo {
            object: object.to_string(),
            upload_id: bucket_two_ids[2].clone(),
        }]
    );

    let prefixed = layer
        .list_multipart_uploads(bucket_three, "min", "", "", "", 100)
        .expect("prefix list");
    assert_eq!(
        prefixed.uploads,
        vec![MultipartInfo {
            object: "minio-object.txt".to_string(),
            upload_id: upload_ids[0].clone(),
        }]
    );

    let marker = layer
        .list_multipart_uploads(bucket_three, "", "neymar.jpeg", "", "", 100)
        .expect("key marker list");
    assert_eq!(
        marker.uploads,
        vec![MultipartInfo {
            object: "parrot.png".to_string(),
            upload_id: upload_ids[2].clone(),
        }]
    );

    assert!(layer
        .list_multipart_uploads(bucket_three, "orange", "", "", "", 100)
        .expect("empty list")
        .uploads
        .is_empty());

    let zero = layer
        .list_multipart_uploads(bucket_two, "", "", "", "", 0)
        .expect("zero max uploads");
    assert!(zero.uploads.is_empty());
    assert!(zero.is_truncated);

    let _ = upload_two;
}

#[test]
fn test_list_object_parts_stale_line_1205() {
    let (layer, dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object-1.txt";
    must_make_bucket(&layer, bucket);
    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload")
        .upload_id;

    let etag1 = upload_part(&layer, bucket, object, &upload_id, 1, b"abcd");
    let _etag2 = upload_part(&layer, bucket, object, &upload_id, 2, b"efgh");
    let etag3 = upload_part(&layer, bucket, object, &upload_id, 3, b"ijkl");
    let etag4 = upload_part(&layer, bucket, object, &upload_id, 4, b"mnop");

    for dir in &dirs {
        let path = multipart_part_path(dir.path(), bucket, object, &upload_id, 2);
        if path.exists() {
            fs::remove_file(path).expect("remove stale part");
        }
    }

    let parts = layer
        .list_object_parts(bucket, object, &upload_id, 0, 10, ObjectOptions::default())
        .expect("list object parts");
    assert_eq!(
        parts,
        ListPartsInfo {
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: upload_id.clone(),
            max_parts: 10,
            parts: vec![
                PartInfo {
                    part_number: 1,
                    size: 4,
                    etag: etag1,
                },
                PartInfo {
                    part_number: 3,
                    size: 4,
                    etag: etag3,
                },
                PartInfo {
                    part_number: 4,
                    size: 4,
                    etag: etag4,
                },
            ],
            ..ListPartsInfo::default()
        }
    );
}

#[test]
fn test_list_object_parts_disk_not_found_line_1513() {
    let (layer, dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object-1.txt";
    must_make_bucket(&layer, bucket);
    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload")
        .upload_id;

    let etag1 = upload_part(&layer, bucket, object, &upload_id, 1, b"abcd");
    let etag2 = upload_part(&layer, bucket, object, &upload_id, 2, b"efgh");
    let etag3 = upload_part(&layer, bucket, object, &upload_id, 3, b"ijkl");
    let etag4 = upload_part(&layer, bucket, object, &upload_id, 4, b"mnop");

    fs::remove_dir_all(dirs[0].path()).expect("remove one disk");

    let listed = layer
        .list_object_parts(bucket, object, &upload_id, 0, 3, ObjectOptions::default())
        .expect("list parts with missing disk");
    assert_eq!(listed.parts.len(), 3);
    assert!(listed.is_truncated);
    assert_eq!(listed.next_part_number_marker, 3);
    assert_eq!(
        listed.parts,
        vec![
            PartInfo {
                part_number: 1,
                size: 4,
                etag: etag1,
            },
            PartInfo {
                part_number: 2,
                size: 4,
                etag: etag2,
            },
            PartInfo {
                part_number: 3,
                size: 4,
                etag: etag3,
            },
        ]
    );

    let resumed = layer
        .list_object_parts(bucket, object, &upload_id, 3, 2, ObjectOptions::default())
        .expect("resume list parts");
    assert_eq!(
        resumed.parts,
        vec![PartInfo {
            part_number: 4,
            size: 4,
            etag: etag4,
        }]
    );
}

#[test]
fn test_list_object_parts_line_1773() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object-1.txt";
    must_make_bucket(&layer, bucket);
    let upload_id = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload")
        .upload_id;

    let etag1 = upload_part(&layer, bucket, object, &upload_id, 1, b"abcd");
    let etag2 = upload_part(&layer, bucket, object, &upload_id, 2, b"efgh");
    let etag3 = upload_part(&layer, bucket, object, &upload_id, 3, b"ijkl");
    let etag4 = upload_part(&layer, bucket, object, &upload_id, 4, b"mnop");

    assert_eq!(
        layer
            .list_object_parts(".test", object, &upload_id, 0, 0, ObjectOptions::default())
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NAME_INVALID)
    );
    assert_eq!(
        layer
            .list_object_parts(
                "volatile-bucket",
                object,
                &upload_id,
                0,
                0,
                ObjectOptions::default()
            )
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NOT_FOUND)
    );
    assert_eq!(
        layer
            .list_object_parts(bucket, "", &upload_id, 0, 0, ObjectOptions::default())
            .err()
            .as_deref(),
        Some(ERR_OBJECT_NAME_INVALID)
    );
    assert!(layer
        .list_object_parts(bucket, object, "abc", 0, 0, ObjectOptions::default())
        .err()
        .is_some_and(|err| err.contains(ERR_INVALID_UPLOAD_ID)));

    let listed = layer
        .list_object_parts(bucket, object, &upload_id, 0, 10, ObjectOptions::default())
        .expect("list parts");
    assert_eq!(
        listed.parts,
        vec![
            PartInfo {
                part_number: 1,
                size: 4,
                etag: etag1,
            },
            PartInfo {
                part_number: 2,
                size: 4,
                etag: etag2,
            },
            PartInfo {
                part_number: 3,
                size: 4,
                etag: etag3,
            },
            PartInfo {
                part_number: 4,
                size: 4,
                etag: etag4,
            },
        ]
    );

    let truncated = layer
        .list_object_parts(bucket, object, &upload_id, 0, 3, ObjectOptions::default())
        .expect("truncated list");
    assert!(truncated.is_truncated);
    assert_eq!(truncated.next_part_number_marker, 3);

    let resumed = layer
        .list_object_parts(bucket, object, &upload_id, 3, 2, ObjectOptions::default())
        .expect("resumed list");
    assert_eq!(
        resumed.parts,
        vec![PartInfo {
            part_number: 4,
            size: 4,
            etag: get_md5_hash(b"mnop"),
        }]
    );
}

#[test]
fn test_object_complete_multipart_upload_line_2013() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object-1.txt";
    must_make_bucket(&layer, bucket);

    let failing_upload = layer
        .new_multipart_upload(
            bucket,
            object,
            ObjectOptions {
                user_defined: BTreeMap::from([("X-Amz-Meta-Id".to_string(), "id".to_string())]),
                ..ObjectOptions::default()
            },
        )
        .expect("new multipart upload")
        .upload_id;

    let small_one = upload_part(&layer, bucket, object, &failing_upload, 1, b"abcd");
    let small_two = upload_part(&layer, bucket, object, &failing_upload, 2, b"efgh");
    let large = vec![b'a'; 6 * 1024 * 1024];
    let large_md5 = upload_part(&layer, bucket, object, &failing_upload, 5, &large);

    assert_eq!(
        layer
            .complete_multipart_upload(
                ".test",
                object,
                &failing_upload,
                &[],
                ObjectOptions::default(),
            )
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NAME_INVALID)
    );
    assert_eq!(
        layer
            .complete_multipart_upload(
                "volatile-bucket",
                object,
                &failing_upload,
                &[],
                ObjectOptions::default(),
            )
            .err()
            .as_deref(),
        Some(ERR_BUCKET_NOT_FOUND)
    );
    assert_eq!(
        layer
            .complete_multipart_upload(bucket, "", &failing_upload, &[], ObjectOptions::default())
            .err()
            .as_deref(),
        Some(ERR_OBJECT_NAME_INVALID)
    );
    assert!(layer
        .complete_multipart_upload(bucket, object, "abc", &[], ObjectOptions::default())
        .err()
        .is_some_and(|err| err.contains(ERR_INVALID_UPLOAD_ID)));
    assert_eq!(
        layer
            .complete_multipart_upload(
                bucket,
                object,
                &failing_upload,
                &[CompletePart {
                    etag: "abc".to_string(),
                    part_number: 1,
                }],
                ObjectOptions::default(),
            )
            .err()
            .as_deref(),
        Some(ERR_INVALID_PART)
    );
    assert_eq!(
        layer
            .complete_multipart_upload(
                bucket,
                object,
                &failing_upload,
                &[CompletePart {
                    etag: "abcd".to_string(),
                    part_number: 10,
                }],
                ObjectOptions::default(),
            )
            .err()
            .as_deref(),
        Some(ERR_INVALID_PART)
    );
    assert_eq!(
        layer
            .complete_multipart_upload(
                bucket,
                object,
                &failing_upload,
                &[
                    CompletePart {
                        etag: small_one.clone(),
                        part_number: 1,
                    },
                    CompletePart {
                        etag: small_two.clone(),
                        part_number: 2,
                    },
                ],
                ObjectOptions::default(),
            )
            .err()
            .as_deref(),
        Some(ERR_PART_TOO_SMALL)
    );

    let success_upload = layer
        .new_multipart_upload(
            bucket,
            object,
            ObjectOptions {
                user_defined: BTreeMap::from([("X-Amz-Meta-Id".to_string(), "id".to_string())]),
                ..ObjectOptions::default()
            },
        )
        .expect("new success upload")
        .upload_id;
    upload_part(&layer, bucket, object, &success_upload, 5, &large);

    let result = layer
        .complete_multipart_upload(
            bucket,
            object,
            &success_upload,
            &[CompletePart {
                etag: format!("\"\"\"\"\"{large_md5}\"\""),
                part_number: 5,
            }],
            ObjectOptions::default(),
        )
        .expect("complete multipart upload");
    assert_eq!(
        result.etag,
        minio_rust::cmd::get_complete_multipart_md5(&[CompletePart {
            etag: large_md5.clone(),
            part_number: 5,
        }])
    );
    assert_eq!(
        result.user_defined.get("X-Amz-Meta-Id").map(String::as_str),
        Some("id")
    );
    assert!(layer
        .complete_multipart_upload(
            bucket,
            object,
            &success_upload,
            &[CompletePart {
                etag: large_md5,
                part_number: 5,
            }],
            ObjectOptions::default(),
        )
        .err()
        .is_some_and(|err| err.contains(ERR_INVALID_UPLOAD_ID)));
}

#[test]
fn benchmark_put_object_part5_mb_fs_line_2201() {
    benchmark_put_object_part_smoke(1, 5 * 1024 * 1024);
}

#[test]
fn benchmark_put_object_part5_mb_erasure_line_2206() {
    benchmark_put_object_part_smoke(4, 5 * 1024 * 1024);
}

#[test]
fn benchmark_put_object_part10_mb_fs_line_2211() {
    benchmark_put_object_part_smoke(1, 10 * 1024 * 1024);
}

#[test]
fn benchmark_put_object_part10_mb_erasure_line_2216() {
    benchmark_put_object_part_smoke(4, 10 * 1024 * 1024);
}

#[test]
fn benchmark_put_object_part25_mb_fs_line_2221() {
    benchmark_put_object_part_smoke(1, 25 * 1024 * 1024);
}

#[test]
fn benchmark_put_object_part25_mb_erasure_line_2226() {
    benchmark_put_object_part_smoke(4, 25 * 1024 * 1024);
}

#[test]
fn benchmark_put_object_part50_mb_fs_line_2231() {
    benchmark_put_object_part_smoke(1, 50 * 1024 * 1024);
}

#[test]
fn benchmark_put_object_part50_mb_erasure_line_2236() {
    benchmark_put_object_part_smoke(4, 50 * 1024 * 1024);
}
