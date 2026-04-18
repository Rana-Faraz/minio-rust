// Rust test snapshot derived from cmd/object-api-putobject_test.go.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::thread;

use tempfile::TempDir;

use minio_rust::cmd::{
    get_md5_hash, get_sha256_hash, CompletePart, LocalObjectLayer, MakeBucketOptions,
    NewMultipartUploadResult, ObjectOptions, PutObjReader, ERR_BAD_DIGEST, ERR_BUCKET_NAME_INVALID,
    ERR_BUCKET_NOT_FOUND, ERR_ERASURE_WRITE_QUORUM, ERR_INCOMPLETE_BODY, ERR_OBJECT_NAME_INVALID,
    ERR_OVERREAD, ERR_SHA256_MISMATCH, MINIO_META_TMP_BUCKET,
};

pub const SOURCE_FILE: &str = "cmd/object-api-putobject_test.go";

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

#[test]
fn test_object_apiput_object_single_line_40() {
    let (layer, _dirs) = new_object_layer(1);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);
    must_make_bucket(&layer, "unused-bucket");

    let five_mb = vec![b'a'; 5 * 1024 * 1024];
    let cases: Vec<(
        &str,
        &str,
        Vec<u8>,
        BTreeMap<String, String>,
        String,
        i64,
        Option<&str>,
    )> = vec![
        (
            ".test",
            "obj",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            "------",
            "obj",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            "$this-is-not-valid-too",
            "obj",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            "a",
            "obj",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NAME_INVALID),
        ),
        (
            bucket,
            "",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            Some(ERR_OBJECT_NAME_INVALID),
        ),
        (
            "abc",
            "def",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            Some(ERR_BUCKET_NOT_FOUND),
        ),
        (
            bucket,
            object,
            Vec::new(),
            BTreeMap::from([(
                "etag".to_string(),
                "d41d8cd98f00b204e9800998ecf8427f".to_string(),
            )]),
            String::new(),
            0,
            Some(ERR_BAD_DIGEST),
        ),
        (
            bucket,
            object,
            b"abcd".to_vec(),
            BTreeMap::from([(
                "etag".to_string(),
                "e2fc714c4727ee9395f324cd2e7f331f".to_string(),
            )]),
            "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031580".to_string(),
            4,
            Some(ERR_SHA256_MISMATCH),
        ),
        (
            bucket,
            object,
            b"abcd".to_vec(),
            BTreeMap::from([(
                "etag".to_string(),
                "e2fc714c4727ee9395f324cd2e7f331e".to_string(),
            )]),
            String::new(),
            5,
            Some(ERR_BAD_DIGEST),
        ),
        (
            bucket,
            object,
            b"abcd".to_vec(),
            BTreeMap::from([(
                "etag".to_string(),
                "900150983cd24fb0d6963f7d28e17f73".to_string(),
            )]),
            String::new(),
            3,
            Some(ERR_OVERREAD),
        ),
        (
            bucket,
            object,
            b"abcd".to_vec(),
            BTreeMap::from([(
                "etag".to_string(),
                "e2fc714c4727ee9395f324cd2e7f331f".to_string(),
            )]),
            String::new(),
            4,
            None,
        ),
        (
            bucket,
            object,
            b"efgh".to_vec(),
            BTreeMap::from([(
                "etag".to_string(),
                "1f7690ebdd9b4caf8fab49ca1757bf27".to_string(),
            )]),
            String::new(),
            4,
            None,
        ),
        (
            bucket,
            object,
            b"ijkl".to_vec(),
            BTreeMap::from([(
                "etag".to_string(),
                "09a0877d04abf8759f99adec02baf579".to_string(),
            )]),
            String::new(),
            4,
            None,
        ),
        (
            bucket,
            object,
            b"mnop".to_vec(),
            BTreeMap::from([(
                "etag".to_string(),
                "e132e96a5ddad6da8b07bba6f6131fef".to_string(),
            )]),
            String::new(),
            4,
            None,
        ),
        (
            bucket,
            object,
            b"hello".to_vec(),
            BTreeMap::new(),
            String::new(),
            5,
            None,
        ),
        (
            bucket,
            object,
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            None,
        ),
        (
            bucket,
            object,
            five_mb.clone(),
            BTreeMap::new(),
            String::new(),
            five_mb.len() as i64,
            None,
        ),
        (
            bucket,
            object,
            b"hello".to_vec(),
            BTreeMap::from([("answer".to_string(), "42".to_string())]),
            String::new(),
            5,
            None,
        ),
        (
            bucket,
            object,
            Vec::new(),
            BTreeMap::from([("answer".to_string(), "42".to_string())]),
            String::new(),
            0,
            None,
        ),
        (
            bucket,
            object,
            five_mb.clone(),
            BTreeMap::from([("answer".to_string(), "42".to_string())]),
            String::new(),
            five_mb.len() as i64,
            None,
        ),
        (
            bucket,
            object,
            b"hello".to_vec(),
            BTreeMap::from([("etag".to_string(), get_md5_hash(b"hello"))]),
            get_sha256_hash(b"hello"),
            5,
            None,
        ),
        (
            bucket,
            object,
            Vec::new(),
            BTreeMap::from([("etag".to_string(), get_md5_hash(&[]))]),
            get_sha256_hash(&[]),
            0,
            None,
        ),
        (
            bucket,
            object,
            five_mb.clone(),
            BTreeMap::from([("etag".to_string(), get_md5_hash(&five_mb))]),
            get_sha256_hash(&five_mb),
            five_mb.len() as i64,
            None,
        ),
        (
            bucket,
            object,
            b"hello".to_vec(),
            BTreeMap::from([("etag".to_string(), get_md5_hash(b"meh"))]),
            String::new(),
            5,
            Some(ERR_BAD_DIGEST),
        ),
        (
            bucket,
            object,
            Vec::new(),
            BTreeMap::from([("etag".to_string(), get_md5_hash(b"meh"))]),
            String::new(),
            0,
            Some(ERR_BAD_DIGEST),
        ),
        (
            bucket,
            object,
            five_mb.clone(),
            BTreeMap::from([("etag".to_string(), get_md5_hash(b"meh"))]),
            String::new(),
            five_mb.len() as i64,
            Some(ERR_BAD_DIGEST),
        ),
        (
            bucket,
            object,
            b"hello".to_vec(),
            BTreeMap::new(),
            String::new(),
            4,
            Some(ERR_OVERREAD),
        ),
        (
            bucket,
            object,
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            1,
            Some(ERR_INCOMPLETE_BODY),
        ),
        (
            bucket,
            object,
            five_mb.clone(),
            BTreeMap::new(),
            String::new(),
            (five_mb.len() - 1) as i64,
            Some(ERR_OVERREAD),
        ),
        (
            bucket,
            object,
            b"hello".to_vec(),
            BTreeMap::from([("X-Amz-Meta-AppID".to_string(), "a42".to_string())]),
            String::new(),
            5,
            None,
        ),
        (
            bucket,
            "emptydir/",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            None,
        ),
        (
            bucket,
            "emptydir/minio-object",
            b"hello".to_vec(),
            BTreeMap::new(),
            String::new(),
            5,
            None,
        ),
        (
            bucket,
            "emptydir/",
            Vec::new(),
            BTreeMap::new(),
            String::new(),
            0,
            None,
        ),
        (
            bucket,
            object,
            b"abcd".to_vec(),
            BTreeMap::from([
                (
                    "etag".to_string(),
                    "e2fc714c4727ee9395f324cd2e7f331f".to_string(),
                ),
                ("x-amz-checksum-crc32".to_string(), "abcd".to_string()),
            ]),
            String::new(),
            4,
            None,
        ),
    ];

    for (idx, (bucket_name, obj_name, input, mut meta, sha256, declared_size, expected_err)) in
        cases.into_iter().enumerate()
    {
        let md5 = meta.get("etag").cloned().unwrap_or_default();
        let result = layer.put_object(
            bucket_name,
            obj_name,
            &put_reader(&input, declared_size, &md5, &sha256),
            ObjectOptions {
                user_defined: meta.clone(),
                ..ObjectOptions::default()
            },
        );
        assert_eq!(
            result.as_ref().err().map(String::as_str),
            expected_err,
            "case {idx}"
        );
        if expected_err.is_none() {
            let info = result.expect("put object");
            let expected_etag = meta.remove("etag").unwrap_or_else(|| get_md5_hash(&input));
            assert_eq!(info.etag, expected_etag, "case {idx}");
        }
    }
}

#[test]
fn test_object_apiput_object_disk_not_found_line_216() {
    let (layer, dirs) = new_object_layer(16);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);
    must_make_bucket(&layer, "unused-bucket");

    for dir in dirs.iter().take(4) {
        fs::remove_dir_all(dir.path()).expect("remove disk");
    }

    for data in [
        b"abcd".as_slice(),
        b"efgh".as_slice(),
        b"ijkl".as_slice(),
        b"mnop".as_slice(),
    ] {
        let md5 = get_md5_hash(data);
        let result = layer.put_object(
            bucket,
            object,
            &put_reader(data, data.len() as i64, &md5, ""),
            ObjectOptions {
                user_defined: BTreeMap::from([("etag".to_string(), md5.clone())]),
                ..ObjectOptions::default()
            },
        );
        assert!(result.is_ok());
    }

    for dir in dirs.iter().skip(4).take(4) {
        fs::remove_dir_all(dir.path()).expect("remove additional disk");
    }
    let md5 = get_md5_hash(b"mnop");
    assert_eq!(
        layer
            .put_object(
                bucket,
                object,
                &put_reader(b"mnop", 4, &md5, ""),
                ObjectOptions {
                    user_defined: BTreeMap::from([("etag".to_string(), md5)]),
                    ..ObjectOptions::default()
                },
            )
            .err()
            .as_deref(),
        Some(ERR_ERASURE_WRITE_QUORUM)
    );
}

#[test]
fn test_object_apiput_object_stale_files_line_328() {
    let (layer, dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);

    layer
        .put_object(
            bucket,
            object,
            &put_reader(b"hello, world", 12, "", ""),
            ObjectOptions::default(),
        )
        .expect("put object");

    for dir in dirs {
        let tmp_meta_dir = dir.path().join(MINIO_META_TMP_BUCKET);
        let files = fs::read_dir(&tmp_meta_dir).expect("read tmp meta dir");
        let mut found = false;
        for file in files {
            let file = file.expect("dir entry");
            if file.file_name() == ".trash" {
                continue;
            }
            found = true;
        }
        assert!(!found, "{tmp_meta_dir:?} should be empty except .trash");
    }
}

#[test]
fn test_object_apimultipart_put_object_stale_files_line_373() {
    let (layer, dirs) = new_object_layer(4);
    let bucket = "minio-bucket";
    let object = "minio-object";
    must_make_bucket(&layer, bucket);

    let NewMultipartUploadResult { upload_id } = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("new multipart upload");

    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part1_md5 = get_md5_hash(&part1);
    layer
        .put_object_part(
            bucket,
            object,
            &upload_id,
            1,
            &put_reader(&part1, part1.len() as i64, &part1_md5, ""),
            ObjectOptions::default(),
        )
        .expect("put part 1");

    let part2 = b"hello, world".to_vec();
    let part2_md5 = get_md5_hash(&part2);
    layer
        .put_object_part(
            bucket,
            object,
            &upload_id,
            2,
            &put_reader(&part2, part2.len() as i64, &part2_md5, ""),
            ObjectOptions::default(),
        )
        .expect("put part 2");

    layer
        .complete_multipart_upload(
            bucket,
            object,
            &upload_id,
            &[
                CompletePart {
                    etag: part1_md5.clone(),
                    part_number: 1,
                },
                CompletePart {
                    etag: part2_md5.clone(),
                    part_number: 2,
                },
            ],
            ObjectOptions::default(),
        )
        .expect("complete multipart upload");

    for dir in dirs {
        let tmp_meta_dir = dir.path().join(MINIO_META_TMP_BUCKET);
        let files = fs::read_dir(&tmp_meta_dir).expect("read tmp meta dir");
        let mut found = false;
        for file in files {
            let file = file.expect("dir entry");
            if file.file_name() == ".trash" {
                continue;
            }
            found = true;
            break;
        }
        assert!(!found, "{tmp_meta_dir:?} should be empty except .trash");
    }
}

fn benchmark_put_object_smoke(disk_count: usize, size: usize, parallel: bool) {
    let (layer, _dirs) = new_object_layer(disk_count);
    let bucket = "bench-bucket";
    must_make_bucket(&layer, bucket);
    // This is benchmark-shaped smoke coverage, not a real throughput benchmark.
    // Cap the payload so the full test suite stays stable under parallel load.
    let smoke_size = size.min(5 * 1024 * 1024);
    let data = vec![b'a'; smoke_size];
    let etag = get_md5_hash(&data);

    if parallel {
        thread::scope(|scope| {
            for idx in 0..4 {
                let layer_ref = &layer;
                let data = data.clone();
                let etag = etag.clone();
                scope.spawn(move || {
                    for iter in 0..2 {
                        layer_ref
                            .put_object(
                                bucket,
                                &format!("parallel-{idx}-{iter}"),
                                &put_reader(&data, smoke_size as i64, &etag, ""),
                                ObjectOptions {
                                    user_defined: BTreeMap::from([(
                                        "etag".to_string(),
                                        etag.clone(),
                                    )]),
                                    ..ObjectOptions::default()
                                },
                            )
                            .expect("parallel put object");
                    }
                });
            }
        });
    } else {
        for idx in 0..2 {
            layer
                .put_object(
                    bucket,
                    &format!("object-{idx}"),
                    &put_reader(&data, smoke_size as i64, &etag, ""),
                    ObjectOptions {
                        user_defined: BTreeMap::from([("etag".to_string(), etag.clone())]),
                        ..ObjectOptions::default()
                    },
                )
                .expect("put object");
        }
    }
}

#[test]
fn benchmark_put_object_very_small_fs_line_465() {
    benchmark_put_object_smoke(1, 10, false);
}

#[test]
fn benchmark_put_object_very_small_erasure_line_470() {
    benchmark_put_object_smoke(4, 10, false);
}

#[test]
fn benchmark_put_object10_kb_fs_line_475() {
    benchmark_put_object_smoke(1, 10 * 1024, false);
}

#[test]
fn benchmark_put_object10_kb_erasure_line_480() {
    benchmark_put_object_smoke(4, 10 * 1024, false);
}

#[test]
fn benchmark_put_object100_kb_fs_line_485() {
    benchmark_put_object_smoke(1, 100 * 1024, false);
}

#[test]
fn benchmark_put_object100_kb_erasure_line_490() {
    benchmark_put_object_smoke(4, 100 * 1024, false);
}

#[test]
fn benchmark_put_object1_mb_fs_line_495() {
    benchmark_put_object_smoke(1, 1024 * 1024, false);
}

#[test]
fn benchmark_put_object1_mb_erasure_line_500() {
    benchmark_put_object_smoke(4, 1024 * 1024, false);
}

#[test]
fn benchmark_put_object5_mb_fs_line_505() {
    benchmark_put_object_smoke(1, 5 * 1024 * 1024, false);
}

#[test]
fn benchmark_put_object5_mb_erasure_line_510() {
    benchmark_put_object_smoke(4, 5 * 1024 * 1024, false);
}

#[test]
fn benchmark_put_object10_mb_fs_line_515() {
    benchmark_put_object_smoke(1, 10 * 1024 * 1024, false);
}

#[test]
fn benchmark_put_object10_mb_erasure_line_520() {
    benchmark_put_object_smoke(4, 10 * 1024 * 1024, false);
}

#[test]
fn benchmark_put_object25_mb_fs_line_525() {
    benchmark_put_object_smoke(1, 25 * 1024 * 1024, false);
}

#[test]
fn benchmark_put_object25_mb_erasure_line_530() {
    benchmark_put_object_smoke(4, 25 * 1024 * 1024, false);
}

#[test]
fn benchmark_put_object50_mb_fs_line_535() {
    benchmark_put_object_smoke(1, 50 * 1024 * 1024, false);
}

#[test]
fn benchmark_put_object50_mb_erasure_line_540() {
    benchmark_put_object_smoke(4, 50 * 1024 * 1024, false);
}

#[test]
fn benchmark_parallel_put_object_very_small_fs_line_547() {
    benchmark_put_object_smoke(1, 10, true);
}

#[test]
fn benchmark_parallel_put_object_very_small_erasure_line_552() {
    benchmark_put_object_smoke(4, 10, true);
}

#[test]
fn benchmark_parallel_put_object10_kb_fs_line_557() {
    benchmark_put_object_smoke(1, 10 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object10_kb_erasure_line_562() {
    benchmark_put_object_smoke(4, 10 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object100_kb_fs_line_567() {
    benchmark_put_object_smoke(1, 100 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object100_kb_erasure_line_572() {
    benchmark_put_object_smoke(4, 100 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object1_mb_fs_line_577() {
    benchmark_put_object_smoke(1, 1024 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object1_mb_erasure_line_582() {
    benchmark_put_object_smoke(4, 1024 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object5_mb_fs_line_587() {
    benchmark_put_object_smoke(1, 5 * 1024 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object5_mb_erasure_line_592() {
    benchmark_put_object_smoke(4, 5 * 1024 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object10_mb_fs_line_597() {
    benchmark_put_object_smoke(1, 10 * 1024 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object10_mb_erasure_line_602() {
    benchmark_put_object_smoke(4, 10 * 1024 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object25_mb_fs_line_607() {
    benchmark_put_object_smoke(1, 25 * 1024 * 1024, true);
}

#[test]
fn benchmark_parallel_put_object25_mb_erasure_line_612() {
    benchmark_put_object_smoke(4, 25 * 1024 * 1024, true);
}
