// Rust test snapshot derived from cmd/erasure-healing_test.go.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use crate::support::workspace_tempdir;
use minio_rust::cmd::{
    get_sha256_hash, is_object_dangling, new_file_info, CompletePart, FileInfo, LocalObjectLayer,
    MakeBucketOptions, ObjectOptions, PutObjReader, CHECK_PART_FILE_CORRUPT,
    CHECK_PART_FILE_NOT_FOUND, CHECK_PART_SUCCESS, ERR_DISK_NOT_FOUND, ERR_ERASURE_READ_QUORUM,
    ERR_FILE_CORRUPT, ERR_FILE_NOT_FOUND,
};
use tempfile::TempDir;

pub const SOURCE_FILE: &str = "cmd/erasure-healing_test.go";

fn healing_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn new_healing_layer(disks: usize) -> (LocalObjectLayer, Vec<TempDir>) {
    let roots = (0..disks)
        .map(|_| workspace_tempdir("erasure-healing"))
        .collect::<Vec<_>>();
    let layer = LocalObjectLayer::new(
        roots
            .iter()
            .map(|root| root.path().to_path_buf())
            .collect::<Vec<_>>(),
    );
    (layer, roots)
}

fn reader(data: &[u8]) -> PutObjReader {
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

fn version_ids(layer: &LocalObjectLayer, bucket: &str, object: &str) -> Vec<String> {
    layer
        .list_object_versions(bucket, "", "", "", "", 100)
        .expect("list versions")
        .objects
        .into_iter()
        .filter(|info| info.name == object)
        .map(|info| info.version_id)
        .collect()
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

fn complete_multipart_object(
    layer: &LocalObjectLayer,
    bucket: &str,
    object: &str,
    parts: &[Vec<u8>],
) -> Vec<u8> {
    let upload = layer
        .new_multipart_upload(bucket, object, ObjectOptions::default())
        .expect("start multipart upload");
    let mut complete_parts = Vec::new();
    let mut full_payload = Vec::new();

    for (index, part) in parts.iter().enumerate().rev() {
        let part_info = layer
            .put_object_part(
                bucket,
                object,
                &upload.upload_id,
                index as i32 + 1,
                &reader(part),
                ObjectOptions::default(),
            )
            .expect("put multipart part");
        complete_parts.push(CompletePart {
            etag: part_info.etag,
            part_number: part_info.part_number,
        });
    }
    complete_parts.sort_by_key(|part| part.part_number);

    for part in parts {
        full_payload.extend(part);
    }

    layer
        .complete_multipart_upload(
            bucket,
            object,
            &upload.upload_id,
            &complete_parts,
            ObjectOptions::default(),
        )
        .expect("complete multipart upload");

    full_payload
}

fn make_test_cases() -> Vec<(
    &'static str,
    Vec<FileInfo>,
    Vec<Option<&'static str>>,
    BTreeMap<i32, Vec<i32>>,
    FileInfo,
    bool,
)> {
    let mut fi = new_file_info("test-object", 2, 2);
    fi.erasure.index = 1;

    let mut ifi = new_file_info("test-object", 2, 2);
    ifi.set_inline_data();
    ifi.erasure.index = 1;

    vec![
        (
            "FileInfoExists-case1",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
                fi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
                None,
                None,
            ],
            BTreeMap::new(),
            fi.clone(),
            false,
        ),
        (
            "FileInfoExists-case2",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
                fi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
                None,
            ],
            BTreeMap::new(),
            fi.clone(),
            false,
        ),
        (
            "FileInfoUndecided-case1",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
                None,
            ],
            BTreeMap::new(),
            fi.clone(),
            false,
        ),
        (
            "FileInfoUndecided-case2",
            vec![],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
                Some(ERR_DISK_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
            ],
            BTreeMap::new(),
            FileInfo::default(),
            false,
        ),
        (
            "FileInfoUndecided-case3(file deleted)",
            vec![],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
            ],
            BTreeMap::new(),
            FileInfo::default(),
            false,
        ),
        (
            "FileInfoUnDecided-case4",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                ifi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_CORRUPT),
                Some(ERR_FILE_CORRUPT),
                None,
            ],
            BTreeMap::new(),
            ifi.clone(),
            false,
        ),
        (
            "FileInfoUnDecided-case5-(ignore errFileCorrupt error)",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
            ],
            vec![Some(ERR_FILE_NOT_FOUND), Some(ERR_FILE_CORRUPT), None, None],
            BTreeMap::from([(
                0,
                vec![
                    CHECK_PART_FILE_CORRUPT,
                    CHECK_PART_FILE_NOT_FOUND,
                    CHECK_PART_SUCCESS,
                    CHECK_PART_FILE_CORRUPT,
                ],
            )]),
            fi.clone(),
            false,
        ),
        (
            "FileInfoUnDecided-case6-(data-dir intact)",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
            ],
            BTreeMap::from([(
                0,
                vec![
                    CHECK_PART_FILE_NOT_FOUND,
                    CHECK_PART_FILE_CORRUPT,
                    CHECK_PART_SUCCESS,
                    CHECK_PART_SUCCESS,
                ],
            )]),
            fi.clone(),
            false,
        ),
        (
            "FileInfoDecided-case1",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                ifi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
            ],
            BTreeMap::new(),
            ifi.clone(),
            true,
        ),
        (
            "FileInfoDecided-case2-delete-marker",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                FileInfo {
                    deleted: true,
                    ..Default::default()
                },
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
            ],
            BTreeMap::new(),
            FileInfo {
                deleted: true,
                ..Default::default()
            },
            true,
        ),
        (
            "FileInfoDecided-case3-(enough data-dir missing)",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
                None,
            ],
            BTreeMap::from([(
                0,
                vec![
                    CHECK_PART_FILE_NOT_FOUND,
                    CHECK_PART_FILE_NOT_FOUND,
                    CHECK_PART_SUCCESS,
                    CHECK_PART_FILE_NOT_FOUND,
                ],
            )]),
            fi.clone(),
            true,
        ),
        (
            "FileInfoDecided-case4-(missing data-dir for part 2)",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
                None,
            ],
            BTreeMap::from([
                (
                    0,
                    vec![
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                    ],
                ),
                (
                    1,
                    vec![
                        CHECK_PART_SUCCESS,
                        CHECK_PART_FILE_NOT_FOUND,
                        CHECK_PART_FILE_NOT_FOUND,
                        CHECK_PART_FILE_NOT_FOUND,
                    ],
                ),
            ]),
            fi.clone(),
            true,
        ),
        (
            "FileInfoDecided-case4-(enough data-dir existing for each part)",
            vec![
                FileInfo::default(),
                FileInfo::default(),
                FileInfo::default(),
                fi.clone(),
            ],
            vec![
                Some(ERR_FILE_NOT_FOUND),
                Some(ERR_FILE_NOT_FOUND),
                None,
                None,
            ],
            BTreeMap::from([
                (
                    0,
                    vec![
                        CHECK_PART_FILE_NOT_FOUND,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                    ],
                ),
                (
                    1,
                    vec![
                        CHECK_PART_SUCCESS,
                        CHECK_PART_FILE_NOT_FOUND,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                    ],
                ),
                (
                    2,
                    vec![
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_FILE_NOT_FOUND,
                        CHECK_PART_SUCCESS,
                    ],
                ),
                (
                    3,
                    vec![
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_SUCCESS,
                        CHECK_PART_FILE_NOT_FOUND,
                    ],
                ),
            ]),
            fi,
            false,
        ),
    ]
}

#[test]
fn test_is_object_dangling_line_40() {
    for (name, meta_arr, errs, data_errs, expected_meta, expected_dangling) in make_test_cases() {
        let (got_meta, dangling) = is_object_dangling(&meta_arr, &errs, &data_errs);
        assert_eq!(got_meta, expected_meta, "unexpected meta for {name}");
        assert_eq!(
            dangling, expected_dangling,
            "unexpected dangling state for {name}"
        );
    }
}

#[test]
fn subtest_test_is_object_dangling_test_case_name_line_299() {
    let cases = make_test_cases();
    assert_eq!(
        cases.len(),
        13,
        "upstream subtest table should stay in sync"
    );
    for (name, meta_arr, errs, data_errs, expected_meta, expected_dangling) in cases {
        let (got_meta, dangling) = is_object_dangling(&meta_arr, &errs, &data_errs);
        assert_eq!(got_meta, expected_meta, "subtest meta mismatch for {name}");
        assert_eq!(
            dangling, expected_dangling,
            "subtest dangling mismatch for {name}"
        );
    }
}

#[test]
fn test_healing_line_312() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let payload = vec![0x5a; 1024 * 1024];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&payload),
            ObjectOptions::default(),
        )
        .expect("put object");

    let first_disk_object = object_path(&roots[0], "bucket", "object");
    fs::remove_file(&first_disk_object).expect("remove object from first disk");
    layer
        .heal_object("bucket", "object")
        .expect("heal missing object copy");
    assert_eq!(
        fs::read(&first_disk_object).expect("read healed object"),
        payload,
        "healed object data should match original payload"
    );

    let first_bucket = roots[0].path().join("bucket");
    fs::remove_dir_all(&first_bucket).expect("remove bucket from first disk");
    assert!(
        !first_bucket.exists(),
        "bucket should be missing before heal"
    );
    layer.heal_bucket("bucket").expect("heal bucket");
    assert!(
        first_bucket.is_dir(),
        "bucket should be restored after heal"
    );
}

#[test]
fn test_healing_versioned_line_466() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket(
            "bucket",
            MakeBucketOptions {
                versioning_enabled: true,
            },
        )
        .expect("make versioned bucket");

    let first_payload = vec![0x11; 1024];
    let second_payload = vec![0x22; 1024];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&first_payload),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put v1");
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&second_payload),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put v2");

    let before_versions = version_ids(&layer, "bucket", "object");
    assert_eq!(before_versions.len(), 2, "expected two object versions");

    let first_disk_object = object_path(&roots[0], "bucket", "object");
    fs::remove_file(&first_disk_object).expect("remove current object from first disk");
    layer
        .heal_object("bucket", "object")
        .expect("heal latest version");

    assert_eq!(
        fs::read(&first_disk_object).expect("read healed latest object"),
        second_payload,
        "healed file should contain latest object contents"
    );
    assert_eq!(
        layer
            .get_object("bucket", "object")
            .expect("get latest object"),
        second_payload,
        "latest visible version should stay intact"
    );

    let after_versions = version_ids(&layer, "bucket", "object");
    assert_eq!(
        after_versions, before_versions,
        "healing should preserve version history"
    );
}

#[test]
fn test_healing_dangling_object_line_646() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket(
            "bucket",
            MakeBucketOptions {
                versioning_enabled: true,
            },
        )
        .expect("make versioned bucket");

    let v1 = vec![b'a'; 128 * 1024];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&v1),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put initial object version");

    let offline_indexes = [0usize, 1, 2, 3];
    let moved = take_disks_offline(&roots, &offline_indexes);
    let deleted = layer
        .delete_object(
            "bucket",
            "object",
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("create delete marker under quorum");
    assert!(deleted.delete_marker, "expected versioned delete marker");
    restore_disks(moved);

    let stale_path = object_path(&roots[0], "bucket", "object");
    assert!(
        stale_path.is_file(),
        "offline disk should still have stale object before delete-marker heal"
    );

    layer
        .heal_object("bucket", "object")
        .expect("heal dangling delete marker");
    assert!(
        !stale_path.exists(),
        "delete-marker heal should remove stale object from reconnected disk"
    );
    assert_eq!(
        layer.get_object("bucket", "object"),
        Err(ERR_FILE_NOT_FOUND.to_string()),
        "current visible state should still be deleted"
    );
    assert_eq!(
        version_ids(&layer, "bucket", "object").len(),
        2,
        "expected original version plus delete marker"
    );

    let v2 = vec![b'b'; 128 * 1024];
    let moved = take_disks_offline(&roots, &offline_indexes);
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&v2),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put replacement object under quorum");
    restore_disks(moved);

    assert!(
        !stale_path.exists(),
        "reconnected disk should still miss the newest version before heal"
    );
    layer
        .heal_object("bucket", "object")
        .expect("heal latest replacement version");
    assert_eq!(
        fs::read(&stale_path).expect("read healed latest version"),
        v2,
        "healed disk should receive the newest object contents"
    );
    assert_eq!(
        layer
            .get_object("bucket", "object")
            .expect("read current object"),
        v2,
        "visible object should be the replacement version"
    );
    assert_eq!(
        version_ids(&layer, "bucket", "object").len(),
        3,
        "expected original version, delete marker, and replacement version"
    );
}

#[test]
fn test_heal_correct_quorum_line_850() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(32);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let upload = layer
        .new_multipart_upload("bucket", "object", ObjectOptions::default())
        .expect("start multipart upload");
    let part = vec![0x6b; 5 * 1024 * 1024];
    let first = layer
        .put_object_part(
            "bucket",
            "object",
            &upload.upload_id,
            2,
            &reader(&part),
            ObjectOptions::default(),
        )
        .expect("put part 2");
    let second = layer
        .put_object_part(
            "bucket",
            "object",
            &upload.upload_id,
            1,
            &reader(&part),
            ObjectOptions::default(),
        )
        .expect("put part 1");
    layer
        .complete_multipart_upload(
            "bucket",
            "object",
            &upload.upload_id,
            &[
                minio_rust::cmd::CompletePart {
                    etag: second.etag.clone(),
                    part_number: second.part_number,
                },
                minio_rust::cmd::CompletePart {
                    etag: first.etag.clone(),
                    part_number: first.part_number,
                },
            ],
            ObjectOptions::default(),
        )
        .expect("complete multipart upload");

    for root in roots.iter().take(15) {
        let path = root.path().join("bucket").join("object");
        if path.exists() {
            fs::remove_file(path).expect("remove object copy while preserving quorum");
        }
    }
    layer
        .heal_object("bucket", "object")
        .expect("heal should succeed at exact read quorum");
    for root in roots.iter().take(15) {
        assert!(
            root.path().join("bucket").join("object").is_file(),
            "healed object should be restored on quorum-loss candidates"
        );
    }

    for root in roots.iter().take(16) {
        let path = root.path().join("bucket").join("object");
        if path.exists() {
            fs::remove_file(path).expect("remove object copy below quorum");
        }
    }
    let err = layer
        .heal_object("bucket", "object")
        .expect_err("heal should fail below read quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);
}

#[test]
fn test_heal_object_corrupted_pools_line_981() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(32);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let part_a = vec![b'a'; 5 * 1024 * 1024];
    let part_b = vec![b'b'; 5 * 1024 * 1024];
    let full_payload = complete_multipart_object(&layer, "bucket", "object", &[part_a, part_b]);

    let first_disk_object = object_path(&roots[0], "bucket", "object");
    fs::write(&first_disk_object, b"corrupt-pool-copy").expect("corrupt first disk object");
    layer
        .heal_object("bucket", "object")
        .expect("heal corrupted replica");
    assert_eq!(
        fs::read(&first_disk_object).expect("read healed replica"),
        full_payload,
        "corrupted replica should be replaced with consensus object"
    );

    for root in roots.iter().take(17) {
        let path = root.path().join("bucket").join("object");
        if path.exists() {
            fs::remove_file(path).expect("remove object copy below quorum");
        }
    }
    let err = layer
        .heal_object("bucket", "object")
        .expect_err("heal should fail below quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);
}

#[test]
fn test_heal_object_corrupted_xlmeta_line_1157() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let payload = vec![0x7f; 5 * 1024 * 1024];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&payload),
            ObjectOptions::default(),
        )
        .expect("put object");

    let first_disk_object = object_path(&roots[0], "bucket", "object");
    fs::remove_file(&first_disk_object).expect("remove file before metadata-style corruption");
    fs::create_dir_all(&first_disk_object).expect("replace object file with directory");

    layer
        .heal_object("bucket", "object")
        .expect("heal path-type corruption");
    assert_eq!(
        fs::read(&first_disk_object).expect("read healed file"),
        payload,
        "directory corruption should be normalized back to file contents"
    );

    for root in roots.iter().take(9) {
        let path = root.path().join("bucket").join("object");
        if path.exists() {
            if path.is_dir() {
                fs::remove_dir_all(path).expect("remove corrupted directory");
            } else {
                fs::remove_file(path).expect("remove object copy below quorum");
            }
        }
    }
    let err = layer
        .heal_object("bucket", "object")
        .expect_err("heal should fail below read quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);
}

#[test]
fn test_heal_object_corrupted_parts_line_1296() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let part1 = vec![b'x'; 5 * 1024 * 1024];
    let part2 = vec![b'y'; 5 * 1024 * 1024];
    let full_payload =
        complete_multipart_object(&layer, "bucket", "object", &[part1.clone(), part2.clone()]);

    let disk0_object = object_path(&roots[0], "bucket", "object");
    let disk1_object = object_path(&roots[1], "bucket", "object");
    fs::write(&disk0_object, b"foobytes").expect("corrupt first disk object");
    fs::remove_file(&disk1_object).expect("remove second disk object");

    layer
        .heal_object("bucket", "object")
        .expect("heal corrupted and missing multipart replicas");

    assert_eq!(
        fs::read(&disk0_object).expect("read healed first disk"),
        full_payload,
        "corrupted multipart replica should be replaced"
    );
    assert_eq!(
        fs::read(&disk1_object).expect("read healed second disk"),
        full_payload,
        "missing multipart replica should be rebuilt"
    );
    assert_eq!(
        layer
            .get_object_part("bucket", "object", 1)
            .expect("read multipart part 1"),
        part1
    );
    assert_eq!(
        layer
            .get_object_part("bucket", "object", 2)
            .expect("read multipart part 2"),
        part2
    );
}

#[test]
fn test_heal_object_erasure_line_1456() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let payload = vec![b'a'; 5 * 1024 * 1024];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&payload),
            ObjectOptions::default(),
        )
        .expect("put object");

    let first_disk_object = object_path(&roots[0], "bucket", "object");
    fs::remove_file(&first_disk_object).expect("remove object on first disk");
    assert!(
        !first_disk_object.exists(),
        "object should be missing before heal"
    );

    layer
        .heal_object("bucket", "object")
        .expect("heal object with one missing disk");
    assert!(
        first_disk_object.is_file(),
        "object should be restored after heal"
    );

    for root in roots.iter().take(9) {
        let path = root.path().join("bucket").join("object");
        if path.exists() {
            fs::remove_file(path).expect("remove object to break quorum");
        }
    }
    let err = layer
        .heal_object("bucket", "object")
        .expect_err("heal should fail without read quorum");
    assert_eq!(err, ERR_ERASURE_READ_QUORUM);
}

#[test]
fn test_heal_empty_directory_erasure_line_1561() {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    layer
        .put_object(
            "bucket",
            "empty-dir/",
            &reader(&[]),
            ObjectOptions::default(),
        )
        .expect("put empty dir");

    let first_disk_dir = object_path(&roots[0], "bucket", "empty-dir/");
    fs::remove_dir_all(&first_disk_dir).expect("remove empty dir from first disk");
    assert!(
        !first_disk_dir.exists(),
        "empty dir should be missing before healing"
    );

    layer
        .heal_object("bucket", "empty-dir/")
        .expect("heal empty dir");
    assert!(
        first_disk_dir.is_dir(),
        "empty dir should be restored on first heal"
    );

    layer
        .heal_object("bucket", "empty-dir/")
        .expect("heal empty dir again");
    assert!(
        first_disk_dir.is_dir(),
        "empty dir should remain present after idempotent heal"
    );
}

fn heal_last_data_shard_cases() -> [(&'static str, usize); 8] {
    [
        ("4KiB", 4 * 1024),
        ("64KiB", 64 * 1024),
        ("128KiB", 128 * 1024),
        ("1MiB", 1024 * 1024),
        ("5MiB", 5 * 1024 * 1024),
        ("10MiB", 10 * 1024 * 1024),
        ("5MiB-1KiB", 5 * 1024 * 1024 - 1024),
        ("10MiB-1KiB", 10 * 1024 * 1024 - 1024),
    ]
}

fn run_heal_last_data_shard_case(name: &str, data_size: usize) {
    let _guard = healing_test_lock().lock().expect("healing test lock");
    let (layer, roots) = new_healing_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let payload = (0..data_size)
        .map(|idx| (idx % 251) as u8)
        .collect::<Vec<_>>();
    let expected_sha256 = get_sha256_hash(&payload);
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&payload),
            ObjectOptions::default(),
        )
        .expect("put object");

    for disk_index in [11usize, 1usize] {
        let path = object_path(&roots[disk_index], "bucket", "object");
        fs::remove_file(&path).expect("remove shard copy");
        assert!(
            !path.exists(),
            "{name}: disk {disk_index} object should be missing before heal"
        );

        layer.heal_object("bucket", "object").unwrap_or_else(|err| {
            panic!("{name}: heal should succeed after removing disk {disk_index}: {err}")
        });

        let healed = layer
            .get_object("bucket", "object")
            .unwrap_or_else(|err| panic!("{name}: get_object should succeed after heal: {err}"));
        assert_eq!(
            get_sha256_hash(&healed),
            expected_sha256,
            "{name}: healed object checksum mismatch after repairing disk {disk_index}"
        );
        assert!(
            path.is_file(),
            "{name}: healed file should be restored to disk {disk_index}"
        );
    }
}

#[test]
fn test_heal_last_data_shard_line_1641() {
    for (name, data_size) in heal_last_data_shard_cases() {
        run_heal_last_data_shard_case(name, data_size);
    }
}

#[test]
fn subtest_test_heal_last_data_shard_test_name_line_1657() {
    let cases = heal_last_data_shard_cases();
    assert_eq!(cases.len(), 8, "upstream subtest table should stay in sync");
    for (name, data_size) in cases {
        run_heal_last_data_shard_case(name, data_size);
    }
}
