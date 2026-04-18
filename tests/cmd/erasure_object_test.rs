use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use crate::support::workspace_tempdir;
use minio_rust::cmd::{
    object_quorum_from_meta, ErasureInfo, FileInfo, LocalObjectLayer, MakeBucketOptions,
    ObjectOptions, PutObjReader, ERR_BUCKET_NAME_INVALID, ERR_ERASURE_READ_QUORUM,
    ERR_ERASURE_WRITE_QUORUM, ERR_FILE_NOT_FOUND, ERR_FILE_VERSION_NOT_FOUND,
    ERR_OBJECT_NAME_INVALID,
};
use tempfile::TempDir;

pub const SOURCE_FILE: &str = "cmd/erasure-object_test.go";

const SMALL_FILE_THRESHOLD: usize = 128 * 1024;

fn new_erasure_layer(disks: usize) -> (LocalObjectLayer, Vec<TempDir>) {
    let roots = (0..disks)
        .map(|_| workspace_tempdir("erasure-object"))
        .collect::<Vec<_>>();
    let layer = LocalObjectLayer::new(
        roots
            .iter()
            .map(|dir| dir.path().to_path_buf())
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

fn object_path(root: &TempDir, bucket: &str, object: &str) -> PathBuf {
    root.path().join(bucket).join(object)
}

fn remove_disks(roots: &[TempDir], indexes: &[usize]) {
    for index in indexes {
        let path = roots[*index].path();
        if path.exists() {
            fs::remove_dir_all(path).expect("remove disk");
        }
    }
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

fn assert_put_no_quorum(layer: &LocalObjectLayer, size: usize, byte: u8) {
    let payload = vec![byte; size];
    let result = layer.put_object(
        "bucket",
        "object",
        &reader(&payload),
        ObjectOptions::default(),
    );
    assert_eq!(result, Err(ERR_ERASURE_WRITE_QUORUM.to_string()));
}

#[test]
fn test_repeat_put_object_part_line_38() {
    let (layer, _roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket1", MakeBucketOptions::default())
        .expect("make bucket");

    let upload = layer
        .new_multipart_upload("bucket1", "mpartObj1", ObjectOptions::default())
        .expect("new multipart upload");
    let five_mb = vec![b'a'; 5 * 1024 * 1024];

    let first = layer
        .put_object_part(
            "bucket1",
            "mpartObj1",
            &upload.upload_id,
            1,
            &reader(&five_mb),
            ObjectOptions::default(),
        )
        .expect("first part");
    let second = layer
        .put_object_part(
            "bucket1",
            "mpartObj1",
            &upload.upload_id,
            1,
            &reader(&five_mb),
            ObjectOptions::default(),
        )
        .expect("repeat part");

    assert_eq!(first.part_number, 1);
    assert_eq!(first.etag, second.etag);

    let listed = layer
        .list_object_parts(
            "bucket1",
            "mpartObj1",
            &upload.upload_id,
            0,
            100,
            ObjectOptions::default(),
        )
        .expect("list parts");
    assert_eq!(listed.parts.len(), 1);
    assert_eq!(listed.parts[0].part_number, 1);
    assert_eq!(listed.parts[0].size, five_mb.len() as i64);
}

#[test]
fn test_erasure_delete_object_basic_line_78() {
    let (layer, _roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "bucket",
            "dir/obj",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("put object");

    let cases = [
        (".test", "dir/obj", Some(ERR_BUCKET_NAME_INVALID)),
        ("----", "dir/obj", Some(ERR_BUCKET_NAME_INVALID)),
        ("bucket", "", Some(ERR_OBJECT_NAME_INVALID)),
        ("bucket", "doesnotexist", Some(ERR_FILE_NOT_FOUND)),
        ("bucket", "dir/doesnotexist", Some(ERR_FILE_NOT_FOUND)),
        ("bucket", "dir", Some(ERR_FILE_NOT_FOUND)),
        ("bucket", "dir/", Some(ERR_FILE_NOT_FOUND)),
        ("bucket", "dir/obj", None),
    ];

    for (bucket, object, expected) in cases {
        let result = layer.delete_object(bucket, object, ObjectOptions::default());
        match expected {
            Some(err) => assert_eq!(result, Err(err.to_string()), "{bucket}/{object}"),
            None => assert!(result.is_ok(), "{bucket}/{object}: {result:?}"),
        }
    }

    assert_eq!(
        layer.get_object_info("bucket", "dir/obj"),
        Err(ERR_FILE_NOT_FOUND.to_string())
    );
}

#[test]
fn subtest_test_erasure_delete_object_basic_line_115() {
    let (layer, _roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "bucket",
            "dir/obj",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("put object");

    for (object, want_ok) in [("dir/missing", false), ("dir/obj", true)] {
        let got = layer
            .delete_object("bucket", object, ObjectOptions::default())
            .is_ok();
        assert_eq!(got, want_ok, "{object}");
    }
}

#[test]
fn test_delete_objects_versioned_two_pools_line_133() {
    let (layer, _roots) = new_erasure_layer(16);
    layer
        .make_bucket(
            "bucket",
            MakeBucketOptions {
                versioning_enabled: true,
            },
        )
        .expect("make versioned bucket");

    let v1 = layer
        .put_object(
            "bucket",
            "myobject",
            &reader(b"abcd"),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put v1")
        .version_id;
    let v2 = layer
        .put_object(
            "bucket",
            "myobject",
            &reader(b"abcd"),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put v2")
        .version_id;

    for version_id in [v2, v1] {
        layer
            .delete_object(
                "bucket",
                "myobject",
                ObjectOptions {
                    versioned: true,
                    version_id: version_id.clone(),
                    ..ObjectOptions::default()
                },
            )
            .expect("delete exact version");
        assert!(
            !version_ids(&layer, "bucket", "myobject").contains(&version_id),
            "version {version_id} should be gone"
        );
    }
}

#[test]
fn test_delete_objects_versioned_line_202() {
    let (layer, _roots) = new_erasure_layer(16);
    layer
        .make_bucket(
            "bucket",
            MakeBucketOptions {
                versioning_enabled: true,
            },
        )
        .expect("make versioned bucket");

    let v1 = layer
        .put_object(
            "bucket",
            "dir/obj1",
            &reader(b"abcd"),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put v1")
        .version_id;
    let v2 = layer
        .put_object(
            "bucket",
            "dir/obj1",
            &reader(b"abcd"),
            ObjectOptions {
                versioned: true,
                ..ObjectOptions::default()
            },
        )
        .expect("put v2")
        .version_id;

    for version_id in [v1.clone(), v2.clone(), String::from("missing-version")] {
        let result = layer.delete_object(
            "bucket",
            "dir/obj1",
            ObjectOptions {
                versioned: true,
                version_id: version_id.clone(),
                ..ObjectOptions::default()
            },
        );
        if version_id == "missing-version" {
            assert_eq!(result, Err(ERR_FILE_VERSION_NOT_FOUND.to_string()));
        } else {
            assert!(result.is_ok(), "{version_id}: {result:?}");
        }
    }

    assert!(version_ids(&layer, "bucket", "dir/obj1").is_empty());
    assert_eq!(
        layer.get_object_info("bucket", "dir/obj1"),
        Err(ERR_FILE_NOT_FOUND.to_string())
    );
}

#[test]
fn test_erasure_delete_objects_erasure_set_line_281() {
    let (layer, _roots) = new_erasure_layer(32);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    for object in ["dir/obj1", "dir/obj2", "obj3", "obj_4"] {
        layer
            .put_object("bucket", object, &reader(b"abcd"), ObjectOptions::default())
            .expect("put object");
    }

    for object in ["dir/obj1", "dir/obj2", "obj3", "obj_4"] {
        layer
            .delete_object("bucket", object, ObjectOptions::default())
            .expect("delete object");
        assert_eq!(
            layer.get_object_info("bucket", object),
            Err(ERR_FILE_NOT_FOUND.to_string())
        );
    }
}

#[test]
fn test_put_object_small_inline_data_line_870() {
    let (layer, _roots) = new_erasure_layer(4);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let small = vec![b'a'];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&small),
            ObjectOptions::default(),
        )
        .expect("put small");
    assert_eq!(
        layer.get_object("bucket", "object").expect("get small"),
        small
    );

    let big = vec![b'b'; SMALL_FILE_THRESHOLD * 2];
    layer
        .put_object("bucket", "object", &reader(&big), ObjectOptions::default())
        .expect("put big");
    assert_eq!(layer.get_object("bucket", "object").expect("get big"), big);
}

#[test]
fn test_object_quorum_from_meta_line_939() {
    let make_file = |parity_blocks: i32, storage_class: Option<&str>| FileInfo {
        metadata: storage_class.map(|value| {
            BTreeMap::from([(String::from("x-amz-storage-class"), value.to_string())])
        }),
        erasure: ErasureInfo {
            data_blocks: 16 - parity_blocks,
            parity_blocks,
            ..ErasureInfo::default()
        },
        ..FileInfo::default()
    };

    let cases = [
        (vec![make_file(4, None); 16], 12, 12),
        (vec![make_file(2, Some("REDUCED_REDUNDANCY")); 16], 14, 14),
        (vec![make_file(6, Some("STANDARD")); 16], 10, 10),
        (vec![make_file(5, Some("STANDARD")); 16], 11, 11),
    ];

    for (parts, read_quorum, write_quorum) in cases {
        let errs = vec![None; parts.len()];
        let got = object_quorum_from_meta(&parts, &errs, 4).expect("quorum from metadata");
        assert_eq!(got, (read_quorum, write_quorum));
    }
}

#[test]
fn test_erasure_delete_object_disk_not_found_line_354() {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "bucket",
            "object",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("put object");

    remove_disks(&roots, &[0, 1, 2, 3, 4, 5, 6]);
    layer
        .delete_object("bucket", "object", ObjectOptions::default())
        .expect("delete with quorum");

    layer
        .put_object(
            "bucket",
            "object",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("recreate object");

    remove_disks(&roots, &[7]);
    assert_eq!(
        layer.delete_object("bucket", "object", ObjectOptions::default()),
        Err(ERR_ERASURE_WRITE_QUORUM.to_string())
    );
}

#[test]
fn test_erasure_delete_object_disk_not_found_erasure4_line_423() {
    let (layer, roots) = new_erasure_layer(4);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "bucket",
            "object",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("put object");

    remove_disks(&roots, &[0, 1]);
    assert_eq!(
        layer.delete_object("bucket", "object", ObjectOptions::default()),
        Err(ERR_ERASURE_WRITE_QUORUM.to_string())
    );
}

#[test]
fn test_erasure_delete_object_disk_not_found_err_line_483() {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "bucket",
            "object",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("put object");

    let moved = take_disks_offline(&roots, &[0, 1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(
        layer.delete_object("bucket", "object", ObjectOptions::default()),
        Err(ERR_ERASURE_WRITE_QUORUM.to_string())
    );
    restore_disks(moved);

    assert_eq!(
        layer.get_object("bucket", "object").expect("get object"),
        b"abcd"
    );
}

#[test]
fn test_get_object_no_quorum_line_554() {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");

    let large = vec![b'a'; SMALL_FILE_THRESHOLD * 16];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&large),
            ObjectOptions::default(),
        )
        .expect("put object");

    for root in &roots {
        let path = object_path(root, "bucket", "object");
        if path.exists() {
            fs::remove_file(path).expect("remove object data");
        }
    }
    assert_eq!(
        layer.get_object("bucket", "object"),
        Err(ERR_ERASURE_READ_QUORUM.to_string())
    );

    layer
        .put_object(
            "bucket",
            "object",
            &reader(&large),
            ObjectOptions::default(),
        )
        .expect("re-put object");
    remove_disks(&roots, &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(
        layer.get_object("bucket", "object"),
        Err(ERR_ERASURE_READ_QUORUM.to_string())
    );
}

#[test]
fn test_head_object_no_quorum_line_663() {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    layer
        .put_object(
            "bucket",
            "object",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("put object");

    for root in &roots {
        let path = object_path(root, "bucket", "object");
        if path.exists() {
            fs::remove_file(path).expect("remove data");
        }
    }
    let info = layer
        .get_object_info("bucket", "object")
        .expect("head should use metadata");
    assert_eq!(info.name, "object");

    layer
        .put_object(
            "bucket",
            "object",
            &reader(b"abcd"),
            ObjectOptions::default(),
        )
        .expect("re-put object");
    remove_disks(&roots, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(
        layer.get_object_info("bucket", "object"),
        Err(ERR_ERASURE_READ_QUORUM.to_string())
    );
}

#[test]
fn test_put_object_no_quorum_line_740() {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    let initial = vec![b'a'; SMALL_FILE_THRESHOLD * 16];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&initial),
            ObjectOptions::default(),
        )
        .expect("put initial");

    remove_disks(&roots, &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_put_no_quorum(&layer, SMALL_FILE_THRESHOLD * 16, 0);
    assert_put_no_quorum(&layer, SMALL_FILE_THRESHOLD * 16, 1);
}

#[test]
fn test_put_object_no_quorum_small_line_803() {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    let initial = vec![b'a'; SMALL_FILE_THRESHOLD / 2];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&initial),
            ObjectOptions::default(),
        )
        .expect("put initial");

    remove_disks(&roots, &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_put_no_quorum(&layer, SMALL_FILE_THRESHOLD / 2, 0);
}

#[test]
fn subtest_test_put_object_no_quorum_small_exec_line_840() {
    let (layer, roots) = new_erasure_layer(16);
    layer
        .make_bucket("bucket", MakeBucketOptions::default())
        .expect("make bucket");
    let initial = vec![b'a'; SMALL_FILE_THRESHOLD / 2];
    layer
        .put_object(
            "bucket",
            "object",
            &reader(&initial),
            ObjectOptions::default(),
        )
        .expect("put initial");

    remove_disks(&roots, &[0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_put_no_quorum(&layer, SMALL_FILE_THRESHOLD / 2, 1);
}

#[test]
fn test_get_object_inline_not_inline_line_1131() {
    let (layer, roots) = new_erasure_layer(4);
    layer
        .make_bucket("testbucket", MakeBucketOptions::default())
        .expect("make bucket");

    let small = vec![b'a'; 16];
    layer
        .put_object(
            "testbucket",
            "file",
            &reader(&small),
            ObjectOptions::default(),
        )
        .expect("put small");

    let moved = take_disks_offline(&roots, &[0]);
    let big = vec![b'b'; SMALL_FILE_THRESHOLD * 2];
    layer
        .put_object(
            "testbucket",
            "file",
            &reader(&big),
            ObjectOptions::default(),
        )
        .expect("put big");
    restore_disks(moved);

    assert_eq!(
        layer.get_object("testbucket", "file").expect("get object"),
        big
    );
}

#[test]
fn test_get_object_with_outdated_disks_line_1188() {
    let (layer, roots) = new_erasure_layer(6);
    let cases = [
        ("bucket1", false, "object1", vec![b'a'; 16]),
        (
            "bucket2",
            false,
            "object2",
            vec![b'a'; SMALL_FILE_THRESHOLD * 2],
        ),
        ("bucket3", true, "version1", vec![b'a'; 16]),
        (
            "bucket4",
            true,
            "version2",
            vec![b'a'; SMALL_FILE_THRESHOLD * 2],
        ),
    ];

    for (bucket, versioned, object, final_content) in cases {
        layer
            .make_bucket(
                bucket,
                MakeBucketOptions {
                    versioning_enabled: versioned,
                },
            )
            .expect("make bucket");

        let initial = vec![b'b'; final_content.len()];
        layer
            .put_object(
                bucket,
                object,
                &reader(&initial),
                ObjectOptions {
                    versioned,
                    ..ObjectOptions::default()
                },
            )
            .expect("put initial");

        let moved = take_disks_offline(&roots, &[0, 1]);
        layer
            .put_object(
                bucket,
                object,
                &reader(&final_content),
                ObjectOptions {
                    versioned,
                    ..ObjectOptions::default()
                },
            )
            .expect("put final");
        restore_disks(moved);

        assert_eq!(
            layer
                .get_object(bucket, object)
                .expect("read latest object"),
            final_content
        );
        if versioned {
            assert!(
                version_ids(&layer, bucket, object).len() >= 2,
                "{bucket}/{object} should retain older versions"
            );
        }
    }
}
