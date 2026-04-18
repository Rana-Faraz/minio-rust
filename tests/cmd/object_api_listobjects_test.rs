// Rust test snapshot derived from cmd/object-api-listobjects_test.go.

use std::path::PathBuf;

use tempfile::TempDir;

use minio_rust::cmd::{LocalObjectLayer, MakeBucketOptions, ObjectOptions, PutObjReader};

pub const SOURCE_FILE: &str = "cmd/object-api-listobjects_test.go";

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

fn must_make_bucket_opts(layer: &LocalObjectLayer, bucket: &str, versioning_enabled: bool) {
    layer
        .make_bucket(bucket, MakeBucketOptions { versioning_enabled })
        .expect("make bucket");
}

fn put_object(layer: &LocalObjectLayer, bucket: &str, object: &str, data: &[u8]) {
    put_object_opts(layer, bucket, object, data, ObjectOptions::default());
}

fn put_object_opts(
    layer: &LocalObjectLayer,
    bucket: &str,
    object: &str,
    data: &[u8],
    opts: ObjectOptions,
) {
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
            opts,
        )
        .expect("put object");
}

#[test]
fn test_list_objects_versioned_folders_line_34() {
    let (layer, _dirs) = new_object_layer(4);
    let folders_bucket = "test-bucket-folders";
    let files_bucket = "test-bucket-files";
    must_make_bucket_opts(&layer, folders_bucket, true);
    must_make_bucket_opts(&layer, files_bucket, true);

    put_object(&layer, folders_bucket, "unique/folder/", b"");
    put_object(&layer, folders_bucket, "unique/folder/1.txt", b"content");
    let deleted_folder = layer
        .delete_object(folders_bucket, "unique/folder/", ObjectOptions::default())
        .expect("delete folder marker");
    assert!(deleted_folder.delete_marker);

    put_object(&layer, files_bucket, "unique/folder/1.txt", b"content");
    let deleted_file = layer
        .delete_object(
            files_bucket,
            "unique/folder/1.txt",
            ObjectOptions::default(),
        )
        .expect("delete file marker");
    assert!(deleted_file.delete_marker);

    let prefix_listing = layer
        .list_objects(folders_bucket, "unique/", "", "/", 1000)
        .expect("list objects");
    assert!(prefix_listing.objects.is_empty());
    assert_eq!(prefix_listing.prefixes, vec!["unique/folder/"]);

    let recursive_listing = layer
        .list_objects(folders_bucket, "unique/", "", "", 1000)
        .expect("list objects");
    assert_eq!(
        recursive_listing
            .objects
            .iter()
            .map(|object| object.name.as_str())
            .collect::<Vec<_>>(),
        vec!["unique/folder/1.txt"]
    );

    let deleted_file_listing = layer
        .list_objects(files_bucket, "unique/folder/", "", "/", 1000)
        .expect("list objects");
    assert!(deleted_file_listing.objects.is_empty());
    assert!(deleted_file_listing.prefixes.is_empty());

    let version_listing = layer
        .list_object_versions(folders_bucket, "unique/", "", "", "", 1000)
        .expect("list object versions");
    assert_eq!(
        version_listing
            .objects
            .iter()
            .map(|object| (object.name.as_str(), object.delete_marker))
            .collect::<Vec<_>>(),
        vec![
            ("unique/folder/", true),
            ("unique/folder/", false),
            ("unique/folder/1.txt", false),
        ]
    );

    let version_prefix_listing = layer
        .list_object_versions(folders_bucket, "unique/", "", "", "/", 1000)
        .expect("list object versions");
    assert!(version_prefix_listing.objects.is_empty());
    assert_eq!(version_prefix_listing.prefixes, vec!["unique/folder/"]);
}

#[test]
fn subtest_file_scope_fmt_sprintf_s_test_d_line_159() {
    let (layer, _dirs) = new_object_layer(2);
    must_make_bucket_opts(&layer, "versioned-subtest-bucket", true);
    put_object(&layer, "versioned-subtest-bucket", "unique/folder/", b"");
    put_object(
        &layer,
        "versioned-subtest-bucket",
        "unique/folder/1.txt",
        b"content",
    );
    let result = layer
        .list_object_versions("versioned-subtest-bucket", "unique/", "", "", "", 1000)
        .expect("list object versions");
    assert_eq!(result.objects.len(), 2);
}

#[test]
fn test_list_objects_on_versioned_buckets_line_301() {
    let (layer, _dirs) = new_object_layer(4);
    let list_bucket = "versioned-list";
    let empty_dir_bucket = "versioned-empty-dir";
    let single_object_bucket = "versioned-single-object";
    let delimiter_bucket = "versioned-delimiter";
    let max_keys_bucket = "versioned-max-keys-prefixes";

    for bucket in [
        list_bucket,
        empty_dir_bucket,
        single_object_bucket,
        delimiter_bucket,
        max_keys_bucket,
    ] {
        must_make_bucket_opts(&layer, bucket, true);
    }

    for (bucket, object, data) in [
        (list_bucket, "Asia-maps.png", b"asis-maps".as_slice()),
        (
            list_bucket,
            "Asia/India/India-summer-photos-1",
            b"contentstring".as_slice(),
        ),
        (
            list_bucket,
            "Asia/India/Karnataka/Bangalore/Koramangala/pics",
            b"contentstring".as_slice(),
        ),
        (list_bucket, "newPrefix0", b"newPrefix0".as_slice()),
        (list_bucket, "newPrefix1", b"newPrefix1".as_slice()),
        (list_bucket, "obj0", b"obj0".as_slice()),
        (list_bucket, "obj1", b"obj1".as_slice()),
        (list_bucket, "obj2", b"obj2".as_slice()),
        (empty_dir_bucket, "obj1", b"obj1".as_slice()),
        (empty_dir_bucket, "obj2", b"obj2".as_slice()),
        (empty_dir_bucket, "temporary/0/", b"".as_slice()),
        (single_object_bucket, "A/B", b"contentstring".as_slice()),
        (
            delimiter_bucket,
            "file1/receipt.json",
            b"content".as_slice(),
        ),
        (
            delimiter_bucket,
            "file1/guidSplunk-aaaa/file",
            b"content".as_slice(),
        ),
        (
            max_keys_bucket,
            "dir/day_id=2017-10-10/issue",
            b"content".as_slice(),
        ),
        (
            max_keys_bucket,
            "dir/day_id=2017-10-11/issue",
            b"content".as_slice(),
        ),
    ] {
        put_object(&layer, bucket, object, data);
    }

    let cases = [
        (
            list_bucket,
            "",
            "",
            "",
            10,
            vec![
                "Asia-maps.png",
                "Asia/India/India-summer-photos-1",
                "Asia/India/Karnataka/Bangalore/Koramangala/pics",
                "newPrefix0",
                "newPrefix1",
                "obj0",
                "obj1",
                "obj2",
            ],
            Vec::<&str>::new(),
            false,
        ),
        (
            list_bucket,
            "Asia",
            "",
            "/",
            10,
            vec!["Asia-maps.png"],
            vec!["Asia/"],
            false,
        ),
        (
            empty_dir_bucket,
            "",
            "",
            "/",
            10,
            vec!["obj1", "obj2"],
            vec!["temporary/"],
            false,
        ),
        (
            single_object_bucket,
            "",
            "A/C",
            "",
            1000,
            Vec::<&str>::new(),
            Vec::<&str>::new(),
            false,
        ),
        (
            delimiter_bucket,
            "",
            "",
            "guidSplunk",
            1000,
            vec!["file1/receipt.json"],
            vec!["file1/guidSplunk"],
            false,
        ),
        (
            max_keys_bucket,
            "dir/",
            "",
            "/",
            1,
            Vec::<&str>::new(),
            vec!["dir/day_id=2017-10-10/"],
            true,
        ),
    ];

    for (
        bucket,
        prefix,
        marker,
        delimiter,
        max_keys,
        expected_objects,
        expected_prefixes,
        truncated,
    ) in cases
    {
        let result = layer
            .list_objects(bucket, prefix, marker, delimiter, max_keys)
            .expect("list versioned objects");
        let object_names: Vec<String> = result
            .objects
            .iter()
            .map(|object| object.name.clone())
            .collect();
        assert_eq!(object_names, expected_objects);
        assert_eq!(result.prefixes, expected_prefixes);
        assert_eq!(result.is_truncated, truncated);
    }
}

#[test]
fn test_list_objects_line_307() {
    let (layer, _dirs) = new_object_layer(4);
    let list_bucket = "test-bucket-list-object";
    let empty_dir_bucket = "test-bucket-empty-dir";
    let empty_bucket = "empty-bucket";
    let single_object_bucket = "test-bucket-single-object";
    let delimiter_bucket = "test-bucket-delimiter";
    let max_keys_bucket = "test-bucket-max-keys-prefixes";
    let custom_delimiter_bucket = "test-bucket-custom-delimiter";

    for bucket in [
        list_bucket,
        empty_dir_bucket,
        empty_bucket,
        single_object_bucket,
        delimiter_bucket,
        max_keys_bucket,
        custom_delimiter_bucket,
    ] {
        must_make_bucket(&layer, bucket);
    }

    for (bucket, object, data) in [
        (list_bucket, "Asia-maps.png", b"asis-maps".as_slice()),
        (
            list_bucket,
            "Asia/India/India-summer-photos-1",
            b"contentstring".as_slice(),
        ),
        (
            list_bucket,
            "Asia/India/Karnataka/Bangalore/Koramangala/pics",
            b"contentstring".as_slice(),
        ),
        (list_bucket, "newPrefix0", b"newPrefix0".as_slice()),
        (list_bucket, "newPrefix1", b"newPrefix1".as_slice()),
        (
            list_bucket,
            "newzen/zen/recurse/again/again/again/pics",
            b"recurse".as_slice(),
        ),
        (list_bucket, "obj0", b"obj0".as_slice()),
        (list_bucket, "obj1", b"obj1".as_slice()),
        (list_bucket, "obj2", b"obj2".as_slice()),
        (empty_dir_bucket, "obj1", b"obj1".as_slice()),
        (empty_dir_bucket, "obj2", b"obj2".as_slice()),
        (empty_dir_bucket, "temporary/0/", b"".as_slice()),
        (single_object_bucket, "A/B", b"contentstring".as_slice()),
        (
            delimiter_bucket,
            "file1/receipt.json",
            b"content".as_slice(),
        ),
        (
            delimiter_bucket,
            "file1/guidSplunk-aaaa/file",
            b"content".as_slice(),
        ),
        (
            max_keys_bucket,
            "dir/day_id=2017-10-10/issue",
            b"content".as_slice(),
        ),
        (
            max_keys_bucket,
            "dir/day_id=2017-10-11/issue",
            b"content".as_slice(),
        ),
        (max_keys_bucket, "foo/201910/1122", b"content".as_slice()),
        (max_keys_bucket, "foo/201910/1112", b"content".as_slice()),
        (max_keys_bucket, "foo/201910/2112", b"content".as_slice()),
        (max_keys_bucket, "foo/201910_txt", b"content".as_slice()),
        (
            max_keys_bucket,
            "201910/foo/bar/xl.meta/1.txt",
            b"content".as_slice(),
        ),
        (custom_delimiter_bucket, "aaa", b"content".as_slice()),
        (custom_delimiter_bucket, "bbb_aaa", b"content".as_slice()),
        (custom_delimiter_bucket, "ccc", b"content".as_slice()),
    ] {
        put_object(&layer, bucket, object, data);
    }

    let cases = [
        (
            list_bucket,
            "",
            "",
            "",
            10,
            vec![
                "Asia-maps.png",
                "Asia/India/India-summer-photos-1",
                "Asia/India/Karnataka/Bangalore/Koramangala/pics",
                "newPrefix0",
                "newPrefix1",
                "newzen/zen/recurse/again/again/again/pics",
                "obj0",
                "obj1",
                "obj2",
            ],
            Vec::<&str>::new(),
            false,
        ),
        (
            list_bucket,
            "",
            "",
            "",
            5,
            vec![
                "Asia-maps.png",
                "Asia/India/India-summer-photos-1",
                "Asia/India/Karnataka/Bangalore/Koramangala/pics",
                "newPrefix0",
                "newPrefix1",
            ],
            Vec::<&str>::new(),
            true,
        ),
        (
            list_bucket,
            "new",
            "",
            "",
            3,
            vec![
                "newPrefix0",
                "newPrefix1",
                "newzen/zen/recurse/again/again/again/pics",
            ],
            Vec::<&str>::new(),
            false,
        ),
        (
            list_bucket,
            "",
            "obj0",
            "",
            4,
            vec!["obj1", "obj2"],
            Vec::<&str>::new(),
            false,
        ),
        (
            list_bucket,
            "Asia",
            "",
            "/",
            10,
            vec!["Asia-maps.png"],
            vec!["Asia/"],
            false,
        ),
        (
            empty_dir_bucket,
            "",
            "",
            "/",
            10,
            vec!["obj1", "obj2"],
            vec!["temporary/"],
            false,
        ),
        (
            single_object_bucket,
            "",
            "A/C",
            "",
            1000,
            Vec::<&str>::new(),
            Vec::<&str>::new(),
            false,
        ),
        (
            delimiter_bucket,
            "",
            "",
            "guidSplunk",
            1000,
            vec!["file1/receipt.json"],
            vec!["file1/guidSplunk"],
            false,
        ),
        (
            max_keys_bucket,
            "dir/",
            "",
            "/",
            1,
            Vec::<&str>::new(),
            vec!["dir/day_id=2017-10-10/"],
            true,
        ),
        (
            list_bucket,
            "",
            "obj1",
            "",
            0,
            Vec::<&str>::new(),
            Vec::<&str>::new(),
            false,
        ),
        (
            custom_delimiter_bucket,
            "",
            "",
            "_",
            1000,
            vec!["aaa", "ccc"],
            vec!["bbb_"],
            false,
        ),
    ];

    for (
        bucket,
        prefix,
        marker,
        delimiter,
        max_keys,
        expected_objects,
        expected_prefixes,
        truncated,
    ) in cases
    {
        let result = layer
            .list_objects(bucket, prefix, marker, delimiter, max_keys)
            .expect("list objects");
        let object_names: Vec<String> = result
            .objects
            .iter()
            .map(|object| object.name.clone())
            .collect();
        let prefixes = result.prefixes.clone();
        assert_eq!(object_names, expected_objects, "bucket={bucket} prefix={prefix} marker={marker} delimiter={delimiter} max_keys={max_keys}");
        assert_eq!(prefixes, expected_prefixes, "bucket={bucket} prefix={prefix} marker={marker} delimiter={delimiter} max_keys={max_keys}");
        assert_eq!(result.is_truncated, truncated, "bucket={bucket} prefix={prefix} marker={marker} delimiter={delimiter} max_keys={max_keys}");
        if truncated {
            assert!(!result.next_marker.is_empty());
        }
    }
}

#[test]
fn subtest_file_scope_fmt_sprintf_s_test_d_line_946() {
    let (layer, _dirs) = new_object_layer(2);
    must_make_bucket_opts(&layer, "versioned-listing-subtest", true);
    put_object(&layer, "versioned-listing-subtest", "obj0", b"obj0");
    put_object(&layer, "versioned-listing-subtest", "obj1", b"obj1");
    let result = layer
        .list_objects("versioned-listing-subtest", "", "obj0", "", 10)
        .expect("list objects");
    assert_eq!(
        result
            .objects
            .iter()
            .map(|object| object.name.as_str())
            .collect::<Vec<_>>(),
        vec!["obj1"]
    );
}

#[test]
fn test_delete_object_version_marker_line_1030() {
    let (layer, _dirs) = new_object_layer(4);
    let marker_bucket = "bucket-suspended-version";
    let null_bucket = "bucket-suspended-version-id";
    must_make_bucket_opts(&layer, marker_bucket, true);
    must_make_bucket_opts(&layer, null_bucket, true);

    put_object(&layer, marker_bucket, "delete-file", b"contentstring");
    let delete_marker = layer
        .delete_object(marker_bucket, "delete-file", ObjectOptions::default())
        .expect("delete marker");
    assert!(delete_marker.delete_marker);

    put_object_opts(
        &layer,
        null_bucket,
        "delete-file",
        b"contentstring",
        ObjectOptions {
            version_suspended: true,
            ..ObjectOptions::default()
        },
    );
    let null_delete = layer
        .delete_object(
            null_bucket,
            "delete-file",
            ObjectOptions {
                version_suspended: true,
                version_id: "null".to_string(),
                ..ObjectOptions::default()
            },
        )
        .expect("delete null version");
    assert!(!null_delete.delete_marker);
}

#[test]
fn test_list_object_versions_line_1106() {
    let (layer, _dirs) = new_object_layer(4);
    let list_bucket = "test-bucket-list-object";
    let empty_dir_bucket = "test-bucket-empty-dir";
    let empty_bucket = "empty-bucket";
    let single_object_bucket = "test-bucket-single-object";
    let delimiter_bucket = "test-bucket-delimiter";
    let max_keys_bucket = "test-bucket-max-keys-prefixes";

    for bucket in [
        list_bucket,
        empty_dir_bucket,
        empty_bucket,
        single_object_bucket,
        delimiter_bucket,
        max_keys_bucket,
    ] {
        must_make_bucket_opts(&layer, bucket, true);
    }

    for (bucket, object, data) in [
        (list_bucket, "Asia-maps.png", b"asis-maps".as_slice()),
        (
            list_bucket,
            "Asia/India/India-summer-photos-1",
            b"contentstring".as_slice(),
        ),
        (
            list_bucket,
            "Asia/India/Karnataka/Bangalore/Koramangala/pics",
            b"contentstring".as_slice(),
        ),
        (list_bucket, "newPrefix0", b"newPrefix0".as_slice()),
        (list_bucket, "newPrefix1", b"newPrefix1".as_slice()),
        (list_bucket, "obj0", b"obj0".as_slice()),
        (list_bucket, "obj1", b"obj1".as_slice()),
        (list_bucket, "obj2", b"obj2".as_slice()),
        (empty_dir_bucket, "obj1", b"obj1".as_slice()),
        (empty_dir_bucket, "obj2", b"obj2".as_slice()),
        (empty_dir_bucket, "temporary/0/", b"".as_slice()),
        (single_object_bucket, "A/B", b"contentstring".as_slice()),
        (
            delimiter_bucket,
            "file1/receipt.json",
            b"content".as_slice(),
        ),
        (
            delimiter_bucket,
            "file1/guidSplunk-aaaa/file",
            b"content".as_slice(),
        ),
        (
            max_keys_bucket,
            "dir/day_id=2017-10-10/issue",
            b"content".as_slice(),
        ),
        (
            max_keys_bucket,
            "dir/day_id=2017-10-11/issue",
            b"content".as_slice(),
        ),
    ] {
        put_object(&layer, bucket, object, data);
    }

    let cases = [
        (
            list_bucket,
            "",
            "",
            "",
            5,
            vec![
                "Asia-maps.png",
                "Asia/India/India-summer-photos-1",
                "Asia/India/Karnataka/Bangalore/Koramangala/pics",
                "newPrefix0",
                "newPrefix1",
            ],
            Vec::<&str>::new(),
            true,
        ),
        (
            list_bucket,
            "new",
            "",
            "",
            3,
            vec!["newPrefix0", "newPrefix1"],
            Vec::<&str>::new(),
            false,
        ),
        (
            empty_dir_bucket,
            "",
            "",
            "/",
            10,
            vec!["obj1", "obj2"],
            vec!["temporary/"],
            false,
        ),
        (
            delimiter_bucket,
            "",
            "",
            "guidSplunk",
            1000,
            vec!["file1/receipt.json"],
            vec!["file1/guidSplunk"],
            false,
        ),
    ];

    for (
        bucket,
        prefix,
        marker,
        delimiter,
        max_keys,
        expected_objects,
        expected_prefixes,
        truncated,
    ) in cases
    {
        let result = layer
            .list_object_versions(bucket, prefix, marker, "", delimiter, max_keys)
            .expect("list object versions");
        let object_names: Vec<String> = result
            .objects
            .iter()
            .map(|object| object.name.clone())
            .collect();
        assert_eq!(object_names, expected_objects);
        assert_eq!(result.prefixes, expected_prefixes);
        assert_eq!(result.is_truncated, truncated);
    }
}

#[test]
fn subtest_file_scope_fmt_sprintf_s_test_d_line_1677() {
    let (layer, _dirs) = new_object_layer(2);
    must_make_bucket_opts(&layer, "version-list-subtest", true);
    put_object(&layer, "version-list-subtest", "a/1.txt", b"1");
    put_object(&layer, "version-list-subtest", "a/2.txt", b"2");
    let result = layer
        .list_object_versions("version-list-subtest", "a/", "", "", "", 1000)
        .expect("list object versions");
    assert_eq!(result.objects.len(), 2);
}

#[test]
fn test_list_objects_continuation_line_1742() {
    let (layer, _dirs) = new_object_layer(4);
    let bucket1 = "test-bucket-list-object-continuation-1";
    let bucket2 = "test-bucket-list-object-continuation-2";
    must_make_bucket(&layer, bucket1);
    must_make_bucket(&layer, bucket2);

    for (bucket, object) in [
        (bucket1, "a/1.txt"),
        (bucket1, "a-1.txt"),
        (bucket1, "a.txt"),
        (bucket1, "apache2-doc/1.txt"),
        (bucket1, "apache2/1.txt"),
        (bucket1, "apache2/-sub/2.txt"),
        (bucket2, "azerty/1.txt"),
        (bucket2, "apache2-doc/1.txt"),
        (bucket2, "apache2/1.txt"),
    ] {
        put_object(&layer, bucket, object, b"contentstring");
    }

    let cases = [
        (
            bucket1,
            "",
            "",
            vec![
                "a-1.txt",
                "a.txt",
                "a/1.txt",
                "apache2-doc/1.txt",
                "apache2/-sub/2.txt",
                "apache2/1.txt",
            ],
            Vec::<&str>::new(),
        ),
        (
            bucket1,
            "a",
            "",
            vec![
                "a-1.txt",
                "a.txt",
                "a/1.txt",
                "apache2-doc/1.txt",
                "apache2/-sub/2.txt",
                "apache2/1.txt",
            ],
            Vec::<&str>::new(),
        ),
        (
            bucket2,
            "apache",
            "",
            vec!["apache2-doc/1.txt", "apache2/1.txt"],
            Vec::<&str>::new(),
        ),
        (
            bucket2,
            "",
            "/",
            Vec::<&str>::new(),
            vec!["apache2-doc/", "apache2/", "azerty/"],
        ),
    ];

    for (bucket, prefix, delimiter, expected_objects, expected_prefixes) in cases {
        let result = layer
            .list_objects(bucket, prefix, "", delimiter, 1000)
            .expect("list objects continuation");
        let object_names: Vec<String> = result
            .objects
            .iter()
            .map(|object| object.name.clone())
            .collect();
        assert_eq!(
            object_names, expected_objects,
            "bucket={bucket} prefix={prefix} delimiter={delimiter}"
        );
        assert_eq!(
            result.prefixes, expected_prefixes,
            "bucket={bucket} prefix={prefix} delimiter={delimiter}"
        );
        assert!(!result.is_truncated);
    }
}

#[test]
fn subtest_file_scope_fmt_sprintf_s_test_d_line_1827() {
    let (layer, _dirs) = new_object_layer(2);
    must_make_bucket(&layer, "continuation-subtest");
    for object in ["a/1.txt", "a/2.txt", "b/1.txt"] {
        put_object(&layer, "continuation-subtest", object, b"x");
    }
    let result = layer
        .list_objects("continuation-subtest", "", "", "/", 10)
        .expect("list objects");
    assert_eq!(result.prefixes, vec!["a/", "b/"]);
}

#[test]
fn benchmark_list_objects_line_1898() {
    let (layer, _dirs) = new_object_layer(2);
    must_make_bucket(&layer, "benchmark-list");
    for idx in 0..128 {
        put_object(
            &layer,
            "benchmark-list",
            &format!("obj{idx:04}"),
            b"content",
        );
    }
    for _ in 0..5 {
        let result = layer
            .list_objects("benchmark-list", "", "obj0090", "", 1000)
            .expect("list objects");
        assert!(!result.objects.is_empty());
    }
}

#[test]
fn test_list_objects_with_ilm_line_1930() {
    let (layer, _dirs) = new_object_layer(4);
    let one_week_ago = chrono::Utc::now().timestamp() - (7 * 24 * 60 * 60);
    let uploads = [
        ("test-list-ilm-nothing-expired", 0, 6),
        ("test-list-ilm-all-expired", 6, 0),
        ("test-list-ilm-all-half-expired", 3, 3),
    ];

    for (bucket, expired, not_expired) in uploads {
        must_make_bucket(&layer, bucket);
        layer
            .set_bucket_expiration_days(bucket, 1)
            .expect("set bucket expiration");

        for idx in 0..expired {
            put_object_opts(
                &layer,
                bucket,
                &format!("expired-{idx}"),
                b"test-content",
                ObjectOptions {
                    mtime: Some(one_week_ago),
                    ..ObjectOptions::default()
                },
            );
        }
        for idx in 0..not_expired {
            put_object(&layer, bucket, &format!("fresh-{idx}"), b"test-content");
        }

        for max_keys in [1, 10, 49] {
            let mut total_objects = 0;
            let mut marker = String::new();
            let mut runs = 0;
            loop {
                runs += 1;
                assert!(runs < 1000);
                let result = layer
                    .list_objects_v2(bucket, "", &marker, "", max_keys, false, "")
                    .expect("list objects v2");
                total_objects += result.objects.len();
                if !result.is_truncated {
                    break;
                }
                assert_ne!(marker, result.next_continuation_token);
                marker = result.next_continuation_token;
            }
            assert_eq!(
                total_objects, not_expired as usize,
                "bucket={bucket} max_keys={max_keys}"
            );
        }
    }
}
