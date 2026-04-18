use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use minio_rust::cmd::{
    DeleteOptions, DiskInfoOptions, StorageRestClient, ERR_UNFORMATTED_DISK, XL_STORAGE_FORMAT_FILE,
};

pub const SOURCE_FILE: &str = "cmd/storage-rest_test.go";

static STORAGE_REST_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_storage_root() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let counter = STORAGE_REST_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("minio-rust-storage-rest-{nanos}-{counter}"))
}

fn new_storage_rest_client() -> StorageRestClient {
    let root = temp_storage_root();
    let client = StorageRestClient::new(root.to_str().expect("utf8 path")).expect("client");
    client.make_vol("foo").expect("make foo");
    client.make_vol("bar").expect("make bar");
    client
}

#[test]
fn test_storage_restclient_disk_info_line_367() {
    let client = new_storage_rest_client();
    let err = client
        .disk_info(DiskInfoOptions {
            metrics: true,
            ..DiskInfoOptions::default()
        })
        .unwrap_err();
    assert_eq!(err, ERR_UNFORMATTED_DISK);
}

#[test]
fn test_storage_restclient_stat_info_file_line_373() {
    let client = new_storage_rest_client();
    client
        .append_file("foo", &format!("myobject/{XL_STORAGE_FORMAT_FILE}"), b"foo")
        .expect("append");

    assert!(client
        .stat_info_file("foo", &format!("myobject/{XL_STORAGE_FORMAT_FILE}"), false)
        .is_ok());
    assert!(client
        .stat_info_file(
            "foo",
            &format!("yourobject/{XL_STORAGE_FORMAT_FILE}"),
            false
        )
        .is_err());
}

#[test]
fn test_storage_restclient_list_dir_line_379() {
    let client = new_storage_rest_client();
    client
        .append_file("foo", "path/to/myobject", b"foo")
        .expect("append");

    let listed = client.list_dir("", "foo", "path", -1).expect("list");
    assert_eq!(listed, vec!["to/".to_string()]);
    assert!(client.list_dir("", "foo", "nodir", -1).is_err());
}

#[test]
fn test_storage_restclient_read_all_line_385() {
    let client = new_storage_rest_client();
    client
        .append_file("foo", "myobject", b"foo")
        .expect("append");

    assert_eq!(client.read_all("foo", "myobject").expect("read"), b"foo");
    assert!(client.read_all("foo", "yourobject").is_err());
}

#[test]
fn test_storage_restclient_read_file_line_391() {
    let client = new_storage_rest_client();
    client
        .append_file("foo", "myobject", b"foo")
        .expect("append");

    let mut full = vec![0_u8; 3];
    client
        .read_file("foo", "myobject", 0, &mut full)
        .expect("read full");
    assert_eq!(full, b"foo");

    let mut partial = vec![0_u8; 2];
    client
        .read_file("foo", "myobject", 1, &mut partial)
        .expect("read partial");
    assert_eq!(partial, b"oo");

    let mut missing = vec![0_u8; 3];
    assert!(client
        .read_file("foo", "yourobject", 0, &mut missing)
        .is_err());
}

#[test]
fn test_storage_restclient_append_file_line_397() {
    let client = new_storage_rest_client();
    let mut cases = vec![
        ("foo", "myobject", b"foo".to_vec(), false),
        ("foo", "myobject-0byte", Vec::new(), false),
        ("foo-bar", "myobject", b"foo".to_vec(), true),
    ];

    if !cfg!(windows) {
        cases.extend([
            ("foo", "newline\n", b"foo".to_vec(), false),
            ("foo", "newline\t", b"foo".to_vec(), false),
            ("foo", "newline \n", b"foo".to_vec(), false),
            ("foo", "newline$$$\n", b"foo".to_vec(), false),
            ("foo", "newline%%%\n", b"foo".to_vec(), false),
            ("foo", "newline \t % $ & * ^ # @ \n", b"foo".to_vec(), false),
            (
                "foo",
                "\n\tnewline \t % $ & * ^ # @ \n",
                b"foo".to_vec(),
                false,
            ),
        ]);
    }

    for (volume, object, data, expect_err) in cases {
        let result = client.append_file(volume, object, &data);
        assert_eq!(
            result.is_err(),
            expect_err,
            "unexpected result for {volume}/{object:?}"
        );
        if !expect_err {
            assert_eq!(client.read_all(volume, object).expect("read back"), data);
        }
    }
}

#[test]
fn test_storage_restclient_delete_file_line_403() {
    let client = new_storage_rest_client();
    client
        .append_file("foo", "myobject", b"foo")
        .expect("append");

    let opts = DeleteOptions {
        recursive: false,
        immediate: false,
        ..DeleteOptions::default()
    };
    client
        .delete("foo", "myobject", opts.clone())
        .expect("delete");
    client
        .delete("foo", "myobject", opts.clone())
        .expect("delete again");
    client
        .delete("foo", "yourobject", opts)
        .expect("delete missing");
}

#[test]
fn test_storage_restclient_rename_file_line_409() {
    let client = new_storage_rest_client();
    client
        .append_file("foo", "myobject", b"foo")
        .expect("append");
    client
        .append_file("foo", "otherobject", b"foo")
        .expect("append");

    client
        .rename_file("foo", "myobject", "foo", "yourobject")
        .expect("rename within volume");
    assert_eq!(client.read_all("foo", "yourobject").expect("read"), b"foo");

    client
        .rename_file("foo", "yourobject", "bar", "myobject")
        .expect("rename cross volume");
    assert_eq!(client.read_all("bar", "myobject").expect("read"), b"foo");

    client
        .rename_file("foo", "otherobject", "bar", "myobject")
        .expect("overwrite destination");
    assert_eq!(client.read_all("bar", "myobject").expect("read"), b"foo");
    assert!(client.read_all("foo", "otherobject").is_err());
}
