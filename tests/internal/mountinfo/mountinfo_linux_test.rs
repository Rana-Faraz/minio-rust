use std::io::Cursor;

use minio_rust::internal::mountinfo::{
    check_cross_device_with_mounts, parse_mount_from, read_proc_mounts, MountInfo,
};

pub const SOURCE_FILE: &str = "internal/mountinfo/mountinfo_linux_test.go";

fn success_case() -> &'static str {
    "/dev/0 /path/to/0/1 type0 flags 0 0\n\
     /dev/1    /path/to/1   type1\tflags 1 1\n\
     /dev/2 /path/to/1/2 type2 flags,1,2=3 2 2\n\
     /dev/3 /path/to/1.1 type3 flags,1,2=3 3 3\n"
}

#[test]
fn cross_device_mount_paths_match_reference_behavior() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mounts_path = dir.path().join("mounts");
    std::fs::write(&mounts_path, success_case()).expect("write mounts");

    let err = check_cross_device_with_mounts(&["/path/to/1"], &mounts_path)
        .expect_err("should detect cross mount");
    assert_eq!(
        err.to_string(),
        "Cross-device mounts detected on path (/path/to/1) at following locations [/path/to/1/2]. Export path should not have any sub-mounts, refusing to start."
    );

    let err = check_cross_device_with_mounts(&["."], &mounts_path)
        .expect_err("relative path should fail");
    assert_eq!(
        err.to_string(),
        "Invalid argument, path (.) is expected to be absolute"
    );

    check_cross_device_with_mounts(&["/path/to/x"], &mounts_path).expect("no cross mounts");
}

#[test]
fn cross_device_mount_match_reference_behavior() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mounts_path = dir.path().join("mounts");
    std::fs::write(&mounts_path, success_case()).expect("write mounts");
    let mounts = read_proc_mounts(&mounts_path).expect("mounts");

    let err = mounts
        .check_cross_mounts("/path/to/1")
        .expect_err("should detect cross mount");
    assert_eq!(
        err.to_string(),
        "Cross-device mounts detected on path (/path/to/1) at following locations [/path/to/1/2]. Export path should not have any sub-mounts, refusing to start."
    );

    let err = mounts
        .check_cross_mounts(".")
        .expect_err("relative path should fail");
    assert_eq!(
        err.to_string(),
        "Invalid argument, path (.) is expected to be absolute"
    );

    mounts
        .check_cross_mounts("/path/to/x")
        .expect("no cross mounts");
}

#[test]
fn read_proc_mount_infos_match_reference_behavior() {
    let content = "/dev/0 /path/to/0 type0 flags 0 0\n\
                   /dev/1    /path/to/1   type1\tflags 1 1\n\
                   /dev/2 /path/to/2 type2 flags,1,2=3 2 2\n";
    let dir = tempfile::tempdir().expect("tempdir");
    let mounts_path = dir.path().join("mounts");
    std::fs::write(&mounts_path, content).expect("write mounts");

    let mounts = read_proc_mounts(&mounts_path).expect("mounts");
    assert_eq!(mounts.len(), 3);
    assert_eq!(
        mounts[0],
        MountInfo {
            device: "/dev/0".to_owned(),
            path: "/path/to/0".to_owned(),
            fs_type: "type0".to_owned(),
            options: vec!["flags".to_owned()],
            freq: "0".to_owned(),
            pass: "0".to_owned(),
        }
    );
    assert_eq!(
        mounts[1],
        MountInfo {
            device: "/dev/1".to_owned(),
            path: "/path/to/1".to_owned(),
            fs_type: "type1".to_owned(),
            options: vec!["flags".to_owned()],
            freq: "1".to_owned(),
            pass: "1".to_owned(),
        }
    );
    assert_eq!(
        mounts[2],
        MountInfo {
            device: "/dev/2".to_owned(),
            path: "/path/to/2".to_owned(),
            fs_type: "type2".to_owned(),
            options: vec!["flags".to_owned(), "1".to_owned(), "2=3".to_owned()],
            freq: "2".to_owned(),
            pass: "2".to_owned(),
        }
    );

    let missing = read_proc_mounts(dir.path().join("non-existent"));
    assert!(missing.is_err());
}

#[test]
fn read_proc_mount_from_match_reference_behavior() {
    let content = "/dev/0 /path/to/0 type0 flags 0 0\n\
                   /dev/1    /path/to/1   type1\tflags 1 1\n\
                   /dev/2 /path/to/2 type2 flags,1,2=3 2 2\n";

    let mounts = parse_mount_from(Cursor::new(content)).expect("parse mounts");
    assert_eq!(mounts.len(), 3);
    assert_eq!(mounts[0].path, "/path/to/0");
    assert_eq!(mounts[1].path, "/path/to/1");
    assert_eq!(mounts[2].options, vec!["flags", "1", "2=3"]);

    for invalid in [
        "/dev/1 /path/to/mount type flags a 0\n",
        "/dev/2 /path/to/mount type flags 0 b\n",
    ] {
        assert!(parse_mount_from(Cursor::new(invalid)).is_err());
    }
}
