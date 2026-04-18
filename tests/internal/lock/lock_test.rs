use std::fs::{self, File};
use std::sync::mpsc;
use std::time::Duration;

use minio_rust::internal::lock::{locked_open_file, new_rlocked_file, rlocked_open_file};

pub const SOURCE_FILE: &str = "internal/lock/lock_test.go";

#[test]
fn lock_fail_matches_reference() {
    let temp = tempfile::NamedTempFile::new().expect("tempfile");
    let result = locked_open_file(temp.path(), libc::O_APPEND, 0o600);
    assert!(result.is_err());
}

#[test]
fn lock_dir_fail_matches_reference() {
    let dir = tempfile::tempdir().expect("tempdir");
    let result = locked_open_file(dir.path(), libc::O_APPEND, 0o600);
    assert!(result.is_err());
}

#[test]
fn rwlocked_file_matches_reference_behavior() {
    let temp = tempfile::NamedTempFile::new().expect("tempfile");
    drop(temp);
    let temp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = temp.path().to_path_buf();
    drop(temp);
    File::create(&path).expect("file should exist");

    let mut locked = rlocked_open_file(&path).expect("read lock should open");
    assert!(!locked.is_closed());

    locked.inc_lock_ref();
    assert!(!locked.is_closed());

    locked.close().expect("first close");
    assert!(!locked.is_closed());

    locked.close().expect("second close");
    assert!(locked.is_closed());

    let err = locked.close().expect_err("third close should fail");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let err = match new_rlocked_file(None) {
        Ok(_) => panic!("nil file should fail"),
        Err(error) => error,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let _ = fs::remove_file(path);
}

#[test]
fn lock_and_unlock_matches_reference_behavior() {
    let temp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = temp.path().to_path_buf();
    drop(temp);
    File::create(&path).expect("file should exist");

    let mut locked = locked_open_file(&path, libc::O_WRONLY, 0o600).expect("first lock");
    locked.close().expect("unlock");

    let mut duplicate =
        locked_open_file(&path, libc::O_WRONLY | libc::O_CREAT, 0o600).expect("second lock");

    let path_for_thread = path.clone();
    let (tx, rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let outcome = locked_open_file(&path_for_thread, libc::O_WRONLY, 0o600)
            .map(|mut file| {
                let _ = file.close();
            })
            .map_err(|error| error.to_string());
        let _ = tx.send(outcome);
    });

    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
    duplicate.close().expect("unlock duplicate");

    let outcome = rx
        .recv_timeout(Duration::from_secs(1))
        .expect("thread should unblock");
    assert!(outcome.is_ok());

    let _ = fs::remove_file(path);
}
