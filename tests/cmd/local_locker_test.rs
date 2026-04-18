use chrono::{Duration, Utc};
use minio_rust::cmd::LocalLocker;
use minio_rust::internal::dsync::LockArgs;

pub const SOURCE_FILE: &str = "cmd/local-locker_test.go";

fn lock_args(uid: &str, resources: Vec<String>, owner: &str) -> LockArgs {
    LockArgs {
        uid: uid.to_string(),
        resources,
        owner: owner.to_string(),
        source: SOURCE_FILE.to_string(),
        quorum: Some(0),
    }
}

fn populate_read_locks(locks: usize, readers: usize) -> LocalLocker {
    let mut locker = LocalLocker::new_locker();
    for lock_index in 0..locks {
        let resource = format!("read-resource-{lock_index}");
        for reader_index in 0..readers {
            let args = lock_args(
                &format!("reader-{lock_index}-{reader_index}"),
                vec![resource.clone()],
                &format!("owner-{reader_index}"),
            );
            assert!(locker.rlock(&args).unwrap());
        }
    }
    locker
}

fn assert_counts(locker: &LocalLocker, lock_map: usize, lock_uid: usize) {
    assert_eq!(locker.lock_map_len(), lock_map);
    assert_eq!(locker.lock_uid_len(), lock_uid);
}

#[test]
fn test_local_locker_expire_line_31() {
    let mut locker = LocalLocker::new_locker();
    let write_resources = 24usize;
    let read_resources = 24usize;

    for index in 0..write_resources {
        let args = lock_args(
            &format!("write-{index}"),
            vec![format!("w-resource-{index}")],
            "owner",
        );
        assert!(locker.lock(&args).unwrap());
    }

    for index in 0..read_resources {
        let args = lock_args(
            &format!("read-{index}"),
            vec![format!("r-resource-{index}")],
            "owner",
        );
        assert!(locker.rlock(&args).unwrap());
        assert!(locker.rlock(&args).unwrap());
    }

    assert_counts(
        &locker,
        write_resources + read_resources,
        write_resources + read_resources,
    );

    locker.expire_old_locks(Duration::hours(1));
    assert_counts(
        &locker,
        write_resources + read_resources,
        write_resources + read_resources,
    );

    locker.expire_old_locks(Duration::minutes(-1));
    assert_counts(&locker, 0, 0);
}

#[test]
fn test_local_locker_unlock_line_106() {
    let mut locker = LocalLocker::new_locker();
    let write_groups = 16usize;
    let write_width = 3usize;
    let read_resources = 16usize;
    let mut write_resources = Vec::with_capacity(write_groups);
    let mut write_uids = Vec::with_capacity(write_groups);
    let mut read_uids = Vec::with_capacity(read_resources);

    for i in 0..write_groups {
        let resources = (0..write_width)
            .map(|j| format!("write-{i}-{j}"))
            .collect::<Vec<_>>();
        let uid = format!("write-uid-{i}");
        assert!(locker
            .lock(&lock_args(&uid, resources.clone(), "owner"))
            .unwrap());
        write_resources.push(resources);
        write_uids.push(uid);
    }

    for i in 0..read_resources {
        let resource = format!("read-{i}");
        let uid_a = format!("read-a-{i}");
        let uid_b = format!("read-b-{i}");
        assert!(locker
            .rlock(&lock_args(&uid_a, vec![resource.clone()], "owner"))
            .unwrap());
        assert!(locker
            .rlock(&lock_args(&uid_b, vec![resource.clone()], "owner"))
            .unwrap());
        read_uids.push((resource, uid_a, uid_b));
    }

    assert_counts(
        &locker,
        read_resources + write_groups * write_width,
        read_resources * 2 + write_groups * write_width,
    );

    for (resource, uid_a, _) in &read_uids {
        assert!(locker
            .runlock(&lock_args(uid_a, vec![resource.clone()], "owner"))
            .unwrap());
    }

    assert_counts(
        &locker,
        read_resources + write_groups * write_width,
        read_resources + write_groups * write_width,
    );

    for (resource, _, uid_b) in &read_uids {
        assert!(locker
            .runlock(&lock_args(uid_b, vec![resource.clone()], "owner"))
            .unwrap());
    }

    assert_counts(
        &locker,
        write_groups * write_width,
        write_groups * write_width,
    );

    for (resources, uid) in write_resources.iter().zip(write_uids.iter()) {
        assert!(locker
            .unlock(&lock_args(uid, resources.clone(), "owner"))
            .unwrap());
    }

    assert_counts(&locker, 0, 0);
}

fn exercise_expire_old_locks(locks: usize, readers: usize) {
    let mut locker = populate_read_locks(locks, readers);
    assert_counts(&locker, locks, locks * readers);

    locker.expire_old_locks(Duration::hours(1));
    assert_counts(&locker, locks, locks * readers);

    let expired = (Utc::now() - Duration::hours(2))
        .timestamp_nanos_opt()
        .unwrap();
    for index in (0..locks).step_by(2) {
        locker.set_resource_last_refresh_nanos(&format!("read-resource-{index}"), expired);
    }

    locker.expire_old_locks(Duration::hours(1));
    assert!(locker.lock_map_len() > 0);
    assert!(locker.lock_map_len() < locks);
    assert!(locker.lock_uid_len() > 0);
    assert!(locker.lock_uid_len() < locks * readers);

    locker.expire_old_locks(Duration::minutes(-1));
    assert_counts(&locker, 0, 0);
}

#[test]
fn test_local_locker_expire_old_locks_expire_line_263() {
    exercise_expire_old_locks(12, 3);
}

#[test]
fn subtest_test_local_locker_expire_old_locks_expire_fmt_sprintf_d_locks_line_271() {
    exercise_expire_old_locks(8, 2);
}

#[test]
fn subtest_test_local_locker_expire_old_locks_expire_fmt_sprintf_d_read_line_280() {
    exercise_expire_old_locks(6, 4);
}

fn exercise_runlock(locks: usize, readers: usize) {
    let mut locker = populate_read_locks(locks, readers);
    let copied = locker.dup_lock_map().0.unwrap_or_default();
    let mut half = Vec::new();
    let mut rest = Vec::new();

    let mut index = 0usize;
    for (resource, entries) in copied {
        for entry in entries {
            let args = lock_args(&entry.uid, vec![resource.clone()], &entry.owner);
            if index.is_multiple_of(2) {
                half.push(args);
            } else {
                rest.push(args);
            }
            index += 1;
        }
    }

    for args in &half {
        assert!(locker.force_unlock(args).unwrap());
    }

    assert_eq!(locker.lock_uid_len(), locks * readers - half.len());

    for args in &rest {
        assert!(locker.runlock(args).unwrap());
    }

    assert_counts(&locker, 0, 0);
}

#[test]
fn test_local_locker_runlock_line_350() {
    exercise_runlock(12, 3);
}

#[test]
fn subtest_test_local_locker_runlock_fmt_sprintf_d_locks_line_358() {
    exercise_runlock(8, 2);
}

#[test]
fn subtest_test_local_locker_runlock_fmt_sprintf_d_read_line_367() {
    exercise_runlock(6, 4);
}
