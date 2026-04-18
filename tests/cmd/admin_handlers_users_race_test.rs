// Rust test snapshot derived from cmd/admin-handlers-users-race_test.go.

use std::sync::{Arc, Mutex};
use std::thread;

use minio_rust::cmd::{AccountStatus, AdminUsers};

pub const SOURCE_FILE: &str = "cmd/admin-handlers-users-race_test.go";

#[test]
fn test_iaminternal_idpconcurrency_server_suite_line_44() {
    let iam = Arc::new(Mutex::new(AdminUsers::new(false)));
    {
        let mut iam_locked = iam.lock().expect("lock");
        iam_locked.add_root_bucket("race-bucket");
        for idx in 0..20 {
            let access = format!("user-{idx}");
            let secret = format!("secret-{idx}");
            iam_locked
                .set_user(&access, &secret, AccountStatus::Enabled)
                .expect("set user");
            iam_locked
                .attach_policy(&access, "readwrite")
                .expect("policy");
        }
    }

    let mut handles = Vec::new();
    for idx in 0..20 {
        let iam = Arc::clone(&iam);
        handles.push(thread::spawn(move || {
            let access = format!("user-{idx}");
            let secret = format!("secret-{idx}");
            let mut iam_locked = iam.lock().expect("lock");
            iam_locked.remove_user(&access).expect("remove");
            assert!(iam_locked.list_buckets(&access, &secret).is_err());
        }));
    }
    for handle in handles {
        handle.join().expect("join");
    }
}

#[test]
fn subtest_test_iaminternal_idpconcurrency_server_suite_line_71() {
    for plugin_mode in [false, true] {
        let iam = Arc::new(Mutex::new(AdminUsers::new(plugin_mode)));
        {
            let mut iam_locked = iam.lock().expect("lock");
            iam_locked.add_root_bucket("subtest-bucket");
            iam_locked
                .set_user("user", "secret", AccountStatus::Enabled)
                .expect("set user");
            iam_locked
                .attach_policy("user", "readwrite")
                .expect("policy");
        }
        let iam_clone = Arc::clone(&iam);
        thread::spawn(move || {
            let mut locked = iam_clone.lock().expect("lock");
            locked.remove_user("user").expect("remove");
        })
        .join()
        .expect("join");
        assert!(iam
            .lock()
            .expect("lock")
            .list_buckets("user", "secret")
            .is_err());
    }
}
