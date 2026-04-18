use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use minio_rust::cmd::{get_source, NamespaceLockMap};

pub const SOURCE_FILE: &str = "cmd/namespace-lock_test.go";

#[test]
fn test_get_source_line_32() {
    let source = get_source(0);
    assert!(source.contains("tests/cmd/namespace_lock_test.rs"));
    assert!(source.contains(':'));
}

#[test]
fn test_nslock_race_line_43() {
    let locks = NamespaceLockMap::new(4);
    let active = Arc::new(AtomicUsize::new(0));
    let violations = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));

    let mut joins = Vec::new();
    for idx in 0..8 {
        let locks = locks.clone();
        let active = Arc::clone(&active);
        let violations = Arc::clone(&violations);
        let completed = Arc::clone(&completed);
        joins.push(thread::spawn(move || {
            let mutex = locks.new_lock("bucket", "object");
            let source = format!("worker-{idx}");
            let ok = mutex.get_lock(
                "test-id",
                &source,
                minio_rust::internal::dsync::Options {
                    timeout: Duration::from_secs(2),
                    retry_interval: Some(Duration::from_millis(5)),
                },
                None,
            );
            assert!(ok, "must acquire lock");
            let previous = active.fetch_add(1, Ordering::SeqCst);
            if previous != 0 {
                violations.fetch_add(1, Ordering::SeqCst);
            }
            thread::sleep(Duration::from_millis(10));
            active.fetch_sub(1, Ordering::SeqCst);
            mutex.unlock();
            thread::sleep(Duration::from_millis(10));
            completed.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for join in joins {
        join.join().expect("join");
    }

    assert_eq!(violations.load(Ordering::SeqCst), 0);
    assert_eq!(completed.load(Ordering::SeqCst), 8);
}
