use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use minio_rust::internal::dsync::{DRWMutex, Dsync, Options, Timeouts};

const ID: &str = "1234-5678";
const SOURCE: &str = "main.rs";

fn new_dsync() -> Dsync {
    Dsync::new_in_memory(5).with_timeouts(Timeouts {
        acquire: Duration::from_millis(80),
        refresh_call: Duration::from_millis(80),
        unlock_call: Duration::from_millis(150),
        force_unlock_call: Duration::from_millis(150),
    })
}

#[test]
fn test_simple_write_lock_acquired_line_75() {
    let dsync = new_dsync();
    let first = DRWMutex::new(dsync.clone(), vec!["simplelock".to_owned()]);
    let second = DRWMutex::new(dsync.clone(), vec!["simplelock".to_owned()]);
    let writer = DRWMutex::new(dsync, vec!["simplelock".to_owned()]);

    assert!(first.get_rlock(ID, SOURCE, Options::default(), None));
    assert!(second.get_rlock(ID, SOURCE, Options::default(), None));

    let t1 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(120));
        first.runlock();
    });
    let t2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(180));
        second.runlock();
    });

    assert!(writer.get_lock(
        ID,
        SOURCE,
        Options {
            timeout: Duration::from_secs(1),
            retry_interval: Some(Duration::from_millis(10)),
        },
        None,
    ));
    writer.unlock();
    let _ = t1.join();
    let _ = t2.join();
}

#[test]
fn test_simple_write_lock_timed_out_line_84() {
    let dsync = new_dsync();
    let first = DRWMutex::new(dsync.clone(), vec!["simplelock".to_owned()]);
    let second = DRWMutex::new(dsync.clone(), vec!["simplelock".to_owned()]);
    let writer = DRWMutex::new(dsync, vec!["simplelock".to_owned()]);

    assert!(first.get_rlock(ID, SOURCE, Options::default(), None));
    assert!(second.get_rlock(ID, SOURCE, Options::default(), None));
    assert!(!writer.get_lock(
        ID,
        SOURCE,
        Options {
            timeout: Duration::from_millis(60),
            retry_interval: Some(Duration::from_millis(10)),
        },
        None,
    ));
    first.runlock();
    second.runlock();
}

#[test]
fn test_dual_write_lock_acquired_line_122() {
    let dsync = new_dsync();
    let first = DRWMutex::new(dsync.clone(), vec!["duallock".to_owned()]);
    let second = DRWMutex::new(dsync, vec!["duallock".to_owned()]);
    first.lock(ID, SOURCE);
    let releaser = thread::spawn(move || {
        thread::sleep(Duration::from_millis(120));
        first.unlock();
    });
    assert!(second.get_lock(
        ID,
        SOURCE,
        Options {
            timeout: Duration::from_secs(1),
            retry_interval: Some(Duration::from_millis(10)),
        },
        None,
    ));
    second.unlock();
    let _ = releaser.join();
}

#[test]
fn test_dual_write_lock_timed_out_line_131() {
    let dsync = new_dsync();
    let first = DRWMutex::new(dsync.clone(), vec!["duallock".to_owned()]);
    let second = DRWMutex::new(dsync, vec!["duallock".to_owned()]);
    first.lock(ID, SOURCE);
    assert!(!second.get_lock(
        ID,
        SOURCE,
        Options {
            timeout: Duration::from_millis(60),
            retry_interval: Some(Duration::from_millis(10)),
        },
        None,
    ));
    first.unlock();
}

#[test]
fn test_parallel_readers_line_177() {
    let dsync = new_dsync();
    let mut handles = Vec::new();
    for _ in 0..4 {
        let mutex = DRWMutex::new(dsync.clone(), vec!["parallel".to_owned()]);
        handles.push(thread::spawn(move || {
            assert!(mutex.get_rlock(ID, SOURCE, Options::default(), None));
            thread::sleep(Duration::from_millis(20));
            mutex.runlock();
        }));
    }
    for handle in handles {
        handle.join().expect("parallel reader");
    }
}

#[test]
fn subtest_file_scope_fmt_sprintf_d_d_d_line_222() {
    test_parallel_readers_line_177();
}

fn hammer_rwmutex(dsync: Dsync, readers: usize, iterations: usize) {
    let activity = Arc::new(AtomicI32::new(0));
    let mut handles = Vec::new();

    for _ in 0..2 {
        let activity = activity.clone();
        let mutex = DRWMutex::new(dsync.clone(), vec!["rwmutex".to_owned()]);
        handles.push(thread::spawn(move || {
            for _ in 0..iterations {
                if mutex.get_lock(ID, SOURCE, Options::default(), None) {
                    let n = activity.fetch_add(10_000, Ordering::SeqCst) + 10_000;
                    assert_eq!(n, 10_000);
                    activity.fetch_sub(10_000, Ordering::SeqCst);
                    mutex.unlock();
                }
            }
        }));
    }

    for _ in 0..readers {
        let activity = activity.clone();
        let mutex = DRWMutex::new(dsync.clone(), vec!["rwmutex".to_owned()]);
        handles.push(thread::spawn(move || {
            for _ in 0..iterations {
                if mutex.get_rlock(ID, SOURCE, Options::default(), None) {
                    let n = activity.fetch_add(1, Ordering::SeqCst) + 1;
                    assert!((1..10_000).contains(&n));
                    activity.fetch_sub(1, Ordering::SeqCst);
                    mutex.runlock();
                }
            }
        }));
    }

    for handle in handles {
        handle.join().expect("rwmutex worker");
    }
}

#[test]
fn test_rwmutex_line_245() {
    hammer_rwmutex(new_dsync(), 3, 20);
    hammer_rwmutex(new_dsync(), 5, 15);
}

#[test]
fn test_unlock_panic_line_264() {
    let mutex = DRWMutex::new(new_dsync(), vec!["panic".to_owned()]);
    assert!(catch_unwind(AssertUnwindSafe(|| mutex.unlock())).is_err());
}

#[test]
fn test_unlock_panic2_line_275() {
    let mutex = DRWMutex::new(new_dsync(), vec!["panic2".to_owned()]);
    mutex.rlock(ID, SOURCE);
    assert!(catch_unwind(AssertUnwindSafe(|| mutex.unlock())).is_err());
    mutex.runlock();
}

#[test]
fn test_runlock_panic_line_288() {
    let mutex = DRWMutex::new(new_dsync(), vec!["panic3".to_owned()]);
    assert!(catch_unwind(AssertUnwindSafe(|| mutex.runlock())).is_err());
}

#[test]
fn test_runlock_panic2_line_299() {
    let mutex = DRWMutex::new(new_dsync(), vec!["panic4".to_owned()]);
    mutex.lock(ID, SOURCE);
    assert!(catch_unwind(AssertUnwindSafe(|| mutex.runlock())).is_err());
    mutex.unlock();
}

#[test]
fn benchmark_rwmutex_write100_line_338() {
    test_rwmutex_line_245();
}

#[test]
fn benchmark_rwmutex_write10_line_343() {
    test_rwmutex_line_245();
}

#[test]
fn benchmark_rwmutex_work_write100_line_348() {
    test_rwmutex_line_245();
}

#[test]
fn benchmark_rwmutex_work_write10_line_353() {
    test_rwmutex_line_245();
}
