use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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

fn short_wait() {
    thread::sleep(Duration::from_millis(20));
}

#[test]
fn test_main_test_main_line_40() {
    let dsync = new_dsync();
    assert_eq!(dsync.lockers().len(), 5);
}

#[test]
fn test_simple_lock_line_64() {
    let mutex = DRWMutex::new(new_dsync(), vec!["test".to_owned()]);
    mutex.lock(ID, SOURCE);
    short_wait();
    mutex.unlock();
}

#[test]
fn test_simple_lock_unlock_multiple_times_line_75() {
    let mutex = DRWMutex::new(new_dsync(), vec!["test".to_owned()]);
    for _ in 0..5 {
        mutex.lock(ID, SOURCE);
        short_wait();
        mutex.unlock();
        short_wait();
    }
}

#[test]
fn test_two_simultaneous_locks_for_same_resource_line_100() {
    let dsync = new_dsync();
    let first = DRWMutex::new(dsync.clone(), vec!["aap".to_owned()]);
    let second = DRWMutex::new(dsync, vec!["aap".to_owned()]);

    first.lock(ID, SOURCE);
    let start = Instant::now();
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
    assert!(start.elapsed() >= Duration::from_millis(100));
    second.unlock();
    let _ = releaser.join();
}

#[test]
fn test_three_simultaneous_locks_for_same_resource_line_123() {
    let dsync = new_dsync();
    let first = DRWMutex::new(dsync.clone(), vec!["aap".to_owned()]);
    let second = DRWMutex::new(dsync.clone(), vec!["aap".to_owned()]);
    let third = DRWMutex::new(dsync, vec!["aap".to_owned()]);

    first.lock(ID, SOURCE);
    let t1 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
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
    let t2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        second.unlock();
    });

    assert!(third.get_lock(
        ID,
        SOURCE,
        Options {
            timeout: Duration::from_secs(1),
            retry_interval: Some(Duration::from_millis(10)),
        },
        None,
    ));
    third.unlock();
    let _ = t1.join();
    let _ = t2.join();
}

#[test]
fn test_two_simultaneous_locks_for_different_resources_line_198() {
    let dsync = new_dsync();
    let first = DRWMutex::new(dsync.clone(), vec!["aap".to_owned()]);
    let second = DRWMutex::new(dsync, vec!["noot".to_owned()]);
    assert!(first.get_lock(ID, SOURCE, Options::default(), None));
    assert!(second.get_lock(ID, SOURCE, Options::default(), None));
    first.unlock();
    second.unlock();
}

#[test]
fn test_successful_lock_refresh_line_209() {
    let mut mutex = DRWMutex::new(new_dsync(), vec!["aap".to_owned()]);
    mutex.set_refresh_interval(Duration::from_millis(30));
    assert!(mutex.get_lock(
        ID,
        SOURCE,
        Options {
            timeout: Duration::from_secs(1),
            retry_interval: Some(Duration::from_millis(10)),
        },
        None,
    ));
    thread::sleep(Duration::from_millis(90));
    mutex.unlock();
}

#[test]
fn test_failed_refresh_lock_line_237() {
    let dsync = new_dsync();
    for locker in dsync.lockers().iter().take(3) {
        locker.set_refresh_reply(false);
    }
    let mut mutex = DRWMutex::new(dsync.clone(), vec!["aap".to_owned()]);
    mutex.set_refresh_interval(Duration::from_millis(20));
    let cancelled = Arc::new(AtomicBool::new(false));
    let callback_flag = cancelled.clone();
    let callback: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        callback_flag.store(true, Ordering::SeqCst);
    });

    assert!(mutex.get_lock(
        ID,
        SOURCE,
        Options {
            timeout: Duration::from_secs(1),
            retry_interval: Some(Duration::from_millis(10)),
        },
        Some(callback),
    ));

    let start = Instant::now();
    while !cancelled.load(Ordering::SeqCst) && start.elapsed() < Duration::from_secs(1) {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(cancelled.load(Ordering::SeqCst));
}

#[test]
fn test_unlock_should_not_timeout_line_274() {
    let dsync = new_dsync();
    let mutex = DRWMutex::new(dsync.clone(), vec!["aap".to_owned()]);
    assert!(mutex.get_lock(ID, SOURCE, Options::default(), None));
    for locker in dsync.lockers() {
        locker.set_response_delay(Duration::from_millis(300));
    }
    let start = Instant::now();
    mutex.unlock();
    assert!(start.elapsed() < Duration::from_millis(100));
    for locker in dsync.lockers() {
        locker.set_response_delay(Duration::ZERO);
    }
}

fn hammer_mutex(mutex: DRWMutex, loops: usize) {
    for _ in 0..loops {
        mutex.lock(ID, SOURCE);
        mutex.unlock();
        short_wait();
    }
}

#[test]
fn test_mutex_line_321() {
    let loops = 20;
    let mutex = DRWMutex::new(new_dsync(), vec!["test".to_owned()]);
    let mut handles = Vec::new();
    for _ in 0..6 {
        handles.push(thread::spawn({
            let mutex = mutex.clone();
            move || hammer_mutex(mutex, loops)
        }));
    }
    for handle in handles {
        handle.join().expect("hammer mutex thread");
    }
}

#[test]
fn benchmark_mutex_uncontended_line_336() {
    let mutex = DRWMutex::new(new_dsync(), vec!["bench".to_owned()]);
    for _ in 0..100 {
        mutex.lock(ID, SOURCE);
        mutex.unlock();
    }
}

#[test]
fn benchmark_mutex_line_376() {
    benchmark_mutex_uncontended_line_336();
}

#[test]
fn benchmark_mutex_slack_line_380() {
    benchmark_mutex_uncontended_line_336();
}

#[test]
fn benchmark_mutex_work_line_384() {
    benchmark_mutex_uncontended_line_336();
}

#[test]
fn benchmark_mutex_work_slack_line_388() {
    benchmark_mutex_uncontended_line_336();
}

#[test]
fn benchmark_mutex_no_spin_line_392() {
    benchmark_mutex_uncontended_line_336();
}

#[test]
fn benchmark_mutex_spin_line_429() {
    benchmark_mutex_uncontended_line_336();
}
