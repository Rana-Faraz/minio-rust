use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use minio_rust::internal::lsync::LRWMutex;

pub const SOURCE_FILE: &str = "internal/lsync/lrwmutex_test.go";

fn test_simple_write_lock(duration: Duration) -> bool {
    let lrwm = Arc::new(LRWMutex::new());
    assert!(lrwm.get_rlock(Duration::from_millis(50)));
    assert!(lrwm.get_rlock(Duration::from_millis(50)));

    let reader1 = lrwm.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(40));
        reader1.runlock();
    });

    let reader2 = lrwm.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(60));
        reader2.runlock();
    });

    let locked = lrwm.get_lock(duration);
    if locked {
        thread::sleep(Duration::from_millis(10));
        lrwm.unlock();
    }
    locked
}

#[test]
fn simple_write_lock_acquired() {
    assert!(test_simple_write_lock(Duration::from_millis(200)));
}

#[test]
fn simple_write_lock_timed_out() {
    assert!(!test_simple_write_lock(Duration::from_millis(10)));
}

fn test_dual_write_lock(duration: Duration) -> bool {
    let lrwm = Arc::new(LRWMutex::new());
    assert!(lrwm.get_lock(Duration::from_millis(50)));

    let writer = lrwm.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(40));
        writer.unlock();
    });

    let locked = lrwm.get_lock(duration);
    if locked {
        thread::sleep(Duration::from_millis(10));
        lrwm.unlock();
    }
    locked
}

#[test]
fn dual_write_lock_acquired() {
    assert!(test_dual_write_lock(Duration::from_millis(100)));
}

#[test]
fn dual_write_lock_timed_out() {
    assert!(!test_dual_write_lock(Duration::from_millis(10)));
}

fn do_test_parallel_readers(num_readers: usize) {
    let lrwm = Arc::new(LRWMutex::new());
    let (clocked_tx, clocked_rx) = mpsc::channel();
    let (unlock_tx, unlock_rx) = mpsc::channel();
    let unlock_rx = Arc::new(std::sync::Mutex::new(unlock_rx));
    let (done_tx, done_rx) = mpsc::channel();

    for _ in 0..num_readers {
        let mutex = lrwm.clone();
        let clocked = clocked_tx.clone();
        let unlock = unlock_rx.clone();
        let done = done_tx.clone();
        thread::spawn(move || {
            if mutex.get_rlock(Duration::from_millis(100)) {
                clocked.send(()).expect("clocked");
                unlock.lock().expect("unlock lock").recv().expect("unlock");
                mutex.runlock();
                done.send(()).expect("done");
            }
        });
    }

    for _ in 0..num_readers {
        clocked_rx.recv().expect("reader should lock");
    }
    for _ in 0..num_readers {
        unlock_tx.send(()).expect("unlock");
    }
    for _ in 0..num_readers {
        done_rx.recv().expect("reader done");
    }
}

#[test]
fn parallel_readers() {
    do_test_parallel_readers(1);
    do_test_parallel_readers(3);
    do_test_parallel_readers(4);
}

fn hammer_rwmutex(num_readers: usize, num_iterations: usize) {
    let mutex = Arc::new(LRWMutex::new());
    let activity = Arc::new(AtomicI32::new(0));
    let (done_tx, done_rx) = mpsc::channel();

    for _ in 0..2 {
        let mutex = mutex.clone();
        let activity = activity.clone();
        let done = done_tx.clone();
        thread::spawn(move || {
            for _ in 0..num_iterations {
                if mutex.get_lock(Duration::from_millis(100)) {
                    let n = activity.fetch_add(10_000, Ordering::SeqCst) + 10_000;
                    assert_eq!(n, 10_000);
                    for _ in 0..50 {}
                    activity.fetch_sub(10_000, Ordering::SeqCst);
                    mutex.unlock();
                }
            }
            done.send(()).expect("writer done");
        });
    }

    for _ in 0..num_readers {
        let mutex = mutex.clone();
        let activity = activity.clone();
        let done = done_tx.clone();
        thread::spawn(move || {
            for _ in 0..num_iterations {
                if mutex.get_rlock(Duration::from_millis(100)) {
                    let n = activity.fetch_add(1, Ordering::SeqCst) + 1;
                    assert!((1..10_000).contains(&n));
                    for _ in 0..50 {}
                    activity.fetch_sub(1, Ordering::SeqCst);
                    mutex.runlock();
                }
            }
            done.send(()).expect("reader done");
        });
    }

    for _ in 0..(2 + num_readers) {
        done_rx.recv().expect("goroutine done");
    }
}

#[test]
fn rwmutex_hammer() {
    hammer_rwmutex(1, 20);
    hammer_rwmutex(3, 20);
    hammer_rwmutex(5, 20);
}

#[test]
fn drlocker_respects_read_and_write_locking() {
    let wl = Arc::new(LRWMutex::new());
    let rl = LRWMutex::drlocker(wl.clone());
    let (wlocked_tx, wlocked_rx) = mpsc::channel();
    let (rlocked_tx, rlocked_rx) = mpsc::channel();

    let writer_mutex = wl.clone();
    let reader_locker = rl.clone();
    thread::spawn(move || {
        for _ in 0..5 {
            reader_locker.lock();
            reader_locker.lock();
            rlocked_tx.send(()).expect("rlocked");
            writer_mutex.lock();
            wlocked_tx.send(()).expect("wlocked");
        }
    });

    for _ in 0..5 {
        rlocked_rx.recv().expect("reader locked");
        rl.unlock();
        assert!(wlocked_rx.recv_timeout(Duration::from_millis(10)).is_err());
        rl.unlock();
        wlocked_rx.recv().expect("writer locked");
        assert!(rlocked_rx.recv_timeout(Duration::from_millis(10)).is_err());
        wl.unlock();
    }
}

#[test]
fn unlock_panic() {
    let mutex = LRWMutex::new();
    assert!(std::panic::catch_unwind(|| mutex.unlock()).is_err());
}

#[test]
fn unlock_panic2() {
    let mutex = LRWMutex::new();
    mutex.rlock();
    assert!(std::panic::catch_unwind(|| mutex.unlock()).is_err());
    mutex.force_unlock();
}

#[test]
fn runlock_panic() {
    let mutex = LRWMutex::new();
    assert!(std::panic::catch_unwind(|| mutex.runlock()).is_err());
}

#[test]
fn runlock_panic2() {
    let mutex = LRWMutex::new();
    mutex.lock();
    assert!(std::panic::catch_unwind(|| mutex.runlock()).is_err());
    mutex.force_unlock();
}
