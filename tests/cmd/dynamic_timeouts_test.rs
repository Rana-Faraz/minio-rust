// Rust test snapshot derived from cmd/dynamic-timeouts_test.go.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use minio_rust::cmd::{new_dynamic_timeout, DynamicTimeout, DYNAMIC_TIMEOUT_LOG_SIZE};

pub const SOURCE_FILE: &str = "cmd/dynamic-timeouts_test.go";

#[test]
fn test_dynamic_timeout_single_increase_line_28() {
    let timeout = new_dynamic_timeout(Duration::from_secs(60), Duration::from_secs(1));
    let initial = timeout.timeout();

    for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
        timeout.log_failure();
    }

    let adjusted = timeout.timeout();
    assert!(initial < adjusted, "expected {adjusted:?} > {initial:?}");
}

#[test]
fn test_dynamic_timeout_dual_increase_line_44() {
    let timeout = new_dynamic_timeout(Duration::from_secs(60), Duration::from_secs(1));
    let initial = timeout.timeout();

    for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
        timeout.log_failure();
    }
    let adjusted = timeout.timeout();

    for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
        timeout.log_failure();
    }
    let adjusted_again = timeout.timeout();

    assert!(initial < adjusted && adjusted < adjusted_again);
}

#[test]
fn test_dynamic_timeout_single_decrease_line_66() {
    let timeout = new_dynamic_timeout(Duration::from_secs(60), Duration::from_secs(1));
    let initial = timeout.timeout();

    for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
        timeout.log_success(Duration::from_secs(20));
    }

    let adjusted = timeout.timeout();
    assert!(initial > adjusted, "expected {adjusted:?} < {initial:?}");
}

#[test]
fn test_dynamic_timeout_dual_decrease_line_82() {
    let timeout = new_dynamic_timeout(Duration::from_secs(60), Duration::from_secs(1));
    let initial = timeout.timeout();

    for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
        timeout.log_success(Duration::from_secs(20));
    }
    let adjusted = timeout.timeout();

    for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
        timeout.log_success(Duration::from_secs(20));
    }
    let adjusted_again = timeout.timeout();

    assert!(initial > adjusted && adjusted > adjusted_again);
}

#[test]
fn test_dynamic_timeout_many_decreases_line_104() {
    let timeout = new_dynamic_timeout(Duration::from_secs(60), Duration::from_secs(1));
    let initial = timeout.timeout();
    let success_timeout = Duration::from_secs(20);

    for _ in 0..100 {
        for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
            timeout.log_success(success_timeout);
        }
    }

    let adjusted = timeout.timeout();
    assert!(initial > adjusted && adjusted > success_timeout);
}

#[test]
fn test_dynamic_timeout_concurrent_line_123() {
    let timeout = Arc::new(new_dynamic_timeout(
        Duration::from_secs(1),
        Duration::from_millis(1),
    ));

    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let mut handles = Vec::new();
    for i in 0..workers {
        let timeout = Arc::clone(&timeout);
        handles.push(thread::spawn(move || {
            let mut state = i as u64 + 1;
            for _ in 0..100 {
                for _ in 0..100 {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let frac = ((state >> 11) as f64) / ((u64::MAX >> 11) as f64);
                    timeout.log_success(Duration::from_secs_f64(frac));
                }
                let current = timeout.timeout();
                assert!(current >= Duration::from_millis(1));
                assert!(current <= Duration::from_secs(1));
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_dynamic_timeout_hit_minimum_line_146() {
    let minimum = Duration::from_secs(30);
    let timeout = new_dynamic_timeout(Duration::from_secs(60), minimum);
    let initial = timeout.timeout();
    let success_timeout = Duration::from_secs(20);

    for _ in 0..100 {
        for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
            timeout.log_success(success_timeout);
        }
    }

    let adjusted = timeout.timeout();
    assert!(initial > adjusted);
    assert_eq!(adjusted, minimum);
}

fn test_dynamic_timeout_adjust(timeout: &DynamicTimeout, mut f: impl FnMut() -> f64) {
    for _ in 0..DYNAMIC_TIMEOUT_LOG_SIZE {
        let rnd = f();
        let duration = Duration::from_secs_f64(20.0 * rnd.max(0.0)).max(Duration::from_millis(100));
        if duration >= Duration::from_secs(60) {
            timeout.log_failure();
        } else {
            timeout.log_success(duration);
        }
    }
}

#[test]
fn test_dynamic_timeout_adjust_exponential_line_180() {
    let timeout = new_dynamic_timeout(Duration::from_secs(60), Duration::from_secs(1));
    let initial = timeout.timeout();
    let mut state = 1u64;

    for _ in 0..10 {
        test_dynamic_timeout_adjust(&timeout, || {
            state = state
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let u = (((state >> 11) as f64) / ((u64::MAX >> 11) as f64)).clamp(1e-12, 1.0 - 1e-12);
            -u.ln()
        });
    }

    let adjusted = timeout.timeout();
    assert!(initial > adjusted, "expected {adjusted:?} < {initial:?}");
}

#[test]
fn test_dynamic_timeout_adjust_normalized_line_197() {
    let timeout = new_dynamic_timeout(Duration::from_secs(60), Duration::from_secs(1));
    let initial = timeout.timeout();
    let mut state = 2u64;

    for _ in 0..10 {
        test_dynamic_timeout_adjust(&timeout, || {
            state = state
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let u1 = (((state >> 11) as f64) / ((u64::MAX >> 11) as f64)).clamp(1e-12, 1.0 - 1e-12);
            state = state
                .wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493);
            let u2 = (((state >> 11) as f64) / ((u64::MAX >> 11) as f64)).clamp(1e-12, 1.0 - 1e-12);
            let z = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
            1.0 + z
        });
    }

    let adjusted = timeout.timeout();
    assert!(initial > adjusted, "expected {adjusted:?} < {initial:?}");
}
