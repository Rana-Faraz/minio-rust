use std::cmp::{max, min};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

pub const DYNAMIC_TIMEOUT_INCREASE_THRESHOLD_PCT: f64 = 0.33;
pub const DYNAMIC_TIMEOUT_DECREASE_THRESHOLD_PCT: f64 = 0.10;
pub const DYNAMIC_TIMEOUT_LOG_SIZE: usize = 16;
pub const MAX_DYNAMIC_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_DURATION_SENTINEL_NS: i64 = i64::MAX;

#[derive(Debug)]
pub struct DynamicTimeout {
    timeout_ns: AtomicI64,
    minimum_ns: i64,
    entries: AtomicI64,
    log: Mutex<[i64; DYNAMIC_TIMEOUT_LOG_SIZE]>,
    retry_interval: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct DynamicTimeoutOpts {
    pub timeout: Duration,
    pub minimum: Duration,
    pub retry_interval: Duration,
}

fn duration_to_i64_ns(duration: Duration) -> i64 {
    min(duration.as_nanos(), i64::MAX as u128) as i64
}

fn i64_ns_to_duration(nanos: i64) -> Duration {
    Duration::from_nanos(nanos.max(0) as u64)
}

pub fn new_dynamic_timeout_with_opts(opts: DynamicTimeoutOpts) -> DynamicTimeout {
    let mut dt = new_dynamic_timeout(opts.timeout, opts.minimum);
    dt.retry_interval = opts.retry_interval;
    dt
}

pub fn new_dynamic_timeout(timeout: Duration, minimum: Duration) -> DynamicTimeout {
    assert!(
        !timeout.is_zero() && !minimum.is_zero(),
        "newDynamicTimeout: negative or zero timeout"
    );
    let minimum = minimum.min(timeout);
    DynamicTimeout {
        timeout_ns: AtomicI64::new(duration_to_i64_ns(timeout)),
        minimum_ns: duration_to_i64_ns(minimum),
        entries: AtomicI64::new(0),
        log: Mutex::new([0; DYNAMIC_TIMEOUT_LOG_SIZE]),
        retry_interval: Duration::ZERO,
    }
}

impl DynamicTimeout {
    pub fn timeout(&self) -> Duration {
        i64_ns_to_duration(self.timeout_ns.load(Ordering::SeqCst))
    }

    pub fn retry_interval(&self) -> Duration {
        self.retry_interval
    }

    pub fn log_success(&self, duration: Duration) {
        self.log_entry(duration_to_i64_ns(duration));
    }

    pub fn log_failure(&self) {
        self.log_entry(MAX_DURATION_SENTINEL_NS);
    }

    fn log_entry(&self, duration_ns: i64) {
        if duration_ns < 0 {
            return;
        }
        let entries = self.entries.fetch_add(1, Ordering::SeqCst) + 1;
        let index = entries - 1;
        if index < DYNAMIC_TIMEOUT_LOG_SIZE as i64 {
            let mut guard = self.log.lock().unwrap();
            guard[index as usize] = duration_ns;
            if entries == DYNAMIC_TIMEOUT_LOG_SIZE as i64 {
                let log_copy = *guard;
                self.entries.store(0, Ordering::SeqCst);
                *guard = [0; DYNAMIC_TIMEOUT_LOG_SIZE];
                drop(guard);
                self.adjust(log_copy);
            }
        }
    }

    pub fn adjust(&self, entries: [i64; DYNAMIC_TIMEOUT_LOG_SIZE]) {
        let mut failures = 0usize;
        let mut max_duration_ns = 0i64;
        for duration_ns in entries {
            if duration_ns == MAX_DURATION_SENTINEL_NS {
                failures += 1;
            } else if duration_ns > max_duration_ns {
                max_duration_ns = duration_ns;
            }
        }

        let fail_pct = failures as f64 / DYNAMIC_TIMEOUT_LOG_SIZE as f64;
        if fail_pct > DYNAMIC_TIMEOUT_INCREASE_THRESHOLD_PCT {
            let timeout = min(
                self.timeout_ns.load(Ordering::SeqCst) * 125 / 100,
                duration_to_i64_ns(MAX_DYNAMIC_TIMEOUT),
            );
            self.timeout_ns
                .store(max(timeout, self.minimum_ns), Ordering::SeqCst);
        } else if fail_pct < DYNAMIC_TIMEOUT_DECREASE_THRESHOLD_PCT {
            let max_duration_ns = max_duration_ns * 125 / 100;
            let mut timeout = self.timeout_ns.load(Ordering::SeqCst);
            if max_duration_ns < timeout {
                timeout = (max_duration_ns + timeout) / 2;
            }
            if timeout < self.minimum_ns {
                timeout = self.minimum_ns;
            }
            self.timeout_ns.store(timeout, Ordering::SeqCst);
        }
    }
}
