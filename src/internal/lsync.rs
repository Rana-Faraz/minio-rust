use std::sync::MutexGuard;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct State {
    readers: usize,
    writer: bool,
}

#[derive(Debug, Default)]
pub struct LRWMutex {
    state: Mutex<State>,
    condvar: Condvar,
}

impl LRWMutex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn lock(&self) {
        let mut guard = self.lock_state();
        while guard.writer || guard.readers > 0 {
            guard = self
                .condvar
                .wait(guard)
                .expect("lrwmutex wait should succeed");
        }
        guard.writer = true;
    }

    pub fn get_lock(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = self.lock_state();
        while guard.writer || guard.readers > 0 {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_guard, result) = self
                .condvar
                .wait_timeout(guard, remaining)
                .expect("lrwmutex wait should succeed");
            guard = next_guard;
            if result.timed_out() && (guard.writer || guard.readers > 0) {
                return false;
            }
        }
        guard.writer = true;
        true
    }

    pub fn rlock(&self) {
        let mut guard = self.lock_state();
        while guard.writer {
            guard = self
                .condvar
                .wait(guard)
                .expect("lrwmutex wait should succeed");
        }
        guard.readers += 1;
    }

    pub fn get_rlock(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = self.lock_state();
        while guard.writer {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_guard, result) = self
                .condvar
                .wait_timeout(guard, remaining)
                .expect("lrwmutex wait should succeed");
            guard = next_guard;
            if result.timed_out() && guard.writer {
                return false;
            }
        }
        guard.readers += 1;
        true
    }

    pub fn unlock(&self) {
        let mut guard = self.lock_state();
        if !guard.writer {
            drop(guard);
            panic!("Trying to Unlock() while no Lock() is active");
        }
        guard.writer = false;
        self.condvar.notify_all();
    }

    pub fn runlock(&self) {
        let mut guard = self.lock_state();
        if guard.writer || guard.readers == 0 {
            drop(guard);
            panic!("Trying to RUnlock() while no RLock() is active");
        }
        guard.readers -= 1;
        if guard.readers == 0 {
            self.condvar.notify_all();
        }
    }

    pub fn force_unlock(&self) {
        let mut guard = self.lock_state();
        guard.writer = false;
        guard.readers = 0;
        self.condvar.notify_all();
    }

    pub fn drlocker(this: Arc<Self>) -> DRLocker {
        DRLocker { inner: this }
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Clone, Debug)]
pub struct DRLocker {
    inner: Arc<LRWMutex>,
}

impl DRLocker {
    pub fn lock(&self) {
        self.inner.rlock();
    }

    pub fn unlock(&self) {
        self.inner.runlock();
    }
}
