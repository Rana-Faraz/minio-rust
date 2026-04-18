use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default)]
pub struct Opts {
    pub return_last_good: bool,
    pub no_wait: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Cancelled,
    Message(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("operation cancelled"),
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

struct State<T> {
    value: Option<T>,
    updated_at: Option<Instant>,
    updating: bool,
}

impl<T> Default for State<T> {
    fn default() -> Self {
        Self {
            value: None,
            updated_at: None,
            updating: false,
        }
    }
}

type UpdateFn<T> = dyn Fn(CancellationToken) -> Result<T, Error> + Send + Sync;

struct Inner<T> {
    ttl: Duration,
    opts: Opts,
    update_fn: Box<UpdateFn<T>>,
    state: Mutex<State<T>>,
    changed: Condvar,
}

pub struct Cache<T: Clone + Send + 'static> {
    inner: OnceLock<Arc<Inner<T>>>,
}

impl<T: Clone + Send + 'static> Cache<T> {
    pub fn new() -> Self {
        Self {
            inner: OnceLock::new(),
        }
    }

    pub fn new_from_func<F>(ttl: Duration, opts: Opts, update: F) -> Self
    where
        F: Fn(CancellationToken) -> Result<T, Error> + Send + Sync + 'static,
    {
        let cache = Self::new();
        cache.init_once(ttl, opts, update);
        cache
    }

    pub fn init_once<F>(&self, ttl: Duration, opts: Opts, update: F)
    where
        F: Fn(CancellationToken) -> Result<T, Error> + Send + Sync + 'static,
    {
        let _ = self.inner.get_or_init(|| {
            Arc::new(Inner {
                ttl,
                opts,
                update_fn: Box::new(update),
                state: Mutex::new(State::default()),
                changed: Condvar::new(),
            })
        });
    }

    pub fn get(&self) -> Result<T, Error> {
        self.get_with_ctx(CancellationToken::new())
    }

    pub fn get_with_ctx(&self, token: CancellationToken) -> Result<T, Error> {
        let inner = Arc::clone(self.inner());

        loop {
            let mut state = inner.state.lock().expect("cache mutex poisoned");

            if let Some(value) = fresh_value(&state, inner.ttl) {
                return Ok(value);
            }

            if inner.opts.no_wait {
                if let Some(value) = stale_value_within(&state, inner.ttl.saturating_mul(2)) {
                    if !state.updating {
                        state.updating = true;
                        let background = Arc::clone(&inner);
                        drop(state);
                        thread::spawn(move || {
                            let _ = update_inner(background, CancellationToken::new());
                        });
                    }
                    return Ok(value);
                }
            }

            if state.updating {
                drop(inner.changed.wait(state).expect("cache mutex poisoned"));
                continue;
            }

            state.updating = true;
            drop(state);
            return update_inner(inner, token);
        }
    }

    fn inner(&self) -> &Arc<Inner<T>> {
        self.inner.get().expect("cache not initialized")
    }
}

fn fresh_value<T: Clone>(state: &State<T>, ttl: Duration) -> Option<T> {
    if is_fresh(state.updated_at, ttl) {
        state.value.clone()
    } else {
        None
    }
}

fn stale_value_within<T: Clone>(state: &State<T>, max_age: Duration) -> Option<T> {
    if is_fresh(state.updated_at, max_age) {
        state.value.clone()
    } else {
        None
    }
}

fn is_fresh(updated_at: Option<Instant>, max_age: Duration) -> bool {
    updated_at.is_some_and(|instant| instant.elapsed() < max_age)
}

fn update_inner<T: Clone + Send + 'static>(
    inner: Arc<Inner<T>>,
    token: CancellationToken,
) -> Result<T, Error> {
    let result = (inner.update_fn)(token);
    let mut state = inner.state.lock().expect("cache mutex poisoned");
    state.updating = false;

    let output = match result {
        Ok(value) => {
            state.value = Some(value.clone());
            state.updated_at = Some(Instant::now());
            Ok(value)
        }
        Err(error) => {
            if inner.opts.return_last_good {
                if let Some(value) = state.value.clone() {
                    Ok(value)
                } else {
                    Err(error)
                }
            } else {
                Err(error)
            }
        }
    };

    inner.changed.notify_all();
    output
}
