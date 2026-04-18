use minio_rust::internal::cachevalue::{Cache, CancellationToken, Error, Opts};
use std::thread;
use std::time::{Duration, Instant};

fn slow_caller(token: CancellationToken) -> Result<(), Error> {
    let started = Instant::now();
    loop {
        if token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if started.elapsed() >= Duration::from_millis(75) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn cache_ctx_matches_reference_cases() {
    let cache = Cache::new();
    cache.init_once(Duration::from_millis(125), Opts::default(), |token| {
        let now = Instant::now();
        slow_caller(token)?;
        Ok(now)
    });

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let result = cache.get_with_ctx(cancelled);
    assert_eq!(result, Err(Error::Cancelled));

    let t1 = cache.get().expect("cache fetch should succeed");
    let t2 = cache.get().expect("cache fetch should reuse cached value");
    assert_eq!(t1, t2);

    thread::sleep(Duration::from_millis(175));

    let t3 = cache.get().expect("cache refresh should succeed");
    assert_ne!(t1, t3);
}

#[test]
fn cache_matches_reference_cases() {
    let cache = Cache::new();
    cache.init_once(Duration::from_millis(100), Opts::default(), |_| {
        Ok(Instant::now())
    });

    let t1 = cache.get().expect("first cache fetch should succeed");
    let t2 = cache.get().expect("second cache fetch should be cached");
    assert_eq!(t1, t2);

    thread::sleep(Duration::from_millis(150));

    let t3 = cache.get().expect("expired cache should refresh");
    assert_ne!(t1, t3);
}

#[test]
fn cache_handles_repeated_reads_under_load() {
    let cache = Cache::new_from_func(Duration::from_millis(1), Opts::default(), |_| {
        Ok(Instant::now())
    });

    for _ in 0..500 {
        let _ = cache.get().expect("cache read should succeed");
    }
}
