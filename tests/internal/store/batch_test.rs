use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use minio_rust::internal::store::{Batch, BatchConfig, QueueStore};

pub const SOURCE_FILE: &str = "internal/store/batch_test.go";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestItem {
    name: String,
    property: String,
}

fn test_item() -> TestItem {
    TestItem {
        name: "test-item".to_owned(),
        property: "property".to_owned(),
    }
}

fn setup_queue_store(limit: u64) -> (tempfile::TempDir, QueueStore<TestItem>) {
    let dir = tempfile::tempdir().expect("tempdir must be created");
    let store = QueueStore::new(dir.path().to_path_buf(), limit, ".test");
    store.open().expect("store must open");
    (dir, store)
}

#[test]
fn test_batch_commit() {
    let (_dir, store) = setup_queue_store(100);
    let limit = 100u32;
    let batch = Batch::new(BatchConfig {
        limit,
        store: Some(store.clone()),
        commit_timeout: Duration::from_secs(300),
        log: Arc::new(|_| {}),
    });

    for _ in 0..limit {
        batch.add(test_item()).expect("add should succeed");
    }
    assert_eq!(batch.len(), limit as usize);
    assert!(store.list().is_empty());

    batch.add(test_item()).expect("overflow add should commit");
    assert_eq!(batch.len(), 1);
    let keys = store.list();
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    assert!(key.compress);
    assert_eq!(key.item_count, limit as usize);
    let items = store
        .get_multiple(key)
        .expect("stored batch should be readable");
    assert_eq!(items.len(), limit as usize);
    batch.close().expect("close should succeed");
}

#[test]
fn test_batch_commit_on_exit() {
    let (_dir, store) = setup_queue_store(100);
    let limit = 100u32;
    let batch = Batch::new(BatchConfig {
        limit,
        store: Some(store.clone()),
        commit_timeout: Duration::from_secs(300),
        log: Arc::new(|_| {}),
    });

    for _ in 0..limit {
        batch.add(test_item()).expect("add should succeed");
    }
    batch.close().expect("close should flush items");
    thread::sleep(Duration::from_millis(50));

    assert_eq!(batch.len(), 0);
    let keys = store.list();
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    assert!(key.compress);
    assert_eq!(key.item_count, limit as usize);
    let items = store
        .get_multiple(key)
        .expect("stored batch should be readable");
    assert_eq!(items.len(), limit as usize);
}

#[test]
fn test_batch_with_concurrency() {
    let (_dir, store) = setup_queue_store(100);
    let limit = 100u32;
    let batch = Arc::new(Batch::new(BatchConfig {
        limit,
        store: Some(store.clone()),
        commit_timeout: Duration::from_secs(300),
        log: Arc::new(|_| {}),
    }));

    let mut handles = Vec::new();
    for _ in 0..limit {
        let batch = batch.clone();
        handles.push(thread::spawn(move || {
            batch
                .add(test_item())
                .expect("concurrent add should succeed");
        }));
    }
    for handle in handles {
        handle.join().expect("worker thread should join");
    }

    assert_eq!(batch.len(), limit as usize);
    assert!(store.list().is_empty());

    batch.add(test_item()).expect("overflow add should commit");
    assert_eq!(batch.len(), 1);
    let keys = store.list();
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    assert!(key.compress);
    assert_eq!(key.item_count, limit as usize);
    let items = store
        .get_multiple(key)
        .expect("stored batch should be readable");
    assert_eq!(items.len(), limit as usize);
    batch.close().expect("close should succeed");
}
