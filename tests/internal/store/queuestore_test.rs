use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use minio_rust::internal::store::{err_limit_exceeded, QueueStore};

pub const SOURCE_FILE: &str = "internal/store/queuestore_test.go";

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
fn test_queue_store_put() {
    let (_dir, store) = setup_queue_store(100);
    for _ in 0..100 {
        store.put(test_item()).expect("put should succeed");
    }
    assert_eq!(store.list().len(), 100);
}

#[test]
fn test_queue_store_get() {
    let (_dir, store) = setup_queue_store(10);
    for _ in 0..10 {
        store.put(test_item()).expect("put should succeed");
    }
    let keys = store.list();
    assert_eq!(keys.len(), 10);
    for key in keys {
        let item = store.get(&key).expect("get should succeed");
        assert_eq!(item, test_item());
    }
}

#[test]
fn test_queue_store_del() {
    let (_dir, store) = setup_queue_store(20);
    for _ in 0..20 {
        store.put(test_item()).expect("put should succeed");
    }
    let keys = store.list();
    assert_eq!(keys.len(), 20);
    for key in keys {
        store.del(&key).expect("del should succeed");
    }
    assert!(store.list().is_empty());
}

#[test]
fn test_queue_store_limit() {
    let (_dir, store) = setup_queue_store(5);
    for _ in 0..5 {
        store.put(test_item()).expect("put should succeed");
    }
    let err = store.put(test_item()).expect_err("6th put should fail");
    assert_eq!(err.to_string(), err_limit_exceeded().to_string());
}

#[test]
fn test_queue_store_list_n() {
    let (dir, store) = setup_queue_store(10);
    for _ in 0..10 {
        store.put(test_item()).expect("put should succeed");
    }
    let keys = store.list();
    assert_eq!(keys.len(), 10);
    assert_eq!(keys.len(), store.len());

    let reopened: QueueStore<TestItem> = QueueStore::new(PathBuf::from(dir.path()), 10, ".test");
    reopened.open().expect("reopened store must open");
    let reopened_keys = reopened.list();
    assert_eq!(reopened_keys.len(), 10);
    assert_eq!(reopened.len(), 10);

    for key in reopened_keys {
        reopened.del(&key).expect("delete should succeed");
    }
    assert!(reopened.list().is_empty());
}

#[test]
fn test_multiple_put_get_raw() {
    let (_dir, store) = setup_queue_store(10);
    let items = (0..10)
        .map(|i| TestItem {
            name: format!("test-item-{i}"),
            property: "property".to_owned(),
        })
        .collect::<Vec<_>>();

    store
        .put_multiple(&items)
        .expect("put multiple should succeed");
    let keys = store.list();
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    assert!(key.compress);
    assert_eq!(key.item_count, 10);

    let raw = store.get_raw(key).expect("get raw should succeed");
    let mut expected = Vec::new();
    for item in &items {
        serde_json::to_writer(&mut expected, item).expect("json encode should succeed");
        expected.push(b'\n');
    }
    assert_eq!(raw, expected);
    store.del(key).expect("del should succeed");
    assert!(store.list().is_empty());
}

#[test]
fn test_multiple_put_gets() {
    let (_dir, store) = setup_queue_store(10);
    let items = (0..10)
        .map(|i| TestItem {
            name: format!("test-item-{i}"),
            property: "property".to_owned(),
        })
        .collect::<Vec<_>>();

    store
        .put_multiple(&items)
        .expect("put multiple should succeed");
    let keys = store.list();
    assert_eq!(keys.len(), 1);
    let result = store
        .get_multiple(&keys[0])
        .expect("get multiple should succeed");
    assert_eq!(result, items);
}

#[test]
fn test_mixed_put_gets() {
    let (_dir, store) = setup_queue_store(10);
    let mut items = (0..5)
        .map(|i| TestItem {
            name: format!("test-item-{i}"),
            property: "property".to_owned(),
        })
        .collect::<Vec<_>>();

    store
        .put_multiple(&items)
        .expect("put multiple should succeed");

    for i in 5..10 {
        let item = TestItem {
            name: format!("test-item-{i}"),
            property: "property".to_owned(),
        };
        store.put(item.clone()).expect("single put should succeed");
        items.push(item);
    }

    let keys = store.list();
    assert_eq!(keys.len(), 6);

    let mut result = Vec::new();
    for key in &keys {
        if key.item_count > 1 {
            result.extend(
                store
                    .get_multiple(key)
                    .expect("get multiple should succeed"),
            );
        } else {
            result.push(store.get(key).expect("get should succeed"));
        }
    }
    assert_eq!(result, items);

    for key in keys {
        store.del(&key).expect("delete should succeed");
    }
    assert!(store.list().is_empty());
}
