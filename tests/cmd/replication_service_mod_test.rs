use super::*;

use std::sync::atomic::{AtomicI64, AtomicUsize};
use tempfile::tempdir;

fn service() -> ReplicationService {
    ReplicationService::new(ReplicationBackoffConfig {
        initial_backoff_ms: 100,
        max_backoff_ms: 1_000,
        default_max_attempts: 3,
    })
}

#[test]
fn enqueue_object_and_delete_requests() {
    let service = service();
    let object_id = service.enqueue_object(
        "arn:minio:replication:us-east-1:remote1:bucket",
        "src",
        "photo.jpg",
        "v1",
        256,
        Some(BTreeMap::from([("k".to_string(), "v".to_string())])),
        1_000,
    );
    let delete_id = service.enqueue_delete(
        "arn:minio:replication:us-east-1:remote1:bucket",
        "src",
        "photo.jpg",
        "v2",
        None,
        1_001,
    );

    let snapshot = service.snapshot(1_001);
    assert_ne!(object_id, delete_id);
    assert_eq!(snapshot.stats.total_entries, 2);
    assert_eq!(snapshot.stats.queued, 2);
    assert_eq!(
        snapshot
            .queue
            .entries
            .get(&object_id)
            .expect("object entry")
            .operation,
        ReplicationOperation::PutObject
    );
    assert_eq!(
        snapshot
            .queue
            .entries
            .get(&delete_id)
            .expect("delete entry")
            .operation,
        ReplicationOperation::DeleteObject
    );
}

#[test]
fn process_due_updates_success_and_retry_state() {
    let service = service();
    let first = service.enqueue_object(
        "arn:minio:replication:us-east-1:remote1:bucket",
        "src",
        "success.txt",
        "",
        128,
        None,
        5_000,
    );
    let second = service.enqueue_object(
        "arn:minio:replication:us-east-1:remote1:bucket",
        "src",
        "retry.txt",
        "",
        64,
        None,
        5_000,
    );

    let report = service.process_due_with(5_000, 10, |entry| {
        if entry.object == "success.txt" {
            Ok(())
        } else {
            Err("temporary failure".to_string())
        }
    });

    assert_eq!(report.leased, 2);
    assert_eq!(report.succeeded, 1);
    assert_eq!(report.retried, 1);
    assert_eq!(report.permanently_failed, 0);

    let snapshot = service.snapshot(5_000);
    assert_eq!(
        snapshot.queue.entries.get(&first).expect("first").status,
        ReplicationQueueStatus::Succeeded
    );
    let retried = snapshot.queue.entries.get(&second).expect("second");
    assert_eq!(retried.status, ReplicationQueueStatus::Queued);
    assert_eq!(retried.retry.next_attempt_at, 5_100);
    assert_eq!(snapshot.stats.succeeded, 1);
    assert_eq!(snapshot.stats.waiting_retry, 1);
}

#[test]
fn process_due_marks_terminal_failures() {
    let mut queue = ReplicationQueue::new(ReplicationBackoffConfig {
        initial_backoff_ms: 50,
        max_backoff_ms: 500,
        default_max_attempts: 2,
    });
    let id = queue.enqueue(
        ReplicationQueueRequest {
            target_arn: "arn:minio:replication:us-east-1:remote1:bucket".to_string(),
            bucket: "src".to_string(),
            object: "fail.txt".to_string(),
            version_id: String::new(),
            operation: ReplicationOperation::PutObject,
            payload_size: 32,
            metadata: None,
            max_attempts: 2,
        },
        100,
    );
    let service = ReplicationService::from_queue(queue);

    let first = service.process_due_with(100, 10, |_| Err("no route".to_string()));
    assert_eq!(first.retried, 1);
    let retry_due = service
        .snapshot(150)
        .queue
        .entries
        .get(&id)
        .expect("entry")
        .retry
        .next_attempt_at;

    let second = service.process_due_with(retry_due, 10, |_| Err("still no route".to_string()));
    assert_eq!(second.permanently_failed, 1);

    let snapshot = service.snapshot(retry_due);
    assert_eq!(
        snapshot.queue.entries.get(&id).expect("entry").status,
        ReplicationQueueStatus::Failed
    );
    assert_eq!(snapshot.stats.failed, 1);
    assert_eq!(snapshot.stats.total_failed, 1);
}

#[test]
fn spawned_worker_processes_due_work() {
    let service = service();
    service.enqueue_object(
        "arn:minio:replication:us-east-1:remote1:bucket",
        "src",
        "background.txt",
        "",
        512,
        None,
        1_000,
    );

    let now = Arc::new(AtomicI64::new(1_000));
    let calls = Arc::new(AtomicUsize::new(0));
    let worker = {
        let now = now.clone();
        let calls = calls.clone();
        service.spawn_worker(
            ReplicationWorkerConfig {
                batch_size: 8,
                idle_sleep: Duration::from_millis(5),
            },
            move || now.load(Ordering::SeqCst),
            move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
    };

    for _ in 0..50 {
        if service.snapshot(1_000).stats.succeeded == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    worker.join().expect("worker thread should join cleanly");
    let snapshot = service.snapshot(1_000);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(snapshot.stats.succeeded, 1);
    assert_eq!(snapshot.stats.queued, 0);
}

#[test]
fn persistent_service_requeues_inflight_entries_after_restart() {
    let tempdir = tempdir().expect("tempdir");
    let path = tempdir.path().join("replication-queue.json");
    let mut queue = ReplicationQueue::new(ReplicationBackoffConfig {
        initial_backoff_ms: 100,
        max_backoff_ms: 1_000,
        default_max_attempts: 3,
    });
    let id = queue.enqueue(
        ReplicationQueueRequest {
            target_arn: "arn:minio:replication:us-east-1:remote1:bucket".to_string(),
            bucket: "src".to_string(),
            object: "persist.txt".to_string(),
            version_id: "vid-1".to_string(),
            operation: ReplicationOperation::PutObject,
            payload_size: 128,
            metadata: None,
            max_attempts: 0,
        },
        1_000,
    );
    let leased = queue.lease_due(1_000, 1);
    assert_eq!(leased.len(), 1);
    persist_queue(&path, &queue).expect("persist queue");

    let service = ReplicationService::new_persistent(
        ReplicationBackoffConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1_000,
            default_max_attempts: 3,
        },
        &path,
        2_000,
    )
    .expect("persistent service");
    let snapshot = service.snapshot(2_000);
    let entry = snapshot.queue.entries.get(&id).expect("entry");
    assert_eq!(entry.status, ReplicationQueueStatus::Queued);
    assert_eq!(entry.retry.next_attempt_at, 2_000);
    assert_eq!(snapshot.stats.queued, 1);
}
