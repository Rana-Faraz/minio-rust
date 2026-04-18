use std::collections::BTreeMap;

use minio_rust::cmd::*;

pub const SOURCE_FILE: &str = "cmd/replication_queue_store_test.go";

fn sample_config() -> ReplicationBackoffConfig {
    ReplicationBackoffConfig {
        initial_backoff_ms: 100,
        max_backoff_ms: 1_000,
        default_max_attempts: 3,
    }
}

fn sample_request() -> ReplicationQueueRequest {
    ReplicationQueueRequest {
        target_arn: "arn:minio:replication:us-east-1:remote1:bucket".to_string(),
        bucket: "source".to_string(),
        object: "photos/object.jpg".to_string(),
        version_id: "vid-1".to_string(),
        operation: ReplicationOperation::PutObject,
        payload_size: 512,
        metadata: Some(BTreeMap::from([(
            "x-amz-bucket-replication-status".to_string(),
            "PENDING".to_string(),
        )])),
        max_attempts: 0,
    }
}

#[test]
fn test_replication_queue_snapshot_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replication-queue.msgpack");

    let mut queue = ReplicationQueue::new(sample_config());
    let first = queue.enqueue(sample_request(), 1_000);
    let second = queue.enqueue(
        ReplicationQueueRequest {
            object: "photos/object-2.jpg".to_string(),
            operation: ReplicationOperation::DeleteObject,
            payload_size: 1_024,
            ..sample_request()
        },
        1_250,
    );

    let leased = queue.lease_due(1_250, 1);
    assert_eq!(leased.len(), 1);
    queue
        .mark_failure(&first, 1_300, "temporary network issue")
        .expect("mark failure");
    queue.mark_success(&second, 1_301).expect("mark success");

    save_replication_queue_snapshot(&path, &queue).expect("save snapshot");
    let loaded = load_replication_queue_snapshot(&path).expect("load snapshot");

    assert_eq!(loaded, queue);
    assert_eq!(loaded.entries.len(), 2);
    assert_eq!(loaded.stats.total_entries, 2);
    assert_eq!(loaded.stats.total_completed, 1);
    assert_eq!(loaded.stats.total_retries_scheduled, 1);
    assert_eq!(loaded.stats.completed_bytes, 1_024);
}

#[test]
fn test_replication_queue_snapshot_supports_restart_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("replication-queue.msgpack");

    let mut queue = ReplicationQueue::new(sample_config());
    let id = queue.enqueue(
        ReplicationQueueRequest {
            object: "photos/retry.jpg".to_string(),
            payload_size: 2_048,
            ..sample_request()
        },
        10_000,
    );

    let leased = queue.lease_due(10_000, 1);
    assert_eq!(leased.len(), 1);
    queue
        .mark_failure(&id, 10_010, "upstream timeout")
        .expect("schedule retry");
    let retry_due = queue.get(&id).expect("entry").retry.next_attempt_at;

    save_replication_queue_snapshot(&path, &queue).expect("save snapshot");

    let mut recovered = load_replication_queue_snapshot(&path).expect("load snapshot");
    assert_eq!(recovered.due_count(retry_due - 1), 0);
    assert_eq!(recovered.due_count(retry_due), 1);

    let leased = recovered.lease_due(retry_due, 1);
    assert_eq!(leased.len(), 1);
    assert_eq!(leased[0].id, id);
    assert_eq!(leased[0].retry.attempts, 2);

    recovered
        .mark_success(&id, retry_due + 1)
        .expect("mark success after restart");

    let entry = recovered.get(&id).expect("entry after recovery");
    assert_eq!(entry.status, ReplicationQueueStatus::Succeeded);
    assert_eq!(entry.retry.last_success_at, retry_due + 1);
    assert_eq!(recovered.stats.total_completed, 1);
    assert_eq!(recovered.stats.waiting_retry, 0);
    assert_eq!(recovered.stats.succeeded, 1);
}
