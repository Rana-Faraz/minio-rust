use super::*;

use std::io::Cursor;

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
fn queue_roundtrips_through_msgpack() {
    let mut queue = ReplicationQueue::new(sample_config());
    let first = queue.enqueue(sample_request(), 1_000);
    let second = queue.enqueue(
        ReplicationQueueRequest {
            object: "photos/object-2.jpg".to_string(),
            operation: ReplicationOperation::DeleteObject,
            ..sample_request()
        },
        1_250,
    );
    let leased = queue.lease_due(1_250, 1);
    assert_eq!(leased.len(), 1);
    queue
        .mark_failure(&first, 1_300, "temporary network issue")
        .unwrap();
    queue.mark_success(&second, 1_260).unwrap();

    let bytes = queue.marshal_msg().expect("marshal");
    let mut decoded = ReplicationQueue::default();
    let remaining = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(remaining.is_empty());
    assert_eq!(decoded, queue);

    let mut cursor = Cursor::new(Vec::new());
    queue.encode(&mut cursor).expect("encode");
    cursor.set_position(0);
    let mut streamed = ReplicationQueue::default();
    streamed.decode(&mut cursor).expect("decode");
    assert_eq!(streamed, queue);
}

#[test]
fn failed_attempts_back_off_and_retry_until_success() {
    let mut queue = ReplicationQueue::new(sample_config());
    let id = queue.enqueue(sample_request(), 10_000);

    let first = queue.lease_due(10_000, 10);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].retry.attempts, 1);
    queue.mark_failure(&id, 10_010, "upstream timeout").unwrap();

    let entry = queue.get(&id).expect("entry");
    assert_eq!(entry.status, ReplicationQueueStatus::Queued);
    assert_eq!(entry.retry.next_attempt_at, 10_110);
    assert_eq!(queue.stats.waiting_retry, 1);
    assert_eq!(queue.stats.total_retries_scheduled, 1);
    assert_eq!(queue.due_count(10_109), 0);
    assert_eq!(queue.due_count(10_110), 1);

    let second = queue.lease_due(10_110, 10);
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].retry.attempts, 2);
    queue.mark_success(&id, 10_111).unwrap();

    let entry = queue.get(&id).expect("entry");
    assert_eq!(entry.status, ReplicationQueueStatus::Succeeded);
    assert_eq!(entry.retry.last_success_at, 10_111);
    assert_eq!(queue.stats.total_completed, 1);
    assert_eq!(queue.stats.completed_bytes, 512);
    assert_eq!(queue.stats.succeeded, 1);
    assert_eq!(queue.stats.failed, 0);
}

#[test]
fn queue_marks_terminal_failure_after_max_attempts() {
    let mut queue = ReplicationQueue::new(sample_config());
    let id = queue.enqueue(
        ReplicationQueueRequest {
            max_attempts: 2,
            payload_size: 1_024,
            ..sample_request()
        },
        5_000,
    );

    let first = queue.lease_due(5_000, 1);
    assert_eq!(first.len(), 1);
    queue.mark_failure(&id, 5_001, "dial failed").unwrap();
    let retry_due = queue.get(&id).expect("entry").retry.next_attempt_at;
    assert_eq!(retry_due, 5_101);

    let second = queue.lease_due(retry_due, 1);
    assert_eq!(second.len(), 1);
    queue
        .mark_failure(&id, retry_due + 1, "dial failed again")
        .unwrap();

    let entry = queue.get(&id).expect("entry");
    assert_eq!(entry.status, ReplicationQueueStatus::Failed);
    assert_eq!(entry.retry.attempts, 2);
    assert_eq!(queue.stats.total_failed, 1);
    assert_eq!(queue.stats.failed_bytes, 1_024);
    assert_eq!(queue.stats.failed, 1);
    assert_eq!(queue.stats.waiting_retry, 0);
}

#[test]
fn lease_due_respects_order_and_limit() {
    let mut queue = ReplicationQueue::new(sample_config());
    let first = queue.enqueue(
        ReplicationQueueRequest {
            object: "a".to_string(),
            ..sample_request()
        },
        1_000,
    );
    let second = queue.enqueue(
        ReplicationQueueRequest {
            object: "b".to_string(),
            ..sample_request()
        },
        1_001,
    );
    let third = queue.enqueue(
        ReplicationQueueRequest {
            object: "c".to_string(),
            ..sample_request()
        },
        1_002,
    );

    let leased = queue.lease_due(1_500, 2);
    let leased_ids = leased.into_iter().map(|entry| entry.id).collect::<Vec<_>>();
    assert_eq!(leased_ids, vec![first.clone(), second.clone()]);
    assert_eq!(
        queue.get(&first).expect("first").status,
        ReplicationQueueStatus::InFlight
    );
    assert_eq!(
        queue.get(&second).expect("second").status,
        ReplicationQueueStatus::InFlight
    );
    assert_eq!(
        queue.get(&third).expect("third").status,
        ReplicationQueueStatus::Queued
    );
    assert_eq!(queue.stats.in_flight, 2);
    assert_eq!(queue.stats.queued, 1);
}
