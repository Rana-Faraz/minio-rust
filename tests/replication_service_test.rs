use std::collections::BTreeMap;

use minio_rust::cmd::{
    ReplicationBackoffConfig, ReplicationOperation, ReplicationQueueStatus, ReplicationService,
};

#[test]
fn replication_service_snapshot_includes_runtime_state() {
    let service = ReplicationService::new(ReplicationBackoffConfig {
        initial_backoff_ms: 100,
        max_backoff_ms: 1_000,
        default_max_attempts: 3,
    });

    let id = service.enqueue_object(
        "arn:minio:replication:us-east-1:remote1:bucket",
        "photos",
        "kitten.jpg",
        "v1",
        4_096,
        Some(BTreeMap::from([(
            "x-amz-bucket-replication-status".to_string(),
            "PENDING".to_string(),
        )])),
        10_000,
    );

    let snapshot = service.snapshot(10_000);
    assert_eq!(snapshot.stats.queued, 1);
    let entry = snapshot.queue.entries.get(&id).expect("entry");
    assert_eq!(entry.operation, ReplicationOperation::PutObject);
    assert_eq!(entry.status, ReplicationQueueStatus::Queued);
    assert_eq!(entry.payload_size, 4_096);
}

#[test]
fn replication_service_process_due_reports_success() {
    let service = ReplicationService::new(ReplicationBackoffConfig {
        initial_backoff_ms: 100,
        max_backoff_ms: 1_000,
        default_max_attempts: 3,
    });
    let id = service.enqueue_delete(
        "arn:minio:replication:us-east-1:remote1:bucket",
        "photos",
        "kitten.jpg",
        "v2",
        None,
        20_000,
    );

    let report = service.process_due_with(20_000, 4, |_| Ok(()));
    assert_eq!(report.leased, 1);
    assert_eq!(report.succeeded, 1);

    let snapshot = service.snapshot(20_000);
    assert_eq!(
        snapshot.queue.entries.get(&id).expect("entry").status,
        ReplicationQueueStatus::Succeeded
    );
    assert_eq!(snapshot.stats.total_completed, 1);
}
