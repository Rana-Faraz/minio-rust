use minio_rust::cmd::{
    build_replication_admin_status_payload_from_runtime, runtime_queue_to_node_stats,
    runtime_snapshots_to_queue_stats, summarize_runtime_queue, ReplicationBackoffConfig,
    ReplicationObjectInfo, ReplicationOperation, ReplicationQueue, ReplicationQueueRequest,
    ReplicationQueueStatus, ReplicationRuntimeNodeSnapshot, REPLICATION_STATUS_COMPLETED,
    REPLICATION_STATUS_PENDING,
};

fn request(target: &str, object: &str, size: u64) -> ReplicationQueueRequest {
    ReplicationQueueRequest {
        target_arn: target.to_string(),
        bucket: "source".to_string(),
        object: object.to_string(),
        version_id: "v1".to_string(),
        operation: ReplicationOperation::PutObject,
        payload_size: size,
        metadata: None,
        max_attempts: 3,
    }
}

fn object(name: &str, size: i64, status: &str, internal: &str) -> ReplicationObjectInfo {
    ReplicationObjectInfo {
        bucket: "source".to_string(),
        name: name.to_string(),
        size,
        replication_status: status.to_string(),
        replication_status_internal: internal.to_string(),
        ..ReplicationObjectInfo::default()
    }
}

#[test]
fn runtime_queue_summary_tracks_pending_inflight_failed_and_succeeded_bytes() {
    let mut queue = ReplicationQueue::new(ReplicationBackoffConfig {
        initial_backoff_ms: 100,
        max_backoff_ms: 1_000,
        default_max_attempts: 3,
    });

    let retry = queue.enqueue(request("arn:remote:a", "retry.txt", 20), 1_000);
    let inflight = queue.enqueue(request("arn:remote:b", "inflight.txt", 30), 1_000);
    let success = queue.enqueue(request("arn:remote:b", "success.txt", 40), 1_000);
    let failure = queue.enqueue(
        ReplicationQueueRequest {
            max_attempts: 1,
            ..request("arn:remote:c", "failure.txt", 50)
        },
        1_000,
    );
    let queued = queue.enqueue(request("arn:remote:a", "queued.txt", 10), 1_000);

    let leased = queue.lease_due(1_000, 4);
    assert_eq!(leased.len(), 4);
    assert!(leased.iter().any(|entry| entry.id == inflight));
    queue
        .mark_failure(&retry, 1_010, "retry later")
        .expect("mark retry");
    queue.mark_success(&success, 1_011).expect("mark success");
    queue
        .mark_failure(&failure, 1_012, "still broken")
        .expect("mark failed");

    assert_eq!(
        queue.get(&inflight).expect("inflight").status,
        ReplicationQueueStatus::InFlight
    );
    assert_eq!(
        queue.get(&queued).expect("queued").status,
        ReplicationQueueStatus::Queued
    );

    let summary = summarize_runtime_queue(&queue, 1_050);
    assert_eq!(summary.queued_count, 1);
    assert_eq!(summary.queued_bytes, 10);
    assert_eq!(summary.waiting_retry_count, 1);
    assert_eq!(summary.waiting_retry_bytes, 20);
    assert_eq!(summary.in_flight_count, 1);
    assert_eq!(summary.in_flight_bytes, 30);
    assert_eq!(summary.succeeded_count, 1);
    assert_eq!(summary.succeeded_bytes, 40);
    assert_eq!(summary.failed_count, 1);
    assert_eq!(summary.failed_bytes, 50);
}

#[test]
fn runtime_node_and_payload_builder_reuse_runtime_and_object_views() {
    let mut node_a = ReplicationQueue::new(ReplicationBackoffConfig {
        initial_backoff_ms: 50,
        max_backoff_ms: 400,
        default_max_attempts: 2,
    });
    let a1 = node_a.enqueue(request("arn:remote:a", "alpha.txt", 11), 1_000);
    let a2 = node_a.enqueue(request("arn:remote:a", "bravo.txt", 22), 1_000);
    let leased = node_a.lease_due(1_000, 10);
    assert_eq!(leased.len(), 2);
    node_a.mark_success(&a1, 1_010).expect("success");
    node_a.mark_failure(&a2, 1_011, "retry").expect("retry");

    let mut node_b = ReplicationQueue::new(ReplicationBackoffConfig {
        initial_backoff_ms: 50,
        max_backoff_ms: 400,
        default_max_attempts: 2,
    });
    let b1 = node_b.enqueue(request("arn:remote:b", "charlie.txt", 33), 1_000);
    let leased = node_b.lease_due(1_000, 10);
    assert_eq!(leased.len(), 1);
    assert_eq!(
        node_b.get(&b1).expect("inflight").status,
        ReplicationQueueStatus::InFlight
    );

    let snapshot_a = ReplicationRuntimeNodeSnapshot {
        node_name: "node-a".to_string(),
        uptime: 99,
        queue: node_a.clone(),
    };
    let node_stats = runtime_queue_to_node_stats(&snapshot_a, 1_050);
    assert_eq!(node_stats.node_name, "node-a");
    assert_eq!(node_stats.active_workers.curr, 0);
    assert_eq!(node_stats.q_stats.curr.count, 1.0);
    assert_eq!(node_stats.q_stats.curr.bytes, 22.0);
    assert_eq!(node_stats.mrf_stats.total_dropped_count, 0);
    assert_eq!(
        node_stats
            .xfer_stats
            .as_ref()
            .expect("xfer stats")
            .get("arn:remote:a")
            .expect("target")
            .curr,
        11.0
    );

    let queue_stats = runtime_snapshots_to_queue_stats(
        &[
            snapshot_a,
            ReplicationRuntimeNodeSnapshot {
                node_name: "node-b".to_string(),
                uptime: 120,
                queue: node_b,
            },
        ],
        1_050,
    );
    assert_eq!(queue_stats.uptime, 120);
    assert_eq!(queue_stats.nodes.as_ref().expect("nodes").len(), 2);

    let payload = build_replication_admin_status_payload_from_runtime(
        &[
            object(
                "alpha.txt",
                11,
                "",
                "arn:minio:replication:us-east-1:remote1:bucket:COMPLETED;",
            ),
            object(
                "beta.txt",
                22,
                "",
                "arn:minio:replication:us-east-1:remote1:bucket:PENDING;",
            ),
        ],
        &[
            ReplicationRuntimeNodeSnapshot {
                node_name: "node-a".to_string(),
                uptime: 99,
                queue: node_a.clone(),
            },
            ReplicationRuntimeNodeSnapshot {
                node_name: "node-b".to_string(),
                uptime: 120,
                queue: ReplicationQueue::default(),
            },
        ],
        1_050,
    );

    assert_eq!(payload.overview.total_count, 2);
    assert_eq!(payload.overview.completed_count, 1);
    assert_eq!(payload.overview.pending_count, 1);
    assert_eq!(payload.statuses[0].status, REPLICATION_STATUS_COMPLETED);
    assert!(payload
        .statuses
        .iter()
        .any(|status| status.status == REPLICATION_STATUS_PENDING && status.count == 1));
    assert_eq!(payload.queue.node_count, 2);
}
