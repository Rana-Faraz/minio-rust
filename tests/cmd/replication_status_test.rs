use std::io::Cursor;

use crate::cmd::{
    aggregate_replication_target_statuses, build_replication_status_payload,
    object_replication_status, parse_replication_target_statuses, snapshot_replication_queue,
    summarize_bucket_replication_stats, summarize_replication_objects, ActiveWorkerStat,
    BucketReplicationStats, InQueueMetric, QStat, ReplQNodeStats, ReplicationAdminStatusPayload,
    ReplicationObjectInfo, ReplicationQueueStats, ReplicationStatusCounters,
    REPLICATION_STATUS_COMPLETED, REPLICATION_STATUS_FAILED, REPLICATION_STATUS_PENDING,
};

fn object(
    name: &str,
    size: i64,
    replication_status: &str,
    replication_status_internal: &str,
) -> ReplicationObjectInfo {
    ReplicationObjectInfo {
        bucket: "bucket".to_string(),
        name: name.to_string(),
        size,
        replication_status: replication_status.to_string(),
        replication_status_internal: replication_status_internal.to_string(),
        ..ReplicationObjectInfo::default()
    }
}

#[test]
fn test_parse_replication_target_statuses_handles_colon_rich_arns() {
    let parsed = parse_replication_target_statuses(
        "arn:minio:replication:us-east-1:remote1:bucket:COMPLETED;\
         arn:minio:replication:us-east-1:remote2:bucket:PENDING;",
    );

    assert_eq!(
        parsed.get("arn:minio:replication:us-east-1:remote1:bucket"),
        Some(&REPLICATION_STATUS_COMPLETED.to_string())
    );
    assert_eq!(
        parsed.get("arn:minio:replication:us-east-1:remote2:bucket"),
        Some(&REPLICATION_STATUS_PENDING.to_string())
    );
}

#[test]
fn test_summarize_replication_objects_and_targets() {
    let objects = vec![
        object(
            "ok.txt",
            10,
            "",
            "arn:minio:replication:us-east-1:remote1:bucket:COMPLETED;",
        ),
        object(
            "wait.txt",
            15,
            "",
            "arn:minio:replication:us-east-1:remote1:bucket:PENDING;\
             arn:minio:replication:us-east-1:remote2:bucket:FAILED;",
        ),
        object("replica.txt", 7, "REPLICA", ""),
    ];

    let summary = summarize_replication_objects(&objects);
    assert_eq!(
        summary,
        ReplicationStatusCounters {
            total_count: 3,
            total_bytes: 32,
            completed_count: 1,
            completed_bytes: 10,
            pending_count: 1,
            pending_bytes: 15,
            failed_count: 0,
            failed_bytes: 0,
            replica_count: 1,
            replica_bytes: 7,
            unknown_count: 0,
            unknown_bytes: 0,
        }
    );

    let targets = aggregate_replication_target_statuses(&objects);
    assert_eq!(targets.len(), 2);
    assert_eq!(
        targets[0].arn,
        "arn:minio:replication:us-east-1:remote1:bucket"
    );
    assert_eq!(targets[0].total_count, 2);
    assert_eq!(targets[0].completed_count, 1);
    assert_eq!(targets[0].pending_count, 1);
    assert_eq!(targets[0].total_bytes, 25);
    assert_eq!(
        targets[1].arn,
        "arn:minio:replication:us-east-1:remote2:bucket"
    );
    assert_eq!(targets[1].failed_count, 1);
    assert_eq!(targets[1].last_status, REPLICATION_STATUS_FAILED);
}

#[test]
fn test_object_replication_status_prefers_composite_target_status() {
    let pending = object(
        "pending.txt",
        5,
        "",
        "arn:minio:replication:us-east-1:r1:b:PENDING;\
         arn:minio:replication:us-east-1:r2:b:COMPLETED;",
    );
    let failed = object(
        "failed.txt",
        5,
        "",
        "arn:minio:replication:us-east-1:r1:b:FAILED;\
         arn:minio:replication:us-east-1:r2:b:COMPLETED;",
    );

    assert_eq!(
        object_replication_status(&pending),
        REPLICATION_STATUS_PENDING
    );
    assert_eq!(
        object_replication_status(&failed),
        REPLICATION_STATUS_FAILED
    );
}

#[test]
fn test_snapshot_replication_queue_and_payload_roundtrip() {
    let queue = ReplicationQueueStats {
        uptime: 20,
        nodes: Some(vec![
            ReplQNodeStats {
                node_name: "node-a".to_string(),
                uptime: 12,
                active_workers: ActiveWorkerStat {
                    curr: 2,
                    avg: 1.5,
                    max: 4,
                },
                q_stats: InQueueMetric {
                    curr: QStat {
                        count: 3.0,
                        bytes: 30.0,
                    },
                    avg: QStat {
                        count: 2.0,
                        bytes: 20.0,
                    },
                    max: QStat {
                        count: 5.0,
                        bytes: 50.0,
                    },
                },
                ..ReplQNodeStats::default()
            },
            ReplQNodeStats {
                node_name: "node-b".to_string(),
                uptime: 18,
                active_workers: ActiveWorkerStat {
                    curr: 1,
                    avg: 0.5,
                    max: 2,
                },
                q_stats: InQueueMetric {
                    curr: QStat {
                        count: 4.0,
                        bytes: 40.0,
                    },
                    avg: QStat {
                        count: 1.0,
                        bytes: 10.0,
                    },
                    max: QStat {
                        count: 6.0,
                        bytes: 60.0,
                    },
                },
                ..ReplQNodeStats::default()
            },
        ]),
    };

    let snapshot = snapshot_replication_queue(&queue);
    assert_eq!(snapshot.node_count, 2);
    assert_eq!(snapshot.uptime, 20);
    assert_eq!(snapshot.active_workers_curr, 3);
    assert_eq!(snapshot.active_workers_avg, 1.0);
    assert_eq!(snapshot.active_workers_max, 6);
    assert_eq!(snapshot.queue_curr_count, 7.0);
    assert_eq!(snapshot.queue_curr_bytes, 70.0);
    assert_eq!(snapshot.queue_avg_count, 3.0);
    assert_eq!(snapshot.queue_avg_bytes, 30.0);
    assert_eq!(snapshot.queue_max_count, 11.0);
    assert_eq!(snapshot.queue_max_bytes, 110.0);

    let payload = build_replication_status_payload(
        &[object(
            "ok.txt",
            10,
            "",
            "arn:minio:replication:us-east-1:remote1:bucket:COMPLETED;",
        )],
        &queue,
    );
    let mut buf = Cursor::new(Vec::new());
    payload.encode(&mut buf).expect("encode payload");
    buf.set_position(0);
    let mut decoded = ReplicationAdminStatusPayload::default();
    decoded.decode(&mut buf).expect("decode payload");
    assert_eq!(decoded, payload);
}

#[test]
fn test_summarize_bucket_replication_stats_uses_bucket_counters() {
    let summary = summarize_bucket_replication_stats(&BucketReplicationStats {
        replicated_size: 100,
        replica_size: 30,
        replicated_count: 4,
        replica_count: 2,
        pending_size: 20,
        failed_size: 5,
        pending_count: 1,
        failed_count: 1,
        ..BucketReplicationStats::default()
    });

    assert_eq!(summary.total_count, 8);
    assert_eq!(summary.total_bytes, 155);
    assert_eq!(summary.completed_count, 4);
    assert_eq!(summary.pending_count, 1);
    assert_eq!(summary.failed_count, 1);
    assert_eq!(summary.replica_count, 2);
}
