use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationRuntimeNodeSnapshot {
    pub node_name: String,
    pub uptime: i64,
    pub queue: ReplicationQueue,
}
impl_msg_codec!(ReplicationRuntimeNodeSnapshot);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationRuntimeQueueBreakdown {
    pub queued_count: u64,
    pub queued_bytes: u64,
    pub waiting_retry_count: u64,
    pub waiting_retry_bytes: u64,
    pub in_flight_count: u64,
    pub in_flight_bytes: u64,
    pub succeeded_count: u64,
    pub succeeded_bytes: u64,
    pub failed_count: u64,
    pub failed_bytes: u64,
}
impl_msg_codec!(ReplicationRuntimeQueueBreakdown);

pub fn summarize_runtime_queue(
    queue: &ReplicationQueue,
    now_ms: i64,
) -> ReplicationRuntimeQueueBreakdown {
    let mut summary = ReplicationRuntimeQueueBreakdown::default();
    for entry in queue.entries.values() {
        let bytes = entry.payload_size;
        match entry.status {
            ReplicationQueueStatus::Queued if entry.retry.next_attempt_at > now_ms => {
                summary.waiting_retry_count += 1;
                summary.waiting_retry_bytes += bytes;
            }
            ReplicationQueueStatus::Queued => {
                summary.queued_count += 1;
                summary.queued_bytes += bytes;
            }
            ReplicationQueueStatus::InFlight => {
                summary.in_flight_count += 1;
                summary.in_flight_bytes += bytes;
            }
            ReplicationQueueStatus::Succeeded => {
                summary.succeeded_count += 1;
                summary.succeeded_bytes += bytes;
            }
            ReplicationQueueStatus::Failed => {
                summary.failed_count += 1;
                summary.failed_bytes += bytes;
            }
        }
    }
    summary
}

pub fn runtime_queue_to_node_stats(
    snapshot: &ReplicationRuntimeNodeSnapshot,
    now_ms: i64,
) -> ReplQNodeStats {
    let breakdown = summarize_runtime_queue(&snapshot.queue, now_ms);
    let pending_count = breakdown.queued_count + breakdown.waiting_retry_count;
    let pending_bytes = breakdown.queued_bytes + breakdown.waiting_retry_bytes;

    let mut per_target_xfer = BTreeMap::<String, XferStats>::new();
    for entry in snapshot.queue.entries.values() {
        if entry.status != ReplicationQueueStatus::Succeeded {
            continue;
        }
        let stats = per_target_xfer.entry(entry.target_arn.clone()).or_default();
        let bytes = entry.payload_size as f64;
        stats.curr += bytes;
        stats.avg += bytes;
        stats.peak = stats.peak.max(bytes);
        stats.n += 1;
    }

    ReplQNodeStats {
        node_name: snapshot.node_name.clone(),
        uptime: snapshot.uptime,
        active_workers: ActiveWorkerStat {
            curr: breakdown.in_flight_count.min(i32::MAX as u64) as i32,
            avg: breakdown.in_flight_count as f32,
            max: breakdown.in_flight_count.min(i32::MAX as u64) as i32,
        },
        xfer_stats: if per_target_xfer.is_empty() {
            None
        } else {
            Some(per_target_xfer)
        },
        tgt_xfer_stats: None,
        q_stats: InQueueMetric {
            curr: QStat {
                count: pending_count as f64,
                bytes: pending_bytes as f64,
            },
            avg: QStat {
                count: pending_count as f64,
                bytes: pending_bytes as f64,
            },
            max: QStat {
                count: pending_count as f64,
                bytes: pending_bytes as f64,
            },
        },
        mrf_stats: ReplicationMRFStats {
            last_failed_count: breakdown.failed_count,
            total_dropped_count: snapshot.queue.stats.total_failed,
            total_dropped_bytes: snapshot.queue.stats.failed_bytes,
        },
    }
}

pub fn runtime_snapshots_to_queue_stats(
    snapshots: &[ReplicationRuntimeNodeSnapshot],
    now_ms: i64,
) -> ReplicationQueueStats {
    ReplicationQueueStats {
        nodes: if snapshots.is_empty() {
            None
        } else {
            Some(
                snapshots
                    .iter()
                    .map(|snapshot| runtime_queue_to_node_stats(snapshot, now_ms))
                    .collect(),
            )
        },
        uptime: snapshots
            .iter()
            .map(|snapshot| snapshot.uptime)
            .max()
            .unwrap_or_default(),
    }
}

pub fn build_replication_admin_status_payload_from_runtime(
    objects: &[ReplicationObjectInfo],
    snapshots: &[ReplicationRuntimeNodeSnapshot],
    now_ms: i64,
) -> ReplicationAdminStatusPayload {
    let queue = runtime_snapshots_to_queue_stats(snapshots, now_ms);
    build_replication_status_payload(objects, &queue)
}
