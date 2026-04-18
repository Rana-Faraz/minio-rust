use super::*;
use std::collections::BTreeMap;

pub const REPLICATION_STATUS_COMPLETED: &str = "COMPLETED";
pub const REPLICATION_STATUS_PENDING: &str = "PENDING";
pub const REPLICATION_STATUS_FAILED: &str = "FAILED";
pub const REPLICATION_STATUS_REPLICA: &str = "REPLICA";
pub const REPLICATION_STATUS_UNKNOWN: &str = "UNKNOWN";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationStatusSummary {
    pub status: String,
    pub count: u64,
    pub bytes: u64,
}
impl_msg_codec!(ReplicationStatusSummary);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationStatusCounters {
    pub total_count: u64,
    pub total_bytes: u64,
    pub completed_count: u64,
    pub completed_bytes: u64,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub failed_count: u64,
    pub failed_bytes: u64,
    pub replica_count: u64,
    pub replica_bytes: u64,
    pub unknown_count: u64,
    pub unknown_bytes: u64,
}
impl_msg_codec!(ReplicationStatusCounters);

impl ReplicationStatusCounters {
    pub fn summaries(&self) -> Vec<ReplicationStatusSummary> {
        vec![
            ReplicationStatusSummary {
                status: REPLICATION_STATUS_COMPLETED.to_string(),
                count: self.completed_count,
                bytes: self.completed_bytes,
            },
            ReplicationStatusSummary {
                status: REPLICATION_STATUS_PENDING.to_string(),
                count: self.pending_count,
                bytes: self.pending_bytes,
            },
            ReplicationStatusSummary {
                status: REPLICATION_STATUS_FAILED.to_string(),
                count: self.failed_count,
                bytes: self.failed_bytes,
            },
            ReplicationStatusSummary {
                status: REPLICATION_STATUS_REPLICA.to_string(),
                count: self.replica_count,
                bytes: self.replica_bytes,
            },
            ReplicationStatusSummary {
                status: REPLICATION_STATUS_UNKNOWN.to_string(),
                count: self.unknown_count,
                bytes: self.unknown_bytes,
            },
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationTargetStatusSummary {
    pub arn: String,
    pub total_count: u64,
    pub total_bytes: u64,
    pub completed_count: u64,
    pub pending_count: u64,
    pub failed_count: u64,
    pub replica_count: u64,
    pub unknown_count: u64,
    pub last_status: String,
}
impl_msg_codec!(ReplicationTargetStatusSummary);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReplicationQueueSnapshot {
    pub node_count: u64,
    pub uptime: i64,
    pub active_workers_curr: i64,
    pub active_workers_avg: f64,
    pub active_workers_max: i64,
    pub queue_curr_count: f64,
    pub queue_curr_bytes: f64,
    pub queue_avg_count: f64,
    pub queue_avg_bytes: f64,
    pub queue_max_count: f64,
    pub queue_max_bytes: f64,
}
impl_msg_codec!(ReplicationQueueSnapshot);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationResyncTargetStatusSummary {
    pub bucket: String,
    pub arn: String,
    pub resync_id: String,
    pub resync_before_date: i64,
    pub start_time: i64,
    pub last_updated: i64,
    pub status: String,
    pub scheduled_count: u64,
    pub scheduled_bytes: u64,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub completed_count: u64,
    pub completed_bytes: u64,
    pub failed_count: u64,
    pub failed_bytes: u64,
}
impl_msg_codec!(ReplicationResyncTargetStatusSummary);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReplicationAdminStatusPayload {
    pub overview: ReplicationStatusCounters,
    pub statuses: Vec<ReplicationStatusSummary>,
    pub targets: Vec<ReplicationTargetStatusSummary>,
    pub resync_targets: Vec<ReplicationResyncTargetStatusSummary>,
    pub queue: ReplicationQueueSnapshot,
}
impl_msg_codec!(ReplicationAdminStatusPayload);

pub fn parse_replication_target_statuses(input: &str) -> BTreeMap<String, String> {
    let mut parsed = BTreeMap::new();
    for entry in input
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let Some((arn, status)) = entry.rsplit_once(':') else {
            continue;
        };
        let arn = arn.trim();
        let status = normalize_status(status);
        if !arn.is_empty() && !status.is_empty() {
            parsed.insert(arn.to_string(), status);
        }
    }
    parsed
}

pub fn object_replication_status(object: &ReplicationObjectInfo) -> String {
    if !object.replication_status.is_empty() {
        return normalize_status(&object.replication_status);
    }

    let targets = parse_replication_target_statuses(&object.replication_status_internal);
    if !targets.is_empty() {
        return composite_status_from_targets(targets.values().map(String::as_str));
    }

    let internal = normalize_status(&object.replication_status_internal);
    if internal.is_empty() {
        REPLICATION_STATUS_UNKNOWN.to_string()
    } else {
        internal
    }
}

pub fn summarize_replication_objects(
    objects: &[ReplicationObjectInfo],
) -> ReplicationStatusCounters {
    let mut counters = ReplicationStatusCounters::default();
    for object in objects {
        let bytes = object.size.max(0) as u64;
        let status = object_replication_status(object);
        counters.total_count += 1;
        counters.total_bytes += bytes;
        apply_status_counts(&mut counters, &status, bytes);
    }
    counters
}

pub fn summarize_bucket_replication_stats(
    stats: &BucketReplicationStats,
) -> ReplicationStatusCounters {
    ReplicationStatusCounters {
        total_count: non_negative_i64(
            stats.replicated_count + stats.pending_count + stats.failed_count + stats.replica_count,
        ),
        total_bytes: non_negative_i64(
            stats.replicated_size + stats.pending_size + stats.failed_size + stats.replica_size,
        ),
        completed_count: non_negative_i64(stats.replicated_count),
        completed_bytes: non_negative_i64(stats.replicated_size),
        pending_count: non_negative_i64(stats.pending_count),
        pending_bytes: non_negative_i64(stats.pending_size),
        failed_count: non_negative_i64(stats.failed_count),
        failed_bytes: non_negative_i64(stats.failed_size),
        replica_count: non_negative_i64(stats.replica_count),
        replica_bytes: non_negative_i64(stats.replica_size),
        unknown_count: 0,
        unknown_bytes: 0,
    }
}

pub fn aggregate_replication_target_statuses(
    objects: &[ReplicationObjectInfo],
) -> Vec<ReplicationTargetStatusSummary> {
    let mut summaries = BTreeMap::<String, ReplicationTargetStatusSummary>::new();
    for object in objects {
        let bytes = object.size.max(0) as u64;
        for (arn, status) in parse_replication_target_statuses(&object.replication_status_internal)
        {
            let summary =
                summaries
                    .entry(arn.clone())
                    .or_insert_with(|| ReplicationTargetStatusSummary {
                        arn: arn.clone(),
                        ..ReplicationTargetStatusSummary::default()
                    });
            summary.total_count += 1;
            summary.total_bytes += bytes;
            summary.last_status = status.clone();
            match status.as_str() {
                REPLICATION_STATUS_COMPLETED => summary.completed_count += 1,
                REPLICATION_STATUS_PENDING => summary.pending_count += 1,
                REPLICATION_STATUS_FAILED => summary.failed_count += 1,
                REPLICATION_STATUS_REPLICA => summary.replica_count += 1,
                _ => summary.unknown_count += 1,
            }
        }
    }
    summaries.into_values().collect()
}

pub fn snapshot_replication_queue(stats: &ReplicationQueueStats) -> ReplicationQueueSnapshot {
    let mut snapshot = ReplicationQueueSnapshot {
        node_count: stats
            .nodes
            .as_ref()
            .map(|nodes| nodes.len() as u64)
            .unwrap_or(0),
        uptime: stats.uptime,
        ..ReplicationQueueSnapshot::default()
    };

    if let Some(nodes) = &stats.nodes {
        for node in nodes {
            apply_queue_node(&mut snapshot, node);
        }
        if !nodes.is_empty() {
            snapshot.active_workers_avg /= nodes.len() as f64;
        }
    }

    snapshot
}

pub fn build_replication_status_payload(
    objects: &[ReplicationObjectInfo],
    queue: &ReplicationQueueStats,
) -> ReplicationAdminStatusPayload {
    let overview = summarize_replication_objects(objects);
    ReplicationAdminStatusPayload {
        statuses: overview.summaries(),
        targets: aggregate_replication_target_statuses(objects),
        resync_targets: Vec::new(),
        queue: snapshot_replication_queue(queue),
        overview,
    }
}

fn apply_queue_node(snapshot: &mut ReplicationQueueSnapshot, node: &ReplQNodeStats) {
    snapshot.uptime = snapshot.uptime.max(node.uptime);
    snapshot.active_workers_curr += i64::from(node.active_workers.curr);
    snapshot.active_workers_avg += f64::from(node.active_workers.avg);
    snapshot.active_workers_max += i64::from(node.active_workers.max);
    snapshot.queue_curr_count += node.q_stats.curr.count;
    snapshot.queue_curr_bytes += node.q_stats.curr.bytes;
    snapshot.queue_avg_count += node.q_stats.avg.count;
    snapshot.queue_avg_bytes += node.q_stats.avg.bytes;
    snapshot.queue_max_count += node.q_stats.max.count;
    snapshot.queue_max_bytes += node.q_stats.max.bytes;
}

fn apply_status_counts(counters: &mut ReplicationStatusCounters, status: &str, bytes: u64) {
    match status {
        REPLICATION_STATUS_COMPLETED => {
            counters.completed_count += 1;
            counters.completed_bytes += bytes;
        }
        REPLICATION_STATUS_PENDING => {
            counters.pending_count += 1;
            counters.pending_bytes += bytes;
        }
        REPLICATION_STATUS_FAILED => {
            counters.failed_count += 1;
            counters.failed_bytes += bytes;
        }
        REPLICATION_STATUS_REPLICA => {
            counters.replica_count += 1;
            counters.replica_bytes += bytes;
        }
        _ => {
            counters.unknown_count += 1;
            counters.unknown_bytes += bytes;
        }
    }
}

fn normalize_status(status: &str) -> String {
    status
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_uppercase()
}

fn composite_status_from_targets<'a>(statuses: impl IntoIterator<Item = &'a str>) -> String {
    let statuses = statuses
        .into_iter()
        .map(normalize_status)
        .filter(|status| !status.is_empty())
        .collect::<Vec<_>>();
    if statuses.is_empty() {
        return REPLICATION_STATUS_UNKNOWN.to_string();
    }
    if statuses
        .iter()
        .any(|status| status == REPLICATION_STATUS_PENDING)
    {
        return REPLICATION_STATUS_PENDING.to_string();
    }
    if statuses
        .iter()
        .any(|status| status == REPLICATION_STATUS_FAILED)
    {
        return REPLICATION_STATUS_FAILED.to_string();
    }
    if statuses
        .iter()
        .all(|status| status == REPLICATION_STATUS_REPLICA)
    {
        return REPLICATION_STATUS_REPLICA.to_string();
    }
    if statuses
        .iter()
        .all(|status| status == REPLICATION_STATUS_COMPLETED)
    {
        return REPLICATION_STATUS_COMPLETED.to_string();
    }
    REPLICATION_STATUS_UNKNOWN.to_string()
}

fn non_negative_i64(value: i64) -> u64 {
    value.max(0) as u64
}

#[cfg(test)]
#[path = "../../../tests/cmd/replication_status_test.rs"]
mod replication_status_test;
