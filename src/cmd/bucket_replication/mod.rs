use std::collections::BTreeMap;

use crate::cmd::{
    ReplicateDecision, ReplicateTargetDecision, ResyncDecision, ResyncTargetDecision,
};

pub const MINIO_REPLICATION_RESET_STATUS: &str = "x-minio-replication-reset-status";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BucketTarget {
    pub arn: String,
    pub reset_id: String,
    pub reset_before_date: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationObjectInfo {
    pub bucket: String,
    pub name: String,
    pub size: i64,
    pub delete_marker: bool,
    pub version_id: String,
    pub mod_time: i64,
    pub replication_status: String,
    pub replication_status_internal: String,
    pub user_defined: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationConfig {
    pub enabled: bool,
    pub remotes: Vec<BucketTarget>,
}

impl ReplicationConfig {
    pub fn empty(&self) -> bool {
        !self.enabled
    }

    pub fn resync_ctx(
        &self,
        object: &ReplicationObjectInfo,
        decision: &ReplicateDecision,
        target_statuses: &BTreeMap<String, String>,
    ) -> ResyncDecision {
        if self.empty() {
            return ResyncDecision::default();
        }
        self.resync(object, decision, target_statuses)
    }

    pub fn resync(
        &self,
        object: &ReplicationObjectInfo,
        decision: &ReplicateDecision,
        target_statuses: &BTreeMap<String, String>,
    ) -> ResyncDecision {
        let mut targets = BTreeMap::new();
        let Some(decisions) = &decision.targets_map else {
            return ResyncDecision::default();
        };

        for target in &self.remotes {
            let Some(target_decision) = decisions.get(&target.arn) else {
                continue;
            };
            if !target_decision.replicate {
                continue;
            }
            let status = target_statuses
                .get(&target.arn)
                .cloned()
                .unwrap_or_default();
            let resync = resync_target(
                object,
                &target.arn,
                &target.reset_id,
                target.reset_before_date,
                &status,
            );
            targets.insert(target.arn.clone(), resync);
        }

        ResyncDecision {
            targets: if targets.is_empty() {
                None
            } else {
                Some(targets)
            },
        }
    }
}

impl ResyncDecision {
    pub fn must_resync(&self) -> bool {
        self.targets
            .as_ref()
            .is_some_and(|targets| targets.values().any(|target| target.replicate))
    }
}

pub fn new_replicate_target_decision(
    arn: &str,
    replicate: bool,
    synchronous: bool,
) -> ReplicateTargetDecision {
    ReplicateTargetDecision {
        replicate,
        synchronous,
        arn: arn.to_string(),
        id: String::new(),
    }
}

fn target_reset_header(arn: &str) -> String {
    format!("x-minio-replication-reset-{arn}")
}

fn resync_target(
    object: &ReplicationObjectInfo,
    arn: &str,
    reset_id: &str,
    reset_before_date: i64,
    target_status: &str,
) -> ResyncTargetDecision {
    let mut decision = ResyncTargetDecision {
        replicate: false,
        reset_id: reset_id.to_string(),
        reset_before_date,
    };

    let reset_state = object
        .user_defined
        .get(&target_reset_header(arn))
        .or_else(|| object.user_defined.get(MINIO_REPLICATION_RESET_STATUS));

    let Some(reset_state) = reset_state else {
        if !reset_id.is_empty() && object.mod_time < reset_before_date {
            decision.replicate = true;
            return decision;
        }
        decision.replicate = target_status.is_empty();
        return decision;
    };

    if reset_id.is_empty() || reset_before_date == 0 {
        return decision;
    }

    let mut parts = reset_state.splitn(2, ';');
    let _reset_at = parts.next();
    let Some(previous_reset_id) = parts.next() else {
        return decision;
    };

    let new_reset = previous_reset_id != reset_id;
    if !new_reset && target_status == "COMPLETED" {
        return decision;
    }

    decision.replicate = new_reset && object.mod_time < reset_before_date;
    decision
}
