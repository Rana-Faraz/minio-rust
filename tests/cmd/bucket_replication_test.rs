use std::collections::BTreeMap;

use minio_rust::cmd::{
    new_replicate_target_decision, BucketTarget, ReplicateDecision, ReplicationConfig,
    ReplicationObjectInfo, MINIO_REPLICATION_RESET_STATUS,
};

pub const SOURCE_FILE: &str = "cmd/bucket-replication_test.go";

#[test]
fn test_replication_resync_line_87() {
    let cases = [
        (
            "no replication config",
            ReplicationObjectInfo {
                size: 100,
                ..Default::default()
            },
            ReplicationConfig::default(),
            ReplicateDecision::default(),
            BTreeMap::new(),
            false,
        ),
        (
            "existing object replication config enabled, no versioning",
            ReplicationObjectInfo {
                size: 100,
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![],
            },
            ReplicateDecision::default(),
            BTreeMap::new(),
            false,
        ),
        (
            "existing object replication config enabled, versioning suspended",
            ReplicationObjectInfo {
                size: 100,
                version_id: "null".to_string(),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![],
            },
            ReplicateDecision::default(),
            BTreeMap::new(),
            false,
        ),
        (
            "existing object replication enabled, versioning enabled; no reset in progress",
            ReplicationObjectInfo {
                size: 100,
                replication_status: "COMPLETED".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![],
            },
            ReplicateDecision::default(),
            BTreeMap::new(),
            false,
        ),
    ];

    for (name, object, cfg, decision, statuses, expected) in cases {
        let sync = cfg.resync_ctx(&object, &decision, &statuses);
        assert_eq!(sync.must_resync(), expected, "{name}");
    }
}

#[test]
fn test_replication_resyncwrapper_line_283() {
    let start = 1_700_000_000i64;
    let now = start + 86_400;
    let remote = BucketTarget {
        arn: "arn1".to_string(),
        reset_id: String::new(),
        reset_before_date: 0,
    };
    let reset_remote = |reset_id: &str, reset_before_date: i64| BucketTarget {
        arn: "arn1".to_string(),
        reset_id: reset_id.to_string(),
        reset_before_date,
    };
    let decision_true = ReplicateDecision {
        targets_map: Some(BTreeMap::from([(
            "arn1".to_string(),
            new_replicate_target_decision("arn1", true, false),
        )])),
    };
    let decision_false = ReplicateDecision {
        targets_map: Some(BTreeMap::from([(
            "arn1".to_string(),
            new_replicate_target_decision("arn1", false, false),
        )])),
    };

    let cases = [
        (
            "pending replication",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:PENDING;".to_string(),
                replication_status: "PENDING".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![remote.clone()],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "failed replication",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:FAILED".to_string(),
                replication_status: "FAILED".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![remote.clone()],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "never replicated",
            ReplicationObjectInfo {
                size: 100,
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![remote.clone()],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "completed and not selected anymore",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:COMPLETED".to_string(),
                replication_status: "COMPLETED".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![remote.clone()],
            },
            decision_false,
            BTreeMap::new(),
            false,
        ),
        (
            "pending with new reset",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:PENDING;".to_string(),
                replication_status: "PENDING".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                user_defined: BTreeMap::from([(
                    MINIO_REPLICATION_RESET_STATUS.to_string(),
                    format!("{};abc", now - 2_592_000),
                )]),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![reset_remote("xyz", now)],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "failed with new reset",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:FAILED;".to_string(),
                replication_status: "FAILED".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                user_defined: BTreeMap::from([(
                    MINIO_REPLICATION_RESET_STATUS.to_string(),
                    format!("{};abc", now - 2_592_000),
                )]),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![reset_remote("xyz", now)],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "never replicated with reset",
            ReplicationObjectInfo {
                size: 100,
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                user_defined: BTreeMap::from([(
                    MINIO_REPLICATION_RESET_STATUS.to_string(),
                    format!("{};abc", now - 2_592_000),
                )]),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![reset_remote("xyz", now)],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "completed with new reset",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:COMPLETED;".to_string(),
                replication_status: "COMPLETED".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df8".to_string(),
                user_defined: BTreeMap::from([(
                    MINIO_REPLICATION_RESET_STATUS.to_string(),
                    format!("{};abc", now - 2_592_000),
                )]),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![reset_remote("xyz", now)],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "newer reset on older object",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:PENDING;".to_string(),
                replication_status: "PENDING".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                user_defined: BTreeMap::from([(
                    MINIO_REPLICATION_RESET_STATUS.to_string(),
                    format!("{};abc", now - 86_400),
                )]),
                mod_time: now - 2 * 86_400,
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![reset_remote("xyz", now)],
            },
            decision_true.clone(),
            BTreeMap::new(),
            true,
        ),
        (
            "reset done and completed",
            ReplicationObjectInfo {
                size: 100,
                replication_status_internal: "arn1:COMPLETED;".to_string(),
                replication_status: "COMPLETED".to_string(),
                version_id: "a3348c34-c352-4498-82f0-1098e8b34df9".to_string(),
                user_defined: BTreeMap::from([(
                    MINIO_REPLICATION_RESET_STATUS.to_string(),
                    format!("{};xyz", start),
                )]),
                ..Default::default()
            },
            ReplicationConfig {
                enabled: true,
                remotes: vec![reset_remote("xyz", start)],
            },
            decision_true,
            BTreeMap::new(),
            false,
        ),
    ];

    for (name, object, cfg, decision, statuses, expected) in cases {
        let sync = cfg.resync(&object, &decision, &statuses);
        assert_eq!(sync.must_resync(), expected, "{name}");
    }
}
