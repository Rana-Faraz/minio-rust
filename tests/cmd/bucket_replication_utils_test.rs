use std::collections::BTreeMap;

use minio_rust::cmd::{
    composite_replication_status, parse_replicate_decision, replicated_infos, ReplicationObjectInfo,
};

pub const SOURCE_FILE: &str = "cmd/bucket-replication-utils_test.go";

#[test]
fn test_replicated_infos_line_116() {
    let objects = vec![
        ReplicationObjectInfo {
            replication_status_internal: "COMPLETED".to_string(),
            ..Default::default()
        },
        ReplicationObjectInfo {
            replication_status_internal: "PENDING".to_string(),
            ..Default::default()
        },
        ReplicationObjectInfo {
            replication_status_internal: "FAILED".to_string(),
            ..Default::default()
        },
        ReplicationObjectInfo {
            replication_status_internal: "REPLICA".to_string(),
            ..Default::default()
        },
    ];

    let infos = replicated_infos(&objects);
    assert_eq!(infos.completed, 1);
    assert_eq!(infos.pending, 1);
    assert_eq!(infos.failed, 1);
    assert_eq!(infos.replica, 1);
}

#[test]
fn test_parse_replicate_decision_line_184() {
    let decision = parse_replicate_decision(
        r#"{"arn:minio:replication:::west":true,"arn:minio:replication:::east":false}"#,
    )
    .expect("parse decision");
    let targets = decision.targets_map.expect("targets");
    assert_eq!(targets.len(), 2);
    assert_eq!(
        targets
            .get("arn:minio:replication:::west")
            .map(|target| target.replicate),
        Some(true)
    );
    assert_eq!(
        targets
            .get("arn:minio:replication:::east")
            .map(|target| target.replicate),
        Some(false)
    );

    let empty = parse_replicate_decision("").expect("empty decision");
    assert!(empty.targets_map.is_none());
}

#[test]
fn test_composite_replication_status_line_241() {
    let cases = BTreeMap::from([
        (vec!["COMPLETED", "COMPLETED"], "COMPLETED"),
        (vec!["COMPLETED", "PENDING"], "PENDING"),
        (vec!["COMPLETED", "FAILED"], "FAILED"),
        (vec!["REPLICA", "REPLICA"], "REPLICA"),
        (Vec::<&str>::new(), ""),
    ]);

    for (statuses, expected) in cases {
        assert_eq!(
            composite_replication_status(statuses.iter().copied()),
            expected
        );
    }
}
