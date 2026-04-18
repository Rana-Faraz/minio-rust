use std::collections::BTreeMap;

use crate::cmd::{ReplicateDecision, ReplicateTargetDecision, ReplicationObjectInfo};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicatedInfos {
    pub completed: usize,
    pub pending: usize,
    pub failed: usize,
    pub replica: usize,
}

pub fn replicated_infos(objects: &[ReplicationObjectInfo]) -> ReplicatedInfos {
    let mut infos = ReplicatedInfos::default();
    for object in objects {
        let status = if object.replication_status_internal.is_empty() {
            object.replication_status.as_str()
        } else {
            object.replication_status_internal.as_str()
        };
        match status {
            "COMPLETED" => infos.completed += 1,
            "PENDING" => infos.pending += 1,
            "FAILED" => infos.failed += 1,
            "REPLICA" => infos.replica += 1,
            _ => {}
        }
    }
    infos
}

pub fn parse_replicate_decision(input: &str) -> Result<ReplicateDecision, String> {
    if input.trim().is_empty() {
        return Ok(ReplicateDecision::default());
    }

    let raw: BTreeMap<String, bool> = serde_json::from_str(input).map_err(|err| err.to_string())?;
    let targets_map = raw
        .into_iter()
        .map(|(arn, replicate)| {
            (
                arn.clone(),
                ReplicateTargetDecision {
                    replicate,
                    synchronous: false,
                    arn,
                    id: String::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok(ReplicateDecision {
        targets_map: Some(targets_map),
    })
}

pub fn composite_replication_status<'a>(statuses: impl IntoIterator<Item = &'a str>) -> String {
    let statuses = statuses.into_iter().collect::<Vec<_>>();
    if statuses.is_empty() {
        return String::new();
    }
    if statuses.iter().any(|status| *status == "FAILED") {
        return "FAILED".to_string();
    }
    if statuses.iter().any(|status| *status == "PENDING") {
        return "PENDING".to_string();
    }
    if statuses.iter().all(|status| *status == "REPLICA") {
        return "REPLICA".to_string();
    }
    if statuses.iter().all(|status| *status == "COMPLETED") {
        return "COMPLETED".to_string();
    }
    "PENDING".to_string()
}
