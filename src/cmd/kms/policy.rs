use crate::cmd::{KmsEffect, KmsPolicyStatement};

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == value;
    }
    let parts = pattern.split('*').collect::<Vec<_>>();
    let mut pos = 0usize;
    let anchored_start = !pattern.starts_with('*');
    let anchored_end = !pattern.ends_with('*');

    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if idx == 0 && anchored_start {
            if !value[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
            continue;
        }
        let Some(found) = value[pos..].find(part) else {
            return false;
        };
        pos += found + part.len();
    }

    if anchored_end {
        if let Some(last) = parts.iter().rev().find(|part| !part.is_empty()) {
            return value.ends_with(last);
        }
    }
    true
}

fn action_matches(statement: &KmsPolicyStatement, action: &str) -> bool {
    statement
        .actions
        .iter()
        .any(|pattern| wildcard_match(pattern, action))
}

pub fn kms_resource_arn(key_id: &str) -> String {
    format!("arn:minio:kms:::{key_id}")
}

pub fn action_ignores_resources(action: &str) -> bool {
    matches!(
        action,
        "kms:Version"
            | "kms:API"
            | "kms:Metrics"
            | "kms:Status"
            | "admin:KMSCreateKey"
            | "admin:KMSKeyStatus"
    )
}

pub fn kms_policy_denies(statements: &[KmsPolicyStatement], action: &str) -> bool {
    statements
        .iter()
        .filter(|statement| statement.effect == KmsEffect::Deny)
        .any(|statement| action_matches(statement, action))
}

pub fn kms_policy_allows(
    statements: &[KmsPolicyStatement],
    action: &str,
    resource: Option<&str>,
) -> bool {
    statements
        .iter()
        .filter(|statement| statement.effect == KmsEffect::Allow)
        .filter(|statement| action_matches(statement, action))
        .any(|statement| {
            if action_ignores_resources(action) || statement.resources.is_empty() {
                return true;
            }
            let Some(resource) = resource else {
                return false;
            };
            statement
                .resources
                .iter()
                .any(|pattern| wildcard_match(pattern, resource))
        })
}

pub fn resource_allowed(statements: &[KmsPolicyStatement], action: &str, resource: &str) -> bool {
    if kms_policy_denies(statements, action) {
        return false;
    }
    kms_policy_allows(statements, action, Some(resource))
}

pub fn pattern_matches_key(pattern: &str, key_id: &str) -> bool {
    wildcard_match(pattern, key_id)
}
