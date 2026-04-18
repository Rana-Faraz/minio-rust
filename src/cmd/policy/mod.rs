#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyStatement {
    pub effect: PolicyEffect,
    pub actions: Vec<String>,
    pub resources: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    pub version: String,
    pub statements: Vec<PolicyStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketAccessPolicy {
    pub bucket: String,
    pub prefix: String,
    pub allow_get: bool,
    pub allow_put: bool,
    pub allow_delete: bool,
}

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

pub fn policy_sys_is_allowed(policy: &Policy, action: &str, resource: &str) -> bool {
    if policy
        .statements
        .iter()
        .filter(|statement| statement.effect == PolicyEffect::Deny)
        .any(|statement| {
            statement
                .actions
                .iter()
                .any(|pattern| wildcard_match(pattern, action))
                && statement
                    .resources
                    .iter()
                    .any(|pattern| wildcard_match(pattern, resource))
        })
    {
        return false;
    }

    policy
        .statements
        .iter()
        .filter(|statement| statement.effect == PolicyEffect::Allow)
        .any(|statement| {
            statement
                .actions
                .iter()
                .any(|pattern| wildcard_match(pattern, action))
                && statement
                    .resources
                    .iter()
                    .any(|pattern| wildcard_match(pattern, resource))
        })
}

fn object_resource(bucket: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        format!("arn:aws:s3:::{bucket}/*")
    } else {
        format!("arn:aws:s3:::{bucket}/{}*", prefix)
    }
}

pub fn bucket_access_policy_to_policy(policy: &BucketAccessPolicy) -> Policy {
    let mut actions = Vec::new();
    if policy.allow_get {
        actions.push("s3:GetObject".to_string());
    }
    if policy.allow_put {
        actions.push("s3:PutObject".to_string());
    }
    if policy.allow_delete {
        actions.push("s3:DeleteObject".to_string());
    }

    Policy {
        version: "2012-10-17".to_string(),
        statements: vec![PolicyStatement {
            effect: PolicyEffect::Allow,
            actions,
            resources: vec![object_resource(&policy.bucket, &policy.prefix)],
        }],
    }
}

pub fn policy_to_bucket_access_policy(policy: &Policy) -> Option<BucketAccessPolicy> {
    let statement = policy
        .statements
        .iter()
        .find(|statement| statement.effect == PolicyEffect::Allow)?;
    let resource = statement.resources.first()?;
    let prefix = "arn:aws:s3:::";
    let remainder = resource.strip_prefix(prefix)?;
    let (bucket, path) = remainder.split_once('/')?;
    let normalized_prefix = path.strip_suffix('*').unwrap_or(path).to_string();

    Some(BucketAccessPolicy {
        bucket: bucket.to_string(),
        prefix: normalized_prefix,
        allow_get: statement
            .actions
            .iter()
            .any(|action| action == "s3:GetObject"),
        allow_put: statement
            .actions
            .iter()
            .any(|action| action == "s3:PutObject"),
        allow_delete: statement
            .actions
            .iter()
            .any(|action| action == "s3:DeleteObject"),
    })
}
