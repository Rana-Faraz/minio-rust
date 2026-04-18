use crate::cmd::StsPolicy;

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
        if let Some(found) = value[pos..].find(part) {
            pos += found + part.len();
        } else {
            return false;
        }
    }
    if anchored_end {
        if let Some(last) = parts.iter().rev().find(|part| !part.is_empty()) {
            return value.ends_with(last);
        }
    }
    true
}

fn any_pattern_matches<'a>(patterns: impl IntoIterator<Item = &'a String>, value: &str) -> bool {
    patterns
        .into_iter()
        .any(|pattern| wildcard_match(pattern, value))
}

pub fn expand_pattern(pattern: &str, username: &str) -> String {
    pattern.replace("${aws:username}", username)
}

pub fn policy_allows(policy: &StsPolicy, username: &str, action: &str, resource: &str) -> bool {
    if any_pattern_matches(policy.deny_actions.iter(), action) {
        return false;
    }
    if !any_pattern_matches(policy.allow_actions.iter(), action) {
        return false;
    }
    policy
        .resource_patterns
        .iter()
        .map(|pattern| expand_pattern(pattern, username))
        .any(|pattern| wildcard_match(&pattern, resource))
}

pub fn policy_denies(policy: &StsPolicy, username: &str, action: &str, resource: &str) -> bool {
    any_pattern_matches(policy.deny_actions.iter(), action)
        && policy
            .resource_patterns
            .iter()
            .map(|pattern| expand_pattern(pattern, username))
            .any(|pattern| wildcard_match(&pattern, resource))
}
