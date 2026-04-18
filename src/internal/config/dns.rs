use std::path::PathBuf;

const ETCD_PATH_SEPARATOR: &str = "/";

pub fn dns_join(labels: &[&str]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    if labels.last() == Some(&".") {
        return format!("{}.", labels[..labels.len() - 1].join("."));
    }
    format!("{}.", labels.join("."))
}

pub fn msg_path(domain: &str, prefix: &str) -> String {
    let mut labels = split_domain_name(domain);
    labels.reverse();

    let mut path = PathBuf::from(format!(
        "{ETCD_PATH_SEPARATOR}{prefix}{ETCD_PATH_SEPARATOR}"
    ));
    for label in labels {
        path.push(label);
    }
    path.to_string_lossy().replace('\\', "/")
}

pub fn msg_unpath(path: &str) -> String {
    let mut parts = path
        .trim_matches('/')
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    parts.reverse();
    parts.join(".")
}

fn split_domain_name(domain: &str) -> Vec<&str> {
    domain
        .trim_end_matches('.')
        .split('.')
        .filter(|part| !part.is_empty())
        .collect()
}
