use std::collections::BTreeMap;

pub fn is_valid_location_constraint(location_constraint: &str, configured_region: &str) -> bool {
    let location_constraint = location_constraint.trim();
    location_constraint.is_empty()
        || crate::cmd::is_valid_region(location_constraint, configured_region)
}

pub fn extract_metadata_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| {
            let lower = key.to_ascii_lowercase();
            if lower.starts_with("x-amz-meta-") {
                Some((lower, value.clone()))
            } else {
                None
            }
        })
        .collect()
}

pub fn get_resource(path: &str, host: &str, domains: &[String]) -> String {
    let host = host.split(':').next().unwrap_or(host).trim_end_matches('.');
    let path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };

    for domain in domains {
        let domain = domain.trim().trim_end_matches('.');
        if let Some(bucket) = host.strip_suffix(&format!(".{domain}")) {
            if !bucket.is_empty()
                && !path.starts_with(&format!("/{bucket}/"))
                && path != format!("/{bucket}")
            {
                return format!("/{bucket}{}", path);
            }
        }
    }

    path
}
