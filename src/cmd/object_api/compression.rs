use super::*;

fn has_string_suffix_in_slice(value: &str, suffixes: &[String]) -> bool {
    let value = value.to_ascii_lowercase();
    suffixes
        .iter()
        .any(|suffix| suffix == "*" || value.ends_with(&suffix.to_ascii_lowercase()))
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern.eq_ignore_ascii_case(value);
    }

    let mut remainder = value;
    for (idx, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if idx == 0
            && !remainder
                .to_ascii_lowercase()
                .starts_with(&part.to_ascii_lowercase())
        {
            return false;
        }
        match remainder
            .to_ascii_lowercase()
            .find(&part.to_ascii_lowercase())
        {
            Some(found) => {
                let advance = found + part.len();
                remainder = &remainder[advance..];
            }
            None => return false,
        }
    }
    if !pattern.ends_with('*') {
        let tail = parts.last().copied().unwrap_or_default();
        value
            .to_ascii_lowercase()
            .ends_with(&tail.to_ascii_lowercase())
    } else {
        true
    }
}

fn has_pattern(patterns: &[String], value: &str) -> bool {
    patterns
        .iter()
        .any(|pattern| wildcard_match(pattern, value))
}

pub fn exclude_for_compression(
    headers: &BTreeMap<String, String>,
    object: &str,
    config: &CompressConfig,
) -> bool {
    let content_type = headers
        .get("Content-Type")
        .map(String::as_str)
        .unwrap_or_default();
    if !config.enabled {
        return true;
    }
    if !config.allow_encrypted
        && headers.keys().any(|key| {
            key.starts_with("X-Amz-Server-Side-Encryption")
                || key.starts_with("X-Minio-Internal-Server-Side-Encryption")
        })
    {
        return true;
    }

    let standard_extensions = [".zip".to_string(), ".gz".to_string(), ".tgz".to_string()];
    let standard_content_types = [
        "application/zip".to_string(),
        "application/gzip".to_string(),
        "application/x-gzip".to_string(),
    ];
    if has_string_suffix_in_slice(object, &standard_extensions)
        || has_pattern(&standard_content_types, content_type)
    {
        return true;
    }

    if config.extensions.is_empty() && config.mime_types.is_empty() {
        return false;
    }
    if !config.extensions.is_empty() && has_string_suffix_in_slice(object, &config.extensions) {
        return false;
    }
    if !config.mime_types.is_empty() && has_pattern(&config.mime_types, content_type) {
        return false;
    }
    true
}

pub fn get_compressed_offsets(object_info: &ObjectInfo, offset: i64) -> (i64, i64, usize) {
    let mut compressed_offset = 0i64;
    let mut cumulative_actual_size = 0i64;
    let mut first_part_idx = 0usize;
    let mut skip_length = 0i64;
    for (idx, part) in object_info.parts.iter().enumerate() {
        cumulative_actual_size += part.actual_size;
        if cumulative_actual_size <= offset {
            compressed_offset += part.size;
        } else {
            first_part_idx = idx;
            skip_length = cumulative_actual_size - part.actual_size;
            break;
        }
    }
    let part_skip = offset - skip_length;
    (compressed_offset, part_skip, first_part_idx)
}
