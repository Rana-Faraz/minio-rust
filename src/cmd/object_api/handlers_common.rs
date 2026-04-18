use super::*;
use chrono::TimeZone;

fn parse_http_date(value: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(value) {
        return Some(dt.with_timezone(&Utc).timestamp());
    }

    let trimmed = value
        .split_once(", ")
        .map(|(_, rest)| rest)
        .unwrap_or(value)
        .trim_end_matches(" GMT");

    chrono::NaiveDateTime::parse_from_str(trimmed, "%d %b %Y %H:%M:%S")
        .ok()
        .map(|dt| Utc.from_utc_datetime(&dt).timestamp())
}

pub fn canonicalize_etag(etag: &str) -> String {
    etag.trim_matches('"').to_string()
}

pub fn check_preconditions(
    headers: &BTreeMap<String, String>,
    obj_info: &ObjectInfo,
    _opts: &ObjectOptions,
) -> (bool, u16) {
    let if_match = headers
        .get("if-match")
        .map(String::as_str)
        .unwrap_or_default();
    if !if_match.is_empty() && canonicalize_etag(if_match) != obj_info.etag {
        return (true, 412);
    }

    let if_unmodified_since = headers
        .get("if-unmodified-since")
        .and_then(|value| parse_http_date(value));
    if if_match.is_empty() && if_unmodified_since.is_some_and(|value| obj_info.mod_time > value) {
        return (true, 412);
    }

    let if_none_match = headers
        .get("if-none-match")
        .map(String::as_str)
        .unwrap_or_default();
    if !if_none_match.is_empty() && canonicalize_etag(if_none_match) == obj_info.etag {
        return (true, 304);
    }

    let if_modified_since = headers
        .get("if-modified-since")
        .and_then(|value| parse_http_date(value));
    if if_modified_since.is_some_and(|value| obj_info.mod_time <= value) {
        return (true, 304);
    }

    (false, 200)
}
