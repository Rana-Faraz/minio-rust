use chrono::{DateTime, Utc};

pub fn parse_version_time(version: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(version)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|err| err.to_string())
}
