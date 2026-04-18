use chrono::{DateTime, Utc};

pub fn must_get_request_id(time: DateTime<Utc>) -> String {
    format!("{:X}", time.timestamp_nanos_opt().unwrap_or(0))
}
