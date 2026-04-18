use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::error::Error as StdError;
use std::fmt;

pub const ISO8601_TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";
const ISO8601_TIME_FORMAT_LONG: &str = "%Y-%m-%dT%H:%M:%S%.6fZ";
const HTTP_TIME_FORMATS: [&str; 2] = ["%a, %d %b %Y %H:%M:%S GMT", "%a, %-d %b %Y %H:%M:%S GMT"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    MalformedDate,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedDate => f.write_str("malformed date"),
        }
    }
}

impl StdError for Error {}

pub fn iso8601_format(time: DateTime<Utc>) -> String {
    time.format(ISO8601_TIME_FORMAT).to_string()
}

pub fn iso8601_parse(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    for layout in [ISO8601_TIME_FORMAT, ISO8601_TIME_FORMAT_LONG] {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, layout) {
            return Ok(Utc.from_utc_datetime(&parsed));
        }
    }

    DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
}

pub fn parse(value: &str) -> Result<DateTime<Utc>, Error> {
    if let Ok(parsed) = NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ") {
        return Ok(Utc.from_utc_datetime(&parsed));
    }

    if let Ok(parsed) = NaiveDateTime::parse_from_str(value, "%a, %d %b %Y %H:%M:%S UTC") {
        return Ok(Utc.from_utc_datetime(&parsed));
    }

    if let Ok(parsed) = DateTime::parse_from_rfc2822(value) {
        return Ok(parsed.with_timezone(&Utc));
    }

    if let Ok(parsed) = DateTime::parse_from_str(value, "%a, %d %b %Y %H:%M:%S %z") {
        return Ok(parsed.with_timezone(&Utc));
    }

    Err(Error::MalformedDate)
}

pub fn parse_header(value: &str) -> Result<DateTime<Utc>, Error> {
    for layout in HTTP_TIME_FORMATS {
        if let Ok(parsed) = NaiveDateTime::parse_from_str(value, layout) {
            return Ok(Utc.from_utc_datetime(&parsed));
        }
    }

    Err(Error::MalformedDate)
}

pub fn parse_replication_ts(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    if let Ok(parsed) = DateTime::parse_from_rfc2822(value) {
        return Ok(parsed.with_timezone(&Utc));
    }

    DateTime::parse_from_rfc3339(value).map(|parsed| parsed.with_timezone(&Utc))
}
