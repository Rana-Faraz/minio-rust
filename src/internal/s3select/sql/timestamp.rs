use chrono::{DateTime, Datelike, FixedOffset, TimeZone, Timelike};

use super::errors::Error;

pub fn parse_sql_timestamp(s: &str) -> Result<DateTime<FixedOffset>, Error> {
    let utc = FixedOffset::east_opt(0).expect("zero UTC offset is valid");
    if let Some(prefix) = s.strip_suffix('T') {
        match prefix.len() {
            4 => {
                if let Ok(year) = prefix.parse::<i32>() {
                    return Ok(utc
                        .with_ymd_and_hms(year, 1, 1, 0, 0, 0)
                        .single()
                        .expect("valid year-only timestamp"));
                }
            }
            7 => {
                if let Some((year, month)) = prefix.split_once('-') {
                    if let (Ok(year), Ok(month)) = (year.parse::<i32>(), month.parse::<u32>()) {
                        return Ok(utc
                            .with_ymd_and_hms(year, month, 1, 0, 0, 0)
                            .single()
                            .expect("valid year-month timestamp"));
                    }
                }
            }
            10 => {
                if let Ok(date) = chrono::NaiveDate::parse_from_str(prefix, "%Y-%m-%d") {
                    return Ok(utc
                        .from_local_datetime(&date.and_hms_opt(0, 0, 0).expect("midnight"))
                        .single()
                        .expect("valid year-month-day timestamp"));
                }
            }
            _ => {}
        }
    }

    let normalized = if let Some(prefix) = s.strip_suffix('Z') {
        format!("{prefix}+00:00")
    } else {
        s.to_owned()
    };
    for layout in [
        "%Y-%m-%dT%H:%M%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
    ] {
        if let Ok(value) = DateTime::parse_from_str(&normalized, layout) {
            return Ok(value);
        }
    }

    Err(Error::TimestampParse(format!(
        "failed to parse SQL timestamp: {s}"
    )))
}

pub fn format_sql_timestamp(t: DateTime<FixedOffset>) -> String {
    let offset = t.offset().local_minus_utc();
    let has_zone = offset != 0;
    let has_frac_second = t.nanosecond() != 0;
    let has_second = t.second() != 0;
    let has_time = t.hour() != 0 || t.minute() != 0;
    let has_day = t.day() != 1;
    let has_month = t.month() != 1;

    match () {
        _ if has_frac_second => {
            let frac = format!("{:09}", t.nanosecond());
            let frac = frac.trim_end_matches('0');
            if offset == 0 {
                format!("{}.{frac}Z", t.format("%Y-%m-%dT%H:%M:%S"))
            } else {
                format!(
                    "{}.{frac}{}",
                    t.format("%Y-%m-%dT%H:%M:%S"),
                    t.format("%:z")
                )
            }
        }
        _ if has_second => {
            if offset == 0 {
                t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
            } else {
                t.format("%Y-%m-%dT%H:%M:%S%:z").to_string()
            }
        }
        _ if has_time || has_zone => {
            if offset == 0 {
                t.format("%Y-%m-%dT%H:%MZ").to_string()
            } else {
                t.format("%Y-%m-%dT%H:%M%:z").to_string()
            }
        }
        _ if has_day => t.format("%Y-%m-%dT").to_string(),
        _ if has_month => t.format("%Y-%mT").to_string(),
        _ => t.format("%YT").to_string(),
    }
}
