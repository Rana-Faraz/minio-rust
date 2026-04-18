use chrono::{FixedOffset, TimeZone, Timelike};

use minio_rust::internal::s3select::sql::{format_sql_timestamp, parse_sql_timestamp};

pub const SOURCE_FILE: &str = "internal/s3select/sql/timestampfuncs_test.go";

#[test]
fn parse_and_display_sqltimestamp_matches_reference_cases() {
    let beijing = FixedOffset::east_opt(8 * 60 * 60).expect("valid offset");
    let fake_los_angeles = FixedOffset::west_opt(8 * 60 * 60).expect("valid offset");
    let cases = [
        (
            "2010T",
            FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2010, 1, 1, 0, 0, 0)
                .single()
                .unwrap(),
        ),
        (
            "2010-02T",
            FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2010, 2, 1, 0, 0, 0)
                .single()
                .unwrap(),
        ),
        (
            "2010-02-03T",
            FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2010, 2, 3, 0, 0, 0)
                .single()
                .unwrap(),
        ),
        (
            "2010-02-03T04:11Z",
            FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 0)
                .single()
                .unwrap(),
        ),
        (
            "2010-02-03T04:11:30Z",
            FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 30)
                .single()
                .unwrap(),
        ),
        (
            "2010-02-03T04:11:30.23Z",
            FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 30)
                .single()
                .unwrap()
                .with_nanosecond(230_000_000)
                .unwrap(),
        ),
        (
            "2010-02-03T04:11+08:00",
            beijing
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 0)
                .single()
                .unwrap(),
        ),
        (
            "2010-02-03T04:11:30+08:00",
            beijing
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 30)
                .single()
                .unwrap(),
        ),
        (
            "2010-02-03T04:11:30.23+08:00",
            beijing
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 30)
                .single()
                .unwrap()
                .with_nanosecond(230_000_000)
                .unwrap(),
        ),
        (
            "2010-02-03T04:11:30-08:00",
            fake_los_angeles
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 30)
                .single()
                .unwrap(),
        ),
        (
            "2010-02-03T04:11:30.23-08:00",
            fake_los_angeles
                .with_ymd_and_hms(2010, 2, 3, 4, 11, 30)
                .single()
                .unwrap()
                .with_nanosecond(230_000_000)
                .unwrap(),
        ),
    ];

    for (index, (input, expected_time)) in cases.into_iter().enumerate() {
        let parsed = parse_sql_timestamp(input)
            .unwrap_or_else(|err| panic!("timestamp parse case {index} failed: {err}"));
        assert_eq!(parsed, expected_time, "parse case {index}");
        assert_eq!(
            format_sql_timestamp(expected_time),
            input,
            "format case {index}"
        );
    }
}
