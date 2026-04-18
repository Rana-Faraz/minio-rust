use chrono::{TimeZone, Timelike, Utc};
use minio_rust::internal::amztime;

pub const SOURCE_FILE: &str = "internal/amztime/iso8601_time_test.go";

#[test]
fn iso8601_format_matches_reference_cases() {
    let test_cases = [
        (
            Utc.with_ymd_and_hms(2009, 11, 13, 4, 51, 1)
                .single()
                .expect("valid timestamp")
                .with_nanosecond(940_303_531)
                .expect("valid nanos"),
            "2009-11-13T04:51:01.940Z",
        ),
        (
            Utc.with_ymd_and_hms(2009, 11, 13, 4, 51, 1)
                .single()
                .expect("valid timestamp")
                .with_nanosecond(901_303_531)
                .expect("valid nanos"),
            "2009-11-13T04:51:01.901Z",
        ),
        (
            Utc.with_ymd_and_hms(2009, 11, 13, 4, 51, 1)
                .single()
                .expect("valid timestamp")
                .with_nanosecond(900_303_531)
                .expect("valid nanos"),
            "2009-11-13T04:51:01.900Z",
        ),
        (
            Utc.with_ymd_and_hms(2009, 11, 13, 4, 51, 1)
                .single()
                .expect("valid timestamp")
                .with_nanosecond(941_303_531)
                .expect("valid nanos"),
            "2009-11-13T04:51:01.941Z",
        ),
    ];

    for (input, expected) in test_cases {
        assert_eq!(amztime::iso8601_format(input), expected);
    }
}
