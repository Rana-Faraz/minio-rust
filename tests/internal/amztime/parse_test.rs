use chrono::{TimeZone, Utc};
use minio_rust::internal::amztime;

pub const SOURCE_FILE: &str = "internal/amztime/parse_test.go";

#[test]
fn parse_matches_reference_cases() {
    let success = amztime::parse("Tue, 10 Nov 2009 23:00:00 UTC").expect("expected parse success");
    let expected = Utc
        .with_ymd_and_hms(2009, 11, 10, 23, 0, 0)
        .single()
        .expect("valid timestamp");
    assert_eq!(success, expected);

    let err =
        amztime::parse("Tue Sep  6 07:10:23 PM PDT 2022").expect_err("expected parse failure");
    assert_eq!(err, amztime::Error::MalformedDate);
}
