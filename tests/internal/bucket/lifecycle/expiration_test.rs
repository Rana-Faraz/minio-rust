use chrono::{TimeZone, Utc};
use minio_rust::internal::bucket::lifecycle::{parse_lifecycle_config, Error, Expiration};

fn parse_expiration(fragment: &str) -> Result<Expiration, Error> {
    parse_lifecycle_config(&format!(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter>{fragment}</Rule></LifecycleConfiguration>"
    ))
    .map(|lc| lc.rules[0].expiration.clone())
}

#[test]
fn test_invalid_expiration_line_27() {
    for (fragment, expected) in [
        (
            "<Expiration><Days>0</Days></Expiration>",
            Error::LifecycleInvalidDays,
        ),
        (
            "<Expiration><Date>invalid date</Date></Expiration>",
            Error::LifecycleInvalidDate,
        ),
        (
            "<Expiration><Date>2019-04-20T00:01:00Z</Date></Expiration>",
            Error::LifecycleDateNotMidnight,
        ),
    ] {
        assert_eq!(parse_expiration(fragment).unwrap_err(), expected);
    }

    let valid_date = parse_expiration("<Expiration><Date>2019-04-20T00:00:00Z</Date></Expiration>")
        .expect("valid expiration date");
    assert_eq!(
        valid_date.date,
        Some(Utc.with_ymd_and_hms(2019, 4, 20, 0, 0, 0).single().unwrap())
    );

    let valid_days =
        parse_expiration("<Expiration><Days>3</Days></Expiration>").expect("valid expiration days");
    assert_eq!(valid_days.days, Some(3));

    for (fragment, expected) in [
        ("<Expiration></Expiration>", Error::XmlNotWellFormed),
        (
            "<Expiration><Days>3</Days><Date>2019-04-20T00:00:00Z</Date></Expiration>",
            Error::LifecycleInvalidExpiration,
        ),
        (
            "<Expiration><Days>3</Days><ExpiredObjectDeleteMarker>false</ExpiredObjectDeleteMarker></Expiration>",
            Error::LifecycleInvalidDeleteMarker,
        ),
        (
            "<Expiration><Date>2019-04-20T00:00:00Z</Date><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration>",
            Error::LifecycleInvalidDeleteAll,
        ),
    ] {
        let expiration = parse_expiration(fragment).expect("parse should succeed");
        assert_eq!(expiration.validate().unwrap_err(), expected);
    }

    let delete_all = parse_expiration(
        "<Expiration><Days>3</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration>",
    )
    .expect("valid delete all expiration");
    assert!(delete_all.validate().is_ok());
}

#[test]
fn subtest_test_invalid_expiration_fmt_sprintf_test_d_line_53() {
    test_invalid_expiration_line_27();
}

#[test]
fn subtest_test_invalid_expiration_fmt_sprintf_test_d_line_113() {
    test_invalid_expiration_line_27();
}
