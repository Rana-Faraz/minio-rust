// Rust test snapshot derived from cmd/object-handlers-common_test.go.

use std::collections::BTreeMap;

use chrono::{TimeZone, Utc};

use minio_rust::cmd::{canonicalize_etag, check_preconditions, ObjectInfo, ObjectOptions};

pub const SOURCE_FILE: &str = "cmd/object-handlers-common_test.go";

#[test]
fn test_canonicalize_etag_line_31() {
    let cases = [
        ("\"\"\"", ""),
        ("\"\"\"abc\"", "abc"),
        ("abcd", "abcd"),
        ("abcd\"\"", "abcd"),
    ];

    for (etag, expected) in cases {
        assert_eq!(canonicalize_etag(etag), expected, "etag={etag}");
    }
}

fn base_object_info() -> ObjectInfo {
    ObjectInfo {
        etag: "aa".to_string(),
        mod_time: Utc
            .with_ymd_and_hms(2024, 8, 26, 2, 1, 1)
            .unwrap()
            .timestamp(),
        ..ObjectInfo::default()
    }
}

#[test]
fn test_check_preconditions_line_62() {
    let obj_info = base_object_info();
    let cases = [
        (
            "If-None-Match1",
            "",
            "aa",
            "Sun, 26 Aug 2024 02:01:00 GMT",
            "",
            true,
            304,
        ),
        (
            "If-None-Match2",
            "",
            "aaa",
            "Sun, 26 Aug 2024 02:01:01 GMT",
            "",
            true,
            304,
        ),
        (
            "If-None-Match3",
            "",
            "aaa",
            "Sun, 26 Aug 2024 02:01:02 GMT",
            "",
            true,
            304,
        ),
    ];

    for (
        name,
        if_match,
        if_none_match,
        if_modified_since,
        if_unmodified_since,
        expected_flag,
        expected_code,
    ) in cases
    {
        let headers = BTreeMap::from([
            ("if-match".to_string(), if_match.to_string()),
            ("if-none-match".to_string(), if_none_match.to_string()),
            (
                "if-modified-since".to_string(),
                if_modified_since.to_string(),
            ),
            (
                "if-unmodified-since".to_string(),
                if_unmodified_since.to_string(),
            ),
        ]);
        let actual = check_preconditions(&headers, &obj_info, &ObjectOptions::default());
        assert_eq!(actual, (expected_flag, expected_code), "case={name}");
    }
}

#[test]
fn subtest_test_check_preconditions_tc_name_line_103() {
    let obj_info = base_object_info();
    let headers = BTreeMap::from([
        ("if-match".to_string(), "aa".to_string()),
        (
            "if-unmodified-since".to_string(),
            "Sun, 26 Aug 2024 02:01:00 GMT".to_string(),
        ),
    ]);
    assert_eq!(
        check_preconditions(&headers, &obj_info, &ObjectOptions::default()),
        (false, 200)
    );
}

#[test]
fn subtest_test_check_preconditions_tc_name_line_165() {
    let obj_info = base_object_info();
    for header_value in [
        "Sun, 26 Aug 2024 02:01:01 GMT",
        "Sun, 26 Aug 2024 02:01:02 GMT",
        "",
    ] {
        let mut headers = BTreeMap::from([("if-match".to_string(), "aa".to_string())]);
        if !header_value.is_empty() {
            headers.insert("if-unmodified-since".to_string(), header_value.to_string());
        }
        assert_eq!(
            check_preconditions(&headers, &obj_info, &ObjectOptions::default()),
            (false, 200),
            "if-unmodified-since={header_value}"
        );
    }
}
