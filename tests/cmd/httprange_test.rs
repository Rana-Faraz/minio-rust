// Rust test snapshot derived from cmd/httprange_test.go.

use minio_rust::cmd::{is_err_invalid_range, parse_request_range_spec};

pub const SOURCE_FILE: &str = "cmd/httprange_test.go";

#[test]
fn test_httprequest_range_spec_line_24() {
    let resource_size = 10i64;
    let valid = [
        ("bytes=0-", 0, 10),
        ("bytes=1-", 1, 9),
        ("bytes=0-9", 0, 10),
        ("bytes=1-10", 1, 9),
        ("bytes=1-1", 1, 1),
        ("bytes=2-5", 2, 4),
        ("bytes=-5", 5, 5),
        ("bytes=-1", 9, 1),
        ("bytes=-1000", 0, 10),
    ];

    for (spec, expected_offset, expected_length) in valid {
        let range = parse_request_range_spec(spec).expect("parse valid range");
        let (offset, length) = range
            .get_offset_length(resource_size)
            .expect("get valid offset length");
        assert_eq!(
            (offset, length),
            (expected_offset, expected_length),
            "spec={spec}"
        );
    }

    let unparsable = [
        "bytes=-",
        "bytes==",
        "bytes==1-10",
        "bytes=",
        "bytes=aa",
        "aa",
        "",
        "bytes=1-10-",
        "bytes=1--10",
        "bytes=-1-10",
        "bytes=0-+3",
        "bytes=+3-+5",
        "bytes=10-11,12-10",
    ];
    for spec in unparsable {
        let err = parse_request_range_spec(spec).expect_err("expected parse error");
        assert!(!is_err_invalid_range(&err), "spec={spec}");
    }

    let invalid = [
        "bytes=5-3",
        "bytes=10-10",
        "bytes=10-",
        "bytes=100-",
        "bytes=-0",
    ];
    for spec in invalid {
        match parse_request_range_spec(spec) {
            Ok(range) => {
                let err = range
                    .get_offset_length(resource_size)
                    .expect_err("expected invalid range");
                assert!(is_err_invalid_range(&err), "spec={spec}");
            }
            Err(err) => assert!(is_err_invalid_range(&err), "spec={spec} err={err}"),
        }
    }
}

#[test]
fn test_httprequest_range_to_header_line_105() {
    let cases = [
        ("bytes=0-", false),
        ("bytes=1-", false),
        ("bytes=0-9", false),
        ("bytes=1-10", false),
        ("bytes=1-1", false),
        ("bytes=2-5", false),
        ("bytes=-5", false),
        ("bytes=-1", false),
        ("bytes=-1000", false),
        ("bytes=", true),
        ("bytes= ", true),
        ("byte=", true),
        ("bytes=A-B", true),
        ("bytes=1-B", true),
        ("bytes=B-1", true),
        ("bytes=-1-1", true),
    ];

    for (spec, err_expected) in cases {
        match parse_request_range_spec(spec) {
            Ok(range) => {
                let result = range.to_header();
                assert_eq!(result.is_err(), err_expected, "spec={spec}");
                if let Ok(header) = result {
                    assert_eq!(header, spec, "spec={spec}");
                }
            }
            Err(_) => assert!(err_expected, "spec={spec}"),
        }
    }
}
