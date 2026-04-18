use minio_rust::cmd::{
    check_copy_part_range_with_size, parse_copy_part_range_spec, ERR_INVALID_RANGE_SOURCE,
};

pub const SOURCE_FILE: &str = "cmd/copy-part-range_test.go";

#[test]
fn test_parse_copy_part_range_spec_line_23() {
    let success_cases = [
        ("bytes=2-5", 2_i64, 5_i64),
        ("bytes=2-9", 2_i64, 9_i64),
        ("bytes=2-2", 2_i64, 2_i64),
        ("bytes=0000-0006", 0_i64, 6_i64),
    ];
    let object_size = 10_i64;

    for (range_string, expected_start, expected_end) in success_cases {
        let spec = parse_copy_part_range_spec(range_string).expect("parse");
        let (start, length) = spec.get_offset_length(object_size).expect("offset length");
        assert_eq!(start, expected_start);
        assert_eq!(start + length - 1, expected_end);
    }

    let invalid_range_strings = [
        "bytes=8",
        "bytes=5-2",
        "bytes=+2-5",
        "bytes=2-+5",
        "bytes=2--5",
        "bytes=-",
        "2-5",
        "bytes = 2-5",
        "bytes=2 - 5",
        "bytes=0-0,-1",
        "bytes=2-5 ",
        "bytes=-1",
        "bytes=1-",
    ];
    for range_string in invalid_range_strings {
        assert!(
            parse_copy_part_range_spec(range_string).is_err(),
            "{range_string} should be invalid"
        );
    }

    let error_range_strings = ["bytes=10-10", "bytes=20-30"];
    for range_string in error_range_strings {
        let spec = parse_copy_part_range_spec(range_string).expect("parse");
        let err =
            check_copy_part_range_with_size(&spec, object_size).expect_err("range should fail");
        assert_eq!(err, ERR_INVALID_RANGE_SOURCE);
    }
}
