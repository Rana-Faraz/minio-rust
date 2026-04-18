use minio_rust::cmd::s3_encode_name;

pub const SOURCE_FILE: &str = "cmd/api-utils_test.go";

fn cases() -> [(&'static str, &'static str, &'static str); 11] {
    [
        ("a b", "", "a b"),
        ("a b", "url", "a+b"),
        ("p- ", "url", "p-+"),
        ("p-%", "url", "p-%25"),
        ("p/", "url", "p/"),
        ("p/", "url", "p/"),
        ("~user", "url", "%7Euser"),
        ("*user", "url", "*user"),
        ("user+password", "url", "user%2Bpassword"),
        ("_user", "url", "_user"),
        ("firstname.lastname", "url", "firstname.lastname"),
    ]
}

#[test]
fn test_s3_encode_name_line_25() {
    for (input, encoding_type, expected) in cases() {
        assert_eq!(s3_encode_name(input, encoding_type), expected);
    }
}

#[test]
fn subtest_test_s3_encode_name_fmt_sprintf_test_d_line_42() {
    for (index, (input, encoding_type, expected)) in cases().into_iter().enumerate() {
        assert_eq!(
            s3_encode_name(input, encoding_type),
            expected,
            "case {}",
            index + 1
        );
    }
}
