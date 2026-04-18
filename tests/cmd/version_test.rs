use minio_rust::cmd::parse_version_time;

pub const SOURCE_FILE: &str = "cmd/version_test.go";

#[test]
fn test_version_line_25() {
    let version = "2017-05-07T06:37:49Z";
    let parsed = parse_version_time(version).expect("valid RFC3339 version");
    assert_eq!(parsed.timestamp(), 1494139069);
}
