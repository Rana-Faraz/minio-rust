use minio_rust::cmd::extract_prefix_and_suffix;

pub const SOURCE_FILE: &str = "cmd/iam-etcd-store_test.go";

#[test]
fn test_extract_prefix_and_suffix_line_24() {
    assert_eq!(
        extract_prefix_and_suffix("/config/iam/users/tester.json", "/config/iam/users"),
        ("config/iam/users/".to_string(), "tester.json".to_string())
    );
    assert_eq!(
        extract_prefix_and_suffix("config/iam/users/", "config/iam/users"),
        ("config/iam/users/".to_string(), String::new())
    );
    assert_eq!(
        extract_prefix_and_suffix("/other/place/value.json", "/config/iam/users"),
        (String::new(), "other/place/value.json".to_string())
    );
}
