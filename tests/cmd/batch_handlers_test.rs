use minio_rust::cmd::BatchJobPrefix;

pub const SOURCE_FILE: &str = "cmd/batch-handlers_test.go";

#[test]
fn test_batch_job_prefix_unmarshal_yaml_line_27() {
    let one: BatchJobPrefix = serde_yaml::from_str("hello\n").expect("single prefix yaml");
    assert_eq!(one.f(), vec!["hello".to_string()]);

    let many: BatchJobPrefix = serde_yaml::from_str("- one\n- two\n").expect("list prefix yaml");
    assert_eq!(many.f(), vec!["one".to_string(), "two".to_string()]);

    let missing: BatchJobPrefix = serde_yaml::from_str("null\n").expect("null prefix yaml");
    assert_eq!(missing.f(), Vec::<String>::new());
}

#[test]
fn subtest_test_batch_job_prefix_unmarshal_yaml_tt_name_line_66() {
    let doc = "prefix:\n  - photos/\n  - videos/\n";
    #[derive(serde::Deserialize)]
    struct Wrapper {
        prefix: BatchJobPrefix,
    }
    let wrapper: Wrapper = serde_yaml::from_str(doc).expect("wrapped prefix yaml");
    assert_eq!(
        wrapper.prefix.f(),
        vec!["photos/".to_string(), "videos/".to_string()]
    );
}
