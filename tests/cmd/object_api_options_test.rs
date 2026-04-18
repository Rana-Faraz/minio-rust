use std::collections::BTreeSet;

use minio_rust::cmd::get_and_validate_attributes_opts;

pub const SOURCE_FILE: &str = "cmd/object-api-options_test.go";

#[test]
fn test_get_and_validate_attributes_opts_line_31() {
    let cases = [
        ("empty header", Vec::<String>::new(), BTreeSet::new()),
        (
            "single header line",
            vec!["test1,test2".to_string()],
            BTreeSet::from(["test1".to_string(), "test2".to_string()]),
        ),
        (
            "multiple header lines with some duplicates",
            vec![
                "test1,test2".to_string(),
                "test3,test4".to_string(),
                "test4,test3".to_string(),
            ],
            BTreeSet::from([
                "test1".to_string(),
                "test2".to_string(),
                "test3".to_string(),
                "test4".to_string(),
            ]),
        ),
    ];

    for (_name, headers, expected) in cases {
        let opts = get_and_validate_attributes_opts(&headers);
        assert_eq!(opts.object_attributes, expected);
    }
}

#[test]
fn subtest_test_get_and_validate_attributes_opts_test_case_name_line_66() {
    let headers = vec!["test1,test2".to_string(), "test2,test3".to_string()];
    let opts = get_and_validate_attributes_opts(&headers);
    assert_eq!(
        opts.object_attributes,
        BTreeSet::from([
            "test1".to_string(),
            "test2".to_string(),
            "test3".to_string()
        ])
    );
}
