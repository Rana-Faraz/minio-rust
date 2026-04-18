use std::collections::BTreeSet;

use minio_rust::internal::config::{is_valid_region, kv_fields};

#[test]
fn kv_fields_matches_reference_cases() {
    let cases = [
        ("", Vec::<&str>::new(), Vec::<&str>::new()),
        (
            r#"comment="Hi this is my comment =""#,
            Vec::<&str>::new(),
            Vec::<&str>::new(),
        ),
        (
            r#"comment="Hi this is my comment =""#,
            vec!["comment"],
            vec![r#"comment="Hi this is my comment =""#],
        ),
        (
            r#"connection_string="host=localhost port=2832" comment="really long comment""#,
            vec!["connection_string", "comment"],
            vec![
                r#"connection_string="host=localhost port=2832""#,
                r#"comment="really long comment""#,
            ],
        ),
        (
            r#"enable=on format=namespace connection_string=" host=localhost port=5432 dbname = cesnietor sslmode=disable" table=holicrayoli"#,
            vec!["enable", "connection_string", "comment", "format", "table"],
            vec![
                "enable=on",
                "format=namespace",
                r#"connection_string=" host=localhost port=5432 dbname = cesnietor sslmode=disable""#,
                "table=holicrayoli",
            ],
        ),
        (
            r#"comment="really long comment" connection_string="host=localhost port=2832""#,
            vec!["connection_string", "comment", "format"],
            vec![
                r#"comment="really long comment""#,
                r#"connection_string="host=localhost port=2832""#,
            ],
        ),
        (
            r#"comment:"really long comment" connection_string:"host=localhost port=2832""#,
            vec!["connection_string", "comment"],
            Vec::<&str>::new(),
        ),
        (
            r#"comme="really long comment" connection_str="host=localhost port=2832""#,
            vec!["connection_string", "comment"],
            Vec::<&str>::new(),
        ),
    ];

    for (input, keys, expected) in cases {
        let actual = kv_fields(input, &keys);
        let actual_set = actual.into_iter().collect::<BTreeSet<_>>();
        let expected_set = expected
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_set, expected_set, "input {input}");
    }
}

#[test]
fn valid_region_matches_reference_cases() {
    let cases = [
        ("us-east-1", true),
        ("us_east", true),
        ("helloWorld", true),
        ("-fdslka", false),
        ("^00[", false),
        ("my region", false),
        ("%%$#!", false),
    ];

    for (input, expected) in cases {
        assert_eq!(is_valid_region(input), expected, "input {input}");
    }
}
