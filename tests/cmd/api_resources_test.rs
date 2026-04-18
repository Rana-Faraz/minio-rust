use std::collections::BTreeMap;

use minio_rust::cmd::{
    get_list_objects_v1_args, get_list_objects_v2_args, get_object_resources, ApiErrorCode,
    QueryValues, MAX_OBJECT_LIST,
};

pub const SOURCE_FILE: &str = "cmd/api-resources_test.go";

fn values(items: &[(&str, &[&str])]) -> QueryValues {
    items
        .iter()
        .map(|(key, vals)| {
            (
                (*key).to_string(),
                vals.iter()
                    .map(|value| (*value).to_string())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>()
}

#[test]
fn test_list_objects_v2_resources_line_26() {
    let cases = [
        (
            values(&[
                ("prefix", &["photos/"]),
                ("continuation-token", &["dG9rZW4="]),
                ("start-after", &["start-after"]),
                ("delimiter", &["/"]),
                ("fetch-owner", &["true"]),
                ("max-keys", &["100"]),
                ("encoding-type", &["gzip"]),
            ]),
            (
                "photos/".to_string(),
                "token".to_string(),
                "start-after".to_string(),
                "/".to_string(),
                true,
                100,
                "gzip".to_string(),
                ApiErrorCode::None,
            ),
        ),
        (
            values(&[
                ("prefix", &["photos/"]),
                ("continuation-token", &["dG9rZW4="]),
                ("start-after", &["start-after"]),
                ("delimiter", &["/"]),
                ("fetch-owner", &["true"]),
                ("encoding-type", &["gzip"]),
            ]),
            (
                "photos/".to_string(),
                "token".to_string(),
                "start-after".to_string(),
                "/".to_string(),
                true,
                MAX_OBJECT_LIST,
                "gzip".to_string(),
                ApiErrorCode::None,
            ),
        ),
        (
            values(&[
                ("prefix", &["photos/"]),
                ("continuation-token", &[""]),
                ("start-after", &["start-after"]),
                ("delimiter", &["/"]),
                ("fetch-owner", &["true"]),
                ("encoding-type", &["gzip"]),
            ]),
            (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                false,
                0,
                String::new(),
                ApiErrorCode::IncorrectContinuationToken,
            ),
        ),
    ];

    for (query, expected) in cases {
        assert_eq!(get_list_objects_v2_args(&query), expected);
    }
}

#[test]
fn test_list_objects_v1_resources_line_123() {
    let cases = [
        (
            values(&[
                ("prefix", &["photos/"]),
                ("marker", &["test"]),
                ("delimiter", &["/"]),
                ("max-keys", &["100"]),
                ("encoding-type", &["gzip"]),
            ]),
            (
                "photos/".to_string(),
                "test".to_string(),
                "/".to_string(),
                100,
                "gzip".to_string(),
                ApiErrorCode::None,
            ),
        ),
        (
            values(&[
                ("prefix", &["photos/"]),
                ("marker", &["test"]),
                ("delimiter", &["/"]),
                ("encoding-type", &["gzip"]),
            ]),
            (
                "photos/".to_string(),
                "test".to_string(),
                "/".to_string(),
                MAX_OBJECT_LIST,
                "gzip".to_string(),
                ApiErrorCode::None,
            ),
        ),
    ];

    for (query, expected) in cases {
        assert_eq!(get_list_objects_v1_args(&query), expected);
    }
}

#[test]
fn test_get_objects_resources_line_183() {
    let query = values(&[
        ("uploadId", &["11123-11312312311231-12313"]),
        ("part-number-marker", &["1"]),
        ("max-parts", &["1000"]),
        ("encoding-type", &["gzip"]),
    ]);

    assert_eq!(
        get_object_resources(&query),
        (
            "11123-11312312311231-12313".to_string(),
            1,
            1000,
            "gzip".to_string(),
            ApiErrorCode::None,
        )
    );
}
