use serde_json::{json, Deserializer, Value as JsonValue};

use minio_rust::internal::s3select::sql::{eval_json_path, JsonPathElement, JsonPathValue};

pub const SOURCE_FILE: &str = "internal/s3select/sql/jsonpath_test.go";

fn books() -> Vec<JsonValue> {
    Deserializer::from_str(include_str!("../../../fixtures/s3select/books.json"))
        .into_iter::<JsonValue>()
        .map(|value| value.expect("fixture JSON should parse"))
        .collect()
}

#[test]
fn jsonpath_eval_matches_reference_cases() {
    let cases = [
        (
            vec![JsonPathElement::key("title")],
            vec![
                JsonPathValue::json(json!("Murder on the Orient Express")),
                JsonPathValue::json(json!("The Robots of Dawn")),
                JsonPathValue::json(json!("Pigs Have Wings")),
            ],
        ),
        (
            vec![
                JsonPathElement::key("authorInfo"),
                JsonPathElement::key("yearRange"),
            ],
            vec![
                JsonPathValue::json(json!([1890, 1976])),
                JsonPathValue::json(json!([1920, 1992])),
                JsonPathValue::json(json!([1881, 1975])),
            ],
        ),
        (
            vec![
                JsonPathElement::key("authorInfo"),
                JsonPathElement::key("name"),
            ],
            vec![
                JsonPathValue::json(json!("Agatha Christie")),
                JsonPathValue::json(json!("Isaac Asimov")),
                JsonPathValue::json(json!("P. G. Wodehouse")),
            ],
        ),
        (
            vec![
                JsonPathElement::key("authorInfo"),
                JsonPathElement::key("yearRange"),
                JsonPathElement::index(0),
            ],
            vec![
                JsonPathValue::json(json!(1890)),
                JsonPathValue::json(json!(1920)),
                JsonPathValue::json(json!(1881)),
            ],
        ),
        (
            vec![
                JsonPathElement::key("publicationHistory"),
                JsonPathElement::index(0),
                JsonPathElement::key("pages"),
            ],
            vec![
                JsonPathValue::json(json!(256)),
                JsonPathValue::json(json!(336)),
                JsonPathValue::Missing,
            ],
        ),
    ];

    let books = books();
    for (case_index, (path, expected)) in cases.into_iter().enumerate() {
        for (book_index, (book, want)) in books.iter().zip(expected.iter()).enumerate() {
            let (got, flat) = eval_json_path(&path, book)
                .unwrap_or_else(|err| panic!("case {case_index} book {book_index} failed: {err}"));
            assert!(
                !flat,
                "case {case_index} book {book_index} should not flatten"
            );
            assert_eq!(got, *want, "case {case_index} book {book_index}");
        }
    }
}

#[test]
fn jsonpath_array_wildcard_flattens_like_reference_behavior() {
    let book = &books()[0];
    let path = vec![
        JsonPathElement::key("publicationHistory"),
        JsonPathElement::array_wildcard(),
        JsonPathElement::key("pages"),
    ];

    let (got, flat) = eval_json_path(&path, book).expect("wildcard evaluation should succeed");
    assert!(flat);
    assert_eq!(
        got,
        JsonPathValue::Sequence(vec![
            JsonPathValue::json(json!(256)),
            JsonPathValue::json(json!(302)),
            JsonPathValue::json(json!(265)),
        ])
    );
}
