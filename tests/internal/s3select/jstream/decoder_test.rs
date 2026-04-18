use std::io::{self, Cursor, Read};

use serde_json::Value;

use minio_rust::internal::s3select::jstream::{
    json_number, Decoder, DecoderError, EmittedValue, Error, ValueType,
};

pub const SOURCE_FILE: &str = "internal/s3select/jstream/decoder_test.go";

fn mk_reader(input: &str) -> Cursor<Vec<u8>> {
    Cursor::new(input.as_bytes().to_vec())
}

struct FailingReader {
    reads: usize,
    fail_after: usize,
    fill: u8,
}

impl Read for FailingReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.reads >= self.fail_after {
            return Err(io::Error::other("intentionally unexpected reader error"));
        }
        self.reads += 1;
        if !buf.is_empty() {
            buf[0] = self.fill;
            Ok(1)
        } else {
            Ok(0)
        }
    }
}

#[test]
fn decoder_simple_matches_reference_case() {
    let body = r#"[{"bio":"bada bing bada boom","id":1,"name":"Charles","falseVal":false}]"#;
    let mut decoder = Decoder::new(mk_reader(body), 1);
    let values: Vec<_> = decoder.stream().collect();
    assert!(!values.is_empty());
    assert_eq!(decoder.err(), None);
}

#[test]
fn decoder_nested_matches_reference_case() {
    let body = r#"{
  "1": {
    "bio": "bada bing bada boom",
    "id": 0,
    "name": "Roberto",
    "nested1": {
      "bio": "utf16 surrogate (\ud834\udcb2)\n\u201cutf 8\u201d",
      "id": 1.5,
      "name": "Roberto*Maestro",
      "nested2": { "nested2arr": [0,1,2], "nested3": {
        "nested4": { "depth": "recursion" }}
      }
    }
  },
  "2": {
    "nullfield": null,
    "id": -2
  }
}"#;
    let mut decoder = Decoder::new(mk_reader(body), 2);
    let values: Vec<_> = decoder.stream().collect();
    assert!(values.len() >= 2);
    assert_eq!(decoder.err(), None);
}

#[test]
fn decoder_flat_matches_reference_case() {
    let body = r#"[
  "1st test string",
  "Roberto*Maestro", "Charles",
  0, null, false,
  1, 2.5
]"#;
    let mut decoder = Decoder::new(mk_reader(body), 1);
    let values: Vec<_> = decoder.stream().collect();
    let expected = [
        (
            Value::String("1st test string".to_owned()),
            ValueType::String,
        ),
        (
            Value::String("Roberto*Maestro".to_owned()),
            ValueType::String,
        ),
        (Value::String("Charles".to_owned()), ValueType::String),
        (json_number(0.0), ValueType::Number),
        (Value::Null, ValueType::Null),
        (Value::Bool(false), ValueType::Boolean),
        (json_number(1.0), ValueType::Number),
        (json_number(2.5), ValueType::Number),
    ];
    assert_eq!(values.len(), expected.len());
    for (got, (expected_value, expected_type)) in values.iter().zip(expected) {
        match &got.value {
            EmittedValue::Json(value) => {
                if expected_type == ValueType::Number {
                    assert_eq!(
                        value.as_f64(),
                        expected_value.as_f64(),
                        "numeric value mismatch"
                    );
                } else {
                    assert_eq!(value, &expected_value);
                }
            }
            other => panic!("expected json value, got {other:?}"),
        }
        assert_eq!(got.value_type, expected_type);
    }
    assert_eq!(decoder.err(), None);
}

#[test]
fn decoder_multi_doc_matches_reference_cases() {
    let body = r#"{ "bio": "bada bing bada boom", "id": 1, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 2, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 3, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 4, "name": "Charles" }
{ "bio": "bada bing bada boom", "id": 5, "name": "Charles" }
"#;

    let mut decoder = Decoder::new(mk_reader(body), 0);
    let values: Vec<_> = decoder.stream().collect();
    assert_eq!(values.len(), 5);
    assert!(values.iter().all(|mv| mv.value_type == ValueType::Object));
    assert_eq!(decoder.err(), None);

    let mut decoder = Decoder::new(mk_reader(body), 1);
    let values: Vec<_> = decoder.stream().collect();
    assert_eq!(values.len(), 15);
    assert!(values
        .iter()
        .all(|mv| !matches!(mv.value, EmittedValue::KV(_))));
    assert_eq!(decoder.err(), None);

    let mut decoder = Decoder::new(mk_reader(body), 1).emit_kv();
    let values: Vec<_> = decoder.stream().collect();
    assert_eq!(values.len(), 15);
    assert!(values
        .iter()
        .all(|mv| matches!(mv.value, EmittedValue::KV(_))));
    assert_eq!(decoder.err(), None);
}

#[test]
fn decoder_reader_failure_matches_reference_case() {
    let reader = FailingReader {
        reads: 0,
        fail_after: 900,
        fill: b'[',
    };
    let mut decoder = Decoder::new(reader, -1);
    let values: Vec<_> = decoder.stream().collect();
    assert!(values.is_empty());
    let err = decoder.err().expect("expected decoder error");
    assert!(matches!(err, DecoderError { .. }));
    assert!(err.reader_err().is_some());
}

#[test]
fn decoder_max_depth_matches_reference_cases() {
    let cases = [
        (
            r#"[{"bio":"bada bing bada boom","id":1,"name":"Charles","falseVal":false}]"#,
            0,
            false,
        ),
        (
            r#"[{"bio":"bada bing bada boom","id":1,"name":"Charles","falseVal":false}]"#,
            1,
            true,
        ),
        (
            r#"[{"bio":"bada bing bada boom","id":1,"name":"Charles","falseVal":false}]"#,
            2,
            false,
        ),
        (
            r#"[[[[[[[[[[[[[[[[[[[[[["ok"]]]]]]]]]]]]]]]]]]]]]]"#,
            2,
            true,
        ),
        (
            r#"[[[[[[[[[[[[[[[[[[[[[["ok"]]]]]]]]]]]]]]]]]]]]]]"#,
            100,
            false,
        ),
        (
            r#"{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"ok":false}}}}}}}}}}}}}}}}}}}}}}"#,
            2,
            true,
        ),
        (
            r#"{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"ok":false}}}}}}}}}}}}}}}}}}}}}}"#,
            100,
            false,
        ),
    ];

    for (input, max_depth, must_fail) in cases {
        let mut decoder = Decoder::new(mk_reader(input), 0).max_depth(max_depth);
        let _: Vec<_> = decoder.stream().collect();
        let err = decoder.err();
        if must_fail {
            assert_eq!(err.map(|e| e.error), Some(Error::MaxDepth));
        } else {
            assert!(err.is_none());
        }
    }
}
