use std::io::{self, Cursor};
use std::sync::mpsc;

use serde_json::{Map, Value};

use minio_rust::internal::s3select::json::{Reader as JsonReader, ReaderArgs as JsonReaderArgs};
use minio_rust::internal::s3select::simdj::{
    Reader as SimdJsonReader, ReaderArgs as SimdJsonReaderArgs,
};

pub const SOURCE_FILE: &str = "internal/s3select/simdj/reader_amd64_test.go";

fn fixture() -> &'static str {
    include_str!("../../../fixtures/s3select/parking-citations-10.json")
}

fn csv_for_json_object(value: &Value) -> String {
    let Value::Object(map) = value else {
        panic!("expected object");
    };
    let line = map
        .values()
        .map(|value| match value {
            Value::Null => String::new(),
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::String(value) => value.clone(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap(),
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{line}\n")
}

#[test]
fn test_ndjson_line_64() {
    let mut reader = SimdJsonReader::new(Cursor::new(fixture().as_bytes()), &SimdJsonReaderArgs)
        .expect("simdj reader should build");
    let mut reference = JsonReader::new(Cursor::new(fixture().as_bytes()), &JsonReaderArgs)
        .expect("json reader should build");

    loop {
        match reader.read(None) {
            Ok(record) => {
                let want = reference.read().expect("reference record should exist");

                let mut got_csv = Vec::new();
                record
                    .write_csv(&mut got_csv)
                    .expect("csv write should succeed");
                assert_eq!(
                    String::from_utf8(got_csv).unwrap(),
                    csv_for_json_object(&want.value),
                    "csv output mismatch"
                );

                let mut got_json = Vec::new();
                record
                    .write_json(&mut got_json)
                    .expect("json write should succeed");
                assert_eq!(
                    serde_json::from_slice::<Value>(&got_json).unwrap(),
                    want.value,
                    "json output mismatch"
                );
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => panic!("unexpected simdj error: {err}"),
        }
    }
}

#[test]
fn subtest_test_ndjson_tt_name_line_70() {
    let (tx, rx) = mpsc::channel::<Map<String, Value>>();
    for line in fixture().lines() {
        let value: Value = serde_json::from_str(line).expect("fixture line should parse");
        let Value::Object(object) = value else {
            panic!("expected object");
        };
        tx.send(object).expect("channel send should succeed");
    }
    drop(tx);

    let mut reader = SimdJsonReader::new_element_reader(rx, &SimdJsonReaderArgs);
    let mut count = 0usize;
    loop {
        match reader.read(None) {
            Ok(record) => {
                let mut json = Vec::new();
                record
                    .write_json(&mut json)
                    .expect("json write should succeed");
                let parsed: Value =
                    serde_json::from_slice(&json).expect("written json should parse");
                assert!(parsed.is_object(), "record should stay object-shaped");
                count += 1;
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(err) => panic!("unexpected element reader error: {err}"),
        }
    }
    assert_eq!(count, 10);
}
