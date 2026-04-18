use std::io::{self, Cursor};

use minio_rust::internal::s3select::json::{Reader, ReaderArgs};

pub const SOURCE_FILE: &str = "internal/s3select/json/reader_test.go";

fn sample_inputs() -> [(&'static str, &'static str, usize); 9] {
    [
        ("2.json", r#"{"text": "hello world\n2nd line"}"#, 1),
        ("3.json", r#"{"hello":"wor{l}d"}"#, 1),
        ("4.json", r#"{
	"id": "0001",
	"type": "donut",
	"name": "Cake"
}"#, 1),
        ("5.json", "{\n\t\"foo\": {\n\t\t\"bar\": \"baz\"\n\t}\n}", 1),
        ("6.json", r#"{ "name": "John", "age":28, "hobby": { "name": "chess", "type": "boardgame" }}"#, 1),
        ("7.json", "{\"name\":\"Michael\", \"age\": 31}\n{\"name\":\"Andy\", \"age\": 30}\n{\"name\":\"Justin\", \"age\": 19}\n", 3),
        ("9.json", "[{\"key_1\":\"value\",\"key_2\":\"value\"}]\n", 1),
        ("11.json", "\"a\"\n1\n3.145\n[\"a\"]\n{}\n{\"a\":1}\n", 6),
        ("12.json", "{\"a\":1}{\"b\":2}", 2),
    ]
}

#[test]
fn new_reader_matches_reference_cases() {
    for (name, input, expected_records) in sample_inputs() {
        let mut reader = Reader::new(Cursor::new(input.as_bytes()), &ReaderArgs)
            .unwrap_or_else(|err| panic!("failed to build reader for {name}: {err}"));
        let mut records = 0usize;
        loop {
            match reader.read() {
                Ok(_) => records += 1,
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => panic!("reading failed for {name}: {err}"),
            }
        }
        reader.close().expect("close should succeed");
        assert_eq!(records, expected_records, "case {name}");
    }
}

#[test]
fn new_reader_close_matches_reference_cases() {
    for (name, input, _) in sample_inputs() {
        let mut reader = Reader::new(Cursor::new(input.as_bytes()), &ReaderArgs)
            .unwrap_or_else(|err| panic!("failed to build reader for {name}: {err}"));
        reader.close().expect("close should succeed");
        let err = reader.read().expect_err("closed reader should eof");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "case {name}");
    }
}

#[test]
fn benchmark_reader_matches_reference_shape() {
    for (name, input, expected_records) in sample_inputs() {
        for _ in 0..10 {
            let mut reader = Reader::new(Cursor::new(input.as_bytes()), &ReaderArgs)
                .unwrap_or_else(|err| panic!("failed to build reader for {name}: {err}"));
            let mut records = 0usize;
            loop {
                match reader.read() {
                    Ok(_) => records += 1,
                    Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(err) => panic!("benchmark read failed for {name}: {err}"),
                }
            }
            assert_eq!(records, expected_records, "case {name}");
        }
    }
}
