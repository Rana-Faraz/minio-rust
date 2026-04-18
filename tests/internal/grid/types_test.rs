use std::collections::BTreeMap;

use minio_rust::internal::grid::{Bytes, MSS};

pub const SOURCE_FILE: &str = "internal/grid/types_test.go";

#[test]
fn test_marshal_unmarshal_mss_line_27() {
    let mut values = BTreeMap::new();
    values.insert("abc".to_owned(), "def".to_owned());
    values.insert("ghi".to_owned(), "jkl".to_owned());
    let value = MSS::with_map(values);
    let bytes = value.marshal_msg().expect("marshal MSS");
    let mut decoded = MSS::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal MSS");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn test_marshal_unmarshal_mssnil_line_54() {
    let value = MSS::default();
    let bytes = value.marshal_msg().expect("marshal nil MSS");
    let mut decoded = MSS::new();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal nil MSS");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_marshal_msg_mss_line_81() {
    let mut values = BTreeMap::new();
    values.insert("abc".to_owned(), "def".to_owned());
    values.insert("ghi".to_owned(), "jkl".to_owned());
    let value = MSS::with_map(values);
    for _ in 0..100 {
        let bytes = value.marshal_msg().expect("marshal MSS");
        assert!(!bytes.is_empty());
    }
}

#[test]
fn benchmark_append_msg_mss_line_90() {
    let mut values = BTreeMap::new();
    values.insert("abc".to_owned(), "def".to_owned());
    values.insert("ghi".to_owned(), "jkl".to_owned());
    let value = MSS::with_map(values);
    let bytes = value.marshal_msg().expect("marshal MSS");
    assert!(value.msgsize() >= bytes.len());
}

#[test]
fn benchmark_unmarshal_mss_line_102() {
    let mut values = BTreeMap::new();
    values.insert("abc".to_owned(), "def".to_owned());
    values.insert("ghi".to_owned(), "jkl".to_owned());
    let value = MSS::with_map(values);
    let bytes = value.marshal_msg().expect("marshal MSS");
    for _ in 0..100 {
        let mut decoded = MSS::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal MSS");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_marshal_unmarshal_bytes_line_116() {
    let value = Bytes::with_bytes(b"abc123123123".to_vec());
    let bytes = value.marshal_msg().expect("marshal bytes");
    let mut decoded = Bytes::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal bytes");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn test_marshal_unmarshal_bytes_nil_line_143() {
    let value = Bytes::default();
    let bytes = value.marshal_msg().expect("marshal nil bytes");
    let mut decoded = Bytes::with_bytes(vec![1]);
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal nil bytes");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}
