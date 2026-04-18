use std::io::{Cursor, Write};

use minio_rust::cmd::PartialOperation;

pub const SOURCE_FILE: &str = "cmd/mrf_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + std::fmt::Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

impl CmdCodec for PartialOperation {
    fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        PartialOperation::marshal_msg(self)
    }

    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        PartialOperation::unmarshal_msg(self, bytes)
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
        PartialOperation::encode(self, writer)
    }

    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
        PartialOperation::decode(self, reader)
    }

    fn msgsize(&self) -> usize {
        PartialOperation::msgsize(self)
    }
}

fn sample_partial_operation() -> PartialOperation {
    PartialOperation {
        bucket: "bucket-a".to_string(),
        object: "photos/image.png".to_string(),
        version_id: "v1".to_string(),
        versions: vec![1, 2, 3, 4],
        set_index: 2,
        pool_index: 1,
        queued: 1_700_000_000,
        bitrot_scan: true,
    }
}

fn assert_msgpack_roundtrip<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = T::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

fn assert_msgpack_stream_roundtrip<T: CmdCodec>(value: T) {
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode");
    assert!(value.msgsize() >= buf.get_ref().len());
    buf.set_position(0);
    let mut decoded = T::default();
    decoded.decode(&mut buf).expect("decode");
    assert_eq!(decoded, value);
}

fn exercise_msgpack_smoke<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
    for _ in 0..100 {
        let mut decoded = T::default();
        let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
        assert!(left.is_empty());
    }
}

#[test]
fn test_marshal_unmarshal_partial_operation_line_12() {
    assert_msgpack_roundtrip(sample_partial_operation());
}

#[test]
fn benchmark_marshal_msg_partial_operation_line_35() {
    exercise_msgpack_smoke(sample_partial_operation());
}

#[test]
fn benchmark_append_msg_partial_operation_line_44() {
    let value = sample_partial_operation();
    let mut target = Vec::with_capacity(value.msgsize());
    for _ in 0..100 {
        target.clear();
        target.extend(value.marshal_msg().expect("marshal"));
        assert!(!target.is_empty());
    }
}

#[test]
fn benchmark_unmarshal_partial_operation_line_56() {
    exercise_msgpack_smoke(sample_partial_operation());
}

#[test]
fn test_encode_decode_partial_operation_line_70() {
    assert_msgpack_stream_roundtrip(sample_partial_operation());
}

#[test]
fn benchmark_encode_partial_operation_line_94() {
    let value = sample_partial_operation();
    for _ in 0..100 {
        let mut buf = Vec::new();
        value.encode(&mut buf).expect("encode");
        assert!(!buf.is_empty());
    }
}

#[test]
fn benchmark_decode_partial_operation_line_108() {
    assert_msgpack_stream_roundtrip(sample_partial_operation());
}
