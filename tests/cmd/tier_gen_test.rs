use std::collections::BTreeMap;
use std::io::{Cursor, Write};

use minio_rust::cmd::{TierConfig, TierConfigMgr};

pub const SOURCE_FILE: &str = "cmd/tier_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + std::fmt::Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

impl CmdCodec for TierConfigMgr {
    fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        TierConfigMgr::marshal_msg(self)
    }

    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        TierConfigMgr::unmarshal_msg(self, bytes)
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
        TierConfigMgr::encode(self, writer)
    }

    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
        TierConfigMgr::decode(self, reader)
    }

    fn msgsize(&self) -> usize {
        TierConfigMgr::msgsize(self)
    }
}

fn sample_tier_config_mgr() -> TierConfigMgr {
    TierConfigMgr {
        tiers: BTreeMap::from([(
            "WARM-S3".to_string(),
            TierConfig {
                name: "WARM-S3".to_string(),
                tier_type: "s3".to_string(),
                endpoint: "https://warm.example.test".to_string(),
                bucket: "archive-bucket".to_string(),
                prefix: "objects/".to_string(),
            },
        )]),
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
fn test_marshal_unmarshal_tier_config_mgr_line_12() {
    assert_msgpack_roundtrip(sample_tier_config_mgr());
}

#[test]
fn benchmark_marshal_msg_tier_config_mgr_line_35() {
    exercise_msgpack_smoke(sample_tier_config_mgr());
}

#[test]
fn benchmark_append_msg_tier_config_mgr_line_44() {
    let value = sample_tier_config_mgr();
    let mut target = Vec::with_capacity(value.msgsize());
    for _ in 0..100 {
        target.clear();
        target.extend(value.marshal_msg().expect("marshal"));
        assert!(!target.is_empty());
    }
}

#[test]
fn benchmark_unmarshal_tier_config_mgr_line_56() {
    exercise_msgpack_smoke(sample_tier_config_mgr());
}

#[test]
fn test_encode_decode_tier_config_mgr_line_70() {
    assert_msgpack_stream_roundtrip(sample_tier_config_mgr());
}

#[test]
fn benchmark_encode_tier_config_mgr_line_94() {
    let value = sample_tier_config_mgr();
    for _ in 0..100 {
        let mut buf = Vec::new();
        value.encode(&mut buf).expect("encode");
        assert!(!buf.is_empty());
    }
}

#[test]
fn benchmark_decode_tier_config_mgr_line_108() {
    assert_msgpack_stream_roundtrip(sample_tier_config_mgr());
}
