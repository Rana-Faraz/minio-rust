use std::collections::BTreeMap;
use std::io::{Cursor, Write};

use minio_rust::cmd::{SiteResyncStatus, TargetReplicationResyncStatus};

pub const SOURCE_FILE: &str = "cmd/site-replication-utils_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + std::fmt::Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

impl CmdCodec for SiteResyncStatus {
    fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        SiteResyncStatus::marshal_msg(self)
    }

    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        SiteResyncStatus::unmarshal_msg(self, bytes)
    }

    fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
        SiteResyncStatus::encode(self, writer)
    }

    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
        SiteResyncStatus::decode(self, reader)
    }

    fn msgsize(&self) -> usize {
        SiteResyncStatus::msgsize(self)
    }
}

fn sample_site_resync_status() -> SiteResyncStatus {
    SiteResyncStatus {
        version: 1,
        status: 2,
        depl_id: "deployment-1".to_string(),
        bucket_statuses: Some(BTreeMap::from([
            ("bucket-a".to_string(), 1),
            ("bucket-b".to_string(), 3),
        ])),
        tot_buckets: 2,
        target_replication_resync_status: TargetReplicationResyncStatus {
            start_time: 1_700_000_000,
            last_update: 1_700_000_123,
            resync_id: "resync-1".to_string(),
            resync_before_date: 1_699_999_999,
            resync_status: 2,
            failed_size: 10,
            failed_count: 1,
            replicated_size: 2048,
            replicated_count: 4,
            bucket: "bucket-a".to_string(),
            object: "path/object.txt".to_string(),
        },
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
fn test_marshal_unmarshal_site_resync_status_line_12() {
    assert_msgpack_roundtrip(sample_site_resync_status());
}

#[test]
fn benchmark_marshal_msg_site_resync_status_line_35() {
    exercise_msgpack_smoke(sample_site_resync_status());
}

#[test]
fn benchmark_append_msg_site_resync_status_line_44() {
    let value = sample_site_resync_status();
    let mut target = Vec::with_capacity(value.msgsize());
    for _ in 0..100 {
        target.clear();
        target.extend(value.marshal_msg().expect("marshal"));
        assert!(!target.is_empty());
    }
}

#[test]
fn benchmark_unmarshal_site_resync_status_line_56() {
    exercise_msgpack_smoke(sample_site_resync_status());
}

#[test]
fn test_encode_decode_site_resync_status_line_70() {
    assert_msgpack_stream_roundtrip(sample_site_resync_status());
}

#[test]
fn benchmark_encode_site_resync_status_line_94() {
    let value = sample_site_resync_status();
    for _ in 0..100 {
        let mut buf = Vec::new();
        value.encode(&mut buf).expect("encode");
        assert!(!buf.is_empty());
    }
}

#[test]
fn benchmark_decode_site_resync_status_line_108() {
    assert_msgpack_stream_roundtrip(sample_site_resync_status());
}
