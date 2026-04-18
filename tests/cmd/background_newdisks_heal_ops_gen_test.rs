use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::HealingTracker;

pub const SOURCE_FILE: &str = "cmd/background-newdisks-heal-ops_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

impl CmdCodec for HealingTracker {
    fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        HealingTracker::marshal_msg(self)
    }

    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        HealingTracker::unmarshal_msg(self, bytes)
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String> {
        HealingTracker::encode(self, writer)
    }

    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
        HealingTracker::decode(self, reader)
    }

    fn msgsize(&self) -> usize {
        HealingTracker::msgsize(self)
    }
}

fn sample_healing_tracker() -> HealingTracker {
    HealingTracker {
        id: "heal-1".to_string(),
        pool_index: 2,
        set_index: 3,
        disk_index: 4,
        path: "/disk1".to_string(),
        endpoint: "http://localhost:9000".to_string(),
        started: 1_700_000_000_000_000_000,
        last_update: 1_700_000_100_000_000_000,
        objects_total_count: 50,
        objects_total_size: 4096,
        items_healed: 10,
        items_failed: 1,
        bytes_done: 2048,
        bytes_failed: 128,
        bucket: "bucket".to_string(),
        object: "object".to_string(),
        resume_items_healed: 7,
        resume_items_failed: 1,
        resume_items_skipped: 2,
        resume_bytes_done: 1024,
        resume_bytes_failed: 64,
        resume_bytes_skipped: 32,
        queued_buckets: Some(vec!["a".to_string(), "b".to_string()]),
        healed_buckets: Some(vec!["c".to_string()]),
        heal_id: "operation-1".to_string(),
        items_skipped: 3,
        bytes_skipped: 256,
        retry_attempts: 2,
        finished: true,
    }
}

fn assert_roundtrip<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = T::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

fn assert_encode_decode<T: CmdCodec>(value: T) {
    let mut buffer = Cursor::new(Vec::new());
    value.encode(&mut buffer).expect("encode");
    assert!(value.msgsize() >= buffer.get_ref().len());
    buffer.set_position(0);
    let mut decoded = T::default();
    decoded.decode(&mut buffer).expect("decode");
    assert_eq!(decoded, value);
}

fn exercise_benchmark_smoke<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
    for _ in 0..50 {
        let mut decoded = T::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal");
    }
}

#[test]
fn test_marshal_unmarshalhealing_tracker_line_12() {
    assert_roundtrip(sample_healing_tracker());
}

#[test]
fn benchmark_marshal_msghealing_tracker_line_35() {
    let value = sample_healing_tracker();
    for _ in 0..50 {
        let bytes = value.marshal_msg().expect("marshal");
        assert!(!bytes.is_empty());
    }
}

#[test]
fn benchmark_append_msghealing_tracker_line_44() {
    let value = sample_healing_tracker();
    for _ in 0..50 {
        let bytes = value.marshal_msg().expect("marshal");
        assert!(value.msgsize() >= bytes.len());
    }
}

#[test]
fn benchmark_unmarshalhealing_tracker_line_56() {
    exercise_benchmark_smoke(sample_healing_tracker());
}

#[test]
fn test_encode_decodehealing_tracker_line_70() {
    assert_encode_decode(sample_healing_tracker());
}

#[test]
fn benchmark_encodehealing_tracker_line_94() {
    let value = sample_healing_tracker();
    for _ in 0..50 {
        let mut buffer = Vec::new();
        value.encode(&mut buffer).expect("encode");
        assert!(!buffer.is_empty());
    }
}

#[test]
fn benchmark_decodehealing_tracker_line_108() {
    let value = sample_healing_tracker();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..50 {
        let mut decoded = HealingTracker::default();
        decoded
            .decode(&mut Cursor::new(bytes.clone()))
            .expect("decode");
        assert_eq!(decoded, value);
    }
}
