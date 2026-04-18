use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::{
    BucketReplicationStat, BucketReplicationStats, BucketStats, BucketStatsMap, ReplQNodeStats,
    ReplicationLastHour, ReplicationLastMinute, ReplicationLatency, ReplicationQueueStats,
};

pub const SOURCE_FILE: &str = "cmd/bucket-stats_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

macro_rules! impl_cmd_codec {
    ($ty:ty) => {
        impl CmdCodec for $ty {
            fn marshal_msg(&self) -> Result<Vec<u8>, String> {
                <$ty>::marshal_msg(self)
            }
            fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
                <$ty>::unmarshal_msg(self, bytes)
            }
            fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String> {
                <$ty>::encode(self, writer)
            }
            fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
                <$ty>::decode(self, reader)
            }
            fn msgsize(&self) -> usize {
                <$ty>::msgsize(self)
            }
        }
    };
}

impl_cmd_codec!(BucketReplicationStat);
impl_cmd_codec!(BucketReplicationStats);
impl_cmd_codec!(BucketStats);
impl_cmd_codec!(BucketStatsMap);
impl_cmd_codec!(ReplQNodeStats);
impl_cmd_codec!(ReplicationLastHour);
impl_cmd_codec!(ReplicationLastMinute);
impl_cmd_codec!(ReplicationLatency);
impl_cmd_codec!(ReplicationQueueStats);

fn assert_roundtrip<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = T::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

fn assert_encode_decode<T: CmdCodec>(value: T) {
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode");
    assert!(value.msgsize() >= buf.get_ref().len());
    buf.set_position(0);
    let mut decoded = T::default();
    decoded.decode(&mut buf).expect("decode");
    assert_eq!(decoded, value);
}

fn exercise_benchmark_smoke<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
    for _ in 0..50 {
        let mut decoded = T::default();
        decoded.unmarshal_msg(&bytes).expect("decode");
    }
}

macro_rules! codec_tests {
    ($module:ident, $ty:ty) => {
        mod $module {
            use super::*;

            #[test]
            fn roundtrip() {
                assert_roundtrip(<$ty>::default());
            }

            #[test]
            fn encode_decode() {
                assert_encode_decode(<$ty>::default());
            }

            #[test]
            fn benchmark_smoke() {
                exercise_benchmark_smoke(<$ty>::default());
            }
        }
    };
}

codec_tests!(bucket_replication_stat, BucketReplicationStat);
codec_tests!(bucket_replication_stats, BucketReplicationStats);
codec_tests!(bucket_stats, BucketStats);
codec_tests!(bucket_stats_map, BucketStatsMap);
codec_tests!(repl_qnode_stats, ReplQNodeStats);
codec_tests!(replication_last_hour, ReplicationLastHour);
codec_tests!(replication_last_minute, ReplicationLastMinute);
codec_tests!(replication_latency, ReplicationLatency);
codec_tests!(replication_queue_stats, ReplicationQueueStats);
