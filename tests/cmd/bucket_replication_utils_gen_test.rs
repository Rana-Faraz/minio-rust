use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::{
    BucketReplicationResyncStatus, MRFReplicateEntries, MRFReplicateEntry, ReplicateDecision,
    ReplicationState, ResyncDecision, ResyncTarget, ResyncTargetDecision, ResyncTargetsInfo,
    TargetReplicationResyncStatus,
};

pub const SOURCE_FILE: &str = "cmd/bucket-replication-utils_gen_test.go";

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

impl_cmd_codec!(BucketReplicationResyncStatus);
impl_cmd_codec!(MRFReplicateEntries);
impl_cmd_codec!(MRFReplicateEntry);
impl_cmd_codec!(ReplicateDecision);
impl_cmd_codec!(ReplicationState);
impl_cmd_codec!(ResyncDecision);
impl_cmd_codec!(ResyncTarget);
impl_cmd_codec!(ResyncTargetDecision);
impl_cmd_codec!(ResyncTargetsInfo);
impl_cmd_codec!(TargetReplicationResyncStatus);

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

codec_tests!(
    bucket_replication_resync_status,
    BucketReplicationResyncStatus
);
codec_tests!(mrf_replicate_entries, MRFReplicateEntries);
codec_tests!(mrf_replicate_entry, MRFReplicateEntry);
codec_tests!(replicate_decision, ReplicateDecision);
codec_tests!(replication_state, ReplicationState);
codec_tests!(resync_decision, ResyncDecision);
codec_tests!(resync_target, ResyncTarget);
codec_tests!(resync_target_decision, ResyncTargetDecision);
codec_tests!(resync_targets_info, ResyncTargetsInfo);
codec_tests!(
    target_replication_resync_status,
    TargetReplicationResyncStatus
);
