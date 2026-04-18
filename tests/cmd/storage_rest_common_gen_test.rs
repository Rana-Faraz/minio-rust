use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::{
    AllTierStats, DataUsageCache, DataUsageCacheInfo, DataUsageEntry, NsScannerOptions,
    NsScannerResp, SizeHistogram, TierStats, VersionsHistogram,
};

pub const SOURCE_FILE: &str = "cmd/storage-rest-common_gen_test.go";

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

impl_cmd_codec!(NsScannerOptions);
impl_cmd_codec!(NsScannerResp);

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

    for _ in 0..100 {
        let mut decoded = T::default();
        let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
        assert!(left.is_empty());
    }
}

fn sample_cache() -> DataUsageCache {
    DataUsageCache {
        info: DataUsageCacheInfo {
            name: "scanner-cache".to_string(),
            next_cycle: 9,
            last_update: 1_716_000_000,
            skip_healing: true,
        },
        cache: Some(BTreeMap::from([(
            "bucket/photos".to_string(),
            DataUsageEntry {
                children: Some(BTreeMap::from([
                    ("bucket/photos/2024".to_string(), true),
                    ("bucket/photos/2025".to_string(), false),
                ])),
                size: 4096,
                objects: 12,
                versions: 14,
                delete_markers: 2,
                obj_sizes: SizeHistogram(Some(vec![1, 4, 7])),
                obj_versions: VersionsHistogram(Some(vec![3, 9])),
                all_tier_stats: Some(AllTierStats {
                    tiers: Some(BTreeMap::from([(
                        "WARM-TIER".to_string(),
                        TierStats {
                            total_size: 4096,
                            num_versions: 14,
                            num_objects: 12,
                        },
                    )])),
                }),
                compacted: false,
            },
        )])),
    }
}

fn sample_ns_scanner_options() -> NsScannerOptions {
    NsScannerOptions {
        disk_id: "disk-1".to_string(),
        scan_mode: 2,
        cache: Some(sample_cache()),
    }
}

fn sample_ns_scanner_resp() -> NsScannerResp {
    NsScannerResp {
        update: sample_cache()
            .cache
            .as_ref()
            .and_then(|cache| cache.get("bucket/photos").cloned()),
        final_cache: Some(sample_cache()),
    }
}

#[test]
fn test_marshal_unmarshalns_scanner_options_line_12() {
    assert_roundtrip(sample_ns_scanner_options());
}

#[test]
fn benchmark_marshal_msgns_scanner_options_line_35() {
    exercise_benchmark_smoke(sample_ns_scanner_options());
}

#[test]
fn benchmark_append_msgns_scanner_options_line_44() {
    assert_encode_decode(sample_ns_scanner_options());
}

#[test]
fn benchmark_unmarshalns_scanner_options_line_56() {
    exercise_benchmark_smoke(sample_ns_scanner_options());
}

#[test]
fn test_encode_decodens_scanner_options_line_70() {
    assert_encode_decode(sample_ns_scanner_options());
}

#[test]
fn benchmark_encodens_scanner_options_line_94() {
    exercise_benchmark_smoke(sample_ns_scanner_options());
}

#[test]
fn benchmark_decodens_scanner_options_line_108() {
    exercise_benchmark_smoke(sample_ns_scanner_options());
}

#[test]
fn test_marshal_unmarshalns_scanner_resp_line_125() {
    assert_roundtrip(sample_ns_scanner_resp());
}

#[test]
fn benchmark_marshal_msgns_scanner_resp_line_148() {
    exercise_benchmark_smoke(sample_ns_scanner_resp());
}

#[test]
fn benchmark_append_msgns_scanner_resp_line_157() {
    assert_encode_decode(sample_ns_scanner_resp());
}

#[test]
fn benchmark_unmarshalns_scanner_resp_line_169() {
    exercise_benchmark_smoke(sample_ns_scanner_resp());
}

#[test]
fn test_encode_decodens_scanner_resp_line_183() {
    assert_encode_decode(sample_ns_scanner_resp());
}

#[test]
fn benchmark_encodens_scanner_resp_line_207() {
    exercise_benchmark_smoke(sample_ns_scanner_resp());
}

#[test]
fn benchmark_decodens_scanner_resp_line_221() {
    exercise_benchmark_smoke(sample_ns_scanner_resp());
}
