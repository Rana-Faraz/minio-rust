use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Write};

use serde::de::DeserializeOwned;
use serde::Serialize;

use minio_rust::cmd::{DiskInfo, FileInfo, ObjectPartInfo, VolInfo};

pub const SOURCE_FILE: &str = "cmd/storage-datatypes_test.go";

trait CmdCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl Write) -> Result<(), String>;
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

            fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
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

impl_cmd_codec!(VolInfo);
impl_cmd_codec!(DiskInfo);
impl_cmd_codec!(FileInfo);

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

fn assert_bincode_roundtrip<T>(value: T)
where
    T: Serialize + DeserializeOwned + Clone + PartialEq + Debug,
{
    let bytes = bincode::serialize(&value).expect("serialize");
    let decoded: T = bincode::deserialize(&bytes).expect("deserialize");
    assert_eq!(decoded, value);
}

fn exercise_msgpack_smoke<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
    for _ in 0..100 {
        let mut decoded = T::default();
        let left = decoded.unmarshal_msg(&bytes).expect("decode");
        assert!(left.is_empty());
    }
}

fn exercise_bincode_smoke<T>(value: T)
where
    T: Serialize + DeserializeOwned + Clone + PartialEq + Debug,
{
    let bytes = bincode::serialize(&value).expect("serialize");
    for _ in 0..100 {
        let decoded: T = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(decoded, value);
    }
}

fn sample_vol_info() -> VolInfo {
    VolInfo {
        name: "uuid".to_string(),
        created: 1_700_000_000,
        count: 1,
        deleted: 0,
    }
}

fn sample_disk_info() -> DiskInfo {
    DiskInfo {
        total: 1000,
        free: 1000,
        used: 1000,
        fs_type: "xfs".to_string(),
        root_disk: true,
        healing: true,
        endpoint: "http://localhost:9001/tmp/drive1".to_string(),
        mount_path: "/tmp/drive1".to_string(),
        id: "uuid".to_string(),
        error: String::new(),
        ..DiskInfo::default()
    }
}

fn sample_file_info() -> FileInfo {
    FileInfo {
        volume: "testbucket".to_string(),
        name: "src/compress/zlib/reader_test.go".to_string(),
        version_id: String::new(),
        is_latest: true,
        deleted: false,
        transition_status: String::new(),
        transitioned_obj_name: String::new(),
        transition_tier: String::new(),
        transition_version_id: String::new(),
        expire_restored: false,
        data_dir: "5e0153cc-621a-4267-8cb6-4919140d53b3".to_string(),
        xlv1: false,
        mod_time: 1_700_000_000,
        size: 3430,
        mode: 0,
        written_by_version: 0,
        metadata: Some(BTreeMap::from([
            (
                "X-Minio-Internal-Server-Side-Encryption-Iv".to_string(),
                "jIJPsrkkVYYMvc7edBrNl+7zcM7+ZwXqMb/YAjBO/ck=".to_string(),
            ),
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            (
                "etag".to_string(),
                "20000f00e2c3709dc94905c6ce31e1cadbd1c064e14acdcd44cf0ac2db777eeedd88d639fcd64de16851ade8b21a9a1a".to_string(),
            ),
        ])),
        parts: Some(vec![ObjectPartInfo {
            number: 1,
            size: 3430,
            actual_size: 3398,
            etag: String::new(),
        }]),
        erasure: minio_rust::cmd::ErasureInfo {
            algorithm: "reedsolomon".to_string(),
            data_blocks: 2,
            parity_blocks: 2,
            block_size: 10_485_760,
            index: 1,
            distribution: Some(vec![3, 4, 1, 2]),
        },
        mark_deleted: false,
        replication_state: Default::default(),
        data: None,
        num_versions: 0,
        successor_mod_time: 0,
        fresh: false,
        idx: 0,
        checksum: None,
        versioned: false,
    }
}

#[test]
fn benchmark_decode_vol_info_msgp_line_30() {
    assert_msgpack_roundtrip(sample_vol_info());
    exercise_msgpack_smoke(sample_vol_info());
}

#[test]
fn benchmark_decode_disk_info_msgp_line_51() {
    assert_msgpack_roundtrip(sample_disk_info());
    exercise_msgpack_smoke(sample_disk_info());
}

#[test]
fn benchmark_decode_disk_info_gob_line_80() {
    assert_bincode_roundtrip(sample_disk_info());
    exercise_bincode_smoke(sample_disk_info());
}

#[test]
fn benchmark_encode_disk_info_msgp_line_110() {
    assert_msgpack_stream_roundtrip(sample_disk_info());
}

#[test]
fn benchmark_encode_disk_info_gob_line_135() {
    assert_bincode_roundtrip(sample_disk_info());
}

#[test]
fn benchmark_decode_file_info_msgp_line_161() {
    assert_msgpack_roundtrip(sample_file_info());
    exercise_msgpack_smoke(sample_file_info());
}

#[test]
fn benchmark_decode_file_info_gob_line_179() {
    assert_bincode_roundtrip(sample_file_info());
    exercise_bincode_smoke(sample_file_info());
}

#[test]
fn benchmark_encode_file_info_msgp_line_197() {
    assert_msgpack_stream_roundtrip(sample_file_info());
}

#[test]
fn benchmark_encode_file_info_gob_line_210() {
    assert_bincode_roundtrip(sample_file_info());
}
