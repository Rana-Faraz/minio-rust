use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::ServerSystemConfig;

pub const SOURCE_FILE: &str = "cmd/bootstrap-peer-server_gen_test.go";

trait CmdCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

impl CmdCodec for ServerSystemConfig {
    fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        ServerSystemConfig::marshal_msg(self)
    }

    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        ServerSystemConfig::unmarshal_msg(self, bytes)
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String> {
        ServerSystemConfig::encode(self, writer)
    }

    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
        ServerSystemConfig::decode(self, reader)
    }

    fn msgsize(&self) -> usize {
        ServerSystemConfig::msgsize(self)
    }
}

fn sample_server_system_config() -> ServerSystemConfig {
    ServerSystemConfig {
        n_endpoints: 4,
        cmd_lines: Some(vec![
            "minio".to_string(),
            "server".to_string(),
            "http://node{1...4}/data".to_string(),
        ]),
        minio_env: Some(
            [
                ("MINIO_ROOT_USER".to_string(), "minioadmin".to_string()),
                ("MINIO_ROOT_PASSWORD".to_string(), "minioadmin".to_string()),
            ]
            .into_iter()
            .collect(),
        ),
        checksum: "abc123".to_string(),
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

fn benchmark_smoke<T: CmdCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
    for _ in 0..50 {
        let mut decoded = T::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal");
    }
}

#[test]
fn test_marshal_unmarshal_server_system_config_line_12() {
    assert_roundtrip(sample_server_system_config());
}

#[test]
fn benchmark_marshal_msg_server_system_config_line_35() {
    let value = sample_server_system_config();
    for _ in 0..50 {
        let bytes = value.marshal_msg().expect("marshal");
        assert!(!bytes.is_empty());
    }
}

#[test]
fn benchmark_append_msg_server_system_config_line_44() {
    let value = sample_server_system_config();
    for _ in 0..50 {
        let bytes = value.marshal_msg().expect("marshal");
        assert!(value.msgsize() >= bytes.len());
    }
}

#[test]
fn benchmark_unmarshal_server_system_config_line_56() {
    benchmark_smoke(sample_server_system_config());
}

#[test]
fn test_encode_decode_server_system_config_line_70() {
    assert_encode_decode(sample_server_system_config());
}

#[test]
fn benchmark_encode_server_system_config_line_94() {
    let value = sample_server_system_config();
    for _ in 0..50 {
        let mut buffer = Vec::new();
        value.encode(&mut buffer).expect("encode");
        assert!(!buffer.is_empty());
    }
}

#[test]
fn benchmark_decode_server_system_config_line_108() {
    let value = sample_server_system_config();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..50 {
        let mut decoded = ServerSystemConfig::default();
        decoded
            .decode(&mut Cursor::new(bytes.clone()))
            .expect("decode");
        assert_eq!(decoded, value);
    }
}
