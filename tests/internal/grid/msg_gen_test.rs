use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::internal::grid::{
    ConnectReq, ConnectResp, Flags, HandlerID, Message, MuxConnectError, PingMsg, PongMsg,
    FLAG_CRC_XXH3, FLAG_EOF, OP_PING, OP_RESPONSE,
};

pub const SOURCE_FILE: &str = "internal/grid/msg_gen_test.go";

trait GridCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
    fn msgsize(&self) -> usize;
}

macro_rules! impl_grid_codec {
    ($ty:ty) => {
        impl GridCodec for $ty {
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

impl_grid_codec!(ConnectReq);
impl_grid_codec!(ConnectResp);
impl_grid_codec!(Message);
impl_grid_codec!(MuxConnectError);
impl_grid_codec!(PingMsg);
impl_grid_codec!(PongMsg);

fn assert_marshal_unmarshal<T: GridCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = T::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

fn benchmark_marshal<T: GridCodec>(value: T) {
    for _ in 0..100 {
        let bytes = value.marshal_msg().expect("marshal");
        assert!(!bytes.is_empty());
    }
}

fn benchmark_append<T: GridCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    assert!(value.msgsize() >= bytes.len());
}

fn benchmark_unmarshal<T: GridCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..100 {
        let mut decoded = T::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal");
        assert_eq!(decoded, value);
    }
}

fn assert_encode_decode<T: GridCodec>(value: T) {
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode");
    assert!(value.msgsize() >= buf.get_ref().len());
    buf.set_position(0);
    let mut decoded = T::default();
    decoded.decode(&mut buf).expect("decode");
    assert_eq!(decoded, value);
}

fn benchmark_encode<T: GridCodec>(value: T) {
    for _ in 0..100 {
        let mut buf = Cursor::new(Vec::new());
        value.encode(&mut buf).expect("encode");
    }
}

fn benchmark_decode<T: GridCodec>(value: T) {
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..100 {
        let mut buf = Cursor::new(bytes.clone());
        let mut decoded = T::default();
        decoded.decode(&mut buf).expect("decode");
    }
}

fn sample_connect_req() -> ConnectReq {
    ConnectReq {
        id: *b"0123456789abcdef",
        host: "minio.local".to_owned(),
        time_unix_ms: 1_710_000_000_123,
        token: "token-1".to_owned(),
    }
}

fn sample_connect_resp() -> ConnectResp {
    ConnectResp {
        id: *b"fedcba9876543210",
        accepted: true,
        rejected_reason: String::new(),
    }
}

fn sample_message() -> Message {
    Message {
        mux_id: 42,
        seq: 7,
        deadline_ms: 5_000,
        handler: HandlerID(3),
        op: OP_RESPONSE,
        flags: Flags(FLAG_CRC_XXH3.0 | FLAG_EOF.0),
        payload: Some(b"payload".to_vec()),
    }
}

fn sample_mux_connect_error() -> MuxConnectError {
    MuxConnectError {
        error: "mux failed".to_owned(),
    }
}

fn sample_ping_msg() -> PingMsg {
    PingMsg {
        t_unix_ms: 1_710_000_000_999,
    }
}

fn sample_pong_msg() -> PongMsg {
    PongMsg {
        not_found: false,
        err: Some("pong error".to_owned()),
        t_unix_ms: 1_710_000_001_111,
    }
}

#[test]
fn test_marshal_unmarshalconnect_req_line_12() {
    assert_marshal_unmarshal(sample_connect_req());
}

#[test]
fn benchmark_marshal_msgconnect_req_line_35() {
    benchmark_marshal(ConnectReq::default());
}

#[test]
fn benchmark_append_msgconnect_req_line_44() {
    benchmark_append(sample_connect_req());
}

#[test]
fn benchmark_unmarshalconnect_req_line_56() {
    benchmark_unmarshal(sample_connect_req());
}

#[test]
fn test_encode_decodeconnect_req_line_70() {
    assert_encode_decode(sample_connect_req());
}

#[test]
fn benchmark_encodeconnect_req_line_94() {
    benchmark_encode(ConnectReq::default());
}

#[test]
fn benchmark_decodeconnect_req_line_108() {
    benchmark_decode(sample_connect_req());
}

#[test]
fn test_marshal_unmarshalconnect_resp_line_125() {
    assert_marshal_unmarshal(sample_connect_resp());
}

#[test]
fn benchmark_marshal_msgconnect_resp_line_148() {
    benchmark_marshal(ConnectResp::default());
}

#[test]
fn benchmark_append_msgconnect_resp_line_157() {
    benchmark_append(sample_connect_resp());
}

#[test]
fn benchmark_unmarshalconnect_resp_line_169() {
    benchmark_unmarshal(sample_connect_resp());
}

#[test]
fn test_encode_decodeconnect_resp_line_183() {
    assert_encode_decode(sample_connect_resp());
}

#[test]
fn benchmark_encodeconnect_resp_line_207() {
    benchmark_encode(ConnectResp::default());
}

#[test]
fn benchmark_decodeconnect_resp_line_221() {
    benchmark_decode(sample_connect_resp());
}

#[test]
fn test_marshal_unmarshalmessage_line_238() {
    assert_marshal_unmarshal(sample_message());
}

#[test]
fn benchmark_marshal_msgmessage_line_261() {
    benchmark_marshal(Message::default());
}

#[test]
fn benchmark_append_msgmessage_line_270() {
    benchmark_append(sample_message());
}

#[test]
fn benchmark_unmarshalmessage_line_282() {
    benchmark_unmarshal(sample_message());
}

#[test]
fn test_encode_decodemessage_line_296() {
    assert_encode_decode(sample_message());
}

#[test]
fn benchmark_encodemessage_line_320() {
    benchmark_encode(Message::default());
}

#[test]
fn benchmark_decodemessage_line_334() {
    benchmark_decode(sample_message());
}

#[test]
fn test_marshal_unmarshalmux_connect_error_line_351() {
    assert_marshal_unmarshal(sample_mux_connect_error());
}

#[test]
fn benchmark_marshal_msgmux_connect_error_line_374() {
    benchmark_marshal(MuxConnectError::default());
}

#[test]
fn benchmark_append_msgmux_connect_error_line_383() {
    benchmark_append(sample_mux_connect_error());
}

#[test]
fn benchmark_unmarshalmux_connect_error_line_395() {
    benchmark_unmarshal(sample_mux_connect_error());
}

#[test]
fn test_encode_decodemux_connect_error_line_409() {
    assert_encode_decode(sample_mux_connect_error());
}

#[test]
fn benchmark_encodemux_connect_error_line_433() {
    benchmark_encode(MuxConnectError::default());
}

#[test]
fn benchmark_decodemux_connect_error_line_447() {
    benchmark_decode(sample_mux_connect_error());
}

#[test]
fn test_marshal_unmarshalping_msg_line_464() {
    assert_marshal_unmarshal(sample_ping_msg());
}

#[test]
fn benchmark_marshal_msgping_msg_line_487() {
    benchmark_marshal(PingMsg::default());
}

#[test]
fn benchmark_append_msgping_msg_line_496() {
    benchmark_append(sample_ping_msg());
}

#[test]
fn benchmark_unmarshalping_msg_line_508() {
    benchmark_unmarshal(sample_ping_msg());
}

#[test]
fn test_encode_decodeping_msg_line_522() {
    let value = PingMsg {
        t_unix_ms: sample_ping_msg().t_unix_ms,
    };
    assert_eq!(value.op(), OP_PING);
    assert_encode_decode(value);
}

#[test]
fn benchmark_encodeping_msg_line_546() {
    benchmark_encode(PingMsg::default());
}

#[test]
fn benchmark_decodeping_msg_line_560() {
    benchmark_decode(sample_ping_msg());
}

#[test]
fn test_marshal_unmarshalpong_msg_line_577() {
    assert_marshal_unmarshal(sample_pong_msg());
}

#[test]
fn benchmark_marshal_msgpong_msg_line_600() {
    benchmark_marshal(PongMsg::default());
}

#[test]
fn benchmark_append_msgpong_msg_line_609() {
    benchmark_append(sample_pong_msg());
}

#[test]
fn benchmark_unmarshalpong_msg_line_621() {
    benchmark_unmarshal(sample_pong_msg());
}

#[test]
fn test_encode_decodepong_msg_line_635() {
    assert_encode_decode(sample_pong_msg());
}

#[test]
fn benchmark_encodepong_msg_line_659() {
    benchmark_encode(PongMsg::default());
}

#[test]
fn benchmark_decodepong_msg_line_673() {
    benchmark_decode(sample_pong_msg());
}
