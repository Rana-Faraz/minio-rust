use std::io::Cursor;

use minio_rust::internal::dsync::{LockArgs, LockResp, ResponseCode};

#[test]
fn test_marshal_unmarshal_lock_args_line_12() {
    let value = LockArgs::default();
    let bytes = value.marshal_msg().expect("marshal lock args");
    let mut decoded = LockArgs::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal lock args");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_marshal_msg_lock_args_line_35() {
    let value = LockArgs::default();
    for _ in 0..100 {
        let bytes = value.marshal_msg().expect("marshal lock args");
        assert!(!bytes.is_empty());
    }
}

#[test]
fn benchmark_append_msg_lock_args_line_44() {
    let value = LockArgs::default();
    let bytes = value.marshal_msg().expect("marshal lock args");
    assert!(value.msgsize() >= bytes.len());
}

#[test]
fn benchmark_unmarshal_lock_args_line_56() {
    let value = LockArgs::default();
    let bytes = value.marshal_msg().expect("marshal lock args");
    for _ in 0..100 {
        let mut decoded = LockArgs::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal lock args");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_encode_decode_lock_args_line_70() {
    let value = LockArgs {
        uid: "uid-1".to_owned(),
        resources: vec!["a".to_owned(), "b".to_owned()],
        owner: "owner".to_owned(),
        source: "main.rs".to_owned(),
        quorum: Some(3),
    };
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode lock args");
    buf.set_position(0);
    let mut decoded = LockArgs::default();
    decoded.decode(&mut buf).expect("decode lock args");
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_encode_lock_args_line_94() {
    let value = LockArgs::default();
    for _ in 0..100 {
        let mut buf = Cursor::new(Vec::new());
        value.encode(&mut buf).expect("encode lock args");
    }
}

#[test]
fn benchmark_decode_lock_args_line_108() {
    let value = LockArgs::default();
    let bytes = value.marshal_msg().expect("marshal lock args");
    for _ in 0..100 {
        let mut buf = Cursor::new(bytes.clone());
        let mut decoded = LockArgs::default();
        decoded.decode(&mut buf).expect("decode lock args");
    }
}

#[test]
fn test_marshal_unmarshal_lock_resp_line_125() {
    let value = LockResp::default();
    let bytes = value.marshal_msg().expect("marshal lock resp");
    let mut decoded = LockResp::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal lock resp");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_marshal_msg_lock_resp_line_148() {
    let value = LockResp::default();
    for _ in 0..100 {
        let bytes = value.marshal_msg().expect("marshal lock resp");
        assert!(!bytes.is_empty());
    }
}

#[test]
fn benchmark_append_msg_lock_resp_line_157() {
    let value = LockResp::default();
    let bytes = value.marshal_msg().expect("marshal lock resp");
    assert!(value.msgsize() >= bytes.len());
}

#[test]
fn benchmark_unmarshal_lock_resp_line_169() {
    let value = LockResp::default();
    let bytes = value.marshal_msg().expect("marshal lock resp");
    for _ in 0..100 {
        let mut decoded = LockResp::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal lock resp");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_encode_decode_lock_resp_line_183() {
    let value = LockResp {
        code: ResponseCode::LockConflict,
        err: "conflict".to_owned(),
    };
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode lock resp");
    buf.set_position(0);
    let mut decoded = LockResp::default();
    decoded.decode(&mut buf).expect("decode lock resp");
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_encode_lock_resp_line_207() {
    let value = LockResp::default();
    for _ in 0..100 {
        let mut buf = Cursor::new(Vec::new());
        value.encode(&mut buf).expect("encode lock resp");
    }
}

#[test]
fn benchmark_decode_lock_resp_line_221() {
    let value = LockResp::default();
    let bytes = value.marshal_msg().expect("marshal lock resp");
    for _ in 0..100 {
        let mut buf = Cursor::new(bytes.clone());
        let mut decoded = LockResp::default();
        decoded.decode(&mut buf).expect("decode lock resp");
    }
}
