use std::io::Cursor;

use minio_rust::cmd::Metacache;

pub const SOURCE_FILE: &str = "cmd/metacache_gen_test.go";

fn sample_metacache() -> Metacache {
    Metacache {
        ended: 120,
        started: 60,
        last_handout: 100,
        last_update: 110,
        bucket: "bucket".to_string(),
        filter: "prefix".to_string(),
        id: "id-1".to_string(),
        error: String::new(),
        root: "folder/prefix".to_string(),
        file_not_found: false,
        status: minio_rust::cmd::ScanStatus::Success,
        recursive: true,
        data_version: minio_rust::cmd::METACACHE_STREAM_VERSION,
    }
}

#[test]
fn test_marshal_unmarshalmetacache_line_12() {
    let value = sample_metacache();
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = Metacache::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_marshal_msgmetacache_line_35() {
    let value = sample_metacache();
    for _ in 0..128 {
        assert!(!value.marshal_msg().expect("marshal").is_empty());
    }
}

#[test]
fn benchmark_append_msgmetacache_line_44() {
    let value = sample_metacache();
    let baseline = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let bytes = value.marshal_msg().expect("marshal");
        assert_eq!(bytes, baseline);
    }
}

#[test]
fn benchmark_unmarshalmetacache_line_56() {
    let value = sample_metacache();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let mut decoded = Metacache::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_encode_decodemetacache_line_70() {
    let value = sample_metacache();
    let mut buf = Vec::new();
    value.encode(&mut buf).expect("encode");
    assert!(buf.len() <= value.msgsize());

    let mut decoded = Metacache::default();
    decoded
        .decode(&mut Cursor::new(buf.clone()))
        .expect("decode");
    assert_eq!(decoded, value);

    let mut skipped = Metacache::default();
    skipped.unmarshal_msg(&buf).expect("skip-style decode");
}

#[test]
fn benchmark_encodemetacache_line_94() {
    let value = sample_metacache();
    for _ in 0..128 {
        let mut buf = Vec::new();
        value.encode(&mut buf).expect("encode");
        assert!(!buf.is_empty());
    }
}

#[test]
fn benchmark_decodemetacache_line_108() {
    let value = sample_metacache();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let mut decoded = Metacache::default();
        decoded
            .decode(&mut Cursor::new(bytes.clone()))
            .expect("decode");
        assert_eq!(decoded, value);
    }
}
