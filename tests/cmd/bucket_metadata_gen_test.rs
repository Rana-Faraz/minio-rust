use std::io::Cursor;

use minio_rust::cmd::BucketMetadata;

pub const SOURCE_FILE: &str = "cmd/bucket-metadata_gen_test.go";

#[test]
fn test_marshal_unmarshal_bucket_metadata_line_12() {
    let value = BucketMetadata::default();
    let bytes = value.marshal_msg().expect("marshal");

    let mut decoded = BucketMetadata::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);

    let skipped: BucketMetadata = rmp_serde::from_slice(&bytes).expect("skip-like decode");
    assert_eq!(skipped, value);
}

#[test]
fn benchmark_marshal_msg_bucket_metadata_line_35() {
    let value = BucketMetadata::default();
    for _ in 0..64 {
        let bytes = value.marshal_msg().expect("marshal");
        assert!(!bytes.is_empty());
    }
}

#[test]
fn benchmark_append_msg_bucket_metadata_line_44() {
    let value = BucketMetadata::default();
    let mut last = Vec::with_capacity(value.msgsize());
    for _ in 0..64 {
        last = value.marshal_msg().expect("marshal");
    }
    assert!(!last.is_empty());
}

#[test]
fn benchmark_unmarshal_bucket_metadata_line_56() {
    let value = BucketMetadata::default();
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = BucketMetadata::default();
    for _ in 0..64 {
        let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
        assert!(left.is_empty());
    }
}

#[test]
fn test_encode_decode_bucket_metadata_line_70() {
    let value = BucketMetadata::default();
    let mut buffer = Vec::new();
    value.encode(&mut buffer).expect("encode");

    assert!(buffer.len() <= value.msgsize());

    let mut decoded = BucketMetadata::default();
    decoded
        .decode(&mut Cursor::new(buffer.clone()))
        .expect("decode");
    assert_eq!(decoded, value);

    let skipped: BucketMetadata = rmp_serde::from_read(Cursor::new(buffer)).expect("skip");
    assert_eq!(skipped, value);
}

#[test]
fn benchmark_encode_bucket_metadata_line_94() {
    let value = BucketMetadata::default();
    let mut buffer = Vec::new();
    value.encode(&mut buffer).expect("encode");
    assert!(!buffer.is_empty());

    for _ in 0..64 {
        let mut sink = Vec::new();
        value.encode(&mut sink).expect("encode");
    }
}

#[test]
fn benchmark_decode_bucket_metadata_line_108() {
    let value = BucketMetadata::default();
    let mut buffer = Vec::new();
    value.encode(&mut buffer).expect("encode");

    for _ in 0..64 {
        let mut decoded = BucketMetadata::default();
        decoded
            .decode(&mut Cursor::new(buffer.as_slice()))
            .expect("decode");
        assert_eq!(decoded, value);
    }
}
