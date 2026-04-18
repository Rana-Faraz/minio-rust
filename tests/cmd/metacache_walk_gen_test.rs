use std::io::Cursor;

use minio_rust::cmd::WalkDirOptions;

pub const SOURCE_FILE: &str = "cmd/metacache-walk_gen_test.go";

fn sample_walk_dir_options() -> WalkDirOptions {
    WalkDirOptions {
        bucket: "bucket".to_string(),
        base_dir: "prefix/".to_string(),
        recursive: true,
        report_not_found: true,
        filter_prefix: "obj".to_string(),
        forward_to: "prefix/obj-10".to_string(),
        limit: 100,
        disk_id: "disk-1".to_string(),
    }
}

#[test]
fn test_marshal_unmarshal_walk_dir_options_line_12() {
    let value = sample_walk_dir_options();
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = WalkDirOptions::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_marshal_msg_walk_dir_options_line_35() {
    let value = sample_walk_dir_options();
    for _ in 0..128 {
        assert!(!value.marshal_msg().expect("marshal").is_empty());
    }
}

#[test]
fn benchmark_append_msg_walk_dir_options_line_44() {
    let value = sample_walk_dir_options();
    let baseline = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let bytes = value.marshal_msg().expect("marshal");
        assert_eq!(bytes, baseline);
    }
}

#[test]
fn benchmark_unmarshal_walk_dir_options_line_56() {
    let value = sample_walk_dir_options();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let mut decoded = WalkDirOptions::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_encode_decode_walk_dir_options_line_70() {
    let value = sample_walk_dir_options();
    let mut buf = Vec::new();
    value.encode(&mut buf).expect("encode");
    assert!(buf.len() <= value.msgsize());

    let mut decoded = WalkDirOptions::default();
    decoded
        .decode(&mut Cursor::new(buf.clone()))
        .expect("decode");
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_encode_walk_dir_options_line_94() {
    let value = sample_walk_dir_options();
    for _ in 0..128 {
        let mut buf = Vec::new();
        value.encode(&mut buf).expect("encode");
        assert!(!buf.is_empty());
    }
}

#[test]
fn benchmark_decode_walk_dir_options_line_108() {
    let value = sample_walk_dir_options();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let mut decoded = WalkDirOptions::default();
        decoded
            .decode(&mut Cursor::new(bytes.clone()))
            .expect("decode");
        assert_eq!(decoded, value);
    }
}
