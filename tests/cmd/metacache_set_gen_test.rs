use std::io::Cursor;

use minio_rust::cmd::ListPathOptions;

pub const SOURCE_FILE: &str = "cmd/metacache-set_gen_test.go";

fn sample_list_path_options() -> ListPathOptions {
    ListPathOptions {
        id: "list-id".to_string(),
        bucket: "bucket".to_string(),
        base_dir: "prefix/".to_string(),
        prefix: "prefix/obj".to_string(),
        filter_prefix: "obj".to_string(),
        marker: "prefix/obj-2".to_string(),
        limit: 100,
        ask_disks: "2".to_string(),
        incl_deleted: true,
        recursive: true,
        separator: "/".to_string(),
        create: true,
        include_directories: true,
        transient: false,
        versioned: true,
        v1: false,
        stop_disk_at_limit: true,
        pool: 1,
        set: 2,
    }
}

#[test]
fn test_marshal_unmarshallist_path_options_line_12() {
    let value = sample_list_path_options();
    let bytes = value.marshal_msg().expect("marshal");
    let mut decoded = ListPathOptions::default();
    let left = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_marshal_msglist_path_options_line_35() {
    let value = sample_list_path_options();
    for _ in 0..128 {
        assert!(!value.marshal_msg().expect("marshal").is_empty());
    }
}

#[test]
fn benchmark_append_msglist_path_options_line_44() {
    let value = sample_list_path_options();
    let baseline = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let bytes = value.marshal_msg().expect("marshal");
        assert_eq!(bytes, baseline);
    }
}

#[test]
fn benchmark_unmarshallist_path_options_line_56() {
    let value = sample_list_path_options();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let mut decoded = ListPathOptions::default();
        decoded.unmarshal_msg(&bytes).expect("unmarshal");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_encode_decodelist_path_options_line_70() {
    let value = sample_list_path_options();
    let mut buf = Vec::new();
    value.encode(&mut buf).expect("encode");
    assert!(buf.len() <= value.msgsize());

    let mut decoded = ListPathOptions::default();
    decoded
        .decode(&mut Cursor::new(buf.clone()))
        .expect("decode");
    assert_eq!(decoded, value);
}

#[test]
fn benchmark_encodelist_path_options_line_94() {
    let value = sample_list_path_options();
    for _ in 0..128 {
        let mut buf = Vec::new();
        value.encode(&mut buf).expect("encode");
        assert!(!buf.is_empty());
    }
}

#[test]
fn benchmark_decodelist_path_options_line_108() {
    let value = sample_list_path_options();
    let bytes = value.marshal_msg().expect("marshal");
    for _ in 0..128 {
        let mut decoded = ListPathOptions::default();
        decoded
            .decode(&mut Cursor::new(bytes.clone()))
            .expect("decode");
        assert_eq!(decoded, value);
    }
}
