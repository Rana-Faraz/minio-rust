use std::io::Cursor;

use minio_rust::internal::grid::{TestRequest, TestResponse};

pub const SOURCE_FILE: &str = "internal/grid/grid_types_msgp_test.go";

#[test]
fn generated_test_request_encode_decode() {
    let value = TestRequest {
        num: 12,
        string: "codec".to_owned(),
    };
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode test request");
    assert!(value.msgsize() >= buf.get_ref().len());
    buf.set_position(0);
    let mut decoded = TestRequest::default();
    decoded.decode(&mut buf).expect("decode test request");
    assert_eq!(decoded, value);
}

#[test]
fn generated_test_response_encode_decode() {
    let value = TestResponse {
        org_num: 21,
        org_string: "response".to_owned(),
        embedded: TestRequest {
            num: 2,
            string: "req".to_owned(),
        },
    };
    let mut buf = Cursor::new(Vec::new());
    value.encode(&mut buf).expect("encode test response");
    assert!(value.msgsize() >= buf.get_ref().len());
    buf.set_position(0);
    let mut decoded = TestResponse::default();
    decoded.decode(&mut buf).expect("decode test response");
    assert_eq!(decoded, value);
}
