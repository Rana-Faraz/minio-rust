use minio_rust::internal::grid::{TestRequest, TestResponse};

pub const SOURCE_FILE: &str = "internal/grid/grid_types_test.go";

#[test]
fn generated_test_request_roundtrip() {
    let value = TestRequest {
        num: 7,
        string: "hello".to_owned(),
    };
    let bytes = value.marshal_msg().expect("marshal test request");
    let mut decoded = TestRequest::default();
    let left = decoded
        .unmarshal_msg(&bytes)
        .expect("unmarshal test request");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}

#[test]
fn generated_test_response_roundtrip() {
    let value = TestResponse {
        org_num: 9,
        org_string: "world".to_owned(),
        embedded: TestRequest {
            num: 3,
            string: "embedded".to_owned(),
        },
    };
    let bytes = value.marshal_msg().expect("marshal test response");
    let mut decoded = TestResponse::default();
    let left = decoded
        .unmarshal_msg(&bytes)
        .expect("unmarshal test response");
    assert!(left.is_empty());
    assert_eq!(decoded, value);
}
