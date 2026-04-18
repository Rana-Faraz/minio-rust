use chrono::Utc;

use minio_rust::cmd::must_get_request_id;

pub const SOURCE_FILE: &str = "cmd/api-headers_test.go";

#[test]
fn test_new_request_id_line_24() {
    let id = must_get_request_id(Utc::now());
    assert_eq!(id.len(), 16);
    assert!(id
        .chars()
        .all(|ch| ch.is_ascii_digit() || ('A'..='Z').contains(&ch)));
}
