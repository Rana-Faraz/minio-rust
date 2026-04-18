use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use minio_rust::cmd::{
    does_policy_signature_v4_match, does_presigned_signature_v4_match, does_signature_v4_match,
    new_test_request, pre_sign_v4_standard, sign_request_v4_standard, ApiErrorCode,
};
use sha2::Sha256;

pub const SOURCE_FILE: &str = "cmd/signature-v4_test.go";

type HmacSha256 = Hmac<Sha256>;

fn hex_hmac_sha256(secret_key: &str, payload: &str) -> String {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret_key.as_bytes()).expect("hmac");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[test]
fn test_does_policy_signature_match_line_39() {
    let access_key = "minioadmin";
    let secret_key = "miniosecret";
    let policy = r#"{"expiration":"2030-01-01T00:00:00Z"}"#;
    let credential = format!("{access_key}/20260418/us-east-1/s3/aws4_request");
    let signature = hex_hmac_sha256(secret_key, policy);

    let form = BTreeMap::from([
        ("policy".to_string(), policy.to_string()),
        ("x-amz-credential".to_string(), credential),
        ("x-amz-signature".to_string(), signature),
    ]);
    assert_eq!(
        does_policy_signature_v4_match(&form, secret_key, "us-east-1"),
        (access_key.to_string(), ApiErrorCode::None)
    );

    let bad = BTreeMap::from([
        ("policy".to_string(), policy.to_string()),
        (
            "x-amz-credential".to_string(),
            format!("{access_key}/20260418/us-east-1/s3/aws4_request"),
        ),
        ("x-amz-signature".to_string(), "deadbeef".to_string()),
    ]);
    assert_eq!(
        does_policy_signature_v4_match(&bad, secret_key, "us-east-1"),
        (access_key.to_string(), ApiErrorCode::SignatureDoesNotMatch)
    );
}

#[test]
fn test_does_presigned_signature_match_line_102() {
    let access_key = "minioadmin";
    let secret_key = "miniosecret";
    let when = Utc::now();
    let expires = 60_i64;

    let mut req =
        new_test_request("GET", "http://127.0.0.1:9000/bucket/object", 0, None).expect("request");
    pre_sign_v4_standard(&mut req, access_key, secret_key, "us-east-1", when, expires)
        .expect("presign");

    assert_eq!(
        does_presigned_signature_v4_match(&req, secret_key, "us-east-1"),
        (access_key.to_string(), ApiErrorCode::None)
    );

    req.set_query_value("X-Amz-Signature", "deadbeef");
    assert_eq!(
        does_presigned_signature_v4_match(&req, secret_key, "us-east-1"),
        (access_key.to_string(), ApiErrorCode::SignatureDoesNotMatch)
    );
}

#[test]
fn test_does_header_signature_match_line_170() {
    let access_key = "minioadmin";
    let secret_key = "miniosecret";
    let when = DateTime::<Utc>::from_timestamp(1_713_654_000, 0).expect("timestamp");

    let mut req = new_test_request(
        "PUT",
        "http://127.0.0.1:9000/bucket/object.txt",
        5,
        Some(b"hello"),
    )
    .expect("request");
    sign_request_v4_standard(&mut req, access_key, secret_key, "us-east-1", when)
        .expect("sign request");

    assert_eq!(
        does_signature_v4_match(&req, secret_key, "us-east-1"),
        (access_key.to_string(), ApiErrorCode::None)
    );

    req.set_header("x-amz-content-sha256", "deadbeef");
    assert_eq!(
        does_signature_v4_match(&req, secret_key, "us-east-1"),
        (access_key.to_string(), ApiErrorCode::SignatureDoesNotMatch)
    );
}
