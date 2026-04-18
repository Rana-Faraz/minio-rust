use base64::Engine;
use md5::{Digest, Md5};

use minio_rust::cmd::{
    check_admin_request_auth, get_request_auth_type, is_req_authenticated,
    is_request_presigned_signature_v2, is_request_presigned_signature_v4,
    is_supported_s3_auth_type, new_test_request, pre_sign_v2, pre_sign_v4, sign_request_v2,
    sign_request_v4, validate_admin_signature, ApiErrorCode, AuthType, Credentials,
};

pub const SOURCE_FILE: &str = "cmd/auth-handler_test.go";

fn active_credentials() -> Credentials {
    Credentials::new("myuser", "mypassword")
}

fn admin_credentials() -> Credentials {
    Credentials::new("admin", "mypassword")
}

fn must_new_request(method: &str, url: &str, body: Option<&[u8]>) -> minio_rust::cmd::TestRequest {
    new_test_request(method, url, body.map(|b| b.len() as i64).unwrap_or(0), body).expect("request")
}

fn must_new_signed_request(
    method: &str,
    url: &str,
    body: Option<&[u8]>,
) -> minio_rust::cmd::TestRequest {
    let mut req = must_new_request(method, url, body);
    let creds = active_credentials();
    sign_request_v4(&mut req, &creds.access_key, &creds.secret_key).expect("sign v4");
    req
}

fn must_new_signed_v2_request(
    method: &str,
    url: &str,
    body: Option<&[u8]>,
) -> minio_rust::cmd::TestRequest {
    let mut req = must_new_request(method, url, body);
    let creds = active_credentials();
    sign_request_v2(&mut req, &creds.access_key, &creds.secret_key).expect("sign v2");
    req
}

fn must_new_presigned_v2_request(
    method: &str,
    url: &str,
    body: Option<&[u8]>,
) -> minio_rust::cmd::TestRequest {
    let mut req = must_new_request(method, url, body);
    let creds = active_credentials();
    pre_sign_v2(&mut req, &creds.access_key, &creds.secret_key, 600).expect("presign v2");
    req
}

fn must_new_presigned_request(
    method: &str,
    url: &str,
    body: Option<&[u8]>,
) -> minio_rust::cmd::TestRequest {
    let mut req = must_new_request(method, url, body);
    let creds = active_credentials();
    pre_sign_v4(&mut req, &creds.access_key, &creds.secret_key, 600).expect("presign v4");
    req
}

fn must_new_signed_short_md5_request(
    method: &str,
    url: &str,
    body: &[u8],
) -> minio_rust::cmd::TestRequest {
    let mut req = must_new_signed_request(method, url, Some(body));
    req.set_header("Content-Md5", "invalid-digest");
    req
}

fn must_new_signed_empty_md5_request(
    method: &str,
    url: &str,
    body: &[u8],
) -> minio_rust::cmd::TestRequest {
    let mut req = must_new_signed_request(method, url, Some(body));
    req.set_header("Content-Md5", "");
    req
}

fn must_new_signed_bad_md5_request(
    method: &str,
    url: &str,
    body: &[u8],
) -> minio_rust::cmd::TestRequest {
    let mut req = must_new_signed_request(method, url, Some(body));
    req.set_header("Content-Md5", "YWFhYWFhYWFhYWFhYWFhCg==");
    req
}

#[test]
fn test_get_request_auth_type_line_41() {
    let mut streaming = must_new_request("PUT", "http://127.0.0.1:9000/", Some(&vec![0; 1024]));
    streaming.set_header(
        "Authorization",
        "AWS4-HMAC-SHA256 Credential=test, Secret=test",
    );
    streaming.set_header("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
    streaming.set_header("Content-Encoding", "aws-chunked");

    let mut jwt = must_new_request("GET", "http://127.0.0.1:9000/", None);
    jwt.set_header("Authorization", "Bearer 12313123");

    let mut unknown = must_new_request("GET", "http://127.0.0.1:9000/", None);
    unknown.set_header("Authorization", "");

    let presigned = must_new_presigned_request("GET", "http://127.0.0.1:9000/", None);

    let mut post_policy = must_new_request("POST", "http://127.0.0.1:9000/", Some(b"x"));
    post_policy.set_header("Content-Type", "multipart/form-data");

    let cases = [
        (streaming, AuthType::StreamingSigned),
        (jwt, AuthType::Jwt),
        (unknown, AuthType::Unknown),
        (presigned, AuthType::Presigned),
        (post_policy, AuthType::PostPolicy),
    ];

    for (request, expected) in cases {
        assert_eq!(get_request_auth_type(&request), expected);
    }
}

#[test]
fn test_s3_supported_auth_type_line_139() {
    let supported = [
        AuthType::Anonymous,
        AuthType::Presigned,
        AuthType::Signed,
        AuthType::PostPolicy,
        AuthType::StreamingSigned,
        AuthType::SignedV2,
        AuthType::PresignedV2,
    ];
    for auth_type in supported {
        assert!(is_supported_s3_auth_type(auth_type));
    }
    for auth_type in [AuthType::Jwt, AuthType::Unknown] {
        assert!(!is_supported_s3_auth_type(auth_type));
    }
}

#[test]
fn test_is_request_presigned_signature_v2_line_206() {
    let empty = must_new_request("GET", "http://example.com", None);
    let mut presigned = must_new_request("GET", "http://example.com", None);
    presigned.set_query_value("AWSAccessKeyId", "");
    let mut other = must_new_request("GET", "http://example.com", None);
    other.set_query_value("X-Amz-Content-Sha256", "");

    assert!(!is_request_presigned_signature_v2(&empty));
    assert!(is_request_presigned_signature_v2(&presigned));
    assert!(!is_request_presigned_signature_v2(&other));
}

#[test]
fn test_is_request_presigned_signature_v4_line_241() {
    let empty = must_new_request("GET", "http://example.com", None);
    let mut presigned = must_new_request("GET", "http://example.com", None);
    presigned.set_query_value("X-Amz-Credential", "");
    let mut other = must_new_request("GET", "http://example.com", None);
    other.set_query_value("X-Amz-Content-Sha256", "");

    assert!(!is_request_presigned_signature_v4(&empty));
    assert!(is_request_presigned_signature_v4(&presigned));
    assert!(!is_request_presigned_signature_v4(&other));
}

#[test]
fn test_is_req_authenticated_line_361() {
    let body = b"hello";
    let cases = [
        (
            must_new_request("GET", "http://127.0.0.1:9000", None),
            ApiErrorCode::AccessDenied,
        ),
        (
            must_new_signed_empty_md5_request("PUT", "http://127.0.0.1:9000/", body),
            ApiErrorCode::InvalidDigest,
        ),
        (
            must_new_signed_short_md5_request("PUT", "http://127.0.0.1:9000/", body),
            ApiErrorCode::InvalidDigest,
        ),
        (
            must_new_signed_bad_md5_request("PUT", "http://127.0.0.1:9000/", body),
            ApiErrorCode::BadDigest,
        ),
        (
            must_new_signed_request("GET", "http://127.0.0.1:9000", None),
            ApiErrorCode::None,
        ),
    ];

    for (request, expected) in cases {
        assert_eq!(
            is_req_authenticated(&request, &active_credentials()),
            expected
        );
    }

    let mut md5_ok = must_new_signed_request("PUT", "http://127.0.0.1:9000/", Some(body));
    let digest = base64::engine::general_purpose::STANDARD.encode(Md5::digest(body));
    md5_ok.set_header("Content-Md5", &digest);
    assert_eq!(
        is_req_authenticated(&md5_ok, &active_credentials()),
        ApiErrorCode::None
    );
}

#[test]
fn test_check_admin_request_auth_type_line_415() {
    let cases = [
        (
            must_new_request("GET", "http://127.0.0.1:9000", None),
            ApiErrorCode::AccessDenied,
        ),
        (
            {
                let mut req = must_new_request("GET", "http://127.0.0.1:9000", None);
                let creds = admin_credentials();
                sign_request_v4(&mut req, &creds.access_key, &creds.secret_key).expect("sign");
                req
            },
            ApiErrorCode::None,
        ),
        (
            must_new_signed_v2_request("GET", "http://127.0.0.1:9000", None),
            ApiErrorCode::AccessDenied,
        ),
        (
            must_new_presigned_v2_request("GET", "http://127.0.0.1:9000", None),
            ApiErrorCode::AccessDenied,
        ),
        (
            must_new_presigned_request("GET", "http://127.0.0.1:9000", None),
            ApiErrorCode::AccessDenied,
        ),
    ];

    for (request, expected) in cases {
        assert_eq!(
            check_admin_request_auth(&request, &admin_credentials()).1,
            expected
        );
    }
}

#[test]
fn test_validate_admin_signature_line_452() {
    let cases = [
        ("", "", ApiErrorCode::InvalidAccessKeyID),
        ("admin", "", ApiErrorCode::SignatureDoesNotMatch),
        (
            "admin",
            "wrongpassword",
            ApiErrorCode::SignatureDoesNotMatch,
        ),
        ("wronguser", "mypassword", ApiErrorCode::InvalidAccessKeyID),
        ("", "mypassword", ApiErrorCode::InvalidAccessKeyID),
        ("admin", "mypassword", ApiErrorCode::None),
    ];

    for (access_key, secret_key, expected) in cases {
        let mut request = must_new_request("GET", "http://localhost:9000/", None);
        sign_request_v4(&mut request, access_key, secret_key).expect("sign");
        let (_, _, got) = validate_admin_signature(&request, &admin_credentials());
        assert_eq!(got, expected);
    }
}
