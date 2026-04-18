use std::collections::BTreeMap;

use chrono::DateTime;
use minio_rust::cmd::{
    calculate_signature_v2, does_policy_signature_v2_match, does_presign_v2_signature_match,
    does_signature_v2_match, pre_sign_v2 as legacy_pre_sign_v2, presign_v2, resource_list,
    sign_request_v2_standard, validate_v2_auth_header, ApiErrorCode, Credentials,
};

pub const SOURCE_FILE: &str = "cmd/signature-v2_test.go";

#[test]
fn test_resource_list_sorting_line_31() {
    let mut sorted = resource_list().to_vec();
    sorted.sort_unstable();
    assert_eq!(resource_list(), sorted.as_slice());
}

#[test]
fn test_does_presigned_v2_signature_match_line_44() {
    let creds = Credentials::new("minioadmin", "minioadmin");
    let now = 1_700_000_000_i64;

    let req = minio_rust::cmd::new_test_request("GET", "http://host/a/b", 0, None).unwrap();
    assert_eq!(
        does_presign_v2_signature_match(&req, &creds, now),
        ApiErrorCode::InvalidQueryParams
    );

    let mut req = minio_rust::cmd::new_test_request(
        "GET",
        "http://host/a/b?Expires=60&Signature=x&AWSAccessKeyId=bad",
        0,
        None,
    )
    .unwrap();
    assert_eq!(
        does_presign_v2_signature_match(&req, &creds, now),
        ApiErrorCode::InvalidAccessKeyID
    );

    req = minio_rust::cmd::new_test_request(
        "GET",
        "http://host/a/b?Expires=60s&Signature=x&AWSAccessKeyId=minioadmin",
        0,
        None,
    )
    .unwrap();
    assert_eq!(
        does_presign_v2_signature_match(&req, &creds, now),
        ApiErrorCode::MalformedExpires
    );

    req = minio_rust::cmd::new_test_request(
        "GET",
        "http://host/a/b?Expires=60&Signature=x&AWSAccessKeyId=minioadmin",
        0,
        None,
    )
    .unwrap();
    assert_eq!(
        does_presign_v2_signature_match(&req, &creds, now),
        ApiErrorCode::ExpiredPresignRequest
    );

    let mut req = minio_rust::cmd::new_test_request(
        "GET",
        "http://host/a/b?Expires=1700000060&Signature=badsignature&AWSAccessKeyId=minioadmin",
        0,
        None,
    )
    .unwrap();
    assert_eq!(
        does_presign_v2_signature_match(&req, &creds, now),
        ApiErrorCode::SignatureDoesNotMatch
    );

    req = minio_rust::cmd::new_test_request(
        "GET",
        "http://host/a/b?response-content-disposition=attachment%3B%20filename%3D%224K-4M.txt%22",
        0,
        None,
    )
    .unwrap();
    presign_v2(&mut req, &creds.access_key, &creds.secret_key, now + 60);
    assert_eq!(
        does_presign_v2_signature_match(&req, &creds, now),
        ApiErrorCode::None
    );

    let mut req = minio_rust::cmd::new_test_request("GET", "http://host/a/b", 0, None).unwrap();
    presign_v2(&mut req, &creds.access_key, &creds.secret_key, now + 60);
    assert_eq!(
        does_presign_v2_signature_match(&req, &creds, now),
        ApiErrorCode::None
    );

    // Keep the older auth helper path exercised too.
    let mut legacy = minio_rust::cmd::new_test_request("GET", "http://host/a/b", 0, None).unwrap();
    legacy_pre_sign_v2(&mut legacy, &creds.access_key, &creds.secret_key, now + 60).unwrap();
    assert!(legacy
        .url
        .query()
        .unwrap_or_default()
        .contains("AWSAccessKeyId"));
}

#[test]
fn test_validate_v2_auth_header_line_166() {
    let creds = Credentials::new("minioadmin", "minioadmin");
    let cases = [
        ("", ApiErrorCode::AuthHeaderEmpty),
        ("NoV2Prefix", ApiErrorCode::SignatureVersionNotSupported),
        ("AWS", ApiErrorCode::MissingFields),
        ("AWS minioadmin", ApiErrorCode::MissingFields),
        (
            "AWS InvalidAccessID:signature",
            ApiErrorCode::InvalidAccessKeyID,
        ),
        ("AWS minioadmin:signature", ApiErrorCode::None),
    ];

    for (auth, expected) in cases {
        let (_, actual) = validate_v2_auth_header(auth, &creds);
        assert_eq!(actual, expected, "auth={auth}");
    }
}

#[test]
fn test_does_signature_v2_match_line_210() {
    let creds = Credentials::new("minioadmin", "minioadmin");
    let when = DateTime::from_timestamp(1_713_654_000, 0).expect("timestamp");

    let mut req = minio_rust::cmd::new_test_request("GET", "http://host/bucket/object", 0, None)
        .expect("request");
    sign_request_v2_standard(&mut req, &creds.access_key, &creds.secret_key, when)
        .expect("sign v2");
    assert_eq!(
        does_signature_v2_match(&req, &creds),
        (creds.access_key.clone(), ApiErrorCode::None)
    );

    req.set_header("date", "Mon, 01 Jan 2001 00:00:00 GMT");
    assert_eq!(
        does_signature_v2_match(&req, &creds),
        (
            creds.access_key.clone(),
            ApiErrorCode::SignatureDoesNotMatch
        )
    );
}

#[test]
fn subtest_test_validate_v2_auth_header_fmt_sprintf_case_d_auth_str_s_line_225() {
    let creds = Credentials::new("minioadmin", "minioadmin");
    let (_, actual) = validate_v2_auth_header("AWS minioadmin:signature", &creds);
    assert_eq!(actual, ApiErrorCode::None);
}

#[test]
fn test_does_policy_signature_v2_match_line_240() {
    let creds = Credentials::new("minioadmin", "minioadmin");
    let policy = "policy";
    let good = calculate_signature_v2(policy, &creds.secret_key);

    let cases = [
        (
            "invalid",
            policy,
            good.clone(),
            ApiErrorCode::InvalidAccessKeyID,
        ),
        (
            "minioadmin",
            policy,
            calculate_signature_v2("random", &creds.secret_key),
            ApiErrorCode::SignatureDoesNotMatch,
        ),
        ("minioadmin", policy, good, ApiErrorCode::None),
    ];

    for (index, (access, policy, signature, expected)) in cases.into_iter().enumerate() {
        let mut values = BTreeMap::new();
        values.insert("Awsaccesskeyid".to_string(), access.to_string());
        values.insert("Signature".to_string(), signature);
        values.insert("Policy".to_string(), policy.to_string());
        let (_, actual) = does_policy_signature_v2_match(&values, &creds);
        assert_eq!(actual, expected, "case={index}");
    }
}
