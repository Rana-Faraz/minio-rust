use std::collections::BTreeMap;

use minio_rust::cmd::{
    active_credentials, check_key_valid, check_meta_headers, extract_signed_headers,
    get_content_sha256_cksum, is_valid_region, new_test_request, sign_request_v4, sign_v4_trim_all,
    skip_content_sha256_cksum, ApiErrorCode, Credentials, EMPTY_SHA256,
    GLOBAL_MINIO_DEFAULT_REGION, UNSIGNED_PAYLOAD,
};

pub const SOURCE_FILE: &str = "cmd/signature-v4-utils_test.go";

#[test]
fn test_check_valid_line_32() {
    let owner = active_credentials();
    let mut req =
        new_test_request("GET", "http://example.com:9000/bucket/object", 0, None).expect("request");
    sign_request_v4(&mut req, &owner.access_key, &owner.secret_key).expect("sign");

    let mut users = BTreeMap::new();
    let user = Credentials::new("myuser1", "mypassword1");
    users.insert(user.access_key.clone(), user.clone());

    let (_, is_owner, code) = check_key_valid(&req, &owner.access_key, &users);
    assert_eq!(code, ApiErrorCode::None);
    assert!(is_owner);

    let (_, is_owner, code) = check_key_valid(&req, "does-not-exist", &users);
    assert_eq!(code, ApiErrorCode::InvalidAccessKeyID);
    assert!(!is_owner);

    let (_, is_owner, code) = check_key_valid(&req, &user.access_key, &users);
    assert_eq!(code, ApiErrorCode::None);
    assert!(!is_owner);
}

#[test]
fn test_skip_content_sha256_cksum_line_118() {
    let cases = [
        (Some(("X-Amz-Content-Sha256", "")), None, false),
        (None, None, true),
        (
            Some(("X-Amz-Content-Sha256", UNSIGNED_PAYLOAD)),
            Some(("X-Amz-Credential", "")),
            true,
        ),
        (None, Some(("X-Amz-Credential", "")), true),
        (
            Some(("X-Amz-Content-Sha256", "somevalue")),
            Some(("X-Amz-Credential", "")),
            false,
        ),
        (Some(("X-Amz-Content-Sha256", UNSIGNED_PAYLOAD)), None, true),
        (None, Some(("X-Amz-Credential", "")), true),
        (Some(("X-Amz-Content-Sha256", "somevalue")), None, false),
    ];
    for (header, query, expected) in cases {
        let mut req = new_test_request("GET", "http://example.com", 0, None).expect("request");
        if let Some((key, value)) = header {
            req.set_header(key, value);
        }
        if let Some((key, value)) = query {
            req.set_query_value(key, value);
        }
        assert_eq!(skip_content_sha256_cksum(&req), expected);
    }
}

#[test]
fn test_is_valid_region_line_189() {
    let cases = [
        ("", "", true),
        (GLOBAL_MINIO_DEFAULT_REGION, "", true),
        (GLOBAL_MINIO_DEFAULT_REGION, "US", true),
        ("us-west-1", "US", false),
        ("us-west-1", "us-west-1", true),
        ("US", "US", true),
    ];
    for (request_region, configured_region, expected) in cases {
        assert_eq!(is_valid_region(request_region, configured_region), expected);
    }
}

#[test]
fn test_extract_signed_headers_line_214() {
    let mut req = new_test_request("GET", "http://play.min.io:9000", 0, None).expect("request");
    req.set_header("x-amz-content-sha256", "1234abcd");
    req.set_header("x-amz-date", "20240321T120000Z");
    req.set_header("transfer-encoding", "gzip");

    let signed = [
        "host",
        "x-amz-content-sha256",
        "x-amz-date",
        "transfer-encoding",
        "expect",
    ];
    let headers = extract_signed_headers(&signed, &req).expect("signed headers");
    assert_eq!(
        headers.get("host").map(String::as_str),
        Some("play.min.io:9000")
    );
    assert_eq!(
        headers.get("x-amz-content-sha256").map(String::as_str),
        Some("1234abcd")
    );
    assert_eq!(
        headers.get("x-amz-date").map(String::as_str),
        Some("20240321T120000Z")
    );
    assert_eq!(
        headers.get("transfer-encoding").map(String::as_str),
        Some("gzip")
    );
    assert_eq!(
        headers.get("expect").map(String::as_str),
        Some("100-continue")
    );

    let missing = ["host", "x-amz-server-side-encryption"];
    assert_eq!(
        extract_signed_headers(&missing, &req),
        Err(ApiErrorCode::UnsignedHeaders)
    );

    req.set_query_value("x-amz-server-side-encryption", "AES256");
    let headers = extract_signed_headers(&missing, &req).expect("query backed header");
    assert_eq!(
        headers
            .get("x-amz-server-side-encryption")
            .map(String::as_str),
        Some("AES256")
    );

    let without_host = ["x-amz-content-sha256", "x-amz-date"];
    assert_eq!(
        extract_signed_headers(&without_host, &req),
        Err(ApiErrorCode::UnsignedHeaders)
    );
}

#[test]
fn test_sign_v4_trim_all_line_307() {
    let cases = [
        ("本語", "本語"),
        (" abc ", "abc"),
        (" a b ", "a b"),
        ("a b ", "a b"),
        ("a  b", "a b"),
        ("a   b", "a b"),
        ("   a   b  c   ", "a b c"),
        ("a \t b  c   ", "a b c"),
        ("\"a \t b  c   ", "\"a b c"),
        (" \t\n\u{000b}\r\u{000c}a \t\n\u{000b}\r\u{000c} b \t\n\u{000b}\r\u{000c} c \t\n\u{000b}\r\u{000c}", "a b c"),
    ];
    for (input, expected) in cases {
        assert_eq!(sign_v4_trim_all(input), expected);
    }
}

#[test]
fn test_get_content_sha256_cksum_line_336() {
    let cases = [
        (Some("shastring"), None, "shastring"),
        (Some(EMPTY_SHA256), None, EMPTY_SHA256),
        (None, None, EMPTY_SHA256),
        (None, Some("X-Amz-Credential=random"), UNSIGNED_PAYLOAD),
        (
            None,
            Some("X-Amz-Credential=random&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD"),
            UNSIGNED_PAYLOAD,
        ),
        (
            None,
            Some("X-Amz-Credential=random&X-Amz-Content-Sha256=shastring"),
            "shastring",
        ),
    ];
    for (header, query, expected) in cases {
        let url = match query {
            Some(query) => format!("http://localhost/?{query}"),
            None => "http://localhost/".to_string(),
        };
        let mut req = new_test_request("GET", &url, 0, None).expect("request");
        if let Some(value) = header {
            req.set_header("x-amz-content-sha256", value);
        }
        assert_eq!(get_content_sha256_cksum(&req), expected);
    }
}

#[test]
fn test_check_meta_headers_line_367() {
    let signed = BTreeMap::from([
        ("X-Amz-Meta-Test".to_string(), vec!["test".to_string()]),
        ("X-Amz-Meta-Extension".to_string(), vec!["png".to_string()]),
        ("X-Amz-Meta-Name".to_string(), vec!["imagepng".to_string()]),
    ]);

    let mut req = new_test_request("PUT", "http://play.min.io:9000", 0, None).expect("request");
    req.set_header("X-Amz-Meta-Test", "test");
    req.set_header("X-Amz-Meta-Extension", "png");
    req.set_header("X-Amz-Meta-Name", "imagepng");
    assert_eq!(check_meta_headers(&signed, &req), ApiErrorCode::None);

    req.set_header("X-Amz-Meta-Clone", "fail");
    assert_eq!(
        check_meta_headers(&signed, &req),
        ApiErrorCode::UnsignedHeaders
    );

    let req = new_test_request(
        "PUT",
        "http://play.min.io:9000?x-amz-meta-test=test&x-amz-meta-extension=png&x-amz-meta-name=imagepng",
        0,
        None,
    )
    .expect("request");
    assert_eq!(check_meta_headers(&signed, &req), ApiErrorCode::None);
}
