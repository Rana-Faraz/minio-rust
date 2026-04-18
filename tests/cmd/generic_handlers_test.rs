// Rust test snapshot derived from cmd/generic-handlers_test.go.

use std::collections::BTreeMap;

use minio_rust::cmd::{
    contains_reserved_metadata, guess_is_rpc_req, has_bad_path_component,
    is_http_header_size_too_large, validate_sse_tls_request, GenericRequest,
};
use minio_rust::internal::crypto;

pub const SOURCE_FILE: &str = "cmd/generic-handlers_test.go";

fn generate_header(size: usize, usersize: usize) -> BTreeMap<String, String> {
    let mut header = BTreeMap::new();
    for i in 0..size {
        header.insert(i.to_string(), String::new());
    }
    let mut userlength = 0usize;
    let mut i = 0usize;
    while userlength < usersize {
        let key = format!("x-amz-meta-{i}");
        userlength += key.len();
        header.insert(key, String::new());
        i += 1;
    }
    header
}

#[test]
fn test_guess_is_rpc_line_34() {
    assert!(!guess_is_rpc_req(None));
    assert!(guess_is_rpc_req(Some(&GenericRequest {
        proto: "HTTP/1.0".to_string(),
        method: "POST".to_string(),
        path: "/minio/lock".to_string(),
        ..GenericRequest::default()
    })));
    assert!(guess_is_rpc_req(Some(&GenericRequest {
        proto: "HTTP/1.1".to_string(),
        method: "GET".to_string(),
        path: "/minio/lock".to_string(),
        ..GenericRequest::default()
    })));
    assert!(guess_is_rpc_req(Some(&GenericRequest {
        path: "/minio/grid".to_string(),
        ..GenericRequest::default()
    })));
    assert!(guess_is_rpc_req(Some(&GenericRequest {
        path: "/minio/grid/lock".to_string(),
        ..GenericRequest::default()
    })));
}

#[test]
fn test_is_httpheader_size_too_large_line_104() {
    let cases = [
        (generate_header(0, 0), false),
        (generate_header(1024, 0), false),
        (generate_header(2048, 0), false),
        (generate_header(8 * 1024 + 1, 0), true),
        (generate_header(0, 1024), false),
        (generate_header(0, 2048), true),
        (generate_header(0, 2049), true),
    ];
    for (idx, (header, should_fail)) in cases.into_iter().enumerate() {
        assert_eq!(
            is_http_header_size_too_large(&header),
            should_fail,
            "case={idx}"
        );
    }
}

#[test]
fn test_contains_reserved_metadata_line_137() {
    let cases = [
        (
            BTreeMap::from([("X-Minio-Key".to_string(), "value".to_string())]),
            false,
        ),
        (
            BTreeMap::from([(crypto::META_IV.to_string(), "iv".to_string())]),
            false,
        ),
        (
            BTreeMap::from([(
                crypto::META_ALGORITHM.to_string(),
                crypto::INSECURE_SEAL_ALGORITHM.to_string(),
            )]),
            false,
        ),
        (
            BTreeMap::from([(crypto::META_SEALED_KEY_SSEC.to_string(), "mac".to_string())]),
            false,
        ),
        (
            BTreeMap::from([("X-Minio-Internal-Key".to_string(), "value".to_string())]),
            true,
        ),
    ];

    for (header, should_fail) in cases {
        assert_eq!(contains_reserved_metadata(&header), should_fail);
    }
}

#[test]
fn subtest_test_contains_reserved_metadata_line_139() {
    let header = BTreeMap::from([("X-Minio-Internal-Key".to_string(), "value".to_string())]);
    assert!(contains_reserved_metadata(&header));
}

#[test]
fn test_ssetlshandler_line_162() {
    let cases = [
        (BTreeMap::new(), false, false),
        (
            BTreeMap::from([(
                crypto::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM.to_string(),
                "AES256".to_string(),
            )]),
            false,
            true,
        ),
        (
            BTreeMap::from([(
                crypto::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM.to_string(),
                "AES256".to_string(),
            )]),
            true,
            false,
        ),
        (
            BTreeMap::from([(
                crypto::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY.to_string(),
                String::new(),
            )]),
            true,
            false,
        ),
        (
            BTreeMap::from([(
                crypto::AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM.to_string(),
                String::new(),
            )]),
            false,
            true,
        ),
    ];

    for (idx, (headers, is_tls, should_fail)) in cases.into_iter().enumerate() {
        let code = validate_sse_tls_request(
            &GenericRequest {
                headers,
                tls: is_tls,
                ..GenericRequest::default()
            },
            is_tls,
        );
        match (should_fail, code) {
            (true, 200) => panic!("case={idx} should fail"),
            (false, code) if code != 200 => panic!("case={idx} unexpected code {code}"),
            _ => {}
        }
    }
}

#[test]
fn benchmark_has_bad_path_component_line_188() {
    let tests = [
        ("", false),
        (r"\a\a\ \\  \\\\\\\\", false),
        (&"a/".repeat(2000), false),
        (&(String::from("a/").repeat(2000) + "../.."), true),
    ];
    for (input, want) in tests {
        assert_eq!(has_bad_path_component(input), want);
    }
}

#[test]
fn subtest_benchmark_has_bad_path_component_tt_name_line_200() {
    assert!(has_bad_path_component("../.."));
    assert!(!has_bad_path_component("safe/path/component"));
}
