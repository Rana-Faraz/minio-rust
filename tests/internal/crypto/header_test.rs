use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use minio_rust::internal::crypto::{
    self, HeaderMap, AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS, AMZ_META_UNENCRYPTED_CONTENT_LENGTH,
    AMZ_META_UNENCRYPTED_CONTENT_MD5, AMZ_SERVER_SIDE_ENCRYPTION,
    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM,
    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
    AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, ARN_PREFIX, ERR_CUSTOMER_KEY_MD5_MISMATCH,
    ERR_INVALID_CUSTOMER_ALGORITHM, ERR_INVALID_CUSTOMER_KEY, ERR_INVALID_ENCRYPTION_KEY_ID,
    ERR_INVALID_ENCRYPTION_METHOD, ERR_MISSING_CUSTOMER_KEY, ERR_MISSING_CUSTOMER_KEY_MD5, S3,
    S3_KMS, SSEC, SSE_COPY,
};

pub const SOURCE_FILE: &str = "internal/crypto/header_test.go";

#[test]
fn test_is_requested() {
    for headers in kms_requested_cases() {
        let (_, requested) = crypto::is_requested(&headers.0);
        assert_eq!(crypto::requested(&headers.0), requested);
        assert_eq!(requested && S3_KMS.is_requested(&headers.0), headers.1);
    }
    for headers in s3_requested_cases() {
        let (_, requested) = crypto::is_requested(&headers.0);
        assert_eq!(crypto::requested(&headers.0), requested);
        assert_eq!(requested && S3.is_requested(&headers.0), headers.1);
    }
    for headers in ssec_requested_cases() {
        let (_, requested) = crypto::is_requested(&headers.0);
        assert_eq!(crypto::requested(&headers.0), requested);
        assert_eq!(requested && SSEC.is_requested(&headers.0), headers.1);
    }
}

#[test]
fn test_kms_is_requested() {
    for (headers, expected) in kms_requested_cases() {
        assert_eq!(S3_KMS.is_requested(&headers), expected);
    }
}

#[test]
fn test_kms_parse_http() {
    let cases = [
        (HeaderMap::new(), true),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS)]),
            false,
        ),
        (
            headers([
                (AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, "s3-007-293847485-724784"),
            ]),
            false,
        ),
        (
            headers([
                (AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
                    &BASE64_STANDARD.encode("{}"),
                ),
            ]),
            false,
        ),
        (
            headers([
                (AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_AES),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, "s3-007-293847485-724784"),
            ]),
            true,
        ),
        (
            headers([
                (AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
                    &BASE64_STANDARD.encode(r#"{"bucket":"some-bucket""#),
                ),
            ]),
            true,
        ),
    ];

    for (idx, (headers, should_fail)) in cases.into_iter().enumerate() {
        let result = S3_KMS.parse_http(&headers);
        assert_eq!(result.is_err(), should_fail, "case {}", idx);
    }

    let kms_headers = headers([
        (AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS),
        (
            AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
            &format!("{ARN_PREFIX}my-key"),
        ),
        (
            AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
            &BASE64_STANDARD.encode(r#"{"bucket":"some-bucket"}"#),
        ),
    ]);
    let (key_id, context) = S3_KMS
        .parse_http(&kms_headers)
        .expect("kms parse should succeed");
    assert_eq!(key_id, "my-key");
    assert_eq!(
        context.get("bucket").map(String::as_str),
        Some("some-bucket")
    );

    let invalid_kms_headers = headers([
        (AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS),
        (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, " my-key "),
    ]);
    let err = S3_KMS
        .parse_http(&invalid_kms_headers)
        .expect_err("spaces should be rejected");
    assert_eq!(err, ERR_INVALID_ENCRYPTION_KEY_ID);
}

#[test]
fn test_s3_is_requested() {
    for (headers, expected) in s3_requested_cases() {
        assert_eq!(S3.is_requested(&headers), expected);
    }
}

#[test]
fn test_s3_parse() {
    let cases = [
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_AES)]),
            None,
        ),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, "AES-256")]),
            Some(ERR_INVALID_ENCRYPTION_METHOD),
        ),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, "")]),
            Some(ERR_INVALID_ENCRYPTION_METHOD),
        ),
        (HeaderMap::new(), Some(ERR_INVALID_ENCRYPTION_METHOD)),
    ];

    for (idx, (headers, expected)) in cases.into_iter().enumerate() {
        let err = S3.parse_http(&headers).err();
        assert_eq!(err, expected, "case {}", idx);
    }
}

#[test]
fn test_ssec_is_requested() {
    for (headers, expected) in ssec_requested_cases() {
        assert_eq!(SSEC.is_requested(&headers), expected);
    }
}

#[test]
fn test_ssecopy_is_requested() {
    let cases = [
        (HeaderMap::new(), false),
        (
            headers([(
                AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM,
                AMZ_ENCRYPTION_AES,
            )]),
            true,
        ),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, CUSTOMER_KEY)]),
            true,
        ),
        (
            headers([(
                AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
                CUSTOMER_KEY_MD5,
            )]),
            true,
        ),
        (
            headers([
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                    AMZ_ENCRYPTION_AES,
                ),
                (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, CUSTOMER_KEY),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
            ]),
            false,
        ),
    ];

    for (headers, expected) in cases {
        assert_eq!(SSE_COPY.is_requested(&headers), expected);
    }
}

#[test]
fn test_ssec_parse() {
    let cases = customer_parse_cases(false);
    for (idx, (headers, expected_err)) in cases.into_iter().enumerate() {
        let key = SSEC.parse_http(&headers);
        assert_eq!(key.as_ref().err(), expected_err.as_ref(), "case {}", idx);
        if expected_err.is_none() {
            assert_ne!(key.expect("valid key"), [0u8; 32], "case {}", idx);
        }
    }
}

#[test]
fn test_ssecopy_parse() {
    let cases = customer_parse_cases(true);
    for (idx, (headers, expected_err)) in cases.into_iter().enumerate() {
        let key = SSE_COPY.parse_http(&headers);
        assert_eq!(key.as_ref().err(), expected_err.as_ref(), "case {}", idx);
        if expected_err.is_none() {
            assert_ne!(key.expect("valid key"), [0u8; 32], "case {}", idx);
        }
    }
}

#[test]
fn test_remove_sensitive_headers() {
    let cases = [
        (
            headers([
                (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, ""),
                (AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, ""),
            ]),
            HeaderMap::new(),
        ),
        (
            headers([
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                    AMZ_ENCRYPTION_AES,
                ),
                (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, CUSTOMER_KEY),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
            ]),
            headers([
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                    AMZ_ENCRYPTION_AES,
                ),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
            ]),
        ),
        (
            headers([
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                    AMZ_ENCRYPTION_AES,
                ),
                (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, CUSTOMER_KEY),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
                (AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, CUSTOMER_KEY),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
            ]),
            headers([
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                    AMZ_ENCRYPTION_AES,
                ),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
            ]),
        ),
        (
            headers([
                (AMZ_META_UNENCRYPTED_CONTENT_MD5, "value"),
                (AMZ_META_UNENCRYPTED_CONTENT_LENGTH, "value"),
                ("X-Amz-Meta-Test-1", "Test-1"),
            ]),
            headers([("X-Amz-Meta-Test-1", "Test-1")]),
        ),
    ];

    for (idx, (mut headers, expected)) in cases.into_iter().enumerate() {
        let mut metadata = headers
            .keys()
            .map(|k| (k.clone(), String::new()))
            .collect::<HashMap<_, _>>();
        crypto::remove_sensitive_headers(&mut headers);
        assert_eq!(headers, expected, "case {}", idx);
        crypto::remove_sensitive_entries(&mut metadata);
        assert_eq!(
            metadata.keys().collect::<Vec<_>>().len(),
            expected.keys().collect::<Vec<_>>().len(),
            "case {}",
            idx
        );
        for key in expected.keys() {
            assert!(
                metadata.contains_key(key),
                "case {} missing key {}",
                idx,
                key
            );
        }
    }
}

fn kms_requested_cases() -> Vec<(HeaderMap, bool)> {
    vec![
        (HeaderMap::new(), false),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS)]),
            true,
        ),
        (
            headers([(
                AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
                "0839-9047947-844842874-481",
            )]),
            true,
        ),
        (
            headers([(
                AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
                "7PpPLAK26ONlVUGOWlusfg==",
            )]),
            true,
        ),
        (
            headers([
                (AMZ_SERVER_SIDE_ENCRYPTION, ""),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, ""),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT, ""),
            ]),
            true,
        ),
        (
            headers([
                (AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_AES),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID, ""),
            ]),
            true,
        ),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_AES)]),
            false,
        ),
    ]
}

fn s3_requested_cases() -> Vec<(HeaderMap, bool)> {
    vec![
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_AES)]),
            true,
        ),
        (headers([(AMZ_SERVER_SIDE_ENCRYPTION, "AES-256")]), true),
        (headers([(AMZ_SERVER_SIDE_ENCRYPTION, "")]), true),
        (HeaderMap::new(), false),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION, AMZ_ENCRYPTION_KMS)]),
            false,
        ),
    ]
}

fn ssec_requested_cases() -> Vec<(HeaderMap, bool)> {
    vec![
        (HeaderMap::new(), false),
        (
            headers([(
                AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
                AMZ_ENCRYPTION_AES,
            )]),
            true,
        ),
        (
            headers([(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, CUSTOMER_KEY)]),
            true,
        ),
        (
            headers([(
                AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
                CUSTOMER_KEY_MD5,
            )]),
            true,
        ),
        (
            headers([
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM,
                    AMZ_ENCRYPTION_AES,
                ),
                (AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, CUSTOMER_KEY),
                (
                    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
                    CUSTOMER_KEY_MD5,
                ),
            ]),
            false,
        ),
    ]
}

fn customer_parse_cases(copy: bool) -> Vec<(HeaderMap, Option<crypto::CryptoError>)> {
    let algo = if copy {
        AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM
    } else {
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM
    };
    let key = if copy {
        AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY
    } else {
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY
    };
    let key_md5 = if copy {
        AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5
    } else {
        AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5
    };

    vec![
        (
            headers([
                (algo, AMZ_ENCRYPTION_AES),
                (key, CUSTOMER_KEY),
                (key_md5, CUSTOMER_KEY_MD5),
            ]),
            None,
        ),
        (
            headers([
                (algo, "AES-256"),
                (key, CUSTOMER_KEY),
                (key_md5, CUSTOMER_KEY_MD5),
            ]),
            Some(ERR_INVALID_CUSTOMER_ALGORITHM),
        ),
        (
            headers([
                (algo, AMZ_ENCRYPTION_AES),
                (key, ""),
                (key_md5, CUSTOMER_KEY_MD5),
            ]),
            Some(ERR_MISSING_CUSTOMER_KEY),
        ),
        (
            headers([
                (algo, AMZ_ENCRYPTION_AES),
                (key, "bad.key"),
                (key_md5, CUSTOMER_KEY_MD5),
            ]),
            Some(ERR_INVALID_CUSTOMER_KEY),
        ),
        (
            headers([
                (algo, AMZ_ENCRYPTION_AES),
                (key, CUSTOMER_KEY),
                (key_md5, ""),
            ]),
            Some(ERR_MISSING_CUSTOMER_KEY_MD5),
        ),
        (
            headers([
                (algo, AMZ_ENCRYPTION_AES),
                (key, "DzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ="),
                (key_md5, CUSTOMER_KEY_MD5),
            ]),
            Some(ERR_CUSTOMER_KEY_MD5_MISMATCH),
        ),
        (
            headers([
                (algo, AMZ_ENCRYPTION_AES),
                (key, CUSTOMER_KEY),
                (key_md5, ".7PpPLAK26ONlVUGOWlusfg=="),
            ]),
            Some(ERR_CUSTOMER_KEY_MD5_MISMATCH),
        ),
    ]
}

fn headers<const N: usize>(pairs: [(&str, &str); N]) -> HeaderMap {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

const CUSTOMER_KEY: &str = "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=";
const CUSTOMER_KEY_MD5: &str = "7PpPLAK26ONlVUGOWlusfg==";
