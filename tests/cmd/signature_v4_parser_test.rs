use std::collections::BTreeMap;

use chrono::Utc;
use minio_rust::cmd::{
    does_v4_presign_params_exist, parse_credential_header, parse_pre_sign_v4, parse_sign_v4,
    parse_signature, parse_signed_header, ApiErrorCode, CredentialHeader, CredentialScope,
    ISO8601_FORMAT, SIGN_V4_ALGORITHM,
};

pub const SOURCE_FILE: &str = "cmd/signature-v4-parser_test.go";

fn generate_credential_str(
    access_key: &str,
    date: &str,
    region: &str,
    service: &str,
    request_version: &str,
) -> String {
    format!("Credential={access_key}/{date}/{region}/{service}/{request_version}")
}

fn expected_credential(
    access_key: &str,
    date: &str,
    region: &str,
    service: &str,
    request_version: &str,
) -> CredentialHeader {
    CredentialHeader {
        access_key: access_key.to_string(),
        scope: CredentialScope {
            date: date.to_string(),
            region: region.to_string(),
            service: service.to_string(),
            request: request_version.to_string(),
        },
    }
}

fn query_pairs(values: &[(&str, String)]) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

#[test]
fn test_parse_credential_header_line_90() {
    let sample_time = Utc::now().format("%Y%m%d").to_string();
    let cases = vec![
        ("Credential".to_string(), ApiErrorCode::MissingFields, None),
        ("Cred=".to_string(), ApiErrorCode::MissingCredTag, None),
        (
            "Credential=abc".to_string(),
            ApiErrorCode::CredMalformed,
            None,
        ),
        (
            generate_credential_str("^#", &sample_time, "ABCD", "ABCD", "ABCD"),
            ApiErrorCode::InvalidAccessKeyID,
            None,
        ),
        (
            generate_credential_str(
                "Z7IXGOO6BZ0REAN1Q26I",
                &Utc::now().to_rfc3339(),
                "ABCD",
                "ABCD",
                "ABCD",
            ),
            ApiErrorCode::MalformedCredentialDate,
            None,
        ),
        (
            generate_credential_str(
                "Z7IXGOO6BZ0REAN1Q26I",
                &sample_time,
                "us-west-1",
                "ABCD",
                "ABCD",
            ),
            ApiErrorCode::InvalidServiceS3,
            None,
        ),
        (
            generate_credential_str(
                "Z7IXGOO6BZ0REAN1Q26I",
                &sample_time,
                "us-west-2",
                "s3",
                "aws4_request",
            ),
            ApiErrorCode::AuthorizationHeaderMalformed,
            None,
        ),
        (
            generate_credential_str(
                "Z7IXGOO6BZ0REAN1Q26I",
                &sample_time,
                "us-west-1",
                "s3",
                "ABCD",
            ),
            ApiErrorCode::InvalidRequestVersion,
            None,
        ),
        (
            generate_credential_str(
                "Z7IXGOO6BZ0REAN1Q26I",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request",
            ),
            ApiErrorCode::None,
            Some(expected_credential(
                "Z7IXGOO6BZ0REAN1Q26I",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request",
            )),
        ),
        (
            generate_credential_str(
                "LOCALKEY/DEV/1",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request",
            ),
            ApiErrorCode::None,
            Some(expected_credential(
                "LOCALKEY/DEV/1",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request",
            )),
        ),
        (
            generate_credential_str(
                "LOCALKEY/DEV/1=",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request",
            ),
            ApiErrorCode::None,
            Some(expected_credential(
                "LOCALKEY/DEV/1=",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request",
            )),
        ),
        (
            generate_credential_str(
                "Z7IXGOO6BZ0REAN1Q26I",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request/",
            ),
            ApiErrorCode::None,
            Some(expected_credential(
                "Z7IXGOO6BZ0REAN1Q26I",
                &sample_time,
                "us-west-1",
                "s3",
                "aws4_request",
            )),
        ),
    ];

    for (input, expected_err, expected_credential) in cases {
        let (actual, err) = parse_credential_header(&input, "us-west-1", "s3");
        assert_eq!(err, expected_err, "{input}");
        if err == ApiErrorCode::None {
            assert_eq!(actual, expected_credential.expect("credential"));
        }
    }
}

#[test]
fn test_parse_signature_line_273() {
    let cases = [
        ("Signature", "", ApiErrorCode::MissingFields),
        ("Signature=", "", ApiErrorCode::MissingFields),
        ("Sign=", "", ApiErrorCode::MissingSignTag),
        ("Signature=abcd", "abcd", ApiErrorCode::None),
    ];

    for (input, expected_signature, expected_err) in cases {
        let (actual, err) = parse_signature(input);
        assert_eq!(err, expected_err);
        if err == ApiErrorCode::None {
            assert_eq!(actual, expected_signature);
        }
    }
}

#[test]
fn test_parse_signed_headers_line_324() {
    let cases = [
        (
            "SignedHeaders",
            Vec::<String>::new(),
            ApiErrorCode::MissingFields,
        ),
        (
            "Sign=",
            Vec::<String>::new(),
            ApiErrorCode::MissingSignHeadersTag,
        ),
        (
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date",
            vec![
                "host".to_string(),
                "x-amz-content-sha256".to_string(),
                "x-amz-date".to_string(),
            ],
            ApiErrorCode::None,
        ),
    ];

    for (input, expected_headers, expected_err) in cases {
        let (actual, err) = parse_signed_header(input);
        assert_eq!(err, expected_err);
        if err == ApiErrorCode::None {
            assert_eq!(actual, expected_headers);
        }
    }
}

#[test]
fn test_parse_sign_v4_line_368() {
    let sample_time = Utc::now().format("%Y%m%d").to_string();
    let cases = vec![
        ("".to_string(), ApiErrorCode::AuthHeaderEmpty, None),
        (
            "no-singv4AlgorithmPrefix".to_string(),
            ApiErrorCode::SignatureVersionNotSupported,
            None,
        ),
        (SIGN_V4_ALGORITHM.to_string(), ApiErrorCode::MissingFields, None),
        (
            format!("{SIGN_V4_ALGORITHM} Cred=,a,b"),
            ApiErrorCode::MissingCredTag,
            None,
        ),
        (
            format!(
                "{SIGN_V4_ALGORITHM} {},SignIncorrectHeader=,b",
                generate_credential_str("Z7IXGOO6BZ0REAN1Q26I", &sample_time, "us-west-1", "s3", "aws4_request")
            ),
            ApiErrorCode::MissingSignHeadersTag,
            None,
        ),
        (
            format!(
                "{SIGN_V4_ALGORITHM} {},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Sign=",
                generate_credential_str("Z7IXGOO6BZ0REAN1Q26I", &sample_time, "us-west-1", "s3", "aws4_request")
            ),
            ApiErrorCode::MissingSignTag,
            None,
        ),
        (
            format!(
                "{SIGN_V4_ALGORITHM} {},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=abcd",
                generate_credential_str("Z7IXGOO6BZ0REAN1Q26I", &sample_time, "us-west-1", "s3", "aws4_request")
            ),
            ApiErrorCode::None,
            Some(("Z7IXGOO6BZ0REAN1Q26I".to_string(), "abcd".to_string())),
        ),
        (
            format!(
                "{SIGN_V4_ALGORITHM} {},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=abcd",
                generate_credential_str("access key", &sample_time, "us-west-1", "s3", "aws4_request")
            ),
            ApiErrorCode::None,
            Some(("access key".to_string(), "abcd".to_string())),
        ),
    ];

    for (input, expected_err, expected) in cases {
        let (actual, err) = parse_sign_v4(&input, "", "s3");
        assert_eq!(err, expected_err, "{input}");
        if let Some((expected_access_key, expected_signature)) = expected {
            assert_eq!(actual.credential.access_key, expected_access_key);
            assert_eq!(actual.signature, expected_signature);
            assert_eq!(
                actual.signed_headers,
                vec![
                    "host".to_string(),
                    "x-amz-content-sha256".to_string(),
                    "x-amz-date".to_string()
                ]
            );
        }
    }
}

#[test]
fn test_does_v4_presign_params_exist_line_538() {
    let cases = vec![
        (
            vec![
                ("X-Amz-Algorithm", ""),
                ("X-Amz-Credential", ""),
                ("X-Amz-Signature", ""),
                ("X-Amz-Date", ""),
                ("X-Amz-SignedHeaders", ""),
                ("X-Amz-Expires", ""),
            ],
            ApiErrorCode::None,
        ),
        (
            vec![
                ("X-Amz-Credential", ""),
                ("X-Amz-Signature", ""),
                ("X-Amz-Date", ""),
                ("X-Amz-SignedHeaders", ""),
                ("X-Amz-Expires", ""),
            ],
            ApiErrorCode::InvalidQueryParams,
        ),
        (
            vec![
                ("X-Amz-Algorithm", ""),
                ("X-Amz-Signature", ""),
                ("X-Amz-Date", ""),
                ("X-Amz-SignedHeaders", ""),
                ("X-Amz-Expires", ""),
            ],
            ApiErrorCode::InvalidQueryParams,
        ),
    ];
    for (pairs, expected_err) in cases {
        let query = pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(does_v4_presign_params_exist(&query), expected_err);
    }
}

#[test]
fn test_parse_pre_sign_v4_line_647() {
    let query_time = Utc::now();
    let sample_time = Utc::now().format("%Y%m%d").to_string();
    let query_time_string = query_time.format(ISO8601_FORMAT).to_string();
    let credential = format!("Z7IXGOO6BZ0REAN1Q26I/{sample_time}/us-west-1/s3/aws4_request");

    let cases = vec![
        (
            query_pairs(&[
                ("X-Amz-Algorithm", "".to_string()),
                ("X-Amz-Credential", "".to_string()),
                ("X-Amz-Signature", "".to_string()),
                ("X-Amz-Expires", "".to_string()),
            ]),
            ApiErrorCode::InvalidQueryParams,
            None,
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", "InvalidValue".to_string()),
                ("X-Amz-Credential", "".to_string()),
                ("X-Amz-Signature", "".to_string()),
                ("X-Amz-Date", "".to_string()),
                ("X-Amz-SignedHeaders", "".to_string()),
                ("X-Amz-Expires", "".to_string()),
            ]),
            ApiErrorCode::InvalidQuerySignatureAlgo,
            None,
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", SIGN_V4_ALGORITHM.to_string()),
                ("X-Amz-Credential", "invalid-credential".to_string()),
                ("X-Amz-Signature", "".to_string()),
                ("X-Amz-Date", "".to_string()),
                ("X-Amz-SignedHeaders", "".to_string()),
                ("X-Amz-Expires", "".to_string()),
            ]),
            ApiErrorCode::CredMalformed,
            None,
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", SIGN_V4_ALGORITHM.to_string()),
                ("X-Amz-Credential", credential.clone()),
                ("X-Amz-Date", "invalid-time".to_string()),
                ("X-Amz-SignedHeaders", "".to_string()),
                ("X-Amz-Expires", "".to_string()),
                ("X-Amz-Signature", "".to_string()),
            ]),
            ApiErrorCode::MalformedPresignedDate,
            None,
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", SIGN_V4_ALGORITHM.to_string()),
                ("X-Amz-Credential", credential.clone()),
                ("X-Amz-Date", query_time_string.clone()),
                ("X-Amz-Expires", "MalformedExpiry".to_string()),
                ("X-Amz-SignedHeaders", "".to_string()),
                ("X-Amz-Signature", "".to_string()),
            ]),
            ApiErrorCode::MalformedExpires,
            None,
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", SIGN_V4_ALGORITHM.to_string()),
                ("X-Amz-Credential", credential.clone()),
                ("X-Amz-Date", query_time_string.clone()),
                ("X-Amz-Expires", "-1".to_string()),
                ("X-Amz-Signature", "abcd".to_string()),
                (
                    "X-Amz-SignedHeaders",
                    "host;x-amz-content-sha256;x-amz-date".to_string(),
                ),
            ]),
            ApiErrorCode::NegativeExpires,
            None,
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", SIGN_V4_ALGORITHM.to_string()),
                ("X-Amz-Credential", credential.clone()),
                ("X-Amz-Date", query_time_string.clone()),
                ("X-Amz-Expires", "100".to_string()),
                ("X-Amz-Signature", "abcd".to_string()),
                ("X-Amz-SignedHeaders", "".to_string()),
            ]),
            ApiErrorCode::MissingFields,
            None,
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", SIGN_V4_ALGORITHM.to_string()),
                ("X-Amz-Credential", credential.clone()),
                ("X-Amz-Date", query_time_string.clone()),
                ("X-Amz-Expires", "100".to_string()),
                ("X-Amz-Signature", "abcd".to_string()),
                (
                    "X-Amz-SignedHeaders",
                    "host;x-amz-content-sha256;x-amz-date".to_string(),
                ),
            ]),
            ApiErrorCode::None,
            Some((
                "Z7IXGOO6BZ0REAN1Q26I".to_string(),
                "abcd".to_string(),
                100_i64,
            )),
        ),
        (
            query_pairs(&[
                ("X-Amz-Algorithm", SIGN_V4_ALGORITHM.to_string()),
                ("X-Amz-Credential", credential),
                ("X-Amz-Date", query_time_string.clone()),
                ("X-Amz-Expires", "605000".to_string()),
                ("X-Amz-Signature", "abcd".to_string()),
                (
                    "X-Amz-SignedHeaders",
                    "host;x-amz-content-sha256;x-amz-date".to_string(),
                ),
            ]),
            ApiErrorCode::MaximumExpires,
            None,
        ),
    ];

    for (query, expected_err, expected) in cases {
        let (actual, err) = parse_pre_sign_v4(&query, "", "s3");
        assert_eq!(err, expected_err);
        if let Some((expected_access_key, expected_signature, expected_expires)) = expected {
            let actual = actual.expect("presign");
            assert_eq!(
                actual.sign_values.credential.access_key,
                expected_access_key
            );
            assert_eq!(actual.sign_values.signature, expected_signature);
            assert_eq!(actual.expires, expected_expires);
            assert_eq!(
                actual.sign_values.signed_headers,
                vec![
                    "host".to_string(),
                    "x-amz-content-sha256".to_string(),
                    "x-amz-date".to_string()
                ]
            );
            assert_eq!(
                actual.date.format(ISO8601_FORMAT).to_string(),
                query_time.format(ISO8601_FORMAT).to_string()
            );
        }
    }
}
