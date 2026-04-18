use std::collections::BTreeMap;

use base64::Engine;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use url::Url;

use crate::cmd::{ApiErrorCode, Credentials, TestRequest};

type HmacSha1 = Hmac<Sha1>;

pub const SIGN_V2_ALGORITHM: &str = "AWS";

pub const RESOURCE_LIST: &[&str] = &[
    "acl",
    "delete",
    "encryption",
    "lifecycle",
    "location",
    "logging",
    "notification",
    "partNumber",
    "policy",
    "replication",
    "requestPayment",
    "response-cache-control",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-content-type",
    "response-expires",
    "uploadId",
    "uploads",
    "versionId",
    "versioning",
    "versions",
];

pub fn resource_list() -> &'static [&'static str] {
    RESOURCE_LIST
}

fn canonical_resource(url: &Url) -> String {
    let mut out = url.path().to_string();
    let mut params = url
        .query_pairs()
        .filter(|(key, _)| key != "Signature" && RESOURCE_LIST.contains(&key.as_ref()))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    params.sort();
    if !params.is_empty() {
        out.push('?');
        for (index, (key, value)) in params.iter().enumerate() {
            if index > 0 {
                out.push('&');
            }
            out.push_str(key);
            if !value.is_empty() {
                out.push('=');
                out.push_str(value);
            }
        }
    }
    out
}

fn canonical_amz_headers(req: &TestRequest) -> String {
    let mut headers = req
        .headers
        .iter()
        .filter(|(key, _)| key.starts_with("x-amz-"))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_string()))
        .collect::<Vec<_>>();
    headers.sort();
    headers
        .into_iter()
        .map(|(key, value)| format!("{key}:{value}\n"))
        .collect::<String>()
}

fn signature_v2_payload(req: &TestRequest, date_value: &str) -> String {
    let content_md5 = req.header("content-md5").unwrap_or_default();
    let content_type = req.header("content-type").unwrap_or_default();
    let date = if req.header("x-amz-date").is_some() {
        ""
    } else {
        date_value
    };
    format!(
        "{}\n{}\n{}\n{}\n{}{}",
        req.method,
        content_md5,
        content_type,
        date,
        canonical_amz_headers(req),
        canonical_resource(&req.url),
    )
}

pub fn calculate_signature_v2(payload: &str, secret_key: &str) -> String {
    let mut mac = <HmacSha1 as Mac>::new_from_slice(secret_key.as_bytes()).expect("hmac");
    mac.update(payload.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn presign_payload(req: &TestRequest, expires: i64) -> String {
    format!(
        "{}\n{}\n{}",
        req.method,
        canonical_resource(&req.url),
        expires
    )
}

pub fn presign_v2(req: &mut TestRequest, access_key: &str, secret_key: &str, expires: i64) {
    req.set_query_value("AWSAccessKeyId", access_key);
    req.set_query_value("Expires", &expires.to_string());
    let signature = calculate_signature_v2(&presign_payload(req, expires), secret_key);
    req.set_query_value("Signature", &signature);
}

pub fn sign_request_v2_standard(
    req: &mut TestRequest,
    access_key: &str,
    secret_key: &str,
    when: DateTime<Utc>,
) -> Result<(), String> {
    let date = when.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    req.set_header("date", &date);
    let payload = signature_v2_payload(req, &date);
    let signature = calculate_signature_v2(&payload, secret_key);
    req.set_header("authorization", &format!("AWS {access_key}:{signature}"));
    Ok(())
}

pub fn does_presign_v2_signature_match(
    req: &TestRequest,
    active: &Credentials,
    now_unix: i64,
) -> ApiErrorCode {
    let Some(access_key) = req.query_value("AWSAccessKeyId") else {
        return ApiErrorCode::InvalidQueryParams;
    };
    let Some(expires_raw) = req.query_value("Expires") else {
        return ApiErrorCode::InvalidQueryParams;
    };
    let Some(signature) = req.query_value("Signature") else {
        return ApiErrorCode::InvalidQueryParams;
    };
    if access_key != active.access_key {
        return ApiErrorCode::InvalidAccessKeyID;
    }
    let Ok(expires) = expires_raw.parse::<i64>() else {
        return ApiErrorCode::MalformedExpires;
    };
    if now_unix > expires {
        return ApiErrorCode::ExpiredPresignRequest;
    }
    let expected = calculate_signature_v2(&presign_payload(req, expires), &active.secret_key);
    if signature != expected {
        ApiErrorCode::SignatureDoesNotMatch
    } else {
        ApiErrorCode::None
    }
}

pub fn validate_v2_auth_header(auth_header: &str, active: &Credentials) -> (String, ApiErrorCode) {
    if auth_header.is_empty() {
        return (String::new(), ApiErrorCode::AuthHeaderEmpty);
    }
    if auth_header == SIGN_V2_ALGORITHM {
        return (String::new(), ApiErrorCode::MissingFields);
    }
    let Some(payload) = auth_header.strip_prefix(&format!("{SIGN_V2_ALGORITHM} ")) else {
        return (String::new(), ApiErrorCode::SignatureVersionNotSupported);
    };
    let Some((access_key, signature)) = payload.split_once(':') else {
        return (String::new(), ApiErrorCode::MissingFields);
    };
    if signature.is_empty() {
        return (String::new(), ApiErrorCode::MissingFields);
    }
    if access_key != active.access_key {
        return (access_key.to_string(), ApiErrorCode::InvalidAccessKeyID);
    }
    (access_key.to_string(), ApiErrorCode::None)
}

pub fn does_signature_v2_match(req: &TestRequest, active: &Credentials) -> (String, ApiErrorCode) {
    let auth_header = req.header("authorization").unwrap_or_default();
    let (access_key, err) = validate_v2_auth_header(auth_header, active);
    if err != ApiErrorCode::None {
        return (access_key, err);
    }
    let Some(signature) = auth_header
        .strip_prefix(&format!("{SIGN_V2_ALGORITHM} "))
        .and_then(|value| value.split_once(':'))
        .map(|(_, signature)| signature)
    else {
        return (access_key, ApiErrorCode::MissingFields);
    };

    let date_value = req
        .header("date")
        .or_else(|| req.header("x-amz-date"))
        .unwrap_or_default();
    if date_value.is_empty() {
        return (access_key, ApiErrorCode::AccessDenied);
    }

    let expected =
        calculate_signature_v2(&signature_v2_payload(req, date_value), &active.secret_key);
    if expected != signature {
        return (access_key, ApiErrorCode::SignatureDoesNotMatch);
    }
    (access_key, ApiErrorCode::None)
}

pub fn does_policy_signature_v2_match(
    form_values: &BTreeMap<String, String>,
    active: &Credentials,
) -> (String, ApiErrorCode) {
    let access_key = form_values
        .get("Awsaccesskeyid")
        .cloned()
        .unwrap_or_default();
    if access_key != active.access_key {
        return (access_key, ApiErrorCode::InvalidAccessKeyID);
    }
    let policy = form_values.get("Policy").cloned().unwrap_or_default();
    let signature = form_values.get("Signature").cloned().unwrap_or_default();
    let expected = calculate_signature_v2(&policy, &active.secret_key);
    if signature != expected {
        return (access_key, ApiErrorCode::SignatureDoesNotMatch);
    }
    (access_key, ApiErrorCode::None)
}
