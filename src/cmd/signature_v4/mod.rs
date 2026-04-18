use std::collections::BTreeMap;

use crate::cmd::{ApiErrorCode, Credentials, TestRequest, GLOBAL_MINIO_DEFAULT_REGION};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const AWS_URI_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'+')
    .add(b'&')
    .add(b'=')
    .add(b'?');

pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const SIGN_V4_ALGORITHM: &str = "AWS4-HMAC-SHA256";
pub const ISO8601_FORMAT: &str = "%Y%m%dT%H%M%SZ";
pub const YYYYMMDD: &str = "%Y%m%d";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CredentialScope {
    pub date: String,
    pub region: String,
    pub service: String,
    pub request: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CredentialHeader {
    pub access_key: String,
    pub scope: CredentialScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SignValues {
    pub credential: CredentialHeader,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreSignValues {
    pub sign_values: SignValues,
    pub date: DateTime<Utc>,
    pub expires: i64,
}

fn valid_access_key(value: &str) -> bool {
    value.chars().count() >= 3
}

pub fn parse_credential_header(
    cred_element: &str,
    region: &str,
    service: &str,
) -> (CredentialHeader, ApiErrorCode) {
    let mut empty = CredentialHeader::default();
    let creds = cred_element.trim().splitn(2, '=').collect::<Vec<_>>();
    if creds.len() != 2 {
        return (empty, ApiErrorCode::MissingFields);
    }
    if creds[0] != "Credential" {
        return (empty, ApiErrorCode::MissingCredTag);
    }
    let elements = creds[1]
        .trim()
        .trim_end_matches('/')
        .split('/')
        .map(str::to_string)
        .collect::<Vec<_>>();
    if elements.len() < 5 {
        return (empty, ApiErrorCode::CredMalformed);
    }
    let access_key = elements[..elements.len() - 4].join("/");
    if !valid_access_key(&access_key) {
        return (empty, ApiErrorCode::InvalidAccessKeyID);
    }

    let scope = &elements[elements.len() - 4..];
    if NaiveDate::parse_from_str(&scope[0], YYYYMMDD).is_err() {
        return (empty, ApiErrorCode::MalformedCredentialDate);
    }
    let expected_region = if region.is_empty() { &scope[1] } else { region };
    if !is_valid_region(&scope[1], expected_region) {
        return (empty, ApiErrorCode::AuthorizationHeaderMalformed);
    }
    if scope[2] != service {
        return (
            empty,
            if service == "sts" {
                ApiErrorCode::InvalidServiceSTS
            } else {
                ApiErrorCode::InvalidServiceS3
            },
        );
    }
    if scope[3] != "aws4_request" {
        return (empty, ApiErrorCode::InvalidRequestVersion);
    }

    empty.access_key = access_key;
    empty.scope = CredentialScope {
        date: scope[0].clone(),
        region: scope[1].clone(),
        service: scope[2].clone(),
        request: scope[3].clone(),
    };
    (empty, ApiErrorCode::None)
}

pub fn parse_signature(sign_element: &str) -> (String, ApiErrorCode) {
    let parts = sign_element.trim().split('=').collect::<Vec<_>>();
    if parts.len() != 2 {
        return (String::new(), ApiErrorCode::MissingFields);
    }
    if parts[0] != "Signature" {
        return (String::new(), ApiErrorCode::MissingSignTag);
    }
    if parts[1].is_empty() {
        return (String::new(), ApiErrorCode::MissingFields);
    }
    (parts[1].to_string(), ApiErrorCode::None)
}

pub fn parse_signed_header(signed_hdr_element: &str) -> (Vec<String>, ApiErrorCode) {
    let parts = signed_hdr_element.trim().split('=').collect::<Vec<_>>();
    if parts.len() != 2 {
        return (Vec::new(), ApiErrorCode::MissingFields);
    }
    if parts[0] != "SignedHeaders" {
        return (Vec::new(), ApiErrorCode::MissingSignHeadersTag);
    }
    if parts[1].is_empty() {
        return (Vec::new(), ApiErrorCode::MissingFields);
    }
    (
        parts[1].split(';').map(str::to_string).collect(),
        ApiErrorCode::None,
    )
}

pub fn parse_sign_v4(v4_auth: &str, region: &str, service: &str) -> (SignValues, ApiErrorCode) {
    let empty = SignValues::default();
    if v4_auth.is_empty() {
        return (empty, ApiErrorCode::AuthHeaderEmpty);
    }
    if !v4_auth.starts_with(SIGN_V4_ALGORITHM) {
        return (empty, ApiErrorCode::SignatureVersionNotSupported);
    }

    let cred_element = v4_auth
        .trim()
        .split(',')
        .next()
        .unwrap_or_default()
        .trim_start_matches(SIGN_V4_ALGORITHM)
        .trim()
        .to_string();
    let normalized = v4_auth.replace(' ', "");
    let auth = normalized.trim_start_matches(SIGN_V4_ALGORITHM);
    let fields = auth.trim().split(',').collect::<Vec<_>>();
    if fields.len() != 3 {
        return (empty, ApiErrorCode::MissingFields);
    }

    let (credential, err) = parse_credential_header(&cred_element, region, service);
    if err != ApiErrorCode::None {
        return (empty, err);
    }
    let (signed_headers, err) = parse_signed_header(fields[1]);
    if err != ApiErrorCode::None {
        return (empty, err);
    }
    let (signature, err) = parse_signature(fields[2]);
    if err != ApiErrorCode::None {
        return (empty, err);
    }

    (
        SignValues {
            credential,
            signed_headers,
            signature,
        },
        ApiErrorCode::None,
    )
}

pub fn does_v4_presign_params_exist(query: &BTreeMap<String, String>) -> ApiErrorCode {
    for key in [
        "X-Amz-Algorithm",
        "X-Amz-Credential",
        "X-Amz-Signature",
        "X-Amz-Date",
        "X-Amz-SignedHeaders",
        "X-Amz-Expires",
    ] {
        if !query.contains_key(key) {
            return ApiErrorCode::InvalidQueryParams;
        }
    }
    ApiErrorCode::None
}

pub fn parse_pre_sign_v4(
    query: &BTreeMap<String, String>,
    region: &str,
    service: &str,
) -> (Option<PreSignValues>, ApiErrorCode) {
    let err = does_v4_presign_params_exist(query);
    if err != ApiErrorCode::None {
        return (None, err);
    }
    if query.get("X-Amz-Algorithm").map(String::as_str) != Some(SIGN_V4_ALGORITHM) {
        return (None, ApiErrorCode::InvalidQuerySignatureAlgo);
    }

    let (credential, err) = parse_credential_header(
        &format!(
            "Credential={}",
            query.get("X-Amz-Credential").cloned().unwrap_or_default()
        ),
        region,
        service,
    );
    if err != ApiErrorCode::None {
        return (None, err);
    }

    let date = match NaiveDateTime::parse_from_str(
        query
            .get("X-Amz-Date")
            .map(String::as_str)
            .unwrap_or_default(),
        ISO8601_FORMAT,
    ) {
        Ok(value) => DateTime::<Utc>::from_naive_utc_and_offset(value, Utc),
        Err(_) => return (None, ApiErrorCode::MalformedPresignedDate),
    };

    let expires = match query
        .get("X-Amz-Expires")
        .map(String::as_str)
        .unwrap_or_default()
        .parse::<i64>()
    {
        Ok(value) => value,
        Err(_) => return (None, ApiErrorCode::MalformedExpires),
    };
    if expires < 0 {
        return (None, ApiErrorCode::NegativeExpires);
    }
    if expires > 604800 {
        return (None, ApiErrorCode::MaximumExpires);
    }

    let (signed_headers, err) = parse_signed_header(&format!(
        "SignedHeaders={}",
        query
            .get("X-Amz-SignedHeaders")
            .cloned()
            .unwrap_or_default()
    ));
    if err != ApiErrorCode::None {
        return (None, err);
    }
    let (signature, err) = parse_signature(&format!(
        "Signature={}",
        query.get("X-Amz-Signature").cloned().unwrap_or_default()
    ));
    if err != ApiErrorCode::None {
        return (None, err);
    }

    (
        Some(PreSignValues {
            sign_values: SignValues {
                credential,
                signed_headers,
                signature,
            },
            date,
            expires,
        }),
        ApiErrorCode::None,
    )
}

pub fn active_credentials() -> Credentials {
    Credentials::new("minioadmin", "miniosecret")
}

pub fn check_key_valid(
    request: &TestRequest,
    access_key: &str,
    users: &BTreeMap<String, Credentials>,
) -> (Option<Credentials>, bool, ApiErrorCode) {
    let owner = active_credentials();
    let req_access = request
        .header("authorization")
        .and_then(|value| value.split("Credential=").nth(1))
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .unwrap_or_default();

    if req_access.is_empty() || req_access != owner.access_key {
        return (None, false, ApiErrorCode::InvalidAccessKeyID);
    }

    if access_key == owner.access_key {
        return (Some(owner), true, ApiErrorCode::None);
    }
    if let Some(user) = users.get(access_key) {
        return (Some(user.clone()), false, ApiErrorCode::None);
    }
    (None, false, ApiErrorCode::InvalidAccessKeyID)
}

pub fn skip_content_sha256_cksum(req: &TestRequest) -> bool {
    let header = req.header("x-amz-content-sha256");
    let query = req.query_value("X-Amz-Content-Sha256");
    let presigned = req.query_value("X-Amz-Credential").is_some();

    match (header, query.as_deref(), presigned) {
        (Some(value), _, _) if value == UNSIGNED_PAYLOAD => true,
        (Some(_), _, _) => false,
        (None, Some(value), _) if value == UNSIGNED_PAYLOAD => true,
        (None, Some(_), _) => false,
        (None, None, true) => true,
        (None, None, false) => true,
    }
}

pub fn is_valid_region(request_region: &str, configured_region: &str) -> bool {
    fn normalize(value: &str) -> String {
        if value.is_empty() || value == "US" {
            GLOBAL_MINIO_DEFAULT_REGION.to_string()
        } else {
            value.to_string()
        }
    }
    normalize(request_region) == normalize(configured_region)
}

pub fn extract_signed_headers(
    signed_headers: &[&str],
    req: &TestRequest,
) -> Result<BTreeMap<String, String>, ApiErrorCode> {
    if !signed_headers
        .iter()
        .any(|header| header.eq_ignore_ascii_case("host"))
    {
        return Err(ApiErrorCode::UnsignedHeaders);
    }

    let mut extracted = BTreeMap::new();
    for header in signed_headers {
        let lower = header.to_ascii_lowercase();
        let value = match lower.as_str() {
            "host" => req.url.host_str().map(|host| match req.url.port() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_string(),
            }),
            "expect" => Some("100-continue".to_string()),
            "transfer-encoding" => req.header("transfer-encoding").map(ToOwned::to_owned),
            _ => req
                .header(&lower)
                .map(ToOwned::to_owned)
                .or_else(|| req.query_value(header))
                .or_else(|| req.query_value(&lower)),
        };
        let Some(value) = value else {
            return Err(ApiErrorCode::UnsignedHeaders);
        };
        extracted.insert(lower, value);
    }
    Ok(extracted)
}

pub fn sign_v4_trim_all(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn get_content_sha256_cksum(req: &TestRequest) -> String {
    if let Some(value) = req.header("x-amz-content-sha256") {
        return value.to_string();
    }
    if req.query_value("X-Amz-Credential").is_some() {
        return req
            .query_value("X-Amz-Content-Sha256")
            .unwrap_or_else(|| UNSIGNED_PAYLOAD.to_string());
    }
    EMPTY_SHA256.to_string()
}

fn hex_hmac_sha256(secret_key: &str, payload: &str) -> String {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret_key.as_bytes()).expect("hmac");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn hmac_sha256_bytes(secret_key: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret_key).expect("hmac");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(payload: &[u8]) -> String {
    hex::encode(Sha256::digest(payload))
}

fn aws_percent_encode(input: &str) -> String {
    utf8_percent_encode(input, AWS_URI_SET).to_string()
}

fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }
    let segments = path.split('/').map(aws_percent_encode).collect::<Vec<_>>();
    let mut canonical = segments.join("/");
    if !canonical.starts_with('/') {
        canonical.insert(0, '/');
    }
    canonical
}

fn canonical_query_string(req: &TestRequest, exclude_signature: bool) -> String {
    let mut pairs = req
        .url
        .query_pairs()
        .filter(|(key, _)| !exclude_signature || key != "X-Amz-Signature")
        .map(|(key, value)| (aws_percent_encode(&key), aws_percent_encode(&value)))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_headers(headers: &BTreeMap<String, String>) -> String {
    headers
        .iter()
        .map(|(name, value)| {
            format!(
                "{}:{}\n",
                name.to_ascii_lowercase(),
                sign_v4_trim_all(value)
            )
        })
        .collect::<String>()
}

fn build_canonical_request(
    req: &TestRequest,
    signed_headers: &[String],
    payload_hash: &str,
    exclude_signature: bool,
) -> Result<String, ApiErrorCode> {
    let header_refs = signed_headers
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let extracted = extract_signed_headers(&header_refs, req)?;
    Ok(format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method,
        canonical_uri(req.url.path()),
        canonical_query_string(req, exclude_signature),
        canonical_headers(&extracted),
        signed_headers.join(";"),
        payload_hash
    ))
}

fn sign_v4_derived(secret_key: &str, scope: &CredentialScope, string_to_sign: &str) -> String {
    let k_date = hmac_sha256_bytes(
        format!("AWS4{secret_key}").as_bytes(),
        scope.date.as_bytes(),
    );
    let k_region = hmac_sha256_bytes(&k_date, scope.region.as_bytes());
    let k_service = hmac_sha256_bytes(&k_region, scope.service.as_bytes());
    let k_signing = hmac_sha256_bytes(&k_service, scope.request.as_bytes());
    hex::encode(hmac_sha256_bytes(&k_signing, string_to_sign.as_bytes()))
}

pub fn does_signature_v4_match(
    req: &TestRequest,
    secret_key: &str,
    region: &str,
) -> (String, ApiErrorCode) {
    let authorization = req.header("authorization").unwrap_or_default();
    let (values, err) = parse_sign_v4(authorization, region, "s3");
    if err != ApiErrorCode::None {
        return (String::new(), err);
    }

    let Some(amz_date) = req.header("x-amz-date") else {
        return (values.credential.access_key, ApiErrorCode::AccessDenied);
    };
    if NaiveDateTime::parse_from_str(amz_date, ISO8601_FORMAT).is_err() {
        return (
            values.credential.access_key,
            ApiErrorCode::AuthorizationHeaderMalformed,
        );
    }

    let payload_hash = get_content_sha256_cksum(req);
    let canonical_request =
        match build_canonical_request(req, &values.signed_headers, &payload_hash, false) {
            Ok(request) => request,
            Err(err) => return (values.credential.access_key, err),
        };
    let scope = format!(
        "{}/{}/{}/{}",
        values.credential.scope.date,
        values.credential.scope.region,
        values.credential.scope.service,
        values.credential.scope.request
    );
    let string_to_sign = format!(
        "{SIGN_V4_ALGORITHM}\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let expected = sign_v4_derived(secret_key, &values.credential.scope, &string_to_sign);
    if expected != values.signature {
        return (
            values.credential.access_key,
            ApiErrorCode::SignatureDoesNotMatch,
        );
    }
    (values.credential.access_key, ApiErrorCode::None)
}

pub fn sign_request_v4_standard(
    req: &mut TestRequest,
    access_key: &str,
    secret_key: &str,
    region: &str,
    when: DateTime<Utc>,
) -> Result<(), String> {
    let amz_date = when.format(ISO8601_FORMAT).to_string();
    let short_date = when.format(YYYYMMDD).to_string();
    let payload_hash = sha256_hex(&req.body);
    req.set_header("x-amz-date", &amz_date);
    req.set_header("x-amz-content-sha256", &payload_hash);

    let signed_headers = vec![
        "host".to_string(),
        "x-amz-content-sha256".to_string(),
        "x-amz-date".to_string(),
    ];
    let canonical_request = build_canonical_request(req, &signed_headers, &payload_hash, false)
        .map_err(|err| format!("{err:?}"))?;
    let scope = CredentialScope {
        date: short_date,
        region: region.to_string(),
        service: "s3".to_string(),
        request: "aws4_request".to_string(),
    };
    let credential_scope = format!(
        "{}/{}/{}/{}",
        scope.date, scope.region, scope.service, scope.request
    );
    let string_to_sign = format!(
        "{SIGN_V4_ALGORITHM}\n{amz_date}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = sign_v4_derived(secret_key, &scope, &string_to_sign);
    req.set_header(
        "authorization",
        &format!(
            "{SIGN_V4_ALGORITHM} Credential={access_key}/{credential_scope}, SignedHeaders={}, Signature={signature}",
            signed_headers.join(";")
        ),
    );
    Ok(())
}

pub fn does_policy_signature_v4_match(
    form: &BTreeMap<String, String>,
    secret_key: &str,
    region: &str,
) -> (String, ApiErrorCode) {
    let credential = form.get("x-amz-credential").cloned().unwrap_or_default();
    let (credential, err) =
        parse_credential_header(&format!("Credential={credential}"), region, "s3");
    if err != ApiErrorCode::None {
        return (String::new(), err);
    }

    let policy = form.get("policy").cloned().unwrap_or_default();
    let signature = form.get("x-amz-signature").cloned().unwrap_or_default();
    let expected = hex_hmac_sha256(secret_key, &policy);
    if signature != expected {
        return (credential.access_key, ApiErrorCode::SignatureDoesNotMatch);
    }
    (credential.access_key, ApiErrorCode::None)
}

pub fn does_presigned_signature_v4_match(
    req: &TestRequest,
    secret_key: &str,
    region: &str,
) -> (String, ApiErrorCode) {
    let query = req
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect::<BTreeMap<_, _>>();
    let (values, err) = parse_pre_sign_v4(&query, region, "s3");
    if err != ApiErrorCode::None {
        return (String::new(), err);
    }
    let Some(values) = values else {
        return (String::new(), ApiErrorCode::InvalidQueryParams);
    };

    let expires_at = values.date + chrono::TimeDelta::seconds(values.expires);
    if Utc::now() > expires_at {
        return (
            values.sign_values.credential.access_key,
            ApiErrorCode::ExpiredPresignRequest,
        );
    }

    let payload_hash = req
        .query_value("X-Amz-Content-Sha256")
        .unwrap_or_else(|| UNSIGNED_PAYLOAD.to_string());
    let canonical_request =
        match build_canonical_request(req, &values.sign_values.signed_headers, &payload_hash, true)
        {
            Ok(request) => request,
            Err(err) => return (values.sign_values.credential.access_key, err),
        };
    let scope = format!(
        "{}/{}/{}/{}",
        values.sign_values.credential.scope.date,
        values.sign_values.credential.scope.region,
        values.sign_values.credential.scope.service,
        values.sign_values.credential.scope.request
    );
    let string_to_sign = format!(
        "{SIGN_V4_ALGORITHM}\n{}\n{scope}\n{}",
        values.date.format(ISO8601_FORMAT),
        sha256_hex(canonical_request.as_bytes())
    );
    let expected = sign_v4_derived(
        secret_key,
        &values.sign_values.credential.scope,
        &string_to_sign,
    );
    if values.sign_values.signature != expected {
        return (
            values.sign_values.credential.access_key,
            ApiErrorCode::SignatureDoesNotMatch,
        );
    }
    (values.sign_values.credential.access_key, ApiErrorCode::None)
}

pub fn pre_sign_v4_standard(
    req: &mut TestRequest,
    access_key: &str,
    secret_key: &str,
    region: &str,
    when: DateTime<Utc>,
    expires: i64,
) -> Result<(), String> {
    let amz_date = when.format(ISO8601_FORMAT).to_string();
    let short_date = when.format(YYYYMMDD).to_string();
    let payload_hash = req
        .query_value("X-Amz-Content-Sha256")
        .unwrap_or_else(|| UNSIGNED_PAYLOAD.to_string());

    req.set_query_value("X-Amz-Algorithm", SIGN_V4_ALGORITHM);
    req.set_query_value(
        "X-Amz-Credential",
        &format!("{access_key}/{short_date}/{region}/s3/aws4_request"),
    );
    req.set_query_value("X-Amz-Date", &amz_date);
    req.set_query_value("X-Amz-Expires", &expires.to_string());
    req.set_query_value("X-Amz-SignedHeaders", "host");

    let signed_headers = vec!["host".to_string()];
    let canonical_request = build_canonical_request(req, &signed_headers, &payload_hash, true)
        .map_err(|err| format!("{err:?}"))?;
    let scope = CredentialScope {
        date: short_date,
        region: region.to_string(),
        service: "s3".to_string(),
        request: "aws4_request".to_string(),
    };
    let credential_scope = format!(
        "{}/{}/{}/{}",
        scope.date, scope.region, scope.service, scope.request
    );
    let string_to_sign = format!(
        "{SIGN_V4_ALGORITHM}\n{amz_date}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signature = sign_v4_derived(secret_key, &scope, &string_to_sign);
    req.set_query_value("X-Amz-Signature", &signature);
    Ok(())
}

pub fn check_meta_headers(
    signed_headers: &BTreeMap<String, Vec<String>>,
    req: &TestRequest,
) -> ApiErrorCode {
    let mut expected = BTreeMap::new();
    for (key, values) in signed_headers {
        expected.insert(key.to_ascii_lowercase(), values.join(","));
    }

    let mut actual = BTreeMap::new();
    for (key, value) in &req.headers {
        if key.starts_with("x-amz-meta-") {
            actual.insert(key.to_ascii_lowercase(), value.clone());
        }
    }
    for (key, value) in req.url.query_pairs() {
        let lower = key.to_ascii_lowercase();
        if lower.starts_with("x-amz-meta-") {
            actual.insert(lower, value.into_owned());
        }
    }

    if actual == expected {
        ApiErrorCode::None
    } else {
        ApiErrorCode::UnsignedHeaders
    }
}
