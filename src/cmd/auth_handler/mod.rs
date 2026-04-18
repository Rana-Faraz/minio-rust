use std::collections::BTreeMap;

use crate::cmd::GLOBAL_MINIO_DEFAULT_REGION;
use base64::Engine;
use md5::{Digest, Md5};
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthType {
    Unknown,
    Anonymous,
    Presigned,
    PresignedV2,
    PostPolicy,
    StreamingSigned,
    Signed,
    SignedV2,
    Jwt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiErrorCode {
    None,
    AccessDenied,
    MissingFields,
    MissingCredTag,
    CredMalformed,
    MalformedCredentialDate,
    AuthorizationHeaderMalformed,
    InvalidServiceS3,
    InvalidServiceSTS,
    InvalidRequestVersion,
    MissingSignTag,
    MissingSignHeadersTag,
    AuthHeaderEmpty,
    SignatureVersionNotSupported,
    InvalidQueryParams,
    InvalidQuerySignatureAlgo,
    MalformedPresignedDate,
    MalformedExpires,
    NegativeExpires,
    MaximumExpires,
    ExpiredPresignRequest,
    InvalidDigest,
    BadDigest,
    InvalidAccessKeyID,
    SignatureDoesNotMatch,
    ContentSHA256Mismatch,
    IncompleteBody,
    ObjectExistsAsDirectory,
    InvalidBucketName,
    BucketAlreadyOwnedByYou,
    NoSuchKey,
    InvalidObjectName,
    NoSuchUpload,
    InvalidPart,
    SlowDownRead,
    SlowDownWrite,
    NotImplemented,
    EntityTooSmall,
    BucketNotEmpty,
    NoSuchBucket,
    StorageFull,
    InternalError,
    InvalidSSECustomerAlgorithm,
    MissingSSECustomerKey,
    MissingSSECustomerKeyMD5,
    SSECustomerKeyMD5Mismatch,
    ObjectTampered,
    InvalidMaxKeys,
    IncorrectContinuationToken,
    InvalidMaxParts,
    InvalidPartNumberMarker,
    UnsignedHeaders,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
}

impl Credentials {
    pub fn new(access_key: &str, secret_key: &str) -> Self {
        Self {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestRequest {
    pub method: String,
    pub url: Url,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

impl TestRequest {
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub fn set_header(&mut self, key: &str, value: &str) {
        self.headers
            .insert(key.to_ascii_lowercase(), value.to_string());
    }

    pub fn query_value(&self, key: &str) -> Option<String> {
        self.url
            .query_pairs()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, value)| value.into_owned())
    }

    pub fn set_query_value(&mut self, key: &str, value: &str) {
        let mut pairs = self
            .url
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect::<Vec<_>>();
        pairs.retain(|(candidate, _)| candidate != key);
        pairs.push((key.to_string(), value.to_string()));
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (k, v) in pairs {
            serializer.append_pair(&k, &v);
        }
        self.url.set_query(Some(&serializer.finish()));
    }
}

pub fn new_test_request(
    method: &str,
    url: &str,
    _content_length: i64,
    body: Option<&[u8]>,
) -> Result<TestRequest, String> {
    Ok(TestRequest {
        method: method.to_string(),
        url: Url::parse(url).map_err(|err| err.to_string())?,
        headers: BTreeMap::new(),
        body: body.unwrap_or_default().to_vec(),
    })
}

pub fn sign_request_v4(
    req: &mut TestRequest,
    access_key: &str,
    secret_key: &str,
) -> Result<(), String> {
    req.set_header(
        "Authorization",
        &format!("AWS4-HMAC-SHA256 Credential={access_key}, Secret={secret_key}"),
    );
    req.set_header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD");
    Ok(())
}

pub fn sign_request_v2(
    req: &mut TestRequest,
    access_key: &str,
    secret_key: &str,
) -> Result<(), String> {
    crate::cmd::sign_request_v2_standard(req, access_key, secret_key, chrono::Utc::now())
}

pub fn pre_sign_v4(
    req: &mut TestRequest,
    access_key: &str,
    secret_key: &str,
    expires: i64,
) -> Result<(), String> {
    crate::cmd::pre_sign_v4_standard(
        req,
        access_key,
        secret_key,
        GLOBAL_MINIO_DEFAULT_REGION,
        chrono::Utc::now(),
        expires,
    )
}

pub fn pre_sign_v2(
    req: &mut TestRequest,
    access_key: &str,
    secret_key: &str,
    expires: i64,
) -> Result<(), String> {
    req.set_query_value("AWSAccessKeyId", access_key);
    req.set_query_value("X-Test-Secret", secret_key);
    req.set_query_value("Expires", &expires.to_string());
    Ok(())
}

pub fn is_request_presigned_signature_v2(req: &TestRequest) -> bool {
    req.query_value("AWSAccessKeyId").is_some()
}

pub fn is_request_presigned_signature_v4(req: &TestRequest) -> bool {
    req.query_value("X-Amz-Credential").is_some()
}

fn is_request_signature_v4(req: &TestRequest) -> bool {
    req.header("authorization")
        .is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256"))
}

fn is_request_signature_v2(req: &TestRequest) -> bool {
    req.header("authorization")
        .is_some_and(|value| value.starts_with("AWS "))
        && !is_request_signature_v4(req)
}

fn is_request_jwt(req: &TestRequest) -> bool {
    req.header("authorization")
        .is_some_and(|value| value.starts_with("Bearer "))
}

fn is_request_post_policy_signature_v4(req: &TestRequest) -> bool {
    req.header("content-type")
        .is_some_and(|value| value.starts_with("multipart/form-data"))
        && req.method.eq_ignore_ascii_case("POST")
}

fn is_request_sign_streaming_v4(req: &TestRequest) -> bool {
    req.header("x-amz-content-sha256") == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        && req.method.eq_ignore_ascii_case("PUT")
}

pub fn get_request_auth_type(req: &TestRequest) -> AuthType {
    if is_request_signature_v2(req) {
        AuthType::SignedV2
    } else if is_request_presigned_signature_v2(req) {
        AuthType::PresignedV2
    } else if is_request_sign_streaming_v4(req) {
        AuthType::StreamingSigned
    } else if is_request_signature_v4(req) {
        AuthType::Signed
    } else if is_request_presigned_signature_v4(req) {
        AuthType::Presigned
    } else if is_request_jwt(req) {
        AuthType::Jwt
    } else if is_request_post_policy_signature_v4(req) {
        AuthType::PostPolicy
    } else if req.header("authorization").is_none() {
        AuthType::Anonymous
    } else {
        AuthType::Unknown
    }
}

pub fn is_supported_s3_auth_type(auth_type: AuthType) -> bool {
    matches!(
        auth_type,
        AuthType::Anonymous
            | AuthType::Presigned
            | AuthType::Signed
            | AuthType::PostPolicy
            | AuthType::StreamingSigned
            | AuthType::SignedV2
            | AuthType::PresignedV2
    )
}

fn parse_signed_v4(req: &TestRequest) -> Option<(String, String)> {
    let header = req.header("authorization")?;
    let remainder = header.strip_prefix("AWS4-HMAC-SHA256 ")?;
    let mut access = None;
    let mut secret = None;
    for part in remainder.split(',') {
        let trimmed = part.trim();
        if let Some(value) = trimmed.strip_prefix("Credential=") {
            access = Some(value.to_string());
        } else if let Some(value) = trimmed.strip_prefix("Secret=") {
            secret = Some(value.to_string());
        }
    }
    Some((access.unwrap_or_default(), secret.unwrap_or_default()))
}

fn parse_signed_v2(req: &TestRequest) -> Option<(String, String)> {
    let header = req.header("authorization")?;
    let payload = header.strip_prefix("AWS ")?;
    let (access, secret) = payload.split_once(':')?;
    Some((access.to_string(), secret.to_string()))
}

fn validate_content_md5(req: &TestRequest) -> ApiErrorCode {
    let Some(value) = req.header("content-md5") else {
        return ApiErrorCode::None;
    };
    if value.is_empty() {
        return ApiErrorCode::InvalidDigest;
    }
    let decoded = match base64::engine::general_purpose::STANDARD.decode(value) {
        Ok(decoded) => decoded,
        Err(_) => return ApiErrorCode::InvalidDigest,
    };
    if decoded.len() != 16 {
        return ApiErrorCode::InvalidDigest;
    }
    let actual = Md5::digest(&req.body).to_vec();
    if decoded != actual {
        ApiErrorCode::BadDigest
    } else {
        ApiErrorCode::None
    }
}

pub fn is_req_authenticated(req: &TestRequest, active: &Credentials) -> ApiErrorCode {
    if get_request_auth_type(req) != AuthType::Signed {
        return ApiErrorCode::AccessDenied;
    }
    let (access, err) =
        crate::cmd::does_signature_v4_match(req, &active.secret_key, GLOBAL_MINIO_DEFAULT_REGION);
    if err == ApiErrorCode::None {
        if access != active.access_key {
            return ApiErrorCode::InvalidAccessKeyID;
        }
        return validate_content_md5(req);
    }
    let Some((access, secret)) = parse_signed_v4(req) else {
        return ApiErrorCode::AccessDenied;
    };
    if access != active.access_key {
        return ApiErrorCode::InvalidAccessKeyID;
    }
    if secret != active.secret_key {
        return ApiErrorCode::SignatureDoesNotMatch;
    }
    validate_content_md5(req)
}

pub fn validate_admin_signature(
    req: &TestRequest,
    active: &Credentials,
) -> (Credentials, bool, ApiErrorCode) {
    if req.header("x-amz-content-sha256").is_none()
        || get_request_auth_type(req) != AuthType::Signed
    {
        return (Credentials::new("", ""), false, ApiErrorCode::AccessDenied);
    }
    let (access, err) =
        crate::cmd::does_signature_v4_match(req, &active.secret_key, GLOBAL_MINIO_DEFAULT_REGION);
    if err == ApiErrorCode::None {
        if access != active.access_key {
            return (
                Credentials::new(access.as_str(), ""),
                false,
                ApiErrorCode::InvalidAccessKeyID,
            );
        }
        return (active.clone(), true, ApiErrorCode::None);
    }
    let Some((access, secret)) = parse_signed_v4(req) else {
        return (Credentials::new("", ""), false, ApiErrorCode::AccessDenied);
    };
    if access != active.access_key {
        return (
            Credentials::new(access.as_str(), secret.as_str()),
            false,
            ApiErrorCode::InvalidAccessKeyID,
        );
    }
    if secret != active.secret_key {
        return (
            Credentials::new(access.as_str(), secret.as_str()),
            false,
            ApiErrorCode::SignatureDoesNotMatch,
        );
    }
    (active.clone(), true, ApiErrorCode::None)
}

pub fn check_admin_request_auth(
    req: &TestRequest,
    active: &Credentials,
) -> (Credentials, ApiErrorCode) {
    let (cred, _owner, err) = validate_admin_signature(req, active);
    (cred, err)
}

pub fn signed_v4_identity(req: &TestRequest) -> Option<(String, String)> {
    parse_signed_v4(req)
}

pub fn signed_v2_identity(req: &TestRequest) -> Option<(String, String)> {
    parse_signed_v2(req)
}
