use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::cmd::ApiErrorCode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiErrorDefinition {
    pub code: &'static str,
    pub description: &'static str,
    pub http_status_code: u16,
}

#[derive(Debug)]
pub struct BadDigest;
impl Display for BadDigest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("bad digest")
    }
}
impl Error for BadDigest {}

#[derive(Debug)]
pub struct SHA256Mismatch;
impl Display for SHA256Mismatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("sha256 mismatch")
    }
}
impl Error for SHA256Mismatch {}

macro_rules! simple_error_type {
    ($name:ident, $message:literal) => {
        #[derive(Debug)]
        pub struct $name;
        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str($message)
            }
        }
        impl Error for $name {}
    };
}

simple_error_type!(IncompleteBodyError, "incomplete body");
simple_error_type!(ObjectExistsAsDirectory, "object exists as directory");
simple_error_type!(BucketNameInvalid, "bucket name invalid");
simple_error_type!(BucketExists, "bucket exists");
simple_error_type!(ObjectNotFound, "object not found");
simple_error_type!(ObjectNameInvalid, "object name invalid");
simple_error_type!(InvalidUploadId, "invalid upload id");
simple_error_type!(InvalidPartError, "invalid part");
simple_error_type!(InsufficientReadQuorum, "insufficient read quorum");
simple_error_type!(InsufficientWriteQuorum, "insufficient write quorum");
simple_error_type!(
    InvalidUploadIdKeyCombination,
    "invalid upload id key combination"
);
simple_error_type!(MalformedUploadId, "malformed upload id");
simple_error_type!(PartTooSmall, "part too small");
simple_error_type!(BucketNotEmptyError, "bucket not empty");
simple_error_type!(BucketNotFound, "bucket not found");
simple_error_type!(StorageFull, "storage full");
simple_error_type!(NotImplementedError, "not implemented");
simple_error_type!(SignatureMismatch, "signature mismatch");
simple_error_type!(InvalidCustomerAlgorithm, "invalid sse customer algorithm");
simple_error_type!(MissingCustomerKey, "missing sse customer key");
simple_error_type!(InvalidCustomerKey, "invalid sse customer key");
simple_error_type!(MissingCustomerKeyMd5, "missing sse customer key md5");
simple_error_type!(CustomerKeyMd5Mismatch, "sse customer key md5 mismatch");
simple_error_type!(ObjectTampered, "object tampered");

pub fn to_api_error_code(err: Option<&(dyn Error + 'static)>) -> ApiErrorCode {
    let Some(err) = err else {
        return ApiErrorCode::None;
    };

    if err.is::<BadDigest>() {
        ApiErrorCode::BadDigest
    } else if err.is::<SHA256Mismatch>() {
        ApiErrorCode::ContentSHA256Mismatch
    } else if err.is::<IncompleteBodyError>() {
        ApiErrorCode::IncompleteBody
    } else if err.is::<ObjectExistsAsDirectory>() {
        ApiErrorCode::ObjectExistsAsDirectory
    } else if err.is::<BucketNameInvalid>() {
        ApiErrorCode::InvalidBucketName
    } else if err.is::<BucketExists>() {
        ApiErrorCode::BucketAlreadyOwnedByYou
    } else if err.is::<ObjectNotFound>() {
        ApiErrorCode::NoSuchKey
    } else if err.is::<ObjectNameInvalid>() {
        ApiErrorCode::InvalidObjectName
    } else if err.is::<InvalidUploadId>() {
        ApiErrorCode::NoSuchUpload
    } else if err.is::<InvalidPartError>() {
        ApiErrorCode::InvalidPart
    } else if err.is::<InsufficientReadQuorum>() {
        ApiErrorCode::SlowDownRead
    } else if err.is::<InsufficientWriteQuorum>() {
        ApiErrorCode::SlowDownWrite
    } else if err.is::<InvalidUploadIdKeyCombination>() {
        ApiErrorCode::NotImplemented
    } else if err.is::<MalformedUploadId>() {
        ApiErrorCode::NoSuchUpload
    } else if err.is::<PartTooSmall>() {
        ApiErrorCode::EntityTooSmall
    } else if err.is::<BucketNotEmptyError>() {
        ApiErrorCode::BucketNotEmpty
    } else if err.is::<BucketNotFound>() {
        ApiErrorCode::NoSuchBucket
    } else if err.is::<StorageFull>() {
        ApiErrorCode::StorageFull
    } else if err.is::<NotImplementedError>() {
        ApiErrorCode::NotImplemented
    } else if err.is::<SignatureMismatch>() {
        ApiErrorCode::SignatureDoesNotMatch
    } else if err.is::<InvalidCustomerAlgorithm>() {
        ApiErrorCode::InvalidSSECustomerAlgorithm
    } else if err.is::<MissingCustomerKey>() {
        ApiErrorCode::MissingSSECustomerKey
    } else if err.is::<InvalidCustomerKey>() {
        ApiErrorCode::AccessDenied
    } else if err.is::<MissingCustomerKeyMd5>() {
        ApiErrorCode::MissingSSECustomerKeyMD5
    } else if err.is::<CustomerKeyMd5Mismatch>() {
        ApiErrorCode::SSECustomerKeyMD5Mismatch
    } else if err.is::<ObjectTampered>() {
        ApiErrorCode::ObjectTampered
    } else {
        ApiErrorCode::InternalError
    }
}

pub fn api_error_definition(code: ApiErrorCode) -> Option<ApiErrorDefinition> {
    let def = match code {
        ApiErrorCode::AccessDenied => ("AccessDenied", "Access Denied.", 403),
        ApiErrorCode::BadDigest => (
            "BadDigest",
            "The Content-MD5 you specified did not match what we received.",
            400,
        ),
        ApiErrorCode::ContentSHA256Mismatch => (
            "XAmzContentSHA256Mismatch",
            "The provided 'x-amz-content-sha256' header does not match what was computed.",
            400,
        ),
        ApiErrorCode::IncompleteBody => (
            "IncompleteBody",
            "You did not provide the number of bytes specified by the Content-Length HTTP header.",
            400,
        ),
        ApiErrorCode::ObjectExistsAsDirectory => (
            "XMinioObjectExistsAsDirectory",
            "Object name already exists as a directory.",
            409,
        ),
        ApiErrorCode::InvalidBucketName => (
            "InvalidBucketName",
            "The specified bucket is not valid.",
            400,
        ),
        ApiErrorCode::BucketAlreadyOwnedByYou => (
            "BucketAlreadyOwnedByYou",
            "Your previous request to create the named bucket succeeded and you already own it.",
            409,
        ),
        ApiErrorCode::NoSuchKey => ("NoSuchKey", "The specified key does not exist.", 404),
        ApiErrorCode::InvalidObjectName => (
            "XMinioInvalidObjectName",
            "Object name contains unsupported characters.",
            400,
        ),
        ApiErrorCode::NoSuchUpload => (
            "NoSuchUpload",
            "The specified multipart upload does not exist.",
            404,
        ),
        ApiErrorCode::InvalidPart => (
            "InvalidPart",
            "One or more of the specified parts could not be found.",
            400,
        ),
        ApiErrorCode::SlowDownRead => (
            "SlowDownRead",
            "Resource requested is unreadable, please reduce your request rate.",
            503,
        ),
        ApiErrorCode::SlowDownWrite => (
            "SlowDownWrite",
            "Resource requested is unwritable, please reduce your request rate.",
            503,
        ),
        ApiErrorCode::NotImplemented => (
            "NotImplemented",
            "A header you provided implies functionality that is not implemented.",
            501,
        ),
        ApiErrorCode::EntityTooSmall => (
            "EntityTooSmall",
            "Your proposed upload is smaller than the minimum allowed object size.",
            400,
        ),
        ApiErrorCode::BucketNotEmpty => (
            "BucketNotEmpty",
            "The bucket you tried to delete is not empty.",
            409,
        ),
        ApiErrorCode::NoSuchBucket => ("NoSuchBucket", "The specified bucket does not exist.", 404),
        ApiErrorCode::StorageFull => (
            "XMinioStorageFull",
            "Storage backend has reached its minimum free drive threshold.",
            507,
        ),
        ApiErrorCode::SignatureDoesNotMatch => (
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature you provided.",
            403,
        ),
        ApiErrorCode::InvalidSSECustomerAlgorithm => (
            "InvalidRequest",
            "The SSE-C algorithm is not supported.",
            400,
        ),
        ApiErrorCode::MissingSSECustomerKey => ("InvalidRequest", "The SSE-C key is missing.", 400),
        ApiErrorCode::MissingSSECustomerKeyMD5 => {
            ("InvalidRequest", "The SSE-C key MD5 is missing.", 400)
        }
        ApiErrorCode::SSECustomerKeyMD5Mismatch => (
            "SSECustomerKeyMD5Mismatch",
            "The calculated MD5 hash of the key did not match the hash that was provided.",
            400,
        ),
        ApiErrorCode::ObjectTampered => ("XMinioObjectTampered", "Object was tampered.", 206),
        ApiErrorCode::InternalError => (
            "InternalError",
            "We encountered an internal error. Please try again.",
            500,
        ),
        ApiErrorCode::None => return None,
        _ => return None,
    };
    Some(ApiErrorDefinition {
        code: def.0,
        description: def.1,
        http_status_code: def.2,
    })
}

pub fn defined_api_error_codes() -> Vec<ApiErrorCode> {
    vec![
        ApiErrorCode::AccessDenied,
        ApiErrorCode::BadDigest,
        ApiErrorCode::ContentSHA256Mismatch,
        ApiErrorCode::IncompleteBody,
        ApiErrorCode::ObjectExistsAsDirectory,
        ApiErrorCode::InvalidBucketName,
        ApiErrorCode::BucketAlreadyOwnedByYou,
        ApiErrorCode::NoSuchKey,
        ApiErrorCode::InvalidObjectName,
        ApiErrorCode::NoSuchUpload,
        ApiErrorCode::InvalidPart,
        ApiErrorCode::SlowDownRead,
        ApiErrorCode::SlowDownWrite,
        ApiErrorCode::NotImplemented,
        ApiErrorCode::EntityTooSmall,
        ApiErrorCode::BucketNotEmpty,
        ApiErrorCode::NoSuchBucket,
        ApiErrorCode::StorageFull,
        ApiErrorCode::SignatureDoesNotMatch,
        ApiErrorCode::InvalidSSECustomerAlgorithm,
        ApiErrorCode::MissingSSECustomerKey,
        ApiErrorCode::MissingSSECustomerKeyMD5,
        ApiErrorCode::SSECustomerKeyMD5Mismatch,
        ApiErrorCode::ObjectTampered,
        ApiErrorCode::InternalError,
    ]
}
