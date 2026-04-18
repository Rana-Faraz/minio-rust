use std::error::Error;

use minio_rust::cmd::{
    api_error_definition, defined_api_error_codes, to_api_error_code, ApiErrorCode, BadDigest,
    BucketExists, BucketNameInvalid, BucketNotEmptyError, BucketNotFound, CustomerKeyMd5Mismatch,
    IncompleteBodyError, InsufficientReadQuorum, InsufficientWriteQuorum, InvalidCustomerAlgorithm,
    InvalidCustomerKey, InvalidPartError, InvalidUploadId, InvalidUploadIdKeyCombination,
    MalformedUploadId, MissingCustomerKey, MissingCustomerKeyMd5, NotImplementedError,
    ObjectExistsAsDirectory, ObjectNameInvalid, ObjectNotFound, ObjectTampered, PartTooSmall,
    SHA256Mismatch, SignatureMismatch, StorageFull,
};

pub const SOURCE_FILE: &str = "cmd/api-errors_test.go";

#[test]
fn test_apierr_code_line_65() {
    let cases: Vec<(Option<Box<dyn Error>>, ApiErrorCode)> = vec![
        (Some(Box::new(BadDigest)), ApiErrorCode::BadDigest),
        (
            Some(Box::new(SHA256Mismatch)),
            ApiErrorCode::ContentSHA256Mismatch,
        ),
        (
            Some(Box::new(IncompleteBodyError)),
            ApiErrorCode::IncompleteBody,
        ),
        (
            Some(Box::new(ObjectExistsAsDirectory)),
            ApiErrorCode::ObjectExistsAsDirectory,
        ),
        (
            Some(Box::new(BucketNameInvalid)),
            ApiErrorCode::InvalidBucketName,
        ),
        (
            Some(Box::new(BucketExists)),
            ApiErrorCode::BucketAlreadyOwnedByYou,
        ),
        (Some(Box::new(ObjectNotFound)), ApiErrorCode::NoSuchKey),
        (
            Some(Box::new(ObjectNameInvalid)),
            ApiErrorCode::InvalidObjectName,
        ),
        (Some(Box::new(InvalidUploadId)), ApiErrorCode::NoSuchUpload),
        (Some(Box::new(InvalidPartError)), ApiErrorCode::InvalidPart),
        (
            Some(Box::new(InsufficientReadQuorum)),
            ApiErrorCode::SlowDownRead,
        ),
        (
            Some(Box::new(InsufficientWriteQuorum)),
            ApiErrorCode::SlowDownWrite,
        ),
        (
            Some(Box::new(InvalidUploadIdKeyCombination)),
            ApiErrorCode::NotImplemented,
        ),
        (
            Some(Box::new(MalformedUploadId)),
            ApiErrorCode::NoSuchUpload,
        ),
        (Some(Box::new(PartTooSmall)), ApiErrorCode::EntityTooSmall),
        (
            Some(Box::new(BucketNotEmptyError)),
            ApiErrorCode::BucketNotEmpty,
        ),
        (Some(Box::new(BucketNotFound)), ApiErrorCode::NoSuchBucket),
        (Some(Box::new(StorageFull)), ApiErrorCode::StorageFull),
        (
            Some(Box::new(NotImplementedError)),
            ApiErrorCode::NotImplemented,
        ),
        (
            Some(Box::new(SignatureMismatch)),
            ApiErrorCode::SignatureDoesNotMatch,
        ),
        (
            Some(Box::new(InvalidCustomerAlgorithm)),
            ApiErrorCode::InvalidSSECustomerAlgorithm,
        ),
        (
            Some(Box::new(MissingCustomerKey)),
            ApiErrorCode::MissingSSECustomerKey,
        ),
        (
            Some(Box::new(InvalidCustomerKey)),
            ApiErrorCode::AccessDenied,
        ),
        (
            Some(Box::new(MissingCustomerKeyMd5)),
            ApiErrorCode::MissingSSECustomerKeyMD5,
        ),
        (
            Some(Box::new(CustomerKeyMd5Mismatch)),
            ApiErrorCode::SSECustomerKeyMD5Mismatch,
        ),
        (Some(Box::new(ObjectTampered)), ApiErrorCode::ObjectTampered),
        (None, ApiErrorCode::None),
        (
            Some(Box::new(std::io::Error::other("custom error"))),
            ApiErrorCode::InternalError,
        ),
    ];

    for (err, expected) in cases {
        assert_eq!(to_api_error_code(err.as_deref()), expected);
    }
}

#[test]
fn test_apierr_code_definition_line_76() {
    for code in defined_api_error_codes() {
        let definition = api_error_definition(code).expect("definition");
        assert!(!definition.code.is_empty());
        assert_ne!(definition.http_status_code, 0);
    }
}
