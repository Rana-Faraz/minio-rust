use std::collections::HashMap;

use minio_rust::internal::crypto::{
    self, HeaderMap, Metadata, AMZ_ENCRYPTION_AES,
    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM,
    AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, ERR_MISSING_CUSTOMER_KEY_MD5,
    ERR_MISSING_INTERNAL_SEAL_ALGORITHM, ERR_SECRET_KEY_MISMATCH, META_ALGORITHM, META_IV,
    META_SEALED_KEY_SSEC, S3, SSEC, SSE_COPY,
};

pub const SOURCE_FILE: &str = "internal/crypto/sse_test.go";

#[test]
fn test_s3_string() {
    assert_eq!(S3.string(), "SSE-S3");
}

#[test]
fn test_ssec_string() {
    assert_eq!(SSEC.string(), "SSE-C");
}

#[test]
fn test_ssec_unseal_object_key() {
    let valid_headers = headers([
        (
            AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
            AMZ_ENCRYPTION_AES,
        ),
        (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, CUSTOMER_KEY),
        (
            AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5,
            CUSTOMER_KEY_MD5,
        ),
    ]);
    let valid_metadata = metadata([
        (META_SEALED_KEY_SSEC, "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ=="),
        (META_ALGORITHM, crypto::SEAL_ALGORITHM),
        (META_IV, "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA="),
    ]);

    assert!(SSEC
        .unseal_object_key(&valid_headers, &valid_metadata, "bucket", "object")
        .is_ok());

    let err = SSEC
        .unseal_object_key(&valid_headers, &valid_metadata, "bucket", "object2")
        .expect_err("wrong object should fail");
    assert_eq!(err, ERR_SECRET_KEY_MISMATCH);

    let invalid_metadata = metadata([
        (META_SEALED_KEY_SSEC, "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ=="),
        (META_IV, "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA="),
    ]);
    let err = SSEC
        .unseal_object_key(&valid_headers, &invalid_metadata, "bucket", "object")
        .expect_err("missing algorithm should fail");
    assert_eq!(err, ERR_MISSING_INTERNAL_SEAL_ALGORITHM);

    let invalid_headers = headers([
        (
            AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
            AMZ_ENCRYPTION_AES,
        ),
        (AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, CUSTOMER_KEY),
    ]);
    let err = SSEC
        .unseal_object_key(&invalid_headers, &valid_metadata, "bucket", "object")
        .expect_err("missing md5 should fail");
    assert_eq!(err, ERR_MISSING_CUSTOMER_KEY_MD5);
}

#[test]
fn test_ssecopy_unseal_object_key() {
    let valid_headers = headers([
        (
            AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM,
            AMZ_ENCRYPTION_AES,
        ),
        (AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, CUSTOMER_KEY),
        (
            AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5,
            CUSTOMER_KEY_MD5,
        ),
    ]);
    let valid_metadata = metadata([
        (META_SEALED_KEY_SSEC, "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ=="),
        (META_ALGORITHM, crypto::SEAL_ALGORITHM),
        (META_IV, "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA="),
    ]);

    assert!(SSE_COPY
        .unseal_object_key(&valid_headers, &valid_metadata, "bucket", "object")
        .is_ok());

    let err = SSE_COPY
        .unseal_object_key(&valid_headers, &valid_metadata, "bucket", "object2")
        .expect_err("wrong object should fail");
    assert_eq!(err, ERR_SECRET_KEY_MISMATCH);

    let invalid_metadata = metadata([
        (META_SEALED_KEY_SSEC, "IAAfAMBdYor5tf/UlVaQvwYlw5yKbPBeQqfygqsfHqhu1wHD9KDAP4bw38AhL12prFTS23JbbR9Re5Qv26ZnlQ=="),
        (META_IV, "coVfGS3I/CTrqexX5vUN+PQPoP9aUFiPYYrSzqTWfBA="),
    ]);
    let err = SSE_COPY
        .unseal_object_key(&valid_headers, &invalid_metadata, "bucket", "object")
        .expect_err("missing algorithm should fail");
    assert_eq!(err, ERR_MISSING_INTERNAL_SEAL_ALGORITHM);

    let invalid_headers = headers([
        (
            AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM,
            AMZ_ENCRYPTION_AES,
        ),
        (AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY, CUSTOMER_KEY),
    ]);
    let err = SSE_COPY
        .unseal_object_key(&invalid_headers, &valid_metadata, "bucket", "object")
        .expect_err("missing md5 should fail");
    assert_eq!(err, ERR_MISSING_CUSTOMER_KEY_MD5);
}

fn headers<const N: usize>(pairs: [(&str, &str); N]) -> HeaderMap {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<HashMap<_, _>>()
}

fn metadata<const N: usize>(pairs: [(&str, &str); N]) -> Metadata {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<HashMap<_, _>>()
}

const CUSTOMER_KEY: &str = "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=";
const CUSTOMER_KEY_MD5: &str = "7PpPLAK26ONlVUGOWlusfg==";
