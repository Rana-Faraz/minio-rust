use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use minio_rust::internal::crypto::{
    self, Metadata, SealedKey, ERR_INVALID_INTERNAL_IV, ERR_INVALID_INTERNAL_SEAL_ALGORITHM,
    ERR_MISSING_INTERNAL_IV, ERR_MISSING_INTERNAL_SEAL_ALGORITHM, INSECURE_SEAL_ALGORITHM,
    META_ALGORITHM, META_DATA_ENCRYPTION_KEY, META_IV, META_KEY_ID, META_MULTIPART,
    META_SEALED_KEY_KMS, META_SEALED_KEY_S3, META_SEALED_KEY_SSEC, S3, SSEC,
};

pub const SOURCE_FILE: &str = "internal/crypto/metadata_test.go";

#[test]
fn test_is_multipart() {
    let cases = [
        (metadata([(META_MULTIPART, "")]), true),
        (
            metadata([("X-Minio-Internal-EncryptedMultipart", "")]),
            false,
        ),
        (metadata([("", "")]), false),
    ];

    for (idx, (metadata, expected)) in cases.into_iter().enumerate() {
        assert_eq!(crypto::is_multipart(&metadata), expected, "case {}", idx);
    }
}

#[test]
fn test_is_encrypted() {
    let cases = [
        (metadata([(META_MULTIPART, "")]), true),
        (metadata([(META_IV, "")]), true),
        (metadata([(META_ALGORITHM, "")]), true),
        (metadata([(META_SEALED_KEY_SSEC, "")]), true),
        (metadata([(META_SEALED_KEY_S3, "")]), true),
        (metadata([(META_KEY_ID, "")]), true),
        (metadata([(META_DATA_ENCRYPTION_KEY, "")]), true),
        (metadata([("", "")]), false),
        (
            metadata([("X-Minio-Internal-Server-Side-Encryption", "")]),
            false,
        ),
    ];

    for (idx, (metadata, expected)) in cases.into_iter().enumerate() {
        let (_, encrypted) = crypto::is_encrypted(&metadata);
        assert_eq!(encrypted, expected, "case {}", idx);
    }
}

#[test]
fn test_s3_is_encrypted() {
    assert!(!S3.is_encrypted(&metadata([(META_MULTIPART, "")])));
    assert!(S3.is_encrypted(&metadata([(META_SEALED_KEY_S3, "")])));
    assert!(!S3.is_encrypted(&metadata([(META_SEALED_KEY_SSEC, "")])));
}

#[test]
fn test_ssec_is_encrypted() {
    assert!(!SSEC.is_encrypted(&metadata([(META_MULTIPART, "")])));
    assert!(SSEC.is_encrypted(&metadata([(META_SEALED_KEY_SSEC, "")])));
    assert!(!SSEC.is_encrypted(&metadata([(META_SEALED_KEY_S3, "")])));
}

#[test]
fn test_s3_parse_metadata() {
    let cases = [
        (Metadata::new(), Some(ERR_MISSING_INTERNAL_IV.clone())),
        (
            metadata([(META_IV, "")]),
            Some(ERR_MISSING_INTERNAL_SEAL_ALGORITHM.clone()),
        ),
        (
            metadata([(META_IV, ""), (META_ALGORITHM, "")]),
            Some(crypto::CryptoError::static_msg(
                "The object metadata is missing the internal sealed key for SSE-S3",
            )),
        ),
        (
            metadata([
                (META_IV, ""),
                (META_ALGORITHM, ""),
                (META_SEALED_KEY_S3, ""),
                (META_DATA_ENCRYPTION_KEY, "IAAF0b=="),
            ]),
            Some(crypto::CryptoError::static_msg(
                "The object metadata is missing the internal KMS key-ID for SSE-S3",
            )),
        ),
        (
            metadata([
                (META_IV, ""),
                (META_ALGORITHM, ""),
                (META_SEALED_KEY_S3, ""),
                (META_KEY_ID, ""),
            ]),
            Some(crypto::CryptoError::static_msg(
                "The object metadata is missing the internal sealed KMS data key for SSE-S3",
            )),
        ),
        (
            metadata([
                (META_IV, &BASE64_STANDARD.encode([0u8; 32])),
                (META_ALGORITHM, ""),
                (META_SEALED_KEY_S3, ""),
                (META_KEY_ID, ""),
                (META_DATA_ENCRYPTION_KEY, ""),
            ]),
            Some(ERR_INVALID_INTERNAL_SEAL_ALGORITHM.clone()),
        ),
        (
            metadata([
                (META_IV, &BASE64_STANDARD.encode([0u8; 32])),
                (META_ALGORITHM, crypto::SEAL_ALGORITHM),
                (META_SEALED_KEY_S3, ""),
                (META_KEY_ID, ""),
                (META_DATA_ENCRYPTION_KEY, ""),
            ]),
            Some(crypto::CryptoError::static_msg(
                "The internal sealed key for SSE-S3 is invalid",
            )),
        ),
        (
            metadata([
                (META_IV, &BASE64_STANDARD.encode([0u8; 32])),
                (META_ALGORITHM, crypto::SEAL_ALGORITHM),
                (META_SEALED_KEY_S3, &BASE64_STANDARD.encode([0u8; 64])),
                (META_KEY_ID, "key-1"),
                (
                    META_DATA_ENCRYPTION_KEY,
                    ".MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=",
                ),
            ]),
            Some(crypto::CryptoError::static_msg(
                "The internal sealed KMS data key for SSE-S3 is invalid",
            )),
        ),
    ];

    for (idx, (metadata, expected_err)) in cases.into_iter().enumerate() {
        let err = S3.parse_metadata(&metadata).err();
        assert_eq!(err, expected_err, "case {}", idx);
    }

    let metadata = metadata([
        (META_IV, &BASE64_STANDARD.encode([1u8; 32])),
        (META_ALGORITHM, crypto::SEAL_ALGORITHM),
        (META_SEALED_KEY_S3, &BASE64_STANDARD.encode([1u8; 64])),
        (META_KEY_ID, "key-1"),
        (META_DATA_ENCRYPTION_KEY, &BASE64_STANDARD.encode([0u8; 48])),
    ]);
    let (key_id, data_key, sealed_key) =
        S3.parse_metadata(&metadata).expect("metadata should parse");
    assert_eq!(key_id, "key-1");
    assert_eq!(data_key, vec![0u8; 48]);
    assert_eq!(sealed_key.algorithm, crypto::SEAL_ALGORITHM);
    assert_eq!(sealed_key.iv[0], 1);
    assert_eq!(sealed_key.key[0], 1);
}

#[test]
fn test_create_multipart_metadata() {
    let metadata = crypto::create_multipart_metadata(None);
    assert_eq!(metadata.get(META_MULTIPART).map(String::as_str), Some(""));
}

#[test]
fn test_ssec_parse_metadata() {
    let cases = [
        (Metadata::new(), Some(ERR_MISSING_INTERNAL_IV.clone())),
        (
            metadata([(META_IV, "")]),
            Some(ERR_MISSING_INTERNAL_SEAL_ALGORITHM.clone()),
        ),
        (
            metadata([(META_IV, ""), (META_ALGORITHM, "")]),
            Some(crypto::CryptoError::static_msg(
                "The object metadata is missing the internal sealed key for SSE-C",
            )),
        ),
        (
            metadata([
                (META_IV, ""),
                (META_ALGORITHM, ""),
                (META_SEALED_KEY_SSEC, ""),
            ]),
            Some(ERR_INVALID_INTERNAL_IV.clone()),
        ),
        (
            metadata([
                (META_IV, &BASE64_STANDARD.encode([0u8; 32])),
                (META_ALGORITHM, ""),
                (META_SEALED_KEY_SSEC, ""),
            ]),
            Some(ERR_INVALID_INTERNAL_SEAL_ALGORITHM.clone()),
        ),
        (
            metadata([
                (META_IV, &BASE64_STANDARD.encode([0u8; 32])),
                (META_ALGORITHM, crypto::SEAL_ALGORITHM),
                (META_SEALED_KEY_SSEC, ""),
            ]),
            Some(crypto::CryptoError::static_msg(
                "The internal sealed key for SSE-C is invalid",
            )),
        ),
    ];

    for (idx, (metadata, expected_err)) in cases.into_iter().enumerate() {
        let err = SSEC.parse_metadata(&metadata).err();
        assert_eq!(err, expected_err, "case {}", idx);
    }

    let metadata = metadata([
        (META_IV, &BASE64_STANDARD.encode([1u8; 32])),
        (META_ALGORITHM, INSECURE_SEAL_ALGORITHM),
        (META_SEALED_KEY_SSEC, &BASE64_STANDARD.encode([1u8; 64])),
    ]);
    let sealed_key = SSEC
        .parse_metadata(&metadata)
        .expect("ssec metadata should parse");
    assert_eq!(sealed_key.algorithm, INSECURE_SEAL_ALGORITHM);
    assert_eq!(sealed_key.iv[0], 1);
    assert_eq!(sealed_key.key[0], 1);
}

#[test]
fn test_s3_create_metadata() {
    let cases = [
        (
            "",
            Vec::new(),
            SealedKey {
                algorithm: crypto::SEAL_ALGORITHM.to_owned(),
                ..SealedKey::default()
            },
        ),
        (
            "my-minio-key",
            vec![0u8; 48],
            SealedKey {
                algorithm: crypto::SEAL_ALGORITHM.to_owned(),
                ..SealedKey::default()
            },
        ),
        (
            "deadbeef",
            vec![0u8; 32],
            SealedKey {
                iv: [0xf7; 32],
                key: [0xea; 64],
                algorithm: crypto::SEAL_ALGORITHM.to_owned(),
            },
        ),
    ];

    for (idx, (key_id, data_key, sealed_key)) in cases.into_iter().enumerate() {
        let metadata = S3.create_metadata(None, key_id, &data_key, sealed_key.clone());
        let (parsed_key_id, parsed_data_key, parsed_key) = S3
            .parse_metadata(&metadata)
            .expect("created metadata should parse");
        assert_eq!(parsed_key_id, key_id, "case {}", idx);
        assert_eq!(parsed_data_key, data_key, "case {}", idx);
        assert_eq!(parsed_key, sealed_key, "case {}", idx);
    }
}

#[test]
fn test_ssec_create_metadata() {
    let cases = [
        SealedKey {
            algorithm: crypto::SEAL_ALGORITHM.to_owned(),
            ..SealedKey::default()
        },
        SealedKey {
            iv: [0xf7; 32],
            key: [0xea; 64],
            algorithm: crypto::SEAL_ALGORITHM.to_owned(),
        },
    ];

    for (idx, sealed_key) in cases.into_iter().enumerate() {
        let metadata = SSEC.create_metadata(None, sealed_key.clone());
        let parsed_key = SSEC
            .parse_metadata(&metadata)
            .expect("created metadata should parse");
        assert_eq!(parsed_key, sealed_key, "case {}", idx);
    }
}

#[test]
fn test_is_etag_sealed() {
    let cases = [
        ("", false),
        ("90682b8e8cc7609c4671e1d64c73fc30", false),
        ("f201040c9dc593e39ea004dc1323699bcd", true),
        ("20000f00fba2ee2ae4845f725964eeb9e092edfabc7ab9f9239e8344341f769a51ce99b4801b0699b92b16a72fa94972", true),
    ];

    for (idx, (etag, expected)) in cases.into_iter().enumerate() {
        let bytes = hex::decode(etag).expect("valid etag hex");
        assert_eq!(crypto::is_etag_sealed(&bytes), expected, "case {}", idx);
    }
}

#[test]
fn test_remove_internal_entries() {
    let cases = [
        (
            metadata([
                (META_MULTIPART, ""),
                (META_IV, ""),
                (META_ALGORITHM, ""),
                (META_SEALED_KEY_SSEC, ""),
                (META_SEALED_KEY_S3, ""),
                (META_SEALED_KEY_KMS, ""),
                (META_KEY_ID, ""),
                (META_DATA_ENCRYPTION_KEY, ""),
            ]),
            Metadata::new(),
        ),
        (
            metadata([
                (META_MULTIPART, ""),
                (META_IV, ""),
                ("X-Amz-Meta-A", "X"),
                ("X-Minio-Internal-B", "Y"),
            ]),
            metadata([("X-Amz-Meta-A", "X"), ("X-Minio-Internal-B", "Y")]),
        ),
    ];

    for (idx, (mut metadata, expected)) in cases.into_iter().enumerate() {
        crypto::remove_internal_entries(&mut metadata);
        assert_eq!(metadata, expected, "case {}", idx);
    }
}

fn metadata<const N: usize>(pairs: [(&str, &str); N]) -> Metadata {
    pairs
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<HashMap<_, _>>()
}
