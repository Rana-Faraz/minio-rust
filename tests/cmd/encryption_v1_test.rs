use std::collections::BTreeMap;

use minio_rust::cmd::{
    decrypt_etag, decrypt_object_info, encrypt_request, get_decrypted_range, get_default_opts,
    normalized_ssec_md5, EncryptionKind, HttpRangeSpec, ObjectInfo, ObjectPartInfo,
    ACTUAL_SIZE_KEY,
};
use minio_rust::internal::crypto::{
    META_ALGORITHM, META_DATA_ENCRYPTION_KEY, META_IV, META_KEY_ID, META_SEALED_KEY_S3,
    META_SEALED_KEY_SSEC,
};

pub const SOURCE_FILE: &str = "cmd/encryption-v1_test.go";

#[test]
fn test_encrypt_request_line_57() {
    let tests = [
        (
            BTreeMap::from([
                (
                    "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key".to_string(),
                    "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key-Md5".to_string(),
                    "bY4wkxQejw9mUJfo72k53A==".to_string(),
                ),
            ]),
            BTreeMap::new(),
        ),
        (
            BTreeMap::from([
                (
                    "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key".to_string(),
                    "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key-Md5".to_string(),
                    "bY4wkxQejw9mUJfo72k53A==".to_string(),
                ),
            ]),
            BTreeMap::from([(
                "X-Amz-Server-Side-Encryption-Customer-Key".to_string(),
                "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=".to_string(),
            )]),
        ),
    ];

    for (index, (headers, mut metadata)) in tests.into_iter().enumerate() {
        encrypt_request(&vec![0; 64], &headers, "bucket", "object", &mut metadata)
            .unwrap_or_else(|err| panic!("case {} encrypt failed: {err}", index + 1));
        assert!(metadata.contains_key(META_ALGORITHM), "case {}", index + 1);
        assert!(metadata.contains_key(META_IV), "case {}", index + 1);
        assert!(
            metadata.contains_key(META_SEALED_KEY_SSEC),
            "case {}",
            index + 1
        );
    }
}

#[test]
fn test_decrypt_object_info_line_124() {
    let tests = [
        (
            ObjectInfo {
                size: 100,
                ..ObjectInfo::default()
            },
            "GET",
            BTreeMap::new(),
            Ok(false),
        ),
        (
            ObjectInfo {
                size: 100,
                user_defined: BTreeMap::from([(
                    META_ALGORITHM.to_string(),
                    "DARE-SHA256".to_string(),
                )]),
                ..ObjectInfo::default()
            },
            "GET",
            BTreeMap::from([(
                "X-Amz-Server-Side-Encryption".to_string(),
                "AES256".to_string(),
            )]),
            Ok(true),
        ),
        (
            ObjectInfo {
                size: 0,
                user_defined: BTreeMap::from([(
                    META_ALGORITHM.to_string(),
                    "DARE-SHA256".to_string(),
                )]),
                ..ObjectInfo::default()
            },
            "GET",
            BTreeMap::from([(
                "X-Amz-Server-Side-Encryption".to_string(),
                "AES256".to_string(),
            )]),
            Ok(true),
        ),
        (
            ObjectInfo {
                size: 100,
                user_defined: BTreeMap::from([(
                    META_SEALED_KEY_SSEC.to_string(),
                    "EAAfAAAAAAD7v1hQq3PFRUHsItalxmrJqrOq6FwnbXNarxOOpb8jTWONPPKyM3Gfjkjyj6NCf+aB/VpHCLCTBA=="
                        .to_string(),
                )]),
                ..ObjectInfo::default()
            },
            "GET",
            BTreeMap::new(),
            Err("encrypted object".to_string()),
        ),
        (
            ObjectInfo {
                size: 100,
                ..ObjectInfo::default()
            },
            "GET",
            BTreeMap::from([(
                "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                "AES256".to_string(),
            )]),
            Err("invalid encryption parameters".to_string()),
        ),
        (
            ObjectInfo {
                size: 100,
                ..ObjectInfo::default()
            },
            "HEAD",
            BTreeMap::from([(
                "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                "AES256".to_string(),
            )]),
            Err("invalid encryption parameters".to_string()),
        ),
        (
            ObjectInfo {
                size: 31,
                user_defined: BTreeMap::from([(
                    META_ALGORITHM.to_string(),
                    "DARE-SHA256".to_string(),
                )]),
                ..ObjectInfo::default()
            },
            "GET",
            BTreeMap::from([(
                "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                "AES256".to_string(),
            )]),
            Err("object tampered".to_string()),
        ),
    ];

    for (index, (info, method, headers, expected)) in tests.into_iter().enumerate() {
        let actual = decrypt_object_info(&info, method, &headers);
        assert_eq!(actual, expected, "case {}", index + 1);
    }
}

#[test]
fn test_decrypt_etag_line_191() {
    let tests = [
        (
            [0u8; 32],
            "20000f00f27834c9a2654927546df57f9e998187496394d4ee80f3d9978f85f3c7d81f72600cdbe03d80dc5a13d69354",
            Ok("8ad3fe6b84bf38489e95c701c84355b6".to_string()),
        ),
        (
            [0u8; 32],
            "20000f00f27834c9a2654927546df57f9e998187496394d4ee80f3d9978f85f3c7d81f72600cdbe03d80dc5a13d6935",
            Err("invalid etag".to_string()),
        ),
        (
            [0u8; 32],
            "00000f00f27834c9a2654927546df57f9e998187496394d4ee80f3d9978f85f3c7d81f72600cdbe03d80dc5a13d69354",
            Err("The secret key does not match the secret key used during upload".to_string()),
        ),
        (
            [0u8; 32],
            "916516b396f0f4d4f2a0e7177557bec4-1",
            Ok("916516b396f0f4d4f2a0e7177557bec4-1".to_string()),
        ),
        (
            [0u8; 32],
            "916516b396f0f4d4f2a0e7177557bec4-Q",
            Err("invalid etag".to_string()),
        ),
    ];

    for (index, (key, etag, expected)) in tests.into_iter().enumerate() {
        assert_eq!(decrypt_etag(key, etag), expected, "case {}", index + 1);
    }
}

#[test]
fn test_get_decrypted_range_issue50_line_210() {
    let range = minio_rust::cmd::parse_request_range_spec("bytes=594870256-594870263")
        .expect("parse range");
    let info = ObjectInfo {
        bucket: "bucket".to_string(),
        name: "object".to_string(),
        size: 595160760,
        user_defined: BTreeMap::from([
            (
                "X-Minio-Internal-Encrypted-Multipart".to_string(),
                "".to_string(),
            ),
            (META_IV.to_string(), "HTexa=".to_string()),
            (META_ALGORITHM.to_string(), "DAREv2-HMAC-SHA256".to_string()),
            (META_SEALED_KEY_SSEC.to_string(), "IAA8PGAA==".to_string()),
            (ACTUAL_SIZE_KEY.to_string(), "594870264".to_string()),
            (
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ),
            (
                "etag".to_string(),
                "166b1545b4c1535294ee0686678bea8c-2".to_string(),
            ),
        ]),
        parts: vec![
            ObjectPartInfo {
                number: 1,
                size: 297580380,
                actual_size: 297435132,
                etag: String::new(),
            },
            ObjectPartInfo {
                number: 2,
                size: 297580380,
                actual_size: 297435132,
                etag: String::new(),
            },
        ],
        ..ObjectInfo::default()
    };

    let actual = get_decrypted_range(&info, Some(&range)).expect("range");
    assert_eq!(actual, (595127964, 32796, 32756, 4538, 1));
}

#[test]
fn test_get_decrypted_range_line_264() {
    let pkg = 64 * 1024_i64;
    let info = ObjectInfo {
        size: 5_487_701 + 32 + 5_487_799 + 32 + 3 + 32,
        user_defined: BTreeMap::from([(
            "X-Minio-Internal-Encrypted-Multipart".to_string(),
            "1".to_string(),
        )]),
        parts: vec![
            ObjectPartInfo {
                number: 1,
                size: 5_487_701 + 32 * ((5_487_701 + pkg - 1) / pkg),
                actual_size: 5_487_701,
                etag: String::new(),
            },
            ObjectPartInfo {
                number: 2,
                size: 5_487_799 + 32 * ((5_487_799 + pkg - 1) / pkg),
                actual_size: 5_487_799,
                etag: String::new(),
            },
            ObjectPartInfo {
                number: 3,
                size: 3 + 32,
                actual_size: 3,
                etag: String::new(),
            },
        ],
        ..ObjectInfo::default()
    };

    assert_eq!(
        get_decrypted_range(&info, None).expect("nil range"),
        (0, info.size, 0, 0, 0)
    );

    let range = HttpRangeSpec::FromTo {
        start: 1_048_576,
        end: Some(2_097_151),
    };
    assert_eq!(
        get_decrypted_range(&info, Some(&range)).expect("middle range"),
        (1_049_088, 1_049_088, 0, 16, 0)
    );

    let suffix = HttpRangeSpec::Suffix {
        length: 6 * 1024 * 1024 + 1,
    };
    let actual = get_decrypted_range(&info, Some(&suffix)).expect("suffix range");
    assert_eq!(actual.2, 30_990);
    assert_eq!(actual.4, 0);
}

#[test]
fn test_get_default_opts_line_633() {
    let valid_key = "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0cHJvdmlkZWQ=";
    let valid_md5 = normalized_ssec_md5(valid_key).expect("valid md5");
    let tests = [
        (
            BTreeMap::from([
                (
                    "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key".to_string(),
                    valid_key.to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key-Md5".to_string(),
                    valid_md5.clone(),
                ),
            ]),
            false,
            None,
            Ok(Some(EncryptionKind::Ssec)),
        ),
        (
            BTreeMap::from([
                (
                    "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key".to_string(),
                    valid_key.to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key-Md5".to_string(),
                    valid_md5.clone(),
                ),
            ]),
            true,
            None,
            Ok(None),
        ),
        (
            BTreeMap::from([
                (
                    "X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key".to_string(),
                    "Mz".to_string(),
                ),
                (
                    "X-Amz-Server-Side-Encryption-Customer-Key-Md5".to_string(),
                    valid_md5.clone(),
                ),
            ]),
            false,
            None,
            Err("The SSE-C client key is invalid".to_string()),
        ),
        (
            BTreeMap::from([(
                "X-Amz-Server-Side-Encryption".to_string(),
                "AES256".to_string(),
            )]),
            false,
            None,
            Ok(Some(EncryptionKind::S3)),
        ),
        (
            BTreeMap::new(),
            false,
            Some(BTreeMap::from([
                (META_SEALED_KEY_S3.to_string(), "AAAA".to_string()),
                (META_KEY_ID.to_string(), "kms-key".to_string()),
                (META_DATA_ENCRYPTION_KEY.to_string(), "m-key".to_string()),
            ])),
            Ok(Some(EncryptionKind::S3)),
        ),
        (
            BTreeMap::new(),
            true,
            Some(BTreeMap::from([
                (META_SEALED_KEY_S3.to_string(), "AAAA".to_string()),
                (META_KEY_ID.to_string(), "kms-key".to_string()),
                (META_DATA_ENCRYPTION_KEY.to_string(), "m-key".to_string()),
            ])),
            Ok(None),
        ),
        (
            BTreeMap::from([
                (
                    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key".to_string(),
                    valid_key.to_string(),
                ),
                (
                    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5".to_string(),
                    valid_md5.clone(),
                ),
            ]),
            true,
            None,
            Ok(Some(EncryptionKind::Ssec)),
        ),
        (
            BTreeMap::from([
                (
                    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm".to_string(),
                    "AES256".to_string(),
                ),
                (
                    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key".to_string(),
                    valid_key.to_string(),
                ),
                (
                    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5".to_string(),
                    valid_md5,
                ),
            ]),
            false,
            None,
            Ok(None),
        ),
    ];

    for (index, (headers, copy_source, metadata, expected)) in tests.into_iter().enumerate() {
        let actual = get_default_opts(&headers, copy_source, metadata.as_ref())
            .map(|opts| opts.server_side_encryption);
        assert_eq!(actual, expected, "case {}", index + 1);
    }
}
