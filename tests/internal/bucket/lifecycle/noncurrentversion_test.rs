use minio_rust::internal::bucket::lifecycle::{Error, NoncurrentVersionExpiration};

#[test]
fn test_noncurrent_versions_expiration_validation_line_22() {
    let cases = [
        (
            NoncurrentVersionExpiration {
                noncurrent_days: None,
                newer_noncurrent_versions: 0,
                set: true,
            },
            Some(Error::XmlNotWellFormed),
        ),
        (
            NoncurrentVersionExpiration {
                noncurrent_days: Some(90),
                newer_noncurrent_versions: 0,
                set: true,
            },
            None,
        ),
        (
            NoncurrentVersionExpiration {
                noncurrent_days: Some(90),
                newer_noncurrent_versions: 2,
                set: true,
            },
            None,
        ),
        (
            NoncurrentVersionExpiration {
                noncurrent_days: Some(-1),
                newer_noncurrent_versions: 0,
                set: true,
            },
            Some(Error::XmlNotWellFormed),
        ),
        (
            NoncurrentVersionExpiration {
                noncurrent_days: Some(90),
                newer_noncurrent_versions: -2,
                set: true,
            },
            Some(Error::XmlNotWellFormed),
        ),
        (
            NoncurrentVersionExpiration {
                noncurrent_days: None,
                newer_noncurrent_versions: 5,
                set: true,
            },
            None,
        ),
    ];

    for (value, expected) in cases {
        match expected {
            None => assert!(value.validate().is_ok()),
            Some(error) => assert_eq!(value.validate().unwrap_err(), error),
        }
    }
}
