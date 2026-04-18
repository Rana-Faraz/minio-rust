use minio_rust::internal::bucket::lifecycle::{parse_lifecycle_config, Error};

#[test]
fn test_transition_unmarshal_xml_line_25() {
    let valid = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Transition><Days>0</Days><StorageClass>S3TIER-1</StorageClass></Transition></Rule></LifecycleConfiguration>",
    )
    .expect("valid transition should parse");
    assert!(valid.rules[0].transition.validate().is_ok());

    let both = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Transition><Days>1</Days><Date>2021-01-01T00:00:00Z</Date><StorageClass>S3TIER-1</StorageClass></Transition></Rule></LifecycleConfiguration>",
    )
    .expect("transition with days/date should parse");
    assert_eq!(
        both.rules[0].transition.validate().unwrap_err(),
        Error::TransitionInvalid
    );

    let missing_storage = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Transition><Days>1</Days></Transition></Rule></LifecycleConfiguration>",
    )
    .expect("transition missing storage class should parse");
    assert_eq!(
        missing_storage.rules[0].transition.validate().unwrap_err(),
        Error::XmlNotWellFormed
    );

    let noncurrent = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><NoncurrentVersionTransition><NoncurrentDays>0</NoncurrentDays><StorageClass>S3TIER-1</StorageClass></NoncurrentVersionTransition></Rule></LifecycleConfiguration>",
    )
    .expect("valid noncurrent transition should parse");
    assert!(noncurrent.rules[0]
        .noncurrent_version_transition
        .validate()
        .is_ok());

    let invalid_noncurrent = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><NoncurrentVersionTransition><Days>1</Days></NoncurrentVersionTransition></Rule></LifecycleConfiguration>",
    )
    .expect("invalid noncurrent transition should parse");
    assert_eq!(
        invalid_noncurrent.rules[0]
            .noncurrent_version_transition
            .validate()
            .unwrap_err(),
        Error::XmlNotWellFormed
    );
}
