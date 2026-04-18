use minio_rust::internal::arn::{self, Arn};

pub const SOURCE_FILE: &str = "internal/arn/arn_test.go";

#[test]
fn arn_string_matches_reference_cases() {
    let test_cases = [
        (
            Arn {
                partition: "minio".to_owned(),
                service: "iam".to_owned(),
                region: "us-east-1".to_owned(),
                resource_type: "role".to_owned(),
                resource_id: "my-role".to_owned(),
            },
            "arn:minio:iam:us-east-1::role/my-role",
        ),
        (
            Arn {
                partition: "minio".to_owned(),
                service: String::new(),
                region: "us-east-1".to_owned(),
                resource_type: "role".to_owned(),
                resource_id: "my-role".to_owned(),
            },
            "arn:minio::us-east-1::role/my-role",
        ),
    ];

    for (arn_value, expected) in test_cases {
        assert_eq!(arn_value.to_string(), expected);
    }
}

#[test]
fn new_iam_role_arn_matches_reference_cases() {
    let success = arn::new_iam_role_arn("my-role", "us-east-1").expect("expected success");
    assert_eq!(
        success,
        Arn {
            partition: "minio".to_owned(),
            service: "iam".to_owned(),
            region: "us-east-1".to_owned(),
            resource_type: "role".to_owned(),
            resource_id: "my-role".to_owned(),
        }
    );

    let empty_region =
        arn::new_iam_role_arn("my-role", "").expect("empty region should still succeed");
    assert_eq!(empty_region.region, "");

    assert!(arn::new_iam_role_arn("", "us-east-1").is_err());
    assert!(arn::new_iam_role_arn("=", "us-east-1").is_err());
}

#[test]
fn parse_matches_reference_cases() {
    let success =
        arn::parse("arn:minio:iam:us-east-1::role/my-role").expect("expected parse success");
    assert_eq!(
        success,
        Arn {
            partition: "minio".to_owned(),
            service: "iam".to_owned(),
            region: "us-east-1".to_owned(),
            resource_type: "role".to_owned(),
            resource_id: "my-role".to_owned(),
        }
    );

    assert!(arn::parse("arn:minio:").is_err());
    assert!(arn::parse("arn:invalid:iam:us-east-1::role/my-role").is_err());
    assert!(arn::parse("arn:minio:invalid:us-east-1::role/my-role").is_err());
    assert!(arn::parse("arn:minio:iam:us-east-1::invalid").is_err());
}
