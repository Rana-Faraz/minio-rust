use minio_rust::internal::config::lambda::event::{Arn, TargetId};

#[test]
fn arn_string_matches_reference_cases() {
    let cases = [
        (Arn::default(), ""),
        (
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            },
            "arn:minio:s3-object-lambda::1:webhook",
        ),
        (
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "us-east-1".to_owned(),
            },
            "arn:minio:s3-object-lambda:us-east-1:1:webhook",
        ),
    ];

    for (arn, expected) in cases {
        assert_eq!(arn.to_string(), expected);
    }
}

#[test]
fn parse_arn_matches_reference_cases() {
    let cases = [
        ("", None, true),
        ("arn:minio:s3-object-lambda:::", None, true),
        ("arn:minio:s3-object-lambda::1:webhook:remote", None, true),
        ("arn:aws:s3-object-lambda::1:webhook", None, true),
        ("arn:minio:sns::1:webhook", None, true),
        (
            "arn:minio:s3-object-lambda::1:webhook",
            Some(Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            }),
            false,
        ),
        (
            "arn:minio:s3-object-lambda:us-east-1:1:webhook",
            Some(Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "us-east-1".to_owned(),
            }),
            false,
        ),
    ];

    for (value, expected, should_err) in cases {
        let result = Arn::parse(value);
        assert_eq!(result.is_err(), should_err);
        if let Ok(arn) = result {
            assert_eq!(Some(arn), expected);
        }
    }
}
