use minio_rust::internal::event::{Arn, TargetId};

#[test]
fn arn_string_matches_reference_cases() {
    let cases = [
        (Arn::default(), ""),
        (
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            },
            "arn:minio:sqs::1:webhook",
        ),
        (
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "us-east-1".to_owned(),
            },
            "arn:minio:sqs:us-east-1:1:webhook",
        ),
    ];

    for (arn, expected) in cases {
        assert_eq!(arn.to_string(), expected);
    }
}

#[test]
fn arn_marshal_xml_matches_reference_cases() {
    let cases = [
        (Arn::default(), "<ARN></ARN>"),
        (
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            },
            "<ARN>arn:minio:sqs::1:webhook</ARN>",
        ),
        (
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "us-east-1".to_owned(),
            },
            "<ARN>arn:minio:sqs:us-east-1:1:webhook</ARN>",
        ),
    ];

    for (arn, expected) in cases {
        assert_eq!(arn.marshal_xml(), expected);
    }
}

#[test]
fn arn_unmarshal_xml_matches_reference_cases() {
    let cases = [
        ("<ARN></ARN>", None, true),
        ("<ARN>arn:minio:sqs:::</ARN>", None, true),
        (
            "<ARN>arn:minio:sqs::1:webhook</ARN>",
            Some(Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            }),
            false,
        ),
        (
            "<ARN>arn:minio:sqs:us-east-1:1:webhook</ARN>",
            Some(Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "us-east-1".to_owned(),
            }),
            false,
        ),
    ];

    for (data, expected, should_err) in cases {
        let result = Arn::unmarshal_xml(data.as_bytes());
        assert_eq!(result.is_err(), should_err);
        if let Ok(arn) = result {
            assert_eq!(Some(arn), expected);
        }
    }
}

#[test]
fn parse_arn_matches_reference_cases() {
    let cases = [
        ("", None, true),
        ("arn:minio:sqs:::", None, true),
        ("arn:minio:sqs::1:webhook:remote", None, true),
        ("arn:aws:sqs::1:webhook", None, true),
        ("arn:minio:sns::1:webhook", None, true),
        (
            "arn:minio:sqs::1:webhook",
            Some(Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            }),
            false,
        ),
        (
            "arn:minio:sqs:us-east-1:1:webhook",
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
