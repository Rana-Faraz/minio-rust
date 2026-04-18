use minio_rust::internal::event::{Arn, TargetId};

#[test]
fn target_id_string_matches_reference_cases() {
    let cases = [
        (TargetId::default(), ":"),
        (TargetId::new("1", "webhook"), "1:webhook"),
        (
            TargetId::new(
                "httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531",
                "localhost:55638",
            ),
            "httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531:localhost:55638",
        ),
    ];

    for (target_id, expected) in cases {
        assert_eq!(target_id.to_string(), expected);
    }
}

#[test]
fn target_id_to_arn_matches_reference_cases() {
    let target_id = TargetId::new("1", "webhook");
    let cases = [
        (
            target_id.clone(),
            "",
            Arn {
                target_id: target_id.clone(),
                region: String::new(),
            },
        ),
        (
            target_id.clone(),
            "us-east-1",
            Arn {
                target_id,
                region: "us-east-1".to_owned(),
            },
        ),
    ];

    for (target_id, region, expected) in cases {
        assert_eq!(target_id.to_arn(region), expected);
    }
}

#[test]
fn target_id_marshal_json_matches_reference_cases() {
    let cases = [
        (TargetId::default(), "\":\""),
        (TargetId::new("1", "webhook"), "\"1:webhook\""),
        (
            TargetId::new(
                "httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531",
                "localhost:55638",
            ),
            "\"httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531:localhost:55638\"",
        ),
    ];

    for (target_id, expected) in cases {
        let data = target_id
            .marshal_json()
            .expect("target id json marshal should succeed");
        assert_eq!(String::from_utf8(data).expect("json is utf-8"), expected);
    }
}

#[test]
fn target_id_unmarshal_json_matches_reference_cases() {
    let cases = [
        (br#""""#.as_slice(), None, true),
        (
            br#""httpclient+2e33cdee-fbec-4bdd-917e-7d8e3c5a2531:localhost:55638""#.as_slice(),
            None,
            true,
        ),
        (b"\":\"".as_slice(), Some(TargetId::default()), false),
        (
            br#""1:webhook""#.as_slice(),
            Some(TargetId::new("1", "webhook")),
            false,
        ),
    ];

    for (data, expected, should_err) in cases {
        let result = TargetId::unmarshal_json(data);
        assert_eq!(result.is_err(), should_err);
        if let Ok(target_id) = result {
            assert_eq!(Some(target_id), expected);
        }
    }
}
