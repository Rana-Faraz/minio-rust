use minio_rust::internal::bucket::lifecycle::{parse_lifecycle_config, Error};

fn parse_rule(fragment: &str) -> Result<minio_rust::internal::bucket::lifecycle::Rule, Error> {
    parse_lifecycle_config(&format!(
        "<LifecycleConfiguration>{fragment}</LifecycleConfiguration>"
    ))
    .map(|lc| lc.rules[0].clone())
}

#[test]
fn test_invalid_rules_line_28() {
    let long_id = "b".repeat(256);
    let cases = vec![
        (
            format!("<Rule><ID>{long_id}</ID></Rule>"),
            Error::InvalidRuleId,
        ),
        (
            "<Rule><ID></ID><Filter><Prefix></Prefix></Filter><Expiration><Days>365</Days></Expiration><Status>Enabled</Status></Rule>"
                .to_owned(),
            Error::Parse(String::new()),
        ),
        (
            "<Rule><ID>rule with empty status</ID><Status></Status></Rule>".to_owned(),
            Error::EmptyRuleStatus,
        ),
        (
            "<Rule><ID>rule with invalid status</ID><Status>OK</Status></Rule>".to_owned(),
            Error::InvalidRuleStatus,
        ),
        (
            "<Rule><ID>negative-obj-size-less-than</ID><Filter><ObjectSizeLessThan>-1</ObjectSizeLessThan></Filter><Expiration><Days>365</Days></Expiration><Status>Enabled</Status></Rule>"
                .to_owned(),
            Error::XmlNotWellFormed,
        ),
        (
            "<Rule><ID>negative-and-obj-size-greater-than</ID><Filter><And><ObjectSizeGreaterThan>-1</ObjectSizeGreaterThan></And></Filter><Expiration><Days>365</Days></Expiration><Status>Enabled</Status></Rule>"
                .to_owned(),
            Error::XmlNotWellFormed,
        ),
        (
            "<Rule><ID>Rule with a tag and DelMarkerExpiration</ID><Filter><Tag><Key>k1</Key><Value>v1</Value></Tag></Filter><DelMarkerExpiration><Days>365</Days></DelMarkerExpiration><Status>Enabled</Status></Rule>"
                .to_owned(),
            Error::InvalidRuleDelMarkerExpiration,
        ),
        (
            "<Rule><ID>Rule with multiple tags and DelMarkerExpiration</ID><Filter><And><Tag><Key>k1</Key><Value>v1</Value></Tag><Tag><Key>k2</Key><Value>v2</Value></Tag></And></Filter><DelMarkerExpiration><Days>365</Days></DelMarkerExpiration><Status>Enabled</Status></Rule>"
                .to_owned(),
            Error::InvalidRuleDelMarkerExpiration,
        ),
    ];

    for (xml, expected) in cases {
        let rule = parse_rule(&xml).expect("rule should parse");
        match expected {
            Error::Parse(_) => assert!(rule.validate().is_ok()),
            other => assert_eq!(rule.validate().unwrap_err(), other),
        }
    }
}

#[test]
fn subtest_test_invalid_rules_fmt_sprintf_test_d_line_136() {
    test_invalid_rules_line_28();
}
