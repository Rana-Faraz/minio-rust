use std::collections::HashMap;

use minio_rust::internal::bucket::object::lock::{
    filter_object_lock_metadata, get_object_legal_hold_meta, get_object_retention_meta,
    is_object_lock_governance_bypass_set, is_object_lock_requested, parse_legal_hold_status,
    parse_object_legal_hold, parse_object_lock_config, parse_object_lock_legal_hold_headers,
    parse_object_lock_retention_headers, parse_object_retention, parse_ret_mode, Config,
    DefaultRetention, Error, LegalHoldStatus, RetMode, Rule, AMZ_OBJECT_LOCK_BYPASS_RET_GOVERNANCE,
    AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, ENABLED,
};

pub const SOURCE_FILE: &str = "internal/bucket/object/lock/lock_test.go";

#[test]
fn parse_mode_matches_reference_cases() {
    assert_eq!(parse_ret_mode("governance"), RetMode::Governance);
    assert_eq!(parse_ret_mode("complIAnce"), RetMode::Compliance);
    assert_eq!(parse_ret_mode("gce"), RetMode::Empty);
}

#[test]
fn parse_legal_hold_status_matches_reference_cases() {
    assert_eq!(parse_legal_hold_status("ON"), LegalHoldStatus::On);
    assert_eq!(parse_legal_hold_status("Off"), LegalHoldStatus::Off);
    assert_eq!(parse_legal_hold_status("x"), LegalHoldStatus::Empty);
}

#[test]
fn unmarshal_default_retention_matches_reference_cases() {
    let invalid_days = 36_501_u64;
    let cases = [
        (
            DefaultRetention {
                mode: RetMode::Empty,
                days: Some(4),
                years: None,
            },
            true,
        ),
        (
            DefaultRetention {
                mode: RetMode::Governance,
                days: None,
                years: None,
            },
            true,
        ),
        (
            DefaultRetention {
                mode: RetMode::Governance,
                days: Some(4),
                years: None,
            },
            false,
        ),
        (
            DefaultRetention {
                mode: RetMode::Governance,
                days: None,
                years: Some(1),
            },
            false,
        ),
        (
            DefaultRetention {
                mode: RetMode::Governance,
                days: Some(4),
                years: Some(1),
            },
            true,
        ),
        (
            DefaultRetention {
                mode: RetMode::Governance,
                days: Some(0),
                years: None,
            },
            true,
        ),
        (
            DefaultRetention {
                mode: RetMode::Governance,
                days: Some(invalid_days),
                years: None,
            },
            true,
        ),
    ];

    for (case, should_fail) in cases {
        let result = case
            .to_xml()
            .and_then(|xml| DefaultRetention::from_xml(&xml));
        assert_eq!(result.is_err(), should_fail);
    }
}

#[test]
fn parse_object_lock_config_matches_reference_cases() {
    let cases = [
        (
            r#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>yes</ObjectLockEnabled></ObjectLockConfiguration>"#,
            true,
        ),
        (
            r#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>0</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#,
            true,
        ),
        (
            r#"<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>"#,
            false,
        ),
    ];

    for (xml, should_fail) in cases {
        let result = parse_object_lock_config(xml.as_bytes());
        assert_eq!(result.is_err(), should_fail);
    }
}

#[test]
fn parse_object_retention_matches_reference_cases() {
    let cases = [
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>string</Mode><RetainUntilDate>2020-01-02T15:04:05Z</RetainUntilDate></Retention>"#,
            Err(Error::UnknownWormModeDirective),
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>COMPLIANCE</Mode><RetainUntilDate>2017-01-02T15:04:05Z</RetainUntilDate></Retention>"#,
            Err(Error::PastObjectLockRetainDate),
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>2057-01-02T15:04:05Z</RetainUntilDate></Retention>"#,
            Ok(()),
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>GOVERNANCE</Mode><RetainUntilDate>2057-01-02T15:04:05.000Z</RetainUntilDate></Retention>"#,
            Ok(()),
        ),
    ];

    for (xml, expected) in cases {
        let result = parse_object_retention(xml.as_bytes()).map(|_| ());
        assert_eq!(result, expected);
    }
}

#[test]
fn is_object_lock_requested_matches_reference_cases() {
    let cases = [
        (HashMap::new(), false),
        (
            HashMap::from([(AMZ_OBJECT_LOCK_LEGAL_HOLD.to_owned(), String::new())]),
            true,
        ),
        (
            HashMap::from([
                (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_owned(), String::new()),
                (AMZ_OBJECT_LOCK_MODE.to_owned(), String::new()),
            ]),
            true,
        ),
        (
            HashMap::from([(
                AMZ_OBJECT_LOCK_BYPASS_RET_GOVERNANCE.to_owned(),
                String::new(),
            )]),
            false,
        ),
    ];

    for (headers, expected) in cases {
        assert_eq!(is_object_lock_requested(&headers), expected);
    }
}

#[test]
fn is_object_lock_governance_bypass_set_matches_reference_cases() {
    let cases = [
        (HashMap::new(), false),
        (
            HashMap::from([(AMZ_OBJECT_LOCK_LEGAL_HOLD.to_owned(), String::new())]),
            false,
        ),
        (
            HashMap::from([
                (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_owned(), String::new()),
                (AMZ_OBJECT_LOCK_MODE.to_owned(), String::new()),
            ]),
            false,
        ),
        (
            HashMap::from([(
                AMZ_OBJECT_LOCK_BYPASS_RET_GOVERNANCE.to_owned(),
                String::new(),
            )]),
            false,
        ),
        (
            HashMap::from([(
                AMZ_OBJECT_LOCK_BYPASS_RET_GOVERNANCE.to_owned(),
                "true".to_owned(),
            )]),
            true,
        ),
    ];

    for (headers, expected) in cases {
        assert_eq!(is_object_lock_governance_bypass_set(&headers), expected);
    }
}

#[test]
fn parse_object_lock_retention_headers_matches_reference_cases() {
    let cases = [
        (HashMap::new(), Err(Error::ObjectLockInvalidHeaders)),
        (
            HashMap::from([
                (AMZ_OBJECT_LOCK_MODE.to_owned(), "lock".to_owned()),
                (
                    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_owned(),
                    "2017-01-02".to_owned(),
                ),
            ]),
            Err(Error::UnknownWormModeDirective),
        ),
        (
            HashMap::from([(AMZ_OBJECT_LOCK_MODE.to_owned(), "governance".to_owned())]),
            Err(Error::ObjectLockInvalidHeaders),
        ),
        (
            HashMap::from([
                (
                    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_owned(),
                    "2017-01-02".to_owned(),
                ),
                (AMZ_OBJECT_LOCK_MODE.to_owned(), "governance".to_owned()),
            ]),
            Err(Error::InvalidRetentionDate),
        ),
        (
            HashMap::from([
                (
                    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_owned(),
                    "2017-01-02T15:04:05Z".to_owned(),
                ),
                (AMZ_OBJECT_LOCK_MODE.to_owned(), "governance".to_owned()),
            ]),
            Err(Error::PastObjectLockRetainDate),
        ),
        (
            HashMap::from([
                (
                    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_owned(),
                    "2087-01-02T15:04:05Z".to_owned(),
                ),
                (AMZ_OBJECT_LOCK_MODE.to_owned(), "governance".to_owned()),
            ]),
            Ok(RetMode::Governance),
        ),
        (
            HashMap::from([
                (
                    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_owned(),
                    "2087-01-02T15:04:05.000Z".to_owned(),
                ),
                (AMZ_OBJECT_LOCK_MODE.to_owned(), "governance".to_owned()),
            ]),
            Ok(RetMode::Governance),
        ),
    ];

    for (headers, expected) in cases {
        let result = parse_object_lock_retention_headers(&headers).map(|(mode, _)| mode);
        assert_eq!(result, expected);
    }
}

#[test]
fn get_object_retention_meta_matches_reference_cases() {
    let empty = get_object_retention_meta(&HashMap::new());
    assert_eq!(empty.mode, RetMode::Empty);

    let mode_only = get_object_retention_meta(&HashMap::from([(
        "x-amz-object-lock-mode".to_owned(),
        "governance".to_owned(),
    )]));
    assert_eq!(mode_only.mode, RetMode::Governance);

    let date_only = get_object_retention_meta(&HashMap::from([(
        "x-amz-object-lock-retain-until-date".to_owned(),
        "2020-02-01".to_owned(),
    )]));
    assert!(!date_only.retain_until_date.is_zero());
}

#[test]
fn get_object_legal_hold_meta_matches_reference_cases() {
    assert_eq!(
        get_object_legal_hold_meta(&HashMap::new()).status,
        LegalHoldStatus::Empty
    );
    assert_eq!(
        get_object_legal_hold_meta(&HashMap::from([(
            "x-amz-object-lock-legal-hold".to_owned(),
            "on".to_owned(),
        )]))
        .status,
        LegalHoldStatus::On
    );
    assert_eq!(
        get_object_legal_hold_meta(&HashMap::from([(
            "x-amz-object-lock-legal-hold".to_owned(),
            "off".to_owned(),
        )]))
        .status,
        LegalHoldStatus::Off
    );
}

#[test]
fn parse_object_legal_hold_matches_reference_cases() {
    let cases = [
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>string</Status></LegalHold>"#,
            true,
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></LegalHold>"#,
            false,
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><ObjectLockLegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></ObjectLockLegalHold>"#,
            false,
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><ObjectLockLegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><MyStatus>ON</MyStatus></ObjectLockLegalHold>"#,
            true,
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><UnknownLegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>ON</Status></UnknownLegalHold>"#,
            true,
        ),
        (
            r#"<?xml version="1.0" encoding="UTF-8"?><LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>On</Status></LegalHold>"#,
            true,
        ),
    ];

    for (xml, should_fail) in cases {
        assert_eq!(
            parse_object_legal_hold(xml.as_bytes()).is_err(),
            should_fail
        );
    }
}

#[test]
fn filter_object_lock_metadata_matches_reference_cases() {
    let cases = [
        (
            HashMap::from([(
                "Authorization".to_owned(),
                "AWS4-HMAC-SHA256 <cred_string>".to_owned(),
            )]),
            false,
            false,
            HashMap::from([(
                "Authorization".to_owned(),
                "AWS4-HMAC-SHA256 <cred_string>".to_owned(),
            )]),
        ),
        (
            HashMap::from([("x-amz-object-lock-mode".to_owned(), "governance".to_owned())]),
            false,
            false,
            HashMap::from([("x-amz-object-lock-mode".to_owned(), "governance".to_owned())]),
        ),
        (
            HashMap::from([
                ("x-amz-object-lock-mode".to_owned(), "governance".to_owned()),
                (
                    "x-amz-object-lock-retain-until-date".to_owned(),
                    "2020-02-01".to_owned(),
                ),
            ]),
            true,
            false,
            HashMap::new(),
        ),
        (
            HashMap::from([("x-amz-object-lock-legal-hold".to_owned(), "off".to_owned())]),
            false,
            true,
            HashMap::new(),
        ),
        (
            HashMap::from([("x-amz-object-lock-legal-hold".to_owned(), "on".to_owned())]),
            false,
            false,
            HashMap::from([("x-amz-object-lock-legal-hold".to_owned(), "on".to_owned())]),
        ),
    ];

    for (metadata, filter_retention, filter_legal_hold, expected) in cases {
        assert_eq!(
            filter_object_lock_metadata(&metadata, filter_retention, filter_legal_hold),
            expected
        );
    }
}

#[test]
fn to_string_matches_reference_cases() {
    let cases = [
        (
            Config {
                object_lock_enabled: ENABLED.to_owned(),
                ..Config::default()
            },
            "Enabled: true".to_owned(),
        ),
        (
            Config {
                object_lock_enabled: ENABLED.to_owned(),
                rule: Some(Rule {
                    default_retention: DefaultRetention {
                        mode: RetMode::Governance,
                        days: Some(30),
                        years: None,
                    },
                }),
                ..Config::default()
            },
            "Enabled: true, Mode: GOVERNANCE, Days: 30".to_owned(),
        ),
        (
            Config {
                object_lock_enabled: ENABLED.to_owned(),
                rule: Some(Rule {
                    default_retention: DefaultRetention {
                        mode: RetMode::Compliance,
                        days: None,
                        years: Some(2),
                    },
                }),
                ..Config::default()
            },
            "Enabled: true, Mode: COMPLIANCE, Years: 2".to_owned(),
        ),
        (Config::default(), "Enabled: false".to_owned()),
    ];

    for (config, expected) in cases {
        assert_eq!(config.to_string(), expected);
    }
}

#[test]
fn parse_object_lock_legal_hold_headers_matches_reference_cases() {
    let hold = parse_object_lock_legal_hold_headers(&HashMap::from([(
        AMZ_OBJECT_LOCK_LEGAL_HOLD.to_owned(),
        "ON".to_owned(),
    )]))
    .expect("valid legal hold header should parse");
    assert_eq!(hold.status, LegalHoldStatus::On);
    assert_eq!(
        parse_object_lock_legal_hold_headers(&HashMap::from([(
            AMZ_OBJECT_LOCK_LEGAL_HOLD.to_owned(),
            "invalid".to_owned(),
        )])),
        Err(Error::UnknownWormModeDirective)
    );
}
