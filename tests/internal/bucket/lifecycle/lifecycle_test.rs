use chrono::{Duration, TimeZone, Utc};
use minio_rust::internal::bucket::lifecycle::{
    expected_expiry_time, parse_lifecycle_config, parse_lifecycle_config_with_id,
    parse_noncurrent_version_expiration, Action, And, Boolean, DelMarkerExpiration, Error,
    Evaluator, Expiration, Filter, Lifecycle, NoncurrentVersionExpiration,
    NoncurrentVersionTransition, ObjectOpts, Prefix, Retention, Rule, Tag, Transition,
    AMZ_EXPIRATION, ENABLED, MINIO_TRANSITION,
};

const MIB: i64 = 1024 * 1024;

fn ts(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, min, s).single().unwrap()
}

#[test]
fn test_parse_and_validate_lifecycle_config_line_37() {
    let cases = [
        (
            "<LifecycleConfiguration><Rule><ID>testRule1</ID><Filter><Prefix>prefix</Prefix></Filter><Status>Enabled</Status><Expiration><Days>3</Days></Expiration></Rule><Rule><ID>testRule2</ID><Filter><Prefix>another-prefix</Prefix></Filter><Status>Enabled</Status><Expiration><Days>3</Days></Expiration></Rule></LifecycleConfiguration>",
            None,
            None,
            Retention::default(),
        ),
        (
            "<LifecycleConfiguration><Rule><ID>expired-all-lock</ID><Filter><Prefix>prefix</Prefix></Filter><Status>Enabled</Status><Expiration><Days>3</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration></Rule></LifecycleConfiguration>",
            None,
            Some(Error::LifecycleBucketLocked),
            Retention { lock_enabled: true },
        ),
        (
            "<LifecycleConfiguration><Rule><ID>delmarker-lock</ID><Filter><Prefix>prefix</Prefix></Filter><Status>Enabled</Status><DelMarkerExpiration><Days>3</Days></DelMarkerExpiration></Rule></LifecycleConfiguration>",
            None,
            Some(Error::LifecycleBucketLocked),
            Retention { lock_enabled: true },
        ),
        (
            "<LifecycleConfiguration></LifecycleConfiguration>",
            None,
            Some(Error::LifecycleNoRule),
            Retention::default(),
        ),
        (
            "<LifecycleConfiguration><Rule><Filter><And><Tag><Key>key1</Key><Value>val1</Value><Key>key2</Key><Value>val2</Value></Tag></And></Filter><Expiration><Days>3</Days></Expiration></Rule></LifecycleConfiguration>",
            Some(Error::DuplicatedXmlTag),
            None,
            Retention::default(),
        ),
        (
            "<LifecycleConfiguration><Rule><Expiration><Days>3</Days></Expiration><Status>Enabled</Status></Rule></LifecycleConfiguration>",
            None,
            None,
            Retention::default(),
        ),
        (
            "<LifecycleConfiguration><Rule><ID>duplicateID</ID><Status>Enabled</Status><Filter><Prefix>/a/b</Prefix></Filter><Expiration><Days>3</Days></Expiration></Rule><Rule><ID>duplicateID</ID><Status>Enabled</Status><Filter><And><Prefix>/x/z</Prefix><Tag><Key>key1</Key><Value>val1</Value></Tag></And></Filter><Expiration><Days>4</Days></Expiration></Rule></LifecycleConfiguration>",
            None,
            Some(Error::LifecycleDuplicateId),
            Retention::default(),
        ),
        (
            "<LifecycleConfiguration><Rule><ID>rule</ID><Filter></Filter><Status>Enabled</Status><Transition><Days>0</Days><StorageClass>S3TIER-1</StorageClass></Transition></Rule></LifecycleConfiguration>",
            None,
            None,
            Retention::default(),
        ),
    ];

    for (xml, parse_error, validate_error, retention) in cases {
        let parsed = parse_lifecycle_config(xml);
        match parse_error {
            Some(error) => assert_eq!(parsed.unwrap_err(), error),
            None => {
                let lifecycle = parsed.expect("lifecycle should parse");
                match validate_error {
                    Some(error) => assert_eq!(lifecycle.validate(retention).unwrap_err(), error),
                    None => assert!(lifecycle.validate(retention).is_ok()),
                }
            }
        }
    }
}

#[test]
fn subtest_test_parse_and_validate_lifecycle_config_fmt_sprintf_test_d_line_188() {
    test_parse_and_validate_lifecycle_config_line_37();
}

#[test]
fn test_marshal_lifecycle_config_line_208() {
    let lifecycle = Lifecycle {
        rules: vec![
            Rule {
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    prefix: Prefix::new("prefix-1"),
                    ..Filter::default()
                },
                expiration: Expiration {
                    days: Some(3),
                    set: true,
                    ..Expiration::default()
                },
                ..Rule::default()
            },
            Rule {
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    prefix: Prefix::new("prefix-2"),
                    ..Filter::default()
                },
                expiration: Expiration {
                    date: Some(ts(2019, 4, 20, 0, 0, 0)),
                    set: true,
                    ..Expiration::default()
                },
                ..Rule::default()
            },
            Rule {
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    prefix: Prefix::new("prefix-3"),
                    ..Filter::default()
                },
                expiration: Expiration {
                    date: Some(ts(2019, 4, 20, 0, 0, 0)),
                    set: true,
                    ..Expiration::default()
                },
                noncurrent_version_transition: NoncurrentVersionTransition {
                    noncurrent_days: 2,
                    storage_class: "TEST".to_owned(),
                    set: true,
                },
                ..Rule::default()
            },
        ],
        ..Lifecycle::default()
    };

    let roundtrip = parse_lifecycle_config(&lifecycle.to_xml()).expect("roundtrip lifecycle");
    assert_eq!(roundtrip.rules.len(), lifecycle.rules.len());
    assert_eq!(roundtrip.rules[0].expiration.days, Some(3));
    assert_eq!(
        roundtrip.rules[1].expiration.date,
        Some(ts(2019, 4, 20, 0, 0, 0))
    );
    assert_eq!(
        roundtrip.rules[2]
            .noncurrent_version_transition
            .storage_class,
        "TEST"
    );
}

#[test]
fn test_expected_expiry_time_line_260() {
    assert_eq!(
        expected_expiry_time(ts(2020, 3, 15, 10, 10, 10), 4),
        ts(2020, 3, 20, 0, 0, 0)
    );
    assert_eq!(
        expected_expiry_time(ts(2020, 3, 15, 0, 0, 0), 1),
        ts(2020, 3, 17, 0, 0, 0)
    );
}

#[test]
fn subtest_test_expected_expiry_time_fmt_sprintf_test_d_line_279() {
    test_expected_expiry_time_line_260();
}

#[test]
fn test_eval_line_288() {
    let now = Utc::now();

    let delete = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>",
    )
    .expect("lifecycle");
    let event = delete.eval(ObjectOpts {
        name: "foodir/fooobject".to_owned(),
        mod_time: Some(now - Duration::days(6)),
        is_latest: true,
        num_versions: 1,
        ..ObjectOpts::default()
    });
    assert_eq!(event.action, Action::Delete);

    let transition = parse_lifecycle_config(
        "<BucketLifecycleConfiguration><Rule><Filter></Filter><Status>Enabled</Status><Transition><Days>0</Days><StorageClass>S3TIER-1</StorageClass></Transition></Rule></BucketLifecycleConfiguration>",
    )
    .expect("transition lifecycle");
    let event = transition.eval(ObjectOpts {
        name: "foodir/fooobject".to_owned(),
        mod_time: Some(Utc::now() - Duration::nanoseconds(1)),
        is_latest: true,
        num_versions: 1,
        ..ObjectOpts::default()
    });
    assert_eq!(event.action, Action::Transition);

    let delmarker = parse_lifecycle_config(
        "<BucketLifecycleConfiguration><Rule><Filter></Filter><Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration></Rule></BucketLifecycleConfiguration>",
    )
    .expect("del marker lifecycle");
    let event = delmarker.eval(ObjectOpts {
        name: "foodir/fooobject".to_owned(),
        mod_time: Some(Utc::now() - Duration::hours(1)),
        delete_marker: true,
        is_latest: true,
        num_versions: 1,
        ..ObjectOpts::default()
    });
    assert_eq!(event.action, Action::DeleteVersion);

    let noncurrent_transition = parse_lifecycle_config(
        "<BucketLifecycleConfiguration><Rule><Filter></Filter><Status>Enabled</Status><NoncurrentVersionTransition><NoncurrentDays>0</NoncurrentDays><StorageClass>S3TIER-1</StorageClass></NoncurrentVersionTransition></Rule></BucketLifecycleConfiguration>",
    )
    .expect("noncurrent transition lifecycle");
    let event = noncurrent_transition.eval(ObjectOpts {
        name: "foodir/fooobject".to_owned(),
        mod_time: Some(now),
        successor_mod_time: Some(now),
        version_id: "version".to_owned(),
        is_latest: false,
        num_versions: 2,
        ..ObjectOpts::default()
    });
    assert_eq!(event.action, Action::TransitionVersion);
}

#[test]
fn subtest_test_eval_line_741() {
    test_eval_line_288();
}

#[test]
fn test_has_active_rules_line_766() {
    let active = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Days>5</Days></Expiration></Rule></LifecycleConfiguration>",
    )
    .unwrap();
    assert!(active.has_active_rules("foodir/fooobject"));
    assert!(!active.has_active_rules("zdir/fooobject"));

    let future_date = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Filter><Prefix>foodir/</Prefix></Filter><Status>Enabled</Status><Expiration><Date>2999-01-01T00:00:00Z</Date></Expiration></Rule></LifecycleConfiguration>",
    )
    .unwrap();
    assert!(!future_date.has_active_rules("foodir/fooobject"));

    let transition = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Transition><StorageClass>S3TIER-1</StorageClass></Transition></Rule></LifecycleConfiguration>",
    )
    .unwrap();
    assert!(transition.has_active_rules("foodir/fooobject"));
}

#[test]
fn subtest_test_has_active_rules_fmt_sprintf_test_d_line_825() {
    test_has_active_rules_line_766();
}

#[test]
fn test_set_prediction_headers_line_841() {
    let lifecycle = Lifecycle {
        rules: vec![
            Rule {
                id: "rule-1".to_owned(),
                status: ENABLED.to_owned(),
                expiration: Expiration {
                    days: Some(3),
                    set: true,
                    ..Expiration::default()
                },
                ..Rule::default()
            },
            Rule {
                id: "rule-2".to_owned(),
                status: ENABLED.to_owned(),
                transition: Transition {
                    days: Some(3),
                    storage_class: "TIER-1".to_owned(),
                    set: true,
                    ..Transition::default()
                },
                ..Rule::default()
            },
            Rule {
                id: "rule-3".to_owned(),
                status: ENABLED.to_owned(),
                noncurrent_version_transition: NoncurrentVersionTransition {
                    noncurrent_days: 5,
                    storage_class: "TIER-2".to_owned(),
                    set: true,
                },
                ..Rule::default()
            },
        ],
        ..Lifecycle::default()
    };

    let current = lifecycle.prediction_headers(&ObjectOpts {
        name: "obj1".to_owned(),
        is_latest: true,
        mod_time: Some(Utc::now()),
        num_versions: 1,
        ..ObjectOpts::default()
    });
    assert!(current.get(AMZ_EXPIRATION).unwrap().contains("rule-1"));

    let noncurrent = lifecycle.prediction_headers(&ObjectOpts {
        name: "obj2".to_owned(),
        is_latest: false,
        mod_time: Some(Utc::now()),
        successor_mod_time: Some(Utc::now()),
        version_id: "v1".to_owned(),
        num_versions: 2,
        ..ObjectOpts::default()
    });
    assert!(noncurrent.get(MINIO_TRANSITION).unwrap().contains("rule-3"));
}

#[test]
fn test_transition_tier_line_923() {
    let lifecycle = Lifecycle {
        rules: vec![
            Rule {
                id: "rule-1".to_owned(),
                status: ENABLED.to_owned(),
                transition: Transition {
                    days: Some(3),
                    storage_class: "TIER-1".to_owned(),
                    set: true,
                    ..Transition::default()
                },
                ..Rule::default()
            },
            Rule {
                id: "rule-2".to_owned(),
                status: ENABLED.to_owned(),
                noncurrent_version_transition: NoncurrentVersionTransition {
                    noncurrent_days: 3,
                    storage_class: "TIER-2".to_owned(),
                    set: true,
                },
                ..Rule::default()
            },
        ],
        ..Lifecycle::default()
    };
    let now = Utc::now() + Duration::days(7);
    let events = Evaluator::new(lifecycle).eval(
        &[
            ObjectOpts {
                name: "obj1".to_owned(),
                is_latest: true,
                mod_time: Some(Utc::now()),
                num_versions: 1,
                ..ObjectOpts::default()
            },
            ObjectOpts {
                name: "obj2".to_owned(),
                is_latest: false,
                mod_time: Some(Utc::now()),
                successor_mod_time: Some(Utc::now()),
                version_id: "v1".to_owned(),
                num_versions: 2,
                ..ObjectOpts::default()
            },
        ],
        now,
    );
    assert_eq!(events[0].action, Action::Transition);
    assert_eq!(events[0].storage_class, "TIER-1");
    assert_eq!(events[1].action, Action::TransitionVersion);
    assert_eq!(events[1].storage_class, "TIER-2");
}

#[test]
fn test_transition_tier_with_prefix_and_tags_line_980() {
    let lifecycle = Lifecycle {
        rules: vec![
            Rule {
                id: "rule-1".to_owned(),
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    prefix: Prefix::new("abcd/"),
                    ..Filter::default()
                },
                transition: Transition {
                    days: Some(3),
                    storage_class: "TIER-1".to_owned(),
                    set: true,
                    ..Transition::default()
                },
                ..Rule::default()
            },
            Rule {
                id: "rule-2".to_owned(),
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    tag: Tag {
                        key: "priority".to_owned(),
                        value: "low".to_owned(),
                    },
                    ..Filter::default()
                },
                transition: Transition {
                    days: Some(3),
                    storage_class: "TIER-2".to_owned(),
                    set: true,
                    ..Transition::default()
                },
                ..Rule::default()
            },
        ],
        ..Lifecycle::default()
    };

    let events = Evaluator::new(lifecycle).eval(
        &[
            ObjectOpts {
                name: "obj1".to_owned(),
                is_latest: true,
                mod_time: Some(Utc::now()),
                num_versions: 1,
                ..ObjectOpts::default()
            },
            ObjectOpts {
                name: "abcd/obj2".to_owned(),
                is_latest: true,
                mod_time: Some(Utc::now()),
                num_versions: 1,
                ..ObjectOpts::default()
            },
            ObjectOpts {
                name: "obj3".to_owned(),
                user_tags: "priority=low".to_owned(),
                is_latest: true,
                mod_time: Some(Utc::now()),
                num_versions: 1,
                ..ObjectOpts::default()
            },
        ],
        Utc::now() + Duration::days(7),
    );

    assert_eq!(events[0].action, Action::None);
    assert_eq!(events[1].action, Action::Transition);
    assert_eq!(events[1].storage_class, "TIER-1");
    assert_eq!(events[2].action, Action::Transition);
    assert_eq!(events[2].storage_class, "TIER-2");
}

#[test]
fn test_noncurrent_versions_limit_line_1066() {
    let lifecycle = Lifecycle {
        rules: (1..=10)
            .map(|value| Rule {
                id: value.to_string(),
                status: ENABLED.to_owned(),
                noncurrent_version_expiration: NoncurrentVersionExpiration {
                    noncurrent_days: Some(value),
                    newer_noncurrent_versions: value,
                    set: true,
                },
                ..Rule::default()
            })
            .collect(),
        ..Lifecycle::default()
    };
    let event = lifecycle.noncurrent_versions_expiration_limit(&ObjectOpts {
        name: "obj".to_owned(),
        is_latest: true,
        num_versions: 1,
        ..ObjectOpts::default()
    });
    assert_eq!(event.rule_id, "1");
    assert_eq!(event.noncurrent_days, 1);
    assert_eq!(event.newer_noncurrent_versions, 1);
}

#[test]
fn test_max_noncurrent_backward_compat_line_1088() {
    let current = parse_noncurrent_version_expiration(
        "<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>3</NewerNoncurrentVersions></NoncurrentVersionExpiration>",
    )
    .unwrap();
    assert_eq!(current.noncurrent_days, Some(1));
    assert_eq!(current.newer_noncurrent_versions, 3);

    let compat = parse_noncurrent_version_expiration(
        "<NoncurrentVersionExpiration><NoncurrentDays>2</NoncurrentDays><MaxNoncurrentVersions>4</MaxNoncurrentVersions></NoncurrentVersionExpiration>",
    )
    .unwrap();
    assert_eq!(compat.noncurrent_days, Some(2));
    assert_eq!(compat.newer_noncurrent_versions, 4);
}

#[test]
fn test_parse_lifecycle_config_with_id_line_1128() {
    let lifecycle = parse_lifecycle_config_with_id(
        "<LifecycleConfiguration><Rule><ID>rule-1</ID><Filter><Prefix>prefix</Prefix></Filter><Status>Enabled</Status><Expiration><Days>3</Days></Expiration></Rule><Rule><Filter><Prefix>another-prefix</Prefix></Filter><Status>Enabled</Status><Expiration><Days>3</Days></Expiration></Rule></LifecycleConfiguration>",
    )
    .expect("parse with ids");
    assert!(lifecycle.rules.iter().all(|rule| !rule.id.is_empty()));
    assert_ne!(lifecycle.rules[0].id, lifecycle.rules[1].id);
}

#[test]
fn test_filter_and_set_prediction_headers_line_1157() {
    let lifecycle = Lifecycle {
        rules: vec![Rule {
            id: "rule-1".to_owned(),
            status: ENABLED.to_owned(),
            filter: Filter {
                set: true,
                prefix: Prefix::new("folder1/folder1/exp_dt=2022-"),
                ..Filter::default()
            },
            expiration: Expiration {
                days: Some(1),
                set: true,
                ..Expiration::default()
            },
            ..Rule::default()
        }],
        ..Lifecycle::default()
    };

    let matching = ObjectOpts {
        name: "folder1/folder1/exp_dt=2022-08-01/obj-1".to_owned(),
        mod_time: Some(Utc::now() - Duration::days(10)),
        is_latest: true,
        num_versions: 1,
        ..ObjectOpts::default()
    };
    let filtered = lifecycle.filter_rules(&matching);
    assert_eq!(filtered.len(), 1);
    let headers = lifecycle.prediction_headers(&matching);
    assert!(headers.contains_key(AMZ_EXPIRATION));

    let non_matching = ObjectOpts {
        name: "folder1/folder1/exp_dt=9999-01-01/obj-1".to_owned(),
        mod_time: Some(Utc::now() - Duration::days(10)),
        is_latest: true,
        num_versions: 1,
        ..ObjectOpts::default()
    };
    assert!(lifecycle.filter_rules(&non_matching).is_empty());
    assert!(!lifecycle
        .prediction_headers(&non_matching)
        .contains_key(AMZ_EXPIRATION));
}

#[test]
fn subtest_test_filter_and_set_prediction_headers_fmt_sprintf_test_d_line_1206() {
    test_filter_and_set_prediction_headers_line_1157();
}

#[test]
fn test_filter_rules_line_1223() {
    let rules = vec![
        Rule {
            id: "rule-1".to_owned(),
            status: ENABLED.to_owned(),
            filter: Filter {
                set: true,
                tag: Tag {
                    key: "key1".to_owned(),
                    value: "val1".to_owned(),
                },
                ..Filter::default()
            },
            expiration: Expiration {
                days: Some(1),
                set: true,
                ..Expiration::default()
            },
            ..Rule::default()
        },
        Rule {
            id: "rule-with-sz-lt".to_owned(),
            status: ENABLED.to_owned(),
            filter: Filter {
                set: true,
                object_size_less_than: 100 * MIB,
                ..Filter::default()
            },
            expiration: Expiration {
                days: Some(1),
                set: true,
                ..Expiration::default()
            },
            ..Rule::default()
        },
        Rule {
            id: "rule-with-sz-gt".to_owned(),
            status: ENABLED.to_owned(),
            filter: Filter {
                set: true,
                object_size_greater_than: MIB,
                ..Filter::default()
            },
            expiration: Expiration {
                days: Some(1),
                set: true,
                ..Expiration::default()
            },
            ..Rule::default()
        },
        Rule {
            id: "rule-with-sz-lt-and-tag".to_owned(),
            status: ENABLED.to_owned(),
            filter: Filter {
                set: true,
                and: And {
                    object_size_less_than: 100 * MIB,
                    tags: vec![Tag {
                        key: "key1".to_owned(),
                        value: "val1".to_owned(),
                    }],
                    ..And::default()
                },
                ..Filter::default()
            },
            expiration: Expiration {
                days: Some(1),
                set: true,
                ..Expiration::default()
            },
            ..Rule::default()
        },
    ];
    let lifecycle = Lifecycle {
        rules,
        ..Lifecycle::default()
    };
    assert!(lifecycle.validate(Retention::default()).is_ok());

    assert!(lifecycle
        .filter_rules(&ObjectOpts {
            delete_marker: true,
            is_latest: true,
            name: "obj-1".to_owned(),
            ..ObjectOpts::default()
        })
        .iter()
        .any(|rule| rule.id == "rule-with-sz-lt"));

    assert!(
        lifecycle
            .filter_rules(&ObjectOpts {
                is_latest: true,
                user_tags: "key1=val1".to_owned(),
                name: "obj-1".to_owned(),
                size: 2 * MIB,
                ..ObjectOpts::default()
            })
            .len()
            >= 2
    );

    assert!(
        lifecycle
            .filter_rules(&ObjectOpts {
                is_latest: true,
                name: "obj-1".to_owned(),
                size: MIB - 1,
                ..ObjectOpts::default()
            })
            .len()
            >= 1
    );
}

#[test]
fn subtest_test_filter_rules_fmt_sprintf_test_d_line_1420() {
    test_filter_rules_line_1223();
}

#[test]
fn test_delete_all_versions_line_1438() {
    let lifecycle = Lifecycle {
        rules: vec![
            Rule {
                id: "ExpiredObjectDeleteAllVersions-20".to_owned(),
                status: ENABLED.to_owned(),
                expiration: Expiration {
                    days: Some(20),
                    delete_all: Boolean {
                        val: true,
                        set: true,
                    },
                    set: true,
                    ..Expiration::default()
                },
                ..Rule::default()
            },
            Rule {
                id: "Transition-10".to_owned(),
                status: ENABLED.to_owned(),
                transition: Transition {
                    days: Some(10),
                    storage_class: "WARM-1".to_owned(),
                    set: true,
                    ..Transition::default()
                },
                ..Rule::default()
            },
        ],
        ..Lifecycle::default()
    };
    let obj = ObjectOpts {
        name: "foo.txt".to_owned(),
        mod_time: Some(Utc::now() - Duration::days(10)),
        version_id: "v1".to_owned(),
        is_latest: true,
        num_versions: 4,
        ..ObjectOpts::default()
    };
    let event = lifecycle.eval_upcoming(&obj);
    assert_eq!(event.action, Action::Transition);

    let lifecycle = Lifecycle {
        rules: vec![
            Rule {
                id: "delmarker-exp-20".to_owned(),
                status: ENABLED.to_owned(),
                del_marker_expiration: DelMarkerExpiration { days: 20 },
                ..Rule::default()
            },
            Rule {
                id: "delmarker-exp-10".to_owned(),
                status: ENABLED.to_owned(),
                del_marker_expiration: DelMarkerExpiration { days: 10 },
                ..Rule::default()
            },
        ],
        ..Lifecycle::default()
    };
    let event = lifecycle.eval_upcoming(&ObjectOpts {
        name: "foo.txt".to_owned(),
        mod_time: Some(Utc::now() - Duration::days(10)),
        delete_marker: true,
        is_latest: true,
        num_versions: 2,
        ..ObjectOpts::default()
    });
    assert_eq!(event.action, Action::DelMarkerDeleteAllVersions);
    assert_eq!(event.rule_id, "delmarker-exp-10");
}
