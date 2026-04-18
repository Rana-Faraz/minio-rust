use minio_rust::internal::bucket::replication::{
    parse_config, Config, DeleteMarkerReplication, DeleteReplication, Filter, ObjectOpts,
    ReplicationType, Rule, Tag, DISABLED, ENABLED,
};

#[test]
fn test_parse_and_validate_replication_config_line_26() {
    let cases = [
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>string</Status></DeleteMarkerReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            Some("Delete marker replication status is invalid".to_owned()),
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            None,
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            None,
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            Some("Missing required parameter `Destination` in Replication rule".to_owned()),
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role></Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Priority>3</Priority><Destination><Bucket>arn:minio:replication:::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><Priority>4</Priority><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:minio:replication:::destinationbucket2</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            None,
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            Some("Status should not be empty".to_owned()),
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enssabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><Priority>1</Priority><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            Some("Status must be set to either Enabled or Disabled".to_owned()),
            "destinationbucket",
            false,
        ),
        (
            &format!("<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><ID>{}</ID><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>", "a".repeat(256)),
            None,
            Some("ID must be less than 255 characters".to_owned()),
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            Some("Replication configuration has duplicate priority".to_owned()),
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role></ReplicationConfiguration>",
            None,
            Some("Replication configuration should have at least one rule".to_owned()),
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination></Destination></Rule></ReplicationConfiguration>",
            Some("invalid destination ''".to_owned()),
            None,
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>destinationbucket2</Bucket></Destination></Rule></ReplicationConfiguration>",
            Some("invalid destination 'destinationbucket2'".to_owned()),
            None,
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:minio:replication::8320b6d18f9032b4700f1f03b50d8d1853de8f22cab86931ee794e12f190852c:destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            None,
            None,
            "destinationbucket",
            false,
        ),
        (
            "<ReplicationConfiguration><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:xx:replication::8320b6d18f9032b4700f1f03b50d8d1853de8f22cab86931ee794e12f190852c:destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            Some("invalid destination 'arn:xx:replication::8320b6d18f9032b4700f1f03b50d8d1853de8f22cab86931ee794e12f190852c:destinationbucket'".to_owned()),
            None,
            "destinationbucket",
            false,
        ),
    ];

    for (xml, parse_error, validate_error, bucket, same_target) in cases {
        match parse_config(xml) {
            Ok(config) => {
                if let Some(expected) = parse_error {
                    panic!("expected parse error {expected} but parsing succeeded");
                }
                let result = config
                    .validate(bucket, same_target)
                    .err()
                    .map(|err| err.to_string());
                assert_eq!(result, validate_error);
            }
            Err(error) => {
                assert_eq!(Some(error.to_string()), parse_error);
            }
        }
    }
}

#[test]
fn subtest_test_parse_and_validate_replication_config_fmt_sprintf_test_d_line_150() {
    test_parse_and_validate_replication_config_line_26();
}

#[test]
fn test_replicate_line_171() {
    let cfgs = [
        Config {
            rules: vec![Rule {
                status: ENABLED.to_owned(),
                priority: 3,
                delete_marker_replication: DeleteMarkerReplication {
                    status: ENABLED.to_owned(),
                },
                delete_replication: DeleteReplication {
                    status: ENABLED.to_owned(),
                },
                ..Rule::default()
            }],
            ..Config::default()
        },
        Config {
            rules: vec![Rule {
                status: ENABLED.to_owned(),
                priority: 3,
                delete_marker_replication: DeleteMarkerReplication {
                    status: DISABLED.to_owned(),
                },
                delete_replication: DeleteReplication {
                    status: DISABLED.to_owned(),
                },
                ..Rule::default()
            }],
            ..Config::default()
        },
        Config {
            rules: vec![
                Rule {
                    status: ENABLED.to_owned(),
                    priority: 2,
                    delete_marker_replication: DeleteMarkerReplication {
                        status: DISABLED.to_owned(),
                    },
                    delete_replication: DeleteReplication {
                        status: ENABLED.to_owned(),
                    },
                    filter: Filter {
                        prefix: "xy".to_owned(),
                        tag: Tag {
                            key: "k1".to_owned(),
                            value: "v1".to_owned(),
                        },
                        ..Filter::default()
                    },
                    ..Rule::default()
                },
                Rule {
                    status: ENABLED.to_owned(),
                    priority: 1,
                    delete_marker_replication: DeleteMarkerReplication {
                        status: ENABLED.to_owned(),
                    },
                    delete_replication: DeleteReplication {
                        status: DISABLED.to_owned(),
                    },
                    filter: Filter {
                        prefix: "xyz".to_owned(),
                        ..Filter::default()
                    },
                    ..Rule::default()
                },
            ],
            ..Config::default()
        },
        Config {
            rules: vec![
                Rule {
                    status: ENABLED.to_owned(),
                    priority: 2,
                    delete_marker_replication: DeleteMarkerReplication {
                        status: DISABLED.to_owned(),
                    },
                    delete_replication: DeleteReplication {
                        status: ENABLED.to_owned(),
                    },
                    filter: Filter {
                        prefix: "xy".to_owned(),
                        tag: Tag {
                            key: "k1".to_owned(),
                            value: "v1".to_owned(),
                        },
                        ..Filter::default()
                    },
                    ..Rule::default()
                },
                Rule {
                    status: ENABLED.to_owned(),
                    priority: 1,
                    delete_marker_replication: DeleteMarkerReplication {
                        status: ENABLED.to_owned(),
                    },
                    delete_replication: DeleteReplication {
                        status: DISABLED.to_owned(),
                    },
                    filter: Filter {
                        prefix: "abc".to_owned(),
                        ..Filter::default()
                    },
                    ..Rule::default()
                },
            ],
            ..Config::default()
        },
        Config {
            rules: vec![Rule {
                status: ENABLED.to_owned(),
                priority: 2,
                delete_marker_replication: DeleteMarkerReplication {
                    status: ENABLED.to_owned(),
                },
                delete_replication: DeleteReplication {
                    status: ENABLED.to_owned(),
                },
                source_selection_criteria:
                    minio_rust::internal::bucket::replication::SourceSelectionCriteria {
                        replica_modifications:
                            minio_rust::internal::bucket::replication::ReplicaModifications {
                                status: DISABLED.to_owned(),
                            },
                    },
                ..Rule::default()
            }],
            ..Config::default()
        },
    ];

    let cases = [
        (ObjectOpts::default(), &cfgs[0], false),
        (
            ObjectOpts {
                name: "c1test".to_owned(),
                ..ObjectOpts::default()
            },
            &cfgs[0],
            true,
        ),
        (
            ObjectOpts {
                name: "c1test".to_owned(),
                version_id: "vid".to_owned(),
                ..ObjectOpts::default()
            },
            &cfgs[0],
            true,
        ),
        (
            ObjectOpts {
                name: "c1test".to_owned(),
                delete_marker: true,
                op_type: ReplicationType::Delete,
                ..ObjectOpts::default()
            },
            &cfgs[0],
            true,
        ),
        (
            ObjectOpts {
                name: "c1test".to_owned(),
                version_id: "vid".to_owned(),
                op_type: ReplicationType::Delete,
                ..ObjectOpts::default()
            },
            &cfgs[0],
            true,
        ),
        (
            ObjectOpts {
                name: "c2test".to_owned(),
                ..ObjectOpts::default()
            },
            &cfgs[1],
            true,
        ),
        (
            ObjectOpts {
                name: "c2test".to_owned(),
                delete_marker: true,
                op_type: ReplicationType::Delete,
                ..ObjectOpts::default()
            },
            &cfgs[1],
            false,
        ),
        (
            ObjectOpts {
                name: "xy/c3test".to_owned(),
                user_tags: "k1=v1".to_owned(),
                ..ObjectOpts::default()
            },
            &cfgs[2],
            true,
        ),
        (
            ObjectOpts {
                name: "xyz/c3test".to_owned(),
                user_tags: "k1=v1".to_owned(),
                delete_marker: true,
                op_type: ReplicationType::Delete,
                ..ObjectOpts::default()
            },
            &cfgs[2],
            false,
        ),
        (
            ObjectOpts {
                name: "xyz/c3test".to_owned(),
                delete_marker: true,
                op_type: ReplicationType::Delete,
                ..ObjectOpts::default()
            },
            &cfgs[2],
            true,
        ),
        (
            ObjectOpts {
                name: "abc/c4test".to_owned(),
                ..ObjectOpts::default()
            },
            &cfgs[3],
            true,
        ),
        (
            ObjectOpts {
                name: "xy/c5test".to_owned(),
                user_tags: "k1=v1".to_owned(),
                replica: true,
                ..ObjectOpts::default()
            },
            &cfgs[4],
            false,
        ),
        (
            ObjectOpts {
                name: "xa/c5test".to_owned(),
                user_tags: "k1=v1".to_owned(),
                replica: false,
                ..ObjectOpts::default()
            },
            &cfgs[4],
            true,
        ),
    ];

    for (opts, config, expected) in cases {
        assert_eq!(
            config.replicate(&opts),
            expected,
            "replicate case failed for {:?}",
            opts
        );
    }
}

#[test]
fn subtest_test_replicate_test_case_opts_name_line_299() {
    test_replicate_line_171();
}

#[test]
fn test_has_active_rules_line_308() {
    let cases = [
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Disabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>key-prefix</Prefix><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            "miss/prefix",
            false,
            false,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Filter><Prefix>key/prefix</Prefix></Filter><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            "key/prefix1",
            true,
            true,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            "key-prefix",
            true,
            true,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Filter><Prefix>testdir/dir1/</Prefix></Filter><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            "testdir/",
            false,
            true,
        ),
        (
            "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Filter><And><Prefix>key-prefix</Prefix><Tag><Key>key1</Key><Value>value1</Value></Tag><Tag><Key>key2</Key><Value>value2</Value></Tag></And></Filter><Destination><Bucket>arn:aws:s3:::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
            "testdir/",
            true,
            true,
        ),
    ];

    for (xml, prefix, non_recursive, recursive) in cases {
        let config = parse_config(xml).expect("config should parse");
        assert_eq!(config.has_active_rules(prefix, false), non_recursive);
        assert_eq!(config.has_active_rules(prefix, true), recursive);
    }
}

#[test]
fn subtest_test_has_active_rules_fmt_sprintf_test_d_line_354() {
    test_has_active_rules_line_308();
}

#[test]
fn test_filter_actionable_rules_line_369() {
    let config = parse_config(
        "<ReplicationConfiguration><Role>arn:aws:iam::AcctID:role/role-name</Role><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>2</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket2</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>4</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket2</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>3</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket</Bucket></Destination></Rule><Rule><Status>Enabled</Status><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><DeleteReplication><Status>Disabled</Status></DeleteReplication><Prefix>prefix</Prefix><Priority>1</Priority><Destination><Bucket>arn:minio:replication:xxx::destinationbucket</Bucket></Destination></Rule></ReplicationConfiguration>",
    )
    .expect("config should parse");

    let rules = config.filter_actionable_rules(&ObjectOpts {
        name: "prefix".to_owned(),
        ..ObjectOpts::default()
    });
    let priorities: Vec<_> = rules
        .iter()
        .map(|rule| (rule.destination.arn.clone(), rule.priority))
        .collect();
    assert_eq!(
        priorities,
        vec![
            (
                "arn:minio:replication:xxx::destinationbucket2".to_owned(),
                4
            ),
            (
                "arn:minio:replication:xxx::destinationbucket2".to_owned(),
                2
            ),
            ("arn:minio:replication:xxx::destinationbucket".to_owned(), 3),
            ("arn:minio:replication:xxx::destinationbucket".to_owned(), 1),
        ]
    );
}
