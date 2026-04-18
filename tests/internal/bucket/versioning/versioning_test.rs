use std::io::Cursor;

use minio_rust::internal::bucket::versioning::{parse_config, ExcludedPrefix, Versioning, ENABLED};

#[test]
fn parse_config_matches_reference_cases() {
    let cases = [
        (
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
            </VersioningConfiguration>"#,
            None,
            Vec::<&str>::new(),
            false,
        ),
        (
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_temporary/</Prefix></ExcludedPrefixes>
            </VersioningConfiguration>"#,
            None,
            vec![
                "path/to/my/workload/_staging/",
                "path/to/my/workload/_temporary/",
            ],
            false,
        ),
        (
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Suspended</Status>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging</Prefix></ExcludedPrefixes>
            </VersioningConfiguration>"#,
            Some("excluded prefixes extension supported only when versioning is enabled"),
            Vec::<&str>::new(),
            false,
        ),
        (
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/ab/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/cd/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/ef/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/gh/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/ij/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/kl/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/mn/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/op/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/qr/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/st/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/uv/</Prefix></ExcludedPrefixes>
            </VersioningConfiguration>"#,
            Some("too many excluded prefixes"),
            Vec::<&str>::new(),
            false,
        ),
        (
            r#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Status>Enabled</Status>
                <ExcludeFolders>true</ExcludeFolders>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_staging/</Prefix></ExcludedPrefixes>
                <ExcludedPrefixes><Prefix>path/to/my/workload/_temporary/</Prefix></ExcludedPrefixes>
            </VersioningConfiguration>"#,
            None,
            vec![
                "path/to/my/workload/_staging/",
                "path/to/my/workload/_temporary/",
            ],
            true,
        ),
    ];

    for (input, expected_error, excluded_prefixes, exclude_folders) in cases {
        let result = parse_config(Cursor::new(input));
        match (result, expected_error) {
            (Ok(versioning), None) => {
                assert_eq!(
                    versioning
                        .excluded_prefixes
                        .iter()
                        .map(|prefix| prefix.prefix.as_str())
                        .collect::<Vec<_>>(),
                    excluded_prefixes
                );
                assert_eq!(versioning.exclude_folders, exclude_folders);
                assert!(versioning.validate().is_ok());
            }
            (Err(error), Some(expected)) => assert_eq!(error.to_string(), expected),
            (other, expected) => panic!("unexpected result: {other:?} expected={expected:?}"),
        }
    }
}

#[test]
fn marshal_xml_omits_excluded_prefixes_when_empty() {
    let versioning = Versioning {
        xmlns: String::new(),
        status: ENABLED.to_owned(),
        excluded_prefixes: Vec::new(),
        exclude_folders: false,
    };

    let xml = versioning
        .to_xml()
        .expect("xml serialization should succeed");
    assert!(!xml.contains("ExcludedPrefixes"));
}

#[test]
fn versioning_zero_matches_reference_cases() {
    let versioning = Versioning {
        xmlns: String::new(),
        status: String::new(),
        excluded_prefixes: Vec::new(),
        exclude_folders: false,
    };

    assert!(!versioning.enabled());
    assert!(!versioning.suspended());
}

#[test]
fn exclude_folders_matches_reference_cases() {
    let mut versioning = Versioning {
        xmlns: String::new(),
        status: ENABLED.to_owned(),
        excluded_prefixes: Vec::new(),
        exclude_folders: true,
    };

    for prefix in ["jobs/output/_temporary/", "jobs/output/", "jobs/"] {
        assert!(!versioning.prefix_enabled(prefix));
        assert!(versioning.prefix_suspended(prefix));
    }

    let prefix = "prefix-1/obj-1";
    assert!(versioning.prefix_enabled(prefix));
    assert!(!versioning.prefix_suspended(prefix));

    versioning.exclude_folders = false;
    for prefix in ["jobs/output/_temporary/", "jobs/output/", "jobs/"] {
        assert!(versioning.prefix_enabled(prefix));
        assert!(!versioning.prefix_suspended(prefix));
    }
}

#[test]
fn excluded_prefixes_match_reference_cases() {
    let versioning = Versioning {
        xmlns: String::new(),
        status: ENABLED.to_owned(),
        excluded_prefixes: vec![ExcludedPrefix {
            prefix: "*/_temporary/".to_owned(),
        }],
        exclude_folders: false,
    };

    versioning.validate().expect("test config should validate");

    assert!(versioning.prefix_suspended("app1-jobs/output/_temporary/attempt1/data.csv"));
    assert!(!versioning.prefix_suspended("app1-jobs/output/final/attempt1/data.csv"));
}
