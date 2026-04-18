use minio_rust::internal::bucket::lifecycle::{
    parse_lifecycle_config, parse_lifecycle_config_with_id, And, Error, Filter, Prefix, Tag,
};

const MIB: i64 = 1024 * 1024;
const GIB: i64 = 1024 * 1024 * 1024;

fn parse_filter(fragment: &str) -> Result<Filter, Error> {
    parse_lifecycle_config_with_id(&format!(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status>{fragment}<Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"
    ))
    .map(|lc| lc.rules[0].filter.clone())
}

#[test]
fn test_unsupported_filters_line_30() {
    for (xml, expected) in [
        (
            "<Filter><And><Prefix>key-prefix</Prefix></And></Filter>",
            Error::XmlNotWellFormed,
        ),
        (
            "<Filter><Tag><Key>key1</Key><Value>value1</Value></Tag></Filter>",
            Error::Parse(String::new()),
        ),
        (
            "<Filter><Prefix>key-prefix</Prefix></Filter>",
            Error::Parse(String::new()),
        ),
        (
            "<Filter><Prefix>key-prefix</Prefix><Tag><Key>key1</Key><Value>value1</Value></Tag><Tag><Key>key2</Key><Value>value2</Value></Tag></Filter>",
            Error::InvalidFilter,
        ),
        (
            "<Filter><And><Prefix>key-prefix</Prefix><Tag><Key>key1</Key><Value>value1</Value></Tag><Tag><Key>key2</Key><Value>value2</Value></Tag></And></Filter>",
            Error::Parse(String::new()),
        ),
        (
            "<Filter><And><Prefix></Prefix><Tag><Key>key1</Key><Value>value1</Value></Tag><Tag><Key>key2</Key><Value>value2</Value></Tag></And></Filter>",
            Error::Parse(String::new()),
        ),
        (
            "<Filter><Prefix>key-prefix</Prefix><Tag><Key>key1</Key><Value>value1</Value></Tag></Filter>",
            Error::InvalidFilter,
        ),
    ] {
        let filter = parse_filter(xml).expect("filter should parse");
        match expected {
            Error::Parse(_) => assert!(filter.validate().is_ok(), "expected valid filter for {xml}"),
            other => assert_eq!(filter.validate().unwrap_err(), other),
        }
    }
}

#[test]
fn subtest_test_unsupported_filters_fmt_sprintf_test_d_line_116() {
    test_unsupported_filters_line_30();
}

#[test]
fn test_object_size_filters_line_130() {
    let f1 = parse_filter(&format!(
        "<Filter><Prefix>doc/</Prefix><ObjectSizeGreaterThan>{}</ObjectSizeGreaterThan><ObjectSizeLessThan>{}</ObjectSizeLessThan></Filter>",
        100 * MIB,
        100 * GIB
    ))
    .expect("size filter should parse");
    assert_eq!(f1.object_size_greater_than, 100 * MIB);
    assert_eq!(f1.object_size_less_than, 100 * GIB);
    assert_eq!(f1.prefix, Prefix::new("doc/"));

    let f2 = parse_filter(&format!(
        "<Filter><And><ObjectSizeGreaterThan>{}</ObjectSizeGreaterThan><ObjectSizeLessThan>{}</ObjectSizeLessThan><Prefix></Prefix></And></Filter>",
        100 * MIB,
        GIB
    ))
    .expect("and size filter should parse");
    assert_eq!(f2.and.object_size_greater_than, 100 * MIB);
    assert_eq!(f2.and.object_size_less_than, GIB);

    let lt = Filter {
        set: true,
        object_size_less_than: 100 * MIB,
        ..Filter::default()
    };
    let gt = Filter {
        set: true,
        object_size_greater_than: MIB,
        ..Filter::default()
    };
    let lt_gt = Filter {
        set: true,
        and: And {
            object_size_greater_than: MIB,
            object_size_less_than: 100 * MIB,
            ..And::default()
        },
        ..Filter::default()
    };

    assert!(!lt.by_size(101 * MIB));
    assert!(lt.by_size(99 * MIB));
    assert!(!gt.by_size(MIB - 1));
    assert!(gt.by_size(MIB + 1));
    assert!(!lt_gt.by_size(MIB - 1));
    assert!(lt_gt.by_size(2 * MIB));
    assert!(!lt_gt.by_size(101 * MIB));
}

#[test]
fn subtest_test_object_size_filters_fmt_sprintf_test_d_line_237() {
    test_object_size_filters_line_130();
}

#[test]
fn test_test_tags_line_245() {
    let filter = Filter {
        set: true,
        and: And {
            tags: vec![
                Tag {
                    key: "tag1".to_owned(),
                    value: "value1".to_owned(),
                },
                Tag {
                    key: "tag 2".to_owned(),
                    value: "value 2".to_owned(),
                },
            ],
            ..And::default()
        },
        ..Filter::default()
    };

    assert!(filter.test_tags("tag1=value1&tag+2=value+2"));
    assert!(filter.test_tags("tag+2=value+2&tag1=value1"));
    assert!(!filter.test_tags("tag1=value1"));
    assert!(!filter.test_tags("tag1=value2&tag+2=value+2"));
    assert!(!filter.test_tags("not-valid%"));

    let tag_only = parse_lifecycle_config(
        "<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter><Tag><Key>key1</Key><Value>val1</Value></Tag></Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
    )
    .expect("tag rule should parse");
    assert!(tag_only.rules[0].filter.test_tags("key1=val1&other=ok"));
}

#[test]
fn subtest_test_test_tags_fmt_sprintf_test_d_line_337() {
    test_test_tags_line_245();
}
