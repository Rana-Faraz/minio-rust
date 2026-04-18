use std::io::Cursor;

use minio_rust::internal::event::{
    parse_config, validate_filter_rule_value, validate_filter_rule_value_bytes, Arn, Config,
    FilterRule, FilterRuleList, Name, Queue, RulesMap, Target, TargetId, TargetList,
};

pub const SOURCE_FILE: &str = "internal/event/config_test.go";

#[derive(Clone)]
struct ExampleTarget {
    id: TargetId,
}

impl ExampleTarget {
    fn new(id: TargetId) -> Self {
        Self { id }
    }
}

impl Target for ExampleTarget {
    fn id(&self) -> TargetId {
        self.id.clone()
    }
}

fn queue_case_1() -> &'static str {
    r#"
<QueueConfiguration>
   <Id>1</Id>
   <Filter></Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectAccessed:*</Event>
   <Event>s3:ObjectCreated:*</Event>
   <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>"#
}

fn queue_case_2() -> &'static str {
    r#"
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>"#
}

fn queue_case_2_without_region() -> &'static str {
    r#"
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs::1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>"#
}

fn queue_case_3() -> &'static str {
    r#"
<QueueConfiguration>
   <Id>1</Id>
   <Filter></Filter>
   <Queue>arn:minio:sqs:eu-west-2:1:webhook</Queue>
   <Event>s3:ObjectAccessed:*</Event>
   <Event>s3:ObjectCreated:*</Event>
   <Event>s3:ObjectRemoved:*</Event>
</QueueConfiguration>"#
}

fn config_case_1() -> &'static str {
    r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
</NotificationConfiguration>
"#
}

fn config_case_2() -> &'static str {
    r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
"#
}

fn config_case_2_without_region() -> &'static str {
    r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs::1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
"#
}

fn config_case_3() -> &'static str {
    r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <QueueConfiguration>
      <Id>2</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
"#
}

fn config_case_6() -> &'static str {
    r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <QueueConfiguration>
      <Id>2</Id>
       <Filter>
           <S3Key>
               <FilterRule>
                   <Name>prefix</Name>
                   <Value>images/</Value>
               </FilterRule>
               <FilterRule>
                   <Name>suffix</Name>
                   <Value>jpg</Value>
               </FilterRule>
           </S3Key>
      </Filter>
      <Queue>arn:minio:sqs:us-east-1:2:amqp</Queue>
      <Event>s3:ObjectCreated:Put</Event>
   </QueueConfiguration>
</NotificationConfiguration>
"#
}

fn config_case_4() -> &'static str {
    r#"
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <QueueConfiguration>
      <Id>1</Id>
      <Filter></Filter>
      <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
      <Event>s3:ObjectAccessed:*</Event>
      <Event>s3:ObjectCreated:*</Event>
      <Event>s3:ObjectRemoved:*</Event>
   </QueueConfiguration>
   <CloudFunctionConfiguration>
      <Id>1</Id>
      <Filter>
             <S3Key>
                 <FilterRule>
                     <Name>suffix</Name>
                     <Value>.jpg</Value>
                 </FilterRule>
             </S3Key>
      </Filter>
      <Cloudcode>arn:aws:lambda:us-west-2:444455556666:cloud-function-A</Cloudcode>
      <Event>s3:ObjectCreated:Put</Event>
   </CloudFunctionConfiguration>
   <TopicConfiguration>
      <Topic>arn:aws:sns:us-west-2:444455556666:sns-notification-one</Topic>
      <Event>s3:ObjectCreated:*</Event>
  </TopicConfiguration>
</NotificationConfiguration>
"#
}

fn config_case_5() -> &'static str {
    r#"<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></NotificationConfiguration>"#
}

fn parse_queue(xml: &str) -> Queue {
    Queue::unmarshal_xml(xml.as_bytes()).expect("queue xml should parse")
}

fn parse_config_xml(xml: &str) -> Config {
    Config::unmarshal_xml(xml.as_bytes()).expect("config xml should parse")
}

#[test]
fn validate_filter_rule_value_matches_reference_cases() {
    let oversized = "foo/bar/baz".repeat(100);
    let cases = [
        (Some("foo/."), None, true),
        (Some("../foo"), None, true),
        (Some(oversized.as_str()), None, true),
        (None, Some(vec![0xff, 0xfe, 0xfd]), true),
        (Some(r"foo\bar"), None, true),
        (Some("Hello/世界"), None, false),
    ];

    for (text, bytes, should_err) in cases {
        let result = match (text, bytes) {
            (Some(value), None) => validate_filter_rule_value(value),
            (None, Some(raw)) => validate_filter_rule_value_bytes(&raw),
            _ => unreachable!(),
        };
        assert_eq!(result.is_err(), should_err);
    }
}

#[test]
fn filter_rule_unmarshal_xml_matches_reference_cases() {
    let cases = [
        (r#"<FilterRule></FilterRule>"#, None, true),
        (r#"<FilterRule><Name></Name></FilterRule>"#, None, true),
        (r#"<FilterRule><Value></Value></FilterRule>"#, None, true),
        (
            r#"<FilterRule><Name></Name><Value></Value></FilterRule>"#,
            None,
            true,
        ),
        (
            r#"<FilterRule><Name>Prefix</Name><Value>Hello/世界</Value></FilterRule>"#,
            None,
            true,
        ),
        (
            r#"<FilterRule><Name>ends</Name><Value>foo/bar</Value></FilterRule>"#,
            None,
            true,
        ),
        (
            r#"<FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule>"#,
            Some(FilterRule {
                name: "prefix".to_owned(),
                value: "Hello/世界".to_owned(),
            }),
            false,
        ),
        (
            r#"<FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule>"#,
            Some(FilterRule {
                name: "suffix".to_owned(),
                value: "foo/bar".to_owned(),
            }),
            false,
        ),
    ];

    for (xml, expected, should_err) in cases {
        let result = FilterRule::unmarshal_xml(xml.as_bytes());
        assert_eq!(result.is_err(), should_err);
        if let Some(expected) = expected {
            assert_eq!(result.expect("filter rule should parse"), expected);
        }
    }
}

#[test]
fn filter_rule_list_unmarshal_xml_matches_reference_cases() {
    let cases = [
        (
            r#"<S3Key><FilterRule><Name>suffix</Name><Value>Hello/世界</Value></FilterRule><FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule></S3Key>"#,
            None,
            true,
        ),
        (
            r#"<S3Key><FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule><FilterRule><Name>prefix</Name><Value>foo/bar</Value></FilterRule></S3Key>"#,
            None,
            true,
        ),
        (
            r#"<S3Key><FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule></S3Key>"#,
            Some(FilterRuleList {
                rules: vec![FilterRule {
                    name: "prefix".to_owned(),
                    value: "Hello/世界".to_owned(),
                }],
            }),
            false,
        ),
        (
            r#"<S3Key><FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule></S3Key>"#,
            Some(FilterRuleList {
                rules: vec![FilterRule {
                    name: "suffix".to_owned(),
                    value: "foo/bar".to_owned(),
                }],
            }),
            false,
        ),
        (
            r#"<S3Key><FilterRule><Name>prefix</Name><Value>Hello/世界</Value></FilterRule><FilterRule><Name>suffix</Name><Value>foo/bar</Value></FilterRule></S3Key>"#,
            Some(FilterRuleList {
                rules: vec![
                    FilterRule {
                        name: "prefix".to_owned(),
                        value: "Hello/世界".to_owned(),
                    },
                    FilterRule {
                        name: "suffix".to_owned(),
                        value: "foo/bar".to_owned(),
                    },
                ],
            }),
            false,
        ),
    ];

    for (xml, expected, should_err) in cases {
        let result = FilterRuleList::unmarshal_xml(xml.as_bytes());
        assert_eq!(result.is_err(), should_err);
        if let Some(expected) = expected {
            assert_eq!(result.expect("rule list should parse"), expected);
        }
    }
}

#[test]
fn filter_rule_list_pattern_matches_reference_cases() {
    let cases = [
        (FilterRuleList::default(), ""),
        (
            FilterRuleList {
                rules: vec![FilterRule {
                    name: "prefix".to_owned(),
                    value: "Hello/世界".to_owned(),
                }],
            },
            "Hello/世界*",
        ),
        (
            FilterRuleList {
                rules: vec![FilterRule {
                    name: "suffix".to_owned(),
                    value: "foo/bar".to_owned(),
                }],
            },
            "*foo/bar",
        ),
        (
            FilterRuleList {
                rules: vec![
                    FilterRule {
                        name: "prefix".to_owned(),
                        value: "Hello/世界".to_owned(),
                    },
                    FilterRule {
                        name: "suffix".to_owned(),
                        value: "foo/bar".to_owned(),
                    },
                ],
            },
            "Hello/世界*foo/bar",
        ),
    ];

    for (rule_list, expected) in cases {
        assert_eq!(rule_list.pattern(), expected);
    }
}

#[test]
fn queue_unmarshal_xml_matches_reference_cases() {
    let duplicate_event_xml = r#"
<QueueConfiguration>
   <Id>1</Id>
    <Filter>
        <S3Key>
            <FilterRule>
                <Name>prefix</Name>
                <Value>images/</Value>
            </FilterRule>
            <FilterRule>
                <Name>suffix</Name>
                <Value>jpg</Value>
            </FilterRule>
        </S3Key>
   </Filter>
   <Queue>arn:minio:sqs:us-east-1:1:webhook</Queue>
   <Event>s3:ObjectCreated:Put</Event>
   <Event>s3:ObjectCreated:Put</Event>
</QueueConfiguration>"#;

    for (xml, should_err) in [
        (queue_case_1(), false),
        (queue_case_2(), false),
        (duplicate_event_xml, true),
    ] {
        assert_eq!(Queue::unmarshal_xml(xml.as_bytes()).is_err(), should_err);
    }
}

#[test]
fn queue_validate_matches_reference_cases() {
    let queue1 = parse_queue(queue_case_1());
    let queue2 = parse_queue(queue_case_2());
    let queue3 = parse_queue(queue_case_3());

    let target_list1 = TargetList::new();

    let mut target_list2 = TargetList::new();
    target_list2
        .add(ExampleTarget::new(TargetId::new("1", "webhook")))
        .expect("seed target should be added");

    let cases = [
        (queue1, "eu-west-1", None, true),
        (queue2.clone(), "us-east-1", Some(&target_list1), true),
        (queue3, "", Some(&target_list2), false),
        (queue2, "us-east-1", Some(&target_list2), false),
    ];

    for (queue, region, target_list, should_err) in cases {
        assert_eq!(queue.validate(region, target_list).is_err(), should_err);
    }
}

#[test]
fn queue_set_region_matches_reference_cases() {
    let cases = [
        (
            queue_case_1(),
            "eu-west-1",
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "eu-west-1".to_owned(),
            },
        ),
        (
            queue_case_1(),
            "",
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            },
        ),
        (
            queue_case_2_without_region(),
            "us-east-1",
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "us-east-1".to_owned(),
            },
        ),
        (
            queue_case_2_without_region(),
            "",
            Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            },
        ),
    ];

    for (xml, region, expected) in cases {
        let mut queue = parse_queue(xml);
        queue.set_region(region);
        assert_eq!(queue.arn, expected);
    }
}

#[test]
fn queue_to_rules_map_matches_reference_cases() {
    let queue_case1 = parse_queue(queue_case_1());
    let queue_case2 = parse_queue(queue_case_2());

    let expected1 = RulesMap::new(
        &[
            Name::ObjectAccessedAll,
            Name::ObjectCreatedAll,
            Name::ObjectRemovedAll,
        ],
        "*",
        TargetId::new("1", "webhook"),
    );
    let expected2 = RulesMap::new(
        &[Name::ObjectCreatedPut],
        "images/*jpg",
        TargetId::new("1", "webhook"),
    );

    assert_eq!(queue_case1.to_rules_map(), expected1);
    assert_eq!(queue_case2.to_rules_map(), expected2);
}

#[test]
fn config_unmarshal_xml_matches_reference_cases() {
    for (xml, should_err) in [
        (config_case_1(), false),
        (config_case_2(), false),
        (config_case_3(), false),
        (config_case_4(), true),
        (config_case_5(), false),
    ] {
        assert_eq!(Config::unmarshal_xml(xml.as_bytes()).is_err(), should_err);
    }
}

#[test]
fn config_validate_matches_reference_cases() {
    let config1 = parse_config_xml(config_case_1());
    let config2 = parse_config_xml(config_case_2());
    let config3 = parse_config_xml(config_case_3());

    let target_list1 = TargetList::new();

    let mut target_list2 = TargetList::new();
    target_list2
        .add(ExampleTarget::new(TargetId::new("1", "webhook")))
        .expect("seed target should be added");

    let cases = [
        (config1, "eu-west-1", None, true),
        (config2.clone(), "us-east-1", Some(&target_list1), true),
        (config3, "", Some(&target_list2), false),
        (config2, "us-east-1", Some(&target_list2), false),
    ];

    for (config, region, target_list, should_err) in cases {
        assert_eq!(config.validate(region, target_list).is_err(), should_err);
    }
}

#[test]
fn config_set_region_matches_reference_cases() {
    let cases = [
        (
            config_case_1(),
            "eu-west-1",
            vec![Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "eu-west-1".to_owned(),
            }],
        ),
        (
            config_case_1(),
            "",
            vec![Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            }],
        ),
        (
            config_case_2_without_region(),
            "us-east-1",
            vec![Arn {
                target_id: TargetId::new("1", "webhook"),
                region: "us-east-1".to_owned(),
            }],
        ),
        (
            config_case_2_without_region(),
            "",
            vec![Arn {
                target_id: TargetId::new("1", "webhook"),
                region: String::new(),
            }],
        ),
        (
            config_case_6(),
            "us-east-1",
            vec![
                Arn {
                    target_id: TargetId::new("1", "webhook"),
                    region: "us-east-1".to_owned(),
                },
                Arn {
                    target_id: TargetId::new("2", "amqp"),
                    region: "us-east-1".to_owned(),
                },
            ],
        ),
        (
            config_case_6(),
            "",
            vec![
                Arn {
                    target_id: TargetId::new("1", "webhook"),
                    region: String::new(),
                },
                Arn {
                    target_id: TargetId::new("2", "amqp"),
                    region: String::new(),
                },
            ],
        ),
    ];

    for (xml, region, expected) in cases {
        let mut config = parse_config_xml(xml);
        config.set_region(region);
        let result: Vec<_> = config
            .queue_list
            .into_iter()
            .map(|queue| queue.arn)
            .collect();
        assert_eq!(result, expected);
    }
}

#[test]
fn config_to_rules_map_matches_reference_cases() {
    let config1 = parse_config_xml(config_case_1());
    let config2 = parse_config_xml(config_case_2_without_region());
    let config3 = parse_config_xml(config_case_6());

    let expected1 = RulesMap::new(
        &[
            Name::ObjectAccessedAll,
            Name::ObjectCreatedAll,
            Name::ObjectRemovedAll,
        ],
        "*",
        TargetId::new("1", "webhook"),
    );

    let expected2 = RulesMap::new(
        &[Name::ObjectCreatedPut],
        "images/*jpg",
        TargetId::new("1", "webhook"),
    );

    let mut expected3 = RulesMap::new(
        &[
            Name::ObjectAccessedAll,
            Name::ObjectCreatedAll,
            Name::ObjectRemovedAll,
        ],
        "*",
        TargetId::new("1", "webhook"),
    );
    expected3.add(&RulesMap::new(
        &[Name::ObjectCreatedPut],
        "images/*jpg",
        TargetId::new("2", "amqp"),
    ));

    assert_eq!(config1.to_rules_map(), expected1);
    assert_eq!(config2.to_rules_map(), expected2);
    assert_eq!(config3.to_rules_map(), expected3);
}

#[test]
fn parse_config_matches_reference_cases() {
    let target_list1 = TargetList::new();

    let mut target_list2 = TargetList::new();
    target_list2
        .add(ExampleTarget::new(TargetId::new("1", "webhook")))
        .expect("seed target should be added");

    let cases = [
        (config_case_1(), "eu-west-1", None, true),
        (config_case_2(), "us-east-1", Some(&target_list1), true),
        (config_case_4(), "us-east-1", Some(&target_list1), true),
        (config_case_3(), "", Some(&target_list2), false),
        (config_case_2(), "us-east-1", Some(&target_list2), false),
    ];

    for (xml, region, target_list, should_err) in cases {
        let cursor = Cursor::new(xml.as_bytes());
        let result = parse_config(cursor, region, target_list);
        assert_eq!(result.is_err(), should_err);
    }
}
