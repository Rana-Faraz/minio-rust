use chrono::{Duration, TimeZone, Utc};
use minio_rust::internal::bucket::lifecycle::{
    Action, Boolean, Evaluator, Event, Expiration, Filter, Lifecycle, NoncurrentVersionExpiration,
    ObjectOpts, Rule, Tag, ENABLED,
};

#[test]
fn test_newer_noncurrent_versions_line_28() {
    let policy = Lifecycle {
        rules: vec![
            Rule {
                id: "rule-3".to_owned(),
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    tag: Tag {
                        key: "tag3".to_owned(),
                        value: "minio".to_owned(),
                    },
                    ..Filter::default()
                },
                noncurrent_version_expiration: NoncurrentVersionExpiration {
                    newer_noncurrent_versions: 3,
                    set: true,
                    ..NoncurrentVersionExpiration::default()
                },
                ..Rule::default()
            },
            Rule {
                id: "rule-4".to_owned(),
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    tag: Tag {
                        key: "tag4".to_owned(),
                        value: "minio".to_owned(),
                    },
                    ..Filter::default()
                },
                noncurrent_version_expiration: NoncurrentVersionExpiration {
                    newer_noncurrent_versions: 4,
                    set: true,
                    ..NoncurrentVersionExpiration::default()
                },
                ..Rule::default()
            },
            Rule {
                id: "rule-5".to_owned(),
                status: ENABLED.to_owned(),
                filter: Filter {
                    set: true,
                    tag: Tag {
                        key: "tag5".to_owned(),
                        value: "minio".to_owned(),
                    },
                    ..Filter::default()
                },
                noncurrent_version_expiration: NoncurrentVersionExpiration {
                    newer_noncurrent_versions: 5,
                    set: true,
                    ..NoncurrentVersionExpiration::default()
                },
                ..Rule::default()
            },
        ],
        ..Lifecycle::default()
    };

    let version_ids = ["v0", "v1", "v2", "v3", "v4", "v5", "v6"];
    let tag_keys = ["tag3", "tag3", "tag3", "tag4", "tag4", "tag5", "tag5"];
    let base = Utc
        .with_ymd_and_hms(2025, 2, 10, 23, 0, 0)
        .single()
        .unwrap();
    let objs: Vec<_> = tag_keys
        .iter()
        .enumerate()
        .map(|(index, key)| ObjectOpts {
            name: "obj".to_owned(),
            version_id: version_ids[index].to_owned(),
            mod_time: Some(base - Duration::seconds(index as i64)),
            user_tags: format!("{key}=minio"),
            num_versions: version_ids.len(),
            is_latest: index == 0,
            successor_mod_time: (index > 0).then_some(base - Duration::seconds(index as i64 - 1)),
            ..ObjectOpts::default()
        })
        .collect();

    let evaluator = Evaluator::new(policy.clone());
    let events = evaluator.eval(&objs, base);
    assert_eq!(events.last().unwrap().action, Action::DeleteVersion);
    assert!(events[..events.len() - 1]
        .iter()
        .all(|event| event.action == Action::None));

    let policy = Lifecycle {
        rules: vec![Rule {
            id: "all".to_owned(),
            status: ENABLED.to_owned(),
            expiration: Expiration {
                days: Some(1),
                delete_all: Boolean {
                    val: true,
                    set: true,
                },
                set: true,
                ..Expiration::default()
            },
            ..Rule::default()
        }],
        ..Lifecycle::default()
    };
    let evaluator = Evaluator::new(policy);
    let events = evaluator.eval(&objs[..6], base + Duration::days(2));
    assert_eq!(events[0].action, Action::DeleteAllVersions);
}

#[test]
fn test_empty_evaluator_line_155() {
    let base = Utc
        .with_ymd_and_hms(2025, 2, 10, 23, 0, 0)
        .single()
        .unwrap();
    let objs: Vec<_> = (0..5)
        .map(|index| ObjectOpts {
            name: "obj".to_owned(),
            version_id: format!("v{index}"),
            mod_time: Some(base - Duration::seconds(index as i64)),
            num_versions: 5,
            is_latest: index == 0,
            successor_mod_time: (index > 0).then_some(base - Duration::seconds(index as i64 - 1)),
            ..ObjectOpts::default()
        })
        .collect();

    let evaluator = Evaluator::new(Lifecycle::default());
    let events = evaluator
        .evaluate(&objs)
        .expect("empty evaluator should succeed");
    assert!(events.iter().all(|event| *event == Event::default()));
}
