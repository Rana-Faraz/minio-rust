use minio_rust::internal::event::{Name, RulesMap, TargetId, TargetIdSet};

#[test]
fn rules_map_clone_matches_reference_cases() {
    let rules_map_case1 = RulesMap::new(&[], "*", TargetId::new("", ""));
    let rules_map_to_add_case1 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );

    let rules_map_case2 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );
    let rules_map_to_add_case2 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );

    let rules_map_case3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );
    let rules_map_to_add_case3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );

    let cases = [
        (rules_map_case1, rules_map_to_add_case1),
        (rules_map_case2, rules_map_to_add_case2),
        (rules_map_case3, rules_map_to_add_case3),
    ];

    for (rules_map, rules_map_to_add) in cases {
        let mut result = rules_map.clone_map();
        assert_eq!(result, rules_map);
        result.add(&rules_map_to_add);
        assert_ne!(result, rules_map);
    }
}

#[test]
fn rules_map_add_matches_reference_cases() {
    let rules_map_case1 = RulesMap::default();
    let rules_map_to_add_case1 = RulesMap::default();
    let expected1 = RulesMap::default();

    let rules_map_case2 = RulesMap::default();
    let rules_map_to_add_case2 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );
    let expected2 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );

    let rules_map_case3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );
    let rules_map_to_add_case3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );
    let mut expected3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );
    expected3.add(&RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    ));

    let cases = [
        (rules_map_case1, rules_map_to_add_case1, expected1),
        (rules_map_case2, rules_map_to_add_case2, expected2),
        (rules_map_case3, rules_map_to_add_case3, expected3),
    ];

    for (mut rules_map, rules_map_to_add, expected) in cases {
        rules_map.add(&rules_map_to_add);
        assert_eq!(rules_map, expected);
    }
}

#[test]
fn rules_map_remove_matches_reference_cases() {
    let rules_map_case1 = RulesMap::default();
    let rules_map_to_add_case1 = RulesMap::default();
    let expected1 = RulesMap::default();

    let rules_map_case2 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );
    let rules_map_to_add_case2 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );
    let expected2 = RulesMap::default();

    let mut rules_map_case3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );
    rules_map_case3.add(&RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    ));
    let rules_map_to_add_case3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );
    let expected3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );

    let cases = [
        (rules_map_case1, rules_map_to_add_case1, expected1),
        (rules_map_case2, rules_map_to_add_case2, expected2),
        (rules_map_case3, rules_map_to_add_case3, expected3),
    ];

    for (mut rules_map, rules_map_to_add, expected) in cases {
        rules_map.remove(&rules_map_to_add);
        assert_eq!(rules_map, expected);
    }
}

#[test]
fn rules_map_match_matches_reference_cases() {
    let rules_map_case1 = RulesMap::default();
    let rules_map_case2 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("1", "webhook"),
    );
    let rules_map_case3 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );
    let mut rules_map_case4 = RulesMap::new(
        &[Name::ObjectCreatedAll],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );
    rules_map_case4.add(&RulesMap::new(
        &[Name::ObjectCreatedAll],
        "*",
        TargetId::new("2", "amqp"),
    ));

    let cases = [
        (
            rules_map_case1,
            Name::ObjectCreatedPut,
            "2010/photo.jpg",
            TargetIdSet::new([]),
        ),
        (
            rules_map_case2,
            Name::ObjectCreatedPut,
            "2010/photo.jpg",
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (
            rules_map_case3,
            Name::ObjectCreatedPut,
            "2000/photo.png",
            TargetIdSet::new([]),
        ),
        (
            rules_map_case4,
            Name::ObjectCreatedPut,
            "2000/photo.png",
            TargetIdSet::new([TargetId::new("2", "amqp")]),
        ),
    ];

    for (rules_map, event_name, object_name, expected) in cases {
        assert_eq!(rules_map.match_object(event_name, object_name), expected);
    }
}

#[test]
fn new_rules_map_matches_reference_cases() {
    let case1 = RulesMap::new(
        &[Name::ObjectAccessedAll],
        "",
        TargetId::new("1", "webhook"),
    );
    let case2 = RulesMap::new(
        &[Name::ObjectAccessedAll, Name::ObjectCreatedPut],
        "",
        TargetId::new("1", "webhook"),
    );
    let case3 = RulesMap::new(
        &[Name::ObjectRemovedDelete],
        "2010*.jpg",
        TargetId::new("1", "webhook"),
    );

    let cases = [
        (
            vec![Name::ObjectAccessedAll],
            "",
            TargetId::new("1", "webhook"),
            case1,
        ),
        (
            vec![Name::ObjectAccessedAll, Name::ObjectCreatedPut],
            "",
            TargetId::new("1", "webhook"),
            case2,
        ),
        (
            vec![Name::ObjectRemovedDelete],
            "2010*.jpg",
            TargetId::new("1", "webhook"),
            case3,
        ),
    ];

    for (event_names, pattern, target_id, expected) in cases {
        assert_eq!(RulesMap::new(&event_names, pattern, target_id), expected);
    }
}
