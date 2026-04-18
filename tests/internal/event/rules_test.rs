use minio_rust::internal::event::{new_pattern, Rules, TargetId, TargetIdSet};

#[test]
fn new_pattern_matches_reference_cases() {
    let cases = [
        ("", "", ""),
        ("*", "", "*"),
        ("", "*", "*"),
        ("images/", "", "images/*"),
        ("images/*", "", "images/*"),
        ("", "jpg", "*jpg"),
        ("", "*jpg", "*jpg"),
        ("images/", "jpg", "images/*jpg"),
        ("images/*", "jpg", "images/*jpg"),
        ("images/", "*jpg", "images/*jpg"),
        ("images/*", "*jpg", "images/*jpg"),
        ("201*/images/", "jpg", "201*/images/*jpg"),
    ];

    for (prefix, suffix, expected) in cases {
        assert_eq!(new_pattern(prefix, suffix), expected);
    }
}

#[test]
fn rules_add_matches_reference_cases() {
    let rules_case1 = Rules::new();

    let mut rules_case2 = Rules::new();
    rules_case2.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));

    let mut rules_case3 = Rules::new();
    rules_case3.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));

    let mut rules_case4 = Rules::new();
    rules_case4.add(new_pattern("", "*.jpg"), TargetId::new("1", "webhook"));

    let rules_case5 = Rules::new();

    let mut rules_case6 = Rules::new();
    rules_case6.add(new_pattern("", "*.jpg"), TargetId::new("1", "webhook"));

    let mut rules_case7 = Rules::new();
    rules_case7.add(new_pattern("", "*.jpg"), TargetId::new("1", "webhook"));

    let mut rules_case8 = Rules::new();
    rules_case8.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));

    let cases = [
        (
            rules_case1,
            new_pattern("*", ""),
            TargetId::new("1", "webhook"),
            1,
        ),
        (
            rules_case2,
            new_pattern("*", ""),
            TargetId::new("2", "amqp"),
            2,
        ),
        (
            rules_case3,
            new_pattern("2010*", ""),
            TargetId::new("1", "webhook"),
            1,
        ),
        (
            rules_case4,
            new_pattern("*", ""),
            TargetId::new("1", "webhook"),
            2,
        ),
        (
            rules_case5,
            new_pattern("", "*.jpg"),
            TargetId::new("1", "webhook"),
            1,
        ),
        (
            rules_case6,
            new_pattern("", "*"),
            TargetId::new("2", "amqp"),
            2,
        ),
        (
            rules_case7,
            new_pattern("", "*.jpg"),
            TargetId::new("1", "webhook"),
            1,
        ),
        (
            rules_case8,
            new_pattern("", "*.jpg"),
            TargetId::new("1", "webhook"),
            2,
        ),
    ];

    for (mut rules, pattern, target_id, expected) in cases {
        rules.add(pattern, target_id);
        assert_eq!(rules.len(), expected);
    }
}

#[test]
fn rules_match_matches_reference_cases() {
    let rules_case1 = Rules::new();

    let mut rules_case2 = Rules::new();
    rules_case2.add(new_pattern("*", "*"), TargetId::new("1", "webhook"));

    let mut rules_case3 = Rules::new();
    rules_case3.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));
    rules_case3.add(new_pattern("", "*.png"), TargetId::new("2", "amqp"));

    let mut rules_case4 = Rules::new();
    rules_case4.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));

    let cases = [
        (rules_case1, "photos.jpg", TargetIdSet::new([])),
        (
            rules_case2,
            "photos.jpg",
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (
            rules_case3,
            "2010/photos.jpg",
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (rules_case4, "2000/photos.jpg", TargetIdSet::new([])),
    ];

    for (rules, object_name, expected) in cases {
        assert_eq!(rules.match_object(object_name), expected);
    }
}

#[test]
fn rules_clone_matches_reference_cases() {
    let rules_case1 = Rules::new();

    let mut rules_case2 = Rules::new();
    rules_case2.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));

    let mut rules_case3 = Rules::new();
    rules_case3.add(new_pattern("", "*.jpg"), TargetId::new("1", "webhook"));

    let cases = [
        (rules_case1, "2010*", TargetId::new("1", "webhook")),
        (rules_case2, "2000*", TargetId::new("2", "amqp")),
        (rules_case3, "2010*", TargetId::new("1", "webhook")),
    ];

    for (rules, prefix, target_id) in cases {
        let mut result = rules.clone_rules();
        assert_eq!(result, rules);
        result.add(new_pattern(prefix, ""), target_id);
        assert_ne!(result, rules);
    }
}

#[test]
fn rules_union_matches_reference_cases() {
    let rules_case1 = Rules::new();
    let rules2_case1 = Rules::new();
    let expected1 = Rules::new();

    let rules_case2 = Rules::new();
    let mut rules2_case2 = Rules::new();
    rules2_case2.add(new_pattern("*", ""), TargetId::new("1", "webhook"));
    let mut expected2 = Rules::new();
    expected2.add(new_pattern("*", ""), TargetId::new("1", "webhook"));

    let mut rules_case3 = Rules::new();
    rules_case3.add(new_pattern("", "*"), TargetId::new("1", "webhook"));
    let rules2_case3 = Rules::new();
    let mut expected3 = Rules::new();
    expected3.add(new_pattern("", "*"), TargetId::new("1", "webhook"));

    let mut rules_case4 = Rules::new();
    rules_case4.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));
    let mut rules2_case4 = Rules::new();
    rules2_case4.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));
    let mut expected4 = Rules::new();
    expected4.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));

    let mut rules_case5 = Rules::new();
    rules_case5.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));
    rules_case5.add(new_pattern("", "*.png"), TargetId::new("2", "amqp"));
    let mut rules2_case5 = Rules::new();
    rules2_case5.add(new_pattern("*", ""), TargetId::new("1", "webhook"));
    let mut expected5 = Rules::new();
    expected5.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));
    expected5.add(new_pattern("", "*.png"), TargetId::new("2", "amqp"));
    expected5.add(new_pattern("*", ""), TargetId::new("1", "webhook"));

    let cases = [
        (rules_case1, rules2_case1, expected1),
        (rules_case2, rules2_case2, expected2),
        (rules_case3, rules2_case3, expected3),
        (rules_case4, rules2_case4, expected4),
        (rules_case5, rules2_case5, expected5),
    ];

    for (rules, rules2, expected) in cases {
        assert_eq!(rules.union(&rules2), expected);
    }
}

#[test]
fn rules_difference_matches_reference_cases() {
    let rules_case1 = Rules::new();
    let rules2_case1 = Rules::new();
    let expected1 = Rules::new();

    let rules_case2 = Rules::new();
    let mut rules2_case2 = Rules::new();
    rules2_case2.add(new_pattern("*", "*"), TargetId::new("1", "webhook"));
    let expected2 = Rules::new();

    let mut rules_case3 = Rules::new();
    rules_case3.add(new_pattern("*", "*"), TargetId::new("1", "webhook"));
    let rules2_case3 = Rules::new();
    let mut expected3 = Rules::new();
    expected3.add(new_pattern("*", "*"), TargetId::new("1", "webhook"));

    let mut rules_case4 = Rules::new();
    rules_case4.add(new_pattern("*", "*"), TargetId::new("1", "webhook"));
    let mut rules2_case4 = Rules::new();
    rules2_case4.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));
    rules2_case4.add(new_pattern("", "*.png"), TargetId::new("2", "amqp"));
    let mut expected4 = Rules::new();
    expected4.add(new_pattern("*", "*"), TargetId::new("1", "webhook"));

    let mut rules_case5 = Rules::new();
    rules_case5.add(new_pattern("*", ""), TargetId::new("1", "webhook"));
    rules_case5.add(new_pattern("", "*"), TargetId::new("2", "amqp"));
    let mut rules2_case5 = Rules::new();
    rules2_case5.add(new_pattern("2010*", ""), TargetId::new("1", "webhook"));
    rules2_case5.add(new_pattern("", "*"), TargetId::new("2", "amqp"));
    let mut expected5 = Rules::new();
    expected5.add(new_pattern("*", ""), TargetId::new("1", "webhook"));

    let cases = [
        (rules_case1, rules2_case1, expected1),
        (rules_case2, rules2_case2, expected2),
        (rules_case3, rules2_case3, expected3),
        (rules_case4, rules2_case4, expected4),
        (rules_case5, rules2_case5, expected5),
    ];

    for (rules, rules2, expected) in cases {
        assert_eq!(rules.difference(&rules2), expected);
    }
}
