use minio_rust::internal::event::{TargetId, TargetIdSet};

#[test]
fn target_id_set_clone_matches_reference_cases() {
    let cases = [
        (TargetIdSet::new([]), TargetId::new("1", "webhook")),
        (
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetId::new("2", "webhook"),
        ),
        (
            TargetIdSet::new([TargetId::new("1", "webhook"), TargetId::new("2", "amqp")]),
            TargetId::new("2", "webhook"),
        ),
    ];

    for (set, target_id_to_add) in cases {
        let mut result = set.clone_set();
        assert_eq!(result, set);
        result.add(target_id_to_add);
        assert_ne!(result, set);
    }
}

#[test]
fn target_id_set_union_matches_reference_cases() {
    let cases = [
        (
            TargetIdSet::new([]),
            TargetIdSet::new([]),
            TargetIdSet::new([]),
        ),
        (
            TargetIdSet::new([]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([TargetId::new("2", "amqp")]),
            TargetIdSet::new([TargetId::new("1", "webhook"), TargetId::new("2", "amqp")]),
        ),
        (
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
    ];

    for (set, set_to_add, expected) in cases {
        assert_eq!(set.union(&set_to_add), expected);
    }
}

#[test]
fn target_id_set_difference_matches_reference_cases() {
    let cases = [
        (
            TargetIdSet::new([]),
            TargetIdSet::new([]),
            TargetIdSet::new([]),
        ),
        (
            TargetIdSet::new([]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([]),
        ),
        (
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([TargetId::new("2", "amqp")]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([TargetId::new("1", "webhook")]),
            TargetIdSet::new([]),
        ),
    ];

    for (set, set_to_remove, expected) in cases {
        assert_eq!(set.difference(&set_to_remove), expected);
    }
}

#[test]
fn new_target_id_set_matches_reference_cases() {
    let cases = [
        (vec![], TargetIdSet::new([])),
        (
            vec![TargetId::new("1", "webhook")],
            TargetIdSet::new([TargetId::new("1", "webhook")]),
        ),
        (
            vec![TargetId::new("1", "webhook"), TargetId::new("2", "amqp")],
            TargetIdSet::new([TargetId::new("1", "webhook"), TargetId::new("2", "amqp")]),
        ),
    ];

    for (target_ids, expected) in cases {
        assert_eq!(TargetIdSet::new(target_ids), expected);
    }
}
