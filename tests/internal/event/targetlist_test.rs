use minio_rust::internal::event::{Target, TargetId, TargetList};

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

#[test]
fn target_list_add_matches_reference_cases() {
    let target_list_case1 = TargetList::new();

    let mut target_list_case2 = TargetList::new();
    target_list_case2
        .add(ExampleTarget::new(TargetId::new("2", "testcase")))
        .expect("seed target should be added");

    let mut target_list_case3 = TargetList::new();
    target_list_case3
        .add(ExampleTarget::new(TargetId::new("3", "testcase")))
        .expect("seed target should be added");

    let cases = [
        (
            target_list_case1,
            ExampleTarget::new(TargetId::new("1", "webhook")),
            vec![TargetId::new("1", "webhook")],
            false,
        ),
        (
            target_list_case2,
            ExampleTarget::new(TargetId::new("1", "webhook")),
            vec![
                TargetId::new("2", "testcase"),
                TargetId::new("1", "webhook"),
            ],
            false,
        ),
        (
            target_list_case3,
            ExampleTarget::new(TargetId::new("3", "testcase")),
            vec![],
            true,
        ),
    ];

    for (mut target_list, target, expected, should_err) in cases {
        let result = target_list.add(target);
        assert_eq!(result.is_err(), should_err);
        if !should_err {
            let listed = target_list.list();
            assert_eq!(listed.len(), expected.len());
            for target_id in expected {
                assert!(listed.contains(&target_id));
            }
        }
    }
}

#[test]
fn target_list_exists_matches_reference_cases() {
    let target_list_case1 = TargetList::new();

    let mut target_list_case2 = TargetList::new();
    target_list_case2
        .add(ExampleTarget::new(TargetId::new("2", "testcase")))
        .expect("seed target should be added");

    let mut target_list_case3 = TargetList::new();
    target_list_case3
        .add(ExampleTarget::new(TargetId::new("3", "testcase")))
        .expect("seed target should be added");

    let cases = [
        (target_list_case1, TargetId::new("1", "webhook"), false),
        (target_list_case2, TargetId::new("1", "webhook"), false),
        (target_list_case3, TargetId::new("3", "testcase"), true),
    ];

    for (target_list, target_id, expected) in cases {
        assert_eq!(target_list.exists(&target_id), expected);
    }
}

#[test]
fn target_list_list_matches_reference_cases() {
    let target_list_case1 = TargetList::new();

    let mut target_list_case2 = TargetList::new();
    target_list_case2
        .add(ExampleTarget::new(TargetId::new("2", "testcase")))
        .expect("seed target should be added");

    let mut target_list_case3 = TargetList::new();
    target_list_case3
        .add(ExampleTarget::new(TargetId::new("3", "testcase")))
        .expect("seed target should be added");
    target_list_case3
        .add(ExampleTarget::new(TargetId::new("1", "webhook")))
        .expect("seed target should be added");

    let cases = [
        (target_list_case1, vec![]),
        (target_list_case2, vec![TargetId::new("2", "testcase")]),
        (
            target_list_case3,
            vec![
                TargetId::new("3", "testcase"),
                TargetId::new("1", "webhook"),
            ],
        ),
    ];

    for (target_list, expected) in cases {
        let listed = target_list.list();
        assert_eq!(listed.len(), expected.len());
        for target_id in expected {
            assert!(listed.contains(&target_id));
        }
    }
}

#[test]
fn new_target_list_matches_reference_cases() {
    let result = TargetList::new();
    assert_eq!(result.list().len(), 0);
}
