use minio_rust::cmd::split_path;

pub const SOURCE_FILE: &str = "cmd/iam-object-store_test.go";

#[test]
fn test_split_path_line_24() {
    let cases = [
        ("format.json", false, "format.json", ""),
        ("users/tester.json", false, "users/", "tester.json"),
        (
            "groups/test/group.json",
            false,
            "groups/",
            "test/group.json",
        ),
        (
            "policydb/groups/testgroup.json",
            true,
            "policydb/groups/",
            "testgroup.json",
        ),
        (
            "policydb/sts-users/uid=slash/user,ou=people,ou=swengg,dc=min,dc=io.json",
            true,
            "policydb/sts-users/",
            "uid=slash/user,ou=people,ou=swengg,dc=min,dc=io.json",
        ),
        (
            "policydb/sts-users/uid=slash/user/twice,ou=people,ou=swengg,dc=min,dc=io.json",
            true,
            "policydb/sts-users/",
            "uid=slash/user/twice,ou=people,ou=swengg,dc=min,dc=io.json",
        ),
        (
            "policydb/groups/cn=project/d,ou=groups,ou=swengg,dc=min,dc=io.json",
            true,
            "policydb/groups/",
            "cn=project/d,ou=groups,ou=swengg,dc=min,dc=io.json",
        ),
    ];

    for (path, second_index, expected_key, expected_item) in cases {
        let (list_key, item) = split_path(path, second_index);
        assert_eq!(list_key, expected_key);
        assert_eq!(item, expected_item);
    }
}
