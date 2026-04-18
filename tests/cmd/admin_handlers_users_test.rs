// Rust test snapshot derived from cmd/admin-handlers-users_test.go.

use minio_rust::cmd::{AccountStatus, AdminUsers};

pub const SOURCE_FILE: &str = "cmd/admin-handlers-users_test.go";

#[test]
fn test_iaminternal_idpserver_suite_line_192() {
    let mut iam = AdminUsers::new(false);
    iam.add_root_bucket("main-bucket");

    iam.set_user("alice", "secret1", AccountStatus::Enabled)
        .expect("create user");
    let users = iam.list_users();
    assert_eq!(users["alice"].status, AccountStatus::Enabled);

    iam.attach_policy("alice", "readwrite").expect("attach");
    iam.make_bucket("alice", "secret1", "alice-bucket")
        .expect("make bucket");
    assert!(iam.list_buckets("alice", "secret1").is_ok());

    iam.set_user("alice", "secret2", AccountStatus::Enabled)
        .expect("update secret");
    assert!(iam.list_buckets("alice", "secret1").is_err());
    assert!(iam.list_buckets("alice", "secret2").is_ok());

    iam.set_user_status("alice", AccountStatus::Disabled)
        .expect("disable");
    assert!(iam.list_buckets("alice", "secret2").is_err());

    iam.set_user_status("alice", AccountStatus::Enabled)
        .expect("enable");
    iam.attach_policy("alice", "consoleAdmin")
        .expect("console admin");
    let svc = iam
        .add_service_account("alice", "secret2", "alice", None, None, None)
        .expect("create service account");
    assert!(iam.list_buckets(&svc.access_key, &svc.secret_key).is_ok());
    assert!(iam
        .add_service_account(&svc.access_key, &svc.secret_key, "alice", None, None, None)
        .is_err());

    iam.remove_user("alice").expect("remove");
    assert!(!iam.list_users().contains_key("alice"));
    assert!(iam.list_buckets("alice", "secret2").is_err());
}

#[test]
fn subtest_test_iaminternal_idpserver_suite_line_197() {
    for name in ["user lifecycle", "service account lifecycle"] {
        let mut iam = AdminUsers::new(false);
        iam.add_root_bucket("bucket");
        iam.set_user("bob", "secret", AccountStatus::Enabled)
            .expect("create");
        iam.attach_policy("bob", "consoleAdmin").expect("attach");
        let svc = iam
            .add_service_account("bob", "secret", "bob", None, Some("svc"), Some("svcsecret"))
            .expect(name);
        assert_eq!(
            iam.list_service_accounts("bob"),
            vec![svc.access_key.clone()]
        );
        iam.update_service_account(
            &svc.access_key,
            Some("newsecret"),
            Some(AccountStatus::Disabled),
        )
        .expect("update");
        assert_eq!(
            iam.service_account_status(&svc.access_key),
            Some(AccountStatus::Disabled)
        );
        iam.delete_service_account(&svc.access_key).expect("delete");
        assert!(iam.list_service_accounts("bob").is_empty());
    }
}

#[test]
fn test_iam_ampinternal_idpserver_suite_line_1379() {
    let mut iam = AdminUsers::new(true);
    iam.add_root_bucket("plugin-bucket");

    iam.set_user("plugin-user", "secret", AccountStatus::Enabled)
        .expect("create user");
    assert!(iam
        .make_bucket("plugin-user", "secret", "plugin-user-bucket")
        .is_ok());
    assert!(iam.list_buckets("plugin-user", "secret").is_ok());
    assert!(iam
        .put_object("plugin-user", "secret", "plugin-bucket", "object")
        .is_err());

    let restricted_policy = br#"{
        "Version":"2012-10-17",
        "Statement":[{"Effect":"Allow","Action":["s3:PutObject"],"Resource":["arn:aws:s3:::plugin-bucket/*"]}]
    }"#;
    let svc = iam
        .add_service_account(
            "plugin-user",
            "secret",
            "plugin-user",
            Some(restricted_policy),
            Some("svc-plugin"),
            Some("svcsecret"),
        )
        .expect("create plugin service account");
    assert!(iam.list_buckets(&svc.access_key, &svc.secret_key).is_ok());
    assert!(iam
        .put_object(&svc.access_key, &svc.secret_key, "plugin-bucket", "object")
        .is_err());

    iam.set_user("other-user", "other-secret", AccountStatus::Enabled)
        .expect("other user");
    assert!(iam
        .add_service_account("plugin-user", "secret", "other-user", None, None, None)
        .is_ok());
}

#[test]
fn subtest_test_iam_ampinternal_idpserver_suite_line_1381() {
    for target_user in ["plugin-user", "root"] {
        let mut iam = AdminUsers::new(true);
        iam.add_root_bucket("plugin-bucket");
        iam.set_user("plugin-user", "secret", AccountStatus::Enabled)
            .expect("create");
        let target = if target_user == "root" {
            iam.root_access_key().to_string()
        } else {
            target_user.to_string()
        };
        let svc = iam
            .add_service_account("plugin-user", "secret", &target, None, None, None)
            .expect("create svc");
        assert!(iam.list_buckets(&svc.access_key, &svc.secret_key).is_ok());
    }
}
