use minio_rust::cmd::{
    KmsError, KmsPolicyStatement, KmsService, KmsUser, STUB_CREATED_AT, STUB_CREATED_BY,
};

pub const SOURCE_FILE: &str = "cmd/kms-handlers_test.go";

const ROOT_ACCESS_KEY: &str = "root-access";
const ROOT_SECRET_KEY: &str = "root-secret";
const USER_ACCESS_KEY: &str = "miniofakeuseraccesskey";
const USER_SECRET_KEY: &str = "miniofakeusersecret";

fn allow(actions: &[&str], resources: &[&str]) -> KmsPolicyStatement {
    KmsPolicyStatement::allow(actions, resources)
}

fn deny(actions: &[&str], resources: &[&str]) -> KmsPolicyStatement {
    KmsPolicyStatement::deny(actions, resources)
}

fn kms_service(configured: bool) -> KmsService {
    KmsService::new(configured, ROOT_ACCESS_KEY, ROOT_SECRET_KEY)
}

fn with_user(service: &mut KmsService, statements: Vec<KmsPolicyStatement>) {
    service.add_user(KmsUser {
        access_key: USER_ACCESS_KEY.to_string(),
        secret_key: USER_SECRET_KEY.to_string(),
        statements,
    });
}

fn sorted_names(
    service: &KmsService,
    pattern: &str,
    statements: Vec<KmsPolicyStatement>,
) -> Vec<String> {
    let mut service = service.clone();
    with_user(&mut service, statements);
    let mut names = service
        .list_keys(USER_ACCESS_KEY, USER_SECRET_KEY, pattern)
        .expect("list keys")
        .into_iter()
        .map(|key| key.name)
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn test_kms_create_key_permissions() {
    let mut service = kms_service(true);

    assert_eq!(
        service.create_key(USER_ACCESS_KEY, USER_SECRET_KEY, "new-test-key", false),
        Err(KmsError::AccessDenied)
    );

    with_user(&mut service, vec![allow(&["kms:CreateKey"], &[])]);
    assert_eq!(
        service.create_key(USER_ACCESS_KEY, USER_SECRET_KEY, "new-test-key", false),
        Ok(())
    );

    let mut scoped = kms_service(true);
    with_user(
        &mut scoped,
        vec![allow(
            &["kms:CreateKey"],
            &["arn:minio:kms:::second-new-test-*"],
        )],
    );
    assert_eq!(
        scoped.create_key(
            USER_ACCESS_KEY,
            USER_SECRET_KEY,
            "second-new-test-key",
            false
        ),
        Ok(())
    );
    assert_eq!(
        scoped.create_key(
            USER_ACCESS_KEY,
            USER_SECRET_KEY,
            "third-new-test-key",
            false
        ),
        Err(KmsError::AccessDenied)
    );
}

#[test]
fn test_kms_key_status_permissions() {
    let mut service = kms_service(true);
    service
        .create_key(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "abc-test-key", false)
        .expect("root create");

    let info = service
        .key_status(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "abc-test-key", false)
        .expect("root key status");
    assert_eq!(info.name, "abc-test-key");
    assert_eq!(info.created_by, STUB_CREATED_BY);
    assert_eq!(info.created_at, STUB_CREATED_AT);

    assert_eq!(
        service.key_status(USER_ACCESS_KEY, USER_SECRET_KEY, "abc-test-key", false),
        Err(KmsError::AccessDenied)
    );

    with_user(&mut service, vec![allow(&["kms:KeyStatus"], &[])]);
    assert_eq!(
        service
            .key_status(USER_ACCESS_KEY, USER_SECRET_KEY, "abc-test-key", false)
            .map(|info| info.name),
        Ok("abc-test-key".to_string())
    );

    let mut scoped = kms_service(true);
    scoped
        .create_key(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "abc-test-key", false)
        .expect("root create");
    with_user(
        &mut scoped,
        vec![allow(&["kms:KeyStatus"], &["arn:minio:kms:::abc-test-*"])],
    );
    assert_eq!(
        scoped
            .key_status(USER_ACCESS_KEY, USER_SECRET_KEY, "abc-test-key", false)
            .map(|info| info.name),
        Ok("abc-test-key".to_string())
    );

    let mut denied = kms_service(true);
    denied
        .create_key(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "abc-test-key", false)
        .expect("root create");
    with_user(
        &mut denied,
        vec![allow(&["kms:KeyStatus"], &["arn:minio:kms:::xyz-test-key"])],
    );
    assert_eq!(
        denied.key_status(USER_ACCESS_KEY, USER_SECRET_KEY, "abc-test-key", false),
        Err(KmsError::AccessDenied)
    );
}

#[test]
fn test_kms_public_api_permissions() {
    let mut service = kms_service(true);

    assert_eq!(
        service.version(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Ok("version".to_string())
    );
    assert_eq!(
        service.api_paths(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Ok(vec!["stub/path".to_string()])
    );
    assert_eq!(
        service.metrics(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Ok("kms".to_string())
    );
    assert_eq!(
        service.status(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Ok("MinIO builtin".to_string())
    );

    assert_eq!(
        service.version(USER_ACCESS_KEY, USER_SECRET_KEY),
        Err(KmsError::AccessDenied)
    );

    with_user(
        &mut service,
        vec![
            allow(&["kms:Version"], &["arn:minio:kms:::ignored"]),
            allow(&["kms:API"], &["arn:minio:kms:::ignored"]),
            allow(&["kms:Metrics"], &["arn:minio:kms:::ignored"]),
            allow(&["kms:Status"], &["arn:minio:kms:::ignored"]),
        ],
    );

    assert_eq!(
        service.version(USER_ACCESS_KEY, USER_SECRET_KEY),
        Ok("version".to_string())
    );
    assert_eq!(
        service.api_paths(USER_ACCESS_KEY, USER_SECRET_KEY),
        Ok(vec!["stub/path".to_string()])
    );
    assert_eq!(
        service.metrics(USER_ACCESS_KEY, USER_SECRET_KEY),
        Ok("kms".to_string())
    );
    assert_eq!(
        service.status(USER_ACCESS_KEY, USER_SECRET_KEY),
        Ok("MinIO builtin".to_string())
    );
}

#[test]
fn test_kms_list_keys_permissions_and_filters() {
    let mut service = kms_service(true);
    service
        .create_key(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "abc-test-key", false)
        .expect("create abc");
    service
        .create_key(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "xyz-test-key", false)
        .expect("create xyz");

    let mut root_names = service
        .list_keys(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "*")
        .expect("root list")
        .into_iter()
        .map(|key| key.name)
        .collect::<Vec<_>>();
    root_names.sort();
    assert_eq!(
        root_names,
        vec!["abc-test-key", "default-test-key", "xyz-test-key"]
    );

    assert_eq!(
        service.list_keys(USER_ACCESS_KEY, USER_SECRET_KEY, "*"),
        Err(KmsError::AccessDenied)
    );

    assert_eq!(
        sorted_names(&service, "*", vec![allow(&["kms:ListKeys"], &[])]),
        vec!["abc-test-key", "default-test-key", "xyz-test-key"]
    );
    assert_eq!(
        sorted_names(
            &service,
            "*",
            vec![allow(&["kms:ListKeys"], &["arn:minio:kms:::abc*"])]
        ),
        vec!["abc-test-key"]
    );
    assert_eq!(
        sorted_names(
            &service,
            "abc*",
            vec![allow(&["kms:ListKeys"], &["arn:minio:kms:::abc*"])]
        ),
        vec!["abc-test-key"]
    );
    assert!(sorted_names(
        &service,
        "xyz*",
        vec![allow(&["kms:ListKeys"], &["arn:minio:kms:::abc*"])]
    )
    .is_empty());
    assert!(sorted_names(
        &service,
        "*",
        vec![allow(&["kms:ListKeys"], &["arn:minio:kms:::nonematch*"])]
    )
    .is_empty());

    let mut denied = service.clone();
    with_user(
        &mut denied,
        vec![
            allow(&["kms:ListKeys"], &[]),
            deny(&["kms:ListKeys"], &["arn:minio:kms:::default-test-key"]),
        ],
    );
    assert_eq!(
        denied.list_keys(USER_ACCESS_KEY, USER_SECRET_KEY, "*"),
        Err(KmsError::AccessDenied)
    );
}

#[test]
fn test_kms_admin_api_permissions() {
    let mut service = kms_service(true);

    assert_eq!(
        service.create_key(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "abc-test-key", true),
        Ok(())
    );
    assert_eq!(
        service.admin_status(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Ok("MinIO builtin".to_string())
    );
    assert_eq!(
        service
            .key_status(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "default-test-key", true)
            .map(|info| info.name),
        Ok("default-test-key".to_string())
    );

    assert_eq!(
        service.create_key(USER_ACCESS_KEY, USER_SECRET_KEY, "new-test-key", true),
        Err(KmsError::AccessDenied)
    );
    assert_eq!(
        service.admin_status(USER_ACCESS_KEY, USER_SECRET_KEY),
        Err(KmsError::AccessDenied)
    );

    with_user(
        &mut service,
        vec![
            allow(&["admin:KMSCreateKey"], &["arn:minio:kms:::ignored"]),
            allow(&["admin:KMSKeyStatus"], &["arn:minio:kms:::ignored"]),
        ],
    );

    assert_eq!(
        service.create_key(USER_ACCESS_KEY, USER_SECRET_KEY, "third-new-test-key", true),
        Ok(())
    );
    assert_eq!(
        service.admin_status(USER_ACCESS_KEY, USER_SECRET_KEY),
        Ok("MinIO builtin".to_string())
    );
    assert_eq!(
        service
            .key_status(USER_ACCESS_KEY, USER_SECRET_KEY, "default-test-key", true)
            .map(|info| info.name),
        Ok("default-test-key".to_string())
    );
}

#[test]
fn test_kms_not_configured_or_invalid_credentials() {
    let service = kms_service(false);

    assert_eq!(
        service.status(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Err(KmsError::NotConfigured)
    );
    assert_eq!(
        service.metrics(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Err(KmsError::NotConfigured)
    );
    assert_eq!(
        service.api_paths(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Err(KmsError::NotConfigured)
    );
    assert_eq!(
        service.version(ROOT_ACCESS_KEY, ROOT_SECRET_KEY),
        Err(KmsError::NotConfigured)
    );
    assert_eq!(
        service.list_keys(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "*"),
        Err(KmsError::NotConfigured)
    );
    assert_eq!(
        service.key_status(ROOT_ACCESS_KEY, ROOT_SECRET_KEY, "master-key-id", false),
        Err(KmsError::NotConfigured)
    );

    let mut configured = kms_service(true);
    assert_eq!(
        configured.status(USER_ACCESS_KEY, USER_SECRET_KEY),
        Err(KmsError::AccessDenied)
    );
    assert_eq!(
        configured.metrics(USER_ACCESS_KEY, USER_SECRET_KEY),
        Err(KmsError::AccessDenied)
    );
    assert_eq!(
        configured.api_paths(USER_ACCESS_KEY, USER_SECRET_KEY),
        Err(KmsError::AccessDenied)
    );
    assert_eq!(
        configured.version(USER_ACCESS_KEY, USER_SECRET_KEY),
        Err(KmsError::AccessDenied)
    );
    assert_eq!(
        configured.create_key(USER_ACCESS_KEY, USER_SECRET_KEY, "master-key-id", false),
        Err(KmsError::AccessDenied)
    );
    assert_eq!(
        configured.list_keys(USER_ACCESS_KEY, USER_SECRET_KEY, "*"),
        Err(KmsError::AccessDenied)
    );
    assert_eq!(
        configured.key_status(USER_ACCESS_KEY, USER_SECRET_KEY, "master-key-id", false),
        Err(KmsError::AccessDenied)
    );
}
