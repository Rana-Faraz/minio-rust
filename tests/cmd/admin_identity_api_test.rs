use std::collections::{BTreeMap, BTreeSet};

use crate::cmd::{
    AdminIdentityApi, IdentityAccountStatus, LdapConfig, OpenIdClaims, OpenIdProvider, StsGroup,
    StsPolicy, StsUser,
};

fn readwrite_policy_json() -> Vec<u8> {
    br#"{
        "Version":"2012-10-17",
        "Statement":[
            {
                "Effect":"Allow",
                "Action":["s3:CreateBucket","s3:ListBucket","s3:PutObject","s3:GetObject","admin:CreateServiceAccount"],
                "Resource":["*"]
            }
        ]
    }"#
    .to_vec()
}

#[test]
fn admin_identity_snapshot_roundtrips_admin_users_policies_and_service_accounts() {
    let mut api = AdminIdentityApi::new(false);

    api.set_user("alice", "alice-secret", IdentityAccountStatus::Enabled)
        .expect("set user");
    api.add_canned_policy("rw", &readwrite_policy_json())
        .expect("add policy");
    api.attach_policy("alice", "rw").expect("attach policy");
    let root_access_key = api.root_access_key().to_string();
    let root_secret_key = api.root_secret_key().to_string();
    api.add_service_account(
        &root_access_key,
        &root_secret_key,
        "alice",
        Some(br#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["*"]}]}"#),
        Some("svc-alice"),
        Some("svc-alice-secret"),
    )
    .expect("add service account");
    api.update_service_account(
        "svc-alice",
        Some("svc-alice-secret-2"),
        Some(IdentityAccountStatus::Disabled),
    )
    .expect("update service account");

    let snapshot = api.export_snapshot();
    let mut restored = AdminIdentityApi::new(false);
    restored.import_snapshot(snapshot).expect("import snapshot");

    let users = restored.list_users();
    let alice = users.get("alice").expect("alice");
    assert_eq!(alice.secret_key, "alice-secret");
    assert_eq!(alice.status, IdentityAccountStatus::Enabled);
    assert!(alice.attached_policies.contains("rw"));

    let service_accounts = restored.list_service_accounts(Some("alice"));
    assert_eq!(service_accounts.len(), 1);
    assert_eq!(service_accounts[0].access_key, "svc-alice");
    assert_eq!(service_accounts[0].secret_key, "svc-alice-secret-2");
    assert_eq!(service_accounts[0].status, IdentityAccountStatus::Disabled);
    assert!(service_accounts[0].session_policy_json.is_some());
}

#[test]
fn admin_identity_facade_issues_openid_and_ldap_sessions_and_session_service_accounts() {
    let mut api = AdminIdentityApi::new(false);

    api.add_sts_policy(StsPolicy {
        name: "readonly".to_string(),
        allow_actions: BTreeSet::from(["s3:GetObject".to_string()]),
        deny_actions: BTreeSet::new(),
        resource_patterns: vec!["arn:aws:s3:::tenant/*".to_string()],
    });
    api.add_sts_group(StsGroup {
        name: "cn=admins,ou=groups".to_string(),
        policies: BTreeSet::from(["readonly".to_string()]),
    });
    api.add_sts_user(StsUser {
        username: "alice".to_string(),
        secret_key: "alice-secret".to_string(),
        policies: BTreeSet::from(["readonly".to_string()]),
        groups: BTreeSet::new(),
        enabled: true,
    });
    api.add_openid_provider(OpenIdProvider {
        name: "oidc".to_string(),
        claim_name: "department".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([(
            "dev".to_string(),
            BTreeSet::from(["readonly".to_string()]),
        )]),
    });
    api.set_ldap_config(LdapConfig {
        normalized_base_dn: false,
    });
    api.validate_openid_configs().expect("validate openid");

    let openid = api
        .assume_role_with_openid(
            "oidc",
            &OpenIdClaims {
                subject: "sub-1".to_string(),
                preferred_username: "alice".to_string(),
                roles: BTreeSet::from(["dev".to_string()]),
                custom: BTreeMap::new(),
            },
            None,
        )
        .expect("openid session");
    assert_eq!(openid.source, "openid");
    assert!(api.sts_allowed(&openid, "s3:GetObject", "arn:aws:s3:::tenant/report.txt"));

    let session_service_account = api
        .create_session_service_account(&openid)
        .expect("service account from session");
    assert_eq!(session_service_account.username, "alice");
    assert!(session_service_account
        .effective_policies
        .contains(&"readonly".to_string()));

    let ldap = api
        .assume_role_with_ldap(
            "alice",
            "uid=alice,ou=people",
            &["cn=admins,ou=groups".to_string()],
        )
        .expect("ldap session");
    assert_eq!(ldap.source, "ldap");
    assert!(api.sts_allowed(&ldap, "s3:GetObject", "arn:aws:s3:::tenant/ldap.txt"));
}
