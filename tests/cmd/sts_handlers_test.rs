use std::collections::{BTreeMap, BTreeSet};

use minio_rust::cmd::{
    LdapConfig, OpenIdClaims, OpenIdProvider, ServiceAccount, StsGroup, StsPolicy, StsService,
    StsUser, TempCredentials,
};

pub const SOURCE_FILE: &str = "cmd/sts-handlers_test.go";

fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn policy(name: &str, allow: &[&str], deny: &[&str], resources: &[&str]) -> StsPolicy {
    StsPolicy {
        name: name.to_string(),
        allow_actions: set(allow),
        deny_actions: set(deny),
        resource_patterns: resources.iter().map(|value| (*value).to_string()).collect(),
    }
}

fn as_temp(creds: &ServiceAccount) -> TempCredentials {
    TempCredentials {
        access_key: creds.access_key.clone(),
        secret_key: creds.secret_key.clone(),
        session_token: creds.session_token.clone(),
        parent: creds.username.clone(),
        username: creds.username.clone(),
        effective_policies: creds.effective_policies.clone(),
        revoked: false,
        tags: BTreeMap::new(),
        source: "service-account".to_string(),
    }
}

fn base_sts_service() -> StsService {
    let mut service = StsService::new("root-access", "root-secret");
    service.add_policy(policy(
        "root-bucket12",
        &[
            "s3:ListBucket",
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
        ],
        &[],
        &["bucket1*", "bucket2*"],
    ));
    service.add_policy(policy(
        "deny-delete-version",
        &["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        &["s3:DeleteObjectVersion"],
        &["versioned*"],
    ));
    service.add_policy(policy(
        "username-buckets",
        &["s3:*"],
        &[],
        &["${aws:username}-*"],
    ));
    service.add_policy(policy(
        "group-policy",
        &["s3:ListBucket"],
        &[],
        &["shared*"],
    ));
    service.add_policy(policy(
        "ldap-policy",
        &["s3:ListBucket"],
        &[],
        &["ldap-bucket*"],
    ));
    service.add_policy(policy(
        "ldap-import-policy",
        &["s3:GetObject"],
        &[],
        &["ldap-import*"],
    ));
    service.add_policy(policy(
        "openid-read",
        &["s3:GetObject"],
        &[],
        &["oidc-bucket*"],
    ));
    service.add_policy(policy(
        "openid-role",
        &["s3:ListBucket"],
        &[],
        &["role-bucket*"],
    ));
    service.add_policy(policy(
        "openid-role-vars",
        &["s3:GetObject"],
        &[],
        &["${aws:username}-oidc*"],
    ));
    service.add_policy(policy("role-a", &["s3:GetObject"], &[], &["multi-a*"]));
    service.add_policy(policy("role-b", &["s3:PutObject"], &[], &["multi-b*"]));
    service.add_policy(policy("amp-role", &["s3:DeleteObject"], &[], &["multi-b*"]));

    service.add_group(StsGroup {
        name: "shared-readers".to_string(),
        policies: set(&["group-policy"]),
    });
    service.add_group(StsGroup {
        name: "cn=devs,dc=example,dc=io".to_string(),
        policies: set(&["ldap-policy"]),
    });

    service.add_user(StsUser {
        username: "dillon".to_string(),
        secret_key: "dillon-123".to_string(),
        policies: set(&["username-buckets"]),
        groups: set(&["shared-readers"]),
        enabled: true,
    });
    service.add_user(StsUser {
        username: "version-user".to_string(),
        secret_key: "version-123".to_string(),
        policies: set(&["deny-delete-version"]),
        groups: BTreeSet::new(),
        enabled: true,
    });
    service
}

#[test]
fn test_iaminternal_idpstsserver_suite_line_55() {
    let mut service = base_sts_service();

    let root_creds = service
        .assume_role_for_root(
            "root-access",
            "root-secret",
            Some(policy(
                "root-session",
                &["s3:GetObject"],
                &[],
                &["bucket1*"],
            )),
        )
        .expect("root sts");
    assert!(service.allowed(&root_creds, "s3:GetObject", "bucket1/object"));
    assert!(!service.allowed(&root_creds, "s3:GetObject", "bucket3/object"));

    let mut tags = BTreeMap::new();
    tags.insert("project".to_string(), "alpha".to_string());
    let user_creds = service
        .assume_role_for_user("dillon", "dillon-123", None, tags.clone())
        .expect("user sts");
    assert!(service.allowed(&user_creds, "s3:GetObject", "dillon-bucket/object.txt"));
    assert!(service.allowed(&user_creds, "s3:ListBucket", "shared-bucket"));
    assert_eq!(user_creds.tags, tags);

    let svc = service
        .create_service_account(&user_creds)
        .expect("service account");
    let svc_creds = as_temp(&svc);
    assert!(service.allowed(&svc_creds, "s3:PutObject", "dillon-archive/file.txt"));

    service.revoke_session(&user_creds.access_key);
    assert!(!service.allowed(&user_creds, "s3:GetObject", "dillon-bucket/object.txt"));

    let version_creds = service
        .assume_role_for_user("version-user", "version-123", None, BTreeMap::new())
        .expect("version sts");
    assert!(service.allowed(&version_creds, "s3:DeleteObject", "versioned/object"));
    assert!(!service.allowed(&version_creds, "s3:DeleteObjectVersion", "versioned/object"));

    let escalation = service.assume_role_for_user(
        "dillon",
        "dillon-123",
        Some(policy(
            "bad-session",
            &["s3:DeleteObjectVersion"],
            &[],
            &["bucket9*"],
        )),
        BTreeMap::new(),
    );
    assert!(escalation.is_err());
}

#[test]
fn subtest_test_iaminternal_idpstsserver_suite_line_78() {
    let mut service = base_sts_service();
    let cases = [
        ("root", "bucket1/object", "s3:GetObject", true),
        ("user-own", "dillon-bucket/file", "s3:GetObject", true),
        ("user-other", "other-bucket/file", "s3:GetObject", false),
    ];

    let root_creds = service
        .assume_role_for_root(
            "root-access",
            "root-secret",
            Some(policy(
                "root-session-sub",
                &["s3:GetObject"],
                &[],
                &["bucket1*"],
            )),
        )
        .expect("root sts");
    let user_creds = service
        .assume_role_for_user("dillon", "dillon-123", None, BTreeMap::new())
        .expect("user sts");

    for (name, resource, action, want) in cases {
        let got = match name {
            "root" => service.allowed(&root_creds, action, resource),
            _ => service.allowed(&user_creds, action, resource),
        };
        assert_eq!(got, want, "{name}");
    }
}

#[test]
fn test_iamwith_ldapserver_suite_line_950() {
    let mut service = base_sts_service();
    let creds = service
        .assume_role_with_ldap(
            "dillon",
            "cn=dillon,dc=example,dc=io",
            &[String::from("cn=devs,dc=example,dc=io")],
        )
        .expect("ldap sts");
    assert!(service.allowed(&creds, "s3:ListBucket", "ldap-bucket"));
    assert!(!service.allowed(&creds, "s3:GetObject", "ldap-bucket/object"));
}

#[test]
fn subtest_test_iamwith_ldapserver_suite_line_952() {
    let mut service = base_sts_service();
    for (name, groups, want) in [
        (
            "mapped-group",
            vec![String::from("cn=devs,dc=example,dc=io")],
            true,
        ),
        ("no-group", Vec::new(), false),
    ] {
        let creds = service
            .assume_role_with_ldap("user", "cn=user,dc=example,dc=io", &groups)
            .expect("ldap sts");
        assert_eq!(
            service.allowed(&creds, "s3:ListBucket", "ldap-bucket"),
            want,
            "{name}"
        );
    }
}

#[test]
fn test_iamwith_ldapnon_normalized_base_dnconfig_server_suite_line_983() {
    let mut service = base_sts_service();
    service.set_ldap_config(LdapConfig {
        normalized_base_dn: true,
    });
    let result = service.assume_role_with_ldap(
        "user",
        "CN=User,DC=Example,DC=IO",
        &[String::from("cn=devs,dc=example,dc=io")],
    );
    assert!(result.is_err());
}

#[test]
fn subtest_test_iamwith_ldapnon_normalized_base_dnconfig_server_suite_line_985() {
    let mut service = base_sts_service();
    service.set_ldap_config(LdapConfig {
        normalized_base_dn: true,
    });
    let creds = service
        .assume_role_with_ldap(
            "user",
            "cn=user,dc=example,dc=io",
            &[String::from("cn=devs,dc=example,dc=io")],
        )
        .expect("normalized ldap");
    assert!(service.allowed(&creds, "s3:ListBucket", "ldap-bucket"));
}

#[test]
fn test_iamexport_import_with_ldap_line_1011() {
    let mut service = base_sts_service();
    service.set_ldap_config(LdapConfig {
        normalized_base_dn: true,
    });
    let exported = service.export_iam();

    let mut imported = StsService::new("root-access", "root-secret");
    imported.import_iam(exported);
    let creds = imported
        .assume_role_with_ldap(
            "user",
            "cn=user,dc=example,dc=io",
            &[String::from("cn=devs,dc=example,dc=io")],
        )
        .expect("ldap sts");
    assert!(imported.allowed(&creds, "s3:ListBucket", "ldap-bucket"));
}

#[test]
fn subtest_test_iamexport_import_with_ldap_line_1013() {
    let service = base_sts_service();
    let exported = service.export_iam();
    assert!(exported
        .groups
        .iter()
        .any(|group| group.name == "cn=devs,dc=example,dc=io"));
    assert!(exported
        .policies
        .iter()
        .any(|policy| policy.name == "ldap-policy"));
}

#[test]
fn test_iamimport_asset_with_ldap_line_1055() {
    let mut service = StsService::new("root-access", "root-secret");
    service.import_iam_assets(
        vec![policy(
            "ldap-import-policy",
            &["s3:GetObject"],
            &[],
            &["ldap-import*"],
        )],
        vec![],
        vec![StsGroup {
            name: "cn=imported,dc=example,dc=io".to_string(),
            policies: set(&["ldap-import-policy"]),
        }],
    );
    let creds = service
        .assume_role_with_ldap(
            "imported-user",
            "cn=imported-user,dc=example,dc=io",
            &[String::from("cn=imported,dc=example,dc=io")],
        )
        .expect("ldap imported");
    assert!(service.allowed(&creds, "s3:GetObject", "ldap-import/object"));
}

#[test]
fn subtest_test_iamimport_asset_with_ldap_line_1198() {
    let mut service = StsService::new("root-access", "root-secret");
    service.import_iam_assets(
        vec![policy(
            "ldap-import-policy",
            &["s3:GetObject"],
            &[],
            &["ldap-import*"],
        )],
        vec![StsUser {
            username: "asset-user".to_string(),
            secret_key: "asset-secret".to_string(),
            policies: set(&["ldap-import-policy"]),
            groups: BTreeSet::new(),
            enabled: true,
        }],
        vec![],
    );
    let creds = service
        .assume_role_for_user("asset-user", "asset-secret", None, BTreeMap::new())
        .expect("asset user");
    assert!(service.allowed(&creds, "s3:GetObject", "ldap-import/object"));
}

#[test]
fn test_iamwith_open_idserver_suite_line_2915() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "primary".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([("viewer".to_string(), set(&["openid-read"]))]),
    });

    let creds = service
        .assume_role_with_openid(
            "primary",
            &OpenIdClaims {
                subject: "sub-1".to_string(),
                preferred_username: "oidc-user".to_string(),
                roles: set(&["viewer"]),
                custom: BTreeMap::new(),
            },
            None,
        )
        .expect("openid sts");
    assert!(service.allowed(&creds, "s3:GetObject", "oidc-bucket/object"));
}

#[test]
fn subtest_test_iamwith_open_idserver_suite_line_2917() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "userinfo".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: true,
        role_policies: BTreeMap::from([("viewer".to_string(), set(&["openid-read"]))]),
    });
    let creds = service
        .assume_role_with_openid(
            "userinfo",
            &OpenIdClaims {
                subject: "sub-2".to_string(),
                preferred_username: "fallback".to_string(),
                roles: set(&["viewer"]),
                custom: BTreeMap::from([(
                    "preferred_username".to_string(),
                    "userinfo-user".to_string(),
                )]),
            },
            None,
        )
        .expect("openid sts");
    assert_eq!(creds.username, "userinfo-user");
}

#[test]
fn test_iamwith_open_idwith_role_policy_server_suite_line_2940() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "roles".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([("writer".to_string(), set(&["openid-role"]))]),
    });
    let creds = service
        .assume_role_with_openid(
            "roles",
            &OpenIdClaims {
                subject: "sub-3".to_string(),
                preferred_username: "role-user".to_string(),
                roles: set(&["writer"]),
                custom: BTreeMap::new(),
            },
            Some("writer"),
        )
        .expect("openid role");
    assert!(service.allowed(&creds, "s3:ListBucket", "role-bucket"));
}

#[test]
fn subtest_test_iamwith_open_idwith_role_policy_server_suite_line_2942() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "roles".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([("writer".to_string(), set(&["openid-role"]))]),
    });
    let err = service.assume_role_with_openid(
        "roles",
        &OpenIdClaims {
            subject: "sub-4".to_string(),
            preferred_username: "role-user".to_string(),
            roles: set(&["viewer"]),
            custom: BTreeMap::new(),
        },
        Some("viewer"),
    );
    assert!(err.is_err());
}

#[test]
fn test_iamwith_open_idwith_role_policy_with_policy_variables_server_suite_line_2963() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "vars".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([("viewer".to_string(), set(&["openid-role-vars"]))]),
    });
    let creds = service
        .assume_role_with_openid(
            "vars",
            &OpenIdClaims {
                subject: "sub-5".to_string(),
                preferred_username: "alice".to_string(),
                roles: set(&["viewer"]),
                custom: BTreeMap::new(),
            },
            None,
        )
        .expect("openid vars");
    assert!(service.allowed(&creds, "s3:GetObject", "alice-oidc/object"));
    assert!(!service.allowed(&creds, "s3:GetObject", "bob-oidc/object"));
}

#[test]
fn subtest_test_iamwith_open_idwith_role_policy_with_policy_variables_server_suite_line_2965() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "vars".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([("viewer".to_string(), set(&["openid-role-vars"]))]),
    });
    let creds = service
        .assume_role_with_openid(
            "vars",
            &OpenIdClaims {
                subject: "sub-6".to_string(),
                preferred_username: "carol".to_string(),
                roles: set(&["viewer"]),
                custom: BTreeMap::new(),
            },
            None,
        )
        .expect("openid vars");
    assert_eq!(creds.tags.get("sub").map(String::as_str), Some("sub-6"));
}

#[test]
fn test_iamwith_open_idmultiple_configs_validation1_line_3310() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "first".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::new(),
    });
    service.add_openid_provider(OpenIdProvider {
        name: "second".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::new(),
    });
    assert!(service.validate_openid_configs().is_err());
}

#[test]
fn subtest_test_iamwith_open_idmultiple_configs_validation1_line_3324() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "first".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::new(),
    });
    service.add_openid_provider(OpenIdProvider {
        name: "second".to_string(),
        claim_name: "email".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::new(),
    });
    assert!(service.validate_openid_configs().is_ok());
}

#[test]
fn test_iamwith_open_idmultiple_configs_validation2_line_3342() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "provider".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::new(),
    });
    let err = service.assume_role_with_openid(
        "provider",
        &OpenIdClaims {
            subject: "sub-7".to_string(),
            preferred_username: "nobody".to_string(),
            roles: set(&["viewer"]),
            custom: BTreeMap::new(),
        },
        None,
    );
    assert!(err.is_err());
}

#[test]
fn subtest_test_iamwith_open_idmultiple_configs_validation2_line_3356() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "provider".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([("viewer".to_string(), set(&["openid-read"]))]),
    });
    let creds = service
        .assume_role_with_openid(
            "provider",
            &OpenIdClaims {
                subject: "sub-8".to_string(),
                preferred_username: "viewer".to_string(),
                roles: set(&["viewer"]),
                custom: BTreeMap::new(),
            },
            None,
        )
        .expect("openid sts");
    assert!(service.allowed(&creds, "s3:GetObject", "oidc-bucket/object"));
}

#[test]
fn test_iamwith_open_idwith_multiple_roles_server_suite_line_3374() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "multi".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([
            ("role-a".to_string(), set(&["role-a"])),
            ("role-b".to_string(), set(&["role-b"])),
        ]),
    });
    let creds = service
        .assume_role_with_openid(
            "multi",
            &OpenIdClaims {
                subject: "sub-9".to_string(),
                preferred_username: "multi".to_string(),
                roles: set(&["role-a", "role-b"]),
                custom: BTreeMap::new(),
            },
            None,
        )
        .expect("multi role");
    assert!(service.allowed(&creds, "s3:GetObject", "multi-a/object"));
    assert!(service.allowed(&creds, "s3:PutObject", "multi-b/object"));
}

#[test]
fn subtest_test_iamwith_open_idwith_multiple_roles_server_suite_line_3388() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "multi".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([
            ("role-a".to_string(), set(&["role-a"])),
            ("role-b".to_string(), set(&["role-b"])),
        ]),
    });
    let creds = service
        .assume_role_with_openid(
            "multi",
            &OpenIdClaims {
                subject: "sub-10".to_string(),
                preferred_username: "multi".to_string(),
                roles: set(&["role-a", "role-b"]),
                custom: BTreeMap::new(),
            },
            Some("role-b"),
        )
        .expect("requested role");
    assert!(!service.allowed(&creds, "s3:GetObject", "multi-a/object"));
    assert!(service.allowed(&creds, "s3:PutObject", "multi-b/object"));
}

#[test]
fn test_iam_ampwith_open_idwith_multiple_roles_server_suite_line_3409() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "amp".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([
            ("role-b".to_string(), set(&["role-b"])),
            ("amp".to_string(), set(&["amp-role"])),
        ]),
    });
    let creds = service
        .assume_role_with_openid(
            "amp",
            &OpenIdClaims {
                subject: "sub-11".to_string(),
                preferred_username: "amp-user".to_string(),
                roles: set(&["role-b", "amp"]),
                custom: BTreeMap::new(),
            },
            Some("amp"),
        )
        .expect("amp role");
    assert!(service.allowed(&creds, "s3:DeleteObject", "multi-b/object"));
    assert!(!service.allowed(&creds, "s3:PutObject", "multi-b/object"));
}

#[test]
fn subtest_test_iam_ampwith_open_idwith_multiple_roles_server_suite_line_3423() {
    let mut service = base_sts_service();
    service.add_openid_provider(OpenIdProvider {
        name: "amp".to_string(),
        claim_name: "preferred_username".to_string(),
        claim_userinfo: false,
        role_policies: BTreeMap::from([
            ("role-b".to_string(), set(&["role-b"])),
            ("amp".to_string(), set(&["amp-role"])),
        ]),
    });
    let creds = service
        .assume_role_with_openid(
            "amp",
            &OpenIdClaims {
                subject: "sub-12".to_string(),
                preferred_username: "amp-user".to_string(),
                roles: set(&["role-b", "amp"]),
                custom: BTreeMap::new(),
            },
            None,
        )
        .expect("combined roles");
    assert!(service.allowed(&creds, "s3:DeleteObject", "multi-b/object"));
    assert!(service.allowed(&creds, "s3:PutObject", "multi-b/object"));
}
