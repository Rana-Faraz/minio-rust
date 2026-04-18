use minio_rust::cmd::{
    bucket_access_policy_to_policy, policy_sys_is_allowed, policy_to_bucket_access_policy,
    BucketAccessPolicy, Policy, PolicyEffect, PolicyStatement,
};

pub const SOURCE_FILE: &str = "cmd/policy_test.go";

#[test]
fn test_policy_sys_is_allowed_line_30() {
    let policy = Policy {
        version: "2012-10-17".to_string(),
        statements: vec![
            PolicyStatement {
                effect: PolicyEffect::Allow,
                actions: vec!["admin:*".to_string()],
                resources: vec!["arn:minio:admin:::*".to_string()],
            },
            PolicyStatement {
                effect: PolicyEffect::Deny,
                actions: vec!["admin:DeleteUser".to_string()],
                resources: vec!["arn:minio:admin:::*".to_string()],
            },
        ],
    };

    assert!(policy_sys_is_allowed(
        &policy,
        "admin:ListUsers",
        "arn:minio:admin:::cluster"
    ));
    assert!(!policy_sys_is_allowed(
        &policy,
        "admin:DeleteUser",
        "arn:minio:admin:::cluster"
    ));
}

#[test]
fn test_policy_to_bucket_access_policy_line_163() {
    let policy = Policy {
        version: "2012-10-17".to_string(),
        statements: vec![PolicyStatement {
            effect: PolicyEffect::Allow,
            actions: vec![
                "s3:GetObject".to_string(),
                "s3:PutObject".to_string(),
                "s3:DeleteObject".to_string(),
            ],
            resources: vec!["arn:aws:s3:::photos/user/*".to_string()],
        }],
    };

    let bucket_policy = policy_to_bucket_access_policy(&policy).expect("convert policy");
    assert_eq!(bucket_policy.bucket, "photos");
    assert_eq!(bucket_policy.prefix, "user/");
    assert!(bucket_policy.allow_get);
    assert!(bucket_policy.allow_put);
    assert!(bucket_policy.allow_delete);
}

#[test]
fn test_bucket_access_policy_to_policy_line_238() {
    let bucket_policy = BucketAccessPolicy {
        bucket: "docs".to_string(),
        prefix: "public/".to_string(),
        allow_get: true,
        allow_put: false,
        allow_delete: false,
    };

    let policy = bucket_access_policy_to_policy(&bucket_policy);
    assert_eq!(policy.version, "2012-10-17");
    assert_eq!(policy.statements.len(), 1);
    assert_eq!(policy.statements[0].effect, PolicyEffect::Allow);
    assert_eq!(
        policy.statements[0].actions,
        vec!["s3:GetObject".to_string()]
    );
    assert_eq!(
        policy.statements[0].resources,
        vec!["arn:aws:s3:::docs/public/*".to_string()]
    );
}
