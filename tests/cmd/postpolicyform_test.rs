use minio_rust::cmd::{parse_post_policy_form, PostPolicyCondition};

pub const SOURCE_FILE: &str = "cmd/postpolicyform_test.go";

#[test]
fn test_parse_post_policy_form_line_31() {
    let form = parse_post_policy_form(
        r#"{
            "expiration":"2026-12-30T12:00:00.000Z",
            "conditions":[
                {"bucket":"photos"},
                ["starts-with", "$key", "user/"],
                ["content-length-range", 1, 1048576]
            ]
        }"#,
    )
    .expect("parse policy");

    assert_eq!(form.expiration, "2026-12-30T12:00:00.000Z");
    assert_eq!(form.bucket(), Some("photos"));
    assert_eq!(form.conditions.len(), 3);
}

#[test]
fn subtest_test_parse_post_policy_form_line_68() {
    let err = parse_post_policy_form(r#"{"conditions":[]}"#).expect_err("missing expiration");
    assert!(err.contains("missing expiration"));
}

#[test]
fn test_post_policy_form_line_103() {
    let form = parse_post_policy_form(
        r#"{
            "expiration":"2026-12-30T12:00:00.000Z",
            "conditions":[
                ["eq", "$bucket", "archive"],
                ["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"]
            ]
        }"#,
    )
    .expect("parse policy");

    assert_eq!(form.bucket(), Some("archive"));
    assert!(form.conditions.iter().any(|condition| matches!(
        condition,
        PostPolicyCondition::Equals { field, value }
            if field == "x-amz-algorithm" && value == "AWS4-HMAC-SHA256"
    )));
}

#[test]
fn subtest_test_post_policy_form_tt_name_line_267() {
    let err = parse_post_policy_form(
        r#"{
            "expiration":"2026-12-30T12:00:00.000Z",
            "conditions":[["unknown-op", "$key", "value"]]
        }"#,
    )
    .expect_err("unknown operator must fail");
    assert!(err.contains("unsupported post policy operator"));
}
