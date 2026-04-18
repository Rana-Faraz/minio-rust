use chrono::{TimeZone, Utc};
use minio_rust::cmd::{
    apply_newer_noncurrent_versions_limit, eval_action_from_lifecycle, FileInfo, FileInfoVersions,
};
use minio_rust::internal::bucket::lifecycle::{parse_lifecycle_config, Action};

pub const SOURCE_FILE: &str = "cmd/data-scanner_test.go";

#[test]
fn test_apply_newer_noncurrent_versions_limit_line_38() {
    run_apply_newer_noncurrent_versions_limit_cases();
}

#[test]
fn subtest_test_apply_newer_noncurrent_versions_limit_fmt_sprintf_test_apply_newer_noncurrent_versions_limit_d_line_255(
) {
    run_apply_newer_noncurrent_versions_limit_cases();
}

#[test]
fn test_eval_action_from_lifecycle_line_313() {
    run_eval_action_from_lifecycle_cases();
}

#[test]
fn subtest_test_eval_action_from_lifecycle_fmt_sprintf_test_eval_action_d_line_400() {
    run_eval_action_from_lifecycle_cases();
}

fn run_apply_newer_noncurrent_versions_limit_cases() {
    let now = dt(2024, 1, 12);
    let tests = [
        (
            "deletes only versions past the retention window",
            parse_lifecycle_config(
                r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"#,
            )
            .expect("parse lifecycle"),
            file_info_versions("object", &[
                ("v5", true, ts(2024, 1, 10), 0),
                ("v4", false, ts(2024, 1, 9), ts(2024, 1, 10)),
                ("v3", false, ts(2024, 1, 8), ts(2024, 1, 9)),
                ("v2", false, ts(2024, 1, 7), ts(2024, 1, 8)),
                ("v1", false, ts(2024, 1, 6), ts(2024, 1, 7)),
            ]),
            vec!["v2", "v1"],
        ),
        (
            "keeps objects when the newer-version count is within the limit",
            parse_lifecycle_config(
                r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"#,
            )
            .expect("parse lifecycle"),
            file_info_versions("object", &[
                ("v3", true, ts(2024, 1, 10), 0),
                ("v2", false, ts(2024, 1, 9), ts(2024, 1, 10)),
                ("v1", false, ts(2024, 1, 8), ts(2024, 1, 9)),
            ]),
            Vec::<&str>::new(),
        ),
        (
            "respects the noncurrent-days gate before deleting",
            parse_lifecycle_config(
                r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>10</NoncurrentDays><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"#,
            )
            .expect("parse lifecycle"),
            file_info_versions("object", &[
                ("v3", true, ts(2024, 1, 10), 0),
                ("v2", false, ts(2024, 1, 9), ts(2024, 1, 10)),
                ("v1", false, ts(2024, 1, 8), ts(2024, 1, 9)),
            ]),
            Vec::<&str>::new(),
        ),
    ];

    for (index, (_name, lifecycle, versions, expected)) in tests.into_iter().enumerate() {
        let actual = apply_newer_noncurrent_versions_limit(&lifecycle, &versions, now)
            .into_iter()
            .map(|file| file.version_id)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "case {}", index + 1);
    }
}

fn run_eval_action_from_lifecycle_cases() {
    let now = dt(2024, 1, 10);
    let tests = [
        (
            "expires an old current object",
            parse_lifecycle_config(
                r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#,
            )
            .expect("parse lifecycle"),
            file_info_versions("object", &[("v1", true, ts(2024, 1, 7), 0)]),
            vec![Action::Delete],
        ),
        (
            "deletes only the oldest noncurrent object once the retention limit is exceeded",
            parse_lifecycle_config(
                r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><NoncurrentVersionExpiration><NoncurrentDays>0</NoncurrentDays><NewerNoncurrentVersions>1</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>"#,
            )
            .expect("parse lifecycle"),
            file_info_versions("object", &[
                ("v3", true, ts(2024, 1, 10), 0),
                ("v2", false, ts(2024, 1, 9), ts(2024, 1, 10)),
                ("v1", false, ts(2024, 1, 7), ts(2024, 1, 9)),
            ]),
            vec![Action::None, Action::None, Action::DeleteVersion],
        ),
        (
            "ignores disabled lifecycle rules",
            parse_lifecycle_config(
                r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Disabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#,
            )
            .expect("parse lifecycle"),
            file_info_versions("object", &[("v1", true, ts(2024, 1, 7), 0)]),
            vec![Action::None],
        ),
    ];

    for (index, (_name, lifecycle, versions, expected)) in tests.into_iter().enumerate() {
        let actual = eval_action_from_lifecycle(&lifecycle, &versions, now)
            .into_iter()
            .map(|event| event.action)
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "case {}", index + 1);
    }
}

fn file_info_versions(name: &str, versions: &[(&str, bool, i64, i64)]) -> FileInfoVersions {
    let num_versions = versions.len() as i32;
    FileInfoVersions {
        volume: "bucket".to_string(),
        name: name.to_string(),
        latest_mod_time: versions
            .first()
            .map(|(_, _, mod_time, _)| *mod_time)
            .unwrap_or(0),
        versions: Some(
            versions
                .iter()
                .map(
                    |(version_id, is_latest, mod_time, successor_mod_time)| FileInfo {
                        volume: "bucket".to_string(),
                        name: name.to_string(),
                        version_id: (*version_id).to_string(),
                        is_latest: *is_latest,
                        mod_time: *mod_time,
                        successor_mod_time: *successor_mod_time,
                        num_versions,
                        size: 1,
                        versioned: true,
                        ..FileInfo::default()
                    },
                )
                .collect(),
        ),
        free_versions: None,
    }
}

fn ts(year: i32, month: u32, day: u32) -> i64 {
    dt(year, month, day).timestamp()
}

fn dt(year: i32, month: u32, day: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .expect("valid test timestamp")
}
