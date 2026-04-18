use chrono::{DateTime, TimeZone, Utc};

use crate::cmd::{FileInfo, FileInfoVersions};
use crate::internal::bucket::lifecycle::{
    expected_expiry_time, Evaluator, Event, Lifecycle, ObjectOpts,
};

pub fn apply_newer_noncurrent_versions_limit(
    lifecycle: &Lifecycle,
    versions: &FileInfoVersions,
    now: DateTime<Utc>,
) -> Vec<FileInfo> {
    let ordered = versions.versions.clone().unwrap_or_default();
    if ordered.is_empty() {
        return Vec::new();
    }

    let limit =
        lifecycle.noncurrent_versions_expiration_limit(&file_info_to_object_opts(&ordered[0]));
    if limit.newer_noncurrent_versions <= 0 {
        return Vec::new();
    }

    let keep_limit = limit.newer_noncurrent_versions as usize;
    let mut kept_noncurrent = 0usize;
    let mut expired = Vec::new();

    for file in ordered.into_iter().filter(|file| !file.is_latest) {
        if kept_noncurrent < keep_limit {
            kept_noncurrent += 1;
            continue;
        }

        let Some(successor) = timestamp_from_unix(file.successor_mod_time)
            .or_else(|| timestamp_from_unix(file.mod_time))
        else {
            continue;
        };

        let due = expected_expiry_time(successor, limit.noncurrent_days);
        if now > due {
            expired.push(file);
        }
    }

    expired
}

pub fn eval_action_from_lifecycle(
    lifecycle: &Lifecycle,
    versions: &FileInfoVersions,
    now: DateTime<Utc>,
) -> Vec<Event> {
    let objects = versions
        .versions
        .as_ref()
        .map(|versions| {
            versions
                .iter()
                .map(file_info_to_object_opts)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if objects.is_empty() {
        return Vec::new();
    }

    Evaluator::new(lifecycle.clone()).eval(&objects, now)
}

fn file_info_to_object_opts(file: &FileInfo) -> ObjectOpts {
    ObjectOpts {
        name: file.name.clone(),
        mod_time: timestamp_from_unix(file.mod_time),
        size: file.size,
        version_id: file.version_id.clone(),
        is_latest: file.is_latest,
        delete_marker: file.deleted,
        num_versions: file.num_versions.max(0) as usize,
        successor_mod_time: timestamp_from_unix(file.successor_mod_time),
        transition_status: file.transition_status.clone(),
        ..ObjectOpts::default()
    }
}

fn timestamp_from_unix(value: i64) -> Option<DateTime<Utc>> {
    if value <= 0 {
        return None;
    }
    Utc.timestamp_opt(value, 0).single()
}
