use std::collections::BTreeMap;

use chrono::{Duration, TimeZone, Utc};
use minio_rust::cmd::{
    completed_restore_obj_status, is_restored_object_on_disk, new_file_info,
    ongoing_restore_obj_status, parse_restore_obj_status, validate_transition_tier, ObjectInfo,
    TierConfig, TierConfigMgr, AMZ_RESTORE_HEADER, ERR_INVALID_STORAGE_CLASS,
    ERR_RESTORE_HDR_MALFORMED, TRANSITION_STATUS_KEY,
};
use minio_rust::internal::bucket::lifecycle::parse_lifecycle_config;

pub const SOURCE_FILE: &str = "cmd/bucket-lifecycle_test.go";

#[test]
fn test_parse_restore_obj_status_line_31() {
    let tests = [
        (
            r#"ongoing-request="false", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT""#,
            Ok(completed_restore_obj_status(
                Utc.with_ymd_and_hms(2012, 12, 21, 0, 0, 0)
                    .single()
                    .expect("valid test timestamp"),
            )),
        ),
        (
            r#"ongoing-request="true""#,
            Ok(ongoing_restore_obj_status()),
        ),
        (
            r#"ongoing-request="true", expiry-date="Fri, 21 Dec 2012 00:00:00 GMT""#,
            Err(ERR_RESTORE_HDR_MALFORMED.to_string()),
        ),
        (
            r#"ongoing-request="false""#,
            Err(ERR_RESTORE_HDR_MALFORMED.to_string()),
        ),
    ];

    for (index, (header, expected)) in tests.into_iter().enumerate() {
        assert_eq!(
            parse_restore_obj_status(header),
            expected,
            "case {}",
            index + 1
        );
    }
}

#[test]
fn test_restore_obj_status_round_trip_line_79() {
    let tests = [
        ongoing_restore_obj_status(),
        completed_restore_obj_status(Utc::now()),
    ];

    for (index, status) in tests.into_iter().enumerate() {
        let actual = parse_restore_obj_status(&status.to_string()).expect("parse roundtrip");
        assert_eq!(actual.ongoing, status.ongoing, "case {}", index + 1);
        assert_eq!(
            actual
                .expiry
                .map(|expiry| expiry.format("%a, %d %b %Y %H:%M:%S GMT").to_string()),
            status
                .expiry
                .map(|expiry| expiry.format("%a, %d %b %Y %H:%M:%S GMT").to_string()),
            "case {}",
            index + 1
        );
    }
}

#[test]
fn test_restore_obj_on_disk_line_96() {
    let tests = [
        (ongoing_restore_obj_status(), false),
        (
            completed_restore_obj_status(Utc::now() - Duration::hours(1)),
            false,
        ),
        (
            completed_restore_obj_status(Utc::now() + Duration::hours(1)),
            true,
        ),
    ];

    for (index, (status, expected)) in tests.into_iter().enumerate() {
        assert_eq!(status.on_disk(), expected, "case {}", index + 1);
    }
}

#[test]
fn test_is_restored_object_on_disk_line_126() {
    let tests = [
        (
            restore_metadata(ongoing_restore_obj_status().to_string()),
            false,
        ),
        (
            restore_metadata(
                completed_restore_obj_status(Utc::now() + Duration::hours(1)).to_string(),
            ),
            true,
        ),
        (
            restore_metadata(
                completed_restore_obj_status(Utc::now() - Duration::hours(1)).to_string(),
            ),
            false,
        ),
    ];

    for (index, (meta, expected)) in tests.into_iter().enumerate() {
        assert_eq!(
            is_restored_object_on_disk(&meta),
            expected,
            "case {}",
            index + 1
        );
    }
}

#[test]
fn test_object_is_remote_line_161() {
    let mut file_info = new_file_info("object", 8, 8);
    file_info.erasure.index = 1;
    assert!(file_info.is_valid());

    let tests = [
        (
            restore_metadata(ongoing_restore_obj_status().to_string()),
            true,
        ),
        (
            restore_metadata(
                completed_restore_obj_status(Utc::now() + Duration::hours(1)).to_string(),
            ),
            false,
        ),
        (
            restore_metadata(
                completed_restore_obj_status(Utc::now() - Duration::hours(1)).to_string(),
            ),
            true,
        ),
        (BTreeMap::new(), true),
    ];

    for (index, (meta, expected)) in tests.into_iter().enumerate() {
        file_info.transition_status = "complete".to_string();
        file_info.metadata = Some(meta.clone());
        assert_eq!(file_info.is_remote(), expected, "case {}.a", index + 1);

        assert_eq!(
            remote_object_info(meta).is_remote(),
            expected,
            "case {}.b",
            index + 1
        );
    }

    file_info.transition_status.clear();
    file_info.metadata = None;
    assert!(!file_info.is_remote());
}

#[test]
fn test_validate_transition_tier_line_219() {
    let tests = [
        (
            r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><Transition><Days>1</Days><StorageClass>"NONEXISTENT"</StorageClass></Transition></Rule></LifecycleConfiguration>"#,
            Some(ERR_INVALID_STORAGE_CLASS.to_string()),
        ),
        (
            r#"<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule</ID><Prefix /><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>"#,
            None,
        ),
    ];

    let tier_config_mgr = TierConfigMgr {
        tiers: BTreeMap::from([(
            "WARM".to_string(),
            TierConfig {
                name: "WARM".to_string(),
                ..TierConfig::default()
            },
        )]),
    };

    for (index, (xml, expected_err)) in tests.into_iter().enumerate() {
        let lifecycle = parse_lifecycle_config(xml).expect("parse lifecycle");
        assert_eq!(
            validate_transition_tier(&lifecycle, &tier_config_mgr).err(),
            expected_err,
            "case {}",
            index + 1
        );
    }
}

fn restore_metadata(value: String) -> BTreeMap<String, String> {
    BTreeMap::from([(AMZ_RESTORE_HEADER.to_string(), value)])
}

fn remote_object_info(meta: BTreeMap<String, String>) -> ObjectInfo {
    let mut user_defined = meta;
    user_defined.insert(TRANSITION_STATUS_KEY.to_string(), "complete".to_string());
    ObjectInfo {
        user_defined,
        ..ObjectInfo::default()
    }
}
