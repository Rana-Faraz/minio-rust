use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::mpsc::channel;

use chrono::{Duration, Utc};

use minio_rust::cmd::{
    completed_restore_obj, merge_entry_channels, merge_xlv2_versions, ongoing_restore_obj,
    read_xl_meta_no_data, xl_meta_v2_trim_data, ErasureInfo, FileInfo, MetaCacheEntry,
    ObjectPartInfo, XlMetaV2, XlMetaV2Object, XlMetaV2StoredVersion, AMZ_RESTORE_HEADER,
    HEALING_KEY, REPLICATION_TIMESTAMP_KEY, REPLICA_TIMESTAMP_KEY, TRANSITION_STATUS_KEY,
    XL_META_DELETE_MARKER_TYPE, XL_META_OBJECT_TYPE,
};

pub const SOURCE_FILE: &str = "cmd/xl-storage-format-v2_test.go";

fn build_file_info(
    version_id: &str,
    data_dir: &str,
    data: Option<&[u8]>,
    mod_time: i64,
) -> FileInfo {
    FileInfo {
        volume: "volume".to_string(),
        name: "object-name".to_string(),
        version_id: version_id.to_string(),
        is_latest: true,
        deleted: false,
        transition_status: String::new(),
        transitioned_obj_name: String::new(),
        transition_tier: String::new(),
        transition_version_id: String::new(),
        expire_restored: false,
        data_dir: data_dir.to_string(),
        xlv1: false,
        mod_time,
        size: if data.is_some() { 0 } else { 42 },
        mode: 0,
        written_by_version: 1,
        metadata: None,
        parts: None,
        erasure: ErasureInfo {
            algorithm: "reedsolomon".to_string(),
            data_blocks: 4,
            parity_blocks: 2,
            block_size: 10_000,
            index: 1,
            distribution: Some(vec![1, 2, 3, 4, 5, 6]),
        },
        mark_deleted: false,
        replication_state: Default::default(),
        data: data.map(|bytes| bytes.to_vec()),
        num_versions: 1,
        successor_mod_time: 0,
        fresh: false,
        idx: 0,
        checksum: None,
        versioned: true,
    }
}

fn sample_xl_meta() -> XlMetaV2 {
    let mut xl = XlMetaV2::default();
    for idx in 0..5 {
        xl.add_version(build_file_info(
            &format!("version-{idx}"),
            &format!("dir-{idx}"),
            None,
            1_700_000_000_000_000_000 + idx as i64,
        ))
        .expect("add sample version");
    }
    xl
}

fn sample_xl_meta_with_many_parts() -> XlMetaV2 {
    let mut xl = XlMetaV2::default();
    let mut file_info = build_file_info(
        "many-parts-version",
        "many-parts-dir",
        None,
        1_700_000_000_123_456_789,
    );
    file_info.parts = Some(
        (1..=128)
            .map(|number| ObjectPartInfo {
                number,
                size: 1024 * i64::from(number),
                etag: format!("etag-{number}"),
                actual_size: 1000 * i64::from(number),
            })
            .collect(),
    );
    file_info.size = 128 * 1024;
    xl.add_version(file_info).expect("add many-parts version");
    xl
}

fn make_merge_version(version_id: &str, mod_time: i64) -> XlMetaV2StoredVersion {
    let mut xl = XlMetaV2::default();
    xl.add_version(build_file_info(
        version_id,
        &format!("dir-{version_id}"),
        None,
        mod_time,
    ))
    .expect("add merge version");
    xl.versions.pop().expect("stored version")
}

fn make_delete_merge_version(version_id: &str, mod_time: i64) -> XlMetaV2StoredVersion {
    let mut version = make_merge_version(version_id, mod_time);
    version.header.type_id = XL_META_DELETE_MARKER_TYPE;
    version.version.type_id = XL_META_DELETE_MARKER_TYPE;
    version
}

#[test]
fn test_read_xlmeta_no_data_line_43() {
    let err = read_xl_meta_no_data(Cursor::new(b"not-a-valid-xl-meta"), 19)
        .expect_err("corrupt metadata should fail");
    assert!(
        !err.is_empty(),
        "read_xl_meta_no_data should surface a decode error"
    );
}

#[test]
fn test_xlv2_format_data_line_66() {
    let data = b"some object data";
    let data2 = b"some other object data";

    let mut xl = XlMetaV2::default();
    xl.add_version(build_file_info(
        "756100c6-b393-4981-928a-d49bbc164741",
        "bffea160-ca7f-465f-98bc-9b4f1c3ba1ef",
        Some(data),
        1_700_000_000_000_000_000,
    ))
    .expect("add first version");
    xl.add_version(build_file_info(
        "11111111-b393-4981-928a-d49bbc164741",
        "cffee160-ca7f-465f-98bc-9b4f1c3ba1ef",
        Some(data2),
        1_700_000_000_000_000_001,
    ))
    .expect("add second version");

    let serialized = xl.append_to(None).expect("serialize xl meta");
    let mut xl2 = XlMetaV2::default();
    xl2.load(&serialized).expect("load roundtrip");

    let list = xl2.data.list().expect("list inline data");
    assert_eq!(list.len(), 2);
    assert_eq!(
        xl2.data.find("756100c6-b393-4981-928a-d49bbc164741"),
        Some(data.as_slice())
    );
    assert_eq!(
        xl2.data.find("11111111-b393-4981-928a-d49bbc164741"),
        Some(data2.as_slice())
    );

    xl2.data.remove("11111111-b393-4981-928a-d49bbc164741");
    xl2.data.validate().expect("validate after remove");
    assert_eq!(xl2.data.find("11111111-b393-4981-928a-d49bbc164741"), None);
    assert_eq!(xl2.data.entries(), 1);

    xl2.data
        .replace("11111111-b393-4981-928a-d49bbc164741", data2.to_vec());
    xl2.data.validate().expect("validate after re-add");
    assert_eq!(xl2.data.entries(), 2);

    xl2.data
        .replace("756100c6-b393-4981-928a-d49bbc164741", data2.to_vec());
    xl2.data.validate().expect("validate after replace");
    assert_eq!(
        xl2.data.find("756100c6-b393-4981-928a-d49bbc164741"),
        Some(data2.as_slice())
    );

    assert!(
        xl2.data
            .rename("756100c6-b393-4981-928a-d49bbc164741", "new-key"),
        "rename should find the original key"
    );
    xl2.data.validate().expect("validate after rename");
    assert_eq!(xl2.data.find("new-key"), Some(data2.as_slice()));
    assert_eq!(xl2.data.entries(), 2);

    let trimmed = xl_meta_v2_trim_data(&serialized);
    let mut trimmed_xl = XlMetaV2::default();
    trimmed_xl.load(&trimmed).expect("load trimmed metadata");
    assert_eq!(trimmed_xl.data.entries(), 0);

    let mut corrupted = trimmed.clone();
    corrupted.truncate(corrupted.len() / 2);
    assert!(
        trimmed_xl.load(&corrupted).is_err(),
        "truncated metadata should fail to load"
    );
}

#[test]
fn test_uses_data_dir_line_192() {
    let version_id = b"version-id".to_vec();
    let data_dir = b"data-dir".to_vec();

    let transitioned = BTreeMap::from([(TRANSITION_STATUS_KEY.to_string(), b"complete".to_vec())]);
    let to_be_restored = BTreeMap::from([(AMZ_RESTORE_HEADER.to_string(), ongoing_restore_obj())]);
    let restored = BTreeMap::from([(
        AMZ_RESTORE_HEADER.to_string(),
        completed_restore_obj(Utc::now() + Duration::hours(1)),
    )]);
    let restored_expired = BTreeMap::from([(
        AMZ_RESTORE_HEADER.to_string(),
        completed_restore_obj(Utc::now() - Duration::hours(1)),
    )]);

    let cases = [
        (
            XlMetaV2Object {
                version_id: Some(version_id.clone()),
                data_dir: Some(data_dir.clone()),
                meta_sys: Some(transitioned.clone()),
                ..Default::default()
            },
            false,
        ),
        (
            XlMetaV2Object {
                version_id: Some(version_id.clone()),
                data_dir: Some(data_dir.clone()),
                meta_sys: Some(transitioned.clone()),
                meta_user: Some(to_be_restored),
                ..Default::default()
            },
            false,
        ),
        (
            XlMetaV2Object {
                version_id: Some(version_id.clone()),
                data_dir: Some(data_dir.clone()),
                meta_sys: Some(transitioned.clone()),
                meta_user: Some(restored),
                ..Default::default()
            },
            true,
        ),
        (
            XlMetaV2Object {
                version_id: Some(version_id.clone()),
                data_dir: Some(data_dir.clone()),
                meta_sys: Some(transitioned),
                meta_user: Some(restored_expired),
                ..Default::default()
            },
            false,
        ),
        (
            XlMetaV2Object {
                version_id: Some(version_id),
                data_dir: Some(data_dir),
                ..Default::default()
            },
            true,
        ),
    ];

    for (index, (object, expected)) in cases.into_iter().enumerate() {
        assert_eq!(
            object.uses_data_dir(),
            expected,
            "case {} should match expected UsesDataDir behavior",
            index + 1
        );
    }
}

#[test]
fn test_delete_version_with_shared_data_dir_line_261() {
    let data = b"some object data";
    let data2 = b"some other object data";

    let d0 = "dir-0";
    let d1 = "dir-1";
    let d2 = "dir-2";

    let future_restore = completed_restore_obj(Utc::now() + Duration::hours(10));

    struct Case<'a> {
        version_id: &'a str,
        data_dir: &'a str,
        data: Option<&'a [u8]>,
        expected_shares: usize,
        transition_status: &'a str,
        restore_obj_status: Option<String>,
        expected_delete_data_dir: &'a str,
    }

    let cases = [
        Case {
            version_id: "version-inline-1",
            data_dir: d0,
            data: Some(data),
            expected_shares: 0,
            transition_status: "",
            restore_obj_status: None,
            expected_delete_data_dir: "",
        },
        Case {
            version_id: "version-inline-2",
            data_dir: d1,
            data: Some(data2),
            expected_shares: 0,
            transition_status: "",
            restore_obj_status: None,
            expected_delete_data_dir: "",
        },
        Case {
            version_id: "version-transitioned",
            data_dir: d2,
            data: None,
            expected_shares: 3,
            transition_status: "complete",
            restore_obj_status: None,
            expected_delete_data_dir: "",
        },
        Case {
            version_id: "version-ongoing-restore",
            data_dir: d2,
            data: None,
            expected_shares: 3,
            transition_status: "complete",
            restore_obj_status: Some(ongoing_restore_obj()),
            expected_delete_data_dir: "",
        },
        Case {
            version_id: "version-restored",
            data_dir: d2,
            data: None,
            expected_shares: 2,
            transition_status: "complete",
            restore_obj_status: Some(future_restore.clone()),
            expected_delete_data_dir: "",
        },
        Case {
            version_id: "version-disk-1",
            data_dir: d2,
            data: None,
            expected_shares: 2,
            transition_status: "",
            restore_obj_status: None,
            expected_delete_data_dir: "",
        },
        Case {
            version_id: "version-disk-2",
            data_dir: d2,
            data: None,
            expected_shares: 2,
            transition_status: "",
            restore_obj_status: None,
            expected_delete_data_dir: d2,
        },
    ];

    let mut xl = XlMetaV2::default();
    let mut file_infos = Vec::new();
    for (idx, case) in cases.iter().enumerate() {
        let mut file_info = build_file_info(
            case.version_id,
            case.data_dir,
            case.data,
            1_700_000_000_000_000_000 + idx as i64,
        );
        file_info.transition_status = case.transition_status.to_string();
        if let Some(restore) = &case.restore_obj_status {
            file_info.metadata = Some(BTreeMap::from([(
                AMZ_RESTORE_HEADER.to_string(),
                restore.clone(),
            )]));
        }
        xl.add_version(file_info.clone()).expect("add version");
        file_infos.push(file_info);
    }

    for case in &cases {
        let (_, version) = xl
            .find_version(case.version_id)
            .expect("version should exist");
        assert_eq!(
            xl.shared_data_dir_count(case.version_id, &version.data_dir()),
            case.expected_shares,
            "shared data-dir count should match for {}",
            case.version_id
        );
    }

    for (idx, case) in cases.iter().enumerate().skip(4) {
        let data_dir = xl
            .delete_version(&file_infos[idx])
            .expect("delete version should succeed");
        assert_eq!(
            data_dir, case.expected_delete_data_dir,
            "delete should only release the data dir when the last user is removed"
        );
    }
}

#[test]
fn benchmark_xl_meta_v2_shallow_load_line_456() {
    let bytes = sample_xl_meta()
        .append_to(None)
        .expect("serialize sample xl");
    for _ in 0..100 {
        let mut xl = XlMetaV2::default();
        xl.load(&bytes).expect("load sample xl");
        assert_eq!(xl.versions.len(), 5);
    }
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_load_legacy_line_467() {
    let bytes = sample_xl_meta()
        .append_to(None)
        .expect("serialize sample xl");
    for _ in 0..50 {
        let mut xl = XlMetaV2::default();
        xl.load(&bytes).expect("legacy-style load smoke");
    }
}

#[test]
fn subbenchmark_benchmark_xl_meta_v2_shallow_load_indexed_line_480() {
    let xl = sample_xl_meta();
    let bytes = xl.append_to(None).expect("serialize indexed sample");
    for _ in 0..50 {
        let mut roundtrip = XlMetaV2::default();
        roundtrip.load(&bytes).expect("indexed-style load smoke");
    }
}

#[test]
fn test_xl_meta_v2_shallow_load_line_502() {
    let bytes = sample_xl_meta()
        .append_to(None)
        .expect("serialize sample xl");
    let mut xl = XlMetaV2::default();
    xl.load(&bytes).expect("load sample xl");
    assert_eq!(xl.versions.len(), 5);

    xl.sort_by_mod_time();
    assert!(
        xl.versions
            .windows(2)
            .all(|pair| pair[0].header.mod_time >= pair[1].header.mod_time),
        "versions should be sorted descending by mod time"
    );

    for idx in 0..xl.versions.len() {
        let header = xl.versions[idx].header.clone();
        let version = xl.get_idx(idx).expect("version by index");
        assert_eq!(version.header(), header, "header should match loaded entry");
    }
}

#[test]
fn subtest_test_xl_meta_v2_shallow_load_load_legacy_line_536() {
    let bytes = sample_xl_meta()
        .append_to(None)
        .expect("serialize sample xl");
    let mut xl = XlMetaV2::default();
    xl.load(&bytes).expect("load legacy bytes");
    assert_eq!(xl.versions.len(), 5);
}

#[test]
fn subtest_test_xl_meta_v2_shallow_load_roundtrip_line_544() {
    let bytes = sample_xl_meta()
        .append_to(None)
        .expect("serialize sample xl");
    let mut xl = XlMetaV2::default();
    xl.load(&bytes).expect("load sample xl");
    let roundtrip = xl.append_to(None).expect("roundtrip serialize");
    let mut reloaded = XlMetaV2::default();
    reloaded.load(&roundtrip).expect("reload roundtrip bytes");
    assert_eq!(reloaded, xl);
}

#[test]
fn subtest_test_xl_meta_v2_shallow_load_write_timestamp_line_561() {
    let mut xl = sample_xl_meta();
    xl.add_version(build_file_info(
        "timestamp-version",
        "timestamp-dir",
        None,
        1_700_000_000_999_999_999,
    ))
    .expect("add timestamp version");

    let bytes = xl.append_to(None).expect("serialize with timestamp");
    let mut reloaded = XlMetaV2::default();
    reloaded.load(&bytes).expect("reload timestamp sample");
    let (_, version) = reloaded
        .find_version("timestamp-version")
        .expect("timestamp version should exist");
    assert_eq!(version.header.mod_time, 1_700_000_000_999_999_999);
}

#[test]
fn subtest_test_xl_meta_v2_shallow_load_comp_index_line_587() {
    let bytes = sample_xl_meta()
        .append_to(None)
        .expect("serialize sample xl");
    let mut xl = XlMetaV2::default();
    xl.load(&bytes).expect("load sample xl");
    for version in &xl.versions {
        assert!(
            version
                .header
                .signature
                .as_ref()
                .is_some_and(|sig| sig.len() == 4),
            "all stored versions should carry a stable 4-byte signature"
        );
    }
}

#[test]
fn test_xl_meta_v2_shallow_load_time_stamp_line_610() {
    let mut xl = XlMetaV2::default();
    let mut file_info = build_file_info(
        "timestamp-version",
        "timestamp-dir",
        Some(b"inline"),
        1_700_000_000_123_456_789,
    );
    file_info.metadata = Some(BTreeMap::from([
        (
            REPLICATION_TIMESTAMP_KEY.to_string(),
            "2022-10-27T15:40:53.195813291+08:00".to_string(),
        ),
        (
            REPLICA_TIMESTAMP_KEY.to_string(),
            "2022-10-27T15:40:53.195813291+08:00".to_string(),
        ),
    ]));
    xl.add_version(file_info).expect("add timestamp version");

    let bytes = xl.append_to(None).expect("serialize timestamp metadata");
    let mut reloaded = XlMetaV2::default();
    reloaded.load(&bytes).expect("reload timestamp metadata");
    let file_info = reloaded
        .to_file_info("volume", "object-name", "timestamp-version", false, true)
        .expect("to_file_info");

    let metadata = file_info.metadata.expect("normalized metadata");
    assert_eq!(
        metadata.get(REPLICATION_TIMESTAMP_KEY).map(String::as_str),
        Some("2022-10-27T07:40:53.195813291Z")
    );
    assert_eq!(
        metadata.get(REPLICA_TIMESTAMP_KEY).map(String::as_str),
        Some("2022-10-27T07:40:53.195813291Z")
    );
}

#[test]
fn benchmark_merge_xlv2_versions_line_407() {
    let sets = vec![
        vec![make_merge_version("v1", 30), make_merge_version("v2", 20)],
        vec![make_merge_version("v1", 30), make_merge_version("v2", 20)],
        vec![make_merge_version("v1", 30), make_merge_version("v3", 10)],
    ];
    for _ in 0..100 {
        let merged = merge_xlv2_versions(2, false, 0, &sets);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].get_version_id(), "v1");
        assert_eq!(merged[1].get_version_id(), "v2");
    }
}

#[test]
fn subbenchmark_benchmark_merge_xlv2_versions_requested_none_line_428() {
    let sets = vec![
        vec![make_merge_version("v1", 50)],
        vec![make_merge_version("v1", 50)],
    ];
    for _ in 0..50 {
        assert_eq!(merge_xlv2_versions(2, false, 0, &sets).len(), 1);
    }
}

#[test]
fn subbenchmark_benchmark_merge_xlv2_versions_requested_v1_line_437() {
    let sets = vec![
        vec![make_merge_version("v1", 50)],
        vec![make_merge_version("v1", 50)],
    ];
    for _ in 0..50 {
        assert_eq!(merge_xlv2_versions(2, false, 1, &sets).len(), 1);
    }
}

#[test]
fn subbenchmark_benchmark_merge_xlv2_versions_requested_v2_line_446() {
    let sets = vec![
        vec![make_merge_version("v1", 50), make_merge_version("v2", 40)],
        vec![make_merge_version("v1", 50), make_merge_version("v2", 40)],
    ];
    for _ in 0..50 {
        assert_eq!(merge_xlv2_versions(2, false, 2, &sets).len(), 2);
    }
}

#[test]
fn test_merge_xlv2_versions_line_643() {
    let base = make_merge_version("v1", 100);
    let mut mutated_sig = base.clone();
    mutated_sig.header.signature = Some(vec![9, 9, 9, 9]);
    let mut mutated_modtime = base.clone();
    mutated_modtime.header.mod_time += 1;

    let non_strict = merge_xlv2_versions(
        2,
        false,
        0,
        &[
            vec![base.clone()],
            vec![mutated_sig.clone()],
            vec![base.clone()],
        ],
    );
    assert_eq!(non_strict.len(), 1);
    assert_eq!(non_strict[0].get_version_id(), "v1");

    let strict = merge_xlv2_versions(
        2,
        true,
        0,
        &[vec![base.clone()], vec![mutated_sig], vec![base.clone()]],
    );
    assert_eq!(strict.len(), 1);

    let strict_modtime = merge_xlv2_versions(
        2,
        true,
        0,
        &[vec![base.clone()], vec![mutated_modtime], vec![base]],
    );
    assert_eq!(strict_modtime.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_non_strict_q_d_line_685() {
    let a = make_merge_version("va", 30);
    let b = make_merge_version("vb", 20);
    let merged = merge_xlv2_versions(1, false, 0, &[vec![a.clone(), b.clone()]]);
    assert_eq!(merged.len(), 2);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_strict_q_d_line_697() {
    let a = make_merge_version("va", 30);
    let merged = merge_xlv2_versions(2, true, 0, &[vec![a.clone()], vec![a.clone()]]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_signature_q_d_line_709() {
    let a = make_merge_version("va", 30);
    let mut b = a.clone();
    b.header.signature = Some(vec![1, 2, 3, 4]);
    let merged = merge_xlv2_versions(2, false, 0, &[vec![a.clone()], vec![b], vec![a]]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_modtime_q_d_line_731() {
    let a = make_merge_version("va", 30);
    let mut b = a.clone();
    b.header.mod_time += 5;
    let merged = merge_xlv2_versions(2, true, 0, &[vec![a.clone()], vec![b], vec![a]]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_flags_q_d_line_757() {
    let a = make_merge_version("va", 30);
    let mut b = a.clone();
    b.header.flags = 1;
    let merged = merge_xlv2_versions(2, false, 0, &[vec![a.clone()], vec![b], vec![a]]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_versionid_q_d_line_779() {
    let a = make_merge_version("va", 30);
    let b = make_merge_version("vb", 30);
    let merged = merge_xlv2_versions(2, false, 0, &[vec![a.clone()], vec![b.clone()], vec![a]]);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].get_version_id(), "va");
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_strict_signature_q_d_line_805() {
    let a = make_merge_version("va", 30);
    let mut b = a.clone();
    b.header.signature = Some(vec![1, 2, 3, 4]);
    let merged = merge_xlv2_versions(2, true, 0, &[vec![a.clone()], vec![b], vec![a]]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_strict_modtime_q_d_line_831() {
    let a = make_merge_version("va", 30);
    let mut b = a.clone();
    b.header.mod_time += 1;
    let merged = merge_xlv2_versions(2, true, 0, &[vec![a.clone()], vec![b], vec![a]]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_strict_flags_q_d_line_857() {
    let a = make_merge_version("va", 30);
    let mut b = a.clone();
    b.header.flags = 1;
    let merged = merge_xlv2_versions(2, true, 0, &[vec![a.clone()], vec![b], vec![a]]);
    assert_eq!(merged.len(), 1);
}

#[test]
fn subtest_test_merge_xlv2_versions_fmt_sprintf_strict_type_q_d_line_883() {
    let a = make_merge_version("va", 30);
    let mut b = a.clone();
    b.header.type_id = XL_META_DELETE_MARKER_TYPE;
    let merged = merge_xlv2_versions(2, true, 0, &[vec![a.clone()], vec![b], vec![a]]);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].header.type_id, XL_META_OBJECT_TYPE);
}

#[test]
fn test_merge_xlv2_versions2_line_912() {
    let v_del = make_delete_merge_version("v-del", 1500);
    let v_obj = make_merge_version("v-obj", 1000);
    let cases = [
        (
            "obj-on-one",
            vec![
                vec![v_del.clone(), v_obj.clone()],
                vec![v_del.clone()],
                vec![v_del.clone()],
            ],
            vec!["v-del"],
        ),
        (
            "obj-on-two",
            vec![
                vec![v_del.clone(), v_obj.clone()],
                vec![v_del.clone(), v_obj.clone()],
                vec![v_del.clone()],
            ],
            vec!["v-del", "v-obj"],
        ),
        (
            "del-on-one",
            vec![
                vec![v_del.clone(), v_obj.clone()],
                vec![v_obj.clone()],
                vec![v_obj.clone()],
            ],
            vec!["v-obj"],
        ),
    ];
    for (_, input, want_ids) in cases {
        let got = merge_xlv2_versions(2, true, 0, &input);
        let ids: Vec<_> = got
            .iter()
            .map(XlMetaV2StoredVersion::get_version_id)
            .collect();
        assert_eq!(ids, want_ids);
    }
}

#[test]
fn subtest_test_merge_xlv2_versions2_test_name_line_1019() {
    let v_del = make_delete_merge_version("v-del", 1500);
    let v_obj = make_merge_version("v-obj", 1000);
    let got = merge_xlv2_versions(
        2,
        true,
        0,
        &[
            vec![v_del.clone(), v_obj.clone()],
            vec![v_del.clone(), v_obj.clone()],
            vec![v_obj],
        ],
    );
    assert_eq!(got.len(), 2);
}

#[test]
fn subtest_test_merge_xlv2_versions2_fmt_sprint_i_line_1022() {
    let v_del = make_delete_merge_version("v-del", 1500);
    let v_obj = make_merge_version("v-obj", 1000);
    for _ in 0..20 {
        let got = merge_xlv2_versions(
            2,
            true,
            0,
            &[
                vec![v_del.clone(), v_obj.clone()],
                vec![v_del.clone(), v_obj.clone()],
                vec![v_obj.clone()],
            ],
        );
        assert_eq!(got.len(), 2);
    }
}

#[test]
fn test_merge_entry_channels_line_1037() {
    let mut xl1 = XlMetaV2::default();
    xl1.add_version(build_file_info("v1", "d1", None, 30))
        .unwrap();
    xl1.add_version(build_file_info("v2", "d2", None, 20))
        .unwrap();
    let mut xl2 = XlMetaV2::default();
    xl2.add_version(build_file_info("v1", "d1", None, 30))
        .unwrap();
    xl2.add_version(build_file_info("v3", "d3", None, 10))
        .unwrap();
    let mut xl3 = XlMetaV2::default();
    xl3.add_version(build_file_info("v1", "d1", None, 30))
        .unwrap();
    xl3.add_version(build_file_info("v2", "d2", None, 20))
        .unwrap();

    let (tx1, rx1) = channel();
    let (tx2, rx2) = channel();
    let (tx3, rx3) = channel();
    let (out_tx, out_rx) = channel();

    tx1.send(MetaCacheEntry {
        name: "a".to_string(),
        metadata: xl1.append_to(None).unwrap(),
    })
    .unwrap();
    tx2.send(MetaCacheEntry {
        name: "a".to_string(),
        metadata: xl2.append_to(None).unwrap(),
    })
    .unwrap();
    tx3.send(MetaCacheEntry {
        name: "a".to_string(),
        metadata: xl3.append_to(None).unwrap(),
    })
    .unwrap();

    merge_entry_channels(vec![rx1, rx2, rx3], out_tx, 2).expect("merge channels");
    let out = out_rx.recv().expect("merged entry");
    let xl = out.xlmeta().expect("decode merged metadata");
    assert_eq!(xl.versions.len(), 2);
    assert!(xl.versions[0].header.sorts_before(&xl.versions[1].header));
}

#[test]
fn test_xmin_iohealing_skip_line_1108() {
    let mut xl = XlMetaV2::default();
    let mut file_info = build_file_info(
        "healing-version",
        "healing-dir",
        None,
        1_700_000_000_222_333_444,
    );
    file_info.set_healing();
    assert!(
        file_info.healing(),
        "precondition: source file info should heal"
    );

    xl.add_version(file_info.clone())
        .expect("add healing version");
    let loaded = xl
        .to_file_info(
            &file_info.volume,
            &file_info.name,
            &file_info.version_id,
            false,
            true,
        )
        .expect("to_file_info should succeed");

    assert!(
        !loaded.healing(),
        "ToFileInfo should clear the internal healing marker"
    );
    assert!(
        loaded
            .metadata
            .as_ref()
            .is_none_or(|meta| !meta.contains_key(HEALING_KEY)),
        "healing marker should not leak into returned metadata"
    );
}

#[test]
fn subbenchmark_file_scope_to_file_info_line_1173() {
    let xl = sample_xl_meta_with_many_parts();
    for _ in 0..100 {
        let file_info = xl
            .to_file_info("volume", "path", "", false, false)
            .expect("to_file_info without parts");
        assert!(file_info.parts.is_none(), "parts should be skipped");
    }
}

#[test]
fn benchmark_to_file_info_no_parts_line_1186() {
    let xl = sample_xl_meta_with_many_parts();
    for _ in 0..100 {
        let file_info = xl
            .to_file_info("volume", "path", "", false, false)
            .expect("to_file_info without parts");
        assert!(file_info.parts.is_none(), "parts should be omitted");
        assert_eq!(file_info.size, 128 * 1024);
    }
}

#[test]
fn benchmark_to_file_info_with_parts_line_1190() {
    let xl = sample_xl_meta_with_many_parts();
    for _ in 0..100 {
        let file_info = xl
            .to_file_info("volume", "path", "", false, true)
            .expect("to_file_info with parts");
        let parts = file_info.parts.expect("parts should be loaded");
        assert_eq!(parts.len(), 128);
        assert_eq!(parts.first().map(|part| part.number), Some(1));
        assert_eq!(parts.last().map(|part| part.number), Some(128));
    }
}
