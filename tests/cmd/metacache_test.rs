use minio_rust::cmd::{base_dir_from_prefix, Metacache, ScanStatus, METACACHE_STREAM_VERSION};

pub const SOURCE_FILE: &str = "cmd/metacache_test.go";

fn sample_metacaches(now: i64) -> Vec<Metacache> {
    vec![
        Metacache {
            id: "case-1-normal".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/prefix".to_string(),
            recursive: false,
            status: ScanStatus::Success,
            file_not_found: false,
            error: String::new(),
            started: now,
            ended: now + 60,
            last_update: now + 60,
            last_handout: now,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-2-recursive".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/prefix".to_string(),
            recursive: true,
            status: ScanStatus::Success,
            started: now,
            ended: now + 60,
            last_update: now + 60,
            last_handout: now,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-3-older".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/prefix".to_string(),
            status: ScanStatus::Success,
            file_not_found: true,
            started: now - 60,
            ended: now,
            last_update: now,
            last_handout: now,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-4-error".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/prefix".to_string(),
            status: ScanStatus::Error,
            error: "an error lol".to_string(),
            started: now - 20 * 60,
            ended: now - 20 * 60,
            last_update: now - 20 * 60,
            last_handout: now - 20 * 60,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-5-noupdate".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/prefix".to_string(),
            status: ScanStatus::Started,
            started: now - 60,
            ended: 0,
            last_update: now - 60,
            last_handout: now,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-6-404notfound".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/notfound".to_string(),
            recursive: true,
            status: ScanStatus::Success,
            file_not_found: true,
            started: now,
            ended: now + 60,
            last_update: now + 60,
            last_handout: now,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-7-oldcycle".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/prefix".to_string(),
            recursive: true,
            status: ScanStatus::Success,
            started: now - 10 * 60,
            ended: now - 8 * 60,
            last_update: now - 8 * 60,
            last_handout: now,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-8-running".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/running".to_string(),
            status: ScanStatus::Started,
            started: now - 60,
            ended: 0,
            last_update: now - 60,
            last_handout: now,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
        Metacache {
            id: "case-8-finished-a-week-ago".to_string(),
            bucket: "bucket".to_string(),
            root: "folder/finished".to_string(),
            status: ScanStatus::Success,
            started: now - 7 * 24 * 60 * 60,
            ended: now - 7 * 24 * 60 * 60,
            last_update: now - 7 * 24 * 60 * 60,
            last_handout: now - 7 * 24 * 60 * 60,
            data_version: METACACHE_STREAM_VERSION,
            ..Default::default()
        },
    ]
}

#[test]
fn test_base_dir_from_prefix_line_156() {
    let cases = [
        ("object.ext", ""),
        ("./object.ext", ""),
        ("/", ""),
        ("prefix/", "prefix/"),
        ("prefix/obj.ext", "prefix/"),
        ("prefix/prefix2/obj.ext", "prefix/prefix2/"),
        ("prefix/prefix2/", "prefix/prefix2/"),
    ];
    for (prefix, want) in cases {
        assert_eq!(base_dir_from_prefix(prefix), want, "{prefix}");
    }
}

#[test]
fn subtest_test_base_dir_from_prefix_tt_name_line_199() {
    let cases = [
        ("root", "object.ext", ""),
        ("folderobj", "prefix/obj.ext", "prefix/"),
        ("folderfolder", "prefix/prefix2/", "prefix/prefix2/"),
    ];
    for (name, prefix, want) in cases {
        assert_eq!(base_dir_from_prefix(prefix), want, "{name}");
    }
}

#[test]
fn test_metacache_finished_line_207() {
    let now = 1_700_000_000;
    let want = [true, true, true, true, false, true, true, false, true];
    for (entry, want) in sample_metacaches(now).into_iter().zip(want) {
        assert_eq!(entry.finished(), want, "{}", entry.id);
    }
}

#[test]
fn subtest_test_metacache_finished_tt_id_line_211() {
    let now = 1_700_000_000;
    let entries = sample_metacaches(now);
    assert!(entries[0].finished(), "{}", entries[0].id);
    assert!(!entries[4].finished(), "{}", entries[4].id);
    assert!(!entries[7].finished(), "{}", entries[7].id);
}

#[test]
fn test_metacache_worth_keeping_line_227() {
    let now = 1_700_000_000;
    let want = [true, true, true, false, true, true, true, true, false];
    for (entry, want) in sample_metacaches(now).into_iter().zip(want) {
        assert_eq!(entry.worth_keeping_at(now), want, "{}", entry.id);
    }
}

#[test]
fn subtest_test_metacache_worth_keeping_tt_id_line_232() {
    let now = 1_700_000_000;
    let entries = sample_metacaches(now);
    assert!(!entries[3].worth_keeping_at(now), "{}", entries[3].id);
    assert!(entries[5].worth_keeping_at(now), "{}", entries[5].id);
    assert!(!entries[8].worth_keeping_at(now), "{}", entries[8].id);
}
