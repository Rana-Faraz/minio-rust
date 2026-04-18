// Rust test snapshot derived from cmd/erasure-healing-common_test.go.

use std::collections::BTreeMap;

use minio_rust::cmd::{
    check_object_with_all_parts, common_parity, common_time, list_object_parities,
    list_online_disks, new_file_info, part_needs_healing, FileInfo, ObjectPartInfo,
    CHECK_PART_FILE_CORRUPT, CHECK_PART_SUCCESS, ERR_DISK_NOT_FOUND, ERR_FILE_NOT_FOUND,
    TIME_SENTINEL,
};

pub const SOURCE_FILE: &str = "cmd/erasure-healing-common_test.go";

#[test]
fn test_common_time_line_82() {
    let cases = [
        (vec![1, 2, 3, 3, 2, 3, 1], 3, 3),
        (vec![3, 3, 3, 3, 3, 3, 3], 3, 4),
        (
            vec![
                3,
                3,
                2,
                1,
                3,
                4,
                3,
                TIME_SENTINEL,
                TIME_SENTINEL,
                TIME_SENTINEL,
            ],
            TIME_SENTINEL,
            5,
        ),
    ];

    for (times, expected, quorum) in cases {
        assert_eq!(common_time(&times, quorum), expected);
    }
}

#[test]
fn test_list_online_disks_line_150() {
    let disks = (0..16).map(|idx| format!("disk-{idx}")).collect::<Vec<_>>();

    let make_parts = |mod_times: &[i64], etag: &str| {
        mod_times
            .iter()
            .enumerate()
            .map(|(index, mod_time)| {
                let mut fi = new_file_info("object", 12, 4);
                fi.erasure.index = (index + 1) as i32;
                fi.mod_time = *mod_time;
                fi.metadata = Some(BTreeMap::from([("etag".to_string(), etag.to_string())]));
                fi
            })
            .collect::<Vec<FileInfo>>()
    };

    let mut mod_times_three_four = vec![3; 16];
    let mut mod_times_three_none = vec![3; 16];
    for idx in 13..16 {
        mod_times_three_four[idx] = 4;
        mod_times_three_none[idx] = TIME_SENTINEL;
    }

    let cases = [
        (
            make_parts(&mod_times_three_four, "etag-a"),
            vec![None; 16],
            12usize,
            3i64,
            13usize,
        ),
        (
            make_parts(&mod_times_three_none, "etag-a"),
            vec![
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(ERR_FILE_NOT_FOUND),
                Some("disk access denied"),
                Some(ERR_DISK_NOT_FOUND),
            ],
            12usize,
            3i64,
            13usize,
        ),
    ];

    for (parts, errs, quorum, expected_time, expected_online) in cases {
        let (online_disks, mod_time, etag) = list_online_disks(&disks, &parts, &errs, quorum);
        assert_eq!(mod_time, expected_time);
        assert!(etag.is_empty());
        assert_eq!(
            online_disks.iter().filter(|disk| disk.is_some()).count(),
            expected_online
        );
    }
}

#[test]
fn subtest_test_list_online_disks_fmt_sprintf_case_d_line_247() {
    let disks = (0..4).map(|idx| format!("disk-{idx}")).collect::<Vec<_>>();
    let mut parts = Vec::new();
    for index in 0..4 {
        let mut fi = new_file_info("object", 2, 2);
        fi.erasure.index = (index + 1) as i32;
        fi.mod_time = if index < 3 { 3 } else { 4 };
        fi.metadata = Some(BTreeMap::from([("etag".to_string(), "etag-a".to_string())]));
        parts.push(fi);
    }

    let (online_disks, mod_time, _) =
        list_online_disks(&disks, &parts, &[None, None, None, None], 3);
    assert_eq!(mod_time, 3);
    assert!(online_disks[0].is_some());
    assert!(online_disks[1].is_some());
    assert!(online_disks[2].is_some());
    assert!(online_disks[3].is_none());
}

#[test]
fn test_list_online_disks_small_objects_line_329() {
    let disks = (0..4).map(|idx| format!("disk-{idx}")).collect::<Vec<_>>();
    let mut parts = Vec::new();
    for index in 0..4 {
        let mut fi = new_file_info("object", 2, 2);
        fi.erasure.index = (index + 1) as i32;
        fi.mod_time = TIME_SENTINEL;
        fi.metadata = Some(BTreeMap::from([(
            "etag".to_string(),
            if index < 3 { "etag-a" } else { "etag-b" }.to_string(),
        )]));
        parts.push(fi);
    }

    let (online_disks, mod_time, etag) =
        list_online_disks(&disks, &parts, &[None, None, None, None], 3);
    assert_eq!(mod_time, TIME_SENTINEL);
    assert_eq!(etag, "etag-a");
    assert!(online_disks[0].is_some());
    assert!(online_disks[1].is_some());
    assert!(online_disks[2].is_some());
    assert!(online_disks[3].is_none());
}

#[test]
fn subtest_test_list_online_disks_small_objects_fmt_sprintf_case_d_line_421() {
    let disks = (0..4).map(|idx| format!("disk-{idx}")).collect::<Vec<_>>();
    let mut parts = Vec::new();
    for index in 0..4 {
        let mut fi = new_file_info("object", 2, 2);
        fi.erasure.index = (index + 1) as i32;
        fi.mod_time = TIME_SENTINEL;
        fi.metadata = Some(BTreeMap::from([(
            "etag".to_string(),
            if index == 3 { "etag-b" } else { "etag-a" }.to_string(),
        )]));
        parts.push(fi);
    }

    let errs = [
        None,
        None,
        Some(ERR_FILE_NOT_FOUND),
        Some(ERR_DISK_NOT_FOUND),
    ];
    let (online_disks, mod_time, etag) = list_online_disks(&disks, &parts, &errs, 2);
    assert_eq!(mod_time, TIME_SENTINEL);
    assert_eq!(etag, "etag-a");
    assert!(online_disks[0].is_some());
    assert!(online_disks[1].is_some());
    assert!(online_disks[2].is_none());
    assert!(online_disks[3].is_none());
}

#[test]
fn test_disks_with_all_parts_line_507() {
    let disks = (0..16).map(|idx| format!("disk-{idx}")).collect::<Vec<_>>();
    let mut parts_metadata = Vec::new();
    for index in 0..16 {
        let mut fi = new_file_info("object", 12, 4);
        fi.erasure.index = (index + 1) as i32;
        fi.mod_time = 100;
        fi.data_dir = "data-dir".to_string();
        fi.parts = Some(vec![
            ObjectPartInfo {
                number: 1,
                size: 10,
                etag: String::new(),
                actual_size: 10,
            },
            ObjectPartInfo {
                number: 2,
                size: 10,
                etag: String::new(),
                actual_size: 10,
            },
            ObjectPartInfo {
                number: 3,
                size: 10,
                etag: String::new(),
                actual_size: 10,
            },
        ]);
        parts_metadata.push(fi);
    }

    let latest_meta = parts_metadata[0].clone();
    let errs = vec![None; disks.len()];

    let (mut online_disks, _, _) = list_online_disks(&disks, &parts_metadata, &errs, 8);
    let data_errs_per_disk = check_object_with_all_parts(
        &mut online_disks,
        &parts_metadata,
        &errs,
        &latest_meta,
        &BTreeMap::new(),
    );
    for (disk_index, disk) in online_disks.iter().enumerate() {
        assert!(
            !part_needs_healing(&data_errs_per_disk[disk_index]),
            "unexpected part errors on disk {disk_index}: {:?}",
            data_errs_per_disk[disk_index]
        );
        assert!(disk.is_some(), "disk {disk_index} should stay online");
    }

    let mut modtime_mismatch = parts_metadata.clone();
    modtime_mismatch[0].mod_time -= 3600;
    let mut online_disks = disks.iter().cloned().map(Some).collect::<Vec<_>>();
    let _ = check_object_with_all_parts(
        &mut online_disks,
        &modtime_mismatch,
        &errs,
        &latest_meta,
        &BTreeMap::new(),
    );
    for (disk_index, disk) in online_disks.iter().enumerate() {
        if disk_index == 0 {
            assert!(
                disk.is_none(),
                "disk 0 should be filtered on modtime mismatch"
            );
        } else {
            assert!(disk.is_some(), "disk {disk_index} should remain online");
        }
    }

    let mut datadir_mismatch = parts_metadata.clone();
    datadir_mismatch[1].data_dir = "foo-random".to_string();
    let mut online_disks = disks.iter().cloned().map(Some).collect::<Vec<_>>();
    let _ = check_object_with_all_parts(
        &mut online_disks,
        &datadir_mismatch,
        &errs,
        &latest_meta,
        &BTreeMap::new(),
    );
    for (disk_index, disk) in online_disks.iter().enumerate() {
        if disk_index == 1 {
            assert!(
                disk.is_none(),
                "disk 1 should be filtered on data-dir mismatch"
            );
        } else {
            assert!(disk.is_some(), "disk {disk_index} should remain online");
        }
    }

    let mut online_disks = disks.iter().cloned().map(Some).collect::<Vec<_>>();
    let simulated_part_errors = BTreeMap::from([
        (
            0usize,
            vec![
                CHECK_PART_FILE_CORRUPT,
                CHECK_PART_SUCCESS,
                CHECK_PART_SUCCESS,
            ],
        ),
        (
            3usize,
            vec![
                CHECK_PART_FILE_CORRUPT,
                CHECK_PART_SUCCESS,
                CHECK_PART_SUCCESS,
            ],
        ),
        (
            15usize,
            vec![
                CHECK_PART_FILE_CORRUPT,
                CHECK_PART_SUCCESS,
                CHECK_PART_SUCCESS,
            ],
        ),
    ]);
    let data_errs_per_disk = check_object_with_all_parts(
        &mut online_disks,
        &parts_metadata,
        &errs,
        &latest_meta,
        &simulated_part_errors,
    );
    for disk_index in 0..online_disks.len() {
        if simulated_part_errors.contains_key(&disk_index) {
            assert!(
                part_needs_healing(&data_errs_per_disk[disk_index]),
                "disk {disk_index} should need healing"
            );
        } else {
            assert!(
                !part_needs_healing(&data_errs_per_disk[disk_index]),
                "disk {disk_index} should not need healing"
            );
        }
    }
}

#[test]
fn test_common_parities_line_646() {
    let mut fi1 = new_file_info("myobject", 6, 6);
    fi1.erasure.index = 1;
    fi1.metadata = Some(BTreeMap::from([("etag".to_string(), "etag-a".to_string())]));

    let mut fi2 = new_file_info("myobject", 7, 5);
    fi2.erasure.index = 2;
    fi2.metadata = Some(BTreeMap::from([("etag".to_string(), "etag-a".to_string())]));

    let fi_del = FileInfo {
        deleted: true,
        ..Default::default()
    };

    let tests = [(fi1.clone(), fi2), (fi1, fi_del)];
    for (idx, (left, right)) in tests.into_iter().enumerate() {
        let mut meta_arr = Vec::new();
        for shard in 0..12 {
            let mut fi = if shard % 2 == 0 {
                right.clone()
            } else {
                left.clone()
            };
            fi.erasure.index = shard + 1;
            meta_arr.push(fi);
        }

        let errs = vec![None; meta_arr.len()];
        let parities = list_object_parities(&meta_arr, &errs);
        let parity = common_parity(&parities, 5);
        let matches = meta_arr
            .iter()
            .filter(|fi| fi.erasure.parity_blocks == parity)
            .count();
        assert!(
            matches >= meta_arr.len() - parity as usize,
            "test {idx}: expected enough drives with parity={parity}, got {matches}"
        );
    }
}
