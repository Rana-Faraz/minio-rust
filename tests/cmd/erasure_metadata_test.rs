// Rust test snapshot derived from cmd/erasure-metadata_test.go.

use minio_rust::cmd::{
    common_parity, find_file_info_in_quorum, list_object_parities, new_file_info,
    object_part_index, FileInfo,
};

pub const SOURCE_FILE: &str = "cmd/erasure-metadata_test.go";
const ACTUAL_SIZE: i64 = 1000;
const MIB: i64 = 1024 * 1024;

#[test]
fn test_add_object_part_line_33() {
    let cases = [
        (1, 0),
        (2, 1),
        (4, 2),
        (5, 3),
        (7, 4),
        (3, 2),
        (4, 3),
        (6, -1),
    ];

    let mut fi = new_file_info("test-object", 8, 8);
    fi.erasure.index = 1;
    assert!(fi.is_valid());

    for (part_num, expected_index) in cases {
        if expected_index > -1 {
            fi.add_object_part(
                part_num,
                &format!("etag.{part_num}"),
                i64::from(part_num) + MIB,
                ACTUAL_SIZE,
            );
        }

        assert_eq!(
            object_part_index(fi.parts.as_deref().unwrap_or(&[]), part_num),
            expected_index,
            "part {part_num}"
        );
    }
}

#[test]
fn test_object_part_index_line_73() {
    let mut fi = new_file_info("test-object", 8, 8);
    fi.erasure.index = 1;
    assert!(fi.is_valid());

    for part_num in [2, 1, 5, 4, 7] {
        fi.add_object_part(
            part_num,
            &format!("etag.{part_num}"),
            i64::from(part_num) + MIB,
            ACTUAL_SIZE,
        );
    }

    let cases = [(2, 1), (1, 0), (5, 3), (4, 2), (7, 4), (6, -1)];
    for (part_num, expected_index) in cases {
        assert_eq!(
            object_part_index(fi.parts.as_deref().unwrap_or(&[]), part_num),
            expected_index,
            "part {part_num}"
        );
    }
}

#[test]
fn test_object_to_part_offset_line_113() {
    let mut fi = new_file_info("test-object", 8, 8);
    fi.erasure.index = 1;
    assert!(fi.is_valid());

    for part_num in [1, 2, 4, 5, 7] {
        fi.add_object_part(
            part_num,
            &format!("etag.{part_num}"),
            i64::from(part_num) + MIB,
            ACTUAL_SIZE,
        );
    }

    let max_offset = (1 + 2 + 4 + 5 + 7) as i64 + (5 * MIB) - 1;
    let invalid_offset = (1 + 2 + 4 + 5 + 7) as i64 + (5 * MIB);
    let cases = [
        (0, Some((0, 0))),
        (MIB, Some((0, MIB))),
        (1 + MIB, Some((1, 0))),
        (2 + MIB, Some((1, 1))),
        (-1, Some((0, -1))),
        (max_offset, Some((4, 1_048_582))),
        (invalid_offset, None),
    ];

    for (offset, expected) in cases {
        match expected {
            Some((expected_index, expected_offset)) => {
                let (index, part_offset) = fi.object_to_part_offset(offset).expect("offset");
                assert_eq!(index, expected_index, "index mismatch for offset {offset}");
                assert_eq!(
                    part_offset, expected_offset,
                    "part offset mismatch for offset {offset}"
                );
            }
            None => {
                assert_eq!(
                    fi.object_to_part_offset(offset),
                    Err("InvalidRange".to_string()),
                    "offset {offset}"
                );
            }
        }
    }
}

fn quorum_file_infos(
    n: usize,
    quorum: usize,
    mod_time: i64,
    data_dir: &str,
    succ_mod_times: Option<&[i64]>,
    num_versions: Option<&[i32]>,
) -> Vec<FileInfo> {
    let mut fi = new_file_info("test", 8, 8);
    fi.add_object_part(1, "etag", 100, 100);
    fi.mod_time = mod_time;
    fi.data_dir = data_dir.to_string();

    let mut fis = vec![FileInfo::default(); n];
    for (index, entry) in fis.iter_mut().enumerate().take(quorum) {
        *entry = fi.clone();
        entry.erasure.index = index as i32 + 1;
        if let Some(times) = succ_mod_times {
            entry.successor_mod_time = times[index];
            entry.is_latest = times[index] == 0;
        }
        if let Some(versions) = num_versions {
            entry.num_versions = versions[index];
        }
    }
    fis
}

#[test]
fn test_find_file_info_in_quorum_line_161() {
    let common_succ_mod_time = 1_692_922_800i64;
    let mut succ_mod_times_in_quorum = vec![0i64; 16];
    let mut succ_mod_times_no_quorum = vec![0i64; 16];
    let common_num_versions = 2i32;
    let mut num_versions_in_quorum = vec![0i32; 16];
    let mut num_versions_no_quorum = vec![0i32; 16];
    for i in 0..16 {
        if i >= 4 {
            succ_mod_times_in_quorum[i] = common_succ_mod_time;
            num_versions_in_quorum[i] = common_num_versions;
        }
        if i >= 9 {
            succ_mod_times_no_quorum[i] = common_succ_mod_time;
            num_versions_no_quorum[i] = common_num_versions;
        }
    }

    let tests = vec![
        (
            quorum_file_infos(16, 16, 1_603_863_445, "dir", None, None),
            1_603_863_445,
            8usize,
            false,
            0i64,
            0i32,
            false,
        ),
        (
            quorum_file_infos(16, 7, 1_603_863_445, "dir", None, None),
            1_603_863_445,
            8usize,
            true,
            0i64,
            0i32,
            false,
        ),
        (
            quorum_file_infos(16, 16, 1_603_863_445, "dir", None, None),
            1_603_863_445,
            0usize,
            true,
            0i64,
            0i32,
            false,
        ),
        (
            quorum_file_infos(
                16,
                16,
                1_603_863_445,
                "dir",
                Some(&succ_mod_times_in_quorum),
                None,
            ),
            1_603_863_445,
            12usize,
            false,
            common_succ_mod_time,
            0i32,
            false,
        ),
        (
            quorum_file_infos(
                16,
                16,
                1_603_863_445,
                "dir",
                Some(&succ_mod_times_no_quorum),
                None,
            ),
            1_603_863_445,
            12usize,
            false,
            0i64,
            0i32,
            true,
        ),
        (
            quorum_file_infos(
                16,
                16,
                1_603_863_445,
                "dir",
                None,
                Some(&num_versions_in_quorum),
            ),
            1_603_863_445,
            12usize,
            false,
            0i64,
            common_num_versions,
            false,
        ),
        (
            quorum_file_infos(
                16,
                16,
                1_603_863_445,
                "dir",
                None,
                Some(&num_versions_no_quorum),
            ),
            1_603_863_445,
            12usize,
            false,
            0i64,
            0i32,
            false,
        ),
    ];

    for (fis, mod_time, quorum, expect_err, expected_succ, expected_versions, expected_latest) in
        tests
    {
        let result = find_file_info_in_quorum(&fis, mod_time, "", quorum);
        if expect_err {
            assert!(result.is_err(), "expected quorum error for quorum {quorum}");
            continue;
        }
        let fi = result.expect("file info in quorum");
        if expected_succ != 0 || expected_latest {
            assert_eq!(fi.successor_mod_time, expected_succ);
            assert_eq!(fi.is_latest, expected_latest);
        }
        if expected_versions >= 0 {
            assert_eq!(fi.num_versions, expected_versions);
        }
    }
}

#[test]
fn subtest_test_find_file_info_in_quorum_line_272() {
    let fis = quorum_file_infos(16, 16, 1_603_863_445, "dir", None, None);
    assert!(find_file_info_in_quorum(&fis, 1_603_863_445, "", 8).is_ok());
}

#[test]
fn test_transition_info_equals_line_296() {
    let inputs = [
        (
            "S3TIER-1",
            "remote-object-1",
            "remote-version-1",
            "complete",
        ),
        (
            "S3TIER-2",
            "remote-object-2",
            "remote-version-2",
            "complete",
        ),
    ];

    for i in 0..8u8 {
        let fi = FileInfo {
            transition_tier: inputs[0].0.to_string(),
            transitioned_obj_name: inputs[0].1.to_string(),
            transition_version_id: inputs[0].2.to_string(),
            transition_status: inputs[0].3.to_string(),
            ..Default::default()
        };
        let mut ofi = fi.clone();
        if i & (1 << 0) != 0 {
            ofi.transition_tier = inputs[1].0.to_string();
        }
        if i & (1 << 1) != 0 {
            ofi.transitioned_obj_name = inputs[1].1.to_string();
        }
        if i & (1 << 2) != 0 {
            ofi.transition_version_id = inputs[1].2.to_string();
        }

        let actual = fi.transition_info_equals(&ofi);
        if i == 0 {
            assert!(actual, "expected equal transition info");
        } else {
            assert!(!actual, "expected unequal transition info for mask {i}");
        }
    }

    let fi = FileInfo {
        transition_tier: inputs[0].0.to_string(),
        transitioned_obj_name: inputs[0].1.to_string(),
        transition_version_id: inputs[0].2.to_string(),
        transition_status: inputs[0].3.to_string(),
        ..Default::default()
    };
    assert!(!fi.transition_info_equals(&FileInfo::default()));
}

#[test]
fn test_skip_tier_free_version_line_355() {
    let mut fi = new_file_info("object", 8, 8);
    fi.set_skip_tier_free_version();
    assert!(fi.skip_tier_free_version());
}

#[test]
fn test_list_object_parities_line_363() {
    fn mk_meta_arr(n: usize, parity: i32, agree: usize) -> Vec<FileInfo> {
        let mut fi = new_file_info("obj-1", n as i32 - parity, parity);
        fi.transition_tier = "WARM-TIER".to_string();
        fi.transitioned_obj_name = "transitioned-object".to_string();
        fi.transition_status = "complete".to_string();
        fi.size = 1 << 20;

        let mut meta_arr = vec![FileInfo::default(); n];
        for (i, meta) in meta_arr.iter_mut().enumerate() {
            fi.erasure.index = i as i32 + 1;
            *meta = fi.clone();
            if i >= agree {
                meta.transition_tier.clear();
                meta.transitioned_obj_name.clear();
                meta.transition_status.clear();
            }
        }
        meta_arr
    }

    fn mk_parities(n: usize, agreed_parity: i32, disagreed_parity: i32, agree: usize) -> Vec<i32> {
        (0..n)
            .map(|i| {
                if i < agree {
                    agreed_parity
                } else {
                    disagreed_parity
                }
            })
            .collect()
    }

    fn mk_test(
        n: usize,
        parity: i32,
        agree: usize,
    ) -> (Vec<FileInfo>, Vec<Option<&'static str>>, Vec<i32>, i32) {
        let meta_arr = mk_meta_arr(n, parity, agree);
        let parities = mk_parities(n, n as i32 - (n as i32 / 2 + 1), parity, agree);
        let parity_out = if agree >= n / 2 + 1 {
            n as i32 - (n as i32 / 2 + 1)
        } else {
            -1
        };
        (meta_arr, vec![None; n], parities, parity_out)
    }

    fn non_tiered_test(
        n: usize,
        parity: i32,
        agree: usize,
    ) -> (Vec<FileInfo>, Vec<Option<&'static str>>, Vec<i32>, i32) {
        let mut fi = new_file_info("obj-1", n as i32 - parity, parity);
        fi.size = 1 << 20;
        let mut meta_arr = vec![FileInfo::default(); n];
        let mut parities = vec![0i32; n];
        for i in 0..n {
            fi.erasure.index = i as i32 + 1;
            meta_arr[i] = fi.clone();
            parities[i] = parity;
            if i >= agree {
                meta_arr[i].erasure.index = 0;
                parities[i] = -1;
            }
        }
        let parity_out = if agree >= n - parity as usize {
            parity
        } else {
            -1
        };
        (meta_arr, vec![None; n], parities, parity_out)
    }

    let tests = vec![
        mk_test(15, 3, 11),
        mk_test(15, 3, 7),
        mk_test(15, 3, 8),
        mk_test(16, 4, 11),
        mk_test(16, 4, 8),
        mk_test(16, 4, 9),
        non_tiered_test(15, 3, 12),
        non_tiered_test(15, 3, 11),
        non_tiered_test(16, 4, 12),
        non_tiered_test(16, 4, 11),
    ];

    for (meta_arr, errs, expected_parities, expected_parity) in tests {
        let got = list_object_parities(&meta_arr, &errs);
        assert_eq!(got, expected_parities);
        assert_eq!(
            common_parity(&got, meta_arr.len() as i32 / 2),
            expected_parity
        );
    }
}

#[test]
fn subtest_test_list_object_parities_fmt_sprintf_test_d_line_473() {
    let mut fi = new_file_info("obj-1", 12, 3);
    fi.size = 1 << 20;
    fi.erasure.index = 1;
    let meta_arr = vec![fi.clone(); 15];
    let errs = vec![None; 15];
    let parities = list_object_parities(&meta_arr, &errs);
    assert_eq!(common_parity(&parities, 7), 3);
}
