use std::fs;

use minio_rust::internal::disk::{self, IoStats};

pub const SOURCE_FILE: &str = "internal/disk/stat_test.go";

#[test]
fn test_read_drive_stats() {
    let test_cases = [
        (
            "1432553   420084 66247626  2398227  7077314  8720147 157049224  7469810        0  7580552  9869354    46037        0 41695120     1315        0        0",
            IoStats {
                read_ios: 1432553,
                read_merges: 420084,
                read_sectors: 66247626,
                read_ticks: 2398227,
                write_ios: 7077314,
                write_merges: 8720147,
                write_sectors: 157049224,
                write_ticks: 7469810,
                current_ios: 0,
                total_ticks: 7580552,
                req_ticks: 9869354,
                discard_ios: 46037,
                discard_merges: 0,
                discard_sectors: 41695120,
                discard_ticks: 1315,
                flush_ios: 0,
                flush_ticks: 0,
            },
            false,
        ),
        (
            "1432553   420084 66247626  2398227  7077314  8720147 157049224  7469810        0  7580552  9869354    46037        0 41695120     1315",
            IoStats {
                read_ios: 1432553,
                read_merges: 420084,
                read_sectors: 66247626,
                read_ticks: 2398227,
                write_ios: 7077314,
                write_merges: 8720147,
                write_sectors: 157049224,
                write_ticks: 7469810,
                current_ios: 0,
                total_ticks: 7580552,
                req_ticks: 9869354,
                discard_ios: 46037,
                discard_merges: 0,
                discard_sectors: 41695120,
                discard_ticks: 1315,
                ..IoStats::default()
            },
            false,
        ),
        (
            "1432553   420084 66247626  2398227  7077314  8720147 157049224  7469810        0  7580552  9869354",
            IoStats {
                read_ios: 1432553,
                read_merges: 420084,
                read_sectors: 66247626,
                read_ticks: 2398227,
                write_ios: 7077314,
                write_merges: 8720147,
                write_sectors: 157049224,
                write_ticks: 7469810,
                current_ios: 0,
                total_ticks: 7580552,
                req_ticks: 9869354,
                ..IoStats::default()
            },
            false,
        ),
        ("1432553   420084 66247626  2398227", IoStats::default(), true),
    ];

    for (idx, (stat, expected, expect_err)) in test_cases.into_iter().enumerate() {
        let temp_dir = tempfile::tempdir().expect("tempdir must be created");
        let stat_file = temp_dir.path().join(format!("diskstats-{idx}.txt"));
        fs::write(&stat_file, stat).expect("stat file must be written");

        let io_stats = disk::read_drive_stats(&stat_file);
        match (io_stats, expect_err) {
            (Ok(actual), false) => assert_eq!(actual, expected),
            (Err(_), true) => {}
            (Ok(actual), true) => panic!("expected an error, got {actual:?}"),
            (Err(err), false) => panic!("unexpected parse error: {err}"),
        }
    }
}
