use std::collections::BTreeMap;

use minio_rust::cmd::{
    migrate_size_histogram_from_v1, size_histogram_to_map, SizeHistogram, SizeHistogramV1,
};

pub const SOURCE_FILE: &str = "cmd/data-usage-cache_test.go";

#[test]
fn test_size_histogram_to_map_line_27() {
    run_size_histogram_to_map_cases();
}

#[test]
fn subtest_test_size_histogram_to_map_fmt_sprintf_test_d_line_52() {
    run_size_histogram_to_map_cases();
}

#[test]
fn test_migrate_size_histogram_from_v1_line_75() {
    run_migrate_size_histogram_cases();
}

#[test]
fn subtest_test_migrate_size_histogram_from_v1_fmt_sprintf_test_d_line_90() {
    run_migrate_size_histogram_cases();
}

fn run_size_histogram_to_map_cases() {
    let tests = [
        (
            SizeHistogram(Some(vec![1, 0, 2, 0, 0, 1, 0])),
            BTreeMap::from([
                ("0B-1KB".to_string(), 1_u64),
                ("1MB-10MB".to_string(), 2_u64),
                ("128MB-1GB".to_string(), 1_u64),
            ]),
        ),
        (SizeHistogram(Some(vec![0; 7])), BTreeMap::new()),
    ];

    for (index, (histogram, expected)) in tests.into_iter().enumerate() {
        assert_eq!(
            size_histogram_to_map(&histogram),
            expected,
            "case {}",
            index + 1
        );
    }
}

fn run_migrate_size_histogram_cases() {
    let tests = [
        (
            SizeHistogramV1(Some(vec![1, 2, 3])),
            SizeHistogram(Some(vec![1, 2, 3])),
        ),
        (SizeHistogramV1(None), SizeHistogram(None)),
    ];

    for (index, (histogram, expected)) in tests.into_iter().enumerate() {
        assert_eq!(
            migrate_size_histogram_from_v1(&histogram),
            expected,
            "case {}",
            index + 1
        );
    }
}
