use minio_rust::cmd::{BatchJobSize, BatchJobSizeFilter, BatchJobYamlErr};

pub const SOURCE_FILE: &str = "cmd/batch-job-common-types_test.go";

#[test]
fn test_batch_job_size_in_range_line_25() {
    let cases = [
        (
            2_i64 << 20,
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(10 << 20),
                lower_bound: BatchJobSize(1 << 20),
            },
            true,
        ),
        (
            2_i64 << 10,
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(10 << 20),
                lower_bound: BatchJobSize(1 << 20),
            },
            false,
        ),
        (
            11_i64 << 20,
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(10 << 20),
                lower_bound: BatchJobSize(1 << 20),
            },
            false,
        ),
        (
            2_i64 << 20,
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(10 << 20),
                lower_bound: BatchJobSize(0),
            },
            true,
        ),
        (
            2_i64 << 20,
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(0),
                lower_bound: BatchJobSize(1 << 20),
            },
            true,
        ),
    ];

    for (size, filter, want) in cases {
        assert_eq!(filter.in_range(size), want);
    }
}

#[test]
fn subtest_test_batch_job_size_in_range_fmt_sprintf_test_d_line_77() {
    let filter = BatchJobSizeFilter {
        upper_bound: BatchJobSize(10 << 20),
        lower_bound: BatchJobSize(1 << 20),
    };
    assert!(filter.in_range(2 << 20));
    assert!(!filter.in_range(2 << 10));
}

#[test]
fn test_batch_job_size_validate_line_85() {
    let invalid = BatchJobYamlErr {
        msg: "invalid batch-job size filter".to_string(),
    };

    let cases = [
        (
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(0),
                lower_bound: BatchJobSize(0),
            },
            None,
        ),
        (
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(0),
                lower_bound: BatchJobSize(1 << 20),
            },
            None,
        ),
        (
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(10 << 20),
                lower_bound: BatchJobSize(0),
            },
            None,
        ),
        (
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(1 << 20),
                lower_bound: BatchJobSize(10 << 20),
            },
            Some(invalid.clone()),
        ),
        (
            BatchJobSizeFilter {
                upper_bound: BatchJobSize(1 << 20),
                lower_bound: BatchJobSize(1 << 20),
            },
            Some(invalid),
        ),
    ];

    for (filter, expected_err) in cases {
        let got = filter.validate().err();
        assert_eq!(
            got.as_ref().map(BatchJobYamlErr::message),
            expected_err.as_ref().map(BatchJobYamlErr::message)
        );
    }
}

#[test]
fn subtest_test_batch_job_size_validate_fmt_sprintf_test_d_line_134() {
    let filter = BatchJobSizeFilter {
        upper_bound: BatchJobSize(1 << 20),
        lower_bound: BatchJobSize(10 << 20),
    };
    let err = filter.validate().expect_err("invalid filter");
    assert_eq!(err.message(), "invalid batch-job size filter");
}
