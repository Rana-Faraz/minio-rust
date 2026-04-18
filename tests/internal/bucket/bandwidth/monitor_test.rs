use std::collections::HashMap;
use std::time::SystemTime;

use minio_rust::internal::bucket::bandwidth::{
    exponential_moving_average, seconds, select_buckets, BucketMeasurement, BucketOptions,
    BucketThrottle, Details, Monitor,
};

const ONE_MIB: u64 = 1024 * 1024;

#[test]
fn monitor_get_report_matches_reference_cases() {
    let start = SystemTime::UNIX_EPOCH + seconds(1_700_000_000);

    let mut m0 = BucketMeasurement::new(start);
    m0.increment_bytes(0);

    let mut m1_mib_ps = BucketMeasurement::new(start);
    m1_mib_ps.increment_bytes(ONE_MIB);

    let bucket = BucketOptions {
        name: "bucket".to_owned(),
        replication_arn: "arn".to_owned(),
    };

    let mut test1_active_buckets = HashMap::new();
    test1_active_buckets.insert(bucket.clone(), m0);

    let mut test1_active_buckets2 = HashMap::new();
    test1_active_buckets2.insert(bucket.clone(), m1_mib_ps);

    let cases = [
        (
            "ZeroToOne",
            test1_active_buckets,
            start + seconds(1),
            ONE_MIB,
            start + seconds(2),
            0.0,
            ONE_MIB as f64,
        ),
        (
            "OneToTwo",
            test1_active_buckets2,
            start + seconds(1),
            2 * ONE_MIB,
            start + seconds(2),
            ONE_MIB as f64,
            exponential_moving_average(BETA_BUCKET_FOR_TEST, ONE_MIB as f64, 2.0 * ONE_MIB as f64),
        ),
    ];

    for (name, active_buckets, end_time, update2, end_time2, want, want2) in cases {
        let mut throttles = HashMap::new();
        throttles.insert(
            bucket.clone(),
            BucketThrottle {
                node_bandwidth_per_sec: ONE_MIB as i64,
            },
        );

        let mut monitor = Monitor {
            buckets_throttle: throttles,
            buckets_measurement: active_buckets.clone(),
            node_count: 1,
        };

        monitor
            .buckets_measurement
            .get_mut(&bucket)
            .expect("measurement should exist")
            .update_exponential_moving_average(end_time);

        let report = monitor.get_report(&*select_buckets(&[]));
        assert_eq!(
            report.bucket_stats.get(&bucket),
            Some(&Details {
                limit_in_bytes_per_second: ONE_MIB as i64,
                current_bandwidth_in_bytes_per_second: want,
            }),
            "case {name}"
        );

        let measurement = monitor
            .buckets_measurement
            .get_mut(&bucket)
            .expect("measurement should exist");
        measurement.increment_bytes(update2);
        measurement.update_exponential_moving_average(end_time2);

        let report = monitor.get_report(&*select_buckets(&[]));
        assert_eq!(
            report.bucket_stats.get(&bucket),
            Some(&Details {
                limit_in_bytes_per_second: ONE_MIB as i64,
                current_bandwidth_in_bytes_per_second: want2,
            }),
            "case {name}"
        );
    }
}

const BETA_BUCKET_FOR_TEST: f64 = 0.1;
