use std::collections::BTreeMap;

use minio_rust::cmd::{get_histogram_metrics, MetricDescription};

pub const SOURCE_FILE: &str = "cmd/metrics-v2_test.go";

#[test]
fn test_get_histogram_metrics_bucket_count_line_29() {
    let metric = get_histogram_metrics(
        MetricDescription {
            namespace: "minio".to_string(),
            subsystem: "scanner".to_string(),
            name: "objects_scanned".to_string(),
            help: "Objects scanned".to_string(),
            metric_type: "histogram".to_string(),
        },
        "le",
        [("1", 4), ("10", 9), ("+Inf", 12)],
    );

    assert_eq!(metric.histogram_bucket_label, "le");
    assert_eq!(metric.histogram.as_ref().map(BTreeMap::len), Some(3));
}

#[test]
fn test_get_histogram_metrics_values_line_96() {
    let metric = get_histogram_metrics(
        MetricDescription {
            namespace: "minio".to_string(),
            subsystem: "replication".to_string(),
            name: "queue_latency".to_string(),
            help: "Queue latency".to_string(),
            metric_type: "histogram".to_string(),
        },
        "seconds",
        [("0.5", 2), ("1", 5), ("5", 8), ("+Inf", 8)],
    );

    let histogram = metric.histogram.expect("histogram");
    assert_eq!(histogram.get("0.5"), Some(&2));
    assert_eq!(histogram.get("1"), Some(&5));
    assert_eq!(histogram.get("5"), Some(&8));
    assert_eq!(histogram.get("+Inf"), Some(&8));
}
