use minio_rust::cmd::TierMetrics;

pub const SOURCE_FILE: &str = "cmd/tier_test.go";

#[test]
fn test_tier_metrics_line_25() {
    let mut metrics = TierMetrics::default();

    metrics.record_pending("WARM-TIER", 128);
    metrics.record_pending("WARM-TIER", 64);
    metrics.record_completed("WARM-TIER", 128);
    metrics.record_failed("COLD-TIER", 32);

    let warm = metrics.metric("WARM-TIER");
    assert_eq!(warm.pending, 2);
    assert_eq!(warm.pending_bytes, 192);
    assert_eq!(warm.completed, 1);
    assert_eq!(warm.completed_bytes, 128);
    assert_eq!(warm.failed, 0);

    let cold = metrics.metric("COLD-TIER");
    assert_eq!(cold.failed, 1);
    assert_eq!(cold.failed_bytes, 32);
    assert_eq!(cold.pending, 0);

    let missing = metrics.metric("MISSING");
    assert_eq!(missing.pending, 0);
    assert_eq!(missing.completed, 0);
    assert_eq!(missing.failed, 0);
}
