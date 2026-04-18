use std::collections::HashMap;

use minio_rust::internal::bucket::bandwidth::{BucketBandwidthReport, BucketOptions, Details};

#[test]
fn marshal_unmarshal_bucket_bandwidth_report_roundtrips() {
    let mut report = BucketBandwidthReport::default();
    report.bucket_stats.insert(
        BucketOptions {
            name: "bucket".to_owned(),
            replication_arn: "arn".to_owned(),
        },
        Details {
            limit_in_bytes_per_second: 1024,
            current_bandwidth_in_bytes_per_second: 512.0,
        },
    );

    let bytes = report.marshal().expect("serialization should succeed");
    let roundtrip =
        BucketBandwidthReport::unmarshal(&bytes).expect("deserialization should succeed");
    assert_eq!(roundtrip, report);
}

#[test]
fn encode_decode_bucket_bandwidth_report_roundtrips() {
    let report = BucketBandwidthReport {
        bucket_stats: HashMap::new(),
    };
    let bytes = report.marshal().expect("serialization should succeed");
    let decoded = BucketBandwidthReport::unmarshal(&bytes).expect("deserialization should succeed");
    assert_eq!(decoded, report);
}

#[test]
fn marshal_unmarshal_details_roundtrips() {
    let details = Details {
        limit_in_bytes_per_second: 1024,
        current_bandwidth_in_bytes_per_second: 1.5,
    };
    let bytes = details.marshal().expect("serialization should succeed");
    let roundtrip = Details::unmarshal(&bytes).expect("deserialization should succeed");
    assert_eq!(roundtrip, details);
}

#[test]
fn encode_decode_details_roundtrips() {
    let details = Details::default();
    let bytes = details.marshal().expect("serialization should succeed");
    let decoded = Details::unmarshal(&bytes).expect("deserialization should succeed");
    assert_eq!(decoded, details);
}
