use std::collections::BTreeMap;

use crate::cmd::{MetricDescription, MetricV2};

pub fn get_histogram_metrics(
    description: MetricDescription,
    bucket_label: &str,
    buckets: impl IntoIterator<Item = (impl Into<String>, u64)>,
) -> MetricV2 {
    let histogram = buckets
        .into_iter()
        .map(|(bucket, value)| (bucket.into(), value))
        .collect::<BTreeMap<_, _>>();

    MetricV2 {
        description,
        histogram_bucket_label: bucket_label.to_string(),
        histogram: Some(histogram),
        ..MetricV2::default()
    }
}
