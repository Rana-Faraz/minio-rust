use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

pub const BETA_BUCKET: f64 = 0.1;

#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
pub struct BucketOptions {
    pub name: String,
    pub replication_arn: String,
}

impl PartialEq for BucketOptions {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.replication_arn == other.replication_arn
    }
}

impl Hash for BucketOptions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.replication_arn.hash(state);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Details {
    pub limit_in_bytes_per_second: i64,
    pub current_bandwidth_in_bytes_per_second: f64,
}

impl Details {
    pub fn marshal(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn unmarshal(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BucketBandwidthReport {
    pub bucket_stats: HashMap<BucketOptions, Details>,
}

impl BucketBandwidthReport {
    pub fn marshal(&self) -> Result<Vec<u8>, serde_json::Error> {
        let entries = self
            .bucket_stats
            .iter()
            .map(|(bucket, details)| BucketStatEntry {
                bucket: bucket.clone(),
                details: details.clone(),
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&entries)
    }

    pub fn unmarshal(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let entries = serde_json::from_slice::<Vec<BucketStatEntry>>(bytes)?;
        Ok(Self {
            bucket_stats: entries
                .into_iter()
                .map(|entry| (entry.bucket, entry.details))
                .collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BucketStatEntry {
    bucket: BucketOptions,
    details: Details,
}

#[derive(Debug, Clone)]
pub struct BucketThrottle {
    pub node_bandwidth_per_sec: i64,
}

#[derive(Debug, Clone)]
pub struct BucketMeasurement {
    bytes_since_last_window: u64,
    start_time: SystemTime,
    exp_moving_avg: f64,
}

impl BucketMeasurement {
    pub fn new(init_time: SystemTime) -> Self {
        Self {
            bytes_since_last_window: 0,
            start_time: init_time,
            exp_moving_avg: 0.0,
        }
    }

    pub fn increment_bytes(&mut self, bytes: u64) {
        self.bytes_since_last_window = self.bytes_since_last_window.saturating_add(bytes);
    }

    pub fn update_exponential_moving_average(&mut self, end_time: SystemTime) {
        let Ok(duration) = end_time.duration_since(self.start_time) else {
            return;
        };
        if duration.is_zero() {
            return;
        }

        let bytes_since_last_window = std::mem::take(&mut self.bytes_since_last_window);
        let increment = bytes_since_last_window as f64 / duration.as_secs_f64();

        if self.exp_moving_avg == 0.0 {
            self.exp_moving_avg = increment;
        } else {
            self.exp_moving_avg =
                exponential_moving_average(BETA_BUCKET, self.exp_moving_avg, increment);
        }

        self.start_time = end_time;
    }

    pub fn exp_moving_avg_bytes_per_second(&self) -> f64 {
        self.exp_moving_avg
    }
}

pub fn exponential_moving_average(beta: f64, previous_avg: f64, increment_avg: f64) -> f64 {
    (1.0 - beta) * increment_avg + beta * previous_avg
}

pub type SelectionFunction = Box<dyn Fn(&str) -> bool + Send + Sync + 'static>;

pub fn select_buckets(buckets: &[&str]) -> SelectionFunction {
    if buckets.is_empty() {
        return Box::new(|_| true);
    }
    let buckets = buckets
        .iter()
        .map(|bucket| (*bucket).to_owned())
        .collect::<Vec<_>>();
    Box::new(move |bucket| buckets.iter().any(|candidate| candidate == bucket))
}

#[derive(Debug, Default)]
pub struct Monitor {
    pub buckets_throttle: HashMap<BucketOptions, BucketThrottle>,
    pub buckets_measurement: HashMap<BucketOptions, BucketMeasurement>,
    pub node_count: u64,
}

impl Monitor {
    pub fn get_report(&self, select_bucket: &dyn Fn(&str) -> bool) -> BucketBandwidthReport {
        let mut report = BucketBandwidthReport {
            bucket_stats: HashMap::new(),
        };

        for (bucket_opts, bucket_measurement) in &self.buckets_measurement {
            if !select_bucket(&bucket_opts.name) {
                continue;
            }

            if let Some(throttle) = self.buckets_throttle.get(bucket_opts) {
                report.bucket_stats.insert(
                    bucket_opts.clone(),
                    Details {
                        limit_in_bytes_per_second: throttle.node_bandwidth_per_sec
                            * self.node_count as i64,
                        current_bandwidth_in_bytes_per_second: bucket_measurement
                            .exp_moving_avg_bytes_per_second(),
                    },
                );
            }
        }

        report
    }

    pub fn set_bandwidth_limit(&mut self, bucket: &str, arn: &str, limit: i64) {
        let limit_bytes = limit / self.node_count.max(1) as i64;
        self.buckets_throttle.insert(
            BucketOptions {
                name: bucket.to_owned(),
                replication_arn: arn.to_owned(),
            },
            BucketThrottle {
                node_bandwidth_per_sec: limit_bytes,
            },
        );
    }

    pub fn is_throttled(&self, bucket: &str, arn: &str) -> bool {
        self.buckets_throttle.contains_key(&BucketOptions {
            name: bucket.to_owned(),
            replication_arn: arn.to_owned(),
        })
    }

    pub fn delete_bucket(&mut self, bucket: &str) {
        self.buckets_throttle.retain(|opts, _| opts.name != bucket);
        self.buckets_measurement
            .retain(|opts, _| opts.name != bucket);
    }

    pub fn delete_bucket_throttle(&mut self, bucket: &str, arn: &str) {
        let key = BucketOptions {
            name: bucket.to_owned(),
            replication_arn: arn.to_owned(),
        };
        self.buckets_throttle.remove(&key);
        self.buckets_measurement.remove(&key);
    }

    pub fn init(&mut self, opts: BucketOptions, now: SystemTime) {
        self.buckets_measurement
            .entry(opts)
            .or_insert_with(|| BucketMeasurement::new(now));
    }

    pub fn update_measurement(&mut self, opts: BucketOptions, bytes: u64, now: Option<SystemTime>) {
        let measurement = self
            .buckets_measurement
            .entry(opts)
            .or_insert_with(|| BucketMeasurement::new(now.unwrap_or_else(SystemTime::now)));
        measurement.increment_bytes(bytes);
    }
}

pub fn seconds(value: u64) -> Duration {
    Duration::from_secs(value)
}
