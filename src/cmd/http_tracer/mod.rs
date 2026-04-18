use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

pub fn redact_ldap_password(input: &str) -> String {
    let mut output = input.to_string();
    for key in ["LDAPPassword=", "ldappassword=", "ldapPassword="] {
        let mut search_from = 0;
        loop {
            let Some(found) = output[search_from..].find(key) else {
                break;
            };
            let start = search_from + found;
            let value_start = start + key.len();
            let value_end = output[value_start..]
                .find(['&', ' ', '\n', '\r'])
                .map(|idx| value_start + idx)
                .unwrap_or(output.len());
            output.replace_range(value_start..value_end, "*REDACTED*");
            search_from = value_start + "*REDACTED*".len();
        }
    }
    output
}

#[derive(Debug, Default)]
pub struct HttpStats {
    total_requests: AtomicU64,
    total_errors: AtomicU64,
    buckets: Mutex<BTreeMap<String, u64>>,
}

impl HttpStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_request(&self, bucket: Option<&str>, is_error: bool) {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
        if is_error {
            self.total_errors.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(bucket) = bucket {
            let mut buckets = self.buckets.lock().expect("bucket stats lock");
            *buckets.entry(bucket.to_string()).or_insert(0) += 1;
        }
    }

    pub fn total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::SeqCst)
    }

    pub fn total_errors(&self) -> u64 {
        self.total_errors.load(Ordering::SeqCst)
    }

    pub fn bucket_requests(&self, bucket: &str) -> u64 {
        self.buckets
            .lock()
            .expect("bucket stats lock")
            .get(bucket)
            .copied()
            .unwrap_or(0)
    }
}
