use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cmd::{ListPathOptions, Metacache, ScanStatus};

#[derive(Debug, Clone, Default)]
pub struct BucketMetacache {
    pub bucket: String,
    pub caches: HashMap<String, Metacache>,
    pub caches_root: HashMap<String, Vec<String>>,
    pub updated: bool,
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl ListPathOptions {
    pub fn new_metacache(&self) -> Metacache {
        let now = now_unix();
        Metacache {
            started: now,
            last_handout: now,
            last_update: now,
            bucket: self.bucket.clone(),
            id: self.id.clone(),
            root: if self.base_dir.is_empty() {
                self.prefix.clone()
            } else {
                self.base_dir.clone()
            },
            recursive: self.recursive,
            status: ScanStatus::Started,
            data_version: crate::cmd::METACACHE_STREAM_VERSION,
            ..Default::default()
        }
    }
}

impl BucketMetacache {
    pub fn new(bucket: impl Into<String>, _cleanup: bool) -> Self {
        Self {
            bucket: bucket.into(),
            caches: HashMap::new(),
            caches_root: HashMap::new(),
            updated: false,
        }
    }

    pub fn find_cache(&mut self, options: ListPathOptions) -> Metacache {
        if let Some(existing) = self.caches.get_mut(&options.id) {
            existing.last_handout = now_unix();
            return existing.clone();
        }

        if !options.create {
            return Metacache {
                id: options.id,
                bucket: options.bucket,
                status: ScanStatus::None,
                ..Default::default()
            };
        }

        let created = options.new_metacache();
        self.caches_root
            .entry(created.root.clone())
            .or_default()
            .push(created.id.clone());
        self.caches.insert(created.id.clone(), created.clone());
        self.updated = true;
        created
    }
}

pub fn new_bucket_metacache(bucket: impl Into<String>, cleanup: bool) -> BucketMetacache {
    BucketMetacache::new(bucket, cleanup)
}
