use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HealingTracker {
    pub id: String,
    pub pool_index: i32,
    pub set_index: i32,
    pub disk_index: i32,
    pub path: String,
    pub endpoint: String,
    pub started: i64,
    pub last_update: i64,
    pub objects_total_count: u64,
    pub objects_total_size: u64,
    pub items_healed: u64,
    pub items_failed: u64,
    pub bytes_done: u64,
    pub bytes_failed: u64,
    pub bucket: String,
    pub object: String,
    pub resume_items_healed: u64,
    pub resume_items_failed: u64,
    pub resume_items_skipped: u64,
    pub resume_bytes_done: u64,
    pub resume_bytes_failed: u64,
    pub resume_bytes_skipped: u64,
    pub queued_buckets: Option<Vec<String>>,
    pub healed_buckets: Option<Vec<String>>,
    pub heal_id: String,
    pub items_skipped: u64,
    pub bytes_skipped: u64,
    pub retry_attempts: u64,
    pub finished: bool,
}
impl_msg_codec!(HealingTracker);
