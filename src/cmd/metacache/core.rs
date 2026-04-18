use std::io::{Read, Write};

use crate::cmd::{marshal_named, unmarshal_named};
use serde::{Deserialize, Serialize};

pub const METACACHE_STREAM_VERSION: u8 = 2;
pub const METACACHE_MAX_RUNNING_AGE_SECS: i64 = 60;
pub const METACACHE_MAX_CLIENT_WAIT_SECS: i64 = 3 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ScanStatus {
    #[default]
    None,
    Started,
    Success,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Metacache {
    pub ended: i64,
    pub started: i64,
    pub last_handout: i64,
    pub last_update: i64,
    pub bucket: String,
    pub filter: String,
    pub id: String,
    pub error: String,
    pub root: String,
    pub file_not_found: bool,
    pub status: ScanStatus,
    pub recursive: bool,
    pub data_version: u8,
}

impl_msg_codec!(Metacache);

impl Metacache {
    pub fn finished(&self) -> bool {
        self.ended != 0
    }

    pub fn worth_keeping_at(&self, now: i64) -> bool {
        if !self.finished() && now - self.last_update > METACACHE_MAX_RUNNING_AGE_SECS {
            return false;
        }
        if self.finished() && now - self.last_handout > 5 * METACACHE_MAX_CLIENT_WAIT_SECS {
            return false;
        }
        if matches!(self.status, ScanStatus::Error | ScanStatus::None) {
            return now - self.last_update <= 5 * 60;
        }
        true
    }
}

pub fn base_dir_from_prefix(prefix: &str) -> String {
    if prefix == "/" {
        return String::new();
    }
    let trimmed = prefix.trim_start_matches("./").trim_start_matches('/');
    if !trimmed.contains('/') {
        return String::new();
    }
    if trimmed.ends_with('/') {
        return trimmed.to_string();
    }
    match trimmed.rsplit_once('/') {
        Some((base, _)) if !base.is_empty() => format!("{base}/"),
        _ => String::new(),
    }
}
