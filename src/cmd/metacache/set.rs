use std::io::{Read, Write};

use crate::cmd::{marshal_named, unmarshal_named};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListPathOptions {
    pub id: String,
    pub bucket: String,
    pub base_dir: String,
    pub prefix: String,
    pub filter_prefix: String,
    pub marker: String,
    pub limit: i32,
    pub ask_disks: String,
    pub incl_deleted: bool,
    pub recursive: bool,
    pub separator: String,
    pub create: bool,
    pub include_directories: bool,
    pub transient: bool,
    pub versioned: bool,
    pub v1: bool,
    pub stop_disk_at_limit: bool,
    pub pool: i32,
    pub set: i32,
}

impl_msg_codec!(ListPathOptions);
