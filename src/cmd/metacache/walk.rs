use std::io::{Read, Write};

use crate::cmd::{marshal_named, unmarshal_named};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WalkDirOptions {
    pub bucket: String,
    pub base_dir: String,
    pub recursive: bool,
    pub report_not_found: bool,
    pub filter_prefix: String,
    pub forward_to: String,
    pub limit: i32,
    pub disk_id: String,
}

impl_msg_codec!(WalkDirOptions);
