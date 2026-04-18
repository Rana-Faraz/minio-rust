use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ServerSystemConfig {
    pub n_endpoints: i32,
    pub cmd_lines: Option<Vec<String>>,
    pub minio_env: Option<BTreeMap<String, String>>,
    pub checksum: String,
}
impl_msg_codec!(ServerSystemConfig);
