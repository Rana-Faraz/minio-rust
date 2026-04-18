use super::super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalanceInfo {
    pub start_time: i64,
    pub end_time: i64,
    pub status: u8,
}
impl_msg_codec!(RebalanceInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalanceStats {
    pub init_free_space: u64,
    pub init_capacity: u64,
    pub buckets: Option<Vec<String>>,
    pub rebalanced_buckets: Option<Vec<String>>,
    pub bucket: String,
    pub object: String,
    pub num_objects: u64,
    pub num_versions: u64,
    pub bytes: u64,
    pub participating: bool,
    pub info: RebalanceInfo,
}
impl_msg_codec!(RebalanceStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalanceMeta {
    pub stopped_at: i64,
    pub id: String,
    pub percent_free_goal: String,
    pub pool_stats: Option<Vec<RebalanceStats>>,
}
impl_msg_codec!(RebalanceMeta);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RebalanceMetrics {}
impl_msg_codec!(RebalanceMetrics);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Rstats(pub Option<Vec<RebalanceStats>>);
impl_msg_codec!(Rstats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PoolDecommissionInfo {
    pub start_time: i64,
    pub start_size: i64,
    pub total_size: i64,
    pub current_size: i64,
    pub complete: bool,
    pub failed: bool,
    pub canceled: bool,
    pub queued_buckets: Option<Vec<String>>,
    pub decommissioned_buckets: Option<Vec<String>>,
    pub bucket: String,
    pub prefix: String,
    pub object: String,
    pub items_decommissioned: i64,
    pub items_decommission_failed: i64,
    pub bytes_done: i64,
    pub bytes_failed: i64,
}
impl_msg_codec!(PoolDecommissionInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PoolStatus {
    pub id: i32,
    pub cmd_line: String,
    pub last_update: i64,
    pub decommission: Option<PoolDecommissionInfo>,
}
impl_msg_codec!(PoolStatus);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DecomError {
    pub err: String,
}
impl_msg_codec!(DecomError);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PoolMeta {
    pub version: i32,
    pub pools: Option<Vec<PoolStatus>>,
}
impl_msg_codec!(PoolMeta);

impl PoolMeta {
    pub fn validate(&self, server_pools: &[PoolEndpoints]) -> Result<bool, String> {
        if self.version <= 0 {
            return Err("invalid pool meta version".to_string());
        }

        let pools = self
            .pools
            .as_ref()
            .ok_or_else(|| "pool meta is missing pools".to_string())?;

        if pools.len() != server_pools.len() {
            return Err("pool meta pool count mismatch".to_string());
        }

        let mut updated = false;
        for (idx, (pool, server_pool)) in pools.iter().zip(server_pools.iter()).enumerate() {
            if pool.id != idx as i32 {
                updated = true;
            }
            if pool.cmd_line != server_pool.cmd_line {
                updated = true;
            }
        }

        Ok(updated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PoolSpaceInfo {
    pub free: i64,
    pub total: i64,
    pub used: i64,
}
impl_msg_codec!(PoolSpaceInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LockRequesterInfo {
    pub name: String,
    pub writer: bool,
    pub uid: String,
    pub timestamp: i64,
    pub time_last_refresh: i64,
    pub source: String,
    pub group: bool,
    pub owner: String,
    pub quorum: i32,
}
impl_msg_codec!(LockRequesterInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LockStats {
    pub total: i32,
    pub writes: i32,
    pub reads: i32,
    pub lock_queue: i32,
    pub locks_abandoned: i32,
    pub last_cleanup: Option<i64>,
}
impl_msg_codec!(LockStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct LocalLockMap(pub Option<BTreeMap<String, Vec<LockRequesterInfo>>>);
impl_msg_codec!(LocalLockMap);
