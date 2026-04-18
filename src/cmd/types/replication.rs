use super::super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicateTargetDecision {
    pub replicate: bool,
    pub synchronous: bool,
    pub arn: String,
    pub id: String,
}
impl_msg_codec!(ReplicateTargetDecision);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicateDecision {
    pub targets_map: Option<BTreeMap<String, ReplicateTargetDecision>>,
}
impl_msg_codec!(ReplicateDecision);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResyncTargetDecision {
    pub replicate: bool,
    pub reset_id: String,
    pub reset_before_date: i64,
}
impl_msg_codec!(ResyncTargetDecision);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResyncDecision {
    pub targets: Option<BTreeMap<String, ResyncTargetDecision>>,
}
impl_msg_codec!(ResyncDecision);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationState {
    pub replica_timestamp: i64,
    pub replica_status: String,
    pub delete_marker: bool,
    pub replication_timestamp: i64,
    pub replication_status_internal: String,
    pub version_purge_status_internal: String,
    pub replicate_decision_str: String,
    pub targets: Option<BTreeMap<String, String>>,
    pub purge_targets: Option<BTreeMap<String, String>>,
    pub reset_statuses_map: Option<BTreeMap<String, String>>,
}
impl_msg_codec!(ReplicationState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FileInfo {
    pub volume: String,
    pub name: String,
    pub version_id: String,
    pub is_latest: bool,
    pub deleted: bool,
    pub transition_status: String,
    pub transitioned_obj_name: String,
    pub transition_tier: String,
    pub transition_version_id: String,
    pub expire_restored: bool,
    pub data_dir: String,
    pub xlv1: bool,
    pub mod_time: i64,
    pub size: i64,
    pub mode: u32,
    pub written_by_version: u64,
    pub metadata: Option<BTreeMap<String, String>>,
    pub parts: Option<Vec<ObjectPartInfo>>,
    pub erasure: ErasureInfo,
    pub mark_deleted: bool,
    pub replication_state: ReplicationState,
    pub data: Option<Vec<u8>>,
    pub num_versions: i32,
    pub successor_mod_time: i64,
    pub fresh: bool,
    pub idx: i32,
    pub checksum: Option<Vec<u8>>,
    pub versioned: bool,
}
impl_msg_codec!(FileInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FilesInfo {
    pub files: Option<Vec<FileInfo>>,
    pub is_truncated: bool,
}
impl_msg_codec!(FilesInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FileInfoVersions {
    pub volume: String,
    pub name: String,
    pub latest_mod_time: i64,
    pub versions: Option<Vec<FileInfo>>,
    pub free_versions: Option<Vec<FileInfo>>,
}
impl_msg_codec!(FileInfoVersions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawFileInfo {
    pub buf: Option<Vec<u8>>,
}
impl_msg_codec!(RawFileInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadMultipleReq {
    pub bucket: String,
    pub prefix: String,
    pub files: Option<Vec<String>>,
    pub max_size: i64,
    pub metadata_only: bool,
    pub abort_on404: bool,
    pub max_results: i32,
}
impl_msg_codec!(ReadMultipleReq);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadMultipleResp {
    pub bucket: String,
    pub prefix: String,
    pub file: String,
    pub exists: bool,
    pub error: String,
    pub data: Option<Vec<u8>>,
    pub modtime: i64,
}
impl_msg_codec!(ReadMultipleResp);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteVersionHandlerParams {
    pub disk_id: String,
    pub volume: String,
    pub file_path: String,
    pub force_del_marker: bool,
    pub opts: DeleteOptions,
    pub fi: FileInfo,
}
impl_msg_codec!(DeleteVersionHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetadataHandlerParams {
    pub disk_id: String,
    pub volume: String,
    pub orig_volume: String,
    pub file_path: String,
    pub update_opts: UpdateMetadataOpts,
    pub fi: FileInfo,
}
impl_msg_codec!(MetadataHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpdateMetadataOpts {
    pub no_persistence: bool,
}
impl_msg_codec!(UpdateMetadataOpts);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CheckPartsHandlerParams {
    pub disk_id: String,
    pub volume: String,
    pub file_path: String,
    pub fi: FileInfo,
}
impl_msg_codec!(CheckPartsHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteFileHandlerParams {
    pub disk_id: String,
    pub volume: String,
    pub file_path: String,
    pub opts: DeleteOptions,
}
impl_msg_codec!(DeleteFileHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenameDataHandlerParams {
    pub disk_id: String,
    pub src_volume: String,
    pub src_path: String,
    pub dst_volume: String,
    pub dst_path: String,
    pub fi: FileInfo,
    pub opts: RenameOptions,
}
impl_msg_codec!(RenameDataHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenameDataInlineHandlerParams {
    pub params: RenameDataHandlerParams,
}
impl_msg_codec!(RenameDataInlineHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenameFileHandlerParams {
    pub disk_id: String,
    pub src_volume: String,
    pub src_file_path: String,
    pub dst_volume: String,
    pub dst_file_path: String,
}
impl_msg_codec!(RenameFileHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenamePartHandlerParams {
    pub disk_id: String,
    pub src_volume: String,
    pub src_file_path: String,
    pub dst_volume: String,
    pub dst_file_path: String,
    pub meta: Option<Vec<u8>>,
    pub skip_parent: String,
}
impl_msg_codec!(RenamePartHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadAllHandlerParams {
    pub disk_id: String,
    pub volume: String,
    pub file_path: String,
}
impl_msg_codec!(ReadAllHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WriteAllHandlerParams {
    pub disk_id: String,
    pub volume: String,
    pub file_path: String,
    pub buf: Option<Vec<u8>>,
}
impl_msg_codec!(WriteAllHandlerParams);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenameDataResp {
    pub sign: Option<Vec<u8>>,
    pub old_data_dir: String,
}
impl_msg_codec!(RenameDataResp);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CheckPartsResp {
    pub results: Option<Vec<i32>>,
}
impl_msg_codec!(CheckPartsResp);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LocalDiskIDs {
    pub ids: Option<Vec<String>>,
}
impl_msg_codec!(LocalDiskIDs);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListDirResult {
    pub entries: Option<Vec<String>>,
}
impl_msg_codec!(ListDirResult);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadPartsReq {
    pub paths: Option<Vec<String>>,
}
impl_msg_codec!(ReadPartsReq);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReadPartsResp {
    pub infos: Option<Vec<ObjectPartInfo>>,
}
impl_msg_codec!(ReadPartsResp);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteBulkReq {
    pub paths: Option<Vec<String>>,
}
impl_msg_codec!(DeleteBulkReq);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteVersionsErrsResp {
    pub errs: Option<Vec<String>>,
}
impl_msg_codec!(DeleteVersionsErrsResp);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BucketReplicationResyncStatus {
    pub version: i32,
    pub targets_map: Option<BTreeMap<String, TargetReplicationResyncStatus>>,
    pub id: i32,
    pub last_update: i64,
}
impl_msg_codec!(BucketReplicationResyncStatus);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MRFReplicateEntry {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub retry_count: i32,
    pub size: i64,
}
impl_msg_codec!(MRFReplicateEntry);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MRFReplicateEntries {
    pub entries: Option<BTreeMap<String, MRFReplicateEntry>>,
    pub version: i32,
}
impl_msg_codec!(MRFReplicateEntries);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResyncTarget {
    pub arn: String,
    pub reset_id: String,
    pub start_time: i64,
    pub end_time: i64,
    pub resync_status: String,
    pub replicated_size: i64,
    pub failed_size: i64,
    pub failed_count: i64,
    pub replicated_count: i64,
    pub bucket: String,
    pub object: String,
}
impl_msg_codec!(ResyncTarget);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResyncTargetsInfo {
    pub targets: Option<Vec<ResyncTarget>>,
}
impl_msg_codec!(ResyncTargetsInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TargetReplicationResyncStatus {
    pub start_time: i64,
    pub last_update: i64,
    pub resync_id: String,
    pub resync_before_date: i64,
    pub resync_status: i32,
    pub failed_size: i64,
    pub failed_count: i64,
    pub replicated_size: i64,
    pub replicated_count: i64,
    pub bucket: String,
    pub object: String,
}
impl_msg_codec!(TargetReplicationResyncStatus);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SiteResyncStatus {
    pub version: i32,
    pub status: i32,
    pub depl_id: String,
    pub bucket_statuses: Option<BTreeMap<String, i32>>,
    pub tot_buckets: i32,
    pub target_replication_resync_status: TargetReplicationResyncStatus,
}
impl_msg_codec!(SiteResyncStatus);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ActiveWorkerStat {
    pub curr: i32,
    pub avg: f32,
    pub max: i32,
}
impl_msg_codec!(ActiveWorkerStat);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct QStat {
    pub count: f64,
    pub bytes: f64,
}
impl_msg_codec!(QStat);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct InQueueMetric {
    pub curr: QStat,
    pub avg: QStat,
    pub max: QStat,
}
impl_msg_codec!(InQueueMetric);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct InQueueStats {
    pub now_bytes: i64,
    pub now_count: i64,
    pub curr: InQueueMetric,
}
impl_msg_codec!(InQueueStats);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReplicationMRFStats {
    pub last_failed_count: u64,
    pub total_dropped_count: u64,
    pub total_dropped_bytes: u64,
}
impl_msg_codec!(ReplicationMRFStats);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SMA {
    pub buf: Option<Vec<f64>>,
    pub window: i32,
    pub idx: i32,
    pub c_avg: f64,
    pub prev_sma: f64,
    pub filled_buf: bool,
}
impl_msg_codec!(SMA);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct XferStats {
    pub curr: f64,
    pub avg: f64,
    pub peak: f64,
    pub n: i64,
    pub sma: Option<SMA>,
}
impl_msg_codec!(XferStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProxyMetric {
    pub put_tag_total: u64,
    pub get_tag_total: u64,
    pub rmv_tag_total: u64,
    pub get_total: u64,
    pub head_total: u64,
    pub put_tag_failed_total: u64,
    pub get_tag_failed_total: u64,
    pub rmv_tag_failed_total: u64,
    pub get_failed_total: u64,
    pub head_failed_total: u64,
}
impl_msg_codec!(ProxyMetric);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BucketReplicationStat {
    pub replicated_size: i64,
    pub replica_size: i64,
    pub fail_stats_total: i64,
    pub failed_total: i64,
    pub replicated_count: i64,
    pub latency: ReplicationLatency,
    pub bandwidth_limit_in_bytes_per_second: i64,
    pub current_bandwidth_in_bytes_per_second: String,
    pub xfer_rate_lrg: Option<XferStats>,
    pub xfer_rate_sml: Option<XferStats>,
    pub pending_size: i64,
    pub failed_size: i64,
    pub pending_count: i64,
    pub failed_count: i64,
}
impl_msg_codec!(BucketReplicationStat);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BucketReplicationStats {
    pub stats: Option<BTreeMap<String, BucketReplicationStat>>,
    pub replicated_size: i64,
    pub replica_size: i64,
    pub failed_total: i64,
    pub replicated_count: i64,
    pub replica_count: i64,
    pub q_stat: InQueueMetric,
    pub pending_size: i64,
    pub failed_size: i64,
    pub pending_count: i64,
    pub failed_count: i64,
}
impl_msg_codec!(BucketReplicationStats);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BucketStats {
    pub uptime: i64,
    pub replication_stats: BucketReplicationStats,
    pub queue_stats: ReplicationQueueStats,
    pub proxy_stats: ProxyMetric,
}
impl_msg_codec!(BucketStats);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BucketStatsMap {
    pub stats: Option<BTreeMap<String, BucketStats>>,
    pub timestamp: i64,
}
impl_msg_codec!(BucketStatsMap);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BucketStatsAccElem {
    pub total: i64,
    pub size: i64,
    pub n: i64,
}
impl_msg_codec!(BucketStatsAccElem);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationLastMinute {
    pub last_minute: BucketStatsAccElem,
}
impl_msg_codec!(ReplicationLastMinute);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationLastHour {
    pub totals: Option<Vec<BucketStatsAccElem>>,
    pub last_min: i64,
}
impl_msg_codec!(ReplicationLastHour);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationLatency {
    pub upload_histogram: Option<BTreeMap<String, u64>>,
}
impl_msg_codec!(ReplicationLatency);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReplQNodeStats {
    pub node_name: String,
    pub uptime: i64,
    pub active_workers: ActiveWorkerStat,
    pub xfer_stats: Option<BTreeMap<String, XferStats>>,
    pub tgt_xfer_stats: Option<BTreeMap<String, BTreeMap<String, XferStats>>>,
    pub q_stats: InQueueMetric,
    pub mrf_stats: ReplicationMRFStats,
}
impl_msg_codec!(ReplQNodeStats);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ReplicationQueueStats {
    pub nodes: Option<Vec<ReplQNodeStats>>,
    pub uptime: i64,
}
impl_msg_codec!(ReplicationQueueStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TierStats {
    pub total_size: u64,
    pub num_versions: i32,
    pub num_objects: i32,
}
impl_msg_codec!(TierStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastDayTierStats {
    pub bins: Vec<TierStats>,
    pub updated_at: i64,
}
impl Default for LastDayTierStats {
    fn default() -> Self {
        Self {
            bins: vec![TierStats::default(); 24],
            updated_at: 0,
        }
    }
}
impl_msg_codec!(LastDayTierStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct DailyAllTierStats(pub Option<BTreeMap<String, LastDayTierStats>>);
impl_msg_codec!(DailyAllTierStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AllTierStats {
    pub tiers: Option<BTreeMap<String, TierStats>>,
}
impl_msg_codec!(AllTierStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct SizeHistogram(pub Option<Vec<u64>>);
impl_msg_codec!(SizeHistogram);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct SizeHistogramV1(pub Option<Vec<u64>>);
impl_msg_codec!(SizeHistogramV1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct VersionsHistogram(pub Option<Vec<u64>>);
impl_msg_codec!(VersionsHistogram);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DataUsageEntry {
    pub children: Option<BTreeMap<String, bool>>,
    pub size: i64,
    pub objects: u64,
    pub versions: u64,
    pub delete_markers: u64,
    pub obj_sizes: SizeHistogram,
    pub obj_versions: VersionsHistogram,
    pub all_tier_stats: Option<AllTierStats>,
    pub compacted: bool,
}
impl_msg_codec!(DataUsageEntry);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DataUsageCacheInfo {
    pub name: String,
    pub next_cycle: u32,
    pub last_update: i64,
    pub skip_healing: bool,
}
impl_msg_codec!(DataUsageCacheInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DataUsageCache {
    pub info: DataUsageCacheInfo,
    pub cache: Option<BTreeMap<String, DataUsageEntry>>,
}
impl_msg_codec!(DataUsageCache);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CurrentScannerCycle {
    pub current: u64,
    pub next: u64,
    pub started: i64,
    pub cycle_completed: Option<Vec<i64>>,
}
impl_msg_codec!(CurrentScannerCycle);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RStat {
    pub count: i64,
    pub bytes: i64,
}
impl_msg_codec!(RStat);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RTimedMetrics {
    pub last_hour: ReplicationLastHour,
    pub since_uptime: RStat,
    pub last_minute: ReplicationLastMinute,
    pub err_counts: Option<BTreeMap<String, i32>>,
}
impl_msg_codec!(RTimedMetrics);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct LatencyStat {
    pub curr: f64,
    pub avg: f64,
    pub max: f64,
}
impl_msg_codec!(LatencyStat);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SRMetric {
    pub deployment_id: String,
    pub endpoint: String,
    pub total_downtime: i64,
    pub last_online: i64,
    pub online: bool,
    pub latency: LatencyStat,
    pub replicated_size: i64,
    pub replicated_count: i64,
    pub failed: RTimedMetrics,
    pub xfer_stats: Option<BTreeMap<String, XferStats>>,
}
impl_msg_codec!(SRMetric);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SRMetricsSummary {
    pub active_workers: ActiveWorkerStat,
    pub replica_size: i64,
    pub replica_count: i64,
    pub queued: InQueueMetric,
    pub proxied: ProxyMetric,
    pub metrics: Option<BTreeMap<String, SRMetric>>,
    pub uptime: i64,
}
impl_msg_codec!(SRMetricsSummary);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SRStatus {
    pub replicated_size: i64,
    pub failed: RTimedMetrics,
    pub replicated_count: i64,
    pub latency: ReplicationLatency,
    pub xfer_rate_lrg: Option<XferStats>,
    pub xfer_rate_sml: Option<XferStats>,
    pub endpoint: String,
    pub secure: bool,
}
impl_msg_codec!(SRStatus);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SRStats {
    pub replica_size: i64,
    pub replica_count: i64,
    pub m: Option<BTreeMap<String, SRStatus>>,
}
impl_msg_codec!(SRStats);
