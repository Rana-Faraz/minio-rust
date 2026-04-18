use super::*;
use serde::Serialize;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BaseOptions {}
impl_msg_codec!(BaseOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteOptions {
    pub base: BaseOptions,
    pub recursive: bool,
    pub immediate: bool,
    pub undo_write: bool,
    pub old_data_dir: String,
}
impl_msg_codec!(DeleteOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RenameOptions {
    pub base: BaseOptions,
}
impl_msg_codec!(RenameOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DiskInfoOptions {
    pub disk_id: String,
    pub metrics: bool,
    pub no_op: bool,
}
impl_msg_codec!(DiskInfoOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AccElem {
    pub total: i64,
    pub size: i64,
    pub n: i64,
}
impl_msg_codec!(AccElem);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastMinuteLatency {
    pub totals: Vec<AccElem>,
    pub last_sec: i64,
}
impl Default for LastMinuteLatency {
    fn default() -> Self {
        Self {
            totals: vec![AccElem::default(); 60],
            last_sec: 0,
        }
    }
}
impl_msg_codec!(LastMinuteLatency);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LastMinuteHistogram(pub Vec<LastMinuteLatency>);
impl Default for LastMinuteHistogram {
    fn default() -> Self {
        Self(vec![LastMinuteLatency::default(); 6])
    }
}
impl_msg_codec!(LastMinuteHistogram);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DiskMetrics {
    pub last_minute: Option<BTreeMap<String, AccElem>>,
    pub api_calls: Option<BTreeMap<String, u64>>,
    pub total_waiting: u32,
    pub total_errors_availability: u64,
    pub total_errors_timeout: u64,
    pub total_writes: u64,
    pub total_deletes: u64,
}
impl_msg_codec!(DiskMetrics);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricDescription {
    pub namespace: String,
    pub subsystem: String,
    pub name: String,
    pub help: String,
    pub metric_type: String,
}
impl_msg_codec!(MetricDescription);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MetricV2 {
    pub description: MetricDescription,
    pub static_labels: Option<BTreeMap<String, String>>,
    pub value: f64,
    pub variable_labels: Option<BTreeMap<String, String>>,
    pub histogram_bucket_label: String,
    pub histogram: Option<BTreeMap<String, u64>>,
}
impl_msg_codec!(MetricV2);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricsGroupOpts {
    pub depend_global_object_api: bool,
    pub depend_global_auth_n_plugin: bool,
    pub depend_global_site_replication_sys: bool,
    pub depend_global_notification_sys: bool,
    pub depend_global_kms: bool,
    pub bucket_only: bool,
    pub depend_global_lambda_target_list: bool,
    pub depend_global_iam_sys: bool,
    pub depend_global_lock_server: bool,
    pub depend_global_is_dist_erasure: bool,
    pub depend_global_background_heal_state: bool,
    pub depend_bucket_target_sys: bool,
}
impl_msg_codec!(MetricsGroupOpts);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricsGroupV2 {
    pub cache_interval_nanos: u64,
    pub metrics_group_opts: MetricsGroupOpts,
}
impl_msg_codec!(MetricsGroupV2);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DiskInfo {
    pub total: u64,
    pub free: u64,
    pub used: u64,
    pub used_inodes: u64,
    pub free_inodes: u64,
    pub major: u32,
    pub minor: u32,
    pub nr_requests: u64,
    pub fs_type: String,
    pub root_disk: bool,
    pub healing: bool,
    pub scanning: bool,
    pub endpoint: String,
    pub mount_path: String,
    pub id: String,
    pub rotational: bool,
    pub metrics: DiskMetrics,
    pub error: String,
}
impl_msg_codec!(DiskInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VolInfo {
    pub name: String,
    pub created: i64,
    pub count: i32,
    pub deleted: i64,
}
impl_msg_codec!(VolInfo);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct VolsInfo(pub Option<Vec<VolInfo>>);
impl_msg_codec!(VolsInfo);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestDumpInput {
    pub method: String,
    pub request_uri: String,
    pub host: String,
    pub headers: BTreeMap<String, String>,
}

pub fn is_max_object_size(size: i64) -> bool {
    size > GLOBAL_MAX_OBJECT_SIZE
}

pub fn is_min_allowed_part_size(size: i64) -> bool {
    size >= GLOBAL_MIN_PART_SIZE
}

pub fn is_max_part_id(part_id: i32) -> bool {
    part_id > GLOBAL_MAX_PART_ID
}

pub fn start_profiler(profiler_type: &str) -> Result<(), String> {
    match profiler_type {
        "cpu" | "mem" | "block" | "mutex" | "trace" => Ok(()),
        _ => Err("invalid profiler".to_string()),
    }
}

pub fn check_url(url_str: &str) -> Result<Url, String> {
    if url_str.is_empty() {
        return Err("Address cannot be empty".to_string());
    }
    Url::parse(url_str).map_err(|err| format!("`{url_str}` invalid: {err}"))
}

pub fn dump_request(request: &RequestDumpInput) -> String {
    #[derive(Debug, Serialize)]
    struct DumpRequest<'a> {
        method: &'a str,
        #[serde(rename = "reqURI")]
        request_uri: String,
        header: BTreeMap<String, String>,
    }

    let mut header = request.headers.clone();
    header.insert("host".to_string(), request.host.clone());
    let req = DumpRequest {
        method: &request.method,
        request_uri: request.request_uri.replace('%', "%%"),
        header,
    };
    serde_json::to_string(&req).unwrap_or_else(|_| format!("{req:?}"))
}

pub fn to_s3_etag(etag: &str) -> String {
    let mut etag = canonicalize_etag(etag);
    if !etag.ends_with("-1") {
        etag.push_str("-1");
    }
    etag
}

pub fn is_err_ignored(err: Option<&str>, ignored_errs: &[&str]) -> bool {
    err.is_some_and(|err| ignored_errs.contains(&err))
}

pub fn base_ignored_errs() -> Vec<&'static str> {
    vec![ERR_FAULTY_DISK]
}

pub fn rest_queries(keys: &[&str]) -> Vec<String> {
    let mut out = Vec::with_capacity(keys.len() * 2);
    for key in keys {
        out.push((*key).to_string());
        out.push(format!("{{{key}:.*}}"));
    }
    out
}

pub fn lcp(strs: &[&str], prefix: bool) -> String {
    if strs.is_empty() {
        return String::new();
    }
    let mut xfix = strs[0].to_string();
    if strs.len() == 1 {
        return xfix;
    }
    for value in &strs[1..] {
        if xfix.is_empty() || value.is_empty() {
            return String::new();
        }
        let max_len = xfix.len().min(value.len());
        if prefix {
            let mut end = max_len;
            for i in 0..max_len {
                if xfix.as_bytes()[i] != value.as_bytes()[i] {
                    end = i;
                    break;
                }
            }
            xfix.truncate(end);
        } else {
            let mut start = xfix.len().saturating_sub(max_len);
            for i in 0..max_len {
                let xi = xfix.len() - i - 1;
                let si = value.len() - i - 1;
                if xfix.as_bytes()[xi] != value.as_bytes()[si] {
                    start = xi + 1;
                    break;
                }
                if i + 1 == max_len {
                    start = xfix.len() - max_len;
                }
            }
            xfix = xfix[start..].to_string();
        }
    }
    xfix
}

pub fn get_minio_mode() -> &'static str {
    if GLOBAL_IS_DIST_ERASURE.load(Ordering::SeqCst) {
        GLOBAL_MINIO_MODE_DIST_ERASURE
    } else if GLOBAL_IS_ERASURE.load(Ordering::SeqCst) {
        GLOBAL_MINIO_MODE_ERASURE
    } else if GLOBAL_IS_ERASURE_SD.load(Ordering::SeqCst) {
        GLOBAL_MINIO_MODE_ERASURE_SD
    } else {
        GLOBAL_MINIO_MODE_FS
    }
}
