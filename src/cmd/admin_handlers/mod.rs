use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use serde::Deserialize;

use crate::cmd::{
    check_admin_request_auth, current_site, is_minio_meta_bucket_name, is_valid_bucket_name,
    is_valid_object_prefix, ApiErrorCode, Credentials, LockRequesterInfo, TestRequest,
    ERR_DISK_NOT_FOUND, ERR_ERASURE_WRITE_QUORUM,
};

pub const MGMT_BUCKET: &str = "bucket";
pub const MGMT_PREFIX: &str = "prefix";
pub const MGMT_CLIENT_TOKEN: &str = "clientToken";
pub const MGMT_FORCE_START: &str = "forceStart";
pub const MGMT_FORCE_STOP: &str = "forceStop";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminApiErrorCode {
    None,
    Api(ApiErrorCode),
    AdminConfigNoQuorum,
    InvalidRequest,
    HealMissingBucket,
    RequestBodyParse,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
pub struct HealOpts {
    pub recursive: bool,
    #[serde(rename = "dryRun")]
    pub dry_run: bool,
    pub remove: bool,
    #[serde(rename = "scanMode")]
    pub scan_mode: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HealInitParams {
    pub bucket: String,
    pub obj_prefix: String,
    pub client_token: String,
    pub force_start: bool,
    pub force_stop: bool,
    pub hs: HealOpts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminLockEntry {
    pub timestamp: i64,
    pub resource: String,
    pub server_list: Vec<String>,
    pub source: String,
    pub owner: String,
    pub id: String,
    pub quorum: i32,
    pub lock_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerLocks {
    pub addr: String,
    pub locks: BTreeMap<String, Vec<LockRequesterInfo>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceSignal {
    Restart,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceResult {
    pub action: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminInfoMessage {
    pub region: String,
}

#[derive(Debug, Clone)]
pub struct AdminHandlers {
    active: Credentials,
    signals: Arc<Mutex<Vec<ServiceSignal>>>,
}

impl AdminHandlers {
    pub fn new(active: Credentials) -> Self {
        Self {
            active,
            signals: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn restart_handler(&self, req: &TestRequest) -> Result<ServiceResult, ApiErrorCode> {
        let (_, err) = check_admin_request_auth(req, &self.active);
        if err != ApiErrorCode::None {
            return Err(err);
        }

        if req.method != "POST" {
            return Err(ApiErrorCode::InvalidQueryParams);
        }

        let action = req.query_value("action").unwrap_or_default();
        let service_type = req.query_value("type").unwrap_or_default();
        if action != "restart" || service_type != "2" {
            return Err(ApiErrorCode::InvalidQueryParams);
        }

        self.signals
            .lock()
            .expect("signals")
            .push(ServiceSignal::Restart);
        Ok(ServiceResult { action })
    }

    pub fn server_info(&self, req: &TestRequest) -> Result<AdminInfoMessage, ApiErrorCode> {
        let (_, err) = check_admin_request_auth(req, &self.active);
        if err != ApiErrorCode::None {
            return Err(err);
        }

        if req.method != "GET" || req.query_value("info").is_none() {
            return Err(ApiErrorCode::InvalidQueryParams);
        }

        let site = current_site();
        Ok(AdminInfoMessage {
            region: if site.region().is_empty() {
                crate::cmd::GLOBAL_MINIO_DEFAULT_REGION.to_string()
            } else {
                site.region().to_string()
            },
        })
    }

    pub fn seen_signals(&self) -> Vec<ServiceSignal> {
        self.signals.lock().expect("signals").clone()
    }
}

fn map_string_error(err: &str) -> ApiErrorCode {
    match err {
        ERR_DISK_NOT_FOUND => ApiErrorCode::InternalError,
        _ => ApiErrorCode::InternalError,
    }
}

pub fn to_admin_api_err_code(err: Option<&str>) -> AdminApiErrorCode {
    match err {
        None => AdminApiErrorCode::None,
        Some(ERR_ERASURE_WRITE_QUORUM) => AdminApiErrorCode::AdminConfigNoQuorum,
        Some(err) => AdminApiErrorCode::Api(map_string_error(err)),
    }
}

pub fn extract_heal_init_params(
    vars: &BTreeMap<String, String>,
    q_params: &BTreeMap<String, Vec<String>>,
    body: &[u8],
) -> (HealInitParams, AdminApiErrorCode) {
    let mut hip = HealInitParams {
        bucket: vars.get(MGMT_BUCKET).cloned().unwrap_or_default(),
        obj_prefix: vars.get(MGMT_PREFIX).cloned().unwrap_or_default(),
        ..HealInitParams::default()
    };

    if hip.bucket.is_empty() {
        if !hip.obj_prefix.is_empty() {
            return (hip, AdminApiErrorCode::HealMissingBucket);
        }
    } else if !is_valid_bucket_name(&hip.bucket) || is_minio_meta_bucket_name(&hip.bucket) {
        return (hip, AdminApiErrorCode::Api(ApiErrorCode::InvalidBucketName));
    }

    if !is_valid_object_prefix(&hip.obj_prefix) {
        return (hip, AdminApiErrorCode::Api(ApiErrorCode::InvalidObjectName));
    }

    if let Some(values) = q_params.get(MGMT_CLIENT_TOKEN) {
        hip.client_token = values.first().cloned().unwrap_or_default();
    }
    hip.force_start = q_params.contains_key(MGMT_FORCE_START);
    hip.force_stop = q_params.contains_key(MGMT_FORCE_STOP);

    if (hip.force_start && hip.force_stop)
        || (!hip.client_token.is_empty() && (hip.force_start || hip.force_stop))
    {
        return (hip, AdminApiErrorCode::InvalidRequest);
    }

    if hip.client_token.is_empty() {
        match serde_json::from_slice::<HealOpts>(body) {
            Ok(parsed) => hip.hs = parsed,
            Err(_) => return (hip, AdminApiErrorCode::RequestBodyParse),
        }
    }

    (hip, AdminApiErrorCode::None)
}

fn lri_to_lock_entry(entry: &LockRequesterInfo, resource: &str, server: &str) -> AdminLockEntry {
    AdminLockEntry {
        timestamp: entry.timestamp,
        resource: resource.to_string(),
        server_list: vec![server.to_string()],
        source: entry.source.clone(),
        owner: entry.owner.clone(),
        id: entry.uid.clone(),
        quorum: entry.quorum,
        lock_type: if entry.writer {
            "WRITE".to_string()
        } else {
            "READ".to_string()
        },
    }
}

pub fn top_lock_entries(peer_locks: &[PeerLocks], stale: bool) -> Vec<AdminLockEntry> {
    let _now = Utc::now();
    let mut entry_map = BTreeMap::<String, AdminLockEntry>::new();
    for peer_lock in peer_locks {
        for (resource, infos) in &peer_lock.locks {
            for info in infos {
                let key = format!("{}/{}", info.name, info.uid);
                if let Some(existing) = entry_map.get_mut(&key) {
                    existing.server_list.push(peer_lock.addr.clone());
                } else {
                    entry_map.insert(key, lri_to_lock_entry(info, resource, &peer_lock.addr));
                }
            }
        }
    }

    let mut entries = entry_map
        .into_values()
        .filter(|entry| stale || entry.server_list.len() as i32 >= entry.quorum)
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.resource
            .cmp(&right.resource)
            .then(left.id.cmp(&right.id))
    });
    entries
}
