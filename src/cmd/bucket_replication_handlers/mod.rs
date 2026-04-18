use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cmd::{
    HandlerCredentials, HandlerResponse, LocalObjectLayer, ObjectInfo, ObjectOptions, PutObjReader,
    ReplicationOperation, ReplicationQueueEntry, ReplicationService, RequestAuth, RequestAuthKind,
};
use crate::internal::bucket::replication;
use base64::Engine;
use serde::{Deserialize, Serialize};

const REPLICATION_FILE: &str = ".bucket-replication.xml";
const REPLICATION_RESYNC_FILE: &str = ".bucket-replication-resync.json";
const REPLICATION_REPLICA_STATUS: &str = "x-amz-bucket-replication-status";
const REPLICATION_REPLICA_MARKER: &str = "x-minio-internal-replica";
const REPLICATION_DELETE_MARKER_VERSION_ID: &str = "x-minio-internal-delete-marker-version-id";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationRemoteTarget {
    pub target_id: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BucketReplicationResyncEnqueueSummary {
    pub target: String,
    pub scheduled_count: u64,
    pub scheduled_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BucketReplicationResyncTargetRecord {
    pub arn: String,
    pub resync_id: String,
    pub resync_before_date: i64,
    pub start_time: i64,
    pub last_updated: i64,
    pub status: String,
    pub scheduled_count: u64,
    pub scheduled_bytes: u64,
}

pub fn put_bucket_replication_for_layer(
    layer: &LocalObjectLayer,
    credentials: &HandlerCredentials,
    bucket: &str,
    auth: &RequestAuth,
    xml: &str,
) -> HandlerResponse {
    if !is_authorized(auth, credentials) {
        return error_response(403, "AccessDenied", "access denied");
    }
    if !layer.bucket_exists(bucket).unwrap_or(false) {
        return error_response(404, "NoSuchBucket", "bucket not found");
    }
    if !layer.bucket_versioning_enabled(bucket).unwrap_or(false) {
        return error_response(
            400,
            "ReplicationNeedsVersioningError",
            "bucket versioning must be enabled",
        );
    }

    let config = match replication::parse_config(xml) {
        Ok(config) => config,
        Err(_) => {
            return error_response(400, "MalformedXML", "malformed replication configuration")
        }
    };
    if config.validate(bucket, false).is_err() {
        return error_response(
            400,
            "ReplicationConfigurationError",
            "invalid replication configuration",
        );
    }

    for disk in layer.disk_paths() {
        if !disk.exists() {
            continue;
        }
        let _ = fs::create_dir_all(disk.join(bucket));
        if fs::write(replication_path(disk, bucket), xml.as_bytes()).is_err() {
            return error_response(500, "InternalError", "unable to persist replication");
        }
    }

    HandlerResponse {
        status: 200,
        ..HandlerResponse::default()
    }
}

pub fn get_bucket_replication_for_layer(
    layer: &LocalObjectLayer,
    credentials: &HandlerCredentials,
    bucket: &str,
    auth: &RequestAuth,
) -> HandlerResponse {
    if !is_authorized(auth, credentials) {
        return error_response(403, "AccessDenied", "access denied");
    }
    if !layer.bucket_exists(bucket).unwrap_or(false) {
        return error_response(404, "NoSuchBucket", "bucket not found");
    }

    for disk in layer.disk_paths() {
        let path = replication_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            return xml_response(200, bytes);
        }
    }

    error_response(
        404,
        "ReplicationConfigurationNotFoundError",
        "bucket replication configuration not found",
    )
}

pub fn delete_bucket_replication_for_layer(
    layer: &LocalObjectLayer,
    credentials: &HandlerCredentials,
    bucket: &str,
    auth: &RequestAuth,
) -> HandlerResponse {
    if !is_authorized(auth, credentials) {
        return error_response(403, "AccessDenied", "access denied");
    }
    if !layer.bucket_exists(bucket).unwrap_or(false) {
        return error_response(404, "NoSuchBucket", "bucket not found");
    }

    for disk in layer.disk_paths() {
        let path = replication_path(disk, bucket);
        if path.exists() {
            let _ = fs::remove_file(path);
        }
    }

    HandlerResponse {
        status: 204,
        ..HandlerResponse::default()
    }
}

pub fn read_bucket_replication_config(
    layer: &LocalObjectLayer,
    bucket: &str,
) -> Result<Option<replication::Config>, String> {
    for disk in layer.disk_paths() {
        let path = replication_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            let xml = String::from_utf8(bytes).map_err(|err| err.to_string())?;
            return replication::parse_config(&xml)
                .map(Some)
                .map_err(|err| err.to_string());
        }
    }
    Ok(None)
}

pub fn read_bucket_replication_resync_state(
    layer: &LocalObjectLayer,
    bucket: &str,
) -> Result<BTreeMap<String, BucketReplicationResyncTargetRecord>, String> {
    for disk in layer.disk_paths() {
        let path = replication_resync_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            return serde_json::from_slice(&bytes).map_err(|err| err.to_string());
        }
    }
    Ok(BTreeMap::new())
}

pub fn write_bucket_replication_resync_state(
    layer: &LocalObjectLayer,
    bucket: &str,
    records: &BTreeMap<String, BucketReplicationResyncTargetRecord>,
) -> Result<(), String> {
    for disk in layer.disk_paths() {
        if !disk.exists() {
            continue;
        }
        let bucket_dir = disk.join(bucket);
        let _ = fs::create_dir_all(&bucket_dir);
        let path = replication_resync_path(disk, bucket);
        if records.is_empty() {
            let _ = fs::remove_file(path);
            continue;
        }
        let bytes = serde_json::to_vec_pretty(records).map_err(|err| err.to_string())?;
        fs::write(path, bytes).map_err(|err| err.to_string())?;
    }
    Ok(())
}

pub fn load_all_bucket_replication_resync_records(
    layer: &LocalObjectLayer,
) -> Result<BTreeMap<String, BucketReplicationResyncTargetRecord>, String> {
    let mut out = BTreeMap::new();
    for bucket in layer
        .list_buckets(crate::cmd::BucketOptions::default())?
        .into_iter()
        .map(|bucket| bucket.name)
    {
        let records = read_bucket_replication_resync_state(layer, &bucket)?;
        for (arn, record) in records {
            out.insert(format!("{bucket}\u{1f}{arn}"), record);
        }
    }
    Ok(out)
}

pub fn replicate_object_for_layer(
    layer: &LocalObjectLayer,
    remote_targets: &BTreeMap<String, ReplicationRemoteTarget>,
    replication_service: Option<&ReplicationService>,
    source_bucket: &str,
    object: &str,
    info: &ObjectInfo,
    data: &[u8],
) -> Result<(), String> {
    if info
        .user_defined
        .get(REPLICATION_REPLICA_MARKER)
        .is_some_and(|value| value == "true")
    {
        return Ok(());
    }

    let Some(config) = read_bucket_replication_config(layer, source_bucket)? else {
        return Ok(());
    };
    let opts = replication::ObjectOpts {
        name: object.to_string(),
        op_type: replication::ReplicationType::Object,
        replica: false,
        ..replication::ObjectOpts::default()
    };
    for rule in config.filter_actionable_rules(&opts) {
        let destination = if rule.destination.arn.is_empty() {
            &rule.destination.bucket
        } else {
            &rule.destination.arn
        };
        let Some((target_id, target_bucket)) = destination_target(destination) else {
            continue;
        };
        if let Some(target_id) = target_id.as_ref() {
            if let Some(remote) = remote_targets.get(target_id) {
                if let Err(error) =
                    replicate_object_to_remote(remote, &target_bucket, object, info, data)
                {
                    enqueue_replication_object_failure(
                        replication_service,
                        &rule.destination.arn,
                        source_bucket,
                        object,
                        info,
                        data.len() as u64,
                    );
                    return Err(error);
                }
                continue;
            }
        }
        if target_bucket == source_bucket || !layer.bucket_exists(&target_bucket).unwrap_or(false) {
            continue;
        }

        let mut user_defined = info.user_defined.clone();
        if !info.content_type.is_empty() {
            user_defined
                .entry("content-type".to_string())
                .or_insert_with(|| info.content_type.clone());
        }
        user_defined.insert(
            REPLICATION_REPLICA_STATUS.to_string(),
            "REPLICA".to_string(),
        );
        user_defined.insert(REPLICATION_REPLICA_MARKER.to_string(), "true".to_string());

        let _ = layer.put_object(
            &target_bucket,
            object,
            &PutObjReader {
                data: data.to_vec(),
                declared_size: data.len() as i64,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions {
                user_defined,
                versioned: !info.version_id.is_empty(),
                version_id: info.version_id.clone(),
                ..ObjectOptions::default()
            },
        );
    }
    Ok(())
}

pub fn replicate_delete_for_layer(
    layer: &LocalObjectLayer,
    remote_targets: &BTreeMap<String, ReplicationRemoteTarget>,
    replication_service: Option<&ReplicationService>,
    source_bucket: &str,
    object: &str,
) -> Result<(), String> {
    replicate_delete_info_for_layer(
        layer,
        remote_targets,
        replication_service,
        source_bucket,
        object,
        &ObjectInfo {
            bucket: source_bucket.to_string(),
            name: object.to_string(),
            ..ObjectInfo::default()
        },
    )
}

pub fn replicate_delete_info_for_layer(
    layer: &LocalObjectLayer,
    remote_targets: &BTreeMap<String, ReplicationRemoteTarget>,
    replication_service: Option<&ReplicationService>,
    source_bucket: &str,
    object: &str,
    info: &ObjectInfo,
) -> Result<(), String> {
    let Some(config) = read_bucket_replication_config(layer, source_bucket)? else {
        return Ok(());
    };
    let opts = replication::ObjectOpts {
        name: object.to_string(),
        op_type: replication::ReplicationType::Delete,
        replica: false,
        ..replication::ObjectOpts::default()
    };
    for rule in config.filter_actionable_rules(&opts) {
        if rule.delete_marker_replication.status != replication::ENABLED
            && rule.delete_replication.status != replication::ENABLED
        {
            continue;
        }
        if info.delete_marker && rule.delete_marker_replication.status != replication::ENABLED {
            continue;
        }
        if !info.delete_marker
            && !info.version_id.is_empty()
            && rule.delete_replication.status != replication::ENABLED
        {
            continue;
        }
        let destination = if rule.destination.arn.is_empty() {
            &rule.destination.bucket
        } else {
            &rule.destination.arn
        };
        let Some((target_id, target_bucket)) = destination_target(destination) else {
            continue;
        };
        if let Some(target_id) = target_id.as_ref() {
            if let Some(remote) = remote_targets.get(target_id) {
                if let Err(error) = replicate_delete_to_remote(remote, &target_bucket, object, info)
                {
                    enqueue_replication_delete_failure(
                        replication_service,
                        &rule.destination.arn,
                        source_bucket,
                        object,
                        info,
                    );
                    return Err(error);
                }
                continue;
            }
        }
        if target_bucket == source_bucket || !layer.bucket_exists(&target_bucket).unwrap_or(false) {
            continue;
        }
        let mut delete_opts = ObjectOptions::default();
        if info.delete_marker {
            delete_opts.versioned = true;
            delete_opts.user_defined.insert(
                REPLICATION_DELETE_MARKER_VERSION_ID.to_string(),
                info.version_id.clone(),
            );
        } else if !info.version_id.is_empty() {
            delete_opts.versioned = true;
            delete_opts.version_id = info.version_id.clone();
        }
        let _ = layer.delete_object(&target_bucket, object, delete_opts);
    }
    Ok(())
}

pub fn retry_replication_entry_for_layer(
    layer: &LocalObjectLayer,
    remote_targets: &BTreeMap<String, ReplicationRemoteTarget>,
    entry: &ReplicationQueueEntry,
) -> Result<(), String> {
    let Some((target_id, target_bucket)) = destination_target(&entry.target_arn) else {
        return Err("invalid replication target arn".to_string());
    };
    match entry.operation {
        ReplicationOperation::DeleteObject => {
            let mut info = ObjectInfo {
                bucket: entry.bucket.clone(),
                name: entry.object.clone(),
                version_id: entry.version_id.clone(),
                ..ObjectInfo::default()
            };
            if entry
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("delete-marker"))
                .is_some_and(|value| value == "true")
            {
                info.delete_marker = true;
            }
            if let Some(target_id) = target_id.as_ref() {
                let remote = remote_targets
                    .get(target_id)
                    .ok_or_else(|| "replication target not configured".to_string())?;
                replicate_delete_to_remote(remote, &target_bucket, &entry.object, &info)
            } else {
                let mut delete_opts = ObjectOptions::default();
                if info.delete_marker {
                    delete_opts.versioned = true;
                    delete_opts.user_defined.insert(
                        REPLICATION_DELETE_MARKER_VERSION_ID.to_string(),
                        info.version_id.clone(),
                    );
                } else if !info.version_id.is_empty() {
                    delete_opts.versioned = true;
                    delete_opts.version_id = info.version_id.clone();
                }
                layer.delete_object(&target_bucket, &entry.object, delete_opts)?;
                Ok(())
            }
        }
        _ => {
            let info =
                layer.get_object_info_version(&entry.bucket, &entry.object, &entry.version_id)?;
            let data = layer.get_object_version(&entry.bucket, &entry.object, &entry.version_id)?;
            if let Some(target_id) = target_id.as_ref() {
                let remote = remote_targets
                    .get(target_id)
                    .ok_or_else(|| "replication target not configured".to_string())?;
                replicate_object_to_remote(remote, &target_bucket, &entry.object, &info, &data)
            } else {
                let mut user_defined = info.user_defined.clone();
                if !info.content_type.is_empty() {
                    user_defined
                        .entry("content-type".to_string())
                        .or_insert_with(|| info.content_type.clone());
                }
                user_defined.insert(
                    REPLICATION_REPLICA_STATUS.to_string(),
                    "REPLICA".to_string(),
                );
                user_defined.insert(REPLICATION_REPLICA_MARKER.to_string(), "true".to_string());
                layer.put_object(
                    &target_bucket,
                    &entry.object,
                    &PutObjReader {
                        data,
                        declared_size: info.size,
                        expected_md5: String::new(),
                        expected_sha256: String::new(),
                    },
                    ObjectOptions {
                        user_defined,
                        versioned: !info.version_id.is_empty(),
                        version_id: info.version_id.clone(),
                        ..ObjectOptions::default()
                    },
                )?;
                Ok(())
            }
        }
    }
}

pub fn enqueue_bucket_replication_resync_for_layer(
    layer: &LocalObjectLayer,
    replication_service: &ReplicationService,
    bucket: &str,
    target_arn: Option<&str>,
    resync_id: &str,
    resync_before_date: i64,
) -> Result<Vec<BucketReplicationResyncEnqueueSummary>, String> {
    let Some(config) = read_bucket_replication_config(layer, bucket)? else {
        return Err("bucket replication configuration not found".to_string());
    };

    let mut summaries = BTreeMap::<String, BucketReplicationResyncEnqueueSummary>::new();
    for info in layer.all_object_versions(bucket)? {
        if info
            .user_defined
            .get(REPLICATION_REPLICA_MARKER)
            .is_some_and(|value| value == "true")
            || info
                .user_defined
                .get(REPLICATION_REPLICA_STATUS)
                .is_some_and(|value| value == "REPLICA")
        {
            continue;
        }

        let opts = replication::ObjectOpts {
            name: info.name.clone(),
            user_tags: info
                .user_defined
                .get("x-amz-tagging")
                .cloned()
                .unwrap_or_default(),
            version_id: info.version_id.clone(),
            delete_marker: info.delete_marker,
            op_type: if info.delete_marker {
                replication::ReplicationType::Delete
            } else {
                replication::ReplicationType::ExistingObject
            },
            replica: false,
            existing_object: true,
            target_arn: target_arn.unwrap_or_default().to_string(),
            ..replication::ObjectOpts::default()
        };

        for rule in config.filter_actionable_rules(&opts) {
            let destination = if rule.destination.arn.is_empty() {
                rule.destination.bucket.clone()
            } else {
                rule.destination.arn.clone()
            };
            let Some((_, target_bucket)) = destination_target(&destination) else {
                continue;
            };
            if target_bucket == bucket {
                continue;
            }

            if info.delete_marker {
                if rule.delete_marker_replication.status != replication::ENABLED {
                    continue;
                }
                let metadata = Some(BTreeMap::from([
                    ("delete-marker".to_string(), "true".to_string()),
                    ("resync-id".to_string(), resync_id.to_string()),
                    (
                        "resync-before-date".to_string(),
                        resync_before_date.to_string(),
                    ),
                ]));
                replication_service.enqueue_delete(
                    destination.clone(),
                    bucket.to_string(),
                    info.name.clone(),
                    info.version_id.clone(),
                    metadata,
                    now_ms(),
                );
            } else {
                replication_service.enqueue_object(
                    destination.clone(),
                    bucket.to_string(),
                    info.name.clone(),
                    info.version_id.clone(),
                    info.size.max(0) as u64,
                    Some(BTreeMap::from([
                        ("resync-id".to_string(), resync_id.to_string()),
                        (
                            "resync-before-date".to_string(),
                            resync_before_date.to_string(),
                        ),
                    ])),
                    now_ms(),
                );
            }
            let summary = summaries.entry(destination.clone()).or_insert_with(|| {
                BucketReplicationResyncEnqueueSummary {
                    target: destination.clone(),
                    ..BucketReplicationResyncEnqueueSummary::default()
                }
            });
            summary.scheduled_count = summary.scheduled_count.saturating_add(1);
            if !info.delete_marker {
                summary.scheduled_bytes = summary
                    .scheduled_bytes
                    .saturating_add(info.size.max(0) as u64);
            }
        }
    }

    Ok(summaries.into_values().collect())
}

fn enqueue_replication_object_failure(
    replication_service: Option<&ReplicationService>,
    target_arn: &str,
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
    payload_size: u64,
) {
    if let Some(service) = replication_service {
        service.enqueue_object(
            target_arn.to_string(),
            bucket.to_string(),
            object.to_string(),
            info.version_id.clone(),
            payload_size,
            None,
            now_ms(),
        );
    }
}

fn enqueue_replication_delete_failure(
    replication_service: Option<&ReplicationService>,
    target_arn: &str,
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
) {
    if let Some(service) = replication_service {
        let metadata = info
            .delete_marker
            .then(|| BTreeMap::from([("delete-marker".to_string(), "true".to_string())]));
        service.enqueue_delete(
            target_arn.to_string(),
            bucket.to_string(),
            object.to_string(),
            info.version_id.clone(),
            metadata,
            now_ms(),
        );
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

pub fn load_replication_remote_targets_from_env() -> BTreeMap<String, ReplicationRemoteTarget> {
    let mut endpoints = BTreeMap::<String, String>::new();
    let mut access_keys = BTreeMap::<String, String>::new();
    let mut secret_keys = BTreeMap::<String, String>::new();

    for (key, value) in env::vars() {
        if let Some(name) = key.strip_prefix("MINIO_REPLICATION_REMOTE_ENDPOINT_") {
            endpoints.insert(name.to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_REPLICATION_REMOTE_ACCESS_KEY_") {
            access_keys.insert(name.to_string(), value);
        } else if let Some(name) = key.strip_prefix("MINIO_REPLICATION_REMOTE_SECRET_KEY_") {
            secret_keys.insert(name.to_string(), value);
        }
    }

    let mut out = BTreeMap::new();
    for (target_id, endpoint) in endpoints {
        let Some(access_key) = access_keys.get(&target_id).cloned() else {
            continue;
        };
        let Some(secret_key) = secret_keys.get(&target_id).cloned() else {
            continue;
        };
        out.insert(
            target_id.clone(),
            ReplicationRemoteTarget {
                target_id,
                endpoint,
                access_key,
                secret_key,
            },
        );
    }
    out
}

fn destination_target(destination: &str) -> Option<(Option<String>, String)> {
    if let Some(value) = destination.strip_prefix(replication::DESTINATION_ARN_PREFIX) {
        return (!value.is_empty()).then(|| (None, value.to_string()));
    }
    if let Some(value) = destination.strip_prefix(replication::DESTINATION_ARN_MINIO_PREFIX) {
        let mut parts = value.splitn(3, ':');
        let _region = parts.next();
        let target_id = parts.next().unwrap_or_default().to_string();
        let bucket = parts.next().unwrap_or_default().to_string();
        if bucket.is_empty() {
            return None;
        }
        return Some(((!target_id.is_empty()).then_some(target_id), bucket));
    }
    (!destination.is_empty()).then(|| (None, destination.to_string()))
}

fn replicate_object_to_remote(
    remote: &ReplicationRemoteTarget,
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
    data: &[u8],
) -> Result<(), String> {
    let mut request = ureq::put(&object_url(
        &remote.endpoint,
        bucket,
        object,
        (!info.version_id.is_empty()).then_some(info.version_id.as_str()),
    ));
    request = request.set(
        "Authorization",
        &format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD
                .encode(format!("{}:{}", remote.access_key, remote.secret_key))
        ),
    );
    request = request
        .set(REPLICATION_REPLICA_STATUS, "REPLICA")
        .set(REPLICATION_REPLICA_MARKER, "true");
    if !info.content_type.is_empty() {
        request = request.set("content-type", &info.content_type);
    }
    for (key, value) in &info.user_defined {
        let should_forward = matches!(
            key.as_str(),
            "cache-control"
                | "content-disposition"
                | "content-encoding"
                | "content-language"
                | "content-type"
                | "expires"
                | "x-amz-server-side-encryption"
                | "x-amz-server-side-encryption-aws-kms-key-id"
                | "x-amz-checksum-crc32"
                | "x-amz-checksum-crc32c"
                | "x-amz-checksum-sha1"
                | "x-amz-checksum-sha256"
        ) || key.starts_with("x-amz-meta-");
        if should_forward {
            request = request.set(key, value);
        }
    }
    request
        .send_bytes(data)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

fn replicate_delete_to_remote(
    remote: &ReplicationRemoteTarget,
    bucket: &str,
    object: &str,
    info: &ObjectInfo,
) -> Result<(), String> {
    let mut request = ureq::delete(&object_url(
        &remote.endpoint,
        bucket,
        object,
        (!info.delete_marker && !info.version_id.is_empty()).then_some(info.version_id.as_str()),
    ));
    request = request
        .set(
            "Authorization",
            &format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{}", remote.access_key, remote.secret_key))
            ),
        )
        .set(REPLICATION_REPLICA_MARKER, "true");
    if info.delete_marker && !info.version_id.is_empty() {
        request = request.set(REPLICATION_DELETE_MARKER_VERSION_ID, &info.version_id);
    }
    request.call().map(|_| ()).map_err(|err| err.to_string())
}

fn object_url(endpoint: &str, bucket: &str, object: &str, version_id: Option<&str>) -> String {
    let base = endpoint.trim_end_matches('/');
    let path = format!("{base}/{bucket}/{object}");
    match version_id.filter(|value| !value.is_empty()) {
        Some(version_id) => format!("{path}?versionId={version_id}"),
        None => path,
    }
}

fn is_authorized(auth: &RequestAuth, credentials: &HandlerCredentials) -> bool {
    matches!(
        auth.kind,
        RequestAuthKind::SignedV2 | RequestAuthKind::SignedV4
    ) && auth.access_key == credentials.access_key
        && (auth.prevalidated || auth.secret_key == credentials.secret_key)
}

fn replication_path(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket).join(REPLICATION_FILE)
}

fn replication_resync_path(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket).join(REPLICATION_RESYNC_FILE)
}

fn xml_response(status: u16, body: Vec<u8>) -> HandlerResponse {
    HandlerResponse {
        status,
        headers: BTreeMap::from([("content-type".to_string(), "application/xml".to_string())]),
        body,
    }
}

fn error_response(status: u16, code: &str, message: &str) -> HandlerResponse {
    xml_response(
        status,
        format!("<Error><Code>{code}</Code><Message>{message}</Message></Error>").into_bytes(),
    )
}
