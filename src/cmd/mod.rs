use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use blake2::Blake2b512;
use chrono::{DateTime, Utc};
use highway::{HighwayHash, HighwayHasher, Key};
use md5::Md5;
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn marshal_named<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|err| err.to_string())
}

fn unmarshal_named<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    rmp_serde::from_slice(bytes).map_err(|err| err.to_string())
}

macro_rules! impl_msg_codec {
    ($ty:ty) => {
        impl $ty {
            pub fn marshal_msg(&self) -> Result<Vec<u8>, String> {
                marshal_named(self)
            }

            pub fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
                *self = unmarshal_named(bytes)?;
                Ok(&[])
            }

            pub fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
                writer
                    .write_all(&self.marshal_msg()?)
                    .map_err(|err| err.to_string())
            }

            pub fn decode(&mut self, reader: &mut impl Read) -> Result<(), String> {
                let mut bytes = Vec::new();
                reader
                    .read_to_end(&mut bytes)
                    .map_err(|err| err.to_string())?;
                self.unmarshal_msg(&bytes)?;
                Ok(())
            }

            pub fn msgsize(&self) -> usize {
                self.marshal_msg().map(|bytes| bytes.len()).unwrap_or(0)
            }
        }
    };
}

pub const SLASH_SEPARATOR: &str = "/";
pub const MINIO_META_BUCKET: &str = ".minio.sys";
pub const GLOBAL_MAX_OBJECT_SIZE: i64 = 5 * 1024 * 1024 * 1024 * 1024;
pub const GLOBAL_MIN_PART_SIZE: i64 = 5 * 1024 * 1024;
pub const GLOBAL_MAX_PART_ID: i32 = 10_000;
pub const GLOBAL_MINIO_MODE_FS: &str = "mode-server-fs";
pub const GLOBAL_MINIO_MODE_ERASURE_SD: &str = "mode-server-xl-single";
pub const GLOBAL_MINIO_MODE_ERASURE: &str = "mode-server-xl";
pub const GLOBAL_MINIO_MODE_DIST_ERASURE: &str = "mode-server-distributed-xl";

pub const ERR_INVALID_ARGUMENT: &str = "invalid argument";
pub const ERR_DISK_NOT_FOUND: &str = "disk not found";
pub const ERR_DISK_NOT_DIR: &str = "disk is not a directory";
pub const ERR_VOLUME_EXISTS: &str = "volume already exists";
pub const ERR_VOLUME_NOT_FOUND: &str = "volume not found";
pub const ERR_VOLUME_NOT_EMPTY: &str = "volume not empty";
pub const ERR_FILE_NOT_FOUND: &str = "file not found";
pub const ERR_FILE_ACCESS_DENIED: &str = "file access denied";
pub const ERR_FILE_NAME_TOO_LONG: &str = "file name too long";
pub const ERR_IS_NOT_REGULAR: &str = "not a regular file";
pub const ERR_PATH_NOT_FOUND: &str = "path not found";
pub const ERR_EOF: &str = "eof";
pub const ERR_UNEXPECTED_EOF: &str = "unexpected eof";
pub const ERR_FILE_CORRUPT: &str = "file corrupt";
pub const ERR_FILE_VERSION_NOT_FOUND: &str = "file version not found";
pub const ERR_BUCKET_NAME_INVALID: &str = "bucket name invalid";
pub const ERR_BUCKET_NOT_FOUND: &str = "bucket not found";
pub const ERR_OBJECT_NAME_INVALID: &str = "object name invalid";
pub const ERR_BAD_DIGEST: &str = "bad digest";
pub const ERR_SHA256_MISMATCH: &str = "sha256 mismatch";
pub const ERR_INCOMPLETE_BODY: &str = "incomplete body";
pub const ERR_OVERREAD: &str = "input provided more bytes than specified";
pub const ERR_ERASURE_READ_QUORUM: &str = "read failed. insufficient number of drives online";
pub const ERR_ERASURE_WRITE_QUORUM: &str = "write failed. insufficient number of drives online";
pub const ERR_INVALID_UPLOAD_ID: &str = "invalid upload id";
pub const ERR_INVALID_PART: &str = "invalid part";
pub const ERR_INVALID_RANGE: &str = "invalid range";
pub const ERR_PART_TOO_SMALL: &str = "part too small";
pub const XL_STORAGE_FORMAT_FILE: &str = "xl.meta";
pub const XL_STORAGE_FORMAT_FILE_V1: &str = "xl.json";
pub const MINIO_META_MULTIPART_BUCKET: &str = ".minio.sys/multipart";
pub const MINIO_META_TMP_BUCKET: &str = ".minio.sys/tmp";
pub const RESERVED_METADATA_PREFIX: &str = "X-Minio-Internal-";
pub const COMPRESSION_KEY: &str = "X-Minio-Internal-compression";
pub const ACTUAL_SIZE_KEY: &str = "X-Minio-Internal-actual-size";
pub const COMPRESSION_ALGORITHM_V1: &str = "golang/snappy/LZ77";
pub const COMPRESSION_ALGORITHM_V2: &str = "klauspost/compress/s2";
pub const ERR_FAULTY_DISK: &str = "faulty disk";

pub static GLOBAL_IS_DIST_ERASURE: AtomicBool = AtomicBool::new(false);
pub static GLOBAL_IS_ERASURE: AtomicBool = AtomicBool::new(false);
pub static GLOBAL_IS_ERASURE_SD: AtomicBool = AtomicBool::new(false);

pub fn set_minio_mode_flags(dist_erasure: bool, erasure: bool, erasure_sd: bool) {
    GLOBAL_IS_DIST_ERASURE.store(dist_erasure, Ordering::SeqCst);
    GLOBAL_IS_ERASURE.store(erasure, Ordering::SeqCst);
    GLOBAL_IS_ERASURE_SD.store(erasure_sd, Ordering::SeqCst);
}

fn cmd_err(value: &str) -> String {
    value.to_string()
}

pub fn get_md5_hash(data: &[u8]) -> String {
    format!("{:x}", Md5::digest(data))
}

pub fn get_sha256_hash(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitrotAlgorithm {
    Sha256,
    Blake2b512,
    HighwayHash256,
    HighwayHash256S,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitrotVerifier {
    pub algorithm: BitrotAlgorithm,
    pub checksum: Vec<u8>,
}

impl BitrotVerifier {
    pub fn new(algorithm: BitrotAlgorithm, checksum: Vec<u8>) -> Self {
        Self {
            algorithm,
            checksum,
        }
    }
}

fn bitrot_digest(algorithm: BitrotAlgorithm, bytes: &[u8]) -> Vec<u8> {
    match algorithm {
        BitrotAlgorithm::Sha256 => Sha256::digest(bytes).to_vec(),
        BitrotAlgorithm::Blake2b512 => Blake2b512::digest(bytes).to_vec(),
        BitrotAlgorithm::HighwayHash256 | BitrotAlgorithm::HighwayHash256S => {
            let mut hasher = HighwayHasher::new(Key([1, 2, 3, 4]));
            hasher.append(bytes);
            let words = hasher.finalize256();
            words
                .into_iter()
                .flat_map(u64::to_le_bytes)
                .collect::<Vec<u8>>()
        }
    }
}

pub fn bitrot_checksum(algorithm: BitrotAlgorithm, bytes: &[u8]) -> Vec<u8> {
    bitrot_digest(algorithm, bytes)
}

fn system_time_to_unix(time: SystemTime) -> Option<i64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs() as i64)
}

mod admin_handlers;
mod admin_identity_api;
mod admin_users;
mod api_errors;
mod api_headers;
mod api_resources;
mod api_response;
mod api_utils;
mod auth_handler;
mod background_heal;
mod benchmark_utils;
mod bootstrap_peer;
mod bucket_encryption;
mod bucket_encryption_handlers;
mod bucket_lifecycle;
mod bucket_lifecycle_handlers;
mod bucket_metadata;
mod bucket_notification_handlers;
mod bucket_policy_handlers;
mod bucket_replication;
mod bucket_replication_handlers;
mod bucket_replication_utils;
mod bucket_versioning_handlers;
mod common;
mod common_main;
mod config_current;
mod config_encrypted;
mod copy_part_range;
mod crossdomain_xml;
mod data_scanner;
mod data_usage;
mod dummy_data_generator;
mod dynamic_timeout;
mod encryption_v1;
mod endpoint;
mod erasure;
mod erasure_sets;
mod format;
mod handler_utils;
mod http;
mod http_tracer;
mod iam_etcd_store;
mod iam_object_store;
mod jwt;
mod kms;
mod kms_service;
mod leak_detect;
mod local_locker;
mod lock_rest_client;
mod metacache;
mod metrics_v2;
mod mrf;
mod namespace_lock;
mod naughty_disk;
mod net;
mod notification_targets;
mod object_api;
mod object_lambda_handlers;
mod os_readdir;
mod os_reliable;
mod path;
mod policy;
mod post_policy;
mod postpolicyform;
mod replication_admin_status;
mod replication_queue_store;
mod replication_runtime;
mod replication_service;
mod replication_status;
mod server;
mod server_main;
mod server_startup;
mod sftp_server;
mod signature_v2;
mod signature_v4;
mod site_replication;
mod storage;
mod streaming_signature_v4;
mod sts;
mod test_utils;
mod tier;
mod types;
mod update;
mod update_notifier;
mod version_info;
mod xl_storage_errors;
mod xl_storage_format_utils;
mod xl_storage_platform;

pub use admin_handlers::*;
pub use admin_identity_api::*;
pub use admin_users::*;
pub use api_errors::*;
pub use api_headers::*;
pub use api_resources::*;
pub use api_response::*;
pub use api_utils::*;
pub use auth_handler::*;
pub use background_heal::*;
pub use benchmark_utils::*;
pub use bootstrap_peer::*;
pub use bucket_encryption::*;
pub use bucket_encryption_handlers::*;
pub use bucket_lifecycle::*;
pub use bucket_lifecycle_handlers::*;
pub use bucket_metadata::*;
pub use bucket_notification_handlers::*;
pub use bucket_policy_handlers::*;
pub use bucket_replication::*;
pub use bucket_replication_handlers::*;
pub use bucket_replication_utils::*;
pub use bucket_versioning_handlers::*;
pub use common::*;
pub use common_main::*;
pub use config_current::*;
pub use config_encrypted::*;
pub use copy_part_range::*;
pub use crossdomain_xml::*;
pub use data_scanner::*;
pub use data_usage::*;
pub use dummy_data_generator::*;
pub use dynamic_timeout::*;
pub use encryption_v1::*;
pub use endpoint::*;
pub use erasure::*;
pub use erasure_sets::*;
pub use format::*;
pub use handler_utils::*;
pub use http::*;
pub use http_tracer::*;
pub use iam_etcd_store::*;
pub use iam_object_store::*;
pub use jwt::*;
pub use kms::*;
pub use kms_service::*;
pub use leak_detect::*;
pub use local_locker::*;
pub use lock_rest_client::*;
pub use metacache::*;
pub use metrics_v2::*;
pub use mrf::*;
pub use namespace_lock::*;
pub use naughty_disk::*;
pub use net::*;
pub use notification_targets::*;
pub use object_api::*;
pub use object_lambda_handlers::*;
pub use os_readdir::*;
pub use os_reliable::*;
pub use path::*;
pub use policy::*;
pub use post_policy::*;
pub use postpolicyform::*;
pub use replication_admin_status::*;
pub use replication_queue_store::{
    load_replication_queue_snapshot, replication_queue_snapshot_temp_path,
    save_replication_queue_snapshot, ReplicationQueueSnapshot as ReplicationQueueStoreSnapshot,
    REPLICATION_QUEUE_SNAPSHOT_FORMAT, REPLICATION_QUEUE_SNAPSHOT_VERSION,
};
pub use replication_runtime::*;
pub use replication_service::*;
pub use replication_status::*;
pub use server::*;
pub use server_main::*;
pub use server_startup::*;
pub use sftp_server::*;
pub use signature_v2::*;
pub use signature_v4::*;
pub use site_replication::*;
pub use storage::*;
pub use streaming_signature_v4::*;
pub use sts::*;
pub use test_utils::*;
pub use tier::*;
pub use types::*;
pub use update::*;
pub use update_notifier::*;
pub use version_info::parse_version_time;
pub use xl_storage_errors::*;
pub use xl_storage_format_utils::*;
pub use xl_storage_platform::*;
