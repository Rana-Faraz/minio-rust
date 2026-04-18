use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use crate::cmd::{
    validate_bucket_sse_config, HandlerCredentials, HandlerResponse, LocalObjectLayer, RequestAuth,
    RequestAuthKind,
};
use crate::internal::bucket::encryption::BucketSseConfig;

const ENCRYPTION_FILE: &str = ".bucket-encryption.xml";

pub fn put_bucket_encryption_for_layer(
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
    if validate_bucket_sse_config(Cursor::new(xml.as_bytes())).is_err() {
        return error_response(400, "MalformedXML", "malformed encryption configuration");
    }

    for disk in layer.disk_paths() {
        if !disk.exists() {
            continue;
        }
        let _ = fs::create_dir_all(disk.join(bucket));
        if fs::write(encryption_path(disk, bucket), xml.as_bytes()).is_err() {
            return error_response(500, "InternalError", "unable to persist encryption");
        }
    }

    HandlerResponse {
        status: 200,
        ..HandlerResponse::default()
    }
}

pub fn get_bucket_encryption_for_layer(
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
        let path = encryption_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            return xml_response(200, bytes);
        }
    }

    error_response(
        404,
        "ServerSideEncryptionConfigurationNotFoundError",
        "bucket encryption configuration not found",
    )
}

pub fn delete_bucket_encryption_for_layer(
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
        let path = encryption_path(disk, bucket);
        if path.exists() {
            let _ = fs::remove_file(path);
        }
    }

    HandlerResponse {
        status: 204,
        ..HandlerResponse::default()
    }
}

pub fn read_bucket_encryption_config(
    layer: &LocalObjectLayer,
    bucket: &str,
) -> Result<Option<BucketSseConfig>, String> {
    for disk in layer.disk_paths() {
        let path = encryption_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            return validate_bucket_sse_config(Cursor::new(bytes))
                .map(Some)
                .map_err(|err| err.to_string());
        }
    }
    Ok(None)
}

fn is_authorized(auth: &RequestAuth, credentials: &HandlerCredentials) -> bool {
    matches!(
        auth.kind,
        RequestAuthKind::SignedV2 | RequestAuthKind::SignedV4
    ) && auth.access_key == credentials.access_key
        && (auth.prevalidated || auth.secret_key == credentials.secret_key)
}

fn encryption_path(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket).join(ENCRYPTION_FILE)
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
