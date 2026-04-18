use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cmd::{
    HandlerCredentials, HandlerResponse, LocalObjectLayer, RequestAuth, RequestAuthKind,
};
use crate::internal::event::Config;

const NOTIFICATION_FILE: &str = ".bucket-notification.xml";

pub fn put_bucket_notification_for_layer(
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
    if Config::unmarshal_xml(xml.as_bytes()).is_err() {
        return error_response(400, "MalformedXML", "malformed notification configuration");
    }

    for disk in layer.disk_paths() {
        if !disk.exists() {
            continue;
        }
        let _ = fs::create_dir_all(disk.join(bucket));
        if fs::write(notification_path(disk, bucket), xml.as_bytes()).is_err() {
            return error_response(500, "InternalError", "unable to persist notification");
        }
    }

    HandlerResponse {
        status: 200,
        ..HandlerResponse::default()
    }
}

pub fn get_bucket_notification_for_layer(
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
        let path = notification_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            return xml_response(200, bytes);
        }
    }

    error_response(
        404,
        "NoSuchNotificationConfiguration",
        "bucket notification configuration not found",
    )
}

pub fn delete_bucket_notification_for_layer(
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
        let path = notification_path(disk, bucket);
        if path.exists() {
            let _ = fs::remove_file(path);
        }
    }

    HandlerResponse {
        status: 204,
        ..HandlerResponse::default()
    }
}

pub fn read_bucket_notification_config(
    layer: &LocalObjectLayer,
    bucket: &str,
) -> Result<Option<Config>, String> {
    for disk in layer.disk_paths() {
        let path = notification_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            return Config::unmarshal_xml(&bytes)
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

fn notification_path(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket).join(NOTIFICATION_FILE)
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
