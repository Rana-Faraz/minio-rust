use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use crate::cmd::{
    HandlerCredentials, HandlerResponse, LocalObjectLayer, RequestAuth, RequestAuthKind,
};
use crate::internal::bucket::versioning::{self, Versioning};

const VERSIONING_FILE: &str = ".bucket-versioning.xml";

pub fn put_bucket_versioning_for_layer(
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

    let config = match versioning::parse_config(Cursor::new(xml.as_bytes())) {
        Ok(config) => config,
        Err(_) => return error_response(400, "MalformedXML", "malformed versioning configuration"),
    };

    for disk in layer.disk_paths() {
        if !disk.exists() {
            continue;
        }
        let _ = fs::create_dir_all(disk.join(bucket));
        if fs::write(versioning_path(disk, bucket), xml.as_bytes()).is_err() {
            return error_response(500, "InternalError", "unable to persist versioning");
        }
    }

    if layer
        .set_bucket_versioning_enabled(bucket, config.enabled())
        .is_err()
    {
        return error_response(500, "InternalError", "unable to apply versioning");
    }

    HandlerResponse {
        status: 200,
        ..HandlerResponse::default()
    }
}

pub fn get_bucket_versioning_for_layer(
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
        let path = versioning_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            return xml_response(200, bytes);
        }
    }

    let fallback = Versioning {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/".to_string(),
        status: String::new(),
        excluded_prefixes: Vec::new(),
        exclude_folders: false,
    };
    let body = fallback
        .to_xml()
        .unwrap_or_else(|_| {
            "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"
                .to_string()
        })
        .into_bytes();
    xml_response(200, body)
}

fn is_authorized(auth: &RequestAuth, credentials: &HandlerCredentials) -> bool {
    matches!(
        auth.kind,
        RequestAuthKind::SignedV2 | RequestAuthKind::SignedV4
    ) && auth.access_key == credentials.access_key
        && (auth.prevalidated || auth.secret_key == credentials.secret_key)
}

fn versioning_path(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket).join(VERSIONING_FILE)
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
