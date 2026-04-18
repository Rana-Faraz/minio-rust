use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cmd::{
    HandlerCredentials, HandlerResponse, LocalObjectLayer, MakeBucketOptions, RequestAuth,
    RequestAuthKind,
};

const POLICY_FILE: &str = ".bucket-policy.json";

#[derive(Debug)]
pub struct BucketPolicyHandlers {
    layer: LocalObjectLayer,
    credentials: HandlerCredentials,
}

impl BucketPolicyHandlers {
    pub fn new(layer: LocalObjectLayer, credentials: HandlerCredentials) -> Self {
        Self { layer, credentials }
    }

    pub fn layer(&self) -> &LocalObjectLayer {
        &self.layer
    }

    pub fn create_bucket(&self, bucket: &str) -> Result<(), String> {
        self.layer.make_bucket(bucket, MakeBucketOptions::default())
    }

    pub fn put_bucket_policy(
        &self,
        bucket: &str,
        auth: &RequestAuth,
        json: &str,
    ) -> HandlerResponse {
        put_bucket_policy_for_layer(&self.layer, &self.credentials, bucket, auth, json)
    }

    pub fn get_bucket_policy(&self, bucket: &str, auth: &RequestAuth) -> HandlerResponse {
        get_bucket_policy_for_layer(&self.layer, &self.credentials, bucket, auth)
    }

    pub fn delete_bucket_policy(&self, bucket: &str, auth: &RequestAuth) -> HandlerResponse {
        delete_bucket_policy_for_layer(&self.layer, &self.credentials, bucket, auth)
    }
}

pub fn put_bucket_policy_for_layer(
    layer: &LocalObjectLayer,
    credentials: &HandlerCredentials,
    bucket: &str,
    auth: &RequestAuth,
    json: &str,
) -> HandlerResponse {
    if !is_authorized(auth, credentials) {
        return error_response(403, "AccessDenied", "access denied");
    }
    if layer.bucket_exists(bucket).unwrap_or(false) == false {
        return error_response(404, "NoSuchBucket", "bucket not found");
    }
    if serde_json::from_str::<serde_json::Value>(json).is_err() {
        return error_response(400, "MalformedPolicy", "malformed policy document");
    }

    for disk in layer.disk_paths() {
        if !disk.exists() {
            continue;
        }
        let _ = fs::create_dir_all(disk.join(bucket));
        if fs::write(policy_path(disk, bucket), json.as_bytes()).is_err() {
            return error_response(500, "InternalError", "unable to persist policy");
        }
    }

    HandlerResponse {
        status: 204,
        ..HandlerResponse::default()
    }
}

pub fn get_bucket_policy_for_layer(
    layer: &LocalObjectLayer,
    credentials: &HandlerCredentials,
    bucket: &str,
    auth: &RequestAuth,
) -> HandlerResponse {
    if !is_authorized(auth, credentials) {
        return error_response(403, "AccessDenied", "access denied");
    }
    if layer.bucket_exists(bucket).unwrap_or(false) == false {
        return error_response(404, "NoSuchBucket", "bucket not found");
    }

    for disk in layer.disk_paths() {
        let path = policy_path(disk, bucket);
        if let Ok(bytes) = fs::read(&path) {
            let mut response = HandlerResponse::default();
            response.status = 200;
            response.headers =
                BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
            response.body = bytes;
            return response;
        }
    }

    error_response(404, "NoSuchBucketPolicy", "bucket policy not found")
}

pub fn delete_bucket_policy_for_layer(
    layer: &LocalObjectLayer,
    credentials: &HandlerCredentials,
    bucket: &str,
    auth: &RequestAuth,
) -> HandlerResponse {
    if !is_authorized(auth, credentials) {
        return error_response(403, "AccessDenied", "access denied");
    }
    if layer.bucket_exists(bucket).unwrap_or(false) == false {
        return error_response(404, "NoSuchBucket", "bucket not found");
    }

    for disk in layer.disk_paths() {
        let path = policy_path(disk, bucket);
        if path.exists() {
            let _ = fs::remove_file(path);
        }
    }

    HandlerResponse {
        status: 204,
        ..HandlerResponse::default()
    }
}

fn is_authorized(auth: &RequestAuth, credentials: &HandlerCredentials) -> bool {
    matches!(
        auth.kind,
        RequestAuthKind::SignedV2 | RequestAuthKind::SignedV4
    ) && auth.access_key == credentials.access_key
        && (auth.prevalidated || auth.secret_key == credentials.secret_key)
}

fn policy_path(root: &Path, bucket: &str) -> PathBuf {
    root.join(bucket).join(POLICY_FILE)
}

fn error_response(status: u16, code: &str, message: &str) -> HandlerResponse {
    HandlerResponse {
        status,
        headers: BTreeMap::from([("content-type".to_string(), "application/xml".to_string())]),
        body: format!("<Error><Code>{code}</Code><Message>{message}</Message></Error>")
            .into_bytes(),
    }
}
