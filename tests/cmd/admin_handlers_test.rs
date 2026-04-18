// Rust test snapshot derived from cmd/admin-handlers_test.go.

use std::collections::BTreeMap;

use minio_rust::cmd::{
    extract_heal_init_params, new_test_config, new_test_request, sign_request_v4,
    to_admin_api_err_code, top_lock_entries, AdminApiErrorCode, AdminHandlers, ApiErrorCode,
    Credentials, LocalObjectLayer, LockRequesterInfo, PeerLocks, ServiceSignal, ERR_DISK_NOT_FOUND,
    ERR_ERASURE_WRITE_QUORUM,
};
use tempfile::tempdir;

pub const SOURCE_FILE: &str = "cmd/admin-handlers_test.go";

fn admin_credentials() -> Credentials {
    Credentials::new("admin", "secret")
}

fn must_signed_admin_request(method: &str, url: &str) -> minio_rust::cmd::TestRequest {
    let mut req = new_test_request(method, url, 0, None).expect("request");
    let creds = admin_credentials();
    sign_request_v4(&mut req, &creds.access_key, &creds.secret_key).expect("sign");
    req
}

#[test]
fn test_service_restart_handler_line_241() {
    let handlers = AdminHandlers::new(admin_credentials());
    let req = must_signed_admin_request(
        "POST",
        "http://127.0.0.1:9000/minio/admin/v3/service?action=restart&type=2",
    );

    let result = handlers.restart_handler(&req).expect("restart");
    assert_eq!(result.action, "restart");
    assert_eq!(handlers.seen_signals(), vec![ServiceSignal::Restart]);
}

#[test]
fn test_admin_server_info_line_265() {
    let temp_dir = tempdir().expect("tempdir");
    let object_layer = LocalObjectLayer::new(vec![temp_dir.path().to_path_buf()]);
    new_test_config("us-east-1", &object_layer).expect("config");

    let handlers = AdminHandlers::new(admin_credentials());
    let req = must_signed_admin_request("GET", "http://127.0.0.1:9000/minio/admin/v3/info?info=");

    let info = handlers.server_info(&req).expect("server info");
    assert_eq!(info.region, "us-east-1");
}

#[test]
fn test_to_admin_apierr_code_line_306() {
    assert_eq!(
        to_admin_api_err_code(Some(ERR_ERASURE_WRITE_QUORUM)),
        AdminApiErrorCode::AdminConfigNoQuorum
    );
    assert_eq!(to_admin_api_err_code(None), AdminApiErrorCode::None);
    assert_eq!(
        to_admin_api_err_code(Some(ERR_DISK_NOT_FOUND)),
        AdminApiErrorCode::Api(ApiErrorCode::InternalError)
    );
}

#[test]
fn test_extract_heal_init_params_line_337() {
    let body = br#"{"recursive": false, "dryRun": true, "remove": false, "scanMode": 0}"#;

    let invalid_force = BTreeMap::from([
        ("forceStart".to_string(), vec![String::new()]),
        ("forceStop".to_string(), vec![String::new()]),
    ]);
    let vars = BTreeMap::new();
    assert_eq!(
        extract_heal_init_params(&vars, &invalid_force, body).1,
        AdminApiErrorCode::InvalidRequest
    );

    let vars = BTreeMap::from([("prefix".to_string(), "objprefix".to_string())]);
    let q = BTreeMap::new();
    assert_eq!(
        extract_heal_init_params(&vars, &q, body).1,
        AdminApiErrorCode::HealMissingBucket
    );

    let vars = BTreeMap::from([
        ("bucket".to_string(), "bucket".to_string()),
        ("prefix".to_string(), "objprefix".to_string()),
    ]);
    let (params, err) = extract_heal_init_params(&vars, &q, body);
    assert_eq!(err, AdminApiErrorCode::None);
    assert_eq!(params.bucket, "bucket");
    assert_eq!(params.obj_prefix, "objprefix");
    assert!(params.hs.dry_run);
}

#[test]
fn test_top_lock_entries_line_402() {
    let owners = ["node-0", "node-1", "node-2", "node-3"];
    let mut locks_held = BTreeMap::<String, Vec<LockRequesterInfo>>::new();

    let write_lock = LockRequesterInfo {
        name: "bucket/delete-object-1".to_string(),
        writer: true,
        uid: "group-uid".to_string(),
        timestamp: 10,
        time_last_refresh: 10,
        source: String::new(),
        group: true,
        owner: owners[0].to_string(),
        quorum: 3,
    };
    locks_held.insert(write_lock.name.clone(), vec![write_lock.clone()]);

    let read_lock_1 = LockRequesterInfo {
        name: "bucket/get-object-1".to_string(),
        writer: false,
        uid: "read-1".to_string(),
        timestamp: 20,
        time_last_refresh: 20,
        source: String::new(),
        group: false,
        owner: owners[1].to_string(),
        quorum: 2,
    };
    let mut read_lock_2 = read_lock_1.clone();
    read_lock_2.uid = "read-2".to_string();
    locks_held.insert(
        read_lock_1.name.clone(),
        vec![read_lock_1.clone(), read_lock_2.clone()],
    );

    let peer_locks = owners
        .iter()
        .map(|owner| PeerLocks {
            addr: (*owner).to_string(),
            locks: locks_held.clone(),
        })
        .collect::<Vec<_>>();

    let entries = top_lock_entries(&peer_locks, false);
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].lock_type, "WRITE");
    assert_eq!(entries[0].id, "group-uid");
    assert_eq!(entries[0].server_list.len(), 4);
    assert_eq!(entries[1].lock_type, "READ");
    assert_eq!(entries[2].lock_type, "READ");
}
