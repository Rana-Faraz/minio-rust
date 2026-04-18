use minio_rust::cmd::LocalLocker;
use minio_rust::internal::dsync::LockArgs;

pub const SOURCE_FILE: &str = "cmd/lock-rest-server-common_test.go";

#[test]
fn test_lock_rpc_server_remove_entry_line_55() {
    let mut locker = LocalLocker::new_locker();
    let args = LockArgs {
        uid: "uid-1".to_string(),
        resources: vec!["bucket/object".to_string()],
        owner: "owner-1".to_string(),
        source: "test".to_string(),
        quorum: Some(1),
    };

    assert!(locker.lock(&args).expect("acquire lock"));
    assert_eq!(locker.lock_map_len(), 1);
    assert_eq!(locker.lock_uid_len(), 1);

    let removed = locker.force_unlock(&args).expect("remove entry");
    assert!(removed);
    assert_eq!(locker.lock_map_len(), 0);
    assert_eq!(locker.lock_uid_len(), 0);
    assert!(locker.dup_lock_map().0.unwrap_or_default().is_empty());
}
