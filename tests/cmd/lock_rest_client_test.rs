use std::time::Duration;

use minio_rust::cmd::new_lock_rest_client;
use minio_rust::internal::dsync::LockArgs;

pub const SOURCE_FILE: &str = "cmd/lock-rest-client_test.go";

#[test]
fn test_lock_restlient_line_28() {
    let client = new_lock_rest_client("locker-1");
    let args = LockArgs {
        uid: "uid-1".to_string(),
        resources: vec!["bucket/object".to_string()],
        owner: "owner-1".to_string(),
        source: "test".to_string(),
        quorum: Some(1),
    };

    assert_eq!(client.endpoint(), "locker-1");
    assert!(client.is_local());
    assert!(client.is_online());

    assert!(client.lock(&args, Duration::from_millis(50)).expect("lock"));
    assert!(client.refresh(&args).expect("refresh"));
    assert!(client.unlock(&args).expect("unlock"));
    assert!(!client.refresh(&args).expect("refresh after unlock"));

    client.set_online(false);
    assert!(!client.is_online());
    assert!(client.lock(&args, Duration::from_millis(50)).is_err());
}
