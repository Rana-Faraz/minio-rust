use std::time::Duration;

use minio_rust::internal::dsync::{Dsync, LockArgs};

#[test]
fn missing_extracted_entries() {
    let dsync = Dsync::new_in_memory(1);
    let locker = &dsync.lockers()[0];
    let args = LockArgs {
        uid: "client-lock".to_owned(),
        resources: vec!["resource".to_owned()],
        owner: "owner".to_owned(),
        source: "client".to_owned(),
        quorum: Some(1),
    };

    assert!(locker.lock(&args, Duration::from_millis(50)).unwrap());
    assert!(locker.refresh(&args).unwrap());
    assert!(locker.unlock(&args).unwrap());
}
