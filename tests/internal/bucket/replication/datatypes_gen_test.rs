use minio_rust::internal::bucket::replication::{StatusType, VersionPurgeStatusType};

#[test]
fn missing_extracted_entries() {
    assert_eq!(StatusType::Pending.as_str(), "PENDING");
    assert_eq!(StatusType::Completed.as_str(), "COMPLETED");
    assert_eq!(StatusType::CompletedLegacy.as_str(), "COMPLETE");
    assert_eq!(StatusType::Failed.as_str(), "FAILED");
    assert_eq!(StatusType::Replica.as_str(), "REPLICA");

    assert!(StatusType::is_empty(None));
    assert!(VersionPurgeStatusType::Pending.pending());
    assert!(VersionPurgeStatusType::Failed.pending());
    assert!(VersionPurgeStatusType::Complete != VersionPurgeStatusType::Pending);
}
