use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;

use minio_rust::cmd::{
    ReplicationBackoffConfig, ReplicationOperation, ReplicationQueue, ReplicationQueueRequest,
    ReplicationQueueStatus,
};

pub const SOURCE_FILE: &str = "cmd/replication-runtime_test.go";

trait CmdCodec: Default + Clone + PartialEq + Debug {
    fn marshal_msg(&self) -> Result<Vec<u8>, String>;
    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String>;
    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String>;
    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String>;
}

impl CmdCodec for ReplicationQueue {
    fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        self.marshal_msg()
    }

    fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        self.unmarshal_msg(bytes)
    }

    fn encode(&self, writer: &mut impl std::io::Write) -> Result<(), String> {
        self.encode(writer)
    }

    fn decode(&mut self, reader: &mut impl std::io::Read) -> Result<(), String> {
        self.decode(reader)
    }
}

fn sample_request() -> ReplicationQueueRequest {
    ReplicationQueueRequest {
        target_arn: "arn:minio:replication:us-east-1:remote1:bucket".to_string(),
        bucket: "src".to_string(),
        object: "demo.txt".to_string(),
        version_id: "v1".to_string(),
        operation: ReplicationOperation::PutObject,
        payload_size: 128,
        metadata: Some(BTreeMap::from([(
            "x-amz-bucket-replication-status".to_string(),
            "PENDING".to_string(),
        )])),
        max_attempts: 0,
    }
}

#[test]
fn replication_queue_codec_roundtrip() {
    let mut queue = ReplicationQueue::new(ReplicationBackoffConfig {
        initial_backoff_ms: 50,
        max_backoff_ms: 500,
        default_max_attempts: 4,
    });
    let id = queue.enqueue(sample_request(), 10);
    queue.lease_due(10, 1);
    queue.mark_failure(&id, 11, "temporary").expect("failure");

    let bytes = queue.marshal_msg().expect("marshal");
    let mut decoded = ReplicationQueue::default();
    let rem = decoded.unmarshal_msg(&bytes).expect("unmarshal");
    assert!(rem.is_empty());
    assert_eq!(decoded, queue);

    let mut buf = Cursor::new(Vec::new());
    queue.encode(&mut buf).expect("encode");
    buf.set_position(0);
    let mut streamed = ReplicationQueue::default();
    streamed.decode(&mut buf).expect("decode");
    assert_eq!(streamed, queue);
}

#[test]
fn replication_queue_retry_and_failure_accounting() {
    let mut queue = ReplicationQueue::new(ReplicationBackoffConfig {
        initial_backoff_ms: 50,
        max_backoff_ms: 500,
        default_max_attempts: 2,
    });
    let id = queue.enqueue(sample_request(), 100);

    let leased = queue.lease_due(100, 1);
    assert_eq!(leased.len(), 1);
    queue.mark_failure(&id, 101, "network").expect("failure");
    assert_eq!(queue.stats.waiting_retry, 1);

    let retry_due = queue.get(&id).expect("entry").retry.next_attempt_at;
    let leased_again = queue.lease_due(retry_due, 1);
    assert_eq!(leased_again.len(), 1);
    queue
        .mark_failure(&id, retry_due + 1, "network again")
        .expect("failure");

    let entry = queue.get(&id).expect("entry");
    assert_eq!(entry.status, ReplicationQueueStatus::Failed);
    assert_eq!(queue.stats.total_failed, 1);
    assert_eq!(queue.stats.failed_bytes, 128);
}
