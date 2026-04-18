use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationQueueSnapshot {
    pub format: String,
    pub version: u16,
    pub queue: ReplicationQueue,
}
impl_msg_codec!(ReplicationQueueSnapshot);

pub const REPLICATION_QUEUE_SNAPSHOT_FORMAT: &str = "minio-rust/replication-queue";
pub const REPLICATION_QUEUE_SNAPSHOT_VERSION: u16 = 1;

impl ReplicationQueueSnapshot {
    pub fn new(queue: ReplicationQueue) -> Self {
        Self {
            format: REPLICATION_QUEUE_SNAPSHOT_FORMAT.to_string(),
            version: REPLICATION_QUEUE_SNAPSHOT_VERSION,
            queue,
        }
    }

    fn validate(self) -> Result<ReplicationQueue, String> {
        if self.format != REPLICATION_QUEUE_SNAPSHOT_FORMAT {
            return Err(format!(
                "invalid replication queue snapshot format: {}",
                self.format
            ));
        }
        if self.version != REPLICATION_QUEUE_SNAPSHOT_VERSION {
            return Err(format!(
                "unsupported replication queue snapshot version: {}",
                self.version
            ));
        }
        Ok(self.queue)
    }
}

pub fn save_replication_queue_snapshot(
    path: impl AsRef<Path>,
    queue: &ReplicationQueue,
) -> Result<(), String> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }

    let snapshot = ReplicationQueueSnapshot::new(queue.clone());
    let bytes = snapshot.marshal_msg()?;
    let temp_path = replication_queue_snapshot_temp_path(path);

    fs::write(&temp_path, &bytes).map_err(|err| err.to_string())?;
    fs::rename(&temp_path, path).map_err(|err| err.to_string())?;
    Ok(())
}

pub fn load_replication_queue_snapshot(path: impl AsRef<Path>) -> Result<ReplicationQueue, String> {
    let path = path.as_ref();
    let bytes = fs::read(path).map_err(|err| err.to_string())?;
    let mut snapshot = ReplicationQueueSnapshot::new(ReplicationQueue::default());
    let remaining = snapshot.unmarshal_msg(&bytes)?;
    if !remaining.is_empty() {
        return Err("replication queue snapshot contained trailing bytes".to_string());
    }
    snapshot.validate()
}

pub fn replication_queue_snapshot_temp_path(path: &Path) -> PathBuf {
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".tmp");
    PathBuf::from(temp)
}
