use super::*;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationServiceSnapshot {
    pub queue: ReplicationQueue,
    pub stats: ReplicationRuntimeStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicationProcessReport {
    pub leased: usize,
    pub succeeded: usize,
    pub retried: usize,
    pub permanently_failed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationWorkerConfig {
    pub batch_size: usize,
    pub idle_sleep: Duration,
}

impl Default for ReplicationWorkerConfig {
    fn default() -> Self {
        Self {
            batch_size: 16,
            idle_sleep: Duration::from_millis(50),
        }
    }
}

#[derive(Debug)]
pub struct ReplicationWorker {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ReplicationWorker {
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn join(mut self) -> std::thread::Result<()> {
        self.stop();
        if let Some(handle) = self.handle.take() {
            handle.join()
        } else {
            Ok(())
        }
    }
}

impl Drop for ReplicationWorker {
    fn drop(&mut self) {
        self.stop();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplicationService {
    queue: Arc<Mutex<ReplicationQueue>>,
    persistence_path: Option<Arc<PathBuf>>,
}

impl ReplicationService {
    pub fn new(config: ReplicationBackoffConfig) -> Self {
        Self {
            queue: Arc::new(Mutex::new(ReplicationQueue::new(config))),
            persistence_path: None,
        }
    }

    pub fn from_queue(queue: ReplicationQueue) -> Self {
        Self {
            queue: Arc::new(Mutex::new(queue)),
            persistence_path: None,
        }
    }

    pub fn new_persistent(
        config: ReplicationBackoffConfig,
        path: impl Into<PathBuf>,
        now_ms: i64,
    ) -> Result<Self, String> {
        let path = path.into();
        let queue = if path.exists() {
            let bytes = fs::read(&path).map_err(|err| err.to_string())?;
            let mut queue: ReplicationQueue =
                serde_json::from_slice(&bytes).map_err(|err| err.to_string())?;
            queue.config = config.sanitized();
            queue.reset_after_restart(now_ms);
            queue
        } else {
            ReplicationQueue::new(config)
        };

        let service = Self {
            queue: Arc::new(Mutex::new(queue)),
            persistence_path: Some(Arc::new(path)),
        };
        service.persist_now()?;
        Ok(service)
    }

    pub fn enqueue_request(&self, request: ReplicationQueueRequest, now_ms: i64) -> String {
        let mut queue = self.queue.lock().expect("replication queue mutex poisoned");
        let id = queue.enqueue(request, now_ms);
        let _ = self.persist_queue_locked(&queue);
        id
    }

    pub fn enqueue_object(
        &self,
        target_arn: impl Into<String>,
        bucket: impl Into<String>,
        object: impl Into<String>,
        version_id: impl Into<String>,
        payload_size: u64,
        metadata: Option<BTreeMap<String, String>>,
        now_ms: i64,
    ) -> String {
        self.enqueue_request(
            ReplicationQueueRequest {
                target_arn: target_arn.into(),
                bucket: bucket.into(),
                object: object.into(),
                version_id: version_id.into(),
                operation: ReplicationOperation::PutObject,
                payload_size,
                metadata,
                max_attempts: 0,
            },
            now_ms,
        )
    }

    pub fn enqueue_delete(
        &self,
        target_arn: impl Into<String>,
        bucket: impl Into<String>,
        object: impl Into<String>,
        version_id: impl Into<String>,
        metadata: Option<BTreeMap<String, String>>,
        now_ms: i64,
    ) -> String {
        self.enqueue_request(
            ReplicationQueueRequest {
                target_arn: target_arn.into(),
                bucket: bucket.into(),
                object: object.into(),
                version_id: version_id.into(),
                operation: ReplicationOperation::DeleteObject,
                payload_size: 0,
                metadata,
                max_attempts: 0,
            },
            now_ms,
        )
    }

    pub fn snapshot(&self, now_ms: i64) -> ReplicationServiceSnapshot {
        let mut queue = self.queue.lock().expect("replication queue mutex poisoned");
        queue.refresh_counts(now_ms);
        ReplicationServiceSnapshot {
            stats: queue.stats.clone(),
            queue: queue.clone(),
        }
    }

    pub fn persist_now(&self) -> Result<(), String> {
        let queue = self.queue.lock().expect("replication queue mutex poisoned");
        self.persist_queue_locked(&queue)
    }

    pub fn process_due_with<F>(
        &self,
        now_ms: i64,
        limit: usize,
        mut executor: F,
    ) -> ReplicationProcessReport
    where
        F: FnMut(&ReplicationQueueEntry) -> Result<(), String>,
    {
        let leased = {
            let mut queue = self.queue.lock().expect("replication queue mutex poisoned");
            queue.lease_due(now_ms, limit)
        };

        let mut report = ReplicationProcessReport {
            leased: leased.len(),
            ..ReplicationProcessReport::default()
        };

        for entry in leased {
            let result = executor(&entry);
            let mut queue = self.queue.lock().expect("replication queue mutex poisoned");
            match result {
                Ok(()) => {
                    queue
                        .mark_success(&entry.id, now_ms)
                        .expect("leased replication entry must exist");
                    report.succeeded = report.succeeded.saturating_add(1);
                }
                Err(error) => {
                    queue
                        .mark_failure(&entry.id, now_ms, error)
                        .expect("leased replication entry must exist");
                    let is_terminal = queue
                        .get(&entry.id)
                        .map(|current| current.status == ReplicationQueueStatus::Failed)
                        .unwrap_or(false);
                    if is_terminal {
                        report.permanently_failed = report.permanently_failed.saturating_add(1);
                    } else {
                        report.retried = report.retried.saturating_add(1);
                    }
                }
            }
            let _ = self.persist_queue_locked(&queue);
        }

        report
    }

    fn persist_queue_locked(&self, queue: &ReplicationQueue) -> Result<(), String> {
        let Some(path) = &self.persistence_path else {
            return Ok(());
        };
        persist_queue(path.as_ref(), queue)
    }

    pub fn spawn_worker<Now, Exec>(
        &self,
        config: ReplicationWorkerConfig,
        now: Now,
        executor: Exec,
    ) -> ReplicationWorker
    where
        Now: Fn() -> i64 + Send + 'static,
        Exec: Fn(&ReplicationQueueEntry) -> Result<(), String> + Send + Sync + 'static,
    {
        let service = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let executor = Arc::new(executor);
        let handle = thread::spawn(move || {
            while !stop_for_thread.load(Ordering::SeqCst) {
                let now_ms = now();
                let report =
                    service.process_due_with(now_ms, config.batch_size, |entry| executor(entry));
                if report.leased == 0 {
                    thread::sleep(config.idle_sleep);
                }
            }
        });

        ReplicationWorker {
            stop,
            handle: Some(handle),
        }
    }
}

fn persist_queue(path: &Path, queue: &ReplicationQueue) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "replication queue path has no parent".to_string())?;
    fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    let bytes = serde_json::to_vec_pretty(queue).map_err(|err| err.to_string())?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).map_err(|err| err.to_string())?;
    fs::rename(&tmp, path).map_err(|err| err.to_string())?;
    Ok(())
}

#[cfg(test)]
#[path = "../../../tests/cmd/replication_service_mod_test.rs"]
mod tests;
