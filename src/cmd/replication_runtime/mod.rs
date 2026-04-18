use super::*;

use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicationOperation {
    #[default]
    PutObject,
    DeleteObject,
    CopyObject,
    CompleteMultipartUpload,
}
impl_msg_codec!(ReplicationOperation);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicationQueueStatus {
    #[default]
    Queued,
    InFlight,
    Succeeded,
    Failed,
}
impl_msg_codec!(ReplicationQueueStatus);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationBackoffConfig {
    pub initial_backoff_ms: i64,
    pub max_backoff_ms: i64,
    pub default_max_attempts: u32,
}
impl_msg_codec!(ReplicationBackoffConfig);

impl ReplicationBackoffConfig {
    pub fn sanitized(&self) -> Self {
        Self {
            initial_backoff_ms: self.initial_backoff_ms.max(1),
            max_backoff_ms: self.max_backoff_ms.max(self.initial_backoff_ms.max(1)),
            default_max_attempts: self.default_max_attempts.max(1),
        }
    }

    pub fn backoff_delay_ms(&self, attempt: u32) -> i64 {
        let config = self.sanitized();
        let mut delay = config.initial_backoff_ms;
        let shifts = attempt.saturating_sub(1).min(30);
        for _ in 0..shifts {
            delay = delay.saturating_mul(2);
            if delay >= config.max_backoff_ms {
                return config.max_backoff_ms;
            }
        }
        delay.min(config.max_backoff_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationRetryState {
    pub attempts: u32,
    pub max_attempts: u32,
    pub next_attempt_at: i64,
    pub last_attempt_at: i64,
    pub last_success_at: i64,
    pub last_failure_at: i64,
    pub last_error: String,
}
impl_msg_codec!(ReplicationRetryState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationQueueEntry {
    pub id: String,
    pub target_arn: String,
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub operation: ReplicationOperation,
    pub payload_size: u64,
    pub status: ReplicationQueueStatus,
    pub created_at: i64,
    pub updated_at: i64,
    pub metadata: Option<BTreeMap<String, String>>,
    pub retry: ReplicationRetryState,
}
impl_msg_codec!(ReplicationQueueEntry);

impl ReplicationQueueEntry {
    pub fn is_due(&self, now_ms: i64) -> bool {
        self.status == ReplicationQueueStatus::Queued && self.retry.next_attempt_at <= now_ms
    }

    pub fn is_waiting_retry(&self, now_ms: i64) -> bool {
        self.status == ReplicationQueueStatus::Queued && self.retry.next_attempt_at > now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationQueueRequest {
    pub target_arn: String,
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub operation: ReplicationOperation,
    pub payload_size: u64,
    pub metadata: Option<BTreeMap<String, String>>,
    pub max_attempts: u32,
}
impl_msg_codec!(ReplicationQueueRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationRuntimeStats {
    pub queued: usize,
    pub waiting_retry: usize,
    pub in_flight: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub total_entries: usize,
    pub total_attempts: u64,
    pub total_retries_scheduled: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub completed_bytes: u64,
    pub failed_bytes: u64,
    pub last_success_at: i64,
    pub last_failure_at: i64,
}
impl_msg_codec!(ReplicationRuntimeStats);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReplicationQueue {
    pub config: ReplicationBackoffConfig,
    pub entries: BTreeMap<String, ReplicationQueueEntry>,
    pub stats: ReplicationRuntimeStats,
    pub next_sequence: u64,
}
impl_msg_codec!(ReplicationQueue);

impl ReplicationQueue {
    pub fn new(config: ReplicationBackoffConfig) -> Self {
        Self {
            config: config.sanitized(),
            ..Self::default()
        }
    }

    pub fn enqueue(&mut self, request: ReplicationQueueRequest, now_ms: i64) -> String {
        let id = format!("repl-{:020}", self.next_sequence);
        self.next_sequence = self.next_sequence.saturating_add(1);

        let max_attempts = if request.max_attempts == 0 {
            self.config.default_max_attempts
        } else {
            request.max_attempts
        };

        let entry = ReplicationQueueEntry {
            id: id.clone(),
            target_arn: request.target_arn,
            bucket: request.bucket,
            object: request.object,
            version_id: request.version_id,
            operation: request.operation,
            payload_size: request.payload_size,
            status: ReplicationQueueStatus::Queued,
            created_at: now_ms,
            updated_at: now_ms,
            metadata: request.metadata,
            retry: ReplicationRetryState {
                max_attempts,
                next_attempt_at: now_ms,
                ..ReplicationRetryState::default()
            },
        };
        self.entries.insert(id.clone(), entry);
        self.refresh_counts(now_ms);
        id
    }

    pub fn lease_due(&mut self, now_ms: i64, limit: usize) -> Vec<ReplicationQueueEntry> {
        if limit == 0 {
            return Vec::new();
        }

        let due_ids = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_due(now_ms))
            .map(|(id, _)| id.clone())
            .take(limit)
            .collect::<Vec<_>>();

        let mut leased = Vec::with_capacity(due_ids.len());
        for id in due_ids {
            if let Some(entry) = self.entries.get_mut(&id) {
                entry.status = ReplicationQueueStatus::InFlight;
                entry.updated_at = now_ms;
                entry.retry.attempts = entry.retry.attempts.saturating_add(1);
                entry.retry.last_attempt_at = now_ms;
                self.stats.total_attempts = self.stats.total_attempts.saturating_add(1);
                leased.push(entry.clone());
            }
        }
        self.refresh_counts(now_ms);
        leased
    }

    pub fn mark_success(&mut self, entry_id: &str, now_ms: i64) -> Result<(), String> {
        let entry = self
            .entries
            .get_mut(entry_id)
            .ok_or_else(|| format!("replication queue entry not found: {entry_id}"))?;
        entry.status = ReplicationQueueStatus::Succeeded;
        entry.updated_at = now_ms;
        entry.retry.last_success_at = now_ms;
        entry.retry.last_error.clear();

        self.stats.total_completed = self.stats.total_completed.saturating_add(1);
        self.stats.completed_bytes = self
            .stats
            .completed_bytes
            .saturating_add(entry.payload_size);
        self.stats.last_success_at = now_ms;
        self.refresh_counts(now_ms);
        Ok(())
    }

    pub fn mark_failure(
        &mut self,
        entry_id: &str,
        now_ms: i64,
        error: impl Into<String>,
    ) -> Result<(), String> {
        let error = error.into();
        let entry = self
            .entries
            .get_mut(entry_id)
            .ok_or_else(|| format!("replication queue entry not found: {entry_id}"))?;

        entry.updated_at = now_ms;
        entry.retry.last_failure_at = now_ms;
        entry.retry.last_error = error;

        if entry.retry.attempts >= entry.retry.max_attempts {
            entry.status = ReplicationQueueStatus::Failed;
            self.stats.total_failed = self.stats.total_failed.saturating_add(1);
            self.stats.failed_bytes = self.stats.failed_bytes.saturating_add(entry.payload_size);
            self.stats.last_failure_at = now_ms;
        } else {
            entry.status = ReplicationQueueStatus::Queued;
            entry.retry.next_attempt_at =
                now_ms.saturating_add(self.config.backoff_delay_ms(entry.retry.attempts));
            self.stats.total_retries_scheduled =
                self.stats.total_retries_scheduled.saturating_add(1);
        }

        self.refresh_counts(now_ms);
        Ok(())
    }

    pub fn get(&self, entry_id: &str) -> Option<&ReplicationQueueEntry> {
        self.entries.get(entry_id)
    }

    pub fn due_count(&self, now_ms: i64) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.is_due(now_ms))
            .count()
    }

    pub fn reset_after_restart(&mut self, now_ms: i64) {
        for entry in self.entries.values_mut() {
            if entry.status == ReplicationQueueStatus::InFlight {
                entry.status = ReplicationQueueStatus::Queued;
                entry.updated_at = now_ms;
                entry.retry.next_attempt_at = now_ms;
            }
        }
        self.refresh_counts(now_ms);
    }

    pub fn refresh_counts(&mut self, now_ms: i64) {
        let mut queued = 0usize;
        let mut waiting_retry = 0usize;
        let mut in_flight = 0usize;
        let mut succeeded = 0usize;
        let mut failed = 0usize;

        for entry in self.entries.values() {
            match entry.status {
                ReplicationQueueStatus::Queued if entry.retry.next_attempt_at > now_ms => {
                    waiting_retry = waiting_retry.saturating_add(1);
                }
                ReplicationQueueStatus::Queued => {
                    queued = queued.saturating_add(1);
                }
                ReplicationQueueStatus::InFlight => {
                    in_flight = in_flight.saturating_add(1);
                }
                ReplicationQueueStatus::Succeeded => {
                    succeeded = succeeded.saturating_add(1);
                }
                ReplicationQueueStatus::Failed => {
                    failed = failed.saturating_add(1);
                }
            }
        }

        self.stats.queued = queued;
        self.stats.waiting_retry = waiting_retry;
        self.stats.in_flight = in_flight;
        self.stats.succeeded = succeeded;
        self.stats.failed = failed;
        self.stats.total_entries = self.entries.len();
    }
}

#[cfg(test)]
#[path = "../../../tests/cmd/replication_runtime_mod_test.rs"]
mod tests;
