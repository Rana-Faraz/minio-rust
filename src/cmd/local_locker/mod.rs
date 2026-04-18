use std::collections::BTreeMap;

use chrono::TimeDelta;

use crate::cmd::{LocalLockMap, LockRequesterInfo};
use crate::internal::dsync::LockArgs;

#[derive(Debug, Clone)]
struct LocalLockEntry {
    info: LockRequesterInfo,
    idx: usize,
}

fn unix_now_nanos() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() * 1_000)
}

fn format_uuid(uid: &str, idx: usize) -> String {
    format!("{uid}{idx}")
}

fn is_write_lock(entries: &[LocalLockEntry]) -> bool {
    entries.len() == 1 && entries[0].info.writer
}

#[derive(Debug, Default)]
pub struct LocalLocker {
    lock_map: BTreeMap<String, Vec<LocalLockEntry>>,
    lock_uid: BTreeMap<String, String>,
}

impl LocalLocker {
    pub fn new_locker() -> Self {
        Self::default()
    }

    pub fn lock_map_len(&self) -> usize {
        self.lock_map.len()
    }

    pub fn lock_uid_len(&self) -> usize {
        self.lock_uid.len()
    }

    pub fn dup_lock_map(&self) -> LocalLockMap {
        let mut copy = BTreeMap::new();
        for (resource, entries) in &self.lock_map {
            let visible = entries
                .iter()
                .map(|entry| entry.info.clone())
                .collect::<Vec<_>>();
            if !visible.is_empty() {
                copy.insert(resource.clone(), visible);
            }
        }
        LocalLockMap(Some(copy))
    }

    pub fn set_resource_last_refresh_nanos(&mut self, resource: &str, timestamp: i64) {
        if let Some(entries) = self.lock_map.get_mut(resource) {
            for entry in entries {
                entry.info.time_last_refresh = timestamp;
            }
        }
    }

    pub fn lock(&mut self, args: &LockArgs) -> Result<bool, String> {
        if args.resources.is_empty() {
            return Ok(false);
        }
        if !self.can_take_lock(&args.resources) {
            return Ok(false);
        }

        let now = unix_now_nanos();
        for (idx, resource) in args.resources.iter().enumerate() {
            let entry = LocalLockEntry {
                info: LockRequesterInfo {
                    name: resource.clone(),
                    writer: true,
                    uid: args.uid.clone(),
                    timestamp: now,
                    time_last_refresh: now,
                    source: args.source.clone(),
                    group: args.resources.len() > 1,
                    owner: args.owner.clone(),
                    quorum: args.quorum.unwrap_or_default(),
                },
                idx,
            };
            self.lock_map.insert(resource.clone(), vec![entry]);
            self.lock_uid
                .insert(format_uuid(&args.uid, idx), resource.clone());
        }
        Ok(true)
    }

    pub fn unlock(&mut self, args: &LockArgs) -> Result<bool, String> {
        let mut reply = false;
        for resource in &args.resources {
            let mut entries = match self.lock_map.get(resource).cloned() {
                Some(entries) => entries,
                None => continue,
            };
            if !is_write_lock(&entries) {
                return Err(format!(
                    "unlock attempted on a read locked entity: {resource}"
                ));
            }
            reply = self.remove_entry(resource, args, &mut entries) || reply;
        }
        Ok(reply)
    }

    pub fn rlock(&mut self, args: &LockArgs) -> Result<bool, String> {
        if args.resources.len() != 1 {
            return Err(
                "internal error: localLocker.RLock called with more than one resource".to_string(),
            );
        }
        let resource = args.resources[0].clone();
        let now = unix_now_nanos();
        let entry = LocalLockEntry {
            info: LockRequesterInfo {
                name: resource.clone(),
                writer: false,
                uid: args.uid.clone(),
                timestamp: now,
                time_last_refresh: now,
                source: args.source.clone(),
                group: false,
                owner: args.owner.clone(),
                quorum: args.quorum.unwrap_or_default(),
            },
            idx: 0,
        };

        match self.lock_map.get_mut(&resource) {
            Some(entries) => {
                if is_write_lock(entries) {
                    return Ok(false);
                }
                entries.push(entry);
            }
            None => {
                self.lock_map.insert(resource.clone(), vec![entry]);
            }
        }

        self.lock_uid.insert(format_uuid(&args.uid, 0), resource);
        Ok(true)
    }

    pub fn runlock(&mut self, args: &LockArgs) -> Result<bool, String> {
        if args.resources.len() != 1 {
            return Err(
                "internal error: localLocker.RUnlock called with more than one resource"
                    .to_string(),
            );
        }
        let resource = &args.resources[0];
        let mut entries = match self.lock_map.get(resource).cloned() {
            Some(entries) => entries,
            None => return Ok(true),
        };
        if is_write_lock(&entries) {
            return Err(format!(
                "RUnlock attempted on a write locked entity: {resource}"
            ));
        }
        self.remove_entry(resource, args, &mut entries);
        Ok(true)
    }

    pub fn force_unlock(&mut self, args: &LockArgs) -> Result<bool, String> {
        if args.uid.is_empty() {
            for resource in &args.resources {
                let entries = match self.lock_map.get(resource).cloned() {
                    Some(entries) => entries,
                    None => continue,
                };
                let ids = entries
                    .iter()
                    .map(|entry| (entry.info.uid.clone(), entry.idx))
                    .collect::<Vec<_>>();
                for (uid, idx) in ids {
                    let mut current = match self.lock_map.get(resource).cloned() {
                        Some(entries) => entries,
                        None => {
                            self.lock_uid.remove(&format_uuid(&uid, idx));
                            continue;
                        }
                    };
                    let force_args = LockArgs {
                        uid,
                        resources: vec![resource.clone()],
                        owner: String::new(),
                        source: String::new(),
                        quorum: None,
                    };
                    self.remove_entry(resource, &force_args, &mut current);
                }
            }
            return Ok(true);
        }

        let mut removed = false;
        let mut idx = 0usize;
        loop {
            let map_id = format_uuid(&args.uid, idx);
            let resource = match self.lock_uid.get(&map_id).cloned() {
                Some(resource) => resource,
                None => return Ok(removed),
            };
            let mut entries = match self.lock_map.get(&resource).cloned() {
                Some(entries) => entries,
                None => {
                    self.lock_uid.remove(&map_id);
                    idx += 1;
                    continue;
                }
            };
            removed = self.remove_entry(&resource, args, &mut entries) || removed;
            idx += 1;
        }
    }

    pub fn expire_old_locks(&mut self, interval: TimeDelta) {
        let now = unix_now_nanos();
        let interval_nanos = interval.num_nanoseconds().unwrap_or(i64::MAX);
        let resources = self.lock_map.keys().cloned().collect::<Vec<_>>();

        for resource in resources {
            let Some(entries) = self.lock_map.get(&resource).cloned() else {
                continue;
            };

            let mut survivors = Vec::with_capacity(entries.len());
            for entry in entries {
                let age = now.saturating_sub(entry.info.time_last_refresh);
                if age > interval_nanos {
                    self.lock_uid
                        .remove(&format_uuid(&entry.info.uid, entry.idx));
                } else {
                    survivors.push(entry);
                }
            }

            if survivors.is_empty() {
                self.lock_map.remove(&resource);
            } else {
                self.lock_map.insert(resource, survivors);
            }
        }
    }

    fn can_take_lock(&self, resources: &[String]) -> bool {
        resources
            .iter()
            .all(|resource| !self.lock_map.contains_key(resource))
    }

    fn remove_entry(
        &mut self,
        name: &str,
        args: &LockArgs,
        entries: &mut Vec<LocalLockEntry>,
    ) -> bool {
        let Some(index) = entries.iter().position(|entry| {
            entry.info.uid == args.uid && (args.owner.is_empty() || entry.info.owner == args.owner)
        }) else {
            return false;
        };

        let removed = entries.remove(index);
        self.lock_uid
            .remove(&format_uuid(&removed.info.uid, removed.idx));

        if entries.is_empty() {
            self.lock_map.remove(name);
        } else {
            self.lock_map.insert(name.to_string(), entries.clone());
        }
        true
    }
}
