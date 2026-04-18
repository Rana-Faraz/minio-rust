use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LockArgs {
    pub uid: String,
    pub resources: Vec<String>,
    pub owner: String,
    pub source: String,
    pub quorum: Option<i32>,
}

impl LockArgs {
    pub fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|err| err.to_string())
    }

    pub fn unmarshal_msg(&mut self, bytes: &[u8]) -> Result<&[u8], String> {
        *self = serde_json::from_slice(bytes).map_err(|err| err.to_string())?;
        Ok(&[])
    }

    pub fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
        writer
            .write_all(&self.marshal_msg()?)
            .map_err(|err| err.to_string())
    }

    pub fn decode(&mut self, reader: &mut impl Read) -> Result<(), String> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|err| err.to_string())?;
        self.unmarshal_msg(&bytes)?;
        Ok(())
    }

    pub fn msgsize(&self) -> usize {
        self.marshal_msg().map(|bytes| bytes.len()).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ResponseCode {
    #[default]
    Ok,
    LockConflict,
    LockNotInitialized,
    LockNotFound,
    Err,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LockResp {
    pub code: ResponseCode,
    pub err: String,
}

impl LockResp {
    pub fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|err| err.to_string())
    }

    pub fn unmarshal_msg(&mut self, bytes: &[u8]) -> Result<&[u8], String> {
        *self = serde_json::from_slice(bytes).map_err(|err| err.to_string())?;
        Ok(&[])
    }

    pub fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
        writer
            .write_all(&self.marshal_msg()?)
            .map_err(|err| err.to_string())
    }

    pub fn decode(&mut self, reader: &mut impl Read) -> Result<(), String> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|err| err.to_string())?;
        self.unmarshal_msg(&bytes)?;
        Ok(())
    }

    pub fn msgsize(&self) -> usize {
        self.marshal_msg().map(|bytes| bytes.len()).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockKind {
    Read,
    Write,
}

#[derive(Debug, Clone)]
struct HeldLock {
    resources: Vec<String>,
    kind: LockKind,
}

#[derive(Debug, Default)]
struct ResourceState {
    readers: HashSet<String>,
    writer: Option<String>,
}

#[derive(Debug, Default)]
struct ManagerState {
    resources: HashMap<String, ResourceState>,
    holds: HashMap<String, HeldLock>,
}

#[derive(Debug, Default)]
struct LockManager {
    state: Mutex<ManagerState>,
    cv: Condvar,
}

impl LockManager {
    fn try_acquire(
        &self,
        uid: &str,
        resources: &[String],
        kind: LockKind,
        timeout: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        loop {
            if can_acquire(&state, uid, resources, kind) {
                grant(&mut state, uid, resources, kind);
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let wait = deadline.saturating_duration_since(now);
            let (next, _) = self
                .cv
                .wait_timeout(state, wait)
                .unwrap_or_else(|err| err.into_inner());
            state = next;
        }
    }

    fn release(&self, uid: &str) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|err| err.into_inner());
        let held = match state.holds.remove(uid) {
            Some(held) => held,
            None => return false,
        };
        for resource in held.resources {
            if let Some(entry) = state.resources.get_mut(&resource) {
                match held.kind {
                    LockKind::Read => {
                        entry.readers.remove(uid);
                    }
                    LockKind::Write => {
                        if entry.writer.as_deref() == Some(uid) {
                            entry.writer = None;
                        }
                    }
                }
                if entry.readers.is_empty() && entry.writer.is_none() {
                    state.resources.remove(&resource);
                }
            }
        }
        self.cv.notify_all();
        true
    }

    fn refresh(&self, uid: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .holds
            .contains_key(uid)
    }
}

fn can_acquire(state: &ManagerState, uid: &str, resources: &[String], kind: LockKind) -> bool {
    resources.iter().all(|resource| {
        let entry = state.resources.get(resource);
        match kind {
            LockKind::Read => {
                entry.is_none_or(|entry| entry.writer.as_deref().is_none_or(|writer| writer == uid))
            }
            LockKind::Write => entry.is_none_or(|entry| {
                entry.writer.as_deref().is_none_or(|writer| writer == uid)
                    && (entry.readers.is_empty()
                        || (entry.readers.len() == 1 && entry.readers.contains(uid)))
            }),
        }
    })
}

fn grant(state: &mut ManagerState, uid: &str, resources: &[String], kind: LockKind) {
    for resource in resources {
        let entry = state.resources.entry(resource.clone()).or_default();
        match kind {
            LockKind::Read => {
                entry.readers.insert(uid.to_owned());
            }
            LockKind::Write => {
                entry.writer = Some(uid.to_owned());
            }
        }
    }
    state.holds.insert(
        uid.to_owned(),
        HeldLock {
            resources: resources.to_vec(),
            kind,
        },
    );
}

#[derive(Debug)]
pub struct InMemoryNetLocker {
    manager: Arc<LockManager>,
    endpoint: String,
    online: AtomicBool,
    refresh_reply: Mutex<Option<bool>>,
    response_delay: Mutex<Duration>,
}

impl InMemoryNetLocker {
    fn new(manager: Arc<LockManager>, endpoint: String) -> Self {
        Self {
            manager,
            endpoint,
            online: AtomicBool::new(true),
            refresh_reply: Mutex::new(None),
            response_delay: Mutex::new(Duration::ZERO),
        }
    }

    pub fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::SeqCst);
    }

    pub fn set_refresh_reply(&self, reply: bool) {
        *self
            .refresh_reply
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(reply);
    }

    pub fn clear_refresh_reply(&self) {
        *self
            .refresh_reply
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = None;
    }

    pub fn set_response_delay(&self, delay: Duration) {
        *self
            .response_delay
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = delay;
    }

    fn pause(&self) {
        let delay = *self
            .response_delay
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if delay > Duration::ZERO {
            thread::sleep(delay);
        }
    }

    pub fn rlock(&self, args: &LockArgs, timeout: Duration) -> Result<bool, String> {
        self.pause();
        if !self.is_online() {
            return Err("netLocker is offline".to_owned());
        }
        Ok(self
            .manager
            .try_acquire(&args.uid, &args.resources, LockKind::Read, timeout))
    }

    pub fn lock(&self, args: &LockArgs, timeout: Duration) -> Result<bool, String> {
        self.pause();
        if !self.is_online() {
            return Err("netLocker is offline".to_owned());
        }
        Ok(self
            .manager
            .try_acquire(&args.uid, &args.resources, LockKind::Write, timeout))
    }

    pub fn runlock(&self, args: &LockArgs) -> Result<bool, String> {
        self.pause();
        if !self.is_online() {
            return Err("netLocker is offline".to_owned());
        }
        Ok(self.manager.release(&args.uid))
    }

    pub fn unlock(&self, args: &LockArgs) -> Result<bool, String> {
        self.pause();
        if !self.is_online() {
            return Err("netLocker is offline".to_owned());
        }
        Ok(self.manager.release(&args.uid))
    }

    pub fn refresh(&self, args: &LockArgs) -> Result<bool, String> {
        self.pause();
        if !self.is_online() {
            return Err("netLocker is offline".to_owned());
        }
        if let Some(reply) = *self
            .refresh_reply
            .lock()
            .unwrap_or_else(|err| err.into_inner())
        {
            return Ok(reply);
        }
        Ok(self.manager.refresh(&args.uid))
    }

    pub fn force_unlock(&self, args: &LockArgs) -> Result<bool, String> {
        self.pause();
        if !self.is_online() {
            return Err("netLocker is offline".to_owned());
        }
        Ok(self.manager.release(&args.uid))
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    pub fn is_local(&self) -> bool {
        true
    }
}

pub fn new_in_memory_net_locker(endpoint: impl Into<String>) -> Arc<InMemoryNetLocker> {
    Arc::new(InMemoryNetLocker::new(
        Arc::new(LockManager::default()),
        endpoint.into(),
    ))
}

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub acquire: Duration,
    pub refresh_call: Duration,
    pub unlock_call: Duration,
    pub force_unlock_call: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            acquire: Duration::from_secs(1),
            refresh_call: Duration::from_secs(5),
            unlock_call: Duration::from_secs(30),
            force_unlock_call: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Dsync {
    lockers: Vec<Arc<InMemoryNetLocker>>,
    owner: String,
    pub timeouts: Timeouts,
}

impl Dsync {
    pub fn new_in_memory(nodes: usize) -> Self {
        let manager = Arc::new(LockManager::default());
        let lockers = (0..nodes)
            .map(|idx| {
                Arc::new(InMemoryNetLocker::new(
                    manager.clone(),
                    format!("locker-{idx}"),
                ))
            })
            .collect();
        Self {
            lockers,
            owner: "owner".to_owned(),
            timeouts: Timeouts::default(),
        }
    }

    pub fn with_timeouts(mut self, timeouts: Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    pub fn lockers(&self) -> &[Arc<InMemoryNetLocker>] {
        &self.lockers
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub timeout: Duration,
    pub retry_interval: Option<Duration>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(1),
            retry_interval: None,
        }
    }
}

#[derive(Default)]
struct ActiveState {
    write_uid: Option<String>,
    read_uid: Option<String>,
    stop_refresh: Option<Arc<AtomicBool>>,
}

static NEXT_MUTEX_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_LOCK_UID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct DRWMutex {
    names: Vec<String>,
    dsync: Dsync,
    active: Arc<Mutex<ActiveState>>,
    lock_id: u64,
    refresh_interval: Duration,
    retry_min_interval: Duration,
}

impl DRWMutex {
    pub fn new(dsync: Dsync, mut names: Vec<String>) -> Self {
        names.sort();
        Self {
            names,
            dsync,
            active: Arc::new(Mutex::new(ActiveState::default())),
            lock_id: NEXT_MUTEX_ID.fetch_add(1, Ordering::Relaxed),
            refresh_interval: Duration::from_secs(10),
            retry_min_interval: Duration::from_millis(50),
        }
    }

    pub fn set_refresh_interval(&mut self, interval: Duration) {
        self.refresh_interval = interval;
    }

    pub fn lock(&self, id: &str, source: &str) {
        assert!(self.get_lock(
            id,
            source,
            Options {
                timeout: Duration::from_secs(300),
                ..Options::default()
            },
            None
        ));
    }

    pub fn rlock(&self, id: &str, source: &str) {
        assert!(self.get_rlock(
            id,
            source,
            Options {
                timeout: Duration::from_secs(300),
                ..Options::default()
            },
            None
        ));
    }

    pub fn get_lock(
        &self,
        id: &str,
        source: &str,
        options: Options,
        on_loss: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> bool {
        self.lock_blocking(id, source, LockKind::Write, options, on_loss)
    }

    pub fn get_rlock(
        &self,
        id: &str,
        source: &str,
        options: Options,
        on_loss: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> bool {
        self.lock_blocking(id, source, LockKind::Read, options, on_loss)
    }

    fn lock_blocking(
        &self,
        id: &str,
        source: &str,
        kind: LockKind,
        options: Options,
        on_loss: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> bool {
        let quorum = self.dsync.lockers.len() / 2 + 1;
        let started = Instant::now();
        let lock_uid = self.lock_uid(id, kind);
        loop {
            let args = LockArgs {
                uid: lock_uid.clone(),
                resources: self.names.clone(),
                owner: self.dsync.owner().to_owned(),
                source: source.to_owned(),
                quorum: Some(quorum as i32),
            };
            let mut granted = Vec::new();
            for locker in self.dsync.lockers() {
                let result = match kind {
                    LockKind::Read => locker.rlock(&args, self.dsync.timeouts.acquire),
                    LockKind::Write => locker.lock(&args, self.dsync.timeouts.acquire),
                };
                if matches!(result, Ok(true)) {
                    granted.push(locker.clone());
                }
            }
            if granted.len() >= quorum {
                let stop = Arc::new(AtomicBool::new(false));
                {
                    let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
                    if let Some(existing) = active.stop_refresh.replace(stop.clone()) {
                        existing.store(true, Ordering::SeqCst);
                    }
                    match kind {
                        LockKind::Read => active.read_uid = Some(lock_uid.clone()),
                        LockKind::Write => active.write_uid = Some(lock_uid.clone()),
                    }
                }
                self.start_refresh(stop, lock_uid, on_loss);
                return true;
            }
            for locker in granted {
                let _ = match kind {
                    LockKind::Read => locker.runlock(&args),
                    LockKind::Write => locker.unlock(&args),
                };
            }
            if started.elapsed() >= options.timeout {
                return false;
            }
            thread::sleep(options.retry_interval.unwrap_or(self.retry_min_interval));
        }
    }

    fn start_refresh(
        &self,
        stop: Arc<AtomicBool>,
        id: String,
        on_loss: Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        let dsync = self.dsync.clone();
        let names = self.names.clone();
        let refresh_interval = self.refresh_interval;
        thread::spawn(move || {
            let quorum = dsync.lockers.len() / 2 + 1;
            let args = LockArgs {
                uid: id.clone(),
                resources: names,
                owner: dsync.owner().to_owned(),
                source: String::new(),
                quorum: Some(quorum as i32),
            };
            while !stop.load(Ordering::SeqCst) {
                thread::sleep(refresh_interval);
                if stop.load(Ordering::SeqCst) {
                    break;
                }
                let refreshed = dsync
                    .lockers()
                    .iter()
                    .filter(|locker| matches!(locker.refresh(&args), Ok(true)))
                    .count();
                if refreshed < quorum {
                    for locker in dsync.lockers() {
                        let _ = locker.force_unlock(&args);
                    }
                    if let Some(callback) = &on_loss {
                        callback();
                    }
                    break;
                }
            }
        });
    }

    fn lock_uid(&self, id: &str, kind: LockKind) -> String {
        let kind = match kind {
            LockKind::Read => "r",
            LockKind::Write => "w",
        };
        let attempt = NEXT_LOCK_UID.fetch_add(1, Ordering::Relaxed);
        format!("{id}:{kind}:{}:{attempt}", self.lock_id)
    }

    pub fn unlock(&self) {
        let (uid, stop) = {
            let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
            let uid = match active.write_uid.take() {
                Some(uid) => uid,
                None => {
                    drop(active);
                    panic!("Trying to Unlock() while no Lock() is active");
                }
            };
            let stop = active.stop_refresh.take();
            (uid, stop)
        };
        if let Some(stop) = stop {
            stop.store(true, Ordering::SeqCst);
        }
        let dsync = self.dsync.clone();
        let names = self.names.clone();
        thread::spawn(move || {
            let args = LockArgs {
                uid,
                resources: names,
                owner: dsync.owner().to_owned(),
                source: String::new(),
                quorum: None,
            };
            for locker in dsync.lockers() {
                let _ = locker.unlock(&args);
            }
        });
    }

    pub fn runlock(&self) {
        let (uid, stop) = {
            let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
            let uid = match active.read_uid.take() {
                Some(uid) => uid,
                None => {
                    drop(active);
                    panic!("Trying to RUnlock() while no RLock() is active");
                }
            };
            let stop = active.stop_refresh.take();
            (uid, stop)
        };
        if let Some(stop) = stop {
            stop.store(true, Ordering::SeqCst);
        }
        let dsync = self.dsync.clone();
        let names = self.names.clone();
        thread::spawn(move || {
            let args = LockArgs {
                uid,
                resources: names,
                owner: dsync.owner().to_owned(),
                source: String::new(),
                quorum: None,
            };
            for locker in dsync.lockers() {
                let _ = locker.runlock(&args);
            }
        });
    }
}
