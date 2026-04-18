use std::fmt;
use std::fs;
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use serde::de::DeserializeOwned;
use serde::Serialize;

const DEFAULT_LIMIT: u64 = 100_000;
const DEFAULT_EXT: &str = ".unknown";
const COMPRESS_EXT: &str = ".snappy";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Key {
    pub name: String,
    pub compress: bool,
    pub extension: String,
    pub item_count: usize,
}

impl Key {
    pub fn string(&self) -> String {
        let mut key = self.name.clone();
        if self.item_count > 1 {
            key = format!("{}:{}", self.item_count, key);
        }
        key.push_str(&self.extension);
        if self.compress {
            key.push_str(COMPRESS_EXT);
        }
        key
    }
}

pub fn parse_key(value: &str) -> Key {
    let mut key = Key {
        name: value.to_owned(),
        compress: value.ends_with(COMPRESS_EXT),
        extension: String::new(),
        item_count: 1,
    };
    if key.compress {
        key.name.truncate(key.name.len() - COMPRESS_EXT.len());
    }
    if let Some((count, rest)) = key.name.split_once(':') {
        if let Ok(item_count) = count.parse::<usize>() {
            key.item_count = item_count;
            key.name = rest.to_owned();
        }
    }
    if let Some(idx) = key.name.rfind('.') {
        key.extension = key.name[idx..].to_owned();
        key.name.truncate(idx);
    }
    key
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError(pub String);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoreError {}

pub fn err_limit_exceeded() -> StoreError {
    StoreError("the maximum store limit reached".to_owned())
}

#[derive(Debug)]
struct EntryState {
    directory: PathBuf,
    entry_limit: u64,
    file_ext: String,
    entries: Vec<(String, SystemTime)>,
    next_id: u64,
}

#[derive(Debug, Clone)]
pub struct QueueStore<I> {
    state: Arc<Mutex<EntryState>>,
    _marker: PhantomData<I>,
}

impl<I> QueueStore<I>
where
    I: Serialize + DeserializeOwned + Clone,
{
    pub fn new(directory: impl Into<PathBuf>, limit: u64, ext: impl Into<String>) -> Self {
        let ext = {
            let ext = ext.into();
            if ext.is_empty() {
                DEFAULT_EXT.to_owned()
            } else {
                ext
            }
        };
        Self {
            state: Arc::new(Mutex::new(EntryState {
                directory: directory.into(),
                entry_limit: if limit == 0 { DEFAULT_LIMIT } else { limit },
                file_ext: ext,
                entries: Vec::new(),
                next_id: 0,
            })),
            _marker: PhantomData,
        }
    }

    pub fn open(&self) -> io::Result<()> {
        let mut state = self.state.lock().expect("queue store mutex poisoned");
        fs::create_dir_all(&state.directory)?;

        let mut entries = fs::read_dir(&state.directory)?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let metadata = entry.metadata().ok()?;
                if metadata.is_dir() {
                    return None;
                }
                Some((
                    entry.file_name().to_string_lossy().into_owned(),
                    metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                ))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(_, modified)| *modified);
        state.entries = entries;
        state.next_id = state.entries.len() as u64;
        Ok(())
    }

    pub fn delete(&self) -> io::Result<()> {
        let state = self.state.lock().expect("queue store mutex poisoned");
        fs::remove_dir_all(&state.directory)
    }

    pub fn put(&self, item: I) -> Result<Key, StoreError> {
        let key = self.new_key(false, 1)?;
        let bytes = serde_json::to_vec(&item).map_err(|err| StoreError(err.to_string()))?;
        self.write_bytes(&key, &bytes)?;
        Ok(key)
    }

    pub fn put_multiple(&self, items: &[I]) -> Result<Key, StoreError> {
        let key = self.new_key(true, items.len())?;
        let mut bytes = Vec::new();
        for item in items {
            serde_json::to_writer(&mut bytes, item).map_err(|err| StoreError(err.to_string()))?;
            bytes.push(b'\n');
        }
        self.write_bytes(&key, &bytes)?;
        Ok(key)
    }

    pub fn put_raw(&self, bytes: &[u8]) -> Result<Key, StoreError> {
        let key = self.new_key(false, 1)?;
        self.write_bytes(&key, bytes)?;
        Ok(key)
    }

    pub fn get_raw(&self, key: &Key) -> Result<Vec<u8>, StoreError> {
        let path = self.path_for_key(key)?;
        let bytes = fs::read(path).map_err(|err| StoreError(err.to_string()))?;
        if bytes.is_empty() {
            return Err(StoreError("file does not exist".to_owned()));
        }
        if key.compress {
            snap::raw::Decoder::new()
                .decompress_vec(&bytes)
                .map_err(|err| StoreError(err.to_string()))
        } else {
            Ok(bytes)
        }
    }

    pub fn get(&self, key: &Key) -> Result<I, StoreError> {
        let mut items = self.get_multiple(key)?;
        items
            .pop()
            .ok_or_else(|| StoreError("queue item missing".to_owned()))
    }

    pub fn get_multiple(&self, key: &Key) -> Result<Vec<I>, StoreError> {
        let bytes = self.get_raw(key)?;
        let mut items = Vec::new();
        let deserializer = serde_json::Deserializer::from_slice(&bytes);
        let iter = deserializer.into_iter::<I>();
        for item in iter {
            items.push(item.map_err(|err| StoreError(err.to_string()))?);
        }
        Ok(items)
    }

    pub fn del(&self, key: &Key) -> Result<(), StoreError> {
        let path = self.path_for_key(key)?;
        let _ = fs::remove_file(path);
        let mut state = self.state.lock().expect("queue store mutex poisoned");
        state.entries.retain(|(entry, _)| entry != &key.string());
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .expect("queue store mutex poisoned")
            .entries
            .len()
    }

    pub fn list(&self) -> Vec<Key> {
        let state = self.state.lock().expect("queue store mutex poisoned");
        let mut entries = state.entries.clone();
        entries.sort_by_key(|(_, modified)| *modified);
        entries
            .into_iter()
            .map(|(entry, _)| parse_key(&entry))
            .collect()
    }

    fn new_key(&self, compress: bool, item_count: usize) -> Result<Key, StoreError> {
        let mut state = self.state.lock().expect("queue store mutex poisoned");
        if state.entries.len() as u64 >= state.entry_limit {
            return Err(err_limit_exceeded());
        }
        state.next_id += 1;
        Ok(Key {
            name: format!("{:020}", state.next_id),
            compress,
            extension: state.file_ext.clone(),
            item_count,
        })
    }

    fn write_bytes(&self, key: &Key, bytes: &[u8]) -> Result<(), StoreError> {
        let path = self.path_for_key(key)?;
        let to_write = if key.compress {
            snap::raw::Encoder::new()
                .compress_vec(bytes)
                .map_err(|err| StoreError(err.to_string()))?
        } else {
            bytes.to_vec()
        };
        fs::write(&path, to_write).map_err(|err| StoreError(err.to_string()))?;
        let modified = fs::metadata(&path)
            .and_then(|metadata| metadata.modified())
            .unwrap_or(SystemTime::now());

        let mut state = self.state.lock().expect("queue store mutex poisoned");
        state.entries.push((key.string(), modified));
        Ok(())
    }

    fn path_for_key(&self, key: &Key) -> Result<PathBuf, StoreError> {
        let state = self.state.lock().expect("queue store mutex poisoned");
        Ok(state.directory.join(key.string()))
    }
}

#[derive(Clone)]
pub struct Batch<I>
where
    I: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    inner: Arc<Mutex<BatchInner<I>>>,
    quit_tx: Arc<Mutex<Option<std::sync::mpsc::Sender<()>>>>,
    worker: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

struct BatchInner<I> {
    items: Vec<I>,
    limit: usize,
    store: Option<QueueStore<I>>,
}

pub struct BatchConfig<I>
where
    I: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    pub limit: u32,
    pub store: Option<QueueStore<I>>,
    pub commit_timeout: Duration,
    pub log: Arc<dyn Fn(StoreError) + Send + Sync>,
}

pub fn err_batch_full() -> StoreError {
    StoreError("batch is full".to_owned())
}

impl<I> Batch<I>
where
    I: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    pub fn new(mut config: BatchConfig<I>) -> Self {
        if config.commit_timeout == Duration::ZERO {
            config.commit_timeout = Duration::from_secs(30);
        }

        let inner = Arc::new(Mutex::new(BatchInner {
            items: Vec::with_capacity(config.limit as usize),
            limit: config.limit as usize,
            store: config.store.clone(),
        }));
        let (quit_tx, quit_rx) = std::sync::mpsc::channel::<()>();
        let worker = if config.store.is_some() {
            let inner = inner.clone();
            let timeout = config.commit_timeout;
            let log = config.log.clone();
            Some(thread::spawn(move || loop {
                match quit_rx.recv_timeout(timeout) {
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        if let Err(err) = commit_locked(&inner) {
                            (log)(err);
                        }
                    }
                }
            }))
        } else {
            None
        };

        Self {
            inner,
            quit_tx: Arc::new(Mutex::new(Some(quit_tx))),
            worker: Arc::new(Mutex::new(worker)),
        }
    }

    pub fn add(&self, item: I) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().expect("batch mutex poisoned");
        if inner.items.len() >= inner.limit {
            if inner.store.is_none() {
                return Err(err_batch_full());
            }
            commit_inner(&mut inner)?;
        }
        inner.items.push(item);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("batch mutex poisoned").items.len()
    }

    pub fn close(&self) -> Result<(), StoreError> {
        if let Some(tx) = self.quit_tx.lock().expect("quit mutex poisoned").take() {
            let _ = tx.send(());
        }

        let result = commit_locked(&self.inner);
        if let Some(handle) = self.worker.lock().expect("worker mutex poisoned").take() {
            let _ = handle.join();
        }
        result
    }
}

impl<I> Drop for Batch<I>
where
    I: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn commit_locked<I>(inner: &Arc<Mutex<BatchInner<I>>>) -> Result<(), StoreError>
where
    I: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    let mut inner = inner.lock().expect("batch mutex poisoned");
    commit_inner(&mut inner)
}

fn commit_inner<I>(inner: &mut BatchInner<I>) -> Result<(), StoreError>
where
    I: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    match inner.items.len() {
        0 => Ok(()),
        1 => {
            let item = inner.items.remove(0);
            if let Some(store) = &inner.store {
                store.put(item)?;
            }
            Ok(())
        }
        _ => {
            let items = std::mem::take(&mut inner.items);
            if let Some(store) = &inner.store {
                store.put_multiple(&items)?;
            }
            Ok(())
        }
    }
}

pub fn replay_items<I>(store: &QueueStore<I>) -> Vec<Key>
where
    I: Serialize + DeserializeOwned + Clone,
{
    store.list()
}

pub fn join_store_path(directory: &Path, key: &Key) -> PathBuf {
    directory.join(key.string())
}
