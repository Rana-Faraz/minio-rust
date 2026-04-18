use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::cmd::LocalXlStorage;

#[derive(Debug)]
pub struct NaughtyDisk {
    storage: LocalXlStorage,
    errors: BTreeMap<usize, String>,
    default_err: Option<String>,
    call_nr: Mutex<usize>,
}

impl NaughtyDisk {
    pub fn new(
        storage: LocalXlStorage,
        errors: BTreeMap<usize, String>,
        default_err: Option<String>,
    ) -> Self {
        Self {
            storage,
            errors,
            default_err,
            call_nr: Mutex::new(0),
        }
    }

    fn calc_error(&self) -> Result<(), String> {
        let mut call_nr = self
            .call_nr
            .lock()
            .map_err(|_| "lock poisoned".to_string())?;
        *call_nr += 1;
        if let Some(err) = self.errors.get(&*call_nr) {
            return Err(err.clone());
        }
        if let Some(err) = &self.default_err {
            return Err(err.clone());
        }
        Ok(())
    }

    pub fn call_nr(&self) -> usize {
        self.call_nr.lock().map(|guard| *guard).unwrap_or_default()
    }

    pub fn make_vol(&self, volume: &str) -> Result<(), String> {
        self.calc_error()?;
        self.storage.make_vol(volume)
    }

    pub fn append_file(&self, volume: &str, path: &str, data: &[u8]) -> Result<(), String> {
        self.calc_error()?;
        self.storage.append_file(volume, path, data)
    }

    pub fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>, String> {
        self.calc_error()?;
        self.storage.read_all(volume, path)
    }

    pub fn is_online(&self) -> bool {
        match self.calc_error() {
            Err(err) => err != crate::cmd::ERR_DISK_NOT_FOUND,
            Ok(()) => true,
        }
    }
}
