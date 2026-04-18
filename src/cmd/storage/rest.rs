use super::*;

pub const ERR_UNFORMATTED_DISK: &str = "unformatted disk";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NsScannerOptions {
    pub disk_id: String,
    pub scan_mode: i32,
    pub cache: Option<DataUsageCache>,
}
impl_msg_codec!(NsScannerOptions);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NsScannerResp {
    pub update: Option<DataUsageEntry>,
    pub final_cache: Option<DataUsageCache>,
}
impl_msg_codec!(NsScannerResp);

#[derive(Debug, Clone)]
pub struct StorageRestClient {
    storage: LocalXlStorage,
    formatted: bool,
}

impl StorageRestClient {
    pub fn new(path: &str) -> Result<Self, String> {
        Ok(Self {
            storage: LocalXlStorage::new(path)?,
            formatted: false,
        })
    }

    pub fn make_vol(&self, volume: &str) -> Result<(), String> {
        self.storage.make_vol(volume)
    }

    pub fn disk_info(&self, _options: DiskInfoOptions) -> Result<DiskInfo, String> {
        if !self.formatted {
            return Err(ERR_UNFORMATTED_DISK.to_string());
        }
        get_disk_info(&self.storage.disk_path().to_string_lossy()).map(|(disk, _)| disk)
    }

    pub fn stat_info_file(
        &self,
        volume: &str,
        path: &str,
        _healing: bool,
    ) -> Result<StatInfo, String> {
        self.storage.stat_info_file(volume, path)
    }

    pub fn list_dir(
        &self,
        _bucket: &str,
        volume: &str,
        path: &str,
        count: isize,
    ) -> Result<Vec<String>, String> {
        self.storage.list_dir(volume, path, count)
    }

    pub fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>, String> {
        self.storage.read_all(volume, path)
    }

    pub fn read_file(
        &self,
        volume: &str,
        path: &str,
        offset: i64,
        buffer: &mut [u8],
    ) -> Result<usize, String> {
        self.storage.read_file(volume, path, offset, buffer)
    }

    pub fn append_file(&self, volume: &str, path: &str, data: &[u8]) -> Result<(), String> {
        self.storage.append_file(volume, path, data)
    }

    pub fn delete(&self, volume: &str, path: &str, _opts: DeleteOptions) -> Result<(), String> {
        self.storage.delete(volume, path)
    }

    pub fn rename_file(
        &self,
        src_volume: &str,
        src_path: &str,
        dst_volume: &str,
        dst_path: &str,
    ) -> Result<(), String> {
        self.storage
            .rename_file(src_volume, src_path, dst_volume, dst_path)
    }
}
