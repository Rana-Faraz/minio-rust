use super::*;

mod rest;

pub use rest::*;

#[derive(Debug, Clone)]
pub struct LocalXlStorage {
    root: PathBuf,
}

impl LocalXlStorage {
    pub fn new(path: &str) -> Result<Self, String> {
        if path.is_empty() {
            return Err(cmd_err(ERR_INVALID_ARGUMENT));
        }

        let root = PathBuf::from(path);
        if root.exists() {
            if !root.is_dir() {
                return Err(cmd_err(ERR_DISK_NOT_DIR));
            }
        } else {
            fs::create_dir_all(&root).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
        }

        Ok(Self { root })
    }

    pub fn disk_path(&self) -> &Path {
        &self.root
    }

    fn ensure_root(&self) -> Result<(), String> {
        if !self.root.exists() || !self.root.is_dir() {
            return Err(cmd_err(ERR_DISK_NOT_FOUND));
        }
        Ok(())
    }

    fn volume_path(&self, volume: &str) -> PathBuf {
        self.root.join(volume)
    }

    pub fn make_vol(&self, volume: &str) -> Result<(), String> {
        self.ensure_root()?;
        if !is_valid_volname(volume) {
            return Err(cmd_err(ERR_INVALID_ARGUMENT));
        }

        let path = self.volume_path(volume);
        if path.exists() {
            return Err(cmd_err(ERR_VOLUME_EXISTS));
        }

        fs::create_dir_all(path).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))
    }

    pub fn delete_vol(&self, volume: &str, _force_delete: bool) -> Result<(), String> {
        self.ensure_root()?;

        let path = self.volume_path(volume);
        if !is_valid_volname(volume) || !path.exists() || !path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        if !is_dir_empty(&path, true) {
            return Err(cmd_err(ERR_VOLUME_NOT_EMPTY));
        }

        fs::remove_dir(path).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))
    }

    pub fn stat_vol(&self, volume: &str) -> Result<VolInfo, String> {
        self.ensure_root()?;

        let path = self.volume_path(volume);
        if !is_valid_volname(volume) || !path.exists() || !path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        let metadata = fs::metadata(path).map_err(|_| cmd_err(ERR_VOLUME_NOT_FOUND))?;
        let created = metadata
            .modified()
            .ok()
            .and_then(system_time_to_unix)
            .unwrap_or_default();
        Ok(VolInfo {
            name: volume.to_string(),
            created,
            ..VolInfo::default()
        })
    }

    pub fn list_vols(&self) -> Result<Vec<VolInfo>, String> {
        self.ensure_root()?;

        let mut volumes = Vec::new();
        for entry in fs::read_dir(&self.root).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))? {
            let entry = entry.map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
            let file_type = entry.file_type().map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
            if !file_type.is_dir() {
                continue;
            }

            let name = entry.file_name().to_string_lossy().to_string();
            let created = entry
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(system_time_to_unix)
                .unwrap_or_default();
            volumes.push(VolInfo {
                name,
                created,
                ..VolInfo::default()
            });
        }

        volumes.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(volumes)
    }

    pub fn list_dir(&self, volume: &str, path: &str, _count: isize) -> Result<Vec<String>, String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        let dir_path = if path.is_empty() {
            volume_path
        } else {
            volume_path.join(path)
        };

        if !dir_path.exists() || !dir_path.is_dir() {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }

        let mut entries = Vec::new();
        for entry in fs::read_dir(dir_path).map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))? {
            let entry = entry.map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))?;
            let file_type = entry.file_type().map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))?;
            let mut name = entry.file_name().to_string_lossy().to_string();
            if file_type.is_dir() {
                name.push('/');
            }
            entries.push(name);
        }

        entries.sort();
        Ok(entries)
    }

    pub fn append_file(&self, volume: &str, path: &str, data: &[u8]) -> Result<(), String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        check_path_length(path)?;

        let file_path = volume_path.join(path);
        if file_path.exists() && file_path.is_dir() {
            return Err(cmd_err(ERR_IS_NOT_REGULAR));
        }
        if let Some(parent) = file_path.parent() {
            if parent.exists() && !parent.is_dir() {
                return Err(cmd_err(ERR_FILE_ACCESS_DENIED));
            }
            fs::create_dir_all(parent).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
        file.write_all(data)
            .map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))
    }

    pub fn write_all(&self, volume: &str, path: &str, data: &[u8]) -> Result<(), String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        let file_path = volume_path.join(path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))?;
        }
        fs::write(file_path, data).map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))
    }

    pub fn read_all(&self, volume: &str, path: &str) -> Result<Vec<u8>, String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        let file_path = volume_path.join(path);
        if !file_path.exists() || file_path.is_dir() {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }

        fs::read(file_path).map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))
    }

    pub fn read_file(
        &self,
        volume: &str,
        path: &str,
        offset: i64,
        buffer: &mut [u8],
    ) -> Result<usize, String> {
        self.ensure_root()?;

        if offset < 0 {
            return Err(cmd_err(ERR_INVALID_ARGUMENT));
        }

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }
        if path.is_empty() {
            return Err(cmd_err(ERR_IS_NOT_REGULAR));
        }
        check_path_length(path)?;

        let file_path = volume_path.join(path);
        if !file_path.exists() {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }
        if file_path.is_dir() {
            return Err(cmd_err(ERR_IS_NOT_REGULAR));
        }

        let data = fs::read(file_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
        let offset = offset as usize;
        if offset > data.len() {
            return Err(cmd_err(ERR_EOF));
        }

        let available = &data[offset..];
        let to_copy = available.len().min(buffer.len());
        buffer[..to_copy].copy_from_slice(&available[..to_copy]);
        if to_copy < buffer.len() {
            return Err(cmd_err(ERR_UNEXPECTED_EOF));
        }

        Ok(to_copy)
    }

    pub fn read_file_with_verifier(
        &self,
        volume: &str,
        path: &str,
        offset: i64,
        buffer: &mut [u8],
        verifier: &BitrotVerifier,
    ) -> Result<usize, String> {
        match self.read_file(volume, path, offset, buffer) {
            Ok(n) => {
                let digest = bitrot_digest(verifier.algorithm, &buffer[..n]);
                if digest != verifier.checksum {
                    return Err(cmd_err(ERR_FILE_CORRUPT));
                }
                Ok(n)
            }
            Err(err) if err == ERR_UNEXPECTED_EOF => {
                let filled = buffer
                    .iter()
                    .rposition(|byte| *byte != 0)
                    .map(|idx| idx + 1)
                    .unwrap_or(0);
                let digest = bitrot_digest(verifier.algorithm, &buffer[..filled]);
                if digest != verifier.checksum {
                    return Err(cmd_err(ERR_FILE_CORRUPT));
                }
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    pub fn delete(&self, volume: &str, path: &str) -> Result<(), String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }
        check_path_length(path)?;

        let file_path = volume_path.join(path);
        if !file_path.exists() {
            return Ok(());
        }

        if file_path.is_dir() {
            fs::remove_dir_all(file_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))
        } else {
            fs::remove_file(file_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))
        }
    }

    pub fn rename_file(
        &self,
        src_volume: &str,
        src_path: &str,
        dest_volume: &str,
        dest_path: &str,
    ) -> Result<(), String> {
        self.ensure_root()?;

        let src_volume_path = self.volume_path(src_volume);
        if !is_valid_volname(src_volume) || !src_volume_path.exists() || !src_volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        let dest_volume_path = self.volume_path(dest_volume);
        if !is_valid_volname(dest_volume)
            || !dest_volume_path.exists()
            || !dest_volume_path.is_dir()
        {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }

        let src_dir_requested = src_path.ends_with(SLASH_SEPARATOR);
        let dest_dir_requested = dest_path.ends_with(SLASH_SEPARATOR);
        let src_trimmed = src_path.trim_end_matches(SLASH_SEPARATOR);
        let dest_trimmed = dest_path.trim_end_matches(SLASH_SEPARATOR);

        check_path_length(src_trimmed)?;
        check_path_length(dest_trimmed)?;

        let src_abs = src_volume_path.join(src_trimmed);
        if !src_abs.exists() {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }
        let src_is_dir = src_abs.is_dir();
        if src_dir_requested != src_is_dir || dest_dir_requested != src_is_dir {
            return Err(cmd_err(ERR_FILE_ACCESS_DENIED));
        }

        let dest_abs = dest_volume_path.join(dest_trimmed);
        if dest_abs.exists() {
            let dest_is_dir = dest_abs.is_dir();
            if src_is_dir != dest_is_dir || src_is_dir {
                return Err(cmd_err(ERR_FILE_ACCESS_DENIED));
            }
            fs::remove_file(&dest_abs).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
        }

        if let Some(parent) = dest_abs.parent() {
            if parent.exists() && !parent.is_dir() {
                return Err(cmd_err(ERR_FILE_ACCESS_DENIED));
            }
            fs::create_dir_all(parent).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
        }

        fs::rename(src_abs, dest_abs).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))
    }

    pub fn stat_info_file(&self, volume: &str, path: &str) -> Result<StatInfo, String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }
        check_path_length(path)?;

        let target = volume_path.join(path);
        if !target.exists() {
            return Err(cmd_err(ERR_PATH_NOT_FOUND));
        }

        let metadata = fs::metadata(&target).map_err(|_| cmd_err(ERR_PATH_NOT_FOUND))?;
        Ok(StatInfo {
            size: metadata.len() as i64,
            mod_time: metadata
                .modified()
                .ok()
                .and_then(system_time_to_unix)
                .unwrap_or_default(),
            name: path.to_string(),
            dir: metadata.is_dir(),
            mode: 0,
        })
    }

    pub fn verify_file(
        &self,
        volume: &str,
        path: &str,
        expected_size: usize,
        algorithm: BitrotAlgorithm,
        checksum: &[u8],
    ) -> Result<(), String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }
        check_path_length(path)?;

        let file_path = volume_path.join(path);
        if !file_path.exists() || file_path.is_dir() {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }

        let data = fs::read(file_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
        if data.len() != expected_size {
            return Err(cmd_err(ERR_FILE_CORRUPT));
        }

        let digest = bitrot_digest(algorithm, &data);
        if digest != checksum {
            return Err(cmd_err(ERR_FILE_CORRUPT));
        }
        Ok(())
    }

    pub fn write_metadata(
        &self,
        volume: &str,
        path: &str,
        file_info: FileInfo,
    ) -> Result<(), String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }
        if path.is_empty() {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }
        check_path_length(path)?;

        let metadata_path = volume_path.join(path).join(XL_STORAGE_FORMAT_FILE);
        let mut xl_meta = if metadata_path.exists() {
            let bytes = fs::read(&metadata_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
            let mut current = XlMetaV2::default();
            current.load_or_convert(&bytes)?;
            current
        } else {
            XlMetaV2::default()
        };

        xl_meta.add_version(file_info)?;
        xl_meta.sort_by_mod_time();
        let encoded = xl_meta.append_to(None)?;
        if let Some(parent) = metadata_path.parent() {
            fs::create_dir_all(parent).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
        }
        fs::write(metadata_path, encoded).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))
    }

    pub fn read_version(
        &self,
        volume: &str,
        path: &str,
        version_id: &str,
    ) -> Result<FileInfo, String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }
        if path.is_empty() {
            return Err(if version_id.is_empty() {
                cmd_err(ERR_FILE_NOT_FOUND)
            } else {
                cmd_err(ERR_FILE_VERSION_NOT_FOUND)
            });
        }
        check_path_length(path)?;

        let base_path = volume_path.join(path);
        let metadata_path = base_path.join(XL_STORAGE_FORMAT_FILE);
        if metadata_path.exists() {
            let bytes = fs::read(&metadata_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
            let mut xl_meta = XlMetaV2::default();
            xl_meta.load_or_convert(&bytes)?;
            return xl_meta
                .to_file_info(volume, path, version_id, false, true)
                .map_err(|_| {
                    if version_id.is_empty() {
                        cmd_err(ERR_FILE_NOT_FOUND)
                    } else {
                        cmd_err(ERR_FILE_VERSION_NOT_FOUND)
                    }
                });
        }

        let legacy_path = base_path.join(XL_STORAGE_FORMAT_FILE_V1);
        if legacy_path.exists() {
            let bytes = fs::read(legacy_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
            return parse_legacy_xl_json(&bytes, volume, path);
        }

        Err(if version_id.is_empty() {
            cmd_err(ERR_FILE_NOT_FOUND)
        } else {
            cmd_err(ERR_FILE_VERSION_NOT_FOUND)
        })
    }

    pub fn delete_version(
        &self,
        volume: &str,
        path: &str,
        file_info: &FileInfo,
    ) -> Result<(), String> {
        self.ensure_root()?;

        let volume_path = self.volume_path(volume);
        if !is_valid_volname(volume) || !volume_path.exists() || !volume_path.is_dir() {
            return Err(cmd_err(ERR_VOLUME_NOT_FOUND));
        }
        if path.is_empty() {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }
        check_path_length(path)?;

        let base_path = volume_path.join(path);
        let metadata_path = base_path.join(XL_STORAGE_FORMAT_FILE);
        if metadata_path.exists() {
            let bytes = fs::read(&metadata_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
            let mut xl_meta = XlMetaV2::default();
            xl_meta.load_or_convert(&bytes)?;
            let data_dir = xl_meta
                .delete_version(file_info)
                .map_err(|_| cmd_err(ERR_FILE_VERSION_NOT_FOUND))?;
            if !data_dir.is_empty() {
                let data_dir_path = base_path.join(data_dir);
                if data_dir_path.exists() {
                    let _ = fs::remove_dir_all(data_dir_path);
                }
            }

            if xl_meta.versions.is_empty() {
                fs::remove_file(&metadata_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
            } else {
                let encoded = xl_meta.append_to(None)?;
                fs::write(&metadata_path, encoded).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED))?;
            }
            return Ok(());
        }

        let legacy_path = base_path.join(XL_STORAGE_FORMAT_FILE_V1);
        if legacy_path.exists() {
            return fs::remove_file(legacy_path).map_err(|_| cmd_err(ERR_FILE_ACCESS_DENIED));
        }

        Err(if file_info.version_id.is_empty() {
            cmd_err(ERR_FILE_NOT_FOUND)
        } else {
            cmd_err(ERR_FILE_VERSION_NOT_FOUND)
        })
    }

    pub fn read_metadata(&self, item_path: &str) -> Result<Vec<u8>, String> {
        check_path_length(item_path)?;

        let metadata_path = Path::new(item_path).join(XL_STORAGE_FORMAT_FILE);
        let bytes = fs::read(&metadata_path).map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))?;
        let mut xl_meta = XlMetaV2::default();
        if xl_meta.load_or_convert(&bytes).is_ok() {
            xl_meta.data = XlMetaInlineData::default();
            return xl_meta.append_to(None);
        }
        Ok(bytes)
    }
}

fn parse_legacy_xl_json(bytes: &[u8], volume: &str, path: &str) -> Result<FileInfo, String> {
    let json: Value = serde_json::from_slice(bytes).map_err(|_| cmd_err(ERR_FILE_CORRUPT))?;

    let size = json
        .get("stat")
        .and_then(|stat| stat.get("size"))
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let mod_time = json
        .get("stat")
        .and_then(|stat| stat.get("modTime"))
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| {
            dt.with_timezone(&Utc)
                .timestamp_nanos_opt()
                .unwrap_or_default()
        })
        .unwrap_or_default();

    let metadata = json.get("meta").and_then(Value::as_object).map(|meta| {
        meta.iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    value
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| value.to_string()),
                )
            })
            .collect::<BTreeMap<_, _>>()
    });

    let parts = json.get("parts").and_then(Value::as_array).map(|parts| {
        parts
            .iter()
            .map(|part| ObjectPartInfo {
                number: part
                    .get("number")
                    .and_then(Value::as_i64)
                    .unwrap_or_default() as i32,
                size: part.get("size").and_then(Value::as_i64).unwrap_or_default(),
                etag: part
                    .get("etag")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                actual_size: part
                    .get("actualSize")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>()
    });

    let distribution = json
        .get("erasure")
        .and_then(|erasure| erasure.get("distribution"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_i64)
                .map(|value| value as i32)
                .collect::<Vec<_>>()
        });

    Ok(FileInfo {
        volume: volume.to_string(),
        name: path.to_string(),
        version_id: String::new(),
        is_latest: true,
        deleted: false,
        transition_status: String::new(),
        transitioned_obj_name: String::new(),
        transition_tier: String::new(),
        transition_version_id: String::new(),
        expire_restored: false,
        data_dir: json
            .get("dataDir")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        xlv1: true,
        mod_time,
        size,
        mode: 0,
        written_by_version: 0,
        metadata,
        parts,
        erasure: ErasureInfo {
            algorithm: json
                .get("erasure")
                .and_then(|erasure| erasure.get("algorithm"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            data_blocks: json
                .get("erasure")
                .and_then(|erasure| erasure.get("data"))
                .and_then(Value::as_i64)
                .unwrap_or_default() as i32,
            parity_blocks: json
                .get("erasure")
                .and_then(|erasure| erasure.get("parity"))
                .and_then(Value::as_i64)
                .unwrap_or_default() as i32,
            block_size: json
                .get("erasure")
                .and_then(|erasure| erasure.get("blockSize"))
                .and_then(Value::as_i64)
                .unwrap_or_default(),
            index: json
                .get("erasure")
                .and_then(|erasure| erasure.get("index"))
                .and_then(Value::as_i64)
                .unwrap_or_default() as i32,
            distribution,
        },
        mark_deleted: false,
        replication_state: Default::default(),
        data: None,
        num_versions: 1,
        successor_mod_time: 0,
        fresh: false,
        idx: 0,
        checksum: None,
        versioned: false,
    })
}
