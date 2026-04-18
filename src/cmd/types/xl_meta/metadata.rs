use super::*;

impl XlMetaV2StoredVersion {
    pub fn get_version_id(&self) -> String {
        match &self.version.object_v2 {
            Some(object) => object.version_id_string(),
            None => String::from_utf8_lossy(self.header.version_id.as_deref().unwrap_or_default())
                .into_owned(),
        }
    }

    pub fn data_dir(&self) -> String {
        self.version
            .object_v2
            .as_ref()
            .map(XlMetaV2Object::data_dir_string)
            .unwrap_or_default()
    }

    pub fn merge_signature(&self, strict: bool) -> Vec<u8> {
        let mut signature = Vec::new();
        signature.extend(self.get_version_id().into_bytes());
        signature.push(self.header.type_id);
        if strict {
            signature.extend(self.header.signature.clone().unwrap_or_default());
            signature.extend(self.header.mod_time.to_be_bytes());
            signature.push(self.header.flags);
        }
        signature
    }
}

impl XlMetaV2Object {
    pub fn version_id_string(&self) -> String {
        String::from_utf8_lossy(self.version_id.as_deref().unwrap_or_default()).into_owned()
    }

    pub fn data_dir_string(&self) -> String {
        String::from_utf8_lossy(self.data_dir.as_deref().unwrap_or_default()).into_owned()
    }

    pub fn uses_data_dir(&self) -> bool {
        let data_dir = self.data_dir_string();
        if data_dir.is_empty() {
            return false;
        }

        let transition_complete = self
            .meta_sys
            .as_ref()
            .and_then(|meta| meta.get(TRANSITION_STATUS_KEY))
            .map(|value| String::from_utf8_lossy(value) == "complete")
            .unwrap_or(false);

        if !transition_complete {
            return true;
        }

        let restore_header = self
            .meta_user
            .as_ref()
            .and_then(|meta| meta.get(AMZ_RESTORE_HEADER))
            .map(String::as_str);

        match restore_header.and_then(parse_restore_header) {
            Some(RestoreStatus::Completed(expires_at)) => expires_at > Utc::now(),
            Some(RestoreStatus::Ongoing) | None => false,
        }
    }

    pub fn to_file_info(&self, volume: &str, path: &str, all_parts: bool) -> FileInfo {
        let mut metadata = self.meta_user.clone().unwrap_or_default();
        for (key, value) in self.meta_sys.clone().unwrap_or_default() {
            metadata.insert(key, String::from_utf8_lossy(&value).into_owned());
        }

        let parts = all_parts.then(|| {
            let numbers = self.part_numbers.clone().unwrap_or_default();
            let sizes = self.part_sizes.clone().unwrap_or_default();
            let etags = self.part_etags.clone().unwrap_or_default();
            let actual_sizes = self.part_actual_sizes.clone().unwrap_or_default();
            let mut out = Vec::with_capacity(numbers.len());
            for idx in 0..numbers.len() {
                out.push(ObjectPartInfo {
                    number: numbers[idx],
                    size: *sizes.get(idx).unwrap_or(&0),
                    etag: etags.get(idx).cloned().unwrap_or_default(),
                    actual_size: *actual_sizes.get(idx).unwrap_or(&0),
                });
            }
            out
        });

        let mut file_info = FileInfo {
            volume: volume.to_string(),
            name: path.to_string(),
            version_id: self.version_id_string(),
            is_latest: false,
            deleted: false,
            transition_status: metadata
                .get(TRANSITION_STATUS_KEY)
                .cloned()
                .unwrap_or_default(),
            transitioned_obj_name: String::new(),
            transition_tier: String::new(),
            transition_version_id: String::new(),
            expire_restored: false,
            data_dir: self.data_dir_string(),
            xlv1: false,
            mod_time: self.mod_time,
            size: self.size,
            mode: 0,
            written_by_version: 0,
            metadata: Some(metadata),
            parts,
            erasure: ErasureInfo {
                algorithm: "reedsolomon".to_string(),
                data_blocks: self.erasure_m,
                parity_blocks: self.erasure_n,
                block_size: self.erasure_block_size,
                index: self.erasure_index,
                distribution: self
                    .erasure_dist
                    .clone()
                    .map(|dist| dist.into_iter().map(i32::from).collect()),
            },
            mark_deleted: false,
            replication_state: Default::default(),
            data: None,
            num_versions: 0,
            successor_mod_time: 0,
            fresh: false,
            idx: 0,
            checksum: None,
            versioned: true,
        };
        if let Some(meta) = file_info.metadata.as_mut() {
            meta.remove(HEALING_KEY);
        }
        file_info
    }
}

impl XlMetaV2Version {
    pub fn header(&self) -> XlMetaV2VersionHeader {
        match &self.object_v2 {
            Some(object) => XlMetaV2VersionHeader {
                version_id: object.version_id.clone(),
                mod_time: object.mod_time,
                signature: Some(version_signature(
                    &object.version_id_string(),
                    object.mod_time,
                )),
                type_id: self.type_id,
                flags: 0,
                ec_n: object.erasure_n as u8,
                ec_m: object.erasure_m as u8,
            },
            None => XlMetaV2VersionHeader {
                version_id: self
                    .delete_marker
                    .as_ref()
                    .and_then(|marker| marker.version_id.clone()),
                mod_time: self
                    .delete_marker
                    .as_ref()
                    .map(|marker| marker.mod_time)
                    .unwrap_or_default(),
                signature: Some(version_signature("", 0)),
                type_id: self.type_id,
                flags: 0,
                ec_n: 0,
                ec_m: 0,
            },
        }
    }
}

impl XlMetaV2 {
    pub fn add_version(&mut self, file_info: FileInfo) -> Result<(), String> {
        let version_id = file_info.version_id.clone();
        let inline_data = file_info.data.clone().unwrap_or_default();
        let has_inline_data = !inline_data.is_empty();

        let mut meta_sys = BTreeMap::new();
        if !file_info.transition_status.is_empty() {
            meta_sys.insert(
                TRANSITION_STATUS_KEY.to_string(),
                file_info.transition_status.clone().into_bytes(),
            );
        }
        if has_inline_data {
            meta_sys.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data"),
                b"true".to_vec(),
            );
        }
        let mut meta_user = BTreeMap::new();
        for (key, value) in file_info.metadata.clone().unwrap_or_default() {
            if key.starts_with(RESERVED_METADATA_PREFIX_LOWER) {
                meta_sys.insert(key, value.into_bytes());
            } else {
                meta_user.insert(key, value);
            }
        }

        let object = XlMetaV2Object {
            version_id: Some(version_id.clone().into_bytes()),
            data_dir: Some(file_info.data_dir.clone().into_bytes()),
            erasure_algorithm: 1,
            erasure_m: file_info.erasure.data_blocks,
            erasure_n: file_info.erasure.parity_blocks,
            erasure_block_size: file_info.erasure.block_size,
            erasure_index: 0,
            erasure_dist: file_info
                .erasure
                .distribution
                .clone()
                .map(|dist| dist.into_iter().map(|item| item as u8).collect()),
            bitrot_checksum_algo: 1,
            part_numbers: file_info
                .parts
                .clone()
                .map(|parts| parts.into_iter().map(|part| part.number).collect()),
            part_etags: file_info
                .parts
                .clone()
                .map(|parts| parts.into_iter().map(|part| part.etag).collect()),
            part_sizes: file_info
                .parts
                .clone()
                .map(|parts| parts.into_iter().map(|part| part.size).collect()),
            part_actual_sizes: file_info.parts.clone().map(|parts| {
                parts
                    .into_iter()
                    .map(|part| part.actual_size)
                    .collect::<Vec<_>>()
            }),
            part_indices: None,
            size: file_info.size,
            mod_time: file_info.mod_time,
            meta_sys: (!meta_sys.is_empty()).then_some(meta_sys),
            meta_user: (!meta_user.is_empty()).then_some(meta_user),
        };
        let version = XlMetaV2Version {
            type_id: XL_META_OBJECT_TYPE,
            object_v1: None,
            object_v2: Some(object.clone()),
            delete_marker: None,
            written_by_version: file_info.written_by_version,
        };
        let stored = XlMetaV2StoredVersion {
            header: version.header(),
            version,
            uses_data_dir: !has_inline_data && object.uses_data_dir(),
        };

        if has_inline_data {
            self.data.replace(version_id, inline_data);
        }
        self.versions.push(stored);
        Ok(())
    }

    pub fn append_to(&self, dst: Option<Vec<u8>>) -> Result<Vec<u8>, String> {
        let mut bytes = dst.unwrap_or_default();
        bytes.extend(marshal_named(self)?);
        Ok(bytes)
    }

    pub fn load(&mut self, bytes: &[u8]) -> Result<(), String> {
        let mut loaded: XlMetaV2 = unmarshal_named(bytes)?;
        for version in &mut loaded.versions {
            normalize_timestamps(version);
        }
        self.versions = loaded.versions;
        self.data = loaded.data;
        Ok(())
    }

    pub fn load_or_convert(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.load(bytes)
    }

    pub fn sort_by_mod_time(&mut self) {
        self.versions
            .sort_by(|left, right| right.header.mod_time.cmp(&left.header.mod_time));
    }

    pub fn get_idx(&self, idx: usize) -> Result<XlMetaV2Version, String> {
        self.versions
            .get(idx)
            .map(|version| version.version.clone())
            .ok_or_else(|| "version index out of bounds".to_string())
    }

    pub fn find_version(&self, version_id: &str) -> Result<(usize, XlMetaV2StoredVersion), String> {
        self.versions
            .iter()
            .cloned()
            .enumerate()
            .find(|(_, version)| version.get_version_id() == version_id)
            .ok_or_else(|| format!("version {version_id} not found"))
    }

    pub fn shared_data_dir_count(&self, version_id: &str, data_dir: &str) -> usize {
        self.versions
            .iter()
            .filter(|version| {
                version.get_version_id() != version_id
                    && version.uses_data_dir
                    && version.data_dir() == data_dir
            })
            .count()
    }

    pub fn delete_version(&mut self, file_info: &FileInfo) -> Result<String, String> {
        let Some(idx) = self
            .versions
            .iter()
            .position(|version| version.get_version_id() == file_info.version_id)
        else {
            return Err(format!("version {} not found", file_info.version_id));
        };

        let removed = self.versions.remove(idx);
        self.data.remove(&file_info.version_id);
        if removed.uses_data_dir
            && self.shared_data_dir_count(&file_info.version_id, &removed.data_dir()) == 0
        {
            return Ok(removed.data_dir());
        }
        Ok(String::new())
    }

    pub fn update_object_version(&mut self, file_info: FileInfo) -> Result<(), String> {
        let (idx, current) = self.find_version(&file_info.version_id)?;
        let Some(mut object) = current.version.object_v2 else {
            return Err("unsupported version type".to_string());
        };

        let mut meta_sys = BTreeMap::new();
        if !file_info.transition_status.is_empty() {
            meta_sys.insert(
                TRANSITION_STATUS_KEY.to_string(),
                file_info.transition_status.clone().into_bytes(),
            );
        }

        let mut meta_user = BTreeMap::new();
        for (key, value) in file_info.metadata.clone().unwrap_or_default() {
            if key.starts_with(RESERVED_METADATA_PREFIX_LOWER) {
                meta_sys.insert(key, value.into_bytes());
            } else {
                meta_user.insert(key, value);
            }
        }

        object.data_dir = Some(file_info.data_dir.clone().into_bytes());
        object.erasure_m = file_info.erasure.data_blocks;
        object.erasure_n = file_info.erasure.parity_blocks;
        object.erasure_block_size = file_info.erasure.block_size;
        object.erasure_dist = file_info
            .erasure
            .distribution
            .clone()
            .map(|dist| dist.into_iter().map(|item| item as u8).collect());
        object.part_numbers = file_info
            .parts
            .clone()
            .map(|parts| parts.into_iter().map(|part| part.number).collect());
        object.part_etags = file_info
            .parts
            .clone()
            .map(|parts| parts.into_iter().map(|part| part.etag).collect());
        object.part_sizes = file_info
            .parts
            .clone()
            .map(|parts| parts.into_iter().map(|part| part.size).collect());
        object.part_actual_sizes = file_info.parts.clone().map(|parts| {
            parts
                .into_iter()
                .map(|part| part.actual_size)
                .collect::<Vec<_>>()
        });
        object.size = file_info.size;
        object.mod_time = file_info.mod_time;
        object.meta_sys = (!meta_sys.is_empty()).then_some(meta_sys);
        object.meta_user = (!meta_user.is_empty()).then_some(meta_user);

        let version = XlMetaV2Version {
            type_id: XL_META_OBJECT_TYPE,
            object_v1: None,
            object_v2: Some(object.clone()),
            delete_marker: None,
            written_by_version: file_info.written_by_version,
        };
        self.versions[idx] = XlMetaV2StoredVersion {
            header: version.header(),
            version,
            uses_data_dir: object.uses_data_dir(),
        };
        self.sort_by_mod_time();
        Ok(())
    }

    pub fn to_file_info(
        &self,
        volume: &str,
        path: &str,
        version_id: &str,
        _incl_free_vers: bool,
        all_parts: bool,
    ) -> Result<FileInfo, String> {
        let mut ordered = self.versions.clone();
        ordered.sort_by(|left, right| right.header.mod_time.cmp(&left.header.mod_time));

        let chosen_idx = if version_id.is_empty() {
            0
        } else {
            ordered
                .iter()
                .position(|version| version.get_version_id() == version_id)
                .ok_or_else(|| format!("version {version_id} not found"))?
        };

        let chosen = ordered
            .get(chosen_idx)
            .ok_or_else(|| "version not found".to_string())?;
        let mut file_info = match &chosen.version.object_v2 {
            Some(object) => object.to_file_info(volume, path, all_parts),
            None => return Err("unsupported version type".to_string()),
        };
        file_info.written_by_version = chosen.version.written_by_version;
        file_info.is_latest = chosen_idx == 0;
        file_info.num_versions = ordered.len() as i32;
        if chosen_idx > 0 {
            file_info.successor_mod_time = ordered[chosen_idx - 1].header.mod_time;
        }
        Ok(file_info)
    }

    pub fn list_versions(
        &self,
        volume: &str,
        path: &str,
        _incl_free_vers: bool,
    ) -> Result<FileInfoVersions, String> {
        let mut ordered = self.versions.clone();
        ordered.sort_by(|left, right| right.header.mod_time.cmp(&left.header.mod_time));

        let mut versions = Vec::new();
        for (idx, version) in ordered.iter().enumerate() {
            let Some(object) = &version.version.object_v2 else {
                continue;
            };
            let mut file_info = object.to_file_info(volume, path, true);
            file_info.is_latest = idx == 0;
            file_info.written_by_version = version.version.written_by_version;
            file_info.num_versions = ordered.len() as i32;
            if idx > 0 {
                file_info.successor_mod_time = ordered[idx - 1].header.mod_time;
            }
            versions.push(file_info);
        }

        Ok(FileInfoVersions {
            volume: volume.to_string(),
            name: path.to_string(),
            latest_mod_time: ordered
                .first()
                .map(|version| version.header.mod_time)
                .unwrap_or(0),
            versions: Some(versions),
            free_versions: None,
        })
    }
}

impl MetaCacheEntry {
    pub fn xlmeta(&self) -> Result<XlMetaV2, String> {
        let mut xl = XlMetaV2::default();
        xl.load(&self.metadata)?;
        Ok(xl)
    }
}

impl XlMetaV2VersionHeader {
    pub fn sorts_before(&self, other: &Self) -> bool {
        self.mod_time > other.mod_time
            || (self.mod_time == other.mod_time && self.version_id < other.version_id)
    }
}
