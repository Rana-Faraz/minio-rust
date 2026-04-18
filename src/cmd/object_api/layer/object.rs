use super::*;

impl LocalObjectLayer {
    fn check_put_preconditions(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Result<(), String> {
        let if_match = opts
            .user_defined
            .get("if-match")
            .cloned()
            .unwrap_or_default();
        let if_none_match = opts
            .user_defined
            .get("if-none-match")
            .cloned()
            .unwrap_or_default();

        if if_match.is_empty() && if_none_match.is_empty() {
            return Ok(());
        }

        match self.get_object_info(bucket, object) {
            Ok(info) => {
                let _ = self.get_object(bucket, object)?;
                if if_none_match == "*" && !info.etag.is_empty() {
                    return Err("precondition failed".to_string());
                }
                if !if_match.is_empty() && Self::clean_etag(&if_match) != info.etag {
                    return Err("precondition failed".to_string());
                }
                Ok(())
            }
            Err(err) if err == ERR_FILE_NOT_FOUND => {
                if !if_match.is_empty() {
                    Err("precondition failed".to_string())
                } else {
                    Ok(())
                }
            }
            Err(err) => Err(err),
        }
    }

    fn remove_empty_parents(&self, disk: &Path, bucket: &str, object: &str) {
        let bucket_root = disk.join(bucket);
        let mut current = PathBuf::from(object);
        while let Some(parent) = current.parent() {
            if parent.as_os_str().is_empty() {
                break;
            }
            let candidate = bucket_root.join(parent);
            if !candidate.exists() || !is_dir_empty(&candidate, false) {
                break;
            }
            let _ = fs::remove_dir(&candidate);
            current = parent.to_path_buf();
        }
    }

    fn listed_objects(&self, bucket: &str) -> Result<Vec<ObjectInfo>, String> {
        if self
            .objects
            .lock()
            .expect("object state lock")
            .contains_key(bucket)
        {
            return Ok(self.visible_object_infos(bucket));
        }

        let disk = self.first_online_disk()?;
        let root = disk.join(bucket);
        let mut names = Vec::new();
        Self::collect_object_names(&root, "", &mut names)?;
        names.sort();
        Ok(names
            .into_iter()
            .filter_map(|name| self.get_object_info(bucket, &name).ok())
            .collect())
    }

    pub fn put_object(
        &self,
        bucket: &str,
        object: &str,
        reader: &PutObjReader,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo, String> {
        let data = self.validate_put_input(bucket, object, reader)?;
        self.check_put_preconditions(bucket, object, &opts)?;
        let online = self.ensure_write_quorum()?;
        for disk in online {
            let target = disk.join(bucket).join(object);
            if object.ends_with('/') && data.is_empty() {
                fs::create_dir_all(&target).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
                continue;
            }
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
            }
            fs::write(&target, &data).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
        }

        let etag = if let Some(etag) = opts.user_defined.get("etag") {
            etag.clone()
        } else {
            get_md5_hash(&data)
        };
        let versioned =
            self.is_versioned_bucket(bucket) || opts.versioned || opts.version_suspended;
        let version_id = if opts.version_suspended {
            "null".to_string()
        } else if versioned {
            if opts.version_id.is_empty() {
                self.next_version_id()
            } else {
                opts.version_id.clone()
            }
        } else {
            String::new()
        };
        let mtime = opts.mtime.unwrap_or_else(|| Utc::now().timestamp());
        let is_dir = object.ends_with('/') && data.is_empty();
        let content_type = opts
            .user_defined
            .get("content-type")
            .cloned()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| Self::guess_content_type(object));
        self.record_object_version(
            bucket,
            object,
            StoredObjectVersion {
                version_id: version_id.clone(),
                delete_marker: false,
                data: data.clone(),
                user_defined: opts.user_defined.clone(),
                is_dir,
                etag: etag.clone(),
                content_type: content_type.clone(),
                mtime,
                parts: vec![ObjectPartInfo {
                    number: 1,
                    size: data.len() as i64,
                    etag: etag.clone(),
                    actual_size: data.len() as i64,
                }],
            },
            versioned,
        );

        Ok(ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            etag,
            content_type,
            is_dir,
            version_id,
            is_latest: true,
            mod_time: mtime,
            user_defined: opts.user_defined,
            size: data.len() as i64,
            actual_size: Some(data.len() as i64),
            ..ObjectInfo::default()
        })
    }

    pub fn get_object(&self, bucket: &str, object: &str) -> Result<Vec<u8>, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || !is_valid_object_prefix(object) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        let online = self.ensure_read_quorum()?;
        if !online.iter().all(|disk| disk.join(bucket).exists()) {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        let current = self.current_object_version(bucket, object);
        if let Some(current) = current.as_ref() {
            if current.delete_marker || current.is_dir {
                return Err(cmd_err(ERR_FILE_NOT_FOUND));
            }
            return self.read_consensus_data(bucket, object, true);
        }

        self.read_consensus_data(bucket, object, false)
    }

    pub fn get_object_version(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
    ) -> Result<Vec<u8>, String> {
        if version_id.is_empty() {
            return self.get_object(bucket, object);
        }
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || !is_valid_object_prefix(object) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        let online = self.ensure_read_quorum()?;
        if !online.iter().all(|disk| disk.join(bucket).exists()) {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        let objects = self.objects.lock().expect("object state lock");
        let Some(bucket_objects) = objects.get(bucket) else {
            return Err(cmd_err(ERR_FILE_VERSION_NOT_FOUND));
        };
        let Some(versions) = bucket_objects.get(object) else {
            return Err(cmd_err(ERR_FILE_VERSION_NOT_FOUND));
        };
        let Some(version) = versions.iter().find(|entry| entry.version_id == version_id) else {
            return Err(cmd_err(ERR_FILE_VERSION_NOT_FOUND));
        };
        if version.delete_marker || version.is_dir {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }
        Ok(version.data.clone())
    }

    pub fn get_object_part(
        &self,
        bucket: &str,
        object: &str,
        part_number: i32,
    ) -> Result<Vec<u8>, String> {
        if part_number <= 0 {
            return Err(cmd_err(ERR_INVALID_RANGE));
        }

        let info = self.get_object_info(bucket, object)?;
        let data = self.get_object(bucket, object)?;
        if info.parts.is_empty() {
            return if part_number == 1 {
                Ok(data)
            } else {
                Err(cmd_err(ERR_INVALID_RANGE))
            };
        }

        let mut offset = 0usize;
        for part in &info.parts {
            let size = part.actual_size.max(part.size).max(0) as usize;
            let next = offset.saturating_add(size).min(data.len());
            if part.number == part_number {
                return Ok(data[offset..next].to_vec());
            }
            offset = next;
        }

        Err(cmd_err(ERR_INVALID_RANGE))
    }

    pub fn get_object_info(&self, bucket: &str, object: &str) -> Result<ObjectInfo, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || (!is_valid_object_name(object) && !object.ends_with('/')) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        let online = self.ensure_read_quorum()?;
        if !online.iter().all(|disk| disk.join(bucket).exists()) {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        if let Some(current) = self.current_object_version(bucket, object) {
            if current.delete_marker {
                return Err(cmd_err(ERR_FILE_NOT_FOUND));
            }
            return Ok(current.to_object_info(bucket, object, true));
        }

        let disk = self.first_online_disk()?;
        let path = self.object_path(disk, bucket, object);
        if object.ends_with('/') && path.is_dir() && is_dir_empty(&path, false) {
            return Ok(ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                content_type: "application/octet-stream".to_string(),
                is_dir: true,
                is_latest: true,
                mod_time: Utc::now().timestamp(),
                size: 0,
                actual_size: Some(0),
                ..ObjectInfo::default()
            });
        }
        let data = self.get_object(bucket, object)?;
        Ok(ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            etag: get_md5_hash(&data),
            content_type: Self::guess_content_type(object),
            is_dir: false,
            is_latest: true,
            mod_time: Utc::now().timestamp(),
            parts: vec![ObjectPartInfo {
                number: 1,
                size: data.len() as i64,
                etag: get_md5_hash(&data),
                actual_size: data.len() as i64,
            }],
            size: data.len() as i64,
            actual_size: Some(data.len() as i64),
            ..ObjectInfo::default()
        })
    }

    pub fn get_object_info_version(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
    ) -> Result<ObjectInfo, String> {
        if version_id.is_empty() {
            return self.get_object_info(bucket, object);
        }
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || (!is_valid_object_name(object) && !object.ends_with('/')) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        let online = self.ensure_read_quorum()?;
        if !online.iter().all(|disk| disk.join(bucket).exists()) {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        let objects = self.objects.lock().expect("object state lock");
        let Some(bucket_objects) = objects.get(bucket) else {
            return Err(cmd_err(ERR_FILE_VERSION_NOT_FOUND));
        };
        let Some(versions) = bucket_objects.get(object) else {
            return Err(cmd_err(ERR_FILE_VERSION_NOT_FOUND));
        };
        let Some(index) = versions
            .iter()
            .position(|entry| entry.version_id == version_id)
        else {
            return Err(cmd_err(ERR_FILE_VERSION_NOT_FOUND));
        };
        let version = versions[index].clone();
        if version.delete_marker {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }
        let is_latest = index + 1 == versions.len();
        Ok(version.to_object_info(bucket, object, is_latest))
    }

    pub fn delete_object(
        &self,
        bucket: &str,
        object: &str,
        opts: ObjectOptions,
    ) -> Result<ObjectInfo, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || (!is_valid_object_name(object) && !object.ends_with('/')) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }

        let versioned =
            self.is_versioned_bucket(bucket) || opts.versioned || opts.version_suspended;
        if versioned {
            let (removed, current) = {
                let mut objects = self.objects.lock().expect("object state lock");
                let Some(bucket_objects) = objects.get_mut(bucket) else {
                    return Err(if opts.version_id.is_empty() {
                        cmd_err(ERR_FILE_NOT_FOUND)
                    } else {
                        cmd_err(ERR_FILE_VERSION_NOT_FOUND)
                    });
                };
                let Some(versions) = bucket_objects.get_mut(object) else {
                    return Err(if opts.version_id.is_empty() {
                        cmd_err(ERR_FILE_NOT_FOUND)
                    } else {
                        cmd_err(ERR_FILE_VERSION_NOT_FOUND)
                    });
                };

                if !opts.version_id.is_empty() {
                    let Some(index) = versions
                        .iter()
                        .position(|version| version.version_id == opts.version_id)
                    else {
                        return Err(cmd_err(ERR_FILE_VERSION_NOT_FOUND));
                    };
                    let removed = versions.remove(index);
                    let current = versions.last().cloned();
                    if versions.is_empty() {
                        bucket_objects.remove(object);
                    }
                    (removed, current)
                } else {
                    let marker = StoredObjectVersion {
                        version_id: opts
                            .user_defined
                            .get("x-minio-internal-delete-marker-version-id")
                            .cloned()
                            .filter(|value| !value.is_empty())
                            .unwrap_or_else(|| self.next_version_id()),
                        delete_marker: true,
                        data: Vec::new(),
                        user_defined: BTreeMap::new(),
                        is_dir: object.ends_with('/'),
                        etag: String::new(),
                        content_type: Self::guess_content_type(object),
                        mtime: Utc::now().timestamp(),
                        parts: Vec::new(),
                    };
                    versions.push(marker.clone());
                    let current = versions.last().cloned();
                    (marker, current)
                }
            };

            for disk in self.ensure_write_quorum()? {
                self.sync_current_version_to_disk(disk, bucket, object, current.as_ref())?;
                if current
                    .as_ref()
                    .is_none_or(|entry| entry.delete_marker || !entry.is_dir)
                {
                    self.remove_empty_parents(disk, bucket, object);
                }
            }

            return Ok(removed.to_object_info(bucket, object, current.is_none()));
        }

        let mut removed = false;
        let online = self.ensure_write_quorum()?;
        for disk in online {
            let path = self.object_path(disk, bucket, object);
            if object.ends_with('/') {
                if path.is_dir() {
                    if is_dir_empty(&path, false) {
                        let _ = fs::remove_dir(&path);
                        self.remove_empty_parents(disk, bucket, object);
                        removed = true;
                    }
                } else if path.is_file() {
                    let _ = fs::remove_file(&path);
                    self.remove_empty_parents(disk, bucket, object);
                    removed = true;
                }
                continue;
            }

            if path.is_file() {
                fs::remove_file(&path).map_err(|_| cmd_err(ERR_FILE_NOT_FOUND))?;
                self.remove_empty_parents(disk, bucket, object);
                removed = true;
            }
        }

        if !removed {
            return Err(cmd_err(ERR_FILE_NOT_FOUND));
        }

        if let Some(bucket_objects) = self
            .objects
            .lock()
            .expect("object state lock")
            .get_mut(bucket)
        {
            bucket_objects.remove(object);
        }

        Ok(ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            is_latest: true,
            ..ObjectInfo::default()
        })
    }

    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectsInfo, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        if max_keys <= 0 {
            return Ok(ListObjectsInfo::default());
        }

        let names: Vec<ObjectInfo> = self
            .listed_objects(bucket)?
            .into_iter()
            .filter(|info| prefix.is_empty() || info.name.starts_with(prefix))
            .filter(|info| marker.is_empty() || info.name.as_str() > marker)
            .collect();

        let max_keys = max_keys as usize;
        let mut info = ListObjectsInfo::default();
        let mut seen_prefixes = BTreeMap::<String, ()>::new();
        let mut entries_count = 0usize;

        for object_info in names {
            let name = object_info.name.clone();
            if !delimiter.is_empty() {
                let after_prefix = name.strip_prefix(prefix).unwrap_or(&name);
                if let Some(pos) = after_prefix.find(delimiter) {
                    let common_prefix =
                        format!("{}{}", prefix, &after_prefix[..pos + delimiter.len()]);
                    if common_prefix.as_str() <= marker
                        || seen_prefixes.contains_key(&common_prefix)
                    {
                        continue;
                    }
                    if entries_count >= max_keys {
                        info.is_truncated = true;
                        break;
                    }
                    seen_prefixes.insert(common_prefix.clone(), ());
                    info.prefixes.push(common_prefix.clone());
                    info.next_marker = common_prefix;
                    entries_count += 1;
                    continue;
                }
            }

            if entries_count >= max_keys {
                info.is_truncated = true;
                break;
            }
            info.next_marker = name;
            info.objects.push(object_info);
            entries_count += 1;
        }

        Ok(info)
    }

    pub fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        delimiter: &str,
        max_keys: i32,
        _fetch_owner: bool,
        _start_after: &str,
    ) -> Result<ListObjectsInfo, String> {
        let mut info =
            self.list_objects(bucket, prefix, continuation_token, delimiter, max_keys)?;
        if info.is_truncated {
            info.next_continuation_token = info.next_marker.clone();
        }
        Ok(info)
    }

    pub fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        _version_id_marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        if max_keys <= 0 {
            return Ok(ListObjectVersionsInfo::default());
        }

        let names: Vec<ObjectInfo> = self
            .versioned_object_infos(bucket)
            .into_iter()
            .filter(|info| prefix.is_empty() || info.name.starts_with(prefix))
            .filter(|info| marker.is_empty() || info.name.as_str() > marker)
            .collect();

        let max_keys = max_keys as usize;
        let mut info = ListObjectVersionsInfo::default();
        let mut seen_prefixes = BTreeMap::<String, ()>::new();
        let mut entries_count = 0usize;

        for object_info in names {
            let name = object_info.name.clone();
            if !delimiter.is_empty() {
                let after_prefix = name.strip_prefix(prefix).unwrap_or(&name);
                if let Some(pos) = after_prefix.find(delimiter) {
                    let common_prefix =
                        format!("{}{}", prefix, &after_prefix[..pos + delimiter.len()]);
                    if common_prefix.as_str() <= marker
                        || seen_prefixes.contains_key(&common_prefix)
                    {
                        continue;
                    }
                    if entries_count >= max_keys {
                        info.is_truncated = true;
                        break;
                    }
                    seen_prefixes.insert(common_prefix.clone(), ());
                    info.next_marker = common_prefix.clone();
                    info.prefixes.push(common_prefix);
                    entries_count += 1;
                    continue;
                }
            }

            if entries_count >= max_keys {
                info.is_truncated = true;
                break;
            }
            info.next_marker = name;
            info.next_version_id_marker = object_info.version_id.clone();
            info.objects.push(object_info);
            entries_count += 1;
        }

        Ok(info)
    }

    pub fn all_object_versions(&self, bucket: &str) -> Result<Vec<ObjectInfo>, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        Ok(self.versioned_object_infos(bucket))
    }
}
