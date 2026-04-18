use super::*;

mod bucket;
mod healing;
mod multipart;
mod object;

#[derive(Debug, Clone, Default)]
struct BucketState {
    versioning_enabled: bool,
    expiration_days: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct StoredObjectVersion {
    version_id: String,
    delete_marker: bool,
    data: Vec<u8>,
    user_defined: BTreeMap<String, String>,
    is_dir: bool,
    etag: String,
    content_type: String,
    mtime: i64,
    parts: Vec<ObjectPartInfo>,
}

impl StoredObjectVersion {
    fn to_object_info(&self, bucket: &str, name: &str, is_latest: bool) -> ObjectInfo {
        ObjectInfo {
            bucket: bucket.to_string(),
            name: name.to_string(),
            etag: self.etag.clone(),
            content_type: self.content_type.clone(),
            is_dir: self.is_dir,
            delete_marker: self.delete_marker,
            version_id: self.version_id.clone(),
            is_latest,
            mod_time: self.mtime,
            user_defined: self.user_defined.clone(),
            parts: self.parts.clone(),
            size: self.data.len() as i64,
            actual_size: Some(self.data.len() as i64),
            ..ObjectInfo::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
struct MultipartStoredPart {
    etag: String,
    size: i64,
    data: Vec<u8>,
}

#[derive(Debug, Default)]
struct MultipartUploadState {
    bucket: String,
    object: String,
    user_defined: BTreeMap<String, String>,
    parts: BTreeMap<i32, MultipartStoredPart>,
}

#[derive(Debug)]
pub struct LocalObjectLayer {
    disks: Vec<PathBuf>,
    buckets: Mutex<BTreeMap<String, BucketState>>,
    objects: Mutex<BTreeMap<String, BTreeMap<String, Vec<StoredObjectVersion>>>>,
    uploads: Mutex<BTreeMap<String, MultipartUploadState>>,
    next_version_seq: Mutex<u64>,
}

impl LocalObjectLayer {
    pub fn new(disks: Vec<PathBuf>) -> Self {
        for disk in &disks {
            let _ = fs::create_dir_all(disk.join(MINIO_META_TMP_BUCKET));
            let _ = fs::create_dir_all(disk.join(MINIO_META_TMP_BUCKET).join(".trash"));
        }
        Self {
            disks,
            buckets: Mutex::new(BTreeMap::new()),
            objects: Mutex::new(BTreeMap::new()),
            uploads: Mutex::new(BTreeMap::new()),
            next_version_seq: Mutex::new(0),
        }
    }

    pub fn disk_paths(&self) -> &[PathBuf] {
        &self.disks
    }

    pub fn total_disk_count(&self) -> usize {
        self.disks.len()
    }

    pub fn online_disk_count(&self) -> usize {
        self.online_disks().len()
    }

    pub fn offline_disk_count(&self) -> usize {
        self.total_disk_count()
            .saturating_sub(self.online_disk_count())
    }

    pub fn has_write_quorum(&self) -> bool {
        self.online_disks().len() >= self.write_quorum_count()
    }

    pub fn has_read_quorum(&self) -> bool {
        self.online_disks().len() >= self.read_quorum_count()
    }

    pub fn can_maintain_quorum_after_one_offline(&self) -> bool {
        let remaining = self.online_disks().len().saturating_sub(1);
        remaining >= self.write_quorum_count() && remaining >= self.read_quorum_count()
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets.lock().expect("bucket state lock").len()
    }

    pub fn visible_object_count(&self) -> usize {
        let buckets = self
            .buckets
            .lock()
            .expect("bucket state lock")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        buckets
            .iter()
            .map(|bucket| self.visible_object_infos(bucket).len())
            .sum()
    }

    pub fn bucket_object_counts(&self) -> Vec<(String, usize)> {
        let buckets = self
            .buckets
            .lock()
            .expect("bucket state lock")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut counts = buckets
            .into_iter()
            .map(|bucket| {
                let count = self.visible_object_infos(&bucket).len();
                (bucket, count)
            })
            .collect::<Vec<_>>();
        counts.sort_by(|left, right| left.0.cmp(&right.0));
        counts
    }

    pub fn disk_statuses(&self) -> Vec<(String, bool)> {
        self.disks
            .iter()
            .map(|disk| (disk.display().to_string(), disk.exists()))
            .collect()
    }

    fn online_disks(&self) -> Vec<&PathBuf> {
        self.disks.iter().filter(|disk| disk.exists()).collect()
    }

    fn first_online_disk(&self) -> Result<&PathBuf, String> {
        self.ensure_write_quorum()?
            .into_iter()
            .next()
            .ok_or_else(|| cmd_err(ERR_ERASURE_WRITE_QUORUM))
    }

    fn object_path(&self, disk: &Path, bucket: &str, object: &str) -> PathBuf {
        disk.join(bucket).join(object)
    }

    fn write_quorum_count(&self) -> usize {
        (self.disks.len() / 2) + 1
    }

    fn read_quorum_count(&self) -> usize {
        self.write_quorum_count()
    }

    fn guess_content_type(object: &str) -> String {
        let lower = object.to_ascii_lowercase();
        if lower.ends_with(".png") {
            "image/png".to_string()
        } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
            "image/jpeg".to_string()
        } else if lower.ends_with(".json") {
            "application/json".to_string()
        } else if lower.ends_with(".txt") {
            "text/plain; charset=utf-8".to_string()
        } else {
            "application/octet-stream".to_string()
        }
    }

    fn collect_object_names(
        root: &Path,
        prefix: &str,
        out: &mut Vec<String>,
    ) -> Result<(), String> {
        let entries = fs::read_dir(root).map_err(|err| err.to_string())?;
        for entry in entries {
            let entry = entry.map_err(|err| err.to_string())?;
            let name = entry.file_name().to_string_lossy().to_string();
            let joined = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            let path = entry.path();
            let file_type = entry.file_type().map_err(|err| err.to_string())?;
            if file_type.is_dir() {
                if is_dir_empty(&path, false) {
                    out.push(format!("{joined}/"));
                    continue;
                }
                Self::collect_object_names(&path, &joined, out)?;
            } else if file_type.is_file() {
                out.push(joined);
            }
        }
        Ok(())
    }

    fn next_version_id(&self) -> String {
        let mut seq = self.next_version_seq.lock().expect("version seq lock");
        *seq += 1;
        format!("v{:020}", *seq)
    }

    fn bucket_state(&self, bucket: &str) -> BucketState {
        self.buckets
            .lock()
            .expect("bucket state lock")
            .get(bucket)
            .cloned()
            .unwrap_or_default()
    }

    fn is_versioned_bucket(&self, bucket: &str) -> bool {
        self.bucket_state(bucket).versioning_enabled
    }

    fn record_object_version(
        &self,
        bucket: &str,
        object: &str,
        version: StoredObjectVersion,
        append_version: bool,
    ) {
        let mut objects = self.objects.lock().expect("object state lock");
        let bucket_objects = objects.entry(bucket.to_string()).or_default();
        let versions = bucket_objects.entry(object.to_string()).or_default();
        if append_version {
            versions.push(version);
        } else {
            versions.clear();
            versions.push(version);
        }
    }

    fn current_object_version(&self, bucket: &str, object: &str) -> Option<StoredObjectVersion> {
        self.objects
            .lock()
            .expect("object state lock")
            .get(bucket)
            .and_then(|bucket_objects| bucket_objects.get(object))
            .and_then(|versions| versions.last().cloned())
    }

    fn visible_object_infos(&self, bucket: &str) -> Vec<ObjectInfo> {
        let bucket_state = self.bucket_state(bucket);
        let now = Utc::now().timestamp();
        let expiry_cutoff = bucket_state
            .expiration_days
            .map(|days| now - (days * 24 * 60 * 60));

        let mut objects = self
            .objects
            .lock()
            .expect("object state lock")
            .get(bucket)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(name, versions)| {
                let latest = versions.last()?.clone();
                if latest.delete_marker {
                    return None;
                }
                if expiry_cutoff.is_some_and(|cutoff| latest.mtime <= cutoff) {
                    return None;
                }
                Some(latest.to_object_info(bucket, &name, true))
            })
            .collect::<Vec<_>>();
        objects.sort_by(|left, right| left.name.cmp(&right.name));
        objects
    }

    fn versioned_object_infos(&self, bucket: &str) -> Vec<ObjectInfo> {
        let mut out = Vec::new();
        let objects = self.objects.lock().expect("object state lock");
        for (name, versions) in objects.get(bucket).cloned().unwrap_or_default() {
            let total = versions.len();
            for (index, version) in versions.into_iter().enumerate().rev() {
                out.push(version.to_object_info(bucket, &name, index + 1 == total));
            }
        }
        out.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| right.mod_time.cmp(&left.mod_time))
                .then_with(|| right.version_id.cmp(&left.version_id))
        });
        out
    }

    fn sync_current_version_to_disk(
        &self,
        disk: &Path,
        bucket: &str,
        object: &str,
        version: Option<&StoredObjectVersion>,
    ) -> Result<(), String> {
        let path = self.object_path(disk, bucket, object);
        if let Some(current) = version {
            if current.delete_marker {
                if path.is_file() {
                    let _ = fs::remove_file(&path);
                } else if path.is_dir() {
                    let _ = fs::remove_dir_all(&path);
                }
                return Ok(());
            }
            if current.is_dir {
                if path.is_file() {
                    let _ = fs::remove_file(&path);
                }
                fs::create_dir_all(&path).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
                return Ok(());
            }
            if path.is_dir() {
                let _ = fs::remove_dir_all(&path);
            }
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
            }
            fs::write(&path, &current.data).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
            return Ok(());
        }

        if path.is_file() {
            let _ = fs::remove_file(&path);
        } else if path.is_dir() {
            let _ = fs::remove_dir(&path);
        }
        Ok(())
    }

    fn bucket_exists_on_online_disks(&self, bucket: &str) -> Result<bool, String> {
        let online = self.ensure_write_quorum()?;
        let present = online
            .iter()
            .filter(|disk| disk.join(bucket).exists())
            .count();
        Ok(present >= self.write_quorum_count())
    }

    fn multipart_upload_dir(disk: &Path, bucket: &str, object: &str, upload_id: &str) -> PathBuf {
        disk.join(MINIO_META_MULTIPART_BUCKET)
            .join(bucket)
            .join(object)
            .join(upload_id)
    }

    fn multipart_part_path(
        disk: &Path,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: i32,
    ) -> PathBuf {
        Self::multipart_upload_dir(disk, bucket, object, upload_id)
            .join(format!("part.{part_number}"))
    }

    fn clean_etag(etag: &str) -> String {
        etag.trim_matches('"').to_string()
    }

    fn ensure_write_quorum(&self) -> Result<Vec<&PathBuf>, String> {
        let online = self.online_disks();
        if online.len() < self.write_quorum_count() {
            return Err(cmd_err(ERR_ERASURE_WRITE_QUORUM));
        }
        Ok(online)
    }

    fn ensure_read_quorum(&self) -> Result<Vec<&PathBuf>, String> {
        let online = self.online_disks();
        if online.len() < self.read_quorum_count() {
            return Err(cmd_err(ERR_ERASURE_READ_QUORUM));
        }
        Ok(online)
    }

    fn read_consensus_data(
        &self,
        bucket: &str,
        object: &str,
        expect_exists: bool,
    ) -> Result<Vec<u8>, String> {
        let online = self.ensure_read_quorum()?;
        let mut counts = BTreeMap::<Vec<u8>, usize>::new();
        let mut found_any = false;
        for disk in online {
            let path = self.object_path(disk, bucket, object);
            if !path.is_file() {
                continue;
            }
            found_any = true;
            let bytes = fs::read(&path).map_err(|_| cmd_err(ERR_ERASURE_READ_QUORUM))?;
            *counts.entry(bytes).or_insert(0) += 1;
        }

        let quorum = self.read_quorum_count();
        if let Some((bytes, count)) =
            counts
                .into_iter()
                .max_by(|(left_bytes, left_count), (right_bytes, right_count)| {
                    left_count
                        .cmp(right_count)
                        .then_with(|| left_bytes.len().cmp(&right_bytes.len()))
                })
        {
            if count >= quorum {
                return Ok(bytes);
            }
        }

        if expect_exists || found_any {
            Err(cmd_err(ERR_ERASURE_READ_QUORUM))
        } else {
            Err(cmd_err(ERR_FILE_NOT_FOUND))
        }
    }

    fn validate_put_input(
        &self,
        bucket: &str,
        object: &str,
        reader: &PutObjReader,
    ) -> Result<Vec<u8>, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty()
            || (!is_valid_object_name(object) && !(object.ends_with('/') && reader.data.is_empty()))
        {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        if reader.declared_size >= 0 && reader.data.len() as i64 > reader.declared_size {
            return Err(cmd_err(ERR_OVERREAD));
        }
        if !reader.expected_md5.is_empty() {
            let actual_md5 = get_md5_hash(&reader.data);
            if actual_md5 != reader.expected_md5 {
                return Err(cmd_err(ERR_BAD_DIGEST));
            }
        }
        if !reader.expected_sha256.is_empty() {
            let actual_sha256 = get_sha256_hash(&reader.data);
            if actual_sha256 != reader.expected_sha256 {
                return Err(cmd_err(ERR_SHA256_MISMATCH));
            }
        }
        if reader.declared_size >= 0 && (reader.data.len() as i64) < reader.declared_size {
            return Err(cmd_err(ERR_INCOMPLETE_BODY));
        }
        Ok(reader.data.clone())
    }
}
