use super::*;

impl LocalObjectLayer {
    pub fn remove_bucket(&self, bucket: &str) -> Result<(), String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        let online = self.ensure_write_quorum()?;
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        if self
            .objects
            .lock()
            .expect("object state lock")
            .get(bucket)
            .is_some_and(|objects| !objects.is_empty())
        {
            return Err(cmd_err(ERR_VOLUME_NOT_EMPTY));
        }
        if self
            .uploads
            .lock()
            .expect("uploads lock")
            .values()
            .any(|upload| upload.bucket == bucket)
        {
            return Err(cmd_err(ERR_VOLUME_NOT_EMPTY));
        }

        for disk in online {
            let path = disk.join(bucket);
            if path.exists() {
                fs::remove_dir_all(path).map_err(|_| cmd_err(ERR_VOLUME_NOT_EMPTY))?;
            }
        }

        self.buckets
            .lock()
            .expect("bucket state lock")
            .remove(bucket);
        self.objects
            .lock()
            .expect("object state lock")
            .remove(bucket);
        Ok(())
    }

    pub fn bucket_exists(&self, bucket: &str) -> Result<bool, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        self.bucket_exists_on_online_disks(bucket)
    }

    pub fn make_bucket(&self, bucket: &str, opts: MakeBucketOptions) -> Result<(), String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        let online = self.ensure_write_quorum()?;
        for disk in online {
            let path = disk.join(bucket);
            if path.exists() {
                return Err(cmd_err(ERR_VOLUME_EXISTS));
            }
            fs::create_dir_all(path).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
        }
        self.buckets.lock().expect("bucket state lock").insert(
            bucket.to_string(),
            BucketState {
                versioning_enabled: opts.versioning_enabled,
                expiration_days: None,
            },
        );
        Ok(())
    }

    pub fn set_bucket_expiration_days(&self, bucket: &str, days: i64) -> Result<(), String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        let mut buckets = self.buckets.lock().expect("bucket state lock");
        let state = buckets.entry(bucket.to_string()).or_default();
        state.expiration_days = Some(days);
        Ok(())
    }

    pub fn set_bucket_versioning_enabled(&self, bucket: &str, enabled: bool) -> Result<(), String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        let mut buckets = self.buckets.lock().expect("bucket state lock");
        let state = buckets.entry(bucket.to_string()).or_default();
        state.versioning_enabled = enabled;
        Ok(())
    }

    pub fn bucket_versioning_enabled(&self, bucket: &str) -> Result<bool, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        Ok(self.bucket_state(bucket).versioning_enabled)
    }

    pub fn list_buckets(&self, _opts: BucketOptions) -> Result<Vec<BucketInfo>, String> {
        let disk = self.first_online_disk()?;
        let mut buckets = Vec::new();
        for entry in fs::read_dir(disk).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))? {
            let entry = entry.map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if entry.path().is_dir() && !name.starts_with(".minio.sys") {
                buckets.push(BucketInfo { name });
            }
        }
        buckets.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(buckets)
    }
}
