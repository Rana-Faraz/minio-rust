use super::*;

impl LocalObjectLayer {
    pub fn heal_bucket(&self, bucket: &str) -> Result<(), String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        let online = self.ensure_write_quorum()?;
        let existing = online
            .iter()
            .filter(|disk| disk.join(bucket).exists())
            .count();
        if existing < self.read_quorum_count() {
            return Err(cmd_err(ERR_ERASURE_READ_QUORUM));
        }
        for disk in online {
            fs::create_dir_all(disk.join(bucket)).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
        }
        Ok(())
    }

    pub fn heal_object(&self, bucket: &str, object: &str) -> Result<(), String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || (!is_valid_object_name(object) && !object.ends_with('/')) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }

        let online = self.ensure_write_quorum()?;
        let current = self
            .current_object_version(bucket, object)
            .ok_or_else(|| cmd_err(ERR_FILE_NOT_FOUND))?;
        if current.delete_marker {
            for disk in online {
                self.sync_current_version_to_disk(disk, bucket, object, Some(&current))?;
            }
            return Ok(());
        }

        if current.is_dir {
            let available = online
                .iter()
                .filter(|disk| self.object_path(disk, bucket, object).is_dir())
                .count();
            if available < self.read_quorum_count() {
                return Err(cmd_err(ERR_ERASURE_READ_QUORUM));
            }
        } else {
            self.read_consensus_data(bucket, object, true)?;
        }

        for disk in online {
            self.sync_current_version_to_disk(disk, bucket, object, Some(&current))?;
        }
        Ok(())
    }
}
