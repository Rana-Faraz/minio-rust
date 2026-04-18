use super::*;

impl LocalObjectLayer {
    fn check_multipart_preconditions(
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

    pub fn new_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: ObjectOptions,
    ) -> Result<NewMultipartUploadResult, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || !is_valid_object_name(object) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        let online = self.ensure_write_quorum()?;
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        self.check_multipart_preconditions(bucket, object, &opts)?;
        let upload_id = format!("upload-{}", rand::random::<u64>());
        for disk in &online {
            fs::create_dir_all(Self::multipart_upload_dir(disk, bucket, object, &upload_id))
                .map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
        }
        self.uploads.lock().expect("uploads lock").insert(
            upload_id.clone(),
            MultipartUploadState {
                bucket: bucket.to_string(),
                object: object.to_string(),
                user_defined: opts.user_defined,
                parts: BTreeMap::new(),
            },
        );
        Ok(NewMultipartUploadResult { upload_id })
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        _opts: ObjectOptions,
    ) -> Result<(), String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        let mut uploads = self.uploads.lock().expect("uploads lock");
        let upload = uploads
            .get(upload_id)
            .ok_or_else(|| format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"))?;
        if upload.bucket != bucket || upload.object != object {
            return Err(format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"));
        }
        uploads.remove(upload_id);
        drop(uploads);
        for disk in self.online_disks() {
            let _ = fs::remove_dir_all(Self::multipart_upload_dir(disk, bucket, object, upload_id));
        }
        Ok(())
    }

    pub fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: i32,
        reader: &PutObjReader,
        _opts: ObjectOptions,
    ) -> Result<MultipartPartInfo, String> {
        let data = self.validate_put_input(bucket, object, reader)?;
        let etag = get_md5_hash(&data);
        let online = self.ensure_write_quorum()?;
        let mut uploads = self.uploads.lock().expect("uploads lock");
        let upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"))?;
        if upload.bucket != bucket || upload.object != object {
            return Err(format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"));
        }
        for disk in online {
            let path = Self::multipart_part_path(disk, bucket, object, upload_id, part_number);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
            }
            fs::write(path, &data).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
        }
        upload.parts.insert(
            part_number,
            MultipartStoredPart {
                etag: etag.clone(),
                size: data.len() as i64,
                data: data.clone(),
            },
        );
        Ok(MultipartPartInfo {
            etag,
            part_number,
            size: data.len() as i64,
        })
    }

    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: &str,
        delimiter: &str,
        max_uploads: i32,
    ) -> Result<ListMultipartsInfo, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        if !upload_id_marker.is_empty() && key_marker.is_empty() {
            return Err(format!(
                "invalid combination of upload id marker '{upload_id_marker}' and marker '{key_marker}'"
            ));
        }
        if upload_id_marker.contains('=') {
            return Err(format!("malformed upload id {upload_id_marker}"));
        }
        let mut result = ListMultipartsInfo {
            key_marker: key_marker.to_string(),
            upload_id_marker: upload_id_marker.to_string(),
            max_uploads,
            prefix: prefix.to_string(),
            delimiter: delimiter.to_string(),
            ..ListMultipartsInfo::default()
        };
        if !delimiter.is_empty() && delimiter != SLASH_SEPARATOR {
            return Ok(result);
        }

        let uploads = self.uploads.lock().expect("uploads lock");
        let mut items: Vec<MultipartInfo> = uploads
            .iter()
            .filter(|(_, upload)| upload.bucket == bucket)
            .map(|(upload_id, upload)| MultipartInfo {
                object: upload.object.clone(),
                upload_id: upload_id.clone(),
            })
            .collect();
        items.sort_by(|left, right| {
            left.object
                .cmp(&right.object)
                .then(left.upload_id.cmp(&right.upload_id))
        });

        let filtered: Vec<MultipartInfo> = items
            .into_iter()
            .filter(|item| prefix.is_empty() || item.object.starts_with(prefix))
            .filter(|item| {
                if key_marker.is_empty() {
                    return true;
                }
                if item.object.as_str() > key_marker {
                    return true;
                }
                item.object == key_marker
                    && !upload_id_marker.is_empty()
                    && item.upload_id.as_str() > upload_id_marker
            })
            .collect();

        if max_uploads <= 0 {
            result.is_truncated = !filtered.is_empty();
            return Ok(result);
        }

        let max_uploads = max_uploads as usize;
        result.is_truncated = filtered.len() > max_uploads;
        result.uploads = filtered.into_iter().take(max_uploads).collect();
        if result.is_truncated {
            if let Some(last) = result.uploads.last() {
                result.next_key_marker = last.object.clone();
                result.next_upload_id_marker = last.upload_id.clone();
            }
        }
        Ok(result)
    }

    pub fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: i32,
        max_parts: i32,
        _opts: ObjectOptions,
    ) -> Result<ListPartsInfo, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || !is_valid_object_name(object) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        let uploads = self.uploads.lock().expect("uploads lock");
        let upload = uploads
            .get(upload_id)
            .ok_or_else(|| format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"))?;
        if upload.bucket != bucket || upload.object != object {
            return Err(format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"));
        }
        let online = self.online_disks();
        let mut parts: Vec<PartInfo> = upload
            .parts
            .iter()
            .filter_map(|(part_number, part)| {
                let exists = online.iter().any(|disk| {
                    Self::multipart_part_path(disk, bucket, object, upload_id, *part_number)
                        .exists()
                });
                exists.then(|| PartInfo {
                    part_number: *part_number,
                    size: part.size,
                    etag: part.etag.clone(),
                })
            })
            .filter(|part| part.part_number > part_number_marker)
            .collect();
        parts.sort_by_key(|part| part.part_number);

        let mut result = ListPartsInfo {
            bucket: bucket.to_string(),
            object: object.to_string(),
            upload_id: upload_id.to_string(),
            part_number_marker,
            max_parts,
            ..ListPartsInfo::default()
        };

        if max_parts <= 0 {
            result.is_truncated = !parts.is_empty();
            return Ok(result);
        }
        let max_parts = max_parts as usize;
        result.is_truncated = parts.len() > max_parts;
        result.parts = parts.into_iter().take(max_parts).collect();
        if result.is_truncated {
            if let Some(last) = result.parts.last() {
                result.next_part_number_marker = last.part_number;
            }
        }
        Ok(result)
    }

    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        parts: &[CompletePart],
        opts: ObjectOptions,
    ) -> Result<ObjectInfo, String> {
        if !is_valid_bucket_name(bucket) {
            return Err(cmd_err(ERR_BUCKET_NAME_INVALID));
        }
        if object.is_empty() || !is_valid_object_name(object) {
            return Err(cmd_err(ERR_OBJECT_NAME_INVALID));
        }
        if !self.bucket_exists_on_online_disks(bucket)? {
            return Err(cmd_err(ERR_BUCKET_NOT_FOUND));
        }
        self.check_multipart_preconditions(bucket, object, &opts)?;
        let mut uploads = self.uploads.lock().expect("uploads lock");
        let upload = uploads
            .get(upload_id)
            .ok_or_else(|| format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"))?;
        if upload.bucket != bucket || upload.object != object {
            return Err(format!("{ERR_INVALID_UPLOAD_ID}: {upload_id}"));
        }

        let mut data = Vec::new();
        let mut md5_parts = Vec::new();
        let mut object_parts = Vec::new();
        for (idx, part) in parts.iter().enumerate() {
            let part_number = if part.part_number > 0 {
                part.part_number
            } else {
                idx as i32 + 1
            };
            if part_number <= 0 {
                return Err(cmd_err(ERR_INVALID_PART));
            }
            let stored = upload
                .parts
                .get(&part_number)
                .ok_or_else(|| cmd_err(ERR_INVALID_PART))?;
            let cleaned_etag = Self::clean_etag(&part.etag);
            if !cleaned_etag.is_empty() && stored.etag != cleaned_etag {
                return Err(cmd_err(ERR_INVALID_PART));
            }
            if parts.len() > 1 && idx + 1 != parts.len() && stored.size < 5 * 1024 * 1024 {
                return Err(cmd_err(ERR_PART_TOO_SMALL));
            }
            data.extend(&stored.data);
            md5_parts.push(CompletePart {
                etag: stored.etag.clone(),
                part_number,
            });
            object_parts.push(ObjectPartInfo {
                number: part_number,
                size: stored.size,
                etag: stored.etag.clone(),
                actual_size: stored.size,
            });
        }
        let mut user_defined = upload.user_defined.clone();
        uploads.remove(upload_id);
        drop(uploads);
        let multipart_etag = get_complete_multipart_md5(&md5_parts);
        user_defined.extend(opts.user_defined);
        user_defined.insert("etag".to_string(), multipart_etag.clone());
        let mut info = self.put_object(
            bucket,
            object,
            &PutObjReader {
                data,
                declared_size: -1,
                expected_md5: String::new(),
                expected_sha256: String::new(),
            },
            ObjectOptions {
                user_defined,
                ..ObjectOptions::default()
            },
        )?;
        info.etag = multipart_etag;
        info.parts = object_parts.clone();
        if let Some(bucket_objects) = self
            .objects
            .lock()
            .expect("object state lock")
            .get_mut(bucket)
        {
            if let Some(versions) = bucket_objects.get_mut(object) {
                if let Some(current) = versions.last_mut() {
                    current.parts = object_parts;
                    current.etag = info.etag.clone();
                    current.user_defined = info.user_defined.clone();
                }
            }
        }
        for disk in self.online_disks() {
            let _ = fs::remove_dir_all(Self::multipart_upload_dir(disk, bucket, object, upload_id));
        }
        Ok(info)
    }
}
