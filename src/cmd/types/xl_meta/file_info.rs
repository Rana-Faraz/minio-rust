use super::*;

impl FileInfo {
    pub fn add_object_part(
        &mut self,
        part_number: i32,
        part_etag: &str,
        part_size: i64,
        actual_size: i64,
    ) {
        let part_info = ObjectPartInfo {
            number: part_number,
            size: part_size,
            etag: part_etag.to_string(),
            actual_size,
        };

        let parts = self.parts.get_or_insert_with(Vec::new);
        for part in parts.iter_mut() {
            if part.number == part_number {
                *part = part_info;
                return;
            }
        }
        parts.push(part_info);
        parts.sort_by_key(|part| part.number);
    }

    pub fn object_to_part_offset(&self, offset: i64) -> Result<(usize, i64), String> {
        if offset == 0 {
            return Ok((0, 0));
        }

        let mut part_offset = offset;
        for (index, part) in self.parts.as_deref().unwrap_or(&[]).iter().enumerate() {
            if part_offset < part.size {
                return Ok((index, part_offset));
            }
            part_offset -= part.size;
        }

        Err(super::constants::ERR_INVALID_RANGE.to_string())
    }

    pub fn transition_info_equals(&self, other: &FileInfo) -> bool {
        self.transition_status == other.transition_status
            && self.transition_tier == other.transition_tier
            && self.transitioned_obj_name == other.transitioned_obj_name
            && self.transition_version_id == other.transition_version_id
    }

    pub fn set_tier_free_version_id(&mut self, version_id: &str) {
        self.metadata
            .get_or_insert_with(BTreeMap::new)
            .insert(TIER_FREE_VERSION_ID_KEY.to_string(), version_id.to_string());
    }

    pub fn tier_free_version_id(&self) -> String {
        self.metadata
            .as_ref()
            .and_then(|meta| meta.get(TIER_FREE_VERSION_ID_KEY))
            .cloned()
            .unwrap_or_default()
    }

    pub fn set_tier_free_version(&mut self) {
        self.metadata
            .get_or_insert_with(BTreeMap::new)
            .insert(TIER_FREE_MARKER_KEY.to_string(), String::new());
    }

    pub fn tier_free_version(&self) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(|meta| meta.contains_key(TIER_FREE_MARKER_KEY))
    }

    pub fn set_skip_tier_free_version(&mut self) {
        self.metadata
            .get_or_insert_with(BTreeMap::new)
            .insert(TIER_SKIP_FVID_KEY.to_string(), String::new());
    }

    pub fn skip_tier_free_version(&self) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(|meta| meta.contains_key(TIER_SKIP_FVID_KEY))
    }

    pub fn is_valid(&self) -> bool {
        if self.deleted {
            return true;
        }

        let data_blocks = self.erasure.data_blocks;
        let parity_blocks = self.erasure.parity_blocks;
        let total_blocks = data_blocks + parity_blocks;
        let distribution_len = self.erasure.distribution.as_ref().map_or(0, Vec::len) as i32;

        data_blocks >= parity_blocks
            && data_blocks > 0
            && parity_blocks >= 0
            && self.erasure.index > 0
            && self.erasure.index <= total_blocks
            && distribution_len == total_blocks
    }

    pub fn set_inline_data(&mut self) {
        self.metadata.get_or_insert_with(BTreeMap::new).insert(
            format!("{RESERVED_METADATA_PREFIX_LOWER}inline-data"),
            "true".to_string(),
        );
    }

    pub fn is_remote(&self) -> bool {
        if self.transition_status != "complete" {
            return false;
        }
        if self.expire_restored {
            return true;
        }
        !self
            .metadata
            .as_ref()
            .and_then(|meta| meta.get(AMZ_RESTORE_HEADER))
            .and_then(|header| parse_restore_header(header))
            .is_some_and(
                |status| matches!(status, RestoreStatus::Completed(expiry) if expiry > Utc::now()),
            )
    }

    pub fn is_compressed(&self) -> bool {
        self.metadata
            .as_ref()
            .is_some_and(|meta| meta.contains_key(COMPRESSION_KEY))
    }

    pub fn set_healing(&mut self) {
        self.metadata
            .get_or_insert_with(BTreeMap::new)
            .insert(HEALING_KEY.to_string(), "true".to_string());
    }

    pub fn healing(&self) -> bool {
        self.metadata
            .as_ref()
            .and_then(|meta| meta.get(HEALING_KEY))
            .is_some_and(|value| value == "true")
    }
}

pub fn object_part_index(parts: &[ObjectPartInfo], part_number: i32) -> i32 {
    parts
        .iter()
        .position(|part| part.number == part_number)
        .map(|index| index as i32)
        .unwrap_or(-1)
}

pub fn find_file_info_in_quorum(
    meta_arr: &[FileInfo],
    mod_time: i64,
    etag: &str,
    quorum: usize,
) -> Result<FileInfo, String> {
    if quorum < 1 {
        return Err(ERR_ERASURE_READ_QUORUM.to_string());
    }

    let mut meta_hashes = vec![String::new(); meta_arr.len()];
    for (index, meta) in meta_arr.iter().enumerate() {
        if !meta.is_valid() {
            continue;
        }
        let etag_only = mod_time == 0
            && !etag.is_empty()
            && meta
                .metadata
                .as_ref()
                .and_then(|map| map.get("etag"))
                .is_some_and(|value| value == etag);
        let mod_time_valid = meta.mod_time == mod_time;
        if !(mod_time_valid || etag_only) {
            continue;
        }

        let mut sig = String::new();
        sig.push_str(&format!("{}", meta.xlv1));
        for part in meta.parts.as_deref().unwrap_or(&[]) {
            sig.push_str(&format!("part.{}part.{}", part.number, part.size));
        }
        if !meta.deleted && meta.size != 0 {
            sig.push_str(&format!(
                "{}+{}{:?}",
                meta.erasure.data_blocks, meta.erasure.parity_blocks, meta.erasure.distribution
            ));
        }
        if meta.is_remote() {
            sig.push_str(&meta.transition_status);
            sig.push_str(&meta.transition_tier);
            sig.push_str(&meta.transitioned_obj_name);
            sig.push_str(&meta.transition_version_id);
        }
        if meta.is_compressed() {
            if let Some(value) = meta
                .metadata
                .as_ref()
                .and_then(|map| map.get(COMPRESSION_KEY))
            {
                sig.push_str(value);
            }
        }
        meta_hashes[index] = get_sha256_hash(sig.as_bytes());
    }

    let mut counts = BTreeMap::<String, usize>::new();
    for hash in &meta_hashes {
        if !hash.is_empty() {
            *counts.entry(hash.clone()).or_default() += 1;
        }
    }

    let mut max_hash = String::new();
    let mut max_count = 0usize;
    for (hash, count) in counts {
        if count > max_count {
            max_count = count;
            max_hash = hash;
        }
    }

    if max_count < quorum {
        return Err(ERR_ERASURE_READ_QUORUM.to_string());
    }

    let mut candidate = None;
    let mut props = BTreeMap::<(i64, i32), usize>::new();
    for (index, hash) in meta_hashes.iter().enumerate() {
        if *hash != max_hash {
            continue;
        }
        let meta = &meta_arr[index];
        if !meta.is_valid() {
            continue;
        }
        if candidate.is_none() {
            candidate = Some(meta.clone());
        }
        *props
            .entry((meta.successor_mod_time, meta.num_versions))
            .or_default() += 1;
    }

    let mut candidate = candidate.ok_or_else(|| ERR_ERASURE_READ_QUORUM.to_string())?;
    if let Some(((succ_mod_time, num_versions), count)) =
        props.into_iter().max_by_key(|entry| entry.1)
    {
        if count >= quorum {
            candidate.successor_mod_time = succ_mod_time;
            candidate.is_latest = succ_mod_time == 0;
            candidate.num_versions = num_versions;
        }
    }

    Ok(candidate)
}
