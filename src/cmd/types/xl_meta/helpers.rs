use super::*;

pub fn merge_xlv2_versions(
    quorum: usize,
    strict: bool,
    _requested_versions: usize,
    version_sets: &[Vec<XlMetaV2StoredVersion>],
) -> Vec<XlMetaV2StoredVersion> {
    let mut groups: BTreeMap<Vec<u8>, (usize, XlMetaV2StoredVersion)> = BTreeMap::new();

    for set in version_sets {
        let mut seen_in_set: BTreeMap<Vec<u8>, XlMetaV2StoredVersion> = BTreeMap::new();
        for version in set {
            let key = version.merge_signature(strict);
            seen_in_set.entry(key).or_insert_with(|| version.clone());
        }
        for (key, version) in seen_in_set {
            groups
                .entry(key)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, version));
        }
    }

    let mut merged: Vec<_> = groups
        .into_iter()
        .filter_map(|(_, (count, version))| (count >= quorum).then_some(version))
        .collect();
    merged.sort_by(|left, right| {
        right
            .header
            .mod_time
            .cmp(&left.header.mod_time)
            .then_with(|| left.get_version_id().cmp(&right.get_version_id()))
    });
    merged
}

pub fn merge_entry_channels(
    receivers: Vec<Receiver<MetaCacheEntry>>,
    sender: Sender<MetaCacheEntry>,
    quorum: usize,
) -> Result<(), String> {
    let mut sets = Vec::new();
    let mut name = String::new();
    for receiver in receivers {
        let entry = receiver.recv().map_err(|err| err.to_string())?;
        if name.is_empty() {
            name = entry.name.clone();
        }
        let xl = entry.xlmeta()?;
        sets.push(xl.versions);
    }

    let merged_versions = merge_xlv2_versions(quorum, true, 0, &sets);
    let merged = XlMetaV2 {
        versions: merged_versions,
        data: XlMetaInlineData::default(),
    };
    let metadata = merged.append_to(None)?;
    sender
        .send(MetaCacheEntry { name, metadata })
        .map_err(|err| err.to_string())
}

pub fn ongoing_restore_obj() -> String {
    r#"ongoing-request="true""#.to_string()
}

pub fn completed_restore_obj(expires_at: DateTime<Utc>) -> String {
    format!(
        r#"ongoing-request="false", expiry-date="{}""#,
        expires_at.format("%a, %d %b %Y %H:%M:%S GMT")
    )
}

pub fn read_xl_meta_no_data(mut reader: impl Read, size: i64) -> Result<XlMetaV2, String> {
    if size < 0 {
        return Err("invalid size".to_string());
    }
    let mut bytes = Vec::with_capacity(size as usize);
    reader
        .read_to_end(&mut bytes)
        .map_err(|err| err.to_string())?;
    let mut xl = XlMetaV2::default();
    xl.load(&bytes)?;
    xl.data = XlMetaInlineData::default();
    Ok(xl)
}

pub fn xl_meta_v2_trim_data(bytes: &[u8]) -> Vec<u8> {
    let mut xl = XlMetaV2::default();
    if xl.load(bytes).is_err() {
        return bytes.to_vec();
    }
    xl.data = XlMetaInlineData::default();
    xl.append_to(None).unwrap_or_else(|_| bytes.to_vec())
}

pub(super) fn version_signature(version_id: &str, mod_time: i64) -> Vec<u8> {
    let mut signature = [0_u8; 4];
    for (idx, byte) in version_id.as_bytes().iter().enumerate() {
        signature[idx % signature.len()] ^= *byte;
    }
    for (idx, byte) in mod_time.to_be_bytes().iter().enumerate() {
        signature[idx % signature.len()] ^= *byte;
    }
    signature.to_vec()
}

pub(super) fn normalize_timestamps(version: &mut XlMetaV2StoredVersion) {
    let Some(object) = version.version.object_v2.as_mut() else {
        return;
    };
    let Some(meta_sys) = object.meta_sys.as_mut() else {
        return;
    };
    for key in [REPLICATION_TIMESTAMP_KEY, REPLICA_TIMESTAMP_KEY] {
        let Some(value) = meta_sys.get(key).cloned() else {
            continue;
        };
        let Ok(raw) = String::from_utf8(value) else {
            continue;
        };
        let Ok(parsed) = DateTime::parse_from_rfc3339(&raw) else {
            continue;
        };
        meta_sys.insert(
            key.to_string(),
            parsed
                .with_timezone(&Utc)
                .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
                .into_bytes(),
        );
    }
    version.header = version.version.header();
}

pub(super) enum RestoreStatus {
    Ongoing,
    Completed(DateTime<Utc>),
}

pub(super) fn parse_restore_header(header: &str) -> Option<RestoreStatus> {
    if header.contains(r#"ongoing-request="true""#) {
        return Some(RestoreStatus::Ongoing);
    }
    if !header.contains(r#"ongoing-request="false""#) {
        return None;
    }
    let marker = r#"expiry-date=""#;
    let start = header.find(marker)? + marker.len();
    let end = header[start..].find('"')? + start;
    DateTime::parse_from_rfc2822(&header[start..end])
        .ok()
        .map(|dt| RestoreStatus::Completed(dt.with_timezone(&Utc)))
}
