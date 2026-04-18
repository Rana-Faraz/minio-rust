use super::*;
use rand::RngCore;

pub const FORMAT_CONFIG_FILE: &str = "format.json";
pub const FORMAT_META_VERSION_V1: &str = "1";
pub const FORMAT_BACKEND_ERASURE: &str = "Erasure";
pub const FORMAT_ERASURE_VERSION_V1: &str = "1";
pub const FORMAT_ERASURE_VERSION_V3: &str = "3";
pub const FORMAT_ERASURE_V2_DISTRIBUTION_ALGO_V1: &str = "CRCMOD";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FormatMetaV1 {
    pub version: String,
    pub format: String,
    pub id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FormatErasureV1Body {
    pub version: String,
    pub disk: String,
    pub jbod: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FormatErasureV1 {
    #[serde(flatten)]
    pub meta: FormatMetaV1,
    pub erasure: FormatErasureV1Body,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FormatErasureV3Body {
    pub version: String,
    pub this: String,
    pub sets: Vec<Vec<String>>,
    #[serde(rename = "distributionAlgo")]
    pub distribution_algo: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FormatErasureV3 {
    #[serde(flatten)]
    pub meta: FormatMetaV1,
    pub erasure: FormatErasureV3Body,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageDisk {
    pub path: PathBuf,
}

fn must_get_uuid() -> String {
    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn format_path(root: &Path) -> PathBuf {
    root.join(MINIO_META_BUCKET).join(FORMAT_CONFIG_FILE)
}

fn write_format(root: &Path, format: &FormatErasureV3) -> Result<(), String> {
    let path = format_path(root);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let bytes = serde_json::to_vec(format).map_err(|err| err.to_string())?;
    fs::write(path, bytes).map_err(|err| err.to_string())
}

fn read_format(root: &Path) -> Result<FormatErasureV3, String> {
    let bytes = fs::read(format_path(root)).map_err(|_| ERR_UNFORMATTED_DISK.to_string())?;
    serde_json::from_slice(&bytes).map_err(|err| err.to_string())
}

fn flattened_set_ids(format: &FormatErasureV3) -> Vec<String> {
    format
        .erasure
        .sets
        .iter()
        .flat_map(|set| set.iter().cloned())
        .collect()
}

fn set_signature(format: &FormatErasureV3) -> String {
    let mut h = Sha256::new();
    for id in flattened_set_ids(format) {
        h.update(id.as_bytes());
    }
    hex::encode(h.finalize())
}

pub fn new_format_erasure_v3(set_count: usize, set_drive_count: usize) -> FormatErasureV3 {
    let mut sets = Vec::with_capacity(set_count);
    for _ in 0..set_count {
        let mut set = Vec::with_capacity(set_drive_count);
        for _ in 0..set_drive_count {
            set.push(must_get_uuid());
        }
        sets.push(set);
    }
    FormatErasureV3 {
        meta: FormatMetaV1 {
            version: FORMAT_META_VERSION_V1.to_string(),
            format: FORMAT_BACKEND_ERASURE.to_string(),
            id: must_get_uuid(),
        },
        erasure: FormatErasureV3Body {
            version: FORMAT_ERASURE_VERSION_V3.to_string(),
            this: String::new(),
            sets,
            distribution_algo: FORMAT_ERASURE_V2_DISTRIBUTION_ALGO_V1.to_string(),
        },
    }
}

pub fn format_erasure_v3_this_empty(formats: &[Option<FormatErasureV3>]) -> bool {
    formats
        .iter()
        .flatten()
        .any(|format| format.erasure.this.is_empty())
}

pub fn format_get_backend_erasure_version(bytes: &[u8]) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|err| err.to_string())?;
    value
        .get("erasure")
        .and_then(|v| v.get("version"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| cmd_err(ERR_FILE_CORRUPT))
}

pub fn format_erasure_migrate(root: &Path) -> Result<(Vec<u8>, FormatErasureV3), String> {
    let path = format_path(root);
    let bytes = fs::read(&path).map_err(|err| err.to_string())?;
    let v1: FormatErasureV1 = serde_json::from_slice(&bytes).map_err(|err| err.to_string())?;
    if v1.meta.format != FORMAT_BACKEND_ERASURE {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    if v1.erasure.version != FORMAT_ERASURE_VERSION_V1 {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    let v3 = FormatErasureV3 {
        meta: FormatMetaV1 {
            version: FORMAT_META_VERSION_V1.to_string(),
            format: FORMAT_BACKEND_ERASURE.to_string(),
            id: must_get_uuid(),
        },
        erasure: FormatErasureV3Body {
            version: FORMAT_ERASURE_VERSION_V3.to_string(),
            this: v1.erasure.disk.clone(),
            sets: vec![v1.erasure.jbod.clone()],
            distribution_algo: FORMAT_ERASURE_V2_DISTRIBUTION_ALGO_V1.to_string(),
        },
    };
    let migrated = serde_json::to_vec(&v3).map_err(|err| err.to_string())?;
    fs::write(path, &migrated).map_err(|err| err.to_string())?;
    Ok((migrated, v3))
}

pub fn check_format_erasure_value(
    format: &FormatErasureV3,
    expected: Option<&FormatErasureV3>,
) -> Result<(), String> {
    if format.meta.version != FORMAT_META_VERSION_V1 {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    if format.meta.format != FORMAT_BACKEND_ERASURE {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    if format.erasure.version != FORMAT_ERASURE_VERSION_V3 {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    if format.erasure.sets.is_empty() || format.erasure.sets.iter().any(Vec::is_empty) {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    if let Some(expected) = expected {
        format_erasure_v3_check(expected, format)?;
    }
    Ok(())
}

pub fn format_erasure_v3_check(
    reference: &FormatErasureV3,
    candidate: &FormatErasureV3,
) -> Result<(), String> {
    check_format_erasure_value(reference, None)?;
    check_format_erasure_value(candidate, None)?;
    if reference.meta.version != candidate.meta.version
        || reference.meta.format != candidate.meta.format
        || reference.erasure.version != candidate.erasure.version
        || reference.erasure.distribution_algo != candidate.erasure.distribution_algo
        || reference.erasure.sets != candidate.erasure.sets
    {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    if candidate.erasure.this.is_empty() {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    let ids = flattened_set_ids(candidate);
    if !ids.iter().any(|id| id == &candidate.erasure.this) {
        return Err(cmd_err(ERR_FILE_CORRUPT));
    }
    Ok(())
}

pub fn get_format_erasure_in_quorum(
    formats: &[Option<FormatErasureV3>],
) -> Result<FormatErasureV3, String> {
    let mut counts = BTreeMap::<String, usize>::new();
    let mut by_hash = BTreeMap::<String, FormatErasureV3>::new();
    for format in formats.iter().flatten() {
        let hash = set_signature(format);
        *counts.entry(hash.clone()).or_insert(0) += 1;
        by_hash.entry(hash).or_insert_with(|| format.clone());
    }

    let Some((hash, count)) = counts.into_iter().max_by_key(|(_, count)| *count) else {
        return Err(cmd_err(ERR_ERASURE_READ_QUORUM));
    };
    if count < formats.len() / 2 {
        return Err(cmd_err(ERR_ERASURE_READ_QUORUM));
    }

    let mut format = by_hash.remove(&hash).expect("quorum format");
    format.erasure.this.clear();
    Ok(format)
}

pub fn new_heal_format_sets(
    quorum_format: &FormatErasureV3,
    set_count: usize,
    set_drive_count: usize,
    formats: &[Option<FormatErasureV3>],
    errs: &[Option<String>],
) -> Result<Vec<Vec<Option<FormatErasureV3>>>, String> {
    let mut out = vec![vec![None; set_drive_count]; set_count];
    for set in 0..set_count {
        for drive in 0..set_drive_count {
            let index = set * set_drive_count + drive;
            if errs
                .get(index)
                .and_then(|err| err.as_deref())
                .is_some_and(|err| err != ERR_UNFORMATTED_DISK)
            {
                continue;
            }
            let mut format = formats
                .get(index)
                .cloned()
                .flatten()
                .unwrap_or_else(|| quorum_format.clone());
            format.meta.id = quorum_format.meta.id.clone();
            format.erasure.this = quorum_format.erasure.sets[set][drive].clone();
            format.erasure.sets = quorum_format.erasure.sets.clone();
            out[set][drive] = Some(format);
        }
    }
    Ok(out)
}

pub fn init_storage_disks_with_errors(
    roots: &[PathBuf],
) -> (Vec<StorageDisk>, Vec<Option<String>>) {
    let mut disks = Vec::with_capacity(roots.len());
    let mut errs = Vec::with_capacity(roots.len());
    for root in roots {
        if fs::create_dir_all(root).is_ok() {
            disks.push(StorageDisk { path: root.clone() });
            errs.push(None);
        } else {
            disks.push(StorageDisk { path: root.clone() });
            errs.push(Some(cmd_err(ERR_DISK_NOT_FOUND)));
        }
    }
    (disks, errs)
}

pub fn fix_format_erasure_v3(
    storage_disks: &[StorageDisk],
    formats: &mut [Option<FormatErasureV3>],
) -> Result<(), String> {
    for (index, disk) in storage_disks.iter().enumerate() {
        let Some(format) = formats.get_mut(index) else {
            continue;
        };
        let Some(current) = format.as_mut() else {
            continue;
        };
        if current.erasure.this.is_empty() {
            let ids = flattened_set_ids(current);
            if let Some(id) = ids.get(index).cloned() {
                current.erasure.this = id;
            }
        }
        write_format(&disk.path, current)?;
    }
    Ok(())
}

pub fn load_format_erasure_all(
    storage_disks: &[StorageDisk],
) -> (Vec<Option<FormatErasureV3>>, Vec<Option<String>>) {
    let mut formats = Vec::with_capacity(storage_disks.len());
    let mut errs = Vec::with_capacity(storage_disks.len());
    for disk in storage_disks {
        match read_format(&disk.path) {
            Ok(format) => {
                formats.push(Some(format));
                errs.push(None);
            }
            Err(err) if err == ERR_UNFORMATTED_DISK => {
                formats.push(None);
                errs.push(Some(err));
            }
            Err(err) => {
                formats.push(None);
                errs.push(Some(err));
            }
        }
    }
    (formats, errs)
}
