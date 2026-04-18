use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::cmd::{LocalObjectLayer, MINIO_META_TMP_BUCKET};

pub const DEFAULT_SERVER_CONFIG_FILE: &str = "config/config.json";

pub fn resolve_server_config_file(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return PathBuf::from(DEFAULT_SERVER_CONFIG_FILE);
    }
    if path.extension().is_none() {
        return path.join("config.json");
    }
    path.to_path_buf()
}

pub fn new_object_layer(disks: Vec<PathBuf>) -> Result<LocalObjectLayer, String> {
    if disks.is_empty() {
        return Err("at least one disk is required".to_string());
    }

    let unique = disks.iter().collect::<BTreeSet<_>>();
    if unique.len() != disks.len() {
        return Err("duplicate disk paths are not allowed".to_string());
    }

    let layer = LocalObjectLayer::new(disks);
    for disk in layer.disk_paths() {
        let meta_tmp = disk.join(MINIO_META_TMP_BUCKET);
        if !meta_tmp.exists() {
            return Err(format!(
                "object layer did not initialize {}",
                meta_tmp.display()
            ));
        }
    }
    Ok(layer)
}
