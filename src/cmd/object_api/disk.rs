use super::*;

pub fn get_disk_info(path: &str) -> Result<(DiskInfo, String), String> {
    let metadata = fs::metadata(path).map_err(|_| cmd_err(ERR_DISK_NOT_FOUND))?;
    if !metadata.is_dir() {
        return Err(cmd_err(ERR_DISK_NOT_DIR));
    }

    let disk = DiskInfo {
        mount_path: path.to_string(),
        total: metadata.len(),
        ..DiskInfo::default()
    };
    Ok((disk, path.to_string()))
}

pub fn is_dir_empty(path: impl AsRef<Path>, _legacy: bool) -> bool {
    let path = path.as_ref();
    if !path.is_dir() {
        return false;
    }

    match fs::read_dir(path) {
        Ok(mut entries) => entries.next().is_none(),
        Err(_) => false,
    }
}
