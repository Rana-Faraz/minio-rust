use std::fs;
use std::path::Path;

pub fn os_mkdir_all(path: impl AsRef<Path>) -> Result<(), String> {
    fs::create_dir_all(path.as_ref()).map_err(|err| err.to_string())
}

pub fn os_rename_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), String> {
    let src = src.as_ref();
    let dst = dst.as_ref();
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    fs::rename(src, dst).map_err(|err| err.to_string())
}
