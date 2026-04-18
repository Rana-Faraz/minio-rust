use std::fs;
use std::path::Path;

use crate::cmd::{cmd_err, ERR_FILE_ACCESS_DENIED, ERR_FILE_NOT_FOUND};

fn map_read_dir_error(error: &std::io::Error) -> String {
    match error.kind() {
        std::io::ErrorKind::NotFound => cmd_err(ERR_FILE_NOT_FOUND),
        std::io::ErrorKind::PermissionDenied => cmd_err(ERR_FILE_ACCESS_DENIED),
        _ => error.to_string(),
    }
}

fn read_entries(path: &Path) -> Result<Vec<String>, String> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(path).map_err(|err| map_read_dir_error(&err))? {
        let entry = entry.map_err(|err| err.to_string())?;
        let metadata = entry.metadata().map_err(|err| err.to_string())?;
        let mut name = entry.file_name().to_string_lossy().into_owned();
        if metadata.is_dir() {
            name.push('/');
        }
        entries.push(name);
    }
    entries.sort();
    Ok(entries)
}

pub fn read_dir(path: impl AsRef<Path>) -> Result<Vec<String>, String> {
    read_entries(path.as_ref())
}

pub fn read_dir_n(path: impl AsRef<Path>, n: usize) -> Result<Vec<String>, String> {
    let mut entries = read_entries(path.as_ref())?;
    entries.truncate(n);
    Ok(entries)
}
