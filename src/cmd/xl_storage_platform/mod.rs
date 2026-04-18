use crate::internal::lock::fix_long_path;

pub fn apply_umask_to_volume_mode(mode: u32, umask: u32) -> u32 {
    mode & !umask
}

pub fn apply_umask_to_file_mode(mode: u32, umask: u32) -> u32 {
    mode & !umask
}

pub fn normalize_windows_storage_path(path: &str) -> String {
    fix_long_path(path)
}
