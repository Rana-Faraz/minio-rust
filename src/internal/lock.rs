use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Mutex;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

pub const ERR_ALREADY_LOCKED: &str = "file already locked";

pub struct LockedFile {
    file: Option<File>,
}

impl LockedFile {
    pub fn close(&mut self) -> io::Result<()> {
        self.file.take();
        Ok(())
    }

    pub fn file(&self) -> Option<&File> {
        self.file.as_ref()
    }
}

pub struct RLockedFile {
    locked_file: Option<LockedFile>,
    refs: Mutex<usize>,
}

impl RLockedFile {
    pub fn is_closed(&self) -> bool {
        *self.refs.lock().expect("lock refs should lock") == 0
    }

    pub fn inc_lock_ref(&self) {
        let mut refs = self.refs.lock().expect("lock refs should lock");
        *refs += 1;
    }

    pub fn close(&mut self) -> io::Result<()> {
        let mut refs = self.refs.lock().expect("lock refs should lock");
        if *refs == 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        *refs -= 1;
        if *refs == 0 {
            if let Some(mut file) = self.locked_file.take() {
                file.close()?;
            }
        }
        Ok(())
    }
}

pub fn new_rlocked_file(locked_file: Option<LockedFile>) -> io::Result<RLockedFile> {
    let Some(locked_file) = locked_file else {
        return Err(io::Error::from(io::ErrorKind::InvalidInput));
    };

    Ok(RLockedFile {
        locked_file: Some(locked_file),
        refs: Mutex::new(1),
    })
}

pub fn rlocked_open_file(path: impl AsRef<Path>) -> io::Result<RLockedFile> {
    let locked = locked_open_file(path, libc::O_RDONLY, 0o666)?;
    new_rlocked_file(Some(locked))
}

#[cfg(unix)]
pub fn try_locked_open_file(
    path: impl AsRef<Path>,
    flag: i32,
    perm: u32,
) -> io::Result<LockedFile> {
    locked_open_file_internal(path.as_ref(), flag, perm, libc::LOCK_NB)
}

#[cfg(unix)]
pub fn locked_open_file(path: impl AsRef<Path>, flag: i32, perm: u32) -> io::Result<LockedFile> {
    locked_open_file_internal(path.as_ref(), flag, perm, 0)
}

#[cfg(unix)]
fn locked_open_file_internal(
    path: &Path,
    flag: i32,
    perm: u32,
    lock_flag: i32,
) -> io::Result<LockedFile> {
    let mut options = OpenOptions::new();
    match flag {
        libc::O_RDONLY => {
            options.read(true);
        }
        libc::O_WRONLY => {
            options.write(true);
        }
        libc::O_RDWR => {
            options.read(true).write(true);
        }
        x if x == (libc::O_WRONLY | libc::O_CREAT) => {
            options.write(true).create(true);
        }
        x if x == (libc::O_RDWR | libc::O_CREAT) => {
            options.read(true).write(true).create(true);
        }
        x if x == (libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND) => {
            options.append(true).create(true);
        }
        _ => {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
    }

    options.mode(perm).custom_flags(libc::O_SYNC);
    let file = options.open(path)?;
    let lock_type = if flag == libc::O_RDONLY {
        libc::LOCK_SH | lock_flag
    } else {
        libc::LOCK_EX | lock_flag
    };

    let flock_result = unsafe { libc::flock(file.as_raw_fd(), lock_type) };
    if flock_result != 0 {
        let error = io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(libc::EWOULDBLOCK)) {
            return Err(io::Error::other(ERR_ALREADY_LOCKED));
        }
        return Err(error);
    }

    let metadata = std::fs::metadata(path)?;
    if metadata.is_dir() {
        return Err(io::Error::from_raw_os_error(libc::EISDIR));
    }

    Ok(LockedFile { file: Some(file) })
}

#[cfg(not(unix))]
pub fn try_locked_open_file(
    path: impl AsRef<Path>,
    flag: i32,
    perm: u32,
) -> io::Result<LockedFile> {
    locked_open_file(path, flag, perm)
}

#[cfg(not(unix))]
pub fn locked_open_file(path: impl AsRef<Path>, _flag: i32, _perm: u32) -> io::Result<LockedFile> {
    let file = File::open(path)?;
    Ok(LockedFile { file: Some(file) })
}

#[cfg(windows)]
pub fn fix_long_path(path: &str) -> String {
    if path.len() < 248 {
        return path.to_owned();
    }
    if path.starts_with(r"\\") || !is_abs_windows(path) {
        return path.to_owned();
    }

    let prefix = r"\\?";
    let normalized = path.replace('/', "\\");
    let mut out = String::from(prefix);
    let mut parts = normalized.split('\\').filter(|segment| !segment.is_empty());
    if let Some(first) = parts.next() {
        out.push_str(first);
    }
    for part in parts {
        if part == "." {
            continue;
        }
        if part == ".." {
            return path.to_owned();
        }
        out.push('\\');
        out.push_str(part);
    }
    if out.len() == prefix.len() + 2 {
        out.push('\\');
    }
    out
}

#[cfg(windows)]
fn is_abs_windows(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 3 && bytes[1] == b':' && (bytes[2] == b'\\' || bytes[2] == b'/')
}

#[cfg(not(windows))]
pub fn fix_long_path(path: &str) -> String {
    path.to_owned()
}
