use std::ffi::{CStr, CString};
use std::fs;
use std::io;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Info {
    pub total: u64,
    pub free: u64,
    pub used: u64,
    pub files: u64,
    pub ffree: u64,
    pub fs_type: String,
    pub major: u32,
    pub minor: u32,
    pub name: String,
    pub rotational: Option<bool>,
    pub nr_requests: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IoStats {
    pub read_ios: u64,
    pub read_merges: u64,
    pub read_sectors: u64,
    pub read_ticks: u64,
    pub write_ios: u64,
    pub write_merges: u64,
    pub write_sectors: u64,
    pub write_ticks: u64,
    pub current_ios: u64,
    pub total_ticks: u64,
    pub req_ticks: u64,
    pub discard_ios: u64,
    pub discard_merges: u64,
    pub discard_sectors: u64,
    pub discard_ticks: u64,
    pub flush_ios: u64,
    pub flush_ticks: u64,
}

pub fn get_info(path: impl AsRef<Path>, first_time: bool) -> io::Result<Info> {
    let path = path.as_ref();
    let c_path = CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))?;

    let mut info = platform_get_info(&c_path, path)?;
    if info.free > info.total {
        return Err(io::Error::other(format!(
            "detected free space ({}) > total drive space ({}) for {}",
            info.free,
            info.total,
            path.display()
        )));
    }
    info.used = info.total.saturating_sub(info.free);

    if first_time {
        populate_device_details(path, &mut info);
    }

    Ok(info)
}

pub fn get_drive_stats(major: u32, minor: u32) -> io::Result<IoStats> {
    read_drive_stats(format!("/sys/dev/block/{major}:{minor}/stat"))
}

pub fn read_drive_stats(path: impl AsRef<Path>) -> io::Result<IoStats> {
    let stats = read_stat(path)?;
    if stats.len() < 11 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "found invalid disk stats format",
        ));
    }

    let mut io_stats = IoStats {
        read_ios: stats[0],
        read_merges: stats[1],
        read_sectors: stats[2],
        read_ticks: stats[3],
        write_ios: stats[4],
        write_merges: stats[5],
        write_sectors: stats[6],
        write_ticks: stats[7],
        current_ios: stats[8],
        total_ticks: stats[9],
        req_ticks: stats[10],
        ..IoStats::default()
    };

    if stats.len() > 14 {
        io_stats.discard_ios = stats[11];
        io_stats.discard_merges = stats[12];
        io_stats.discard_sectors = stats[13];
        io_stats.discard_ticks = stats[14];
    }
    if stats.len() > 16 {
        io_stats.flush_ios = stats[15];
        io_stats.flush_ticks = stats[16];
    }

    Ok(io_stats)
}

fn read_stat(path: impl AsRef<Path>) -> io::Result<Vec<u64>> {
    let content = fs::read_to_string(path)?;
    content
        .split_whitespace()
        .map(|token| {
            token.parse::<u64>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid disk stat token {token:?}: {err}"),
                )
            })
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn platform_get_info(c_path: &CString, path: &Path) -> io::Result<Info> {
    use std::mem::MaybeUninit;
    use std::os::unix::fs::MetadataExt;

    let mut stat = MaybeUninit::<libc::statfs>::zeroed();
    let rc = unsafe { libc::statfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };

    let reserved_blocks = stat.f_bfree.saturating_sub(stat.f_bavail);
    let total = u64::try_from(stat.f_bsize).unwrap_or_default()
        * stat.f_blocks.saturating_sub(reserved_blocks);
    let free = u64::try_from(stat.f_bsize).unwrap_or_default() * stat.f_bavail;

    let metadata = fs::metadata(path)?;
    let dev = metadata.dev();

    Ok(Info {
        total,
        free,
        files: stat.f_files,
        ffree: stat.f_ffree,
        fs_type: linux_fs_type_name(stat.f_type),
        major: unsafe { libc::major(dev) as u32 },
        minor: unsafe { libc::minor(dev) as u32 },
        ..Info::default()
    })
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn platform_get_info(c_path: &CString, _path: &Path) -> io::Result<Info> {
    use std::mem::MaybeUninit;

    let mut stat = MaybeUninit::<libc::statfs>::zeroed();
    let rc = unsafe { libc::statfs(c_path.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };

    let reserved_blocks = stat.f_bfree.saturating_sub(stat.f_bavail);
    let total = u64::try_from(stat.f_bsize).unwrap_or_default()
        * stat.f_blocks.saturating_sub(reserved_blocks);
    let free = u64::try_from(stat.f_bsize).unwrap_or_default() * stat.f_bavail;
    let fs_type = unsafe { CStr::from_ptr(stat.f_fstypename.as_ptr()) }
        .to_string_lossy()
        .into_owned();

    Ok(Info {
        total,
        free,
        files: stat.f_files,
        ffree: stat.f_ffree,
        fs_type,
        ..Info::default()
    })
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
)))]
fn platform_get_info(_c_path: &CString, _path: &Path) -> io::Result<Info> {
    Ok(Info {
        fs_type: std::env::consts::OS.to_owned(),
        ..Info::default()
    })
}

#[cfg(target_os = "linux")]
fn populate_device_details(_path: &Path, info: &mut Info) {
    let sysfs_path = format!("/sys/dev/block/{}:{}", info.major, info.minor);
    if let Ok(target) = fs::read_link(&sysfs_path) {
        if let Some(name) = target.file_name().and_then(|name| name.to_str()) {
            info.name = name.to_owned();
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn populate_device_details(_path: &Path, _info: &mut Info) {}

#[cfg(target_os = "linux")]
fn linux_fs_type_name(fs_type: libc::c_long) -> String {
    let name = match fs_type as libc::c_ulong {
        0x5846_5342 => "xfs",
        0xEF53 => "ext4",
        0x0102_1994 => "tmpfs",
        0x794C_7630 => "overlayfs",
        0x9123_683E => "btrfs",
        0x2FC1_2FC1 => "zfs",
        0x6969 => "nfs",
        0x6573_5546 => "fuse",
        _ => "",
    };

    if name.is_empty() {
        return format!("0x{:x}", fs_type as libc::c_ulong);
    }
    name.to_owned()
}
