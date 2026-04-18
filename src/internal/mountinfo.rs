use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::ops::{Deref, DerefMut};
use std::path::Path;

pub const PROC_MOUNTS_PATH: &str = "/proc/mounts";
const EXPECTED_NUM_FIELDS_PER_LINE: usize = 6;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountInfo {
    pub device: String,
    pub path: String,
    pub fs_type: String,
    pub options: Vec<String>,
    pub freq: String,
    pub pass: String,
}

impl fmt::Display for MountInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MountInfos(Vec<MountInfo>);

impl Deref for MountInfos {
    type Target = [MountInfo];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MountInfos {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub fn check_cross_device(abs_paths: &[&str]) -> io::Result<()> {
    check_cross_device_with_mounts(abs_paths, PROC_MOUNTS_PATH)
}

pub fn check_cross_device_with_mounts(
    abs_paths: &[&str],
    mounts_path: impl AsRef<Path>,
) -> io::Result<()> {
    let mounts = read_proc_mounts(mounts_path)?;
    for path in abs_paths {
        mounts.check_cross_mounts(path)?;
    }
    Ok(())
}

impl MountInfos {
    pub fn check_cross_mounts(&self, path: &str) -> io::Result<()> {
        if !Path::new(path).is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid argument, path ({path}) is expected to be absolute"),
            ));
        }
        let base = format!("{}/", path.trim_end_matches('/'));
        let mut cross_mounts = Vec::new();
        for mount in self.0.iter() {
            let mount_path = format!("{}/", mount.path.trim_end_matches('/'));
            if mount_path.starts_with(&base) && mount.path != path {
                cross_mounts.push(mount.clone());
            }
        }
        if !cross_mounts.is_empty() {
            return Err(io::Error::other(format!(
                "Cross-device mounts detected on path ({path}) at following locations {}. Export path should not have any sub-mounts, refusing to start.",
                DisplayMountInfos(&cross_mounts)
            )));
        }
        Ok(())
    }
}

struct DisplayMountInfos<'a>(&'a [MountInfo]);

impl fmt::Display for DisplayMountInfos<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (index, mount) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(" ")?;
            }
            write!(f, "{mount}")?;
        }
        f.write_str("]")
    }
}

pub fn read_proc_mounts(path: impl AsRef<Path>) -> io::Result<MountInfos> {
    let file = File::open(path)?;
    parse_mount_from(file)
}

pub fn parse_mount_from(reader: impl Read) -> io::Result<MountInfos> {
    let mut mounts = MountInfos::default();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }

        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() != EXPECTED_NUM_FIELDS_PER_LINE {
            continue;
        }

        fields[4].parse::<i32>().map_err(io::Error::other)?;
        fields[5].parse::<i32>().map_err(io::Error::other)?;

        mounts.0.push(MountInfo {
            device: fields[0].to_owned(),
            path: fields[1].to_owned(),
            fs_type: fields[2].to_owned(),
            options: fields[3].split(',').map(str::to_owned).collect(),
            freq: fields[4].to_owned(),
            pass: fields[5].to_owned(),
        });
    }
    Ok(mounts)
}
