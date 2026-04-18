use std::collections::BTreeMap;

use crate::cmd::*;
use crc::{Crc, CRC_32_ISO_HDLC};

pub const TIME_SENTINEL: i64 = 0;
const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

fn common_etags(etags: &[String]) -> (String, usize) {
    let mut occurrences = BTreeMap::<String, usize>::new();
    for etag in etags {
        if etag.is_empty() {
            continue;
        }
        *occurrences.entry(etag.clone()).or_default() += 1;
    }

    let mut maxima = 0;
    let mut latest = String::new();
    for (etag, count) in occurrences {
        if count < maxima {
            continue;
        }
        if count > maxima {
            maxima = count;
            latest = etag;
        }
    }

    (latest, maxima)
}

pub fn common_time(times: &[i64], quorum: usize) -> i64 {
    let mut occurrences = BTreeMap::<i64, usize>::new();
    for time in times {
        if *time == TIME_SENTINEL {
            continue;
        }
        *occurrences.entry(*time).or_default() += 1;
    }

    let mut maxima = 0;
    let mut latest = TIME_SENTINEL;
    for (time, count) in occurrences {
        if count < maxima {
            continue;
        }
        if count > maxima || time > latest {
            maxima = count;
            latest = time;
        }
    }

    if maxima >= quorum {
        latest
    } else {
        TIME_SENTINEL
    }
}

fn common_etag(etags: &[String], quorum: usize) -> String {
    let (etag, count) = common_etags(etags);
    if count >= quorum {
        etag
    } else {
        String::new()
    }
}

fn list_object_modtimes(parts_metadata: &[FileInfo], errs: &[Option<&str>]) -> Vec<i64> {
    let mut mod_times = vec![TIME_SENTINEL; parts_metadata.len()];
    for (index, metadata) in parts_metadata.iter().enumerate() {
        if errs.get(index).and_then(|err| *err).is_some() {
            continue;
        }
        mod_times[index] = metadata.mod_time;
    }
    mod_times
}

fn list_object_etags(
    parts_metadata: &[FileInfo],
    errs: &[Option<&str>],
    quorum: usize,
) -> Vec<String> {
    let mut etags = vec![String::new(); parts_metadata.len()];
    let mut version_counts = BTreeMap::<String, usize>::new();

    for (index, metadata) in parts_metadata.iter().enumerate() {
        if errs.get(index).and_then(|err| *err).is_some() {
            continue;
        }
        let version_id = if metadata.version_id.is_empty() {
            "null".to_string()
        } else {
            metadata.version_id.clone()
        };
        *version_counts.entry(version_id).or_default() += 1;
        etags[index] = metadata
            .metadata
            .as_ref()
            .and_then(|meta| meta.get("etag"))
            .cloned()
            .unwrap_or_default();
    }

    if version_counts.values().any(|count| *count >= quorum) {
        etags
    } else {
        vec![String::new(); parts_metadata.len()]
    }
}

pub fn list_online_disks<T: Clone>(
    disks: &[T],
    parts_metadata: &[FileInfo],
    errs: &[Option<&str>],
    quorum: usize,
) -> (Vec<Option<T>>, i64, String) {
    let mut online_disks = vec![None; disks.len()];
    let mod_times = list_object_modtimes(parts_metadata, errs);
    let mod_time = common_time(&mod_times, quorum);

    if mod_time == TIME_SENTINEL {
        let etags = list_object_etags(parts_metadata, errs, quorum);
        let etag = common_etag(&etags, quorum);
        if !etag.is_empty() {
            for (index, disk) in disks.iter().enumerate() {
                if parts_metadata[index].is_valid()
                    && etags.get(index).is_some_and(|value| value == &etag)
                {
                    online_disks[index] = Some(disk.clone());
                }
            }
            return (online_disks, mod_time, etag);
        }
    }

    for (index, disk) in disks.iter().enumerate() {
        if parts_metadata[index].is_valid() && mod_times.get(index).copied() == Some(mod_time) {
            online_disks[index] = Some(disk.clone());
        }
    }

    (online_disks, mod_time, String::new())
}

pub fn common_parity(parities: &[i32], default_parity_count: i32) -> i32 {
    let total_shards = parities.len() as i32;
    let mut occurrences = BTreeMap::<i32, i32>::new();
    for parity in parities {
        *occurrences.entry(*parity).or_default() += 1;
    }

    let mut max_occurrence = 0;
    let mut common_parity = 0;
    for (parity, occurrence) in occurrences {
        if parity == -1 {
            continue;
        }

        let mut read_quorum = total_shards - parity;
        if default_parity_count > 0 && parity == 0 {
            read_quorum = total_shards / 2 + 1;
        }
        if occurrence < read_quorum {
            continue;
        }
        if occurrence > max_occurrence {
            max_occurrence = occurrence;
            common_parity = parity;
        }
    }

    if max_occurrence == 0 {
        -1
    } else {
        common_parity
    }
}

pub fn disk_count<T>(disks: &[Option<T>]) -> usize {
    disks.iter().filter(|disk| disk.is_some()).count()
}

pub fn reduce_errs(errs: &[Option<&str>], ignored_errs: &[&str]) -> (usize, Option<String>) {
    let mut error_counts = BTreeMap::<Option<String>, usize>::new();
    for err in errs {
        let normalized = match err {
            Some(value) if ignored_errs.iter().any(|ignored| ignored == value) => continue,
            Some(value) if value.contains("context canceled") => {
                Some("context canceled".to_string())
            }
            Some(value) => Some((*value).to_string()),
            None => None,
        };
        *error_counts.entry(normalized).or_default() += 1;
    }

    let mut max_count = 0usize;
    let mut max_err = None;
    for (err, count) in error_counts {
        if count > max_count || (count == max_count && err.is_none()) {
            max_count = count;
            max_err = err;
        }
    }
    (max_count, max_err)
}

pub fn reduce_read_quorum_errs(
    errs: &[Option<&str>],
    ignored_errs: &[&str],
    read_quorum: usize,
) -> Option<String> {
    let (max_count, max_err) = reduce_errs(errs, ignored_errs);
    if max_count >= read_quorum {
        max_err
    } else {
        Some(ERR_ERASURE_READ_QUORUM.to_string())
    }
}

pub fn reduce_write_quorum_errs(
    errs: &[Option<&str>],
    ignored_errs: &[&str],
    write_quorum: usize,
) -> Option<String> {
    let (max_count, max_err) = reduce_errs(errs, ignored_errs);
    if max_count >= write_quorum {
        max_err
    } else {
        Some(ERR_ERASURE_WRITE_QUORUM.to_string())
    }
}

pub fn hash_order_bytes(key: &[u8], cardinality: i32) -> Option<Vec<i32>> {
    if cardinality <= 0 {
        return None;
    }
    let key_crc = CRC32.checksum(key);
    let start = (key_crc % cardinality as u32) as i32;
    let mut nums = Vec::with_capacity(cardinality as usize);
    for i in 1..=cardinality {
        nums.push(1 + ((start + i) % cardinality));
    }
    Some(nums)
}

pub fn hash_order(key: &str, cardinality: i32) -> Option<Vec<i32>> {
    hash_order_bytes(key.as_bytes(), cardinality)
}

pub fn shuffle_disks<T: Clone>(disks: &[T], distribution: &[i32]) -> Vec<T> {
    let mut shuffled = vec![disks[0].clone(); disks.len()];
    for (index, disk) in disks.iter().enumerate() {
        let block_index = distribution[index] as usize;
        shuffled[block_index - 1] = disk.clone();
    }
    shuffled
}

pub fn eval_disks<T: Clone>(disks: &[T], errs: &[Option<&str>]) -> Option<Vec<Option<T>>> {
    if errs.len() != disks.len() {
        return None;
    }
    Some(
        disks
            .iter()
            .enumerate()
            .map(|(index, disk)| {
                if errs[index].is_none() {
                    Some(disk.clone())
                } else {
                    None
                }
            })
            .collect(),
    )
}

pub fn list_object_parities(parts_metadata: &[FileInfo], errs: &[Option<&str>]) -> Vec<i32> {
    let total_shards = parts_metadata.len() as i32;
    parts_metadata
        .iter()
        .enumerate()
        .map(|(index, metadata)| {
            if errs.get(index).and_then(|err| *err).is_some() {
                -1
            } else if !metadata.is_valid() {
                -1
            } else if metadata.deleted || metadata.size == 0 {
                total_shards / 2
            } else if metadata.transition_status == "complete" {
                (total_shards - (total_shards / 2 + 1)).max(metadata.erasure.parity_blocks)
            } else {
                let parity = metadata.erasure.parity_blocks;
                if parity < 0 || parity >= total_shards {
                    -1
                } else {
                    parity
                }
            }
        })
        .collect()
}

pub fn part_needs_healing(part_errs: &[i32]) -> bool {
    part_errs
        .iter()
        .any(|part_err| *part_err != CHECK_PART_SUCCESS && *part_err != CHECK_PART_UNKNOWN)
}

pub fn check_object_with_all_parts<T: Clone>(
    online_disks: &mut [Option<T>],
    parts_metadata: &[FileInfo],
    errs: &[Option<&str>],
    latest_meta: &FileInfo,
    simulated_part_errors: &BTreeMap<usize, Vec<i32>>,
) -> Vec<Vec<i32>> {
    let part_count = latest_meta.parts.as_ref().map_or(0, Vec::len);
    let mut data_errs_per_disk = vec![vec![CHECK_PART_SUCCESS; part_count]; online_disks.len()];

    for index in 0..online_disks.len() {
        if errs.get(index).and_then(|err| *err).is_some() {
            online_disks[index] = None;
            continue;
        }

        let Some(meta) = parts_metadata.get(index) else {
            online_disks[index] = None;
            continue;
        };

        if !meta.is_valid() {
            online_disks[index] = None;
            continue;
        }

        let corrupted =
            meta.mod_time != latest_meta.mod_time || meta.data_dir != latest_meta.data_dir;
        if corrupted {
            online_disks[index] = None;
            continue;
        }

        if let Some(simulated) = simulated_part_errors.get(&index) {
            data_errs_per_disk[index] = simulated.clone();
        }
    }

    data_errs_per_disk
}
