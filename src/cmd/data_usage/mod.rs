use std::collections::BTreeMap;

use crate::cmd::{
    DataUsageCache, DataUsageCacheInfo, DataUsageEntry, SizeHistogram, SizeHistogramV1,
    VersionsHistogram,
};

const SIZE_HISTOGRAM_LABELS: [&str; 7] = [
    "0B-1KB",
    "1KB-1MB",
    "1MB-10MB",
    "10MB-64MB",
    "64MB-128MB",
    "128MB-1GB",
    "1GB+",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageUpdate {
    pub path: String,
    pub size: i64,
    pub versions: u64,
    pub delete_markers: u64,
}

impl UsageUpdate {
    pub fn new(path: impl Into<String>, size: i64) -> Self {
        Self {
            path: path.into(),
            size,
            versions: 1,
            delete_markers: 0,
        }
    }
}

pub fn size_histogram_to_map(histogram: &SizeHistogram) -> BTreeMap<String, u64> {
    histogram
        .0
        .clone()
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .filter_map(|(idx, count)| {
            (count > 0).then(|| {
                (
                    SIZE_HISTOGRAM_LABELS
                        .get(idx)
                        .unwrap_or(&"overflow")
                        .to_string(),
                    count,
                )
            })
        })
        .collect()
}

pub fn migrate_size_histogram_from_v1(histogram: &SizeHistogramV1) -> SizeHistogram {
    SizeHistogram(histogram.0.clone())
}

pub fn build_data_usage_cache(name: &str, updates: &[UsageUpdate]) -> DataUsageCache {
    build_data_usage_cache_with_prefix(name, updates, "")
}

pub fn build_data_usage_cache_with_prefix(
    name: &str,
    updates: &[UsageUpdate],
    prefix: &str,
) -> DataUsageCache {
    let mut cache = DataUsageCache {
        info: DataUsageCacheInfo {
            name: name.to_string(),
            ..DataUsageCacheInfo::default()
        },
        cache: Some(BTreeMap::new()),
    };

    for update in updates
        .iter()
        .filter(|update| prefix.is_empty() || update.path.starts_with(prefix))
    {
        apply_data_usage_update(&mut cache, update, prefix);
    }

    cache
}

pub fn serialize_data_usage_cache(cache: &DataUsageCache) -> Result<Vec<u8>, String> {
    cache.marshal_msg()
}

pub fn deserialize_data_usage_cache(bytes: &[u8]) -> Result<DataUsageCache, String> {
    let mut cache = DataUsageCache::default();
    cache.unmarshal_msg(bytes)?;
    Ok(cache)
}

fn apply_data_usage_update(cache: &mut DataUsageCache, update: &UsageUpdate, prefix: &str) {
    let relative = update
        .path
        .strip_prefix(prefix)
        .unwrap_or(update.path.as_str());
    let relative = relative.trim_matches('/');
    let mut prefixes = vec![String::new()];
    let mut current = String::new();

    let parts = relative
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    for segment in parts.iter().take(parts.len().saturating_sub(1)) {
        if !current.is_empty() {
            current.push('/');
        }
        current.push_str(segment);
        prefixes.push(current.clone());
    }

    for prefix_key in &prefixes {
        let entry = ensure_entry(cache, prefix_key);
        entry.size += update.size;
        entry.objects += 1;
        entry.versions += update.versions;
        entry.delete_markers += update.delete_markers;
        bump_size_histogram(&mut entry.obj_sizes, update.size);
        bump_versions_histogram(&mut entry.obj_versions, update.versions);
    }

    if let Some(parent) = prefixes.last() {
        let child_name = parts.last().copied().unwrap_or(relative);
        let is_dir = false;
        ensure_entry(cache, parent)
            .children
            .get_or_insert_with(BTreeMap::new)
            .insert(child_name.to_string(), is_dir);
    }

    for window in prefixes.windows(2) {
        let [parent, child] = window else { continue };
        let child_name = child.rsplit('/').next().unwrap_or(child);
        ensure_entry(cache, parent)
            .children
            .get_or_insert_with(BTreeMap::new)
            .insert(child_name.to_string(), true);
    }
}

fn ensure_entry<'a>(cache: &'a mut DataUsageCache, key: &str) -> &'a mut DataUsageEntry {
    cache
        .cache
        .get_or_insert_with(BTreeMap::new)
        .entry(key.to_string())
        .or_insert_with(|| DataUsageEntry {
            children: Some(BTreeMap::new()),
            obj_sizes: SizeHistogram(Some(vec![0; SIZE_HISTOGRAM_LABELS.len()])),
            obj_versions: VersionsHistogram(Some(vec![0; 8])),
            ..DataUsageEntry::default()
        })
}

fn bump_size_histogram(histogram: &mut SizeHistogram, size: i64) {
    let bins = histogram
        .0
        .get_or_insert_with(|| vec![0; SIZE_HISTOGRAM_LABELS.len()]);
    let index = match size {
        ..=1023 => 0,
        1024..=1_048_575 => 1,
        1_048_576..=10_485_759 => 2,
        10_485_760..=67_108_863 => 3,
        67_108_864..=134_217_727 => 4,
        134_217_728..=1_073_741_823 => 5,
        _ => 6,
    };
    bins[index] += 1;
}

fn bump_versions_histogram(histogram: &mut VersionsHistogram, versions: u64) {
    let bins = histogram.0.get_or_insert_with(|| vec![0; 8]);
    let index = versions.saturating_sub(1).min((bins.len() - 1) as u64) as usize;
    bins[index] += 1;
}
