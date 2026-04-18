use crate::cmd::{
    merge_xlv2_versions, XlMetaInlineData, XlMetaV2, ERR_FILE_NOT_FOUND, SLASH_SEPARATOR,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetaCacheEntryExt {
    pub name: String,
    pub metadata: Vec<u8>,
    pub cached: Option<XlMetaV2>,
    pub reusable: bool,
}

impl MetaCacheEntryExt {
    pub fn is_dir(&self) -> bool {
        self.metadata.is_empty() && self.name.ends_with(SLASH_SEPARATOR)
    }

    pub fn is_object(&self) -> bool {
        !self.metadata.is_empty()
    }

    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.name.starts_with(prefix)
    }

    pub fn is_in_dir(&self, dir: &str, separator: &str) -> bool {
        if dir.is_empty() {
            return self
                .name
                .find(separator)
                .is_none_or(|idx| idx == self.name.len() - separator.len());
        }
        let ext = self.name.strip_prefix(dir).unwrap_or(&self.name);
        if ext.len() == self.name.len() {
            return false;
        }
        ext.find(separator)
            .is_none_or(|idx| idx == ext.len() - separator.len())
    }

    pub fn xlmeta(&self) -> Result<XlMetaV2, String> {
        if self.is_dir() || self.metadata.is_empty() {
            return Err(ERR_FILE_NOT_FOUND.to_string());
        }
        let mut xl = XlMetaV2::default();
        xl.load_or_convert(&self.metadata)?;
        Ok(xl)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetadataResolutionParams {
    pub dir_quorum: usize,
    pub obj_quorum: usize,
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MetaCacheEntriesSorted {
    entries: Vec<MetaCacheEntryExt>,
}

impl MetaCacheEntriesSorted {
    pub fn new(entries: Vec<MetaCacheEntryExt>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[MetaCacheEntryExt] {
        &self.entries
    }

    pub fn entries_mut(&mut self) -> &mut Vec<MetaCacheEntryExt> {
        &mut self.entries
    }

    pub fn names(&self) -> Vec<String> {
        self.entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect()
    }

    pub fn is_sorted(&self) -> bool {
        self.entries
            .windows(2)
            .all(|window| window[0].name <= window[1].name)
    }

    pub fn sort(&mut self) {
        self.entries
            .sort_by(|left, right| left.name.cmp(&right.name));
    }

    pub fn shallow_clone(&self) -> Self {
        self.clone()
    }

    pub fn forward_to(&mut self, name: &str) {
        if name.is_empty() {
            return;
        }
        let idx = self
            .entries
            .partition_point(|entry| entry.name.as_str() < name);
        self.entries = self.entries.split_off(idx);
    }

    pub fn merge(&mut self, other: Self, limit: isize) {
        let mut merged = Vec::with_capacity(self.entries.len() + other.entries.len());
        let mut left = self.entries.clone().into_iter().peekable();
        let mut right = other.entries.into_iter().peekable();

        while let (Some(l), Some(r)) = (left.peek(), right.peek()) {
            if l.name == r.name && l.metadata == r.metadata {
                merged.push(left.next().expect("left"));
                right.next();
            } else if l.name <= r.name {
                merged.push(left.next().expect("left"));
            } else {
                merged.push(right.next().expect("right"));
            }
            if limit > 0 && merged.len() >= limit as usize {
                self.entries = merged;
                return;
            }
        }

        merged.extend(left);
        merged.extend(right);
        if limit > 0 {
            merged.truncate(limit as usize);
        }
        self.entries = merged;
    }

    pub fn filter_objects_only(&mut self) {
        self.entries.retain(|entry| !entry.is_dir());
    }

    pub fn filter_prefixes_only(&mut self) {
        self.entries.retain(MetaCacheEntryExt::is_dir);
    }

    pub fn filter_recursive_entries(&mut self, prefix: &str, separator: &str) {
        if prefix.is_empty() {
            self.entries.retain(|entry| !entry.name.contains(separator));
            return;
        }

        self.forward_to(prefix);
        self.entries.retain(|entry| {
            let Some(ext) = entry.name.strip_prefix(prefix) else {
                return false;
            };
            !ext.contains(separator)
        });
    }

    pub fn filter_prefix(&mut self, prefix: &str) {
        if prefix.is_empty() {
            return;
        }
        self.forward_to(prefix);
        self.entries.retain(|entry| entry.has_prefix(prefix));
    }

    pub fn resolve(
        entries: &[MetaCacheEntryExt],
        params: &MetadataResolutionParams,
    ) -> Option<MetaCacheEntryExt> {
        let mut selected_dir: Option<MetaCacheEntryExt> = None;
        let mut dir_exists = 0usize;
        let mut valid_entries = Vec::new();
        let mut version_sets = Vec::new();

        for entry in entries.iter().filter(|entry| !entry.name.is_empty()) {
            if entry.is_dir() {
                dir_exists += 1;
                selected_dir.get_or_insert_with(|| entry.clone());
                continue;
            }

            let Ok(xl) = entry.xlmeta() else {
                continue;
            };
            version_sets.push(xl.versions.clone());
            valid_entries.push((entry.clone(), xl));
        }

        if let Some(directory) = selected_dir {
            if dir_exists >= params.dir_quorum {
                return Some(directory);
            }
        }

        if valid_entries.len() < params.obj_quorum || valid_entries.is_empty() {
            return None;
        }

        if params.obj_quorum > 1 {
            for (entry, _) in &valid_entries {
                let count = valid_entries
                    .iter()
                    .filter(|(other, _)| other.metadata == entry.metadata)
                    .count();
                if count >= params.obj_quorum {
                    return Some(entry.clone());
                }
            }
        }

        let merged = merge_xlv2_versions(params.obj_quorum, params.strict, 0, &version_sets);
        if merged.is_empty() {
            return None;
        }

        let merged_xl = XlMetaV2 {
            versions: merged,
            data: XlMetaInlineData::default(),
        };
        let metadata = merged_xl.append_to(None).ok()?;
        Some(MetaCacheEntryExt {
            name: valid_entries[0].0.name.clone(),
            metadata,
            cached: Some(merged_xl),
            reusable: true,
        })
    }
}
