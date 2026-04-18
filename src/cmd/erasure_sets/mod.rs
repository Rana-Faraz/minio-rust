use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crc::{Crc, CRC_32_ISO_HDLC};

use crate::cmd::get_all_sets;

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureSets {
    pub sets: Vec<Vec<String>>,
}

impl ErasureSets {
    pub fn new(set_drive_count: u64, args: &[String]) -> Result<Self, String> {
        let sets = get_all_sets(set_drive_count, args)?;
        if sets.is_empty() {
            return Err("no erasure sets resolved".to_string());
        }
        Ok(Self { sets })
    }

    pub fn set_count(&self) -> usize {
        self.sets.len()
    }

    pub fn drives_per_set(&self) -> usize {
        self.sets.first().map(|set| set.len()).unwrap_or(0)
    }

    pub fn crc_hash_mod(&self, key: &str) -> usize {
        crc_hash_mod(key, self.sets.len())
    }

    pub fn sip_hash_mod(&self, key: &str) -> usize {
        sip_hash_mod(key, self.sets.len())
    }

    pub fn hashed_layer_crc(&self, key: &str) -> &[String] {
        &self.sets[self.crc_hash_mod(key)]
    }

    pub fn hashed_layer_sip(&self, key: &str) -> &[String] {
        &self.sets[self.sip_hash_mod(key)]
    }
}

pub fn crc_hash_mod(key: &str, cardinality: usize) -> usize {
    if cardinality == 0 {
        return 0;
    }
    (CRC32.checksum(key.as_bytes()) as usize) % cardinality
}

pub fn sip_hash_mod(key: &str, cardinality: usize) -> usize {
    if cardinality == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % cardinality
}
