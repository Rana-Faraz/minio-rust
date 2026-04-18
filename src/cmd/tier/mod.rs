use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Write};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierConfig {
    pub name: String,
    pub tier_type: String,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierConfigMgr {
    #[serde(rename = "tiers")]
    pub tiers: BTreeMap<String, TierConfig>,
}

impl TierConfigMgr {
    pub fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec_named(self).map_err(|err| err.to_string())
    }

    pub fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        *self = rmp_serde::from_slice(bytes).map_err(|err| err.to_string())?;
        Ok(&[])
    }

    pub fn encode(&self, writer: &mut impl Write) -> Result<(), String> {
        writer
            .write_all(&self.marshal_msg()?)
            .map_err(|err| err.to_string())
    }

    pub fn decode(&mut self, reader: &mut impl Read) -> Result<(), String> {
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .map_err(|err| err.to_string())?;
        self.unmarshal_msg(&bytes)?;
        Ok(())
    }

    pub fn msgsize(&self) -> usize {
        self.marshal_msg().map(|bytes| bytes.len()).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierMetric {
    pub pending: i64,
    pub completed: i64,
    pub failed: i64,
    pub pending_bytes: i64,
    pub completed_bytes: i64,
    pub failed_bytes: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierMetrics {
    pub tiers: BTreeMap<String, TierMetric>,
}

impl TierMetrics {
    pub fn record_pending(&mut self, tier: &str, bytes: i64) {
        let metric = self.tiers.entry(tier.to_string()).or_default();
        metric.pending += 1;
        metric.pending_bytes += bytes;
    }

    pub fn record_completed(&mut self, tier: &str, bytes: i64) {
        let metric = self.tiers.entry(tier.to_string()).or_default();
        metric.completed += 1;
        metric.completed_bytes += bytes;
    }

    pub fn record_failed(&mut self, tier: &str, bytes: i64) {
        let metric = self.tiers.entry(tier.to_string()).or_default();
        metric.failed += 1;
        metric.failed_bytes += bytes;
    }

    pub fn metric(&self, tier: &str) -> TierMetric {
        self.tiers.get(tier).cloned().unwrap_or_default()
    }
}
