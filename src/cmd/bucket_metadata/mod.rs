use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use super::{marshal_named, unmarshal_named};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BucketMetadata {
    pub name: String,
    pub created: i64,
    pub lock_enabled: bool,
    pub policy_config_json: Vec<u8>,
    pub notification_config_xml: Vec<u8>,
    pub lifecycle_config_xml: Vec<u8>,
    pub object_lock_config_xml: Vec<u8>,
    pub versioning_config_xml: Vec<u8>,
    pub encryption_config_xml: Vec<u8>,
    pub tagging_config_xml: Vec<u8>,
    pub quota_config_json: Vec<u8>,
    pub replication_config_xml: Vec<u8>,
    pub bucket_targets_config_json: Vec<u8>,
    pub bucket_targets_config_meta_json: Vec<u8>,
    pub policy_config_updated_at: i64,
    pub object_lock_config_updated_at: i64,
    pub encryption_config_updated_at: i64,
    pub tagging_config_updated_at: i64,
    pub quota_config_updated_at: i64,
    pub replication_config_updated_at: i64,
    pub versioning_config_updated_at: i64,
    pub lifecycle_config_updated_at: i64,
    pub notification_config_updated_at: i64,
    pub bucket_targets_config_updated_at: i64,
    pub bucket_targets_config_meta_updated_at: i64,
    pub bucket_target_config_meta: BTreeMap<String, String>,
}

impl BucketMetadata {
    pub fn marshal_msg(&self) -> Result<Vec<u8>, String> {
        marshal_named(self)
    }

    pub fn unmarshal_msg<'a>(&mut self, bytes: &'a [u8]) -> Result<&'a [u8], String> {
        *self = unmarshal_named(bytes)?;
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
