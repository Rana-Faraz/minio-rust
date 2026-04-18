use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialOperation {
    pub bucket: String,
    pub object: String,
    pub version_id: String,
    pub versions: Vec<u8>,
    pub set_index: i32,
    pub pool_index: i32,
    pub queued: i64,
    pub bitrot_scan: bool,
}

impl PartialOperation {
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
