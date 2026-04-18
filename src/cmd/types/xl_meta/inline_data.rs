use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct XlMetaInlineData {
    entries: BTreeMap<String, Vec<u8>>,
}

impl XlMetaInlineData {
    pub fn list(&self) -> Result<Vec<(String, Vec<u8>)>, String> {
        Ok(self
            .entries
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    pub fn find(&self, key: &str) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    pub fn remove(&mut self, key: &str) {
        self.entries.remove(key);
    }

    pub fn replace(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) {
        self.entries.insert(key.into(), value.into());
    }

    pub fn rename(&mut self, old_key: &str, new_key: &str) -> bool {
        let Some(value) = self.entries.remove(old_key) else {
            return false;
        };
        self.entries.insert(new_key.to_string(), value);
        true
    }

    pub fn entries(&self) -> usize {
        self.entries.len()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.entries.keys().any(|key| key.is_empty()) {
            return Err("inline data keys must not be empty".to_string());
        }
        Ok(())
    }
}
