use std::fmt;
use std::io::Read;

use quick_xml::{de::from_str, se::to_string};
use regex::Regex;
use serde::{Deserialize, Serialize};

pub const ENABLED: &str = "Enabled";
pub const SUSPENDED: &str = "Suspended";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename = "VersioningConfiguration")]
pub struct Versioning {
    #[serde(rename = "@xmlns", default, skip_serializing_if = "String::is_empty")]
    pub xmlns: String,
    #[serde(rename = "Status", default)]
    pub status: String,
    #[serde(
        rename = "ExcludedPrefixes",
        default,
        skip_serializing_if = "Vec::is_empty"
    )]
    pub excluded_prefixes: Vec<ExcludedPrefix>,
    #[serde(rename = "ExcludeFolders", default, skip_serializing_if = "is_false")]
    pub exclude_folders: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExcludedPrefix {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersioningError(String);

impl VersioningError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for VersioningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for VersioningError {}

impl Versioning {
    pub fn validate(&self) -> Result<(), VersioningError> {
        match self.status.as_str() {
            ENABLED => {
                const MAX_EXCLUDED_PREFIXES: usize = 10;
                if self.excluded_prefixes.len() > MAX_EXCLUDED_PREFIXES {
                    return Err(VersioningError::new("too many excluded prefixes"));
                }
            }
            SUSPENDED => {
                if !self.excluded_prefixes.is_empty() {
                    return Err(VersioningError::new(
                        "excluded prefixes extension supported only when versioning is enabled",
                    ));
                }
            }
            _ => {
                return Err(VersioningError::new(format!(
                    "unsupported Versioning status {}",
                    self.status
                )));
            }
        }

        Ok(())
    }

    pub fn enabled(&self) -> bool {
        self.status == ENABLED
    }

    pub fn suspended(&self) -> bool {
        self.status == SUSPENDED
    }

    pub fn versioned(&self, prefix: &str) -> bool {
        self.prefix_enabled(prefix) || self.prefix_suspended(prefix)
    }

    pub fn prefix_enabled(&self, prefix: &str) -> bool {
        if self.status != ENABLED {
            return false;
        }
        if prefix.is_empty() {
            return true;
        }
        if self.exclude_folders && prefix.ends_with('/') {
            return false;
        }

        !self
            .excluded_prefixes
            .iter()
            .any(|excluded| wildcard_match_simple(&format!("{}*", excluded.prefix), prefix))
    }

    pub fn prefix_suspended(&self, prefix: &str) -> bool {
        if self.status == SUSPENDED {
            return true;
        }
        if self.status == ENABLED {
            if prefix.is_empty() {
                return false;
            }
            if self.exclude_folders && prefix.ends_with('/') {
                return true;
            }
            return self
                .excluded_prefixes
                .iter()
                .any(|excluded| wildcard_match_simple(&format!("{}*", excluded.prefix), prefix));
        }
        false
    }

    pub fn prefixes_excluded(&self) -> bool {
        !self.excluded_prefixes.is_empty() || self.exclude_folders
    }

    pub fn to_xml(&self) -> Result<String, VersioningError> {
        to_string(self).map_err(|err| VersioningError::new(err.to_string()))
    }
}

pub fn parse_config(mut reader: impl Read) -> Result<Versioning, VersioningError> {
    let mut xml = String::new();
    reader
        .read_to_string(&mut xml)
        .map_err(|err| VersioningError::new(err.to_string()))?;
    let versioning: Versioning =
        from_str(&xml).map_err(|err| VersioningError::new(err.to_string()))?;
    versioning.validate()?;
    Ok(versioning)
}

fn wildcard_match_simple(pattern: &str, text: &str) -> bool {
    let regex_pattern = format!("^{}$", regex::escape(pattern).replace("\\*", ".*"));
    Regex::new(&regex_pattern)
        .expect("wildcard regex should compile")
        .is_match(text)
}

fn is_false(value: &bool) -> bool {
    !*value
}
