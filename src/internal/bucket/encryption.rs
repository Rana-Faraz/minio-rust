use std::fmt;
use std::io::Read;

use quick_xml::{de::from_str, se::to_string};
use serde::{Deserialize, Serialize};

pub const AES256: &str = "AES256";
pub const AWS_KMS: &str = "aws:kms";
pub const XML_NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
const ARN_PREFIX: &str = "arn:aws:kms:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename = "ServerSideEncryptionConfiguration")]
pub struct BucketSseConfig {
    #[serde(rename = "@xmlns", default, skip_serializing_if = "String::is_empty")]
    pub xmlns: String,
    #[serde(rename = "Rule")]
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    #[serde(rename = "ApplyServerSideEncryptionByDefault")]
    pub default_encryption_action: EncryptionAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionAction {
    #[serde(rename = "SSEAlgorithm")]
    pub algorithm: String,
    #[serde(
        rename = "KMSMasterKeyID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub master_key_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSseError(String);

impl BucketSseError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for BucketSseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BucketSseError {}

impl BucketSseConfig {
    pub fn algo(&self) -> &str {
        self.rules
            .first()
            .map(|rule| rule.default_encryption_action.algorithm.as_str())
            .unwrap_or("")
    }

    pub fn key_id(&self) -> String {
        self.rules
            .first()
            .map(|rule| {
                rule.default_encryption_action
                    .master_key_id
                    .strip_prefix(ARN_PREFIX)
                    .unwrap_or(&rule.default_encryption_action.master_key_id)
                    .to_owned()
            })
            .unwrap_or_default()
    }

    pub fn to_xml(&self) -> Result<String, BucketSseError> {
        to_string(self).map_err(|err| BucketSseError::new(err.to_string()))
    }
}

pub fn parse_bucket_sse_config(mut reader: impl Read) -> Result<BucketSseConfig, BucketSseError> {
    let mut xml = String::new();
    reader
        .read_to_string(&mut xml)
        .map_err(|err| BucketSseError::new(err.to_string()))?;

    if let Some((leading_space, trailing_space)) = raw_kms_key_spaces(&xml) {
        if leading_space || trailing_space {
            return Err(BucketSseError::new(
                "MasterKeyID contains unsupported characters",
            ));
        }
    }

    let mut config: BucketSseConfig =
        from_str(&xml).map_err(|err| BucketSseError::new(err.to_string()))?;

    if config.rules.len() != 1 {
        return Err(BucketSseError::new(
            "only one server-side encryption rule is allowed at a time",
        ));
    }

    for rule in &config.rules {
        match rule.default_encryption_action.algorithm.as_str() {
            AES256 => {
                if !rule.default_encryption_action.master_key_id.is_empty() {
                    return Err(BucketSseError::new(
                        "MasterKeyID is allowed with aws:kms only",
                    ));
                }
            }
            AWS_KMS => {
                let key_id = &rule.default_encryption_action.master_key_id;
                if key_id.is_empty() {
                    return Err(BucketSseError::new("MasterKeyID is missing with aws:kms"));
                }
                if key_id.starts_with(' ') || key_id.ends_with(' ') {
                    return Err(BucketSseError::new(
                        "MasterKeyID contains unsupported characters",
                    ));
                }
            }
            _ => return Err(BucketSseError::new("Unknown SSE algorithm")),
        }
    }

    if config.xmlns.is_empty() {
        config.xmlns = XML_NS.to_owned();
    }

    Ok(config)
}

fn raw_kms_key_spaces(xml: &str) -> Option<(bool, bool)> {
    let start_tag = "<KMSMasterKeyID>";
    let end_tag = "</KMSMasterKeyID>";
    let start = xml.find(start_tag)? + start_tag.len();
    let rest = &xml[start..];
    let end = rest.find(end_tag)?;
    let key = &rest[..end];
    Some((key.starts_with(' '), key.ends_with(' ')))
}
