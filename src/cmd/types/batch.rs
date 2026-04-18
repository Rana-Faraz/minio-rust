use super::super::*;
use serde::de::{Deserializer, Error as DeError};

fn deserialize_prefix<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum PrefixValue {
        One(String),
        Many(Vec<String>),
    }

    let value = Option::<PrefixValue>::deserialize(deserializer)?;
    Ok(match value {
        Some(PrefixValue::One(value)) => Some(vec![value]),
        Some(PrefixValue::Many(values)) => Some(values),
        None => None,
    })
}

fn deserialize_i64_durationish<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DurationValue {
        Integer(i64),
        String(String),
    }

    Ok(match DurationValue::deserialize(deserializer)? {
        DurationValue::Integer(value) => value,
        DurationValue::String(value) => value.len() as i64,
    })
}

fn deserialize_opt_i64_stringish<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MaybeValue {
        Integer(i64),
        String(String),
    }

    Ok(match Option::<MaybeValue>::deserialize(deserializer)? {
        Some(MaybeValue::Integer(value)) => Some(value),
        Some(MaybeValue::String(value)) => Some(value.len() as i64),
        None => None,
    })
}

fn parse_human_size(value: &str) -> Option<i64> {
    let trimmed = value.trim();
    let upper = trimmed.to_ascii_uppercase();
    for (suffix, scale) in [
        ("KIB", 1024_i64),
        ("MIB", 1024_i64 * 1024),
        ("GIB", 1024_i64 * 1024 * 1024),
        ("TIB", 1024_i64 * 1024 * 1024 * 1024),
    ] {
        if let Some(number) = upper.strip_suffix(suffix) {
            return number.trim().parse::<i64>().ok().map(|n| n * scale);
        }
    }
    trimmed.parse::<i64>().ok()
}

fn deserialize_batch_job_size<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SizeValue {
        Integer(i64),
        String(String),
    }

    match SizeValue::deserialize(deserializer)? {
        SizeValue::Integer(value) => Ok(value),
        SizeValue::String(value) => parse_human_size(&value)
            .map(|size| size)
            .ok_or_else(|| D::Error::custom(format!("invalid size value: {value}"))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobKV {
    pub key: String,
    pub value: String,
}
impl_msg_codec!(BatchJobKV);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobNotification {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub token: String,
}
impl_msg_codec!(BatchJobNotification);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobRetry {
    #[serde(default)]
    pub attempts: i32,
    #[serde(default, deserialize_with = "deserialize_i64_durationish")]
    pub delay: i64,
}
impl_msg_codec!(BatchJobRetry);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobSize(#[serde(deserialize_with = "deserialize_batch_job_size")] pub i64);
impl_msg_codec!(BatchJobSize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchJobYamlErr {
    pub msg: String,
}

impl BatchJobYamlErr {
    pub fn message(&self) -> &str {
        &self.msg
    }
}

impl std::fmt::Display for BatchJobYamlErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for BatchJobYamlErr {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobSizeFilter {
    #[serde(rename = "lessThan", alias = "upperBound", default)]
    pub upper_bound: BatchJobSize,
    #[serde(rename = "greaterThan", alias = "lowerBound", default)]
    pub lower_bound: BatchJobSize,
}
impl_msg_codec!(BatchJobSizeFilter);

impl BatchJobSizeFilter {
    pub fn in_range(&self, size: i64) -> bool {
        if self.lower_bound.0 > 0 && size <= self.lower_bound.0 {
            return false;
        }
        if self.upper_bound.0 > 0 && size >= self.upper_bound.0 {
            return false;
        }
        true
    }

    pub fn validate(&self) -> Result<(), BatchJobYamlErr> {
        if self.lower_bound.0 > 0
            && self.upper_bound.0 > 0
            && self.lower_bound.0 >= self.upper_bound.0
        {
            return Err(BatchJobYamlErr {
                msg: "invalid batch-job size filter".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobSnowball {
    pub disable: Option<bool>,
    pub batch: Option<i32>,
    #[serde(rename = "inmemory", alias = "inMemory")]
    pub in_memory: Option<bool>,
    pub compress: Option<bool>,
    #[serde(rename = "smallerThan")]
    pub smaller_than: Option<String>,
    #[serde(rename = "skipErrs")]
    pub skip_errs: Option<bool>,
}
impl_msg_codec!(BatchJobSnowball);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct BatchJobPrefix(
    #[serde(deserialize_with = "deserialize_prefix")] pub Option<Vec<String>>,
);
impl_msg_codec!(BatchJobPrefix);

impl BatchJobPrefix {
    pub fn f(&self) -> Vec<String> {
        self.0.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobExpirePurge {
    #[serde(default)]
    pub retain_versions: i32,
}
impl_msg_codec!(BatchJobExpirePurge);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchJobExpireFilter {
    #[serde(rename = "olderThan", deserialize_with = "deserialize_i64_durationish")]
    pub older_than: i64,
    #[serde(
        rename = "createdBefore",
        default,
        deserialize_with = "deserialize_opt_i64_stringish"
    )]
    pub created_before: Option<i64>,
    pub tags: Option<Vec<BatchJobKV>>,
    pub metadata: Option<Vec<BatchJobKV>>,
    #[serde(default)]
    pub size: BatchJobSizeFilter,
    #[serde(rename = "type")]
    pub type_name: String,
    pub name: String,
    #[serde(default)]
    pub purge: BatchJobExpirePurge,
}
impl_msg_codec!(BatchJobExpireFilter);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchJobExpire {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub bucket: String,
    #[serde(default)]
    pub prefix: BatchJobPrefix,
    #[serde(rename = "notify", default)]
    pub notification_cfg: BatchJobNotification,
    #[serde(default)]
    pub retry: BatchJobRetry,
    pub rules: Option<Vec<BatchJobExpireFilter>>,
}
impl_msg_codec!(BatchJobExpire);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchReplicateFilter {
    #[serde(rename = "newerThan", deserialize_with = "deserialize_i64_durationish")]
    pub newer_than: i64,
    #[serde(rename = "olderThan", deserialize_with = "deserialize_i64_durationish")]
    pub older_than: i64,
    #[serde(default)]
    pub created_after: i64,
    #[serde(default)]
    pub created_before: i64,
    #[serde(default)]
    pub tags: Option<Vec<BatchJobKV>>,
    #[serde(default)]
    pub metadata: Option<Vec<BatchJobKV>>,
}
impl_msg_codec!(BatchReplicateFilter);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchJobReplicateFlags {
    #[serde(default)]
    pub filter: BatchReplicateFilter,
    #[serde(rename = "notify", default)]
    pub notify: BatchJobNotification,
    #[serde(default)]
    pub retry: BatchJobRetry,
}
impl_msg_codec!(BatchJobReplicateFlags);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobReplicateCredentials {
    #[serde(rename = "accessKey")]
    #[serde(default)]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    #[serde(default)]
    pub secret_key: String,
    #[serde(rename = "sessionToken")]
    #[serde(default)]
    pub session_token: String,
}
impl_msg_codec!(BatchJobReplicateCredentials);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchJobReplicateTarget {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub path: String,
    #[serde(rename = "credentials", default)]
    pub creds: BatchJobReplicateCredentials,
}
impl_msg_codec!(BatchJobReplicateTarget);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchJobReplicateSource {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub prefix: BatchJobPrefix,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub path: String,
    #[serde(rename = "credentials", default)]
    pub creds: BatchJobReplicateCredentials,
    #[serde(default)]
    pub snowball: BatchJobSnowball,
}
impl_msg_codec!(BatchJobReplicateSource);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchJobReplicateV1 {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    #[serde(default)]
    pub flags: BatchJobReplicateFlags,
    #[serde(default)]
    pub target: BatchJobReplicateTarget,
    #[serde(default)]
    pub source: BatchJobReplicateSource,
}
impl_msg_codec!(BatchJobReplicateV1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobKeyRotateEncryption {
    pub type_name: String,
    pub key: String,
    pub context: String,
}
impl_msg_codec!(BatchJobKeyRotateEncryption);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchKeyRotateFilter {
    pub newer_than: i64,
    pub older_than: i64,
    pub created_after: i64,
    pub created_before: i64,
    pub tags: Option<Vec<BatchJobKV>>,
    pub metadata: Option<Vec<BatchJobKV>>,
    pub kms_key_id: String,
}
impl_msg_codec!(BatchKeyRotateFilter);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchKeyRotateNotification {
    pub endpoint: String,
    pub token: String,
}
impl_msg_codec!(BatchKeyRotateNotification);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobKeyRotateFlags {
    pub filter: BatchKeyRotateFilter,
    pub notify: BatchJobNotification,
    pub retry: BatchJobRetry,
}
impl_msg_codec!(BatchJobKeyRotateFlags);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobKeyRotateV1 {
    pub api_version: String,
    pub flags: BatchJobKeyRotateFlags,
    pub bucket: String,
    pub prefix: String,
    pub encryption: BatchJobKeyRotateEncryption,
}
impl_msg_codec!(BatchJobKeyRotateV1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BatchJobRequest {
    pub id: String,
    pub user: String,
    pub started: i64,
    pub replicate: Option<BatchJobReplicateV1>,
    pub key_rotate: Option<BatchJobKeyRotateV1>,
    pub expire: Option<BatchJobExpire>,
}
impl_msg_codec!(BatchJobRequest);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BatchJobInfo {
    pub version: i32,
    pub job_id: String,
    pub job_type: String,
    pub start_time: i64,
    pub last_update: i64,
    pub retry_attempts: i32,
    pub attempts: i32,
    pub complete: bool,
    pub failed: bool,
    pub bucket: String,
    pub object: String,
    pub objects: i64,
    pub delete_markers: i64,
    pub objects_failed: i64,
    pub delete_markers_failed: i64,
    pub bytes_transferred: i64,
    pub bytes_failed: i64,
}
impl_msg_codec!(BatchJobInfo);
