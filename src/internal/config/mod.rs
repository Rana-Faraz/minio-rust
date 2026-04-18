use std::fmt;
use std::sync::OnceLock;

use regex::Regex;

pub mod certs;
pub mod compress;
pub mod crypto;
pub mod dns;
pub mod etcd;
pub mod identity;
pub mod lambda;
pub mod storageclass;

pub const ENABLE_ON: &str = "on";
pub const ENABLE_OFF: &str = "off";
pub const ENV_MINIO_STS_DURATION: &str = "MINIO_STS_DURATION";
pub const MIN_EXPIRATION: i64 = 900;
pub const MAX_EXPIRATION: i64 = 31_536_000;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BoolFlag(pub bool);

impl BoolFlag {
    pub fn marshal_json(self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.to_string())
    }

    pub fn unmarshal_json(data: &[u8]) -> Result<Self, Error> {
        let value: String = serde_json::from_slice(data).map_err(Error::Json)?;
        if value.is_empty() {
            return Ok(Self(true));
        }
        parse_bool_flag(&value).map_err(Error::Parse)
    }
}

impl fmt::Display for BoolFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(format_bool(self.0))
    }
}

#[derive(Debug)]
pub enum Error {
    Json(serde_json::Error),
    Parse(ParseBoolError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => error.fmt(f),
            Self::Parse(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseBoolError {
    input: String,
}

impl fmt::Display for ParseBoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ParseBool: parsing '{}': invalid digit found in string",
            self.input
        )
    }
}

impl std::error::Error for ParseBoolError {}

pub fn format_bool(value: bool) -> &'static str {
    if value {
        "on"
    } else {
        "off"
    }
}

pub fn parse_bool(input: &str) -> Result<bool, ParseBoolError> {
    match input {
        "1" | "t" | "T" | "true" | "TRUE" | "True" | "on" | "ON" | "On" => Ok(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" | "off" | "OFF" | "Off" => Ok(false),
        _ if input.eq_ignore_ascii_case("enabled") => Ok(true),
        _ if input.eq_ignore_ascii_case("disabled") => Ok(false),
        _ => Err(ParseBoolError {
            input: input.to_owned(),
        }),
    }
}

pub fn parse_bool_flag(input: &str) -> Result<BoolFlag, ParseBoolError> {
    parse_bool(input).map(BoolFlag)
}

pub fn kv_fields(input: &str, keys: &[&str]) -> Vec<String> {
    let mut indices = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some(index) = input.find(&format!("{key}=")) {
            indices.push(index);
        }
    }

    indices.sort_unstable();

    indices
        .iter()
        .enumerate()
        .map(|(position, start)| {
            let end = indices.get(position + 1).copied().unwrap_or(input.len());
            input[*start..end].trim().to_owned()
        })
        .collect()
}

pub fn is_valid_region(region: &str) -> bool {
    static VALID_REGION_REGEX: OnceLock<Regex> = OnceLock::new();
    VALID_REGION_REGEX
        .get_or_init(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9-_-]+$").expect("regex should compile"))
        .is_match(region)
}
