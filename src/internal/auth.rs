use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use rand::rngs::OsRng;
use rand::RngCore;
use serde_json::Number;
use std::error::Error as StdError;
use std::fmt;
use std::time::{Duration, SystemTime};

pub const ACCESS_KEY_MIN_LEN: usize = 3;
pub const ACCESS_KEY_MAX_LEN: usize = 20;
pub const SECRET_KEY_MIN_LEN: usize = 8;
pub const SECRET_KEY_MAX_LEN: usize = 40;
pub const ALPHA_NUMERIC_TABLE: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const RESERVED_CHARS: &str = "=,";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountStatus {
    On,
    Off,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub expiration: Option<SystemTime>,
    pub status: AccountStatus,
}

impl Credentials {
    pub fn is_expired(&self) -> bool {
        self.expiration
            .is_some_and(|value| value < SystemTime::now())
    }

    pub fn is_valid(&self) -> bool {
        self.status != AccountStatus::Off
            && is_access_key_valid(&self.access_key)
            && is_secret_key_valid(&self.secret_key)
            && !self.is_expired()
    }

    pub fn equal(&self, other: &Self) -> bool {
        other.is_valid()
            && self.access_key == other.access_key
            && self.secret_key == other.secret_key
            && self.session_token == other.session_token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidAccessKeyLength,
    InvalidSecretKeyLength,
    InvalidDuration,
    RandomFailure(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidAccessKeyLength => write!(
                f,
                "access key length should be between {} and {}",
                ACCESS_KEY_MIN_LEN, ACCESS_KEY_MAX_LEN
            ),
            Self::InvalidSecretKeyLength => write!(
                f,
                "secret key length should be between {} and {}",
                SECRET_KEY_MIN_LEN, SECRET_KEY_MAX_LEN
            ),
            Self::InvalidDuration => f.write_str("invalid token expiry"),
            Self::RandomFailure(message) => f.write_str(message),
        }
    }
}

impl StdError for Error {}

#[derive(Debug, Clone)]
pub enum ExpValue {
    String(String),
    Float64(f64),
    Int64(i64),
    Int(i64),
    Uint64(u64),
    Uint(u64),
    JsonNumber(Number),
    Duration(Duration),
    None,
}

impl From<&str> for ExpValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for ExpValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<f64> for ExpValue {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<i64> for ExpValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<i32> for ExpValue {
    fn from(value: i32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<u64> for ExpValue {
    fn from(value: u64) -> Self {
        Self::Uint64(value)
    }
}

impl From<u32> for ExpValue {
    fn from(value: u32) -> Self {
        Self::Uint(u64::from(value))
    }
}

impl From<usize> for ExpValue {
    fn from(value: usize) -> Self {
        Self::Uint(value as u64)
    }
}

impl From<Number> for ExpValue {
    fn from(value: Number) -> Self {
        Self::JsonNumber(value)
    }
}

impl From<Duration> for ExpValue {
    fn from(value: Duration) -> Self {
        Self::Duration(value)
    }
}

pub fn contains_reserved_chars(value: &str) -> bool {
    value.contains(|character| RESERVED_CHARS.contains(character))
}

pub fn is_access_key_valid(access_key: &str) -> bool {
    access_key.len() >= ACCESS_KEY_MIN_LEN
}

pub fn is_secret_key_valid(secret_key: &str) -> bool {
    secret_key.len() >= SECRET_KEY_MIN_LEN
}

pub fn exp_to_int64<T: Into<ExpValue>>(value: T) -> Result<i64, Error> {
    let parsed = match value.into() {
        ExpValue::String(value) => value.parse::<i64>().map_err(|_| Error::InvalidDuration)?,
        ExpValue::Float64(value) => value as i64,
        ExpValue::Int64(value) => value,
        ExpValue::Int(value) => value,
        ExpValue::Uint64(value) => i64::try_from(value).map_err(|_| Error::InvalidDuration)?,
        ExpValue::Uint(value) => i64::try_from(value).map_err(|_| Error::InvalidDuration)?,
        ExpValue::JsonNumber(value) => value.as_i64().ok_or(Error::InvalidDuration)?,
        ExpValue::Duration(duration) => {
            let expiration = SystemTime::now()
                .checked_add(duration)
                .ok_or(Error::InvalidDuration)?;
            expiration
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| Error::InvalidDuration)?
                .as_secs() as i64
        }
        ExpValue::None => 0,
    };

    if parsed < 0 {
        return Err(Error::InvalidDuration);
    }

    Ok(parsed)
}

pub fn get_new_credentials() -> Result<Credentials, Error> {
    let access_key = generate_access_key(ACCESS_KEY_MAX_LEN)?;
    let secret_key = generate_secret_key(SECRET_KEY_MAX_LEN)?;
    create_credentials(&access_key, &secret_key)
}

pub fn create_credentials(access_key: &str, secret_key: &str) -> Result<Credentials, Error> {
    if !(ACCESS_KEY_MIN_LEN..=ACCESS_KEY_MAX_LEN).contains(&access_key.len()) {
        return Err(Error::InvalidAccessKeyLength);
    }
    if !(SECRET_KEY_MIN_LEN..=SECRET_KEY_MAX_LEN).contains(&secret_key.len()) {
        return Err(Error::InvalidSecretKeyLength);
    }

    Ok(Credentials {
        access_key: access_key.to_owned(),
        secret_key: secret_key.to_owned(),
        session_token: String::new(),
        expiration: None,
        status: AccountStatus::On,
    })
}

fn generate_access_key(length: usize) -> Result<String, Error> {
    let mut bytes = vec![0_u8; length];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|err| Error::RandomFailure(err.to_string()))?;
    let alphabet = ALPHA_NUMERIC_TABLE.as_bytes();
    Ok(bytes
        .into_iter()
        .map(|value| alphabet[(value as usize) % alphabet.len()] as char)
        .collect())
}

fn generate_secret_key(length: usize) -> Result<String, Error> {
    let decoded_len = (length * 3) / 4;
    let mut bytes = vec![0_u8; decoded_len];
    OsRng
        .try_fill_bytes(&mut bytes)
        .map_err(|err| Error::RandomFailure(err.to_string()))?;
    let encoded = STANDARD_NO_PAD.encode(bytes);
    let secret = encoded.replace('/', "+");
    Ok(secret.chars().take(length).collect())
}
