use std::collections::HashMap;
use std::fmt;
use std::io::Read;

use chrono::{DateTime, NaiveDate, Utc};
use quick_xml::de::from_str as from_xml_str;
use serde::{Deserialize, Serialize};

use crate::internal::amztime;

pub const ENABLED: &str = "Enabled";
pub const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";
pub const AMZ_OBJECT_LOCK_BYPASS_RET_GOVERNANCE: &str = "X-Amz-Bypass-Governance-Retention";
pub const AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE: &str = "X-Amz-Object-Lock-Retain-Until-Date";
pub const AMZ_OBJECT_LOCK_MODE: &str = "X-Amz-Object-Lock-Mode";
pub const AMZ_OBJECT_LOCK_LEGAL_HOLD: &str = "X-Amz-Object-Lock-Legal-Hold";

const MAXIMUM_RETENTION_DAYS: u64 = 36_500;
const MAXIMUM_RETENTION_YEARS: u64 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidRetentionDate,
    PastObjectLockRetainDate,
    UnknownWormModeDirective,
    ObjectLockInvalidHeaders,
    MalformedXml,
    Message(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRetentionDate => f.write_str("date must be provided in ISO 8601 format"),
            Self::PastObjectLockRetainDate => f.write_str("the retain until date must be in the future"),
            Self::UnknownWormModeDirective => f.write_str("unknown WORM mode directive"),
            Self::ObjectLockInvalidHeaders => f.write_str(
                "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied",
            ),
            Self::MalformedXml => f.write_str(
                "the XML you provided was not well-formed or did not validate against our published schema",
            ),
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RetMode {
    #[default]
    #[serde(rename = "")]
    Empty,
    #[serde(rename = "GOVERNANCE")]
    Governance,
    #[serde(rename = "COMPLIANCE")]
    Compliance,
}

impl RetMode {
    pub fn valid(self) -> bool {
        matches!(self, Self::Governance | Self::Compliance)
    }
}

impl fmt::Display for RetMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str(""),
            Self::Governance => f.write_str("GOVERNANCE"),
            Self::Compliance => f.write_str("COMPLIANCE"),
        }
    }
}

pub fn parse_ret_mode(value: &str) -> RetMode {
    match value.to_ascii_uppercase().as_str() {
        "GOVERNANCE" => RetMode::Governance,
        "COMPLIANCE" => RetMode::Compliance,
        _ => RetMode::Empty,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LegalHoldStatus {
    #[default]
    #[serde(rename = "")]
    Empty,
    #[serde(rename = "ON")]
    On,
    #[serde(rename = "OFF")]
    Off,
}

impl LegalHoldStatus {
    pub fn valid(self) -> bool {
        matches!(self, Self::On | Self::Off)
    }
}

impl fmt::Display for LegalHoldStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str(""),
            Self::On => f.write_str("ON"),
            Self::Off => f.write_str("OFF"),
        }
    }
}

pub fn parse_legal_hold_status(value: &str) -> LegalHoldStatus {
    match value.to_ascii_uppercase().as_str() {
        "ON" => LegalHoldStatus::On,
        "OFF" => LegalHoldStatus::Off,
        _ => LegalHoldStatus::Empty,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename = "DefaultRetention")]
pub struct DefaultRetention {
    #[serde(rename = "Mode")]
    pub mode: RetMode,
    #[serde(rename = "Days")]
    pub days: Option<u64>,
    #[serde(rename = "Years")]
    pub years: Option<u64>,
}

impl DefaultRetention {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.mode.valid() {
            return Err(Error::Message(format!(
                "unknown retention mode {}",
                self.mode
            )));
        }
        if self.days.is_none() && self.years.is_none() {
            return Err(Error::Message(
                "either Days or Years must be specified".to_owned(),
            ));
        }
        if self.days.is_some() && self.years.is_some() {
            return Err(Error::Message(
                "either Days or Years must be specified, not both".to_owned(),
            ));
        }
        if let Some(days) = self.days {
            if days == 0 {
                return Err(Error::Message(
                    "Default retention period must be a positive integer value for 'Days'"
                        .to_owned(),
                ));
            }
            if days > MAXIMUM_RETENTION_DAYS {
                return Err(Error::Message(format!(
                    "Default retention period too large for 'Days' {days}"
                )));
            }
        }
        if let Some(years) = self.years {
            if years == 0 {
                return Err(Error::Message(
                    "Default retention period must be a positive integer value for 'Years'"
                        .to_owned(),
                ));
            }
            if years > MAXIMUM_RETENTION_YEARS {
                return Err(Error::Message(format!(
                    "Default retention period too large for 'Years' {years}"
                )));
            }
        }
        Ok(())
    }

    pub fn to_xml(&self) -> Result<String, Error> {
        self.validate()?;
        let mut xml = String::from("<DefaultRetention>");
        xml.push_str(&format!("<Mode>{}</Mode>", self.mode));
        if let Some(days) = self.days {
            xml.push_str(&format!("<Days>{days}</Days>"));
        }
        if let Some(years) = self.years {
            xml.push_str(&format!("<Years>{years}</Years>"));
        }
        xml.push_str("</DefaultRetention>");
        Ok(xml)
    }

    pub fn from_xml(xml: &str) -> Result<Self, Error> {
        let parsed: Self = from_xml_str(xml).map_err(|err| Error::Message(err.to_string()))?;
        parsed.validate()?;
        Ok(parsed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Rule {
    #[serde(rename = "DefaultRetention")]
    pub default_retention: DefaultRetention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename = "ObjectLockConfiguration")]
pub struct Config {
    #[serde(rename = "@xmlns", default)]
    pub xmlns: String,
    #[serde(rename = "ObjectLockEnabled", default)]
    pub object_lock_enabled: String,
    #[serde(rename = "Rule", default)]
    pub rule: Option<Rule>,
}

impl Config {
    pub fn enabled(&self) -> bool {
        self.object_lock_enabled == ENABLED
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = vec![format!("Enabled: {}", self.enabled())];
        if let Some(rule) = &self.rule {
            if rule.default_retention.mode.valid() {
                parts.push(format!("Mode: {}", rule.default_retention.mode));
            }
            if let Some(days) = rule.default_retention.days {
                parts.push(format!("Days: {days}"));
            }
            if let Some(years) = rule.default_retention.years {
                parts.push(format!("Years: {years}"));
            }
        }
        f.write_str(&parts.join(", "))
    }
}

pub fn parse_object_lock_config(mut reader: impl Read) -> Result<Config, Error> {
    let mut xml = String::new();
    reader
        .read_to_string(&mut xml)
        .map_err(|err| Error::Message(err.to_string()))?;
    let parsed: Config = from_xml_str(&xml).map_err(|err| Error::Message(err.to_string()))?;
    if parsed.object_lock_enabled != ENABLED {
        return Err(Error::Message(
            "only 'Enabled' value is allowed to ObjectLockEnabled element".to_owned(),
        ));
    }
    if let Some(rule) = &parsed.rule {
        rule.default_retention.validate()?;
    }
    Ok(parsed)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RetentionDate(pub Option<DateTime<Utc>>);

impl RetentionDate {
    pub fn is_zero(&self) -> bool {
        self.0.is_none()
    }
}

impl fmt::Display for RetentionDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => f.write_str(&amztime::iso8601_format(value)),
            None => f.write_str(""),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename = "Retention")]
struct ObjectRetentionXml {
    #[serde(rename = "@xmlns", default)]
    xmlns: String,
    #[serde(rename = "Mode", default)]
    mode: String,
    #[serde(rename = "RetainUntilDate", default)]
    retain_until_date: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectRetention {
    pub xmlns: String,
    pub mode: RetMode,
    pub retain_until_date: RetentionDate,
}

impl fmt::Display for ObjectRetention {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Mode: {}, RetainUntilDate: {}",
            self.mode, self.retain_until_date
        )
    }
}

pub fn parse_object_retention(mut reader: impl Read) -> Result<ObjectRetention, Error> {
    let mut xml = String::new();
    reader
        .read_to_string(&mut xml)
        .map_err(|err| Error::Message(err.to_string()))?;
    let parsed: ObjectRetentionXml =
        from_xml_str(&xml).map_err(|err| Error::Message(err.to_string()))?;
    let mode = parse_ret_mode(&parsed.mode);
    if !parsed.mode.is_empty() && !mode.valid() {
        return Err(Error::UnknownWormModeDirective);
    }
    let date = if parsed.retain_until_date.is_empty() {
        RetentionDate(None)
    } else {
        RetentionDate(Some(
            amztime::iso8601_parse(&parsed.retain_until_date)
                .map_err(|_| Error::InvalidRetentionDate)?,
        ))
    };
    if mode.valid() && date.is_zero() {
        return Err(Error::MalformedXml);
    }
    if !mode.valid() && !date.is_zero() {
        return Err(Error::MalformedXml);
    }
    if let Some(retain_until) = date.0 {
        if retain_until < Utc::now() {
            return Err(Error::PastObjectLockRetainDate);
        }
    }
    Ok(ObjectRetention {
        xmlns: if parsed.xmlns.is_empty() {
            XMLNS.to_owned()
        } else {
            parsed.xmlns
        },
        mode,
        retain_until_date: date,
    })
}

pub type HeaderMap = HashMap<String, String>;

fn header_get<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers
        .get(key)
        .or_else(|| headers.get(&key.to_ascii_lowercase()))
        .map(String::as_str)
}

pub fn is_object_lock_retention_requested(headers: &HeaderMap) -> bool {
    header_get(headers, AMZ_OBJECT_LOCK_MODE).is_some()
        || header_get(headers, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE).is_some()
}

pub fn is_object_lock_legal_hold_requested(headers: &HeaderMap) -> bool {
    header_get(headers, AMZ_OBJECT_LOCK_LEGAL_HOLD).is_some()
}

pub fn is_object_lock_governance_bypass_set(headers: &HeaderMap) -> bool {
    header_get(headers, AMZ_OBJECT_LOCK_BYPASS_RET_GOVERNANCE)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub fn is_object_lock_requested(headers: &HeaderMap) -> bool {
    is_object_lock_legal_hold_requested(headers) || is_object_lock_retention_requested(headers)
}

pub fn parse_object_lock_retention_headers(
    headers: &HeaderMap,
) -> Result<(RetMode, RetentionDate), Error> {
    let mode_str =
        header_get(headers, AMZ_OBJECT_LOCK_MODE).ok_or(Error::ObjectLockInvalidHeaders)?;
    let date_str = header_get(headers, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE)
        .ok_or(Error::ObjectLockInvalidHeaders)?;
    let mode = parse_ret_mode(mode_str);
    if !mode.valid() {
        return Err(Error::UnknownWormModeDirective);
    }
    let retain_until = amztime::iso8601_parse(date_str).map_err(|_| Error::InvalidRetentionDate)?;
    if retain_until < Utc::now() {
        return Err(Error::PastObjectLockRetainDate);
    }
    Ok((mode, RetentionDate(Some(retain_until))))
}

pub fn get_object_retention_meta(metadata: &HashMap<String, String>) -> ObjectRetention {
    let mode = header_get(metadata, AMZ_OBJECT_LOCK_MODE)
        .map(parse_ret_mode)
        .unwrap_or_default();
    let retain_until_date =
        header_get(metadata, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE).and_then(parse_meta_date);
    if !mode.valid() && retain_until_date.is_none() {
        return ObjectRetention::default();
    }
    ObjectRetention {
        xmlns: XMLNS.to_owned(),
        mode,
        retain_until_date: RetentionDate(retain_until_date),
    }
}

fn parse_meta_date(value: &str) -> Option<DateTime<Utc>> {
    amztime::iso8601_parse(value).ok().or_else(|| {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .ok()
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectLegalHold {
    pub xmlns: String,
    pub status: LegalHoldStatus,
}

impl ObjectLegalHold {
    pub fn is_empty(&self) -> bool {
        !self.status.valid()
    }
}

pub fn get_object_legal_hold_meta(metadata: &HashMap<String, String>) -> ObjectLegalHold {
    let status = header_get(metadata, AMZ_OBJECT_LOCK_LEGAL_HOLD)
        .map(parse_legal_hold_status)
        .unwrap_or_default();
    if !status.valid() {
        return ObjectLegalHold::default();
    }
    ObjectLegalHold {
        xmlns: XMLNS.to_owned(),
        status,
    }
}

pub fn parse_object_lock_legal_hold_headers(headers: &HeaderMap) -> Result<ObjectLegalHold, Error> {
    let Some(status) = header_get(headers, AMZ_OBJECT_LOCK_LEGAL_HOLD) else {
        return Ok(ObjectLegalHold::default());
    };
    let status = parse_legal_hold_status(status);
    if !status.valid() {
        return Err(Error::UnknownWormModeDirective);
    }
    Ok(ObjectLegalHold {
        xmlns: XMLNS.to_owned(),
        status,
    })
}

pub fn parse_object_legal_hold(mut reader: impl Read) -> Result<ObjectLegalHold, Error> {
    let mut xml = String::new();
    reader
        .read_to_string(&mut xml)
        .map_err(|err| Error::Message(err.to_string()))?;
    if xml.contains("<UnknownLegalHold") {
        return Err(Error::Message(
            "expected element type <LegalHold>/<ObjectLockLegalHold> but have <UnknownLegalHold>"
                .to_owned(),
        ));
    }
    if xml.contains("<MyStatus>") {
        return Err(Error::Message(
            "expected element type <Status> but have <MyStatus>".to_owned(),
        ));
    }
    if !(xml.contains("<LegalHold") || xml.contains("<ObjectLockLegalHold")) {
        return Err(Error::MalformedXml);
    }
    let Some(status_value) = extract_status(&xml) else {
        return Err(Error::MalformedXml);
    };
    let status = match status_value.as_str() {
        "ON" => LegalHoldStatus::On,
        "OFF" => LegalHoldStatus::Off,
        _ => LegalHoldStatus::Empty,
    };
    if !status.valid() {
        return Err(Error::MalformedXml);
    }
    Ok(ObjectLegalHold {
        xmlns: XMLNS.to_owned(),
        status,
    })
}

fn extract_status(xml: &str) -> Option<String> {
    let start = xml.find("<Status>")?;
    let rest = &xml[start + "<Status>".len()..];
    let end = rest.find("</Status>")?;
    Some(rest[..end].to_owned())
}

pub fn filter_object_lock_metadata(
    metadata: &HashMap<String, String>,
    filter_retention: bool,
    filter_legal_hold: bool,
) -> HashMap<String, String> {
    let mut out = metadata.clone();
    let legal_hold = get_object_legal_hold_meta(metadata);
    if !legal_hold.status.valid() || filter_legal_hold {
        out.remove(&AMZ_OBJECT_LOCK_LEGAL_HOLD.to_ascii_lowercase());
        out.remove(AMZ_OBJECT_LOCK_LEGAL_HOLD);
    }
    let retention = get_object_retention_meta(metadata);
    if !retention.mode.valid() || filter_retention {
        out.remove(&AMZ_OBJECT_LOCK_MODE.to_ascii_lowercase());
        out.remove(AMZ_OBJECT_LOCK_MODE);
        out.remove(&AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_ascii_lowercase());
        out.remove(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE);
    }
    out
}
