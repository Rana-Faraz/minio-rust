use std::collections::HashMap;

use chrono::{DateTime, SecondsFormat, Utc};

use super::constants::{DISABLED, ENABLED};
use super::error::Error;
use super::evaluation::expected_expiry_time;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Retention {
    pub lock_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    None,
    Delete,
    DeleteVersion,
    Transition,
    TransitionVersion,
    DeleteRestored,
    DeleteRestoredVersion,
    DeleteAllVersions,
    DelMarkerDeleteAllVersions,
}

impl Default for Action {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Prefix {
    value: String,
    pub set: bool,
}

impl Prefix {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            set: true,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }

    fn write_xml(&self, name: &str, xml: &mut String) {
        if self.set {
            xml.push_str(&format!("<{name}>{}</{name}>", escape_xml(&self.value)));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

impl Tag {
    pub fn is_empty(&self) -> bool {
        self.key.is_empty()
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.key.is_empty() || self.key.chars().count() > 128 {
            return Err(Error::InvalidTagKey);
        }
        if self.value.chars().count() > 256 {
            return Err(Error::InvalidTagValue);
        }
        Ok(())
    }

    fn write_xml(&self, xml: &mut String) {
        if self.is_empty() {
            return;
        }
        xml.push_str("<Tag>");
        xml.push_str(&format!("<Key>{}</Key>", escape_xml(&self.key)));
        xml.push_str(&format!("<Value>{}</Value>", escape_xml(&self.value)));
        xml.push_str("</Tag>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct And {
    pub object_size_greater_than: i64,
    pub object_size_less_than: i64,
    pub prefix: Prefix,
    pub tags: Vec<Tag>,
}

impl And {
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
            && !self.prefix.set
            && self.object_size_greater_than == 0
            && self.object_size_less_than == 0
    }

    pub fn validate(&self) -> Result<(), Error> {
        let mut pred_count = 0;
        if self.prefix.set {
            pred_count += 1;
        }
        pred_count += self.tags.len();
        if self.object_size_greater_than > 0 {
            pred_count += 1;
        }
        if self.object_size_less_than > 0 {
            pred_count += 1;
        }
        if pred_count < 2 {
            return Err(Error::XmlNotWellFormed);
        }
        if self.contains_duplicate_tag() {
            return Err(Error::DuplicateTagKey);
        }
        for tag in &self.tags {
            tag.validate()?;
        }
        if self.object_size_greater_than < 0 || self.object_size_less_than < 0 {
            return Err(Error::XmlNotWellFormed);
        }
        Ok(())
    }

    pub fn contains_duplicate_tag(&self) -> bool {
        let mut seen = HashMap::new();
        for tag in &self.tags {
            if seen.insert(tag.key.clone(), ()).is_some() {
                return true;
            }
        }
        false
    }

    pub fn by_size(&self, size: i64) -> bool {
        if self.object_size_greater_than > 0 && size <= self.object_size_greater_than {
            return false;
        }
        if self.object_size_less_than > 0 && size >= self.object_size_less_than {
            return false;
        }
        true
    }

    fn write_xml(&self, xml: &mut String) {
        if self.is_empty() {
            return;
        }
        xml.push_str("<And>");
        self.prefix.write_xml("Prefix", xml);
        if self.object_size_less_than != 0 {
            xml.push_str(&format!(
                "<ObjectSizeLessThan>{}</ObjectSizeLessThan>",
                self.object_size_less_than
            ));
        }
        if self.object_size_greater_than != 0 {
            xml.push_str(&format!(
                "<ObjectSizeGreaterThan>{}</ObjectSizeGreaterThan>",
                self.object_size_greater_than
            ));
        }
        for tag in &self.tags {
            tag.write_xml(xml);
        }
        xml.push_str("</And>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Filter {
    pub set: bool,
    pub prefix: Prefix,
    pub object_size_greater_than: i64,
    pub object_size_less_than: i64,
    pub and: And,
    pub tag: Tag,
    pub direct_tag_count: usize,
}

impl Filter {
    pub fn is_empty(&self) -> bool {
        !self.set
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.is_empty() {
            return Err(Error::XmlNotWellFormed);
        }
        let mut pred_count = 0;
        let mut pred = 0_u8;
        if !self.and.is_empty() {
            pred = 1;
            pred_count += 1;
        }
        if self.prefix.set {
            pred = 2;
            pred_count += 1;
        }
        if self.direct_tag_count > 0 {
            pred = 3;
            pred_count += 1;
        }
        if self.object_size_greater_than != 0 {
            pred = 4;
            pred_count += 1;
        }
        if self.object_size_less_than != 0 {
            pred = 5;
            pred_count += 1;
        }
        if pred_count > 1 {
            return Err(Error::InvalidFilter);
        }
        match pred {
            0 | 2 => Ok(()),
            1 => self.and.validate(),
            3 => self.tag.validate(),
            4 => {
                if self.object_size_greater_than < 0 {
                    Err(Error::XmlNotWellFormed)
                } else {
                    Ok(())
                }
            }
            5 => {
                if self.object_size_less_than < 0 {
                    Err(Error::XmlNotWellFormed)
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    pub fn test_tags(&self, user_tags: &str) -> bool {
        let mut required = HashMap::new();
        for tag in &self.and.tags {
            if !tag.is_empty() {
                required.insert(tag.key.clone(), tag.value.clone());
            }
        }
        if !self.tag.is_empty() {
            required.insert(self.tag.key.clone(), self.tag.value.clone());
        }
        if required.is_empty() {
            return true;
        }
        let parsed = match parse_object_tags(user_tags) {
            Some(tags) => tags,
            None => return false,
        };
        if parsed.len() < required.len() {
            return false;
        }
        required
            .iter()
            .all(|(key, value)| parsed.get(key).is_some_and(|candidate| candidate == value))
    }

    pub fn by_size(&self, size: i64) -> bool {
        if self.object_size_greater_than > 0 && size <= self.object_size_greater_than {
            return false;
        }
        if self.object_size_less_than > 0 && size >= self.object_size_less_than {
            return false;
        }
        if !self.and.is_empty() {
            return self.and.by_size(size);
        }
        true
    }

    fn write_xml(&self, xml: &mut String) {
        if !self.set {
            return;
        }
        xml.push_str("<Filter>");
        if !self.and.is_empty() {
            self.and.write_xml(xml);
        } else if !self.tag.is_empty() {
            self.tag.write_xml(xml);
        } else {
            self.prefix.write_xml("Prefix", xml);
            if self.object_size_less_than > 0 {
                xml.push_str(&format!(
                    "<ObjectSizeLessThan>{}</ObjectSizeLessThan>",
                    self.object_size_less_than
                ));
            }
            if self.object_size_greater_than > 0 {
                xml.push_str(&format!(
                    "<ObjectSizeGreaterThan>{}</ObjectSizeGreaterThan>",
                    self.object_size_greater_than
                ));
            }
        }
        xml.push_str("</Filter>");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Boolean {
    pub val: bool,
    pub set: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Expiration {
    pub days: Option<i32>,
    pub date: Option<DateTime<Utc>>,
    pub delete_marker: Boolean,
    pub delete_all: Boolean,
    pub set: bool,
}

impl Expiration {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.set {
            return Ok(());
        }
        if (self.days.is_some() || self.date.is_some()) && self.delete_marker.set {
            return Err(Error::LifecycleInvalidDeleteMarker);
        }
        if !self.delete_marker.set
            && !self.delete_all.set
            && self.days.is_none()
            && self.date.is_none()
        {
            return Err(Error::XmlNotWellFormed);
        }
        if self.days.is_some() && self.date.is_some() {
            return Err(Error::LifecycleInvalidExpiration);
        }
        if self.delete_all.set && self.days.is_none() {
            return Err(Error::LifecycleInvalidDeleteAll);
        }
        Ok(())
    }

    pub fn is_days_null(&self) -> bool {
        self.days.is_none()
    }

    pub fn is_date_null(&self) -> bool {
        self.date.is_none()
    }

    pub fn is_null(&self) -> bool {
        self.is_days_null() && self.is_date_null()
    }

    fn write_xml(&self, xml: &mut String) {
        if !self.set {
            return;
        }
        xml.push_str("<Expiration>");
        if let Some(days) = self.days {
            xml.push_str(&format!("<Days>{days}</Days>"));
        }
        if let Some(date) = self.date {
            xml.push_str(&format!(
                "<Date>{}</Date>",
                date.to_rfc3339_opts(SecondsFormat::Secs, true)
            ));
        }
        if self.delete_marker.set {
            xml.push_str(&format!(
                "<ExpiredObjectDeleteMarker>{}</ExpiredObjectDeleteMarker>",
                self.delete_marker.val
            ));
        }
        if self.delete_all.set {
            xml.push_str(&format!(
                "<ExpiredObjectAllVersions>{}</ExpiredObjectAllVersions>",
                self.delete_all.val
            ));
        }
        xml.push_str("</Expiration>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DelMarkerExpiration {
    pub days: i32,
}

impl DelMarkerExpiration {
    pub fn empty(&self) -> bool {
        self.days == 0
    }

    pub fn next_due(&self, obj: &ObjectOpts) -> Option<DateTime<Utc>> {
        if !obj.is_latest || !obj.delete_marker || self.empty() {
            return None;
        }
        obj.mod_time
            .map(|mod_time| expected_expiry_time(mod_time, self.days))
    }

    fn write_xml(&self, xml: &mut String) {
        if self.empty() {
            return;
        }
        xml.push_str("<DelMarkerExpiration>");
        xml.push_str(&format!("<Days>{}</Days>", self.days));
        xml.push_str("</DelMarkerExpiration>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Transition {
    pub days: Option<i32>,
    pub date: Option<DateTime<Utc>>,
    pub storage_class: String,
    pub set: bool,
}

impl Transition {
    pub fn is_enabled(&self) -> bool {
        self.set
    }

    pub fn validate(&self) -> Result<(), Error> {
        if !self.set {
            return Ok(());
        }
        if self.date.is_some() && self.days.is_some_and(|days| days > 0) {
            return Err(Error::TransitionInvalid);
        }
        if self.storage_class.is_empty() {
            return Err(Error::XmlNotWellFormed);
        }
        Ok(())
    }

    pub fn is_date_null(&self) -> bool {
        self.date.is_none()
    }

    pub fn is_null(&self) -> bool {
        self.storage_class.is_empty()
    }

    pub fn next_due(&self, obj: &ObjectOpts) -> Option<DateTime<Utc>> {
        if !obj.is_latest || self.is_null() {
            return None;
        }
        if let Some(date) = self.date {
            return Some(date);
        }
        let mod_time = obj.mod_time?;
        if self.days == Some(0) {
            return Some(mod_time);
        }
        Some(expected_expiry_time(
            mod_time,
            self.days.unwrap_or_default(),
        ))
    }

    fn write_xml(&self, xml: &mut String) {
        if !self.set {
            return;
        }
        xml.push_str("<Transition>");
        if let Some(days) = self.days {
            xml.push_str(&format!("<Days>{days}</Days>"));
        }
        if let Some(date) = self.date {
            xml.push_str(&format!(
                "<Date>{}</Date>",
                date.to_rfc3339_opts(SecondsFormat::Secs, true)
            ));
        }
        if !self.storage_class.is_empty() {
            xml.push_str(&format!(
                "<StorageClass>{}</StorageClass>",
                escape_xml(&self.storage_class)
            ));
        }
        xml.push_str("</Transition>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NoncurrentVersionExpiration {
    pub noncurrent_days: Option<i32>,
    pub newer_noncurrent_versions: i32,
    pub set: bool,
}

impl NoncurrentVersionExpiration {
    pub fn is_null(&self) -> bool {
        self.noncurrent_days.is_none() && self.newer_noncurrent_versions == 0
    }

    pub fn validate(&self) -> Result<(), Error> {
        if !self.set {
            return Ok(());
        }
        let days = self.noncurrent_days.unwrap_or(0);
        match (days, self.newer_noncurrent_versions) {
            (0, 0) => Err(Error::XmlNotWellFormed),
            (d, _) if d < 0 => Err(Error::XmlNotWellFormed),
            (_, versions) if versions < 0 => Err(Error::XmlNotWellFormed),
            _ => Ok(()),
        }
    }

    fn write_xml(&self, xml: &mut String) {
        if self.is_null() {
            return;
        }
        xml.push_str("<NoncurrentVersionExpiration>");
        if let Some(days) = self.noncurrent_days {
            xml.push_str(&format!("<NoncurrentDays>{days}</NoncurrentDays>"));
        }
        if self.newer_noncurrent_versions > 0 {
            xml.push_str(&format!(
                "<NewerNoncurrentVersions>{}</NewerNoncurrentVersions>",
                self.newer_noncurrent_versions
            ));
        }
        xml.push_str("</NoncurrentVersionExpiration>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NoncurrentVersionTransition {
    pub noncurrent_days: i32,
    pub storage_class: String,
    pub set: bool,
}

impl NoncurrentVersionTransition {
    pub fn is_null(&self) -> bool {
        self.storage_class.is_empty()
    }

    pub fn validate(&self) -> Result<(), Error> {
        if !self.set {
            return Ok(());
        }
        if self.storage_class.is_empty() {
            return Err(Error::XmlNotWellFormed);
        }
        Ok(())
    }

    pub fn next_due(&self, obj: &ObjectOpts) -> Option<DateTime<Utc>> {
        if obj.is_latest || self.storage_class.is_empty() {
            return None;
        }
        let successor = obj.successor_mod_time?;
        if self.noncurrent_days == 0 {
            return Some(successor);
        }
        Some(expected_expiry_time(successor, self.noncurrent_days))
    }

    fn write_xml(&self, xml: &mut String) {
        if self.is_null() {
            return;
        }
        xml.push_str("<NoncurrentVersionTransition>");
        xml.push_str(&format!(
            "<NoncurrentDays>{}</NoncurrentDays>",
            self.noncurrent_days
        ));
        xml.push_str(&format!(
            "<StorageClass>{}</StorageClass>",
            escape_xml(&self.storage_class)
        ));
        xml.push_str("</NoncurrentVersionTransition>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rule {
    pub id: String,
    pub status: String,
    pub filter: Filter,
    pub prefix: Prefix,
    pub expiration: Expiration,
    pub transition: Transition,
    pub del_marker_expiration: DelMarkerExpiration,
    pub noncurrent_version_expiration: NoncurrentVersionExpiration,
    pub noncurrent_version_transition: NoncurrentVersionTransition,
}

impl Rule {
    pub fn validate(&self) -> Result<(), Error> {
        if self.id.len() > 255 {
            return Err(Error::InvalidRuleId);
        }
        if self.status.is_empty() {
            return Err(Error::EmptyRuleStatus);
        }
        if self.status != ENABLED && self.status != DISABLED {
            return Err(Error::InvalidRuleStatus);
        }
        self.expiration.validate()?;
        self.noncurrent_version_expiration.validate()?;
        if self.prefix.set && !self.filter.is_empty() && self.filter.prefix.set {
            return Err(Error::XmlNotWellFormed);
        }
        if self.filter.set {
            self.filter.validate()?;
        }
        self.transition.validate()?;
        self.noncurrent_version_transition.validate()?;
        if (!self.filter.tag.is_empty() || !self.filter.and.tags.is_empty())
            && !self.del_marker_expiration.empty()
        {
            return Err(Error::InvalidRuleDelMarkerExpiration);
        }
        if !self.expiration.set
            && !self.transition.set
            && !self.noncurrent_version_expiration.set
            && !self.noncurrent_version_transition.set
            && self.del_marker_expiration.empty()
        {
            return Err(Error::XmlNotWellFormed);
        }
        Ok(())
    }

    pub fn get_prefix(&self) -> &str {
        if self.prefix.set && !self.prefix.as_str().is_empty() {
            return self.prefix.as_str();
        }
        if self.filter.prefix.set {
            return self.filter.prefix.as_str();
        }
        if self.filter.and.prefix.set {
            return self.filter.and.prefix.as_str();
        }
        ""
    }

    pub(super) fn write_xml(&self, xml: &mut String) {
        xml.push_str("<Rule>");
        if !self.id.is_empty() {
            xml.push_str(&format!("<ID>{}</ID>", escape_xml(&self.id)));
        }
        self.prefix.write_xml("Prefix", xml);
        self.filter.write_xml(xml);
        if !self.status.is_empty() {
            xml.push_str(&format!("<Status>{}</Status>", escape_xml(&self.status)));
        }
        self.expiration.write_xml(xml);
        self.transition.write_xml(xml);
        self.del_marker_expiration.write_xml(xml);
        self.noncurrent_version_expiration.write_xml(xml);
        self.noncurrent_version_transition.write_xml(xml);
        xml.push_str("</Rule>");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Lifecycle {
    pub rules: Vec<Rule>,
    pub expiry_updated_at: Option<DateTime<Utc>>,
}

impl Lifecycle {
    pub fn has_transition(&self) -> bool {
        self.rules.iter().any(|rule| rule.transition.is_enabled())
    }

    pub fn has_expiry(&self) -> bool {
        self.rules
            .iter()
            .any(|rule| !rule.expiration.is_null() || !rule.noncurrent_version_expiration.is_null())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectOpts {
    pub name: String,
    pub user_tags: String,
    pub mod_time: Option<DateTime<Utc>>,
    pub size: i64,
    pub version_id: String,
    pub is_latest: bool,
    pub delete_marker: bool,
    pub num_versions: usize,
    pub successor_mod_time: Option<DateTime<Utc>>,
    pub transition_status: String,
    pub restore_expires: Option<DateTime<Utc>>,
}

impl ObjectOpts {
    pub fn expired_object_delete_marker(&self) -> bool {
        self.delete_marker && self.num_versions == 1
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Event {
    pub action: Action,
    pub rule_id: String,
    pub due: Option<DateTime<Utc>>,
    pub noncurrent_days: i32,
    pub newer_noncurrent_versions: i32,
    pub storage_class: String,
}

#[derive(Debug, Clone)]
pub struct Evaluator {
    pub(super) policy: Lifecycle,
}

impl Evaluator {
    pub fn new(policy: Lifecycle) -> Self {
        Self { policy }
    }
}

fn parse_object_tags(value: &str) -> Option<HashMap<String, String>> {
    if value.is_empty() {
        return Some(HashMap::new());
    }
    let mut tags = HashMap::new();
    for pair in value.split('&') {
        let (key, val) = pair.split_once('=')?;
        tags.insert(decode_tag_component(key)?, decode_tag_component(val)?);
    }
    Some(tags)
}

fn decode_tag_component(value: &str) -> Option<String> {
    let mut out = String::new();
    let bytes = value.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= bytes.len() {
                    return None;
                }
                let hi = hex_value(bytes[index + 1])?;
                let lo = hex_value(bytes[index + 2])?;
                out.push((hi * 16 + lo) as char);
                index += 3;
            }
            byte => {
                out.push(byte as char);
                index += 1;
            }
        }
    }
    Some(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
