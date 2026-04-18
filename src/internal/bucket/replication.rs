use std::cmp::Ordering;
use std::collections::HashMap;

use roxmltree::{Document, Node};

pub const ENABLED: &str = "Enabled";
pub const DISABLED: &str = "Disabled";
pub const DESTINATION_ARN_PREFIX: &str = "arn:aws:s3:::";
pub const DESTINATION_ARN_MINIO_PREFIX: &str = "arn:minio:replication:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ReplicationTooManyRules,
    ReplicationNoRule,
    ReplicationUniquePriority,
    RoleArnMissingLegacy,
    DestinationArnMissing,
    InvalidSourceSelectionCriteria,
    RoleArnPresentForMultipleTargets,
    InvalidFilter,
    InvalidRuleId,
    EmptyRuleStatus,
    InvalidRuleStatus,
    DeleteMarkerReplicationMissing,
    PriorityMissing,
    InvalidDeleteMarkerReplicationStatus,
    DestinationSourceIdentical,
    DeleteReplicationMissing,
    InvalidDeleteReplicationStatus,
    InvalidExistingObjectReplicationStatus,
    TagsDeleteMarkerReplicationDisallowed,
    InvalidTagKey,
    InvalidTagValue,
    DuplicateTagKey,
    Message(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReplicationTooManyRules => {
                f.write_str("Replication configuration allows a maximum of 1000 rules")
            }
            Self::ReplicationNoRule => {
                f.write_str("Replication configuration should have at least one rule")
            }
            Self::ReplicationUniquePriority => {
                f.write_str("Replication configuration has duplicate priority")
            }
            Self::RoleArnMissingLegacy => {
                f.write_str("Missing required parameter `Role` in ReplicationConfiguration")
            }
            Self::DestinationArnMissing => {
                f.write_str("Missing required parameter `Destination` in Replication rule")
            }
            Self::InvalidSourceSelectionCriteria => {
                f.write_str("Invalid ReplicaModification status")
            }
            Self::RoleArnPresentForMultipleTargets => f.write_str(
                "`Role` should be empty in ReplicationConfiguration for multiple targets",
            ),
            Self::InvalidFilter => {
                f.write_str("Filter must have exactly one of Prefix, Tag, or And specified")
            }
            Self::InvalidRuleId => f.write_str("ID must be less than 255 characters"),
            Self::EmptyRuleStatus => f.write_str("Status should not be empty"),
            Self::InvalidRuleStatus => {
                f.write_str("Status must be set to either Enabled or Disabled")
            }
            Self::DeleteMarkerReplicationMissing => {
                f.write_str("DeleteMarkerReplication must be specified")
            }
            Self::PriorityMissing => f.write_str("Priority must be specified"),
            Self::InvalidDeleteMarkerReplicationStatus => {
                f.write_str("Delete marker replication status is invalid")
            }
            Self::DestinationSourceIdentical => {
                f.write_str("Destination bucket cannot be the same as the source bucket.")
            }
            Self::DeleteReplicationMissing => f.write_str("Delete replication must be specified"),
            Self::InvalidDeleteReplicationStatus => {
                f.write_str("Delete replication is either enable|disable")
            }
            Self::InvalidExistingObjectReplicationStatus => {
                f.write_str("Existing object replication status is invalid")
            }
            Self::TagsDeleteMarkerReplicationDisallowed => f.write_str(
                "Delete marker replication is not supported if any Tag filter is specified",
            ),
            Self::InvalidTagKey => f.write_str("The TagKey you have provided is invalid"),
            Self::InvalidTagValue => f.write_str("The TagValue you have provided is invalid"),
            Self::DuplicateTagKey => f.write_str("Duplicate Tag Keys are not allowed"),
            Self::Message(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationType {
    Unset = 0,
    Object = 1,
    Delete = 2,
    Metadata = 3,
    Heal = 4,
    ExistingObject = 5,
    Resync = 6,
    All = 7,
}

impl Default for ReplicationType {
    fn default() -> Self {
        Self::Unset
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusType {
    Pending,
    Completed,
    CompletedLegacy,
    Failed,
    Replica,
}

impl StatusType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Completed => "COMPLETED",
            Self::CompletedLegacy => "COMPLETE",
            Self::Failed => "FAILED",
            Self::Replica => "REPLICA",
        }
    }

    pub fn is_empty(value: Option<Self>) -> bool {
        value.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionPurgeStatusType {
    Pending,
    Complete,
    Failed,
}

impl VersionPurgeStatusType {
    pub fn empty(value: Option<Self>) -> bool {
        value.is_none()
    }

    pub fn pending(self) -> bool {
        matches!(self, Self::Pending | Self::Failed)
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

    pub fn as_query(&self) -> String {
        format!("{}={}", self.key, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct And {
    pub prefix: String,
    pub tags: Vec<Tag>,
}

impl And {
    pub fn is_empty(&self) -> bool {
        self.prefix.is_empty() && self.tags.is_empty()
    }

    pub fn validate(&self) -> Result<(), Error> {
        let mut seen = HashMap::new();
        for tag in &self.tags {
            tag.validate()?;
            if seen.insert(tag.key.clone(), ()).is_some() {
                return Err(Error::DuplicateTagKey);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Filter {
    pub prefix: String,
    pub and: And,
    pub tag: Tag,
}

impl Filter {
    pub fn is_empty(&self) -> bool {
        self.prefix.is_empty() && self.and.is_empty() && self.tag.is_empty()
    }

    pub fn validate(&self) -> Result<(), Error> {
        if !self.and.is_empty() {
            if !self.prefix.is_empty() || !self.tag.is_empty() {
                return Err(Error::InvalidFilter);
            }
            self.and.validate()?;
        }
        if !self.prefix.is_empty() && !self.tag.is_empty() {
            return Err(Error::InvalidFilter);
        }
        if !self.tag.is_empty() {
            self.tag.validate()?;
        }
        Ok(())
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
            Some(value) => value,
            None => return false,
        };
        if parsed.is_empty() {
            return false;
        }
        required
            .iter()
            .any(|(key, value)| parsed.get(key).is_some_and(|candidate| candidate == value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteMarkerReplication {
    pub status: String,
}

impl DeleteMarkerReplication {
    pub fn validate(&self) -> Result<(), Error> {
        if self.status.is_empty() {
            return Err(Error::DeleteMarkerReplicationMissing);
        }
        if self.status != ENABLED && self.status != DISABLED {
            return Err(Error::InvalidDeleteMarkerReplicationStatus);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteReplication {
    pub status: String,
}

impl DeleteReplication {
    pub fn validate(&self) -> Result<(), Error> {
        if self.status.is_empty() {
            return Err(Error::DeleteReplicationMissing);
        }
        if self.status != ENABLED && self.status != DISABLED {
            return Err(Error::InvalidDeleteReplicationStatus);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExistingObjectReplication {
    pub status: String,
}

impl ExistingObjectReplication {
    pub fn validate(&self) -> Result<(), Error> {
        if self.status.is_empty() {
            return Ok(());
        }
        if self.status != ENABLED && self.status != DISABLED {
            return Err(Error::InvalidExistingObjectReplicationStatus);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplicaModifications {
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceSelectionCriteria {
    pub replica_modifications: ReplicaModifications,
}

impl SourceSelectionCriteria {
    pub fn validate(&self) -> Result<(), Error> {
        if self == &Self::default() {
            return Ok(());
        }
        if self.replica_modifications.status == ENABLED
            || self.replica_modifications.status == DISABLED
        {
            return Ok(());
        }
        Err(Error::InvalidSourceSelectionCriteria)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Destination {
    pub bucket: String,
    pub storage_class: String,
    pub arn: String,
}

impl Destination {
    pub fn is_valid(&self) -> bool {
        !self.bucket.is_empty() && self.is_valid_storage_class()
    }

    pub fn is_valid_storage_class(&self) -> bool {
        self.storage_class.is_empty()
            || self.storage_class == "STANDARD"
            || self.storage_class == "REDUCED_REDUNDANCY"
    }

    pub fn legacy_arn(&self) -> bool {
        self.arn.starts_with(DESTINATION_ARN_PREFIX)
    }

    pub fn target_arn(&self) -> bool {
        self.arn.starts_with(DESTINATION_ARN_MINIO_PREFIX)
    }

    pub fn validate(&self, bucket_name: &str) -> Result<(), Error> {
        if !self.is_valid() {
            return Err(Error::Message("invalid destination".to_owned()));
        }
        if self.bucket != bucket_name {
            return Err(Error::Message("bucket name does not match".to_owned()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rule {
    pub id: String,
    pub status: String,
    pub priority: i32,
    pub delete_marker_replication: DeleteMarkerReplication,
    pub delete_replication: DeleteReplication,
    pub destination: Destination,
    pub source_selection_criteria: SourceSelectionCriteria,
    pub filter: Filter,
    pub existing_object_replication: ExistingObjectReplication,
}

impl Rule {
    pub fn validate(&self, bucket: &str, same_target: bool) -> Result<(), Error> {
        if self.id.len() > 255 {
            return Err(Error::InvalidRuleId);
        }
        if self.status.is_empty() {
            return Err(Error::EmptyRuleStatus);
        }
        if self.status != ENABLED && self.status != DISABLED {
            return Err(Error::InvalidRuleStatus);
        }
        self.filter.validate()?;
        self.delete_marker_replication.validate()?;
        self.delete_replication.validate()?;
        self.source_selection_criteria.validate()?;
        if self.priority < 0 {
            return Err(Error::PriorityMissing);
        }
        if self.destination.bucket == bucket && same_target {
            return Err(Error::DestinationSourceIdentical);
        }
        if !self.filter.tag.is_empty() && self.delete_marker_replication.status == ENABLED {
            return Err(Error::TagsDeleteMarkerReplicationDisallowed);
        }
        self.existing_object_replication.validate()
    }

    pub fn prefix(&self) -> &str {
        if !self.filter.prefix.is_empty() {
            &self.filter.prefix
        } else {
            &self.filter.and.prefix
        }
    }

    pub fn metadata_replicate(&self, obj: &ObjectOpts) -> bool {
        if !obj.replica {
            return true;
        }
        self.source_selection_criteria.replica_modifications.status == ENABLED
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub rules: Vec<Rule>,
    pub role_arn: String,
}

impl Config {
    pub fn validate(&self, bucket: &str, same_target: bool) -> Result<(), Error> {
        if self.rules.len() > 1000 {
            return Err(Error::ReplicationTooManyRules);
        }
        if self.rules.is_empty() {
            return Err(Error::ReplicationNoRule);
        }
        let mut targets = HashMap::<String, ()>::new();
        let mut priorities = HashMap::<i32, ()>::new();
        let mut legacy_arn = false;
        for rule in &self.rules {
            targets.insert(rule.destination.bucket.clone(), ());
            rule.validate(bucket, same_target)?;
            if priorities.insert(rule.priority, ()).is_some() {
                return Err(Error::ReplicationUniquePriority);
            }
            if rule.destination.legacy_arn() {
                legacy_arn = true;
            }
            if self.role_arn.is_empty() && !rule.destination.target_arn() {
                return Err(Error::DestinationArnMissing);
            }
        }
        if !self.role_arn.is_empty() && targets.len() > 1 {
            return Err(Error::RoleArnPresentForMultipleTargets);
        }
        if self.role_arn.is_empty() && legacy_arn {
            return Err(Error::RoleArnMissingLegacy);
        }
        Ok(())
    }

    pub fn filter_actionable_rules(&self, obj: &ObjectOpts) -> Vec<Rule> {
        if obj.name.is_empty()
            && obj.op_type != ReplicationType::Resync
            && obj.op_type != ReplicationType::All
        {
            return Vec::new();
        }
        let mut rules: Vec<_> = self
            .rules
            .iter()
            .filter(|rule| rule.status != DISABLED)
            .filter(|rule| {
                if !obj.target_arn.is_empty() {
                    rule.destination.arn == obj.target_arn || self.role_arn == obj.target_arn
                } else {
                    true
                }
            })
            .filter(|rule| {
                if obj.op_type == ReplicationType::Resync || obj.op_type == ReplicationType::All {
                    return true;
                }
                if obj.existing_object && rule.existing_object_replication.status == DISABLED {
                    return false;
                }
                if !obj.name.starts_with(rule.prefix()) {
                    return false;
                }
                rule.filter.test_tags(&obj.user_tags)
            })
            .cloned()
            .collect();

        rules.sort_by(|left, right| {
            if left.destination.arn == right.destination.arn {
                right.priority.cmp(&left.priority)
            } else {
                Ordering::Equal
            }
        });
        rules
    }

    pub fn replicate(&self, obj: &ObjectOpts) -> bool {
        for rule in self.filter_actionable_rules(obj) {
            if rule.status == DISABLED {
                continue;
            }
            if obj.existing_object && rule.existing_object_replication.status == DISABLED {
                return false;
            }
            if obj.op_type == ReplicationType::Delete {
                if !obj.version_id.is_empty() {
                    return rule.delete_replication.status == ENABLED;
                }
                return rule.delete_marker_replication.status == ENABLED;
            }
            return rule.metadata_replicate(obj);
        }
        false
    }

    pub fn has_active_rules(&self, prefix: &str, recursive: bool) -> bool {
        if self.rules.is_empty() {
            return false;
        }
        for rule in &self.rules {
            if rule.status == DISABLED {
                continue;
            }
            if !prefix.is_empty() && !rule.filter.prefix.is_empty() {
                if !recursive && !prefix.starts_with(&rule.filter.prefix) {
                    continue;
                }
                if recursive
                    && !rule.prefix().starts_with(prefix)
                    && !prefix.starts_with(rule.prefix())
                {
                    continue;
                }
            }
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ObjectOpts {
    pub name: String,
    pub user_tags: String,
    pub version_id: String,
    pub delete_marker: bool,
    pub ssec: bool,
    pub op_type: ReplicationType,
    pub replica: bool,
    pub existing_object: bool,
    pub target_arn: String,
}

pub fn parse_config(xml: &str) -> Result<Config, Error> {
    let document = Document::parse(xml).map_err(|err| Error::Message(err.to_string()))?;
    let root = document.root_element();
    if root.tag_name().name() != "ReplicationConfiguration" {
        return Err(Error::Message(
            "expected ReplicationConfiguration".to_owned(),
        ));
    }
    let mut config = Config::default();
    for child in children(root) {
        match child.tag_name().name() {
            "Role" => config.role_arn = child.text().unwrap_or("").trim().to_owned(),
            "Rule" => config.rules.push(parse_rule(child)?),
            _ => {}
        }
    }
    for rule in &mut config.rules {
        if rule
            .source_selection_criteria
            .replica_modifications
            .status
            .is_empty()
        {
            rule.source_selection_criteria.replica_modifications.status = ENABLED.to_owned();
        }
        if rule.delete_replication.status.is_empty() {
            rule.delete_replication.status = DISABLED.to_owned();
        }
    }
    Ok(config)
}

fn parse_rule(node: Node<'_, '_>) -> Result<Rule, Error> {
    let mut rule = Rule::default();
    for child in children(node) {
        match child.tag_name().name() {
            "ID" => rule.id = child.text().unwrap_or("").trim().to_owned(),
            "Status" => rule.status = child.text().unwrap_or("").trim().to_owned(),
            "Priority" => rule.priority = parse_i32(child.text().unwrap_or("").trim())?,
            "DeleteMarkerReplication" => {
                rule.delete_marker_replication = DeleteMarkerReplication {
                    status: parse_status_child(child),
                }
            }
            "DeleteReplication" => {
                rule.delete_replication = DeleteReplication {
                    status: parse_status_child(child),
                }
            }
            "Destination" => rule.destination = parse_destination(child)?,
            "SourceSelectionCriteria" => {
                rule.source_selection_criteria = parse_source_selection_criteria(child)
            }
            "Filter" => rule.filter = parse_filter(child)?,
            "ExistingObjectReplication" => {
                rule.existing_object_replication = ExistingObjectReplication {
                    status: parse_status_child(child),
                }
            }
            "Prefix" => rule.filter.prefix = child.text().unwrap_or("").trim().to_owned(),
            _ => {}
        }
    }
    Ok(rule)
}

fn parse_status_child(node: Node<'_, '_>) -> String {
    children(node)
        .into_iter()
        .find(|child| child.tag_name().name() == "Status")
        .and_then(|child| child.text())
        .unwrap_or("")
        .trim()
        .to_owned()
}

fn parse_filter(node: Node<'_, '_>) -> Result<Filter, Error> {
    let mut filter = Filter::default();
    for child in children(node) {
        match child.tag_name().name() {
            "Prefix" => filter.prefix = child.text().unwrap_or("").trim().to_owned(),
            "Tag" => filter.tag = parse_tag(child),
            "And" => filter.and = parse_and(child),
            _ => {}
        }
    }
    Ok(filter)
}

fn parse_tag(node: Node<'_, '_>) -> Tag {
    let mut tag = Tag::default();
    for child in children(node) {
        match child.tag_name().name() {
            "Key" => tag.key = child.text().unwrap_or("").trim().to_owned(),
            "Value" => tag.value = child.text().unwrap_or("").trim().to_owned(),
            _ => {}
        }
    }
    tag
}

fn parse_and(node: Node<'_, '_>) -> And {
    let mut and = And::default();
    for child in children(node) {
        match child.tag_name().name() {
            "Prefix" => and.prefix = child.text().unwrap_or("").trim().to_owned(),
            "Tag" => and.tags.push(parse_tag(child)),
            _ => {}
        }
    }
    and
}

fn parse_source_selection_criteria(node: Node<'_, '_>) -> SourceSelectionCriteria {
    let mut criteria = SourceSelectionCriteria::default();
    for child in children(node) {
        if child.tag_name().name() == "ReplicaModifications" {
            criteria.replica_modifications.status = parse_status_child(child);
        }
    }
    criteria
}

fn parse_destination(node: Node<'_, '_>) -> Result<Destination, Error> {
    let mut bucket = String::new();
    let mut storage_class = String::new();
    for child in children(node) {
        match child.tag_name().name() {
            "Bucket" => bucket = child.text().unwrap_or("").trim().to_owned(),
            "StorageClass" => storage_class = child.text().unwrap_or("").trim().to_owned(),
            _ => {}
        }
    }
    let mut destination = parse_destination_arn(&bucket)?;
    if !storage_class.is_empty()
        && storage_class != "STANDARD"
        && storage_class != "REDUCED_REDUNDANCY"
    {
        return Err(Error::Message(format!(
            "unknown storage class {storage_class}"
        )));
    }
    destination.storage_class = storage_class;
    Ok(destination)
}

fn parse_destination_arn(value: &str) -> Result<Destination, Error> {
    if !value.starts_with(DESTINATION_ARN_PREFIX)
        && !value.starts_with(DESTINATION_ARN_MINIO_PREFIX)
    {
        return Err(Error::Message(format!("invalid destination '{value}'")));
    }
    let bucket = value
        .trim_start_matches(DESTINATION_ARN_PREFIX)
        .split(':')
        .next_back()
        .unwrap_or("")
        .to_owned();
    Ok(Destination {
        bucket,
        arn: value.to_owned(),
        ..Destination::default()
    })
}

fn parse_i32(value: &str) -> Result<i32, Error> {
    value
        .parse::<i32>()
        .map_err(|_| Error::Message("invalid integer".to_owned()))
}

fn children<'a, 'input>(node: Node<'a, 'input>) -> Vec<Node<'a, 'input>> {
    node.children().filter(Node::is_element).collect()
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
