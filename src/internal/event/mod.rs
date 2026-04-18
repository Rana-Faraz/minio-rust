use std::collections::{HashMap, HashSet};
use std::fmt;

mod config;
pub use config::{
    parse_config, validate_filter_rule_value, validate_filter_rule_value_bytes, Config, FilterRule,
    FilterRuleList, Queue, S3Key,
};
pub mod target;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    fn invalid_arn(value: &str) -> Self {
        Self(format!("invalid ARN '{}'", value))
    }

    fn invalid_event_name(value: &str) -> Self {
        Self(format!("invalid event name '{}'", value))
    }

    fn invalid_target_id(value: &str) -> Self {
        Self(format!("invalid TargetID format '{}'", value))
    }

    fn invalid_xml(value: &str) -> Self {
        Self(format!("invalid XML '{}'", value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

fn extract_xml_text(data: &[u8], tag: &str) -> Result<String, Error> {
    let text = String::from_utf8(data.to_vec()).map_err(|error| Error(error.to_string()))?;
    let prefix = format!("<{tag}>");
    let suffix = format!("</{tag}>");
    if !text.starts_with(&prefix) || !text.ends_with(&suffix) {
        return Err(Error::invalid_xml(&text));
    }
    Ok(text[prefix.len()..text.len() - suffix.len()].to_owned())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct TargetId {
    pub id: String,
    pub name: String,
}

impl TargetId {
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
        }
    }

    pub fn to_arn(&self, region: &str) -> Arn {
        Arn {
            target_id: self.clone(),
            region: region.to_owned(),
        }
    }

    pub fn marshal_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.to_string())
    }

    pub fn unmarshal_json(data: &[u8]) -> Result<Self, Error> {
        let value: String =
            serde_json::from_slice(data).map_err(|error| Error(error.to_string()))?;
        parse_target_id(&value)
    }
}

impl fmt::Display for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.id, self.name)
    }
}

pub fn parse_target_id(value: &str) -> Result<TargetId, Error> {
    let tokens: Vec<_> = value.split(':').collect();
    if tokens.len() != 2 {
        return Err(Error::invalid_target_id(value));
    }

    Ok(TargetId::new(tokens[0], tokens[1]))
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Arn {
    pub target_id: TargetId,
    pub region: String,
}

impl Arn {
    pub fn parse(value: &str) -> Result<Self, Error> {
        if !value.starts_with("arn:minio:sqs:") {
            return Err(Error::invalid_arn(value));
        }

        let tokens: Vec<_> = value.split(':').collect();
        if tokens.len() != 6 || tokens[4].is_empty() || tokens[5].is_empty() {
            return Err(Error::invalid_arn(value));
        }

        Ok(Self {
            target_id: TargetId::new(tokens[4], tokens[5]),
            region: tokens[3].to_owned(),
        })
    }

    pub fn marshal_xml(&self) -> String {
        format!("<ARN>{}</ARN>", self)
    }

    pub fn unmarshal_xml(data: &[u8]) -> Result<Self, Error> {
        let value = extract_xml_text(data, "ARN")?;
        Self::parse(&value)
    }
}

impl fmt::Display for Arn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.target_id.id.is_empty() && self.target_id.name.is_empty() && self.region.is_empty()
        {
            return Ok(());
        }

        write!(f, "arn:minio:sqs:{}:{}", self.region, self.target_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TargetIdSet(HashSet<TargetId>);

impl TargetIdSet {
    pub fn new(target_ids: impl IntoIterator<Item = TargetId>) -> Self {
        Self(target_ids.into_iter().collect())
    }

    pub fn clone_set(&self) -> Self {
        self.clone()
    }

    pub fn add(&mut self, target_id: TargetId) {
        self.0.insert(target_id);
    }

    pub fn union(&self, other: &Self) -> Self {
        Self(self.0.union(&other.0).cloned().collect())
    }

    pub fn difference(&self, other: &Self) -> Self {
        Self(self.0.difference(&other.0).cloned().collect())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn to_vec(&self) -> Vec<TargetId> {
        self.0.iter().cloned().collect()
    }
}

fn match_simple(pattern: &str, text: &str) -> bool {
    let pattern = pattern.as_bytes();
    let text = text.as_bytes();
    let (mut pi, mut ti) = (0_usize, 0_usize);
    let mut star: Option<usize> = None;
    let mut match_after_star = 0_usize;

    while ti < text.len() {
        if pi < pattern.len() && pattern[pi] == text[ti] {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star = Some(pi);
            pi += 1;
            match_after_star = ti;
        } else if let Some(star_index) = star {
            pi = star_index + 1;
            match_after_star += 1;
            ti = match_after_star;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

pub fn new_pattern(prefix: &str, suffix: &str) -> String {
    let mut pattern = String::new();

    if !prefix.is_empty() {
        pattern.push_str(prefix);
        if !prefix.ends_with('*') {
            pattern.push('*');
        }
    }

    if !suffix.is_empty() {
        if !suffix.starts_with('*') {
            pattern.push('*');
        }
        pattern.push_str(suffix);
    }

    pattern.replace("**", "*")
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rules(HashMap<String, TargetIdSet>);

impl Rules {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn add(&mut self, pattern: impl Into<String>, target_id: TargetId) {
        let pattern = pattern.into();
        let current = self
            .0
            .get(&pattern)
            .cloned()
            .unwrap_or_else(|| TargetIdSet::new([]));
        self.0
            .insert(pattern, TargetIdSet::new([target_id]).union(&current));
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn match_simple(&self, object_name: &str) -> bool {
        self.0
            .keys()
            .any(|pattern| match_simple(pattern, object_name))
    }

    pub fn match_object(&self, object_name: &str) -> TargetIdSet {
        let mut target_ids = TargetIdSet::new([]);
        for (pattern, target_id_set) in &self.0 {
            if match_simple(pattern, object_name) {
                target_ids = target_ids.union(target_id_set);
            }
        }
        target_ids
    }

    pub fn clone_rules(&self) -> Self {
        self.clone()
    }

    pub fn union(&self, other: &Self) -> Self {
        let mut combined = self.clone();
        for (pattern, target_id_set) in &other.0 {
            let current = combined
                .0
                .get(pattern)
                .cloned()
                .unwrap_or_else(|| TargetIdSet::new([]));
            combined
                .0
                .insert(pattern.clone(), current.union(target_id_set));
        }
        combined
    }

    pub fn difference(&self, other: &Self) -> Self {
        let mut result = Self::new();
        for (pattern, target_id_set) in &self.0 {
            let diff =
                target_id_set.difference(other.0.get(pattern).unwrap_or(&TargetIdSet::new([])));
            if !diff.is_empty() {
                result.0.insert(pattern.clone(), diff);
            }
        }
        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RulesMap(HashMap<Name, Rules>);

impl RulesMap {
    pub fn new(event_names: &[Name], pattern: &str, target_id: TargetId) -> Self {
        let pattern = if pattern.is_empty() { "*" } else { pattern };
        let mut rules_map = Self(HashMap::new());
        rules_map.add_expanded(event_names, pattern, target_id);
        rules_map
    }

    fn add_expanded(&mut self, event_names: &[Name], pattern: &str, target_id: TargetId) {
        let mut rules = Rules::new();
        rules.add(pattern.to_owned(), target_id);

        for event_name in event_names {
            for expanded in event_name.expand() {
                let current = self.0.get(&expanded).cloned().unwrap_or_else(Rules::new);
                self.0.insert(expanded, current.union(&rules));
            }
        }
    }

    pub fn clone_map(&self) -> Self {
        self.clone()
    }

    pub fn add(&mut self, other: &Self) {
        for (event_name, rules) in &other.0 {
            let current = self.0.get(event_name).cloned().unwrap_or_else(Rules::new);
            self.0.insert(*event_name, rules.union(&current));
        }
    }

    pub fn remove(&mut self, other: &Self) {
        let keys: Vec<Name> = self.0.keys().copied().collect();
        for event_name in keys {
            let Some(rules) = self.0.get(&event_name).cloned() else {
                continue;
            };
            let diff = rules.difference(other.0.get(&event_name).unwrap_or(&Rules::new()));
            if diff.len() == 0 {
                self.0.remove(&event_name);
            } else {
                self.0.insert(event_name, diff);
            }
        }
    }

    pub fn match_object(&self, event_name: Name, object_name: &str) -> TargetIdSet {
        self.0
            .get(&event_name)
            .map(|rules| rules.match_object(object_name))
            .unwrap_or_else(|| TargetIdSet::new([]))
    }
}

pub trait Target: Send + Sync {
    fn id(&self) -> TargetId;
}

#[derive(Default)]
pub struct TargetList {
    targets: HashMap<TargetId, Box<dyn Target>>,
}

impl TargetList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add<T: Target + 'static>(&mut self, target: T) -> Result<(), Error> {
        let id = target.id();
        if self.targets.contains_key(&id) {
            return Err(Error(format!("target {} already exists", id)));
        }
        self.targets.insert(id, Box::new(target));
        Ok(())
    }

    pub fn exists(&self, id: &TargetId) -> bool {
        self.targets.contains_key(id)
    }

    pub fn list(&self) -> Vec<TargetId> {
        self.targets.keys().cloned().collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Name {
    ObjectAccessedGet,
    ObjectAccessedGetRetention,
    ObjectAccessedGetLegalHold,
    ObjectAccessedHead,
    ObjectAccessedAttributes,
    ObjectCreatedCompleteMultipartUpload,
    ObjectCreatedCopy,
    ObjectCreatedPost,
    ObjectCreatedPut,
    ObjectCreatedPutRetention,
    ObjectCreatedPutLegalHold,
    ObjectCreatedPutTagging,
    ObjectCreatedDeleteTagging,
    ObjectRemovedDelete,
    ObjectRemovedDeleteMarkerCreated,
    ObjectRemovedDeleteAllVersions,
    ObjectRemovedNoOP,
    BucketCreated,
    BucketRemoved,
    ObjectReplicationFailed,
    ObjectReplicationComplete,
    ObjectReplicationMissedThreshold,
    ObjectReplicationReplicatedAfterThreshold,
    ObjectReplicationNotTracked,
    ObjectRestorePost,
    ObjectRestoreCompleted,
    ObjectTransitionFailed,
    ObjectTransitionComplete,
    ObjectManyVersions,
    ObjectLargeVersions,
    PrefixManyFolders,
    IlmDelMarkerExpirationDelete,
    ObjectAccessedAll,
    ObjectCreatedAll,
    ObjectRemovedAll,
    ObjectReplicationAll,
    ObjectRestoreAll,
    ObjectTransitionAll,
    ObjectScannerAll,
    Everything,
}

impl Name {
    pub fn expand(self) -> Vec<Self> {
        match self {
            Self::ObjectAccessedAll => vec![
                Self::ObjectAccessedGet,
                Self::ObjectAccessedHead,
                Self::ObjectAccessedGetRetention,
                Self::ObjectAccessedGetLegalHold,
                Self::ObjectAccessedAttributes,
            ],
            Self::ObjectCreatedAll => vec![
                Self::ObjectCreatedCompleteMultipartUpload,
                Self::ObjectCreatedCopy,
                Self::ObjectCreatedPost,
                Self::ObjectCreatedPut,
                Self::ObjectCreatedPutRetention,
                Self::ObjectCreatedPutLegalHold,
                Self::ObjectCreatedPutTagging,
                Self::ObjectCreatedDeleteTagging,
            ],
            Self::ObjectRemovedAll => vec![
                Self::ObjectRemovedDelete,
                Self::ObjectRemovedDeleteMarkerCreated,
                Self::ObjectRemovedNoOP,
                Self::ObjectRemovedDeleteAllVersions,
            ],
            Self::ObjectReplicationAll => vec![
                Self::ObjectReplicationFailed,
                Self::ObjectReplicationComplete,
                Self::ObjectReplicationNotTracked,
                Self::ObjectReplicationMissedThreshold,
                Self::ObjectReplicationReplicatedAfterThreshold,
            ],
            Self::ObjectRestoreAll => {
                vec![Self::ObjectRestorePost, Self::ObjectRestoreCompleted]
            }
            Self::ObjectTransitionAll => {
                vec![Self::ObjectTransitionFailed, Self::ObjectTransitionComplete]
            }
            Self::ObjectScannerAll => vec![
                Self::ObjectManyVersions,
                Self::ObjectLargeVersions,
                Self::PrefixManyFolders,
            ],
            Self::Everything => vec![
                Self::ObjectAccessedGet,
                Self::ObjectAccessedGetRetention,
                Self::ObjectAccessedGetLegalHold,
                Self::ObjectAccessedHead,
                Self::ObjectAccessedAttributes,
                Self::ObjectCreatedCompleteMultipartUpload,
                Self::ObjectCreatedCopy,
                Self::ObjectCreatedPost,
                Self::ObjectCreatedPut,
                Self::ObjectCreatedPutRetention,
                Self::ObjectCreatedPutLegalHold,
                Self::ObjectCreatedPutTagging,
                Self::ObjectCreatedDeleteTagging,
                Self::ObjectRemovedDelete,
                Self::ObjectRemovedDeleteMarkerCreated,
                Self::ObjectRemovedDeleteAllVersions,
                Self::ObjectRemovedNoOP,
                Self::BucketCreated,
                Self::BucketRemoved,
                Self::ObjectReplicationFailed,
                Self::ObjectReplicationComplete,
                Self::ObjectReplicationMissedThreshold,
                Self::ObjectReplicationReplicatedAfterThreshold,
                Self::ObjectReplicationNotTracked,
                Self::ObjectRestorePost,
                Self::ObjectRestoreCompleted,
                Self::ObjectTransitionFailed,
                Self::ObjectTransitionComplete,
                Self::ObjectManyVersions,
                Self::ObjectLargeVersions,
                Self::PrefixManyFolders,
                Self::IlmDelMarkerExpirationDelete,
            ],
            single => vec![single],
        }
    }

    pub fn marshal_xml(self) -> String {
        format!("<Name>{}</Name>", self)
    }

    pub fn unmarshal_xml(data: &[u8]) -> Result<Self, Error> {
        let value = extract_xml_text(data, "Name")?;
        parse_name(&value)
    }

    pub fn marshal_json(self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&self.to_string())
    }

    pub fn unmarshal_json(data: &[u8]) -> Result<Self, Error> {
        let value: String =
            serde_json::from_slice(data).map_err(|error| Error(error.to_string()))?;
        parse_name(&value)
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::BucketCreated => "s3:BucketCreated:*",
            Self::BucketRemoved => "s3:BucketRemoved:*",
            Self::ObjectAccessedAll => "s3:ObjectAccessed:*",
            Self::ObjectAccessedGet => "s3:ObjectAccessed:Get",
            Self::ObjectAccessedGetRetention => "s3:ObjectAccessed:GetRetention",
            Self::ObjectAccessedGetLegalHold => "s3:ObjectAccessed:GetLegalHold",
            Self::ObjectAccessedHead => "s3:ObjectAccessed:Head",
            Self::ObjectAccessedAttributes => "s3:ObjectAccessed:Attributes",
            Self::ObjectCreatedAll => "s3:ObjectCreated:*",
            Self::ObjectCreatedCompleteMultipartUpload => {
                "s3:ObjectCreated:CompleteMultipartUpload"
            }
            Self::ObjectCreatedCopy => "s3:ObjectCreated:Copy",
            Self::ObjectCreatedPost => "s3:ObjectCreated:Post",
            Self::ObjectCreatedPut => "s3:ObjectCreated:Put",
            Self::ObjectCreatedPutTagging => "s3:ObjectCreated:PutTagging",
            Self::ObjectCreatedDeleteTagging => "s3:ObjectCreated:DeleteTagging",
            Self::ObjectCreatedPutRetention => "s3:ObjectCreated:PutRetention",
            Self::ObjectCreatedPutLegalHold => "s3:ObjectCreated:PutLegalHold",
            Self::ObjectRemovedAll => "s3:ObjectRemoved:*",
            Self::ObjectRemovedDelete => "s3:ObjectRemoved:Delete",
            Self::ObjectRemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
            Self::ObjectRemovedNoOP => "s3:ObjectRemoved:NoOP",
            Self::ObjectRemovedDeleteAllVersions => "s3:ObjectRemoved:DeleteAllVersions",
            Self::IlmDelMarkerExpirationDelete => "s3:LifecycleDelMarkerExpiration:Delete",
            Self::ObjectReplicationAll => "s3:Replication:*",
            Self::ObjectReplicationFailed => "s3:Replication:OperationFailedReplication",
            Self::ObjectReplicationComplete => "s3:Replication:OperationCompletedReplication",
            Self::ObjectReplicationNotTracked => "s3:Replication:OperationNotTracked",
            Self::ObjectReplicationMissedThreshold => "s3:Replication:OperationMissedThreshold",
            Self::ObjectReplicationReplicatedAfterThreshold => {
                "s3:Replication:OperationReplicatedAfterThreshold"
            }
            Self::ObjectRestoreAll => "s3:ObjectRestore:*",
            Self::ObjectRestorePost => "s3:ObjectRestore:Post",
            Self::ObjectRestoreCompleted => "s3:ObjectRestore:Completed",
            Self::ObjectTransitionAll => "s3:ObjectTransition:*",
            Self::ObjectTransitionFailed => "s3:ObjectTransition:Failed",
            Self::ObjectTransitionComplete => "s3:ObjectTransition:Complete",
            Self::ObjectManyVersions => "s3:Scanner:ManyVersions",
            Self::ObjectLargeVersions => "s3:Scanner:LargeVersions",
            Self::PrefixManyFolders => "s3:Scanner:BigPrefix",
            Self::ObjectScannerAll => "s3:Scanner:*",
            Self::Everything => "s3:*",
        };
        f.write_str(value)
    }
}

pub fn parse_name(value: &str) -> Result<Name, Error> {
    match value {
        "s3:BucketCreated:*" => Ok(Name::BucketCreated),
        "s3:BucketRemoved:*" => Ok(Name::BucketRemoved),
        "s3:ObjectAccessed:*" => Ok(Name::ObjectAccessedAll),
        "s3:ObjectAccessed:Get" => Ok(Name::ObjectAccessedGet),
        "s3:ObjectAccessed:GetRetention" => Ok(Name::ObjectAccessedGetRetention),
        "s3:ObjectAccessed:GetLegalHold" => Ok(Name::ObjectAccessedGetLegalHold),
        "s3:ObjectAccessed:Head" => Ok(Name::ObjectAccessedHead),
        "s3:ObjectAccessed:Attributes" => Ok(Name::ObjectAccessedAttributes),
        "s3:ObjectCreated:*" => Ok(Name::ObjectCreatedAll),
        "s3:ObjectCreated:CompleteMultipartUpload" => {
            Ok(Name::ObjectCreatedCompleteMultipartUpload)
        }
        "s3:ObjectCreated:Copy" => Ok(Name::ObjectCreatedCopy),
        "s3:ObjectCreated:Post" => Ok(Name::ObjectCreatedPost),
        "s3:ObjectCreated:Put" => Ok(Name::ObjectCreatedPut),
        "s3:ObjectCreated:PutRetention" => Ok(Name::ObjectCreatedPutRetention),
        "s3:ObjectCreated:PutLegalHold" => Ok(Name::ObjectCreatedPutLegalHold),
        "s3:ObjectCreated:PutTagging" => Ok(Name::ObjectCreatedPutTagging),
        "s3:ObjectCreated:DeleteTagging" => Ok(Name::ObjectCreatedDeleteTagging),
        "s3:ObjectRemoved:*" => Ok(Name::ObjectRemovedAll),
        "s3:ObjectRemoved:Delete" => Ok(Name::ObjectRemovedDelete),
        "s3:ObjectRemoved:DeleteMarkerCreated" => Ok(Name::ObjectRemovedDeleteMarkerCreated),
        "s3:ObjectRemoved:NoOP" => Ok(Name::ObjectRemovedNoOP),
        "s3:ObjectRemoved:DeleteAllVersions" => Ok(Name::ObjectRemovedDeleteAllVersions),
        "s3:LifecycleDelMarkerExpiration:Delete" => Ok(Name::IlmDelMarkerExpirationDelete),
        "s3:Replication:*" => Ok(Name::ObjectReplicationAll),
        "s3:Replication:OperationFailedReplication" => Ok(Name::ObjectReplicationFailed),
        "s3:Replication:OperationCompletedReplication" => Ok(Name::ObjectReplicationComplete),
        "s3:Replication:OperationMissedThreshold" => Ok(Name::ObjectReplicationMissedThreshold),
        "s3:Replication:OperationReplicatedAfterThreshold" => {
            Ok(Name::ObjectReplicationReplicatedAfterThreshold)
        }
        "s3:Replication:OperationNotTracked" => Ok(Name::ObjectReplicationNotTracked),
        "s3:ObjectRestore:*" => Ok(Name::ObjectRestoreAll),
        "s3:ObjectRestore:Post" => Ok(Name::ObjectRestorePost),
        "s3:ObjectRestore:Completed" => Ok(Name::ObjectRestoreCompleted),
        "s3:ObjectTransition:*" => Ok(Name::ObjectTransitionAll),
        "s3:ObjectTransition:Failed" => Ok(Name::ObjectTransitionFailed),
        "s3:ObjectTransition:Complete" => Ok(Name::ObjectTransitionComplete),
        "s3:Scanner:ManyVersions" => Ok(Name::ObjectManyVersions),
        "s3:Scanner:LargeVersions" => Ok(Name::ObjectLargeVersions),
        "s3:Scanner:BigPrefix" => Ok(Name::PrefixManyFolders),
        _ => Err(Error::invalid_event_name(value)),
    }
}
