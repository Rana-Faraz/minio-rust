use std::collections::HashSet;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    fn invalid_arn(value: &str) -> Self {
        Self(format!("invalid ARN '{}'", value))
    }

    fn invalid_target_id(value: &str) -> Self {
        Self(format!("invalid TargetID format '{}'", value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

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
        if !value.starts_with("arn:minio:s3-object-lambda:") {
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
}

impl fmt::Display for Arn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.target_id.id.is_empty() && self.target_id.name.is_empty() && self.region.is_empty()
        {
            return Ok(());
        }

        write!(
            f,
            "arn:minio:s3-object-lambda:{}:{}",
            self.region, self.target_id
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TargetIdSet(HashSet<TargetId>);

impl TargetIdSet {
    pub fn new(target_ids: impl IntoIterator<Item = TargetId>) -> Self {
        Self(target_ids.into_iter().collect())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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
}
