use regex::Regex;
use std::error::Error as StdError;
use std::fmt;
use std::sync::OnceLock;

const ARN_PREFIX: &str = "arn";
const PARTITION_MINIO: &str = "minio";
const SERVICE_IAM: &str = "iam";
const RESOURCE_TYPE_ROLE: &str = "role";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Arn {
    pub partition: String,
    pub service: String,
    pub region: String,
    pub resource_type: String,
    pub resource_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidResourceId(String),
    InvalidFormat,
    InvalidPartition,
    InvalidService,
    UnsupportedAccountId,
    MissingResourceSeparator,
    InvalidResourceType,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidResourceId(value) => write!(f, "invalid resource ID: {value}"),
            Self::InvalidFormat => f.write_str("invalid ARN string format"),
            Self::InvalidPartition => f.write_str("invalid ARN - bad partition field"),
            Self::InvalidService => f.write_str("invalid ARN - bad service field"),
            Self::UnsupportedAccountId => f.write_str("invalid ARN - unsupported account-id field"),
            Self::MissingResourceSeparator => {
                f.write_str("invalid ARN - resource does not contain a \"/\"")
            }
            Self::InvalidResourceType => f.write_str("invalid ARN: resource type is invalid"),
        }
    }
}

impl StdError for Error {}

impl fmt::Display for Arn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{ARN_PREFIX}:{partition}:{service}:{region}::{resource_type}/{resource_id}",
            partition = self.partition,
            service = self.service,
            region = self.region,
            resource_type = self.resource_type,
            resource_id = self.resource_id
        )
    }
}

pub fn new_iam_role_arn(resource_id: &str, server_region: &str) -> Result<Arn, Error> {
    validate_resource_id(resource_id)?;
    Ok(Arn {
        partition: PARTITION_MINIO.to_owned(),
        service: SERVICE_IAM.to_owned(),
        region: server_region.to_owned(),
        resource_type: RESOURCE_TYPE_ROLE.to_owned(),
        resource_id: resource_id.to_owned(),
    })
}

pub fn parse(value: &str) -> Result<Arn, Error> {
    let parts: Vec<_> = value.split(':').collect();
    if parts.len() != 6 || parts[0] != ARN_PREFIX {
        return Err(Error::InvalidFormat);
    }
    if parts[1] != PARTITION_MINIO {
        return Err(Error::InvalidPartition);
    }
    if parts[2] != SERVICE_IAM {
        return Err(Error::InvalidService);
    }
    if !parts[4].is_empty() {
        return Err(Error::UnsupportedAccountId);
    }

    let Some((resource_type, resource_id)) = parts[5].split_once('/') else {
        return Err(Error::MissingResourceSeparator);
    };
    if resource_type != RESOURCE_TYPE_ROLE {
        return Err(Error::InvalidResourceType);
    }
    validate_resource_id(resource_id)?;

    Ok(Arn {
        partition: PARTITION_MINIO.to_owned(),
        service: SERVICE_IAM.to_owned(),
        region: parts[3].to_owned(),
        resource_type: RESOURCE_TYPE_ROLE.to_owned(),
        resource_id: resource_id.to_owned(),
    })
}

fn validate_resource_id(value: &str) -> Result<(), Error> {
    static VALID_RESOURCE_ID: OnceLock<Regex> = OnceLock::new();
    let regex =
        VALID_RESOURCE_ID.get_or_init(|| Regex::new(r"^[A-Za-z0-9_/\.-]+$").expect("valid regex"));
    if regex.is_match(value) {
        Ok(())
    } else {
        Err(Error::InvalidResourceId(value.to_owned()))
    }
}
