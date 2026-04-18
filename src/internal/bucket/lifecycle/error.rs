#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    LifecycleTooManyRules,
    LifecycleNoRule,
    LifecycleDuplicateId,
    XmlNotWellFormed,
    LifecycleBucketLocked,
    InvalidFilter,
    InvalidRuleId,
    EmptyRuleStatus,
    InvalidRuleStatus,
    InvalidRuleDelMarkerExpiration,
    LifecycleInvalidDate,
    LifecycleInvalidDays,
    LifecycleInvalidExpiration,
    LifecycleInvalidDeleteMarker,
    LifecycleDateNotMidnight,
    LifecycleInvalidDeleteAll,
    InvalidDaysDelMarkerExpiration,
    TransitionInvalidDays,
    TransitionInvalidDate,
    TransitionInvalid,
    TransitionDateNotMidnight,
    InvalidTagKey,
    InvalidTagValue,
    DuplicatedXmlTag,
    UnknownXmlTag,
    DuplicateTagKey,
    Parse(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LifecycleTooManyRules => {
                f.write_str("Lifecycle configuration allows a maximum of 1000 rules")
            }
            Self::LifecycleNoRule => {
                f.write_str("Lifecycle configuration should have at least one rule")
            }
            Self::LifecycleDuplicateId => {
                f.write_str("Rule ID must be unique. Found same ID for more than one rule")
            }
            Self::XmlNotWellFormed => f.write_str(
                "The XML you provided was not well-formed or did not validate against our published schema",
            ),
            Self::LifecycleBucketLocked => f.write_str(
                "ExpiredObjectAllVersions element and DelMarkerExpiration action cannot be used on an object locked bucket",
            ),
            Self::InvalidFilter => {
                f.write_str("Filter must have exactly one of Prefix, Tag, or And specified")
            }
            Self::InvalidRuleId => f.write_str("ID length is limited to 255 characters"),
            Self::EmptyRuleStatus => f.write_str("Status should not be empty"),
            Self::InvalidRuleStatus => {
                f.write_str("Status must be set to either Enabled or Disabled")
            }
            Self::InvalidRuleDelMarkerExpiration => {
                f.write_str("Rule with DelMarkerExpiration cannot have tags based filtering")
            }
            Self::LifecycleInvalidDate => {
                f.write_str("Date must be provided in ISO 8601 format")
            }
            Self::LifecycleInvalidDays => {
                f.write_str("Days must be positive integer when used with Expiration")
            }
            Self::LifecycleInvalidExpiration => f.write_str(
                "Exactly one of Days (positive integer) or Date (positive ISO 8601 format) should be present inside Expiration.",
            ),
            Self::LifecycleInvalidDeleteMarker => f.write_str(
                "Delete marker cannot be specified with Days or Date in a Lifecycle Expiration Policy",
            ),
            Self::LifecycleDateNotMidnight => f.write_str("'Date' must be at midnight GMT"),
            Self::LifecycleInvalidDeleteAll => f.write_str(
                "Days (positive integer) should be present inside Expiration with ExpiredObjectAllVersions.",
            ),
            Self::InvalidDaysDelMarkerExpiration => {
                f.write_str("Days must be a positive integer with DelMarkerExpiration")
            }
            Self::TransitionInvalidDays => {
                f.write_str("Days must be 0 or greater when used with Transition")
            }
            Self::TransitionInvalidDate => {
                f.write_str("Date must be provided in ISO 8601 format")
            }
            Self::TransitionInvalid => f.write_str(
                "Exactly one of Days (0 or greater) or Date (positive ISO 8601 format) should be present in Transition.",
            ),
            Self::TransitionDateNotMidnight => f.write_str("'Date' must be at midnight GMT"),
            Self::InvalidTagKey => f.write_str("The TagKey you have provided is invalid"),
            Self::InvalidTagValue => f.write_str("The TagValue you have provided is invalid"),
            Self::DuplicatedXmlTag => f.write_str("duplicated XML Tag"),
            Self::UnknownXmlTag => f.write_str("unknown XML Tag"),
            Self::DuplicateTagKey => f.write_str("Duplicate Tag Keys are not allowed"),
            Self::Parse(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}
