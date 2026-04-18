mod constants;
mod error;
mod evaluation;
mod model;
mod xml;

pub use self::constants::{AMZ_EXPIRATION, DISABLED, ENABLED, MINIO_TRANSITION};
pub use self::error::Error;
pub use self::evaluation::expected_expiry_time;
pub use self::model::{
    Action, And, Boolean, DelMarkerExpiration, Evaluator, Event, Expiration, Filter, Lifecycle,
    NoncurrentVersionExpiration, NoncurrentVersionTransition, ObjectOpts, Prefix, Retention, Rule,
    Tag, Transition,
};
pub use self::xml::{
    parse_lifecycle_config, parse_lifecycle_config_with_id, parse_noncurrent_version_expiration,
};
