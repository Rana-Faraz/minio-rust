use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use crate::cmd::{TierConfigMgr, AMZ_RESTORE_HEADER};
use crate::internal::bucket::lifecycle::Lifecycle;

pub const ERR_RESTORE_HDR_MALFORMED: &str = "restore header malformed";
pub const ERR_INVALID_STORAGE_CLASS: &str = "invalid storage class";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreObjStatus {
    pub ongoing: bool,
    pub expiry: Option<DateTime<Utc>>,
}

impl RestoreObjStatus {
    pub fn on_disk(&self) -> bool {
        !self.ongoing && self.expiry.is_some_and(|expiry| expiry > Utc::now())
    }
}

impl Display for RestoreObjStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.ongoing {
            return f.write_str(r#"ongoing-request="true""#);
        }
        let expiry = self
            .expiry
            .expect("completed restore status must include expiry");
        write!(
            f,
            r#"ongoing-request="false", expiry-date="{}""#,
            expiry.format("%a, %d %b %Y %H:%M:%S GMT")
        )
    }
}

pub fn ongoing_restore_obj_status() -> RestoreObjStatus {
    RestoreObjStatus {
        ongoing: true,
        expiry: None,
    }
}

pub fn completed_restore_obj_status(expires_at: DateTime<Utc>) -> RestoreObjStatus {
    RestoreObjStatus {
        ongoing: false,
        expiry: Some(expires_at),
    }
}

pub fn parse_restore_obj_status(header: &str) -> Result<RestoreObjStatus, String> {
    let ongoing = if header.contains(r#"ongoing-request="true""#) {
        true
    } else if header.contains(r#"ongoing-request="false""#) {
        false
    } else {
        return Err(ERR_RESTORE_HDR_MALFORMED.to_string());
    };

    let expiry = extract_expiry(header)?;
    match (ongoing, expiry) {
        (true, None) => Ok(ongoing_restore_obj_status()),
        (false, Some(expiry)) => Ok(completed_restore_obj_status(expiry)),
        _ => Err(ERR_RESTORE_HDR_MALFORMED.to_string()),
    }
}

pub fn is_restored_object_on_disk(meta: &BTreeMap<String, String>) -> bool {
    meta.get(AMZ_RESTORE_HEADER)
        .and_then(|header| parse_restore_obj_status(header).ok())
        .is_some_and(|status| status.on_disk())
}

pub fn validate_transition_tier(
    lifecycle: &Lifecycle,
    tier_config_mgr: &TierConfigMgr,
) -> Result<(), String> {
    for rule in &lifecycle.rules {
        for storage_class in [
            rule.transition.storage_class.as_str(),
            rule.noncurrent_version_transition.storage_class.as_str(),
        ] {
            if storage_class.is_empty() {
                continue;
            }
            if !tier_config_mgr.tiers.contains_key(storage_class) {
                return Err(ERR_INVALID_STORAGE_CLASS.to_string());
            }
        }
    }
    Ok(())
}

fn extract_expiry(header: &str) -> Result<Option<DateTime<Utc>>, String> {
    let marker = r#"expiry-date=""#;
    let Some(start) = header.find(marker).map(|index| index + marker.len()) else {
        return Ok(None);
    };
    let Some(end) = header[start..].find('"').map(|index| index + start) else {
        return Err(ERR_RESTORE_HDR_MALFORMED.to_string());
    };
    DateTime::parse_from_rfc2822(&header[start..end])
        .map(|expiry| Some(expiry.with_timezone(&Utc)))
        .map_err(|_| ERR_RESTORE_HDR_MALFORMED.to_string())
}
