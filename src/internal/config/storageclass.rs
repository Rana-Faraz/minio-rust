use std::fmt;

pub const RRS: &str = "REDUCED_REDUNDANCY";
pub const STANDARD: &str = "STANDARD";
const SCHEME_PREFIX: &str = "EC";
const MIN_PARITY_DRIVES: i32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StorageClass {
    pub parity: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub standard: StorageClass,
    pub rrs: StorageClass,
    pub initialized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageClassError(String);

impl StorageClassError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for StorageClassError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StorageClassError {}

pub fn is_valid(sc: &str) -> bool {
    matches!(sc, RRS | STANDARD)
}

pub fn parse_storage_class(storage_class_env: &str) -> Result<StorageClass, StorageClassError> {
    let parts = storage_class_env.split(':').collect::<Vec<_>>();

    if parts.len() > 2 {
        return Err(StorageClassError::new(format!(
            "Too many sections in {storage_class_env}"
        )));
    }
    if parts.len() < 2 {
        return Err(StorageClassError::new(format!(
            "Too few sections in {storage_class_env}"
        )));
    }
    if parts[0] != SCHEME_PREFIX {
        return Err(StorageClassError::new(format!(
            "Unsupported scheme {}. Supported scheme is EC",
            parts[0]
        )));
    }

    let parity = parts[1]
        .parse::<i32>()
        .map_err(|err| StorageClassError::new(err.to_string()))?;
    if parity < 0 {
        return Err(StorageClassError::new(format!(
            "Unsupported parity value {} provided",
            parts[1]
        )));
    }

    Ok(StorageClass { parity })
}

pub fn validate_parity(
    ss_parity: i32,
    rrs_parity: i32,
    set_drive_count: i32,
) -> Result<(), StorageClassError> {
    if ss_parity > 0 && ss_parity < MIN_PARITY_DRIVES {
        return Err(StorageClassError::new(format!(
            "Standard storage class parity {ss_parity} should be greater than or equal to {MIN_PARITY_DRIVES}"
        )));
    }
    if rrs_parity > 0 && rrs_parity < MIN_PARITY_DRIVES {
        return Err(StorageClassError::new(format!(
            "Reduced redundancy storage class parity {rrs_parity} should be greater than or equal to {MIN_PARITY_DRIVES}"
        )));
    }
    if set_drive_count > 2 {
        if ss_parity > set_drive_count / 2 {
            return Err(StorageClassError::new(format!(
                "Standard storage class parity {ss_parity} should be less than or equal to {}",
                set_drive_count / 2
            )));
        }
        if rrs_parity > set_drive_count / 2 {
            return Err(StorageClassError::new(format!(
                "Reduced redundancy storage class parity {rrs_parity} should be less than or equal to {}",
                set_drive_count / 2
            )));
        }
    }
    if ss_parity > 0 && rrs_parity > 0 && ss_parity < rrs_parity {
        return Err(StorageClassError::new(format!(
            "Standard storage class parity drives {ss_parity} should be greater than or equal to Reduced redundancy storage class parity drives {rrs_parity}"
        )));
    }
    Ok(())
}

impl Config {
    pub fn get_parity_for_sc(&self, sc: &str) -> i32 {
        match sc.trim() {
            RRS => {
                if !self.initialized {
                    -1
                } else {
                    self.rrs.parity
                }
            }
            _ => {
                if !self.initialized {
                    -1
                } else {
                    self.standard.parity
                }
            }
        }
    }
}
