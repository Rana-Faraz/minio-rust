use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KmsEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsPolicyStatement {
    pub effect: KmsEffect,
    pub actions: BTreeSet<String>,
    pub resources: Vec<String>,
}

impl KmsPolicyStatement {
    pub fn allow(actions: &[&str], resources: &[&str]) -> Self {
        Self {
            effect: KmsEffect::Allow,
            actions: actions.iter().map(|value| (*value).to_string()).collect(),
            resources: resources.iter().map(|value| (*value).to_string()).collect(),
        }
    }

    pub fn deny(actions: &[&str], resources: &[&str]) -> Self {
        Self {
            effect: KmsEffect::Deny,
            actions: actions.iter().map(|value| (*value).to_string()).collect(),
            resources: resources.iter().map(|value| (*value).to_string()).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsUser {
    pub access_key: String,
    pub secret_key: String,
    pub statements: Vec<KmsPolicyStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KmsKeyInfo {
    pub name: String,
    pub created_at: String,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KmsError {
    NotConfigured,
    AccessDenied,
    KeyNotFound,
}

pub const STUB_CREATED_AT: &str = "2024-01-01T15:00:00Z";
pub const STUB_CREATED_BY: &str = "MinIO";
