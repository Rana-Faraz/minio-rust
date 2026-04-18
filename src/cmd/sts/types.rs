use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StsPolicy {
    pub name: String,
    pub allow_actions: BTreeSet<String>,
    pub deny_actions: BTreeSet<String>,
    pub resource_patterns: Vec<String>,
}

impl Default for StsPolicy {
    fn default() -> Self {
        Self {
            name: String::new(),
            allow_actions: BTreeSet::new(),
            deny_actions: BTreeSet::new(),
            resource_patterns: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StsUser {
    pub username: String,
    pub secret_key: String,
    pub policies: BTreeSet<String>,
    pub groups: BTreeSet<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StsGroup {
    pub name: String,
    pub policies: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TempCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub parent: String,
    pub username: String,
    pub effective_policies: Vec<String>,
    pub revoked: bool,
    pub tags: BTreeMap<String, String>,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ServiceAccount {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: String,
    pub username: String,
    pub effective_policies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OpenIdProvider {
    pub name: String,
    pub claim_name: String,
    pub claim_userinfo: bool,
    pub role_policies: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct OpenIdClaims {
    pub subject: String,
    pub preferred_username: String,
    pub roles: BTreeSet<String>,
    pub custom: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LdapConfig {
    pub normalized_base_dn: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ExportedIam {
    pub users: Vec<StsUser>,
    pub groups: Vec<StsGroup>,
    pub policies: Vec<StsPolicy>,
    pub ldap_config: LdapConfig,
}
