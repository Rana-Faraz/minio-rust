use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::cmd::{
    expand_pattern, policy_allows, policy_denies, ExportedIam, LdapConfig, OpenIdClaims,
    OpenIdProvider, ServiceAccount, StsGroup, StsPolicy, StsUser, TempCredentials,
};

#[derive(Debug, Clone, Default)]
pub struct StsService {
    pub root_access_key: String,
    pub root_secret_key: String,
    policies: HashMap<String, StsPolicy>,
    users: HashMap<String, StsUser>,
    groups: HashMap<String, StsGroup>,
    openid: HashMap<String, OpenIdProvider>,
    sessions: HashMap<String, TempCredentials>,
    ldap_config: LdapConfig,
    next_id: u64,
}

impl StsService {
    fn action_granted_by(names: &BTreeSet<String>, requested_action: &str) -> bool {
        names.iter().any(|name| {
            if name == "*" {
                return true;
            }
            let parts = name.split('*').collect::<Vec<_>>();
            if parts.len() == 1 {
                return name == requested_action;
            }
            let mut pos = 0usize;
            let anchored_start = !name.starts_with('*');
            let anchored_end = !name.ends_with('*');
            for (idx, part) in parts.iter().enumerate() {
                if part.is_empty() {
                    continue;
                }
                if idx == 0 && anchored_start {
                    if !requested_action[pos..].starts_with(part) {
                        return false;
                    }
                    pos += part.len();
                    continue;
                }
                let Some(found) = requested_action[pos..].find(part) else {
                    return false;
                };
                pos += found + part.len();
            }
            if anchored_end {
                if let Some(last) = parts.iter().rev().find(|part| !part.is_empty()) {
                    requested_action.ends_with(last)
                } else {
                    true
                }
            } else {
                true
            }
        })
    }

    fn resource_pattern_within_base(
        requested_pattern: &str,
        base_pattern: &str,
        username: &str,
    ) -> bool {
        let requested = expand_pattern(requested_pattern, username);
        let base = expand_pattern(base_pattern, username);
        if base == "*" || requested == base {
            return true;
        }
        if !requested.contains('*') {
            return base == requested
                || (base.ends_with('*') && requested.starts_with(base.trim_end_matches('*')));
        }
        if base.ends_with('*') {
            let base_prefix = base.trim_end_matches('*');
            let requested_prefix = requested.trim_end_matches('*');
            return requested_prefix.starts_with(base_prefix);
        }
        false
    }

    pub fn new(root_access_key: &str, root_secret_key: &str) -> Self {
        Self {
            root_access_key: root_access_key.to_string(),
            root_secret_key: root_secret_key.to_string(),
            ..Default::default()
        }
    }

    fn next_credential_triplet(&mut self, prefix: &str) -> (String, String, String) {
        self.next_id += 1;
        (
            format!("{prefix}AK{:08}", self.next_id),
            format!("{prefix}SK{:08}", self.next_id),
            format!("{prefix}ST{:08}", self.next_id),
        )
    }

    pub fn add_policy(&mut self, policy: StsPolicy) {
        self.policies.insert(policy.name.clone(), policy);
    }

    pub fn add_group(&mut self, group: StsGroup) {
        self.groups.insert(group.name.clone(), group);
    }

    pub fn add_user(&mut self, user: StsUser) {
        self.users.insert(user.username.clone(), user);
    }

    pub fn add_openid_provider(&mut self, provider: OpenIdProvider) {
        self.openid.insert(provider.name.clone(), provider);
    }

    pub fn set_ldap_config(&mut self, cfg: LdapConfig) {
        self.ldap_config = cfg;
    }

    fn effective_policy_names_for_user(&self, username: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        if let Some(user) = self.users.get(username) {
            out.extend(user.policies.iter().cloned());
            for group in &user.groups {
                if let Some(g) = self.groups.get(group) {
                    out.extend(g.policies.iter().cloned());
                }
            }
        }
        out
    }

    fn issue_session(
        &mut self,
        parent: &str,
        username: &str,
        effective_policies: BTreeSet<String>,
        tags: BTreeMap<String, String>,
        source: &str,
    ) -> TempCredentials {
        let (access_key, secret_key, session_token) = self.next_credential_triplet("STS");
        let creds = TempCredentials {
            access_key: access_key.clone(),
            secret_key,
            session_token,
            parent: parent.to_string(),
            username: username.to_string(),
            effective_policies: effective_policies.into_iter().collect(),
            revoked: false,
            tags,
            source: source.to_string(),
        };
        self.sessions.insert(access_key, creds.clone());
        creds
    }

    pub fn assume_role_for_root(
        &mut self,
        access_key: &str,
        secret_key: &str,
        session_policy: Option<StsPolicy>,
    ) -> Result<TempCredentials, String> {
        if access_key != self.root_access_key || secret_key != self.root_secret_key {
            return Err("invalid root credentials".to_string());
        }
        let mut names = BTreeSet::new();
        if let Some(policy) = session_policy {
            self.add_policy(policy.clone());
            names.insert(policy.name);
        }
        Ok(self.issue_session("root", "root", names, BTreeMap::new(), "internal"))
    }

    pub fn assume_role_for_user(
        &mut self,
        username: &str,
        secret_key: &str,
        session_policy: Option<StsPolicy>,
        tags: BTreeMap<String, String>,
    ) -> Result<TempCredentials, String> {
        let user = self
            .users
            .get(username)
            .ok_or_else(|| "user not found".to_string())?;
        if !user.enabled {
            return Err("user disabled".to_string());
        }
        if user.secret_key != secret_key {
            return Err("invalid user credentials".to_string());
        }
        let mut effective = self.effective_policy_names_for_user(username);
        if let Some(policy) = session_policy {
            let base_policies = effective
                .iter()
                .filter_map(|name| self.policies.get(name))
                .cloned()
                .collect::<Vec<_>>();
            let base_actions = base_policies
                .iter()
                .flat_map(|policy| policy.allow_actions.iter().cloned())
                .collect::<BTreeSet<_>>();
            if !policy
                .allow_actions
                .iter()
                .all(|action| Self::action_granted_by(&base_actions, action))
            {
                return Err("session policy attempts privilege escalation".to_string());
            }
            let resources_scoped = policy.resource_patterns.iter().all(|requested_resource| {
                policy.allow_actions.iter().all(|requested_action| {
                    base_policies.iter().any(|base_policy| {
                        policy_allows(
                            base_policy,
                            username,
                            requested_action,
                            &expand_pattern(requested_resource, username),
                        ) || base_policy.resource_patterns.iter().any(|base_resource| {
                            Self::resource_pattern_within_base(
                                requested_resource,
                                base_resource,
                                username,
                            ) && Self::action_granted_by(
                                &base_policy.allow_actions,
                                requested_action,
                            )
                        })
                    })
                })
            });
            if !resources_scoped {
                return Err("session policy attempts privilege escalation".to_string());
            }
            self.add_policy(policy.clone());
            effective.insert(policy.name);
        }
        Ok(self.issue_session(username, username, effective, tags, "internal"))
    }

    pub fn create_service_account(
        &mut self,
        creds: &TempCredentials,
    ) -> Result<ServiceAccount, String> {
        let session = self
            .sessions
            .get(&creds.access_key)
            .ok_or_else(|| "session not found".to_string())?;
        let session = session.clone();
        if session.revoked {
            return Err("session revoked".to_string());
        }
        let (access_key, secret_key, session_token) = self.next_credential_triplet("SVC");
        let account = ServiceAccount {
            access_key,
            secret_key,
            session_token,
            username: session.username.clone(),
            effective_policies: session.effective_policies.clone(),
        };
        self.sessions.insert(
            account.access_key.clone(),
            TempCredentials {
                access_key: account.access_key.clone(),
                secret_key: account.secret_key.clone(),
                session_token: account.session_token.clone(),
                parent: session.username.clone(),
                username: session.username.clone(),
                effective_policies: session.effective_policies.clone(),
                revoked: false,
                tags: session.tags.clone(),
                source: "service-account".to_string(),
            },
        );
        Ok(account)
    }

    pub fn revoke_session(&mut self, access_key: &str) {
        if let Some(session) = self.sessions.get_mut(access_key) {
            session.revoked = true;
        }
    }

    pub fn export_iam(&self) -> ExportedIam {
        ExportedIam {
            users: self.users.values().cloned().collect(),
            groups: self.groups.values().cloned().collect(),
            policies: self.policies.values().cloned().collect(),
            ldap_config: self.ldap_config.clone(),
        }
    }

    pub fn import_iam(&mut self, exported: ExportedIam) {
        self.users = exported
            .users
            .into_iter()
            .map(|user| (user.username.clone(), user))
            .collect();
        self.groups = exported
            .groups
            .into_iter()
            .map(|group| (group.name.clone(), group))
            .collect();
        self.policies = exported
            .policies
            .into_iter()
            .map(|policy| (policy.name.clone(), policy))
            .collect();
        self.ldap_config = exported.ldap_config;
    }

    pub fn import_iam_assets(
        &mut self,
        policies: Vec<StsPolicy>,
        users: Vec<StsUser>,
        groups: Vec<StsGroup>,
    ) {
        for policy in policies {
            self.add_policy(policy);
        }
        for group in groups {
            self.add_group(group);
        }
        for user in users {
            self.add_user(user);
        }
    }

    pub fn assume_role_with_ldap(
        &mut self,
        username: &str,
        dn: &str,
        group_dns: &[String],
    ) -> Result<TempCredentials, String> {
        if self.ldap_config.normalized_base_dn && dn != dn.to_lowercase() {
            return Err("ldap dn not normalized".to_string());
        }
        let mut effective = BTreeSet::new();
        for group in group_dns {
            if let Some(mapped) = self.groups.get(group) {
                effective.extend(mapped.policies.iter().cloned());
            }
        }
        Ok(self.issue_session(dn, username, effective, BTreeMap::new(), "ldap"))
    }

    pub fn assume_role_with_openid(
        &mut self,
        provider_name: &str,
        claims: &OpenIdClaims,
        requested_role: Option<&str>,
    ) -> Result<TempCredentials, String> {
        let provider = self
            .openid
            .get(provider_name)
            .ok_or_else(|| "openid provider not found".to_string())?;

        let username = if provider.claim_userinfo {
            claims
                .custom
                .get(&provider.claim_name)
                .cloned()
                .unwrap_or_else(|| claims.preferred_username.clone())
        } else {
            claims.preferred_username.clone()
        };

        let mut effective = BTreeSet::new();
        match requested_role {
            Some(role) => {
                let mapped = provider
                    .role_policies
                    .get(role)
                    .ok_or_else(|| "role not mapped".to_string())?;
                effective.extend(mapped.iter().cloned());
            }
            None => {
                for role in &claims.roles {
                    if let Some(mapped) = provider.role_policies.get(role) {
                        effective.extend(mapped.iter().cloned());
                    }
                }
            }
        }
        if effective.is_empty() {
            return Err("no effective openid policy".to_string());
        }
        let mut tags = BTreeMap::new();
        tags.insert("sub".to_string(), claims.subject.clone());
        Ok(self.issue_session(&claims.subject, &username, effective, tags, "openid"))
    }

    pub fn allowed(&self, creds: &TempCredentials, action: &str, resource: &str) -> bool {
        let Some(session) = self.sessions.get(&creds.access_key) else {
            return false;
        };
        if session.revoked {
            return false;
        }

        let mut allowed = false;
        for policy_name in &session.effective_policies {
            if let Some(policy) = self.policies.get(policy_name) {
                if policy_denies(policy, &session.username, action, resource) {
                    return false;
                }
                if policy_allows(policy, &session.username, action, resource) {
                    allowed = true;
                }
            }
        }
        allowed
    }

    pub fn validate_openid_configs(&self) -> Result<(), String> {
        let mut claim_names = BTreeSet::new();
        for provider in self.openid.values() {
            if !claim_names.insert(provider.claim_name.clone()) {
                return Err("duplicate openid claim mapping".to_string());
            }
        }
        Ok(())
    }
}
