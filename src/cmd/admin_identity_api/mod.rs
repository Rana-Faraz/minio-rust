use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum IdentityAccountStatus {
    #[default]
    Enabled,
    Disabled,
}
impl_msg_codec!(IdentityAccountStatus);

impl From<AccountStatus> for IdentityAccountStatus {
    fn from(value: AccountStatus) -> Self {
        match value {
            AccountStatus::Enabled => Self::Enabled,
            AccountStatus::Disabled => Self::Disabled,
        }
    }
}

impl From<IdentityAccountStatus> for AccountStatus {
    fn from(value: IdentityAccountStatus) -> Self {
        match value {
            IdentityAccountStatus::Enabled => Self::Enabled,
            IdentityAccountStatus::Disabled => Self::Disabled,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdminIdentityPolicyRecord {
    pub name: String,
    pub document_json: Vec<u8>,
}
impl_msg_codec!(AdminIdentityPolicyRecord);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdminIdentityUserRecord {
    pub access_key: String,
    pub secret_key: String,
    pub status: IdentityAccountStatus,
    pub attached_policies: BTreeSet<String>,
}
impl_msg_codec!(AdminIdentityUserRecord);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdminIdentityServiceAccountRecord {
    pub access_key: String,
    pub secret_key: String,
    pub target_user: String,
    pub status: IdentityAccountStatus,
    pub session_policy_json: Option<Vec<u8>>,
}
impl_msg_codec!(AdminIdentityServiceAccountRecord);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdminIdentitySnapshot {
    pub users: Vec<AdminIdentityUserRecord>,
    pub policies: Vec<AdminIdentityPolicyRecord>,
    pub service_accounts: Vec<AdminIdentityServiceAccountRecord>,
    pub sts_iam: ExportedIam,
    pub openid_providers: Vec<OpenIdProvider>,
}
impl_msg_codec!(AdminIdentitySnapshot);

#[derive(Debug, Clone)]
pub struct AdminIdentityApi {
    admin_users: AdminUsers,
    sts: StsService,
    users: BTreeMap<String, AdminIdentityUserRecord>,
    policies: BTreeMap<String, AdminIdentityPolicyRecord>,
    service_accounts: BTreeMap<String, AdminIdentityServiceAccountRecord>,
    openid_providers: BTreeMap<String, OpenIdProvider>,
}

impl Default for AdminIdentityApi {
    fn default() -> Self {
        Self::new(false)
    }
}

impl AdminIdentityApi {
    pub fn new(plugin_mode: bool) -> Self {
        let admin_users = AdminUsers::new(plugin_mode);
        let sts = StsService::new(admin_users.root_access_key(), admin_users.root_secret_key());
        Self {
            admin_users,
            sts,
            users: BTreeMap::new(),
            policies: BTreeMap::new(),
            service_accounts: BTreeMap::new(),
            openid_providers: BTreeMap::new(),
        }
    }

    pub fn root_access_key(&self) -> &str {
        self.admin_users.root_access_key()
    }

    pub fn root_secret_key(&self) -> &str {
        self.admin_users.root_secret_key()
    }

    pub fn set_user(
        &mut self,
        access_key: &str,
        secret_key: &str,
        status: IdentityAccountStatus,
    ) -> Result<(), String> {
        self.admin_users
            .set_user(access_key, secret_key, status.into())?;
        let existing_policies = self
            .users
            .get(access_key)
            .map(|record| record.attached_policies.clone())
            .unwrap_or_default();
        self.users.insert(
            access_key.to_string(),
            AdminIdentityUserRecord {
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                status,
                attached_policies: existing_policies,
            },
        );
        Ok(())
    }

    pub fn list_users(&self) -> BTreeMap<String, AdminIdentityUserRecord> {
        self.users.clone()
    }

    pub fn set_user_status(
        &mut self,
        access_key: &str,
        status: IdentityAccountStatus,
    ) -> Result<(), String> {
        self.admin_users
            .set_user_status(access_key, status.into())?;
        let Some(record) = self.users.get_mut(access_key) else {
            return Err("no such user".to_string());
        };
        record.status = status;
        Ok(())
    }

    pub fn remove_user(&mut self, access_key: &str) -> Result<(), String> {
        self.admin_users.remove_user(access_key)?;
        self.users.remove(access_key);
        self.service_accounts
            .retain(|_, account| account.target_user != access_key);
        Ok(())
    }

    pub fn add_canned_policy(&mut self, name: &str, document_json: &[u8]) -> Result<(), String> {
        self.admin_users.add_canned_policy(name, document_json)?;
        self.policies.insert(
            name.to_string(),
            AdminIdentityPolicyRecord {
                name: name.to_string(),
                document_json: document_json.to_vec(),
            },
        );
        Ok(())
    }

    pub fn list_canned_policies(&self) -> Vec<AdminIdentityPolicyRecord> {
        self.policies.values().cloned().collect()
    }

    pub fn remove_canned_policy(&mut self, name: &str) -> Result<(), String> {
        self.admin_users.remove_canned_policy(name)?;
        self.policies.remove(name);
        for user in self.users.values_mut() {
            user.attached_policies.remove(name);
        }
        Ok(())
    }

    pub fn attach_policy(&mut self, access_key: &str, policy_name: &str) -> Result<(), String> {
        self.admin_users.attach_policy(access_key, policy_name)?;
        let Some(user) = self.users.get_mut(access_key) else {
            return Err("no such user".to_string());
        };
        user.attached_policies.insert(policy_name.to_string());
        Ok(())
    }

    pub fn detach_policy(&mut self, access_key: &str, policy_name: &str) -> Result<(), String> {
        self.admin_users.detach_policy(access_key, policy_name)?;
        let Some(user) = self.users.get_mut(access_key) else {
            return Err("no such user".to_string());
        };
        user.attached_policies.remove(policy_name);
        Ok(())
    }

    pub fn add_service_account(
        &mut self,
        actor_access: &str,
        actor_secret: &str,
        target_user: &str,
        session_policy_json: Option<&[u8]>,
        access_key: Option<&str>,
        secret_key: Option<&str>,
    ) -> Result<AdminIdentityServiceAccountRecord, String> {
        let creds = self.admin_users.add_service_account(
            actor_access,
            actor_secret,
            target_user,
            session_policy_json,
            access_key,
            secret_key,
        )?;
        let record = AdminIdentityServiceAccountRecord {
            access_key: creds.access_key.clone(),
            secret_key: creds.secret_key.clone(),
            target_user: target_user.to_string(),
            status: self
                .admin_users
                .service_account_status(&creds.access_key)
                .unwrap_or(AccountStatus::Enabled)
                .into(),
            session_policy_json: session_policy_json.map(|bytes| bytes.to_vec()),
        };
        self.service_accounts
            .insert(record.access_key.clone(), record.clone());
        Ok(record)
    }

    pub fn list_service_accounts(
        &self,
        target_user: Option<&str>,
    ) -> Vec<AdminIdentityServiceAccountRecord> {
        self.service_accounts
            .values()
            .filter(|record| target_user.is_none_or(|target| record.target_user == target))
            .cloned()
            .collect()
    }

    pub fn update_service_account(
        &mut self,
        access_key: &str,
        new_secret_key: Option<&str>,
        status: Option<IdentityAccountStatus>,
    ) -> Result<(), String> {
        self.admin_users.update_service_account(
            access_key,
            new_secret_key,
            status.map(AccountStatus::from),
        )?;
        let Some(record) = self.service_accounts.get_mut(access_key) else {
            return Err("no such service account".to_string());
        };
        if let Some(secret_key) = new_secret_key {
            record.secret_key = secret_key.to_string();
        }
        if let Some(status) = status {
            record.status = status;
        }
        Ok(())
    }

    pub fn delete_service_account(&mut self, access_key: &str) -> Result<(), String> {
        self.admin_users.delete_service_account(access_key)?;
        self.service_accounts.remove(access_key);
        Ok(())
    }

    pub fn add_sts_policy(&mut self, policy: StsPolicy) {
        self.sts.add_policy(policy);
    }

    pub fn add_sts_group(&mut self, group: StsGroup) {
        self.sts.add_group(group);
    }

    pub fn add_sts_user(&mut self, user: StsUser) {
        self.sts.add_user(user);
    }

    pub fn add_openid_provider(&mut self, provider: OpenIdProvider) {
        self.openid_providers
            .insert(provider.name.clone(), provider.clone());
        self.sts.add_openid_provider(provider);
    }

    pub fn set_ldap_config(&mut self, config: LdapConfig) {
        self.sts.set_ldap_config(config);
    }

    pub fn assume_role_for_root(
        &mut self,
        access_key: &str,
        secret_key: &str,
        session_policy: Option<StsPolicy>,
    ) -> Result<TempCredentials, String> {
        self.sts
            .assume_role_for_root(access_key, secret_key, session_policy)
    }

    pub fn assume_role_for_user(
        &mut self,
        username: &str,
        secret_key: &str,
        session_policy: Option<StsPolicy>,
        tags: BTreeMap<String, String>,
    ) -> Result<TempCredentials, String> {
        self.sts
            .assume_role_for_user(username, secret_key, session_policy, tags)
    }

    pub fn assume_role_with_openid(
        &mut self,
        provider_name: &str,
        claims: &OpenIdClaims,
        requested_role: Option<&str>,
    ) -> Result<TempCredentials, String> {
        self.sts
            .assume_role_with_openid(provider_name, claims, requested_role)
    }

    pub fn assume_role_with_ldap(
        &mut self,
        username: &str,
        dn: &str,
        group_dns: &[String],
    ) -> Result<TempCredentials, String> {
        self.sts.assume_role_with_ldap(username, dn, group_dns)
    }

    pub fn create_session_service_account(
        &mut self,
        creds: &TempCredentials,
    ) -> Result<ServiceAccount, String> {
        self.sts.create_service_account(creds)
    }

    pub fn revoke_session(&mut self, access_key: &str) {
        self.sts.revoke_session(access_key);
    }

    pub fn sts_allowed(&self, creds: &TempCredentials, action: &str, resource: &str) -> bool {
        self.sts.allowed(creds, action, resource)
    }

    pub fn validate_openid_configs(&self) -> Result<(), String> {
        self.sts.validate_openid_configs()
    }

    pub fn export_snapshot(&self) -> AdminIdentitySnapshot {
        AdminIdentitySnapshot {
            users: self.users.values().cloned().collect(),
            policies: self.policies.values().cloned().collect(),
            service_accounts: self.service_accounts.values().cloned().collect(),
            sts_iam: self.sts.export_iam(),
            openid_providers: self.openid_providers.values().cloned().collect(),
        }
    }

    pub fn import_snapshot(&mut self, snapshot: AdminIdentitySnapshot) -> Result<(), String> {
        let plugin_mode = self.admin_users.list_users().is_empty()
            && self.users.is_empty()
            && self.service_accounts.is_empty()
            && self.policies.is_empty()
            && self.openid_providers.is_empty();
        let mut rebuilt = Self::new(plugin_mode);

        for policy in &snapshot.policies {
            rebuilt.add_canned_policy(&policy.name, &policy.document_json)?;
        }
        for user in &snapshot.users {
            rebuilt.set_user(&user.access_key, &user.secret_key, user.status)?;
        }
        for user in &snapshot.users {
            for policy in &user.attached_policies {
                rebuilt.attach_policy(&user.access_key, policy)?;
            }
        }
        for service_account in &snapshot.service_accounts {
            let root_access_key = rebuilt.root_access_key().to_string();
            let root_secret_key = rebuilt.root_secret_key().to_string();
            rebuilt.add_service_account(
                &root_access_key,
                &root_secret_key,
                &service_account.target_user,
                service_account.session_policy_json.as_deref(),
                Some(&service_account.access_key),
                Some(&service_account.secret_key),
            )?;
            if service_account.status != IdentityAccountStatus::Enabled {
                rebuilt.update_service_account(
                    &service_account.access_key,
                    None,
                    Some(service_account.status),
                )?;
            }
        }

        rebuilt.sts.import_iam(snapshot.sts_iam);
        for provider in snapshot.openid_providers {
            rebuilt.add_openid_provider(provider);
        }
        *self = rebuilt;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../../tests/cmd/admin_identity_api_test.rs"]
mod admin_identity_api_test;
