use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountStatus {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminUserInfo {
    pub status: AccountStatus,
}

#[derive(Debug, Clone, Default)]
struct ParsedPolicy {
    allow_make_bucket: bool,
    allow_list_bucket: bool,
    allow_put_object: bool,
    allow_get_object: bool,
    allow_admin_create_service_account: bool,
}

impl ParsedPolicy {
    fn full_s3() -> Self {
        Self {
            allow_make_bucket: true,
            allow_list_bucket: true,
            allow_put_object: true,
            allow_get_object: true,
            allow_admin_create_service_account: false,
        }
    }

    fn console_admin() -> Self {
        let mut policy = Self::full_s3();
        policy.allow_admin_create_service_account = true;
        policy
    }

    fn intersect(&self, other: &Self) -> Self {
        Self {
            allow_make_bucket: self.allow_make_bucket && other.allow_make_bucket,
            allow_list_bucket: self.allow_list_bucket && other.allow_list_bucket,
            allow_put_object: self.allow_put_object && other.allow_put_object,
            allow_get_object: self.allow_get_object && other.allow_get_object,
            allow_admin_create_service_account: self.allow_admin_create_service_account
                && other.allow_admin_create_service_account,
        }
    }
}

#[derive(Debug, Clone)]
struct IamUser {
    secret_key: String,
    status: AccountStatus,
    attached_policies: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct ServiceAccount {
    access_key: String,
    secret_key: String,
    target_user: String,
    status: AccountStatus,
    session_policy: Option<ParsedPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Identity {
    Root,
    User(String),
    ServiceAccount(String),
}

static NEXT_ACCOUNT_ID: AtomicUsize = AtomicUsize::new(1);

fn next_id(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        NEXT_ACCOUNT_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn parse_policy_json(input: &[u8]) -> Result<ParsedPolicy, String> {
    let value: Value = serde_json::from_slice(input).map_err(|err| err.to_string())?;
    let mut parsed = ParsedPolicy::default();
    let statements = value
        .get("Statement")
        .and_then(Value::as_array)
        .ok_or_else(|| "missing Statement".to_string())?;
    for statement in statements {
        if statement
            .get("Effect")
            .and_then(Value::as_str)
            .unwrap_or_default()
            != "Allow"
        {
            continue;
        }
        let Some(actions) = statement.get("Action") else {
            continue;
        };
        let mut mark_action = |action: &str| match action {
            "s3:*" => {
                parsed.allow_make_bucket = true;
                parsed.allow_list_bucket = true;
                parsed.allow_put_object = true;
                parsed.allow_get_object = true;
            }
            "s3:CreateBucket" => parsed.allow_make_bucket = true,
            "s3:ListBucket" => parsed.allow_list_bucket = true,
            "s3:PutObject" | "s3:Put*" => parsed.allow_put_object = true,
            "s3:GetObject" => parsed.allow_get_object = true,
            "admin:CreateServiceAccount" => parsed.allow_admin_create_service_account = true,
            _ => {}
        };

        if let Some(list) = actions.as_array() {
            for action in list.iter().filter_map(Value::as_str) {
                mark_action(action);
            }
        } else if let Some(action) = actions.as_str() {
            mark_action(action);
        }
    }
    Ok(parsed)
}

#[derive(Debug, Clone)]
pub struct ServiceAccountCreds {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone)]
pub struct AdminUsers {
    root_access_key: String,
    root_secret_key: String,
    plugin_mode: bool,
    users: BTreeMap<String, IamUser>,
    policies: BTreeMap<String, ParsedPolicy>,
    service_accounts: BTreeMap<String, ServiceAccount>,
    buckets: BTreeSet<String>,
}

impl AdminUsers {
    pub fn new(plugin_mode: bool) -> Self {
        let mut policies = BTreeMap::new();
        policies.insert("readwrite".to_string(), ParsedPolicy::full_s3());
        policies.insert("consoleAdmin".to_string(), ParsedPolicy::console_admin());
        policies.insert(
            "readonly".to_string(),
            ParsedPolicy {
                allow_list_bucket: true,
                allow_get_object: true,
                ..ParsedPolicy::default()
            },
        );
        Self {
            root_access_key: "root".to_string(),
            root_secret_key: "rootsecret".to_string(),
            plugin_mode,
            users: BTreeMap::new(),
            policies,
            service_accounts: BTreeMap::new(),
            buckets: BTreeSet::new(),
        }
    }

    fn authenticate(&self, access_key: &str, secret_key: &str) -> Option<Identity> {
        if access_key == self.root_access_key && secret_key == self.root_secret_key {
            return Some(Identity::Root);
        }
        if let Some(user) = self.users.get(access_key) {
            if user.secret_key == secret_key && user.status == AccountStatus::Enabled {
                return Some(Identity::User(access_key.to_string()));
            }
        }
        if let Some(account) = self.service_accounts.get(access_key) {
            if account.secret_key == secret_key && account.status == AccountStatus::Enabled {
                return Some(Identity::ServiceAccount(access_key.to_string()));
            }
        }
        None
    }

    fn user_policy(&self, username: &str) -> ParsedPolicy {
        let mut policy = ParsedPolicy::default();
        if let Some(user) = self.users.get(username) {
            for name in &user.attached_policies {
                if let Some(attached) = self.policies.get(name) {
                    policy.allow_make_bucket |= attached.allow_make_bucket;
                    policy.allow_list_bucket |= attached.allow_list_bucket;
                    policy.allow_put_object |= attached.allow_put_object;
                    policy.allow_get_object |= attached.allow_get_object;
                    policy.allow_admin_create_service_account |=
                        attached.allow_admin_create_service_account;
                }
            }
        }
        policy
    }

    fn effective_policy(&self, identity: &Identity) -> ParsedPolicy {
        match identity {
            Identity::Root => ParsedPolicy::console_admin(),
            Identity::User(username) => {
                if self.plugin_mode {
                    ParsedPolicy {
                        allow_make_bucket: true,
                        allow_list_bucket: true,
                        allow_put_object: false,
                        allow_get_object: true,
                        allow_admin_create_service_account: true,
                    }
                } else {
                    self.user_policy(username)
                }
            }
            Identity::ServiceAccount(access_key) => {
                let account = self
                    .service_accounts
                    .get(access_key)
                    .expect("service account exists");
                if self.plugin_mode {
                    ParsedPolicy {
                        allow_make_bucket: true,
                        allow_list_bucket: true,
                        allow_put_object: false,
                        allow_get_object: true,
                        allow_admin_create_service_account: false,
                    }
                } else {
                    let inherited = self.user_policy(&account.target_user);
                    match &account.session_policy {
                        Some(session) => inherited.intersect(session),
                        None => inherited,
                    }
                }
            }
        }
    }

    pub fn root_access_key(&self) -> &str {
        &self.root_access_key
    }

    pub fn root_secret_key(&self) -> &str {
        &self.root_secret_key
    }

    pub fn add_root_bucket(&mut self, bucket: &str) {
        self.buckets.insert(bucket.to_string());
    }

    pub fn set_user(
        &mut self,
        access_key: &str,
        secret_key: &str,
        status: AccountStatus,
    ) -> Result<(), String> {
        let attached_policies = self
            .users
            .get(access_key)
            .map(|user| user.attached_policies.clone())
            .unwrap_or_default();
        self.users.insert(
            access_key.to_string(),
            IamUser {
                secret_key: secret_key.to_string(),
                status,
                attached_policies,
            },
        );
        Ok(())
    }

    pub fn list_users(&self) -> BTreeMap<String, AdminUserInfo> {
        self.users
            .iter()
            .map(|(access_key, user)| {
                (
                    access_key.clone(),
                    AdminUserInfo {
                        status: user.status,
                    },
                )
            })
            .collect()
    }

    pub fn set_user_status(
        &mut self,
        access_key: &str,
        status: AccountStatus,
    ) -> Result<(), String> {
        let Some(user) = self.users.get_mut(access_key) else {
            return Err("no such user".to_string());
        };
        user.status = status;
        Ok(())
    }

    pub fn remove_user(&mut self, access_key: &str) -> Result<(), String> {
        self.users.remove(access_key);
        self.service_accounts
            .retain(|_, account| account.target_user != access_key);
        Ok(())
    }

    pub fn add_canned_policy(&mut self, name: &str, bytes: &[u8]) -> Result<(), String> {
        let policy = parse_policy_json(bytes)?;
        self.policies.insert(name.to_string(), policy);
        Ok(())
    }

    pub fn list_canned_policies(&self) -> BTreeSet<String> {
        self.policies.keys().cloned().collect()
    }

    pub fn remove_canned_policy(&mut self, name: &str) -> Result<(), String> {
        if self
            .users
            .values()
            .any(|user| user.attached_policies.contains(name))
        {
            return Err("policy attached".to_string());
        }
        self.policies.remove(name);
        Ok(())
    }

    pub fn attach_policy(&mut self, username: &str, policy_name: &str) -> Result<(), String> {
        let Some(user) = self.users.get_mut(username) else {
            return Err("no such user".to_string());
        };
        if !self.policies.contains_key(policy_name) {
            return Err("no such policy".to_string());
        }
        user.attached_policies.insert(policy_name.to_string());
        Ok(())
    }

    pub fn detach_policy(&mut self, username: &str, policy_name: &str) -> Result<(), String> {
        let Some(user) = self.users.get_mut(username) else {
            return Err("no such user".to_string());
        };
        user.attached_policies.remove(policy_name);
        Ok(())
    }

    pub fn make_bucket(
        &mut self,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
    ) -> Result<(), String> {
        let identity = self
            .authenticate(access_key, secret_key)
            .ok_or_else(|| "Access Denied.".to_string())?;
        let policy = self.effective_policy(&identity);
        if !policy.allow_make_bucket {
            return Err("Access Denied.".to_string());
        }
        self.buckets.insert(bucket.to_string());
        Ok(())
    }

    pub fn list_buckets(&self, access_key: &str, secret_key: &str) -> Result<Vec<String>, String> {
        let identity = self
            .authenticate(access_key, secret_key)
            .ok_or_else(|| "Access Denied.".to_string())?;
        let policy = self.effective_policy(&identity);
        if !policy.allow_list_bucket {
            return Err("Access Denied.".to_string());
        }
        Ok(self.buckets.iter().cloned().collect())
    }

    pub fn put_object(
        &self,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        _object: &str,
    ) -> Result<(), String> {
        let identity = self
            .authenticate(access_key, secret_key)
            .ok_or_else(|| "Access Denied.".to_string())?;
        let policy = self.effective_policy(&identity);
        if !self.buckets.contains(bucket) || !policy.allow_put_object {
            return Err("Access Denied.".to_string());
        }
        Ok(())
    }

    pub fn add_service_account(
        &mut self,
        actor_access: &str,
        actor_secret: &str,
        target_user: &str,
        session_policy: Option<&[u8]>,
        access_key: Option<&str>,
        secret_key: Option<&str>,
    ) -> Result<ServiceAccountCreds, String> {
        let identity = self
            .authenticate(actor_access, actor_secret)
            .ok_or_else(|| "Access Denied.".to_string())?;
        if matches!(identity, Identity::ServiceAccount(_)) {
            return Err("Access Denied.".to_string());
        }

        if !self.plugin_mode {
            let policy = self.effective_policy(&identity);
            if !policy.allow_admin_create_service_account {
                return Err("Access Denied.".to_string());
            }
            if let Identity::User(username) = &identity {
                if target_user != username {
                    return Err("Access Denied.".to_string());
                }
            }
        }

        if target_user != self.root_access_key && !self.users.contains_key(target_user) {
            return Err("no such user".to_string());
        }

        let account_access = access_key
            .map(str::to_string)
            .unwrap_or_else(|| next_id("svc"));
        let account_secret = secret_key
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}-secret", next_id("svc")));
        let parsed_session = match session_policy {
            Some(bytes) if !self.plugin_mode => Some(parse_policy_json(bytes)?),
            _ => None,
        };
        self.service_accounts.insert(
            account_access.clone(),
            ServiceAccount {
                access_key: account_access.clone(),
                secret_key: account_secret.clone(),
                target_user: target_user.to_string(),
                status: AccountStatus::Enabled,
                session_policy: parsed_session,
            },
        );
        Ok(ServiceAccountCreds {
            access_key: account_access,
            secret_key: account_secret,
        })
    }

    pub fn list_service_accounts(&self, target_user: &str) -> Vec<String> {
        self.service_accounts
            .values()
            .filter(|account| account.target_user == target_user)
            .map(|account| account.access_key.clone())
            .collect()
    }

    pub fn service_account_status(&self, access_key: &str) -> Option<AccountStatus> {
        self.service_accounts
            .get(access_key)
            .map(|account| account.status)
    }

    pub fn update_service_account(
        &mut self,
        access_key: &str,
        new_secret_key: Option<&str>,
        status: Option<AccountStatus>,
    ) -> Result<(), String> {
        let Some(account) = self.service_accounts.get_mut(access_key) else {
            return Err("no such service account".to_string());
        };
        if let Some(secret) = new_secret_key {
            account.secret_key = secret.to_string();
        }
        if let Some(status) = status {
            account.status = status;
        }
        Ok(())
    }

    pub fn delete_service_account(&mut self, access_key: &str) -> Result<(), String> {
        self.service_accounts.remove(access_key);
        Ok(())
    }
}
