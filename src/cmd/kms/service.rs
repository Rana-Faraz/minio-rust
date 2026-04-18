use std::collections::{BTreeMap, HashMap};

use crate::cmd::{
    action_ignores_resources, kms_policy_allows, kms_policy_denies, kms_resource_arn,
    pattern_matches_key, resource_allowed, KmsError, KmsKeyInfo, KmsPolicyStatement, KmsUser,
    STUB_CREATED_AT, STUB_CREATED_BY,
};

#[derive(Debug, Clone)]
struct AuthIdentity {
    is_root: bool,
    statements: Vec<KmsPolicyStatement>,
}

#[derive(Debug, Clone)]
pub struct KmsService {
    configured: bool,
    root_access_key: String,
    root_secret_key: String,
    keys: BTreeMap<String, KmsKeyInfo>,
    users: HashMap<String, KmsUser>,
}

impl KmsService {
    pub fn new(configured: bool, root_access_key: &str, root_secret_key: &str) -> Self {
        let mut service = Self {
            configured,
            root_access_key: root_access_key.to_string(),
            root_secret_key: root_secret_key.to_string(),
            keys: BTreeMap::new(),
            users: HashMap::new(),
        };
        if configured {
            service.insert_key("default-test-key");
        }
        service
    }

    pub fn add_user(&mut self, user: KmsUser) {
        self.users.insert(user.access_key.clone(), user);
    }

    pub fn create_key(
        &mut self,
        access_key: &str,
        secret_key: &str,
        key_id: &str,
        admin_api: bool,
    ) -> Result<(), KmsError> {
        let action = if admin_api {
            "admin:KMSCreateKey"
        } else {
            "kms:CreateKey"
        };
        let resource = kms_resource_arn(key_id);
        self.authorize(access_key, secret_key, action, Some(&resource))?;
        self.insert_key(key_id);
        Ok(())
    }

    pub fn key_status(
        &self,
        access_key: &str,
        secret_key: &str,
        key_id: &str,
        admin_api: bool,
    ) -> Result<KmsKeyInfo, KmsError> {
        let action = if admin_api {
            "admin:KMSKeyStatus"
        } else {
            "kms:KeyStatus"
        };
        let resource = kms_resource_arn(key_id);
        self.authorize(access_key, secret_key, action, Some(&resource))?;
        self.keys.get(key_id).cloned().ok_or(KmsError::KeyNotFound)
    }

    pub fn admin_status(&self, access_key: &str, secret_key: &str) -> Result<String, KmsError> {
        self.authorize(access_key, secret_key, "admin:KMSKeyStatus", None)?;
        Ok("MinIO builtin".to_string())
    }

    pub fn version(&self, access_key: &str, secret_key: &str) -> Result<String, KmsError> {
        self.authorize(access_key, secret_key, "kms:Version", None)?;
        Ok("version".to_string())
    }

    pub fn api_paths(&self, access_key: &str, secret_key: &str) -> Result<Vec<String>, KmsError> {
        self.authorize(access_key, secret_key, "kms:API", None)?;
        Ok(vec!["stub/path".to_string()])
    }

    pub fn metrics(&self, access_key: &str, secret_key: &str) -> Result<String, KmsError> {
        self.authorize(access_key, secret_key, "kms:Metrics", None)?;
        Ok("kms".to_string())
    }

    pub fn status(&self, access_key: &str, secret_key: &str) -> Result<String, KmsError> {
        self.authorize(access_key, secret_key, "kms:Status", None)?;
        Ok("MinIO builtin".to_string())
    }

    pub fn list_keys(
        &self,
        access_key: &str,
        secret_key: &str,
        pattern: &str,
    ) -> Result<Vec<KmsKeyInfo>, KmsError> {
        let identity = self.authenticate(access_key, secret_key)?;
        if identity.is_root {
            return Ok(self
                .keys
                .values()
                .filter(|key| pattern_matches_key(pattern, &key.name))
                .cloned()
                .collect());
        }
        if kms_policy_denies(&identity.statements, "kms:ListKeys") {
            return Err(KmsError::AccessDenied);
        }
        let may_list = identity.statements.iter().any(|statement| {
            matches!(statement.effect, crate::cmd::KmsEffect::Allow)
                && statement
                    .actions
                    .iter()
                    .any(|action| action == "kms:ListKeys" || action == "kms:*" || action == "*")
        });
        if !may_list {
            return Err(KmsError::AccessDenied);
        }

        let out = self
            .keys
            .values()
            .filter(|key| pattern_matches_key(pattern, &key.name))
            .filter(|key| {
                let resource = kms_resource_arn(&key.name);
                if action_ignores_resources("kms:ListKeys") {
                    true
                } else if identity
                    .statements
                    .iter()
                    .filter(|statement| {
                        statement
                            .actions
                            .iter()
                            .any(|action| action == "kms:ListKeys" || action == "kms:*")
                    })
                    .any(|statement| statement.resources.is_empty())
                {
                    true
                } else {
                    resource_allowed(&identity.statements, "kms:ListKeys", &resource)
                }
            })
            .cloned()
            .collect();
        Ok(out)
    }

    fn insert_key(&mut self, key_id: &str) {
        self.keys.insert(
            key_id.to_string(),
            KmsKeyInfo {
                name: key_id.to_string(),
                created_at: STUB_CREATED_AT.to_string(),
                created_by: STUB_CREATED_BY.to_string(),
            },
        );
    }

    fn authenticate(&self, access_key: &str, secret_key: &str) -> Result<AuthIdentity, KmsError> {
        if !self.configured {
            return Err(KmsError::NotConfigured);
        }
        if access_key == self.root_access_key && secret_key == self.root_secret_key {
            return Ok(AuthIdentity {
                is_root: true,
                statements: Vec::new(),
            });
        }
        let Some(user) = self.users.get(access_key) else {
            return Err(KmsError::AccessDenied);
        };
        if user.secret_key != secret_key {
            return Err(KmsError::AccessDenied);
        }
        Ok(AuthIdentity {
            is_root: false,
            statements: user.statements.clone(),
        })
    }

    fn authorize(
        &self,
        access_key: &str,
        secret_key: &str,
        action: &str,
        resource: Option<&str>,
    ) -> Result<(), KmsError> {
        let identity = self.authenticate(access_key, secret_key)?;
        if identity.is_root {
            return Ok(());
        }
        if kms_policy_denies(&identity.statements, action) {
            return Err(KmsError::AccessDenied);
        }
        if kms_policy_allows(&identity.statements, action, resource) {
            return Ok(());
        }
        Err(KmsError::AccessDenied)
    }
}
