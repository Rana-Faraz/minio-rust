use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SftpAuthError {
    Authentication,
    NoSuchUser,
    UserHasNoPolicies,
    LdapNotEnabled,
    PublicKeyBadFormat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SftpPermissions {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LdapUser {
    password: String,
    public_key: Option<String>,
    has_policies: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SftpAuthService {
    ldap_enabled: bool,
    internal_users: BTreeMap<String, String>,
    ldap_users: BTreeMap<String, LdapUser>,
}

impl SftpAuthService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_ldap_enabled(&mut self, enabled: bool) {
        self.ldap_enabled = enabled;
    }

    pub fn add_internal_user(&mut self, access_key: &str, secret_key: &str) {
        self.internal_users
            .insert(access_key.to_string(), secret_key.to_string());
    }

    pub fn add_ldap_user(
        &mut self,
        username: &str,
        password: &str,
        public_key: Option<&str>,
        has_policies: bool,
    ) -> Result<(), SftpAuthError> {
        let public_key = match public_key {
            Some(value) => Some(normalize_public_key(value)?),
            None => None,
        };
        self.ldap_users.insert(
            username.to_string(),
            LdapUser {
                password: password.to_string(),
                public_key,
                has_policies,
            },
        );
        Ok(())
    }

    pub fn ssh_password_auth(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SftpPermissions, SftpAuthError> {
        if let Some(user) = username.strip_suffix("=ldap") {
            if !self.ldap_enabled {
                return Err(SftpAuthError::LdapNotEnabled);
            }
            return self.process_ldap_password(user, password);
        }

        if let Some(user) = username.strip_suffix("=svc") {
            return self.process_internal_password(user, password);
        }

        if self.ldap_enabled {
            if let Ok(perms) = self.process_ldap_password(username, password) {
                return Ok(perms);
            }
        }

        self.process_internal_password(username, password)
    }

    pub fn ssh_pubkey_auth(
        &self,
        username: &str,
        public_key: &str,
    ) -> Result<SftpPermissions, SftpAuthError> {
        if let Some(user) = username.strip_suffix("=ldap") {
            if !self.ldap_enabled {
                return Err(SftpAuthError::LdapNotEnabled);
            }
            return self.process_ldap_pubkey(user, public_key);
        }

        if let Some(user) = username.strip_suffix("=svc") {
            return self.process_internal_pubkey(user);
        }

        if self.ldap_enabled {
            if let Ok(perms) = self.process_ldap_pubkey(username, public_key) {
                return Ok(perms);
            }
        }

        self.process_internal_pubkey(username)
    }

    fn process_internal_password(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SftpPermissions, SftpAuthError> {
        let Some(secret_key) = self.internal_users.get(username) else {
            return Err(SftpAuthError::NoSuchUser);
        };
        if secret_key != password {
            return Err(SftpAuthError::Authentication);
        }
        Ok(SftpPermissions {
            access_key: username.to_string(),
            secret_key: secret_key.clone(),
        })
    }

    fn process_internal_pubkey(&self, username: &str) -> Result<SftpPermissions, SftpAuthError> {
        if self.internal_users.contains_key(username) {
            Err(SftpAuthError::Authentication)
        } else {
            Err(SftpAuthError::NoSuchUser)
        }
    }

    fn process_ldap_password(
        &self,
        username: &str,
        password: &str,
    ) -> Result<SftpPermissions, SftpAuthError> {
        let Some(user) = self.ldap_users.get(username) else {
            return Err(SftpAuthError::NoSuchUser);
        };
        if user.password != password {
            return Err(SftpAuthError::Authentication);
        }
        if !user.has_policies {
            return Err(SftpAuthError::UserHasNoPolicies);
        }
        Ok(SftpPermissions {
            access_key: username.to_string(),
            secret_key: user.password.clone(),
        })
    }

    fn process_ldap_pubkey(
        &self,
        username: &str,
        public_key: &str,
    ) -> Result<SftpPermissions, SftpAuthError> {
        let Some(user) = self.ldap_users.get(username) else {
            return Err(SftpAuthError::NoSuchUser);
        };
        let expected = user
            .public_key
            .as_ref()
            .ok_or(SftpAuthError::Authentication)?;
        let provided = normalize_public_key(public_key)?;
        if expected != &provided {
            return Err(SftpAuthError::Authentication);
        }
        if !user.has_policies {
            return Err(SftpAuthError::UserHasNoPolicies);
        }
        Ok(SftpPermissions {
            access_key: username.to_string(),
            secret_key: user.password.clone(),
        })
    }
}

fn normalize_public_key(value: &str) -> Result<String, SftpAuthError> {
    let mut parts = value.split_whitespace();
    let key_type = parts.next().ok_or(SftpAuthError::PublicKeyBadFormat)?;
    let key_body = parts.next().ok_or(SftpAuthError::PublicKeyBadFormat)?;
    if !key_type.starts_with("ssh-") || key_body.is_empty() {
        return Err(SftpAuthError::PublicKeyBadFormat);
    }
    Ok(format!("{key_type} {key_body}"))
}
