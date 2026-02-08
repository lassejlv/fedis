use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub struct Auth {
    users: HashMap<String, User>,
    default_user: String,
}

#[derive(Clone)]
pub struct User {
    password: String,
    enabled: bool,
    permissions: Permissions,
}

#[derive(Clone)]
pub enum Permissions {
    All,
    Commands(HashSet<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    NoPasswordConfigured,
    InvalidCredentials,
}

impl Auth {
    pub fn new(users: HashMap<String, User>, default_user: String) -> Self {
        Self {
            users,
            default_user,
        }
    }

    pub fn requires_auth(&self) -> bool {
        self.users.values().any(|v| !v.password.is_empty())
    }

    pub fn authenticate(
        &self,
        username: Option<&str>,
        password: &str,
    ) -> Result<String, AuthError> {
        if !self.requires_auth() {
            return Err(AuthError::NoPasswordConfigured);
        }

        let user = username.unwrap_or(&self.default_user);
        let Some(entry) = self.users.get(user) else {
            return Err(AuthError::InvalidCredentials);
        };

        if !entry.enabled {
            return Err(AuthError::InvalidCredentials);
        }

        if entry.password == password {
            return Ok(user.to_string());
        }

        Err(AuthError::InvalidCredentials)
    }

    pub fn can_execute(&self, user: Option<&str>, command: &str) -> bool {
        if self.users.is_empty() {
            return true;
        }

        let subject = user.unwrap_or(&self.default_user);
        let Some(entry) = self.users.get(subject) else {
            return false;
        };

        if !entry.enabled {
            return false;
        }

        match &entry.permissions {
            Permissions::All => true,
            Permissions::Commands(commands) => commands.contains(command),
        }
    }
}

impl User {
    pub fn new(password: String, enabled: bool, permissions: Permissions) -> Self {
        Self {
            password,
            enabled,
            permissions,
        }
    }
}

#[derive(Default, Clone)]
pub struct SessionAuth {
    pub user: Option<String>,
    pub client_name: Option<String>,
}

impl SessionAuth {
    pub fn is_authenticated(&self, auth: &Auth) -> bool {
        !auth.requires_auth() || self.user.is_some()
    }
}
