use std::collections::{HashMap, HashSet};
use std::env;
use std::path::PathBuf;
use url::Url;

use crate::auth::{Permissions, User};
use crate::persistence::AofFsync;

#[derive(Clone)]
pub struct Config {
    pub listen_addr: String,
    pub aof_path: PathBuf,
    pub users: HashMap<String, User>,
    pub default_user: String,
    pub aof_fsync: AofFsync,
    pub non_redis_mode: bool,
    pub debug_response_ids: bool,
}

impl Config {
    pub fn from_env_and_args() -> Result<Self, Box<dyn std::error::Error>> {
        let host = env::var("FEDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        let port = env::var("FEDIS_PORT").unwrap_or_else(|_| "6379".to_string());
        let mut listen_addr =
            env::var("FEDIS_LISTEN").unwrap_or_else(|_| format!("{}:{}", host, port));
        let mut users: HashMap<String, User> = HashMap::new();
        let mut default_user = env::var("FEDIS_USERNAME").unwrap_or_else(|_| "default".to_string());

        let data_path = env::var("FEDIS_DATA_PATH").unwrap_or_else(|_| ".".to_string());
        let mut aof_path = PathBuf::from(data_path).join("fedis.aof");

        if let Ok(password) = env::var("FEDIS_PASSWORD") {
            let enabled = env::var("FEDIS_USER_ENABLED")
                .map(|v| parse_bool(&v))
                .unwrap_or(true);
            let permissions = env::var("FEDIS_USER_COMMANDS")
                .ok()
                .map(|v| parse_permissions(Some(v.as_str())))
                .unwrap_or(Permissions::All);
            users.insert(
                default_user.clone(),
                User::new(password, enabled, permissions),
            );
        }

        if let Ok(user_list) = env::var("FEDIS_USERS") {
            for pair in user_list
                .split(',')
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
            {
                if let Some((user, definition)) = pair.split_once(':') {
                    let user = user.trim().to_string();
                    let mut chunks = definition.split(':').map(|v| v.trim());
                    let password = chunks.next().unwrap_or_default().to_string();
                    let next = chunks.next();
                    let (enabled, permissions) = if let Some(token) = next {
                        if is_bool_token(token) {
                            (parse_bool(token), parse_permissions(chunks.next()))
                        } else {
                            (true, parse_permissions(Some(token)))
                        }
                    } else {
                        (true, Permissions::All)
                    };
                    users.insert(user, User::new(password, enabled, permissions));
                }
            }
        }

        let args: Vec<String> = env::args().skip(1).collect();
        if let Some(first) = args.first() {
            if first.starts_with("redis://") {
                let parsed = Self::parse_redis_url(first)?;
                listen_addr = parsed.0;
                if let Some((u, p, perms)) = parsed.1 {
                    default_user = u.clone();
                    users.insert(u, User::new(p, true, perms));
                }
            } else {
                listen_addr = first.clone();
            }
        }

        if let Ok(url) = env::var("FEDIS_URL") {
            let parsed = Self::parse_redis_url(&url)?;
            listen_addr = parsed.0;
            if let Some((u, p, perms)) = parsed.1 {
                default_user = u.clone();
                users.insert(u, User::new(p, true, perms));
            }
        }

        if let Ok(path) = env::var("FEDIS_AOF_PATH") {
            aof_path = PathBuf::from(path);
        }

        if let Some(parent) = aof_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if !users.contains_key(&default_user) && !users.is_empty() {
            let first = users.keys().next().cloned();
            if let Some(first) = first {
                default_user = first;
            }
        }

        let non_redis_mode = env::var("FEDIS_NON_REDIS_MODE")
            .map(|v| parse_bool(&v))
            .unwrap_or(false);
        let debug_response_ids = env::var("FEDIS_DEBUG_RESPONSE_ID")
            .map(|v| parse_bool(&v))
            .unwrap_or(false);
        let aof_fsync = parse_aof_fsync(env::var("FEDIS_AOF_FSYNC").ok().as_deref())?;

        Ok(Self {
            listen_addr,
            aof_path,
            users,
            default_user,
            aof_fsync,
            non_redis_mode,
            debug_response_ids,
        })
    }

    fn parse_redis_url(
        input: &str,
    ) -> Result<(String, Option<(String, String, Permissions)>), Box<dyn std::error::Error>> {
        let url = Url::parse(input)?;
        if url.scheme() != "redis" {
            return Err("URL scheme must be redis://".into());
        }

        let host = url.host_str().ok_or("redis:// URL requires host")?;
        let port = url.port().unwrap_or(6379);
        let listen_addr = format!("{}:{}", host, port);

        let db_path = url.path().trim();
        if !db_path.is_empty() && db_path != "/" && db_path != "/0" {
            return Err("only database 0 is supported".into());
        }

        let username = if url.username().is_empty() {
            None
        } else {
            Some(url.username().to_string())
        };
        let password = url.password().map(ToString::to_string);

        let auth = match (username, password) {
            (Some(u), Some(p)) => Some((u, p, Permissions::All)),
            (None, Some(p)) => Some(("default".to_string(), p, Permissions::All)),
            _ => None,
        };

        Ok((listen_addr, auth))
    }
}

fn parse_permissions(raw: Option<&str>) -> Permissions {
    let Some(raw) = raw else {
        return Permissions::All;
    };
    if raw.eq_ignore_ascii_case("all") || raw == "*" {
        return Permissions::All;
    }

    let commands: HashSet<String> = raw
        .split('|')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.trim_start_matches('+').to_uppercase())
        .collect();

    if commands.is_empty() {
        Permissions::All
    } else {
        Permissions::Commands(commands)
    }
}

fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn is_bool_token(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "0" | "false" | "no" | "off"
    )
}

fn parse_aof_fsync(value: Option<&str>) -> Result<AofFsync, Box<dyn std::error::Error>> {
    match value
        .unwrap_or("everysec")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "always" => Ok(AofFsync::Always),
        "everysec" => Ok(AofFsync::EverySec),
        "no" => Ok(AofFsync::No),
        _ => Err("FEDIS_AOF_FSYNC must be one of: always, everysec, no".into()),
    }
}
