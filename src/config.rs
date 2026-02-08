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
    pub snapshot_path: Option<PathBuf>,
    pub snapshot_interval_sec: Option<u64>,
    pub max_connections: usize,
    pub max_request_bytes: usize,
    pub idle_timeout_sec: u64,
    pub max_memory_bytes: Option<u64>,
    pub metrics_addr: Option<String>,
    pub non_redis_mode: bool,
    pub debug_response_ids: bool,
}

impl Config {
    pub fn from_env_and_args() -> Result<Self, Box<dyn std::error::Error>> {
        let file_settings = if let Ok(path) = env::var("FEDIS_CONFIG") {
            parse_env_file(std::path::Path::new(&path))?
        } else {
            HashMap::new()
        };
        let setting = |key: &str| -> Option<String> {
            env::var(key)
                .ok()
                .or_else(|| file_settings.get(key).cloned())
        };

        let host = setting("FEDIS_HOST").unwrap_or_else(|| "127.0.0.1".to_string());
        let port = setting("FEDIS_PORT").unwrap_or_else(|| "6379".to_string());
        let mut listen_addr =
            setting("FEDIS_LISTEN").unwrap_or_else(|| format!("{}:{}", host, port));
        let mut users: HashMap<String, User> = HashMap::new();
        let mut default_user = setting("FEDIS_USERNAME").unwrap_or_else(|| "default".to_string());

        let data_path = setting("FEDIS_DATA_PATH").unwrap_or_else(|| ".".to_string());
        let mut aof_path = PathBuf::from(data_path).join("fedis.aof");

        if let Some(password) = setting("FEDIS_PASSWORD") {
            let enabled = setting("FEDIS_USER_ENABLED")
                .map(|v| parse_bool(v.as_str()))
                .unwrap_or(true);
            let permissions = setting("FEDIS_USER_COMMANDS")
                .map(|v| parse_permissions(Some(v.as_str())))
                .unwrap_or(Permissions::All);
            users.insert(
                default_user.clone(),
                User::new(password, enabled, permissions),
            );
        }

        if let Some(user_list) = setting("FEDIS_USERS") {
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

        if let Some(url) = setting("FEDIS_URL") {
            let parsed = Self::parse_redis_url(&url)?;
            listen_addr = parsed.0;
            if let Some((u, p, perms)) = parsed.1 {
                default_user = u.clone();
                users.insert(u, User::new(p, true, perms));
            }
        }

        if let Some(path) = setting("FEDIS_AOF_PATH") {
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

        let non_redis_mode = setting("FEDIS_NON_REDIS_MODE")
            .map(|v| parse_bool(v.as_str()))
            .unwrap_or(false);
        let debug_response_ids = setting("FEDIS_DEBUG_RESPONSE_ID")
            .map(|v| parse_bool(v.as_str()))
            .unwrap_or(false);
        let aof_fsync = parse_aof_fsync(setting("FEDIS_AOF_FSYNC").as_deref())?;
        let snapshot_path = setting("FEDIS_SNAPSHOT_PATH").map(PathBuf::from);
        let snapshot_interval_sec = setting("FEDIS_SNAPSHOT_INTERVAL_SEC")
            .as_deref()
            .map(parse_u64)
            .transpose()?;
        let max_connections = setting("FEDIS_MAX_CONNECTIONS")
            .as_deref()
            .map(parse_u64)
            .transpose()?
            .unwrap_or(1024) as usize;
        let max_request_bytes = setting("FEDIS_MAX_REQUEST_BYTES")
            .as_deref()
            .map(parse_u64)
            .transpose()?
            .unwrap_or(8 * 1024 * 1024) as usize;
        let idle_timeout_sec = setting("FEDIS_IDLE_TIMEOUT_SEC")
            .as_deref()
            .map(parse_u64)
            .transpose()?
            .unwrap_or(300);
        let max_memory_bytes = setting("FEDIS_MAXMEMORY_BYTES")
            .as_deref()
            .map(parse_u64)
            .transpose()?;
        let metrics_addr = setting("FEDIS_METRICS_ADDR");

        if let Some(path) = &snapshot_path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }

        Ok(Self {
            listen_addr,
            aof_path,
            users,
            default_user,
            aof_fsync,
            snapshot_path,
            snapshot_interval_sec,
            max_connections,
            max_request_bytes,
            idle_timeout_sec,
            max_memory_bytes,
            metrics_addr,
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

fn parse_env_file(
    path: &std::path::Path,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)?;
    let mut out = HashMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            out.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    Ok(out)
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

fn parse_u64(value: &str) -> Result<u64, Box<dyn std::error::Error>> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| "value must be an unsigned integer".into())
}
