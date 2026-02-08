mod auth_compat;
mod expiry;
mod info;
mod keyspace;
mod strings;

#[cfg(test)]
mod tests;

use crate::auth::{Auth, SessionAuth};
use crate::protocol::RespValue;
use crate::stats::ServerStats;
use crate::store::Store;
use std::sync::Arc;

pub struct CommandExecutor {
    auth: Auth,
    store: Store,
    stats: Arc<ServerStats>,
    listen_addr: String,
}

pub enum SessionAction {
    Continue,
    Close,
}

impl CommandExecutor {
    pub fn new(auth: Auth, store: Store, stats: Arc<ServerStats>, listen_addr: String) -> Self {
        Self {
            auth,
            store,
            stats,
            listen_addr,
        }
    }

    pub async fn execute(
        &self,
        args: Vec<Vec<u8>>,
        session: &mut SessionAuth,
    ) -> (RespValue, SessionAction) {
        if args.is_empty() {
            return (
                RespValue::Error("ERR empty command".to_string()),
                SessionAction::Continue,
            );
        }

        let cmd = upper(&args[0]);
        self.stats.on_command(&cmd);
        if cmd != "AUTH"
            && cmd != "PING"
            && cmd != "QUIT"
            && cmd != "HELLO"
            && !session.is_authenticated(&self.auth)
        {
            return (
                RespValue::Error("NOAUTH Authentication required.".to_string()),
                SessionAction::Continue,
            );
        }

        if cmd != "AUTH"
            && cmd != "PING"
            && cmd != "QUIT"
            && cmd != "HELLO"
            && !self.auth.can_execute(session.user.as_deref(), &cmd)
        {
            return (
                RespValue::Error(format!(
                    "NOPERM this user has no permissions to run the '{}' command",
                    cmd.to_lowercase()
                )),
                SessionAction::Continue,
            );
        }

        match cmd.as_str() {
            "PING" => self.ping(&args),
            "ECHO" => self.echo(&args),
            "TIME" => self.time(&args),
            "AUTH" => self.auth_cmd(&args, session),
            "HELLO" => self.hello(&args, session),
            "CLIENT" => self.client(&args, session).await,
            "COMMAND" => self.command_meta(&args),
            "CONFIG" => self.config_cmd(&args),
            "LATENCY" => self.latency(&args),
            "SLOWLOG" => self.slowlog(&args),
            "BGREWRITEAOF" => self.bgrewriteaof(&args).await,
            "GET" => self.get(&args).await,
            "GETDEL" => self.getdel(&args).await,
            "GETEX" => self.getex(&args).await,
            "GETSET" => self.getset(&args).await,
            "MGET" => self.mget(&args).await,
            "GETRANGE" => self.getrange(&args).await,
            "SET" => self.set(&args).await,
            "SETRANGE" => self.setrange(&args).await,
            "SETNX" => self.setnx(&args).await,
            "SETEX" => self.setex(&args).await,
            "PSETEX" => self.psetex(&args).await,
            "UPDATE" => self.update(&args).await,
            "MSET" => self.mset(&args).await,
            "MSETNX" => self.msetnx(&args).await,
            "INCR" => self.incr(&args).await,
            "DECR" => self.decr(&args).await,
            "INCRBY" => self.incrby(&args).await,
            "DECRBY" => self.decrby(&args).await,
            "DEL" => self.del(&args).await,
            "UNLINK" => self.unlink(&args).await,
            "DBSIZE" => self.dbsize(&args).await,
            "KEYS" => self.keys(&args).await,
            "SCAN" => self.scan(&args).await,
            "TYPE" => self.key_type(&args).await,
            "EXISTS" => self.exists(&args).await,
            "EXPIRE" => self.expire(&args).await,
            "PEXPIRE" => self.pexpire(&args).await,
            "EXPIREAT" => self.expireat(&args).await,
            "PEXPIREAT" => self.pexpireat(&args).await,
            "PERSIST" => self.persist(&args).await,
            "TTL" => self.ttl(&args).await,
            "PTTL" => self.pttl(&args).await,
            "MEMORY" => self.memory(&args).await,
            "OBJECT" => self.object(&args).await,
            "INFO" => self.info(&args).await,
            "SELECT" => self.select(&args),
            "QUIT" => (RespValue::Simple("OK".to_string()), SessionAction::Close),
            "STRLEN" => self.strlen(&args).await,
            "APPEND" => self.append(&args).await,
            _ => (
                RespValue::Error(format!("ERR unknown command '{}'", cmd.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }
}

pub(super) fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse::<u64>().ok()
}

pub(super) fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

pub(super) fn upper(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_uppercase()
}

pub(super) fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) fn glob_match_ascii(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    let mut pi = 0_usize;
    let mut ti = 0_usize;
    let mut star_idx: Option<usize> = None;
    let mut match_idx = 0_usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
            continue;
        }

        if pi < p.len() && p[pi] == b'*' {
            star_idx = Some(pi);
            pi += 1;
            match_idx = ti;
            continue;
        }

        if let Some(star) = star_idx {
            pi = star + 1;
            match_idx += 1;
            ti = match_idx;
            continue;
        }

        return false;
    }

    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }

    pi == p.len()
}
