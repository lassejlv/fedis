use super::*;
use crate::auth::AuthError;

impl CommandExecutor {
    pub(super) fn ping(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() > 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'ping' command".to_string()),
                SessionAction::Continue,
            );
        }
        if args.len() == 2 {
            return (
                RespValue::Bulk(Some(args[1].clone())),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Simple("PONG".to_string()),
            SessionAction::Continue,
        )
    }

    pub(super) fn echo(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'echo' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Bulk(Some(args[1].clone())),
            SessionAction::Continue,
        )
    }

    pub(super) fn time(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 1 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'time' command".to_string()),
                SessionAction::Continue,
            );
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        (
            RespValue::Array(vec![
                RespValue::Bulk(Some(now.as_secs().to_string().into_bytes())),
                RespValue::Bulk(Some(now.subsec_micros().to_string().into_bytes())),
            ]),
            SessionAction::Continue,
        )
    }

    pub(super) fn hello(
        &self,
        args: &[Vec<u8>],
        session: &mut SessionAuth,
    ) -> (RespValue, SessionAction) {
        let mut proto = 2_i64;
        if args.len() > 1 {
            let Some(parsed) = parse_i64(&args[1]) else {
                return (
                    RespValue::Error(
                        "ERR Protocol version is not an integer or out of range".to_string(),
                    ),
                    SessionAction::Continue,
                );
            };
            if parsed != 2 && parsed != 3 {
                return (
                    RespValue::Error("NOPROTO unsupported protocol version".to_string()),
                    SessionAction::Continue,
                );
            }
            proto = parsed;
        }

        let mut idx = 2;
        while idx < args.len() {
            let token = upper(&args[idx]);
            match token.as_str() {
                "AUTH" => {
                    if idx + 2 >= args.len() {
                        return (
                            RespValue::Error("ERR Syntax error in HELLO option AUTH".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let user = String::from_utf8_lossy(&args[idx + 1]);
                    let pass = String::from_utf8_lossy(&args[idx + 2]);
                    match self.auth.authenticate(Some(&user), &pass) {
                        Ok(u) => session.user = Some(u),
                        Err(AuthError::NoPasswordConfigured) => {
                            return (
                                RespValue::Error(
                                    "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"
                                        .to_string(),
                                ),
                                SessionAction::Continue,
                            )
                        }
                        Err(AuthError::InvalidCredentials) => {
                            return (
                                RespValue::Error(
                                    "WRONGPASS invalid username-password pair or user is disabled"
                                        .to_string(),
                                ),
                                SessionAction::Continue,
                            )
                        }
                    }
                    idx += 3;
                }
                "SETNAME" => {
                    if idx + 1 >= args.len() {
                        return (
                            RespValue::Error(
                                "ERR Syntax error in HELLO option SETNAME".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    }
                    session.client_name = Some(String::from_utf8_lossy(&args[idx + 1]).to_string());
                    idx += 2;
                }
                _ => {
                    return (
                        RespValue::Error("ERR Syntax error in HELLO option".to_string()),
                        SessionAction::Continue,
                    );
                }
            }
        }

        let fields = vec![
            (
                RespValue::Bulk(Some(b"server".to_vec())),
                RespValue::Bulk(Some(b"redis".to_vec())),
            ),
            (
                RespValue::Bulk(Some(b"version".to_vec())),
                RespValue::Bulk(Some(b"7.2.0-fedis".to_vec())),
            ),
            (
                RespValue::Bulk(Some(b"proto".to_vec())),
                RespValue::Integer(proto),
            ),
            (RespValue::Bulk(Some(b"id".to_vec())), RespValue::Integer(0)),
            (
                RespValue::Bulk(Some(b"mode".to_vec())),
                RespValue::Bulk(Some(b"standalone".to_vec())),
            ),
            (
                RespValue::Bulk(Some(b"role".to_vec())),
                RespValue::Bulk(Some(b"master".to_vec())),
            ),
            (
                RespValue::Bulk(Some(b"modules".to_vec())),
                RespValue::Array(Vec::new()),
            ),
        ];

        if proto == 3 {
            (RespValue::Map(fields), SessionAction::Continue)
        } else {
            let mut flat = Vec::with_capacity(fields.len() * 2);
            for (k, v) in fields {
                flat.push(k);
                flat.push(v);
            }
            (RespValue::Array(flat), SessionAction::Continue)
        }
    }

    pub(super) async fn client(
        &self,
        args: &[Vec<u8>],
        session: &mut SessionAuth,
    ) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'client' command".to_string()),
                SessionAction::Continue,
            );
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "SETINFO" => {
                if args.len() != 4 {
                    return (
                        RespValue::Error(
                            "ERR wrong number of arguments for 'client|setinfo' command"
                                .to_string(),
                        ),
                        SessionAction::Continue,
                    );
                }
                (RespValue::Simple("OK".to_string()), SessionAction::Continue)
            }
            "SETNAME" => {
                if args.len() != 3 {
                    return (
                        RespValue::Error(
                            "ERR wrong number of arguments for 'client|setname' command"
                                .to_string(),
                        ),
                        SessionAction::Continue,
                    );
                }
                session.client_name = Some(String::from_utf8_lossy(&args[2]).to_string());
                (RespValue::Simple("OK".to_string()), SessionAction::Continue)
            }
            "GETNAME" => {
                if args.len() != 2 {
                    return (
                        RespValue::Error(
                            "ERR wrong number of arguments for 'client|getname' command"
                                .to_string(),
                        ),
                        SessionAction::Continue,
                    );
                }
                (
                    RespValue::Bulk(session.client_name.clone().map(|v| v.into_bytes())),
                    SessionAction::Continue,
                )
            }
            "ID" => (RespValue::Integer(0), SessionAction::Continue),
            "GETREDIR" => (RespValue::Integer(-1), SessionAction::Continue),
            "LIST" => (
                RespValue::Bulk(Some(b"id=0 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default redir=-1 resp=2".to_vec())),
                SessionAction::Continue,
            ),
            "INFO" => (
                RespValue::Bulk(Some(
                    format!(
                        "id=0 addr=127.0.0.1:0 laddr=127.0.0.1:0 fd=0 name={} age=0 idle=0 flags=N db=0 sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user={} redir=-1 resp=2",
                        session.client_name.as_deref().unwrap_or(""),
                        session.user.as_deref().unwrap_or("default")
                    )
                    .into_bytes(),
                )),
                SessionAction::Continue,
            ),
            "PAUSE" | "UNPAUSE" => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            "TRACKING" | "CACHING" | "NO-EVICT" => {
                (RespValue::Simple("OK".to_string()), SessionAction::Continue)
            }
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn acl(
        &self,
        args: &[Vec<u8>],
        session: &SessionAuth,
    ) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'acl' command".to_string()),
                SessionAction::Continue,
            );
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "WHOAMI" => (
                RespValue::Bulk(Some(
                    session
                        .user
                        .clone()
                        .unwrap_or_else(|| self.auth.default_user().to_string())
                        .into_bytes(),
                )),
                SessionAction::Continue,
            ),
            "LIST" => {
                let users = self
                    .auth
                    .list_users()
                    .into_iter()
                    .map(|u| RespValue::Bulk(Some(format!("user {} on", u).into_bytes())))
                    .collect();
                (RespValue::Array(users), SessionAction::Continue)
            }
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn module_cmd(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'module' command".to_string()),
                SessionAction::Continue,
            );
        }
        let sub = upper(&args[1]);
        match sub.as_str() {
            "LIST" => (RespValue::Array(Vec::new()), SessionAction::Continue),
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn command_meta(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        let table = command_table();
        if args.len() == 1 {
            let payload = table
                .iter()
                .map(command_meta_entry)
                .collect::<Vec<RespValue>>();
            return (RespValue::Array(payload), SessionAction::Continue);
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "COUNT" => (
                RespValue::Integer(table.len() as i64),
                SessionAction::Continue,
            ),
            "INFO" => {
                let mut out = Vec::new();
                for name in args.iter().skip(2) {
                    let needle = String::from_utf8_lossy(name).to_ascii_uppercase();
                    if let Some(spec) = table.iter().find(|spec| spec.name == needle) {
                        out.push(command_meta_entry(spec));
                    } else {
                        out.push(RespValue::Bulk(None));
                    }
                }
                (RespValue::Array(out), SessionAction::Continue)
            }
            "DOCS" => (RespValue::Array(Vec::new()), SessionAction::Continue),
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn config_cmd(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'config' command".to_string()),
                SessionAction::Continue,
            );
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "GET" => {
                if args.len() != 3 {
                    return (
                        RespValue::Error(
                            "ERR wrong number of arguments for 'config|get' command".to_string(),
                        ),
                        SessionAction::Continue,
                    );
                }

                let pattern = String::from_utf8_lossy(&args[2]).to_ascii_lowercase();
                let mut pairs: Vec<(String, String)> = Vec::new();
                if glob_match_ascii(&pattern, "databases") {
                    pairs.push(("databases".to_string(), "1".to_string()));
                }
                if glob_match_ascii(&pattern, "appendonly") {
                    pairs.push(("appendonly".to_string(), "yes".to_string()));
                }
                if glob_match_ascii(&pattern, "timeout") {
                    pairs.push(("timeout".to_string(), "0".to_string()));
                }
                if glob_match_ascii(&pattern, "maxmemory") {
                    pairs.push(("maxmemory".to_string(), "0".to_string()));
                }

                let mut out = Vec::new();
                for (k, v) in pairs {
                    out.push(RespValue::Bulk(Some(k.into_bytes())));
                    out.push(RespValue::Bulk(Some(v.into_bytes())));
                }
                (RespValue::Array(out), SessionAction::Continue)
            }
            "SET" => (
                RespValue::Error("ERR CONFIG SET is disabled in fedis".to_string()),
                SessionAction::Continue,
            ),
            "RESETSTAT" => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn latency(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'latency' command".to_string()),
                SessionAction::Continue,
            );
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "LATEST" | "DOCTOR" | "HISTOGRAM" | "GRAPH" | "HELP" => {
                (RespValue::Array(Vec::new()), SessionAction::Continue)
            }
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn slowlog(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'slowlog' command".to_string()),
                SessionAction::Continue,
            );
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "GET" => (RespValue::Array(Vec::new()), SessionAction::Continue),
            "LEN" => (RespValue::Integer(0), SessionAction::Continue),
            "RESET" => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn bgrewriteaof(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 1 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'bgrewriteaof' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }

        if self.store.bgrewriteaof().await {
            (
                RespValue::Simple("Background append only file rewriting started".to_string()),
                SessionAction::Continue,
            )
        } else {
            (
                RespValue::Error(
                    "ERR Background append only file rewriting already in progress".to_string(),
                ),
                SessionAction::Continue,
            )
        }
    }

    pub(super) async fn bgsave(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 1 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'bgsave' command".to_string()),
                SessionAction::Continue,
            );
        }

        if self.store.bgsave().await {
            (
                RespValue::Simple("Background saving started".to_string()),
                SessionAction::Continue,
            )
        } else {
            (
                RespValue::Error(
                    "ERR Background save already in progress or snapshots disabled".to_string(),
                ),
                SessionAction::Continue,
            )
        }
    }

    pub(super) async fn save(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 1 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'save' command".to_string()),
                SessionAction::Continue,
            );
        }

        match self.store.save_snapshot_now().await {
            Ok(()) => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            Err(_) => (
                RespValue::Error("ERR snapshots are not configured".to_string()),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn lastsave(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 1 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'lastsave' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }

        let metrics = self.store.persistence_metrics();
        let ts = if metrics.last_snapshot_epoch_sec > 0 {
            metrics.last_snapshot_epoch_sec as i64
        } else {
            (now_ms() / 1000) as i64
        };
        (RespValue::Integer(ts), SessionAction::Continue)
    }

    pub(super) fn auth_cmd(
        &self,
        args: &[Vec<u8>],
        session: &mut SessionAuth,
    ) -> (RespValue, SessionAction) {
        let result = match args.len() {
            2 => {
                let pwd = String::from_utf8_lossy(&args[1]);
                self.auth.authenticate(None, &pwd)
            }
            3 => {
                let user = String::from_utf8_lossy(&args[1]);
                let pwd = String::from_utf8_lossy(&args[2]);
                self.auth.authenticate(Some(&user), &pwd)
            }
            _ => {
                return (
                    RespValue::Error(
                        "ERR wrong number of arguments for 'auth' command".to_string(),
                    ),
                    SessionAction::Continue,
                );
            }
        };

        match result {
            Ok(user) => {
                session.user = Some(user);
                (RespValue::Simple("OK".to_string()), SessionAction::Continue)
            }
            Err(AuthError::NoPasswordConfigured) => (
                RespValue::Error(
                    "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"
                        .to_string(),
                ),
                SessionAction::Continue,
            ),
            Err(AuthError::InvalidCredentials) => (
                RespValue::Error(
                    "WRONGPASS invalid username-password pair or user is disabled".to_string(),
                ),
                SessionAction::Continue,
            ),
        }
    }
}

#[derive(Clone, Copy)]
struct CommandSpec {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
}

fn command_meta_entry(spec: &CommandSpec) -> RespValue {
    RespValue::Array(vec![
        RespValue::Bulk(Some(spec.name.to_ascii_lowercase().into_bytes())),
        RespValue::Integer(spec.arity),
        RespValue::Array(
            spec.flags
                .iter()
                .map(|v| RespValue::Bulk(Some(v.as_bytes().to_vec())))
                .collect(),
        ),
        RespValue::Integer(spec.first_key),
        RespValue::Integer(spec.last_key),
        RespValue::Integer(spec.step),
    ])
}

fn command_table() -> &'static [CommandSpec] {
    &[
        CommandSpec {
            name: "APPEND",
            arity: 3,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "ACL",
            arity: -2,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "AUTH",
            arity: -2,
            flags: &["fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "BGSAVE",
            arity: 1,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "BGREWRITEAOF",
            arity: 1,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "CLIENT",
            arity: -2,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "COMMAND",
            arity: -1,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "CONFIG",
            arity: -2,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "DBSIZE",
            arity: 1,
            flags: &["readonly", "fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "DECR",
            arity: 2,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "DECRBY",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "DEL",
            arity: -2,
            flags: &["write"],
            first_key: 1,
            last_key: -1,
            step: 1,
        },
        CommandSpec {
            name: "ECHO",
            arity: 2,
            flags: &["fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "EXISTS",
            arity: -2,
            flags: &["readonly", "fast"],
            first_key: 1,
            last_key: -1,
            step: 1,
        },
        CommandSpec {
            name: "EXPIRE",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "EXPIREAT",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "GET",
            arity: 2,
            flags: &["readonly", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "GETDEL",
            arity: 2,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "GETEX",
            arity: -2,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "GETRANGE",
            arity: 4,
            flags: &["readonly"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "GETSET",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "HELLO",
            arity: -1,
            flags: &["fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "INCR",
            arity: 2,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "INCRBY",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "INFO",
            arity: -1,
            flags: &["readonly"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "JSON.DEL",
            arity: -2,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "JSON.GET",
            arity: -2,
            flags: &["readonly"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "JSON.SET",
            arity: 4,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "JSON.TYPE",
            arity: -2,
            flags: &["readonly"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "KEYS",
            arity: 2,
            flags: &["readonly"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "LATENCY",
            arity: -2,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "LASTSAVE",
            arity: 1,
            flags: &["readonly"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "MEMORY",
            arity: -2,
            flags: &["readonly"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "MGET",
            arity: -2,
            flags: &["readonly"],
            first_key: 1,
            last_key: -1,
            step: 1,
        },
        CommandSpec {
            name: "MSET",
            arity: -3,
            flags: &["write"],
            first_key: 1,
            last_key: -1,
            step: 2,
        },
        CommandSpec {
            name: "MSETNX",
            arity: -3,
            flags: &["write"],
            first_key: 1,
            last_key: -1,
            step: 2,
        },
        CommandSpec {
            name: "MODULE",
            arity: -2,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "OBJECT",
            arity: -3,
            flags: &["readonly"],
            first_key: 2,
            last_key: 2,
            step: 1,
        },
        CommandSpec {
            name: "PERSIST",
            arity: 2,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "PEXPIRE",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "PEXPIREAT",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "PING",
            arity: -1,
            flags: &["fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "PSETEX",
            arity: 4,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "PTTL",
            arity: 2,
            flags: &["readonly", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "QUIT",
            arity: 1,
            flags: &["fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "SCAN",
            arity: -2,
            flags: &["readonly"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "SAVE",
            arity: 1,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "SELECT",
            arity: 2,
            flags: &["fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "SET",
            arity: -3,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "SETEX",
            arity: 4,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "SETNX",
            arity: 3,
            flags: &["write", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "SETRANGE",
            arity: 4,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "SLOWLOG",
            arity: -2,
            flags: &["admin"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "STRLEN",
            arity: 2,
            flags: &["readonly", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "TIME",
            arity: 1,
            flags: &["fast"],
            first_key: 0,
            last_key: 0,
            step: 0,
        },
        CommandSpec {
            name: "TTL",
            arity: 2,
            flags: &["readonly", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "TYPE",
            arity: 2,
            flags: &["readonly", "fast"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
        CommandSpec {
            name: "UNLINK",
            arity: -2,
            flags: &["write"],
            first_key: 1,
            last_key: -1,
            step: 1,
        },
        CommandSpec {
            name: "UPDATE",
            arity: -3,
            flags: &["write"],
            first_key: 1,
            last_key: 1,
            step: 1,
        },
    ]
}
