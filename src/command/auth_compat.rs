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
        if args.len() > 1 {
            let Some(proto) = parse_i64(&args[1]) else {
                return (
                    RespValue::Error(
                        "ERR Protocol version is not an integer or out of range".to_string(),
                    ),
                    SessionAction::Continue,
                );
            };
            if proto != 2 && proto != 3 {
                return (
                    RespValue::Error("NOPROTO unsupported protocol version".to_string()),
                    SessionAction::Continue,
                );
            }
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

        (
            RespValue::Array(vec![
                RespValue::Bulk(Some(b"server".to_vec())),
                RespValue::Bulk(Some(b"redis".to_vec())),
                RespValue::Bulk(Some(b"version".to_vec())),
                RespValue::Bulk(Some(b"7.2.0-fedis".to_vec())),
                RespValue::Bulk(Some(b"proto".to_vec())),
                RespValue::Integer(2),
                RespValue::Bulk(Some(b"id".to_vec())),
                RespValue::Integer(0),
                RespValue::Bulk(Some(b"mode".to_vec())),
                RespValue::Bulk(Some(b"standalone".to_vec())),
                RespValue::Bulk(Some(b"role".to_vec())),
                RespValue::Bulk(Some(b"master".to_vec())),
                RespValue::Bulk(Some(b"modules".to_vec())),
                RespValue::Array(Vec::new()),
            ]),
            SessionAction::Continue,
        )
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
            "TRACKING" => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) fn command_meta(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() == 1 {
            return (RespValue::Array(Vec::new()), SessionAction::Continue);
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "COUNT" => (RespValue::Integer(0), SessionAction::Continue),
            "INFO" => {
                let mut out = Vec::new();
                for _ in args.iter().skip(2) {
                    out.push(RespValue::Bulk(None));
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
