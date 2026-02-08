use super::*;

impl CommandExecutor {
    pub(super) async fn expire(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'expire' command".to_string()),
                SessionAction::Continue,
            );
        }

        let Some(seconds) = parse_u64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        match self.store.expire(&args[1], seconds).await {
            Ok(v) => (
                RespValue::Integer(if v { 1 } else { 0 }),
                SessionAction::Continue,
            ),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn pexpire(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'pexpire' command".to_string()),
                SessionAction::Continue,
            );
        }

        let Some(milliseconds) = parse_u64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        match self.store.pexpire(&args[1], milliseconds).await {
            Ok(v) => (
                RespValue::Integer(if v { 1 } else { 0 }),
                SessionAction::Continue,
            ),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn pexpireat(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'pexpireat' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }

        let Some(ms_timestamp) = parse_u64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        match self.store.expire_at_ms(&args[1], ms_timestamp).await {
            Ok(v) => (
                RespValue::Integer(if v { 1 } else { 0 }),
                SessionAction::Continue,
            ),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn expireat(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'expireat' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }

        let Some(seconds_timestamp) = parse_u64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        match self.store.expire_at(&args[1], seconds_timestamp).await {
            Ok(v) => (
                RespValue::Integer(if v { 1 } else { 0 }),
                SessionAction::Continue,
            ),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn persist(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'persist' command".to_string()),
                SessionAction::Continue,
            );
        }

        match self.store.persist(&args[1]).await {
            Ok(v) => (
                RespValue::Integer(if v { 1 } else { 0 }),
                SessionAction::Continue,
            ),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn ttl(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'ttl' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Integer(self.store.ttl(&args[1]).await),
            SessionAction::Continue,
        )
    }

    pub(super) async fn pttl(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'pttl' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Integer(self.store.pttl(&args[1]).await),
            SessionAction::Continue,
        )
    }
}
