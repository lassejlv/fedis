use super::*;
use crate::store::{GetExMode, IncrByError, SetCondition};

impl CommandExecutor {
    pub(super) async fn get(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'get' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Bulk(self.store.get(&args[1]).await),
            SessionAction::Continue,
        )
    }

    pub(super) async fn getset(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'getset' command".to_string()),
                SessionAction::Continue,
            );
        }
        match self.store.getset(args[1].clone(), args[2].clone()).await {
            Ok(v) => (RespValue::Bulk(v), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn getdel(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'getdel' command".to_string()),
                SessionAction::Continue,
            );
        }
        match self.store.getdel(&args[1]).await {
            Ok(v) => (RespValue::Bulk(v), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn getex(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 || args.len() > 4 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'getex' command".to_string()),
                SessionAction::Continue,
            );
        }

        let mode = if args.len() == 2 {
            GetExMode::None
        } else {
            let token = upper(&args[2]);
            match token.as_str() {
                "EX" => {
                    if args.len() != 4 {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let Some(seconds) = parse_u64(&args[3]) else {
                        return (
                            RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    };
                    GetExMode::Ex(seconds)
                }
                "PX" => {
                    if args.len() != 4 {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let Some(milliseconds) = parse_u64(&args[3]) else {
                        return (
                            RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    };
                    GetExMode::Px(milliseconds)
                }
                "PERSIST" => {
                    if args.len() != 3 {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    GetExMode::Persist
                }
                _ => {
                    return (
                        RespValue::Error("ERR syntax error".to_string()),
                        SessionAction::Continue,
                    );
                }
            }
        };

        match self.store.getex(&args[1], mode).await {
            Ok(v) => (RespValue::Bulk(v), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn mget(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'mget' command".to_string()),
                SessionAction::Continue,
            );
        }

        let mut values = Vec::with_capacity(args.len() - 1);
        for key in &args[1..] {
            values.push(RespValue::Bulk(self.store.get(key).await));
        }

        (RespValue::Array(values), SessionAction::Continue)
    }

    pub(super) async fn getrange(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 4 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'getrange' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }

        let Some(start) = parse_i64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };
        let Some(end) = parse_i64(&args[3]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        (
            RespValue::Bulk(Some(self.store.getrange(&args[1], start, end).await)),
            SessionAction::Continue,
        )
    }

    pub(super) async fn set(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'set' command".to_string()),
                SessionAction::Continue,
            );
        }

        let mut expires_at = None;
        let mut saw_ex = false;
        let mut saw_px = false;
        let mut saw_nx = false;
        let mut saw_xx = false;
        let mut condition = SetCondition::None;
        let mut idx = 3;
        while idx < args.len() {
            let token = upper(&args[idx]);
            match token.as_str() {
                "EX" => {
                    if saw_ex || saw_px {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    if idx + 1 >= args.len() {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let Some(secs) = parse_u64(&args[idx + 1]) else {
                        return (
                            RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    };
                    saw_ex = true;
                    expires_at = Some(now_ms().saturating_add(secs.saturating_mul(1000)));
                    idx += 2;
                }
                "PX" => {
                    if saw_px || saw_ex {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    if idx + 1 >= args.len() {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let Some(ms) = parse_u64(&args[idx + 1]) else {
                        return (
                            RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    };
                    saw_px = true;
                    expires_at = Some(now_ms().saturating_add(ms));
                    idx += 2;
                }
                "NX" => {
                    if saw_nx || saw_xx {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    saw_nx = true;
                    condition = SetCondition::Nx;
                    idx += 1;
                }
                "XX" => {
                    if saw_xx || saw_nx {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    saw_xx = true;
                    condition = SetCondition::Xx;
                    idx += 1;
                }
                _ => {
                    return (
                        RespValue::Error("ERR syntax error".to_string()),
                        SessionAction::Continue,
                    );
                }
            }
        }

        match self
            .store
            .set(args[1].clone(), args[2].clone(), expires_at, condition)
            .await
        {
            Ok(true) => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            Ok(false) => (RespValue::Bulk(None), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn setrange(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 4 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'setrange' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }

        let Some(offset) = parse_u64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        match self
            .store
            .setrange(&args[1], offset as usize, &args[3])
            .await
        {
            Ok(v) => (RespValue::Integer(v), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn setnx(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'setnx' command".to_string()),
                SessionAction::Continue,
            );
        }

        match self
            .store
            .set(args[1].clone(), args[2].clone(), None, SetCondition::Nx)
            .await
        {
            Ok(true) => (RespValue::Integer(1), SessionAction::Continue),
            Ok(false) => (RespValue::Integer(0), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn setex(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 4 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'setex' command".to_string()),
                SessionAction::Continue,
            );
        }

        let Some(seconds) = parse_u64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        let expires_at = Some(now_ms().saturating_add(seconds.saturating_mul(1000)));
        match self
            .store
            .set(
                args[1].clone(),
                args[3].clone(),
                expires_at,
                SetCondition::None,
            )
            .await
        {
            Ok(_) => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn psetex(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 4 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'psetex' command".to_string()),
                SessionAction::Continue,
            );
        }

        let Some(milliseconds) = parse_u64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        let expires_at = Some(now_ms().saturating_add(milliseconds));
        match self
            .store
            .set(
                args[1].clone(),
                args[3].clone(),
                expires_at,
                SetCondition::None,
            )
            .await
        {
            Ok(_) => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn update(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'update' command".to_string()),
                SessionAction::Continue,
            );
        }

        let mut expires_at = None;
        let mut saw_ex = false;
        let mut saw_px = false;
        let mut idx = 3;
        while idx < args.len() {
            let token = upper(&args[idx]);
            match token.as_str() {
                "EX" => {
                    if saw_ex || saw_px {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    if idx + 1 >= args.len() {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let Some(secs) = parse_u64(&args[idx + 1]) else {
                        return (
                            RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    };
                    saw_ex = true;
                    expires_at = Some(now_ms().saturating_add(secs.saturating_mul(1000)));
                    idx += 2;
                }
                "PX" => {
                    if saw_px || saw_ex {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    if idx + 1 >= args.len() {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let Some(ms) = parse_u64(&args[idx + 1]) else {
                        return (
                            RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    };
                    saw_px = true;
                    expires_at = Some(now_ms().saturating_add(ms));
                    idx += 2;
                }
                _ => {
                    return (
                        RespValue::Error("ERR syntax error".to_string()),
                        SessionAction::Continue,
                    );
                }
            }
        }

        match self
            .store
            .set(
                args[1].clone(),
                args[2].clone(),
                expires_at,
                SetCondition::Xx,
            )
            .await
        {
            Ok(true) => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            Ok(false) => (RespValue::Bulk(None), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn mset(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 3 || args.len() % 2 == 0 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'mset' command".to_string()),
                SessionAction::Continue,
            );
        }

        let mut idx = 1;
        while idx < args.len() {
            let key = args[idx].clone();
            let value = args[idx + 1].clone();
            if let Err(e) = self.store.set(key, value, None, SetCondition::None).await {
                return (
                    RespValue::Error(format!("ERR internal: {}", e)),
                    SessionAction::Continue,
                );
            }
            idx += 2;
        }

        (RespValue::Simple("OK".to_string()), SessionAction::Continue)
    }

    pub(super) async fn msetnx(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 3 || args.len() % 2 == 0 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'msetnx' command".to_string()),
                SessionAction::Continue,
            );
        }

        let mut pairs = Vec::new();
        let mut idx = 1;
        while idx < args.len() {
            pairs.push((args[idx].clone(), args[idx + 1].clone()));
            idx += 2;
        }

        match self.store.msetnx(&pairs).await {
            Ok(true) => (RespValue::Integer(1), SessionAction::Continue),
            Ok(false) => (RespValue::Integer(0), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn incr(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        self.incrby_impl(args, 1, "incr").await
    }

    pub(super) async fn decr(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        self.incrby_impl(args, -1, "decr").await
    }

    pub(super) async fn incrby(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'incrby' command".to_string()),
                SessionAction::Continue,
            );
        }
        let Some(by) = parse_i64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };
        self.incrby_impl(args, by, "incrby").await
    }

    pub(super) async fn decrby(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'decrby' command".to_string()),
                SessionAction::Continue,
            );
        }
        let Some(by) = parse_i64(&args[2]) else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };
        let Some(negated) = by.checked_neg() else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };
        self.incrby_impl(args, negated, "decrby").await
    }

    async fn incrby_impl(
        &self,
        args: &[Vec<u8>],
        by: i64,
        cmd: &str,
    ) -> (RespValue, SessionAction) {
        if args.len() < 2 || args.len() > 3 {
            return (
                RespValue::Error(format!(
                    "ERR wrong number of arguments for '{}' command",
                    cmd
                )),
                SessionAction::Continue,
            );
        }

        match self.store.incr_by(&args[1], by).await {
            Ok(v) => (RespValue::Integer(v), SessionAction::Continue),
            Err(IncrByError::NotInteger | IncrByError::OutOfRange) => (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            ),
            Err(IncrByError::Internal) => (
                RespValue::Error("ERR internal persistence failure".to_string()),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn memory(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'memory' command".to_string()),
                SessionAction::Continue,
            );
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "USAGE" => {
                if args.len() < 3 {
                    return (
                        RespValue::Error(
                            "ERR wrong number of arguments for 'memory|usage' command".to_string(),
                        ),
                        SessionAction::Continue,
                    );
                }
                (
                    RespValue::Bulk(
                        self.store
                            .memory_usage(&args[2])
                            .await
                            .map(|v| v.to_string().into_bytes()),
                    ),
                    SessionAction::Continue,
                )
            }
            "STATS" => (
                RespValue::Array(vec![
                    RespValue::Bulk(Some(b"peak.allocated".to_vec())),
                    RespValue::Integer(0),
                    RespValue::Bulk(Some(b"total.allocated".to_vec())),
                    RespValue::Integer(0),
                ]),
                SessionAction::Continue,
            ),
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn object(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'object' command".to_string()),
                SessionAction::Continue,
            );
        }

        let sub = upper(&args[1]);
        match sub.as_str() {
            "ENCODING" => (
                RespValue::Bulk(
                    self.store
                        .object_encoding(&args[2])
                        .await
                        .map(|v| v.as_bytes().to_vec()),
                ),
                SessionAction::Continue,
            ),
            "IDLETIME" | "FREQ" | "REFCOUNT" => {
                let exists = self.store.key_type(&args[2]).await != "none";
                if !exists {
                    return (RespValue::Bulk(None), SessionAction::Continue);
                }
                (RespValue::Integer(0), SessionAction::Continue)
            }
            _ => (
                RespValue::Error(format!("ERR unknown subcommand '{}'", sub.to_lowercase())),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn strlen(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'strlen' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Integer(self.store.strlen(&args[1]).await),
            SessionAction::Continue,
        )
    }

    pub(super) async fn append(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 3 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'append' command".to_string()),
                SessionAction::Continue,
            );
        }
        match self.store.append(&args[1], &args[2]).await {
            Ok(v) => (RespValue::Integer(v), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }
}
