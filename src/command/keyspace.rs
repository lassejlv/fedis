use super::*;

impl CommandExecutor {
    pub(super) async fn del(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'del' command".to_string()),
                SessionAction::Continue,
            );
        }
        match self.store.del(&args[1..]).await {
            Ok(v) => (RespValue::Integer(v), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn unlink(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        self.del(args).await
    }

    pub(super) async fn exists(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'exists' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Integer(self.store.exists(&args[1..]).await),
            SessionAction::Continue,
        )
    }

    pub(super) async fn keys(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'keys' command".to_string()),
                SessionAction::Continue,
            );
        }

        let keys = self.store.keys(&args[1]).await;
        (
            RespValue::Array(keys.into_iter().map(|k| RespValue::Bulk(Some(k))).collect()),
            SessionAction::Continue,
        )
    }

    pub(super) async fn scan(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'scan' command".to_string()),
                SessionAction::Continue,
            );
        }

        let Some(cursor) = parse_u64(&args[1]) else {
            return (
                RespValue::Error("ERR invalid cursor".to_string()),
                SessionAction::Continue,
            );
        };

        let mut pattern = b"*".to_vec();
        let mut count: usize = 10;
        let mut idx = 2;
        while idx < args.len() {
            let token = upper(&args[idx]);
            match token.as_str() {
                "MATCH" => {
                    if idx + 1 >= args.len() {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    pattern = args[idx + 1].clone();
                    idx += 2;
                }
                "COUNT" => {
                    if idx + 1 >= args.len() {
                        return (
                            RespValue::Error("ERR syntax error".to_string()),
                            SessionAction::Continue,
                        );
                    }
                    let Some(v) = parse_u64(&args[idx + 1]) else {
                        return (
                            RespValue::Error(
                                "ERR value is not an integer or out of range".to_string(),
                            ),
                            SessionAction::Continue,
                        );
                    };
                    count = v as usize;
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

        let result = self.store.scan(cursor, &pattern, count).await;
        (
            RespValue::Array(vec![
                RespValue::Bulk(Some(result.next_cursor.to_string().into_bytes())),
                RespValue::Array(
                    result
                        .keys
                        .into_iter()
                        .map(|k| RespValue::Bulk(Some(k)))
                        .collect(),
                ),
            ]),
            SessionAction::Continue,
        )
    }

    pub(super) async fn dbsize(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 1 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'dbsize' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Integer(self.store.dbsize().await),
            SessionAction::Continue,
        )
    }

    pub(super) async fn key_type(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'type' command".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Simple(self.store.key_type(&args[1]).await.to_string()),
            SessionAction::Continue,
        )
    }
}
