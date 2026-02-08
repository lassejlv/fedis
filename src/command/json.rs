use super::*;

impl CommandExecutor {
    pub(super) async fn json_set(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 4 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'json.set' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }
        if !is_root_path(&args[2]) {
            return (
                RespValue::Error("ERR only root path is supported".to_string()),
                SessionAction::Continue,
            );
        }

        match self.store.json_set_root(args[1].clone(), &args[3]).await {
            Ok(()) => (RespValue::Simple("OK".to_string()), SessionAction::Continue),
            Err(_) => (
                RespValue::Error("ERR invalid JSON".to_string()),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn json_get(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 || args.len() > 3 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'json.get' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }
        if args.len() == 3 && !is_root_path(&args[2]) {
            return (
                RespValue::Error("ERR only root path is supported".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Bulk(self.store.json_get_root(&args[1]).await),
            SessionAction::Continue,
        )
    }

    pub(super) async fn json_del(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 || args.len() > 3 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'json.del' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }
        if args.len() == 3 && !is_root_path(&args[2]) {
            return (
                RespValue::Error("ERR only root path is supported".to_string()),
                SessionAction::Continue,
            );
        }
        match self.store.json_del_root(&args[1]).await {
            Ok(v) => (RespValue::Integer(v), SessionAction::Continue),
            Err(e) => (
                RespValue::Error(format!("ERR internal: {}", e)),
                SessionAction::Continue,
            ),
        }
    }

    pub(super) async fn json_type(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() < 2 || args.len() > 3 {
            return (
                RespValue::Error(
                    "ERR wrong number of arguments for 'json.type' command".to_string(),
                ),
                SessionAction::Continue,
            );
        }
        if args.len() == 3 && !is_root_path(&args[2]) {
            return (
                RespValue::Error("ERR only root path is supported".to_string()),
                SessionAction::Continue,
            );
        }
        (
            RespValue::Bulk(
                self.store
                    .json_type_root(&args[1])
                    .await
                    .map(|v| v.as_bytes().to_vec()),
            ),
            SessionAction::Continue,
        )
    }
}

fn is_root_path(path: &[u8]) -> bool {
    path == b"$" || path == b"."
}
