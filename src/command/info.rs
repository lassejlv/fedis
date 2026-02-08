use super::*;

impl CommandExecutor {
    pub(super) async fn info(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() > 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'info' command".to_string()),
                SessionAction::Continue,
            );
        }

        let section = args
            .get(1)
            .map(|s| String::from_utf8_lossy(s).to_ascii_lowercase())
            .unwrap_or_else(|| "default".to_string());

        let metrics = self.store.metrics().await;
        let persistence = self.store.persistence_metrics();
        let commandstats = self.stats.command_stats_snapshot();
        let uptime = self.stats.uptime_secs();
        let lines = match section.as_str() {
            "default" | "all" => vec![
                server_section(uptime, &self.listen_addr),
                clients_section(self.stats.connected_clients()),
                memory_section(metrics.approx_memory_bytes),
                stats_section(
                    self.stats.total_connections(),
                    self.stats.total_commands(),
                    self.stats.total_command_usec(),
                ),
                commandstats_section(&commandstats),
                persistence_section(&persistence),
                keyspace_section(metrics.keys, metrics.expiring_keys),
            ],
            "server" => vec![server_section(uptime, &self.listen_addr)],
            "clients" => vec![clients_section(self.stats.connected_clients())],
            "memory" => vec![memory_section(metrics.approx_memory_bytes)],
            "stats" => vec![stats_section(
                self.stats.total_connections(),
                self.stats.total_commands(),
                self.stats.total_command_usec(),
            )],
            "commandstats" => vec![commandstats_section(&commandstats)],
            "persistence" => vec![persistence_section(&persistence)],
            "keyspace" => vec![keyspace_section(metrics.keys, metrics.expiring_keys)],
            _ => {
                return (
                    RespValue::Error("ERR unsupported INFO section".to_string()),
                    SessionAction::Continue,
                );
            }
        };

        (
            RespValue::Bulk(Some(lines.join("\n").into_bytes())),
            SessionAction::Continue,
        )
    }

    pub(super) fn select(&self, args: &[Vec<u8>]) -> (RespValue, SessionAction) {
        if args.len() != 2 {
            return (
                RespValue::Error("ERR wrong number of arguments for 'select' command".to_string()),
                SessionAction::Continue,
            );
        }
        let Some(db) = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        else {
            return (
                RespValue::Error("ERR value is not an integer or out of range".to_string()),
                SessionAction::Continue,
            );
        };

        if db == 0 {
            return (RespValue::Simple("OK".to_string()), SessionAction::Continue);
        }
        (
            RespValue::Error("ERR DB index is out of range".to_string()),
            SessionAction::Continue,
        )
    }
}

fn server_section(uptime: u64, listen_addr: &str) -> String {
    let days = uptime / 86_400;
    let port = listen_addr
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
        .unwrap_or(6379);
    format!(
        "# Server\nredis_version:7.2.0-fedis\nfedis_version:0.1.0\ntcp_port:{}\nuptime_in_seconds:{}\nuptime_in_days:{}",
        port, uptime, days
    )
}

fn clients_section(connected_clients: usize) -> String {
    format!("# Clients\nconnected_clients:{}", connected_clients)
}

fn memory_section(memory_bytes: usize) -> String {
    format!(
        "# Memory\nused_memory:{}\nused_memory_human:{}",
        memory_bytes,
        human_bytes(memory_bytes)
    )
}

fn stats_section(total_connections: u64, total_commands: u64, total_command_usec: u64) -> String {
    let usec_per_call = if total_commands == 0 {
        0.0
    } else {
        total_command_usec as f64 / total_commands as f64
    };
    format!(
        "# Stats\ntotal_connections_received:{}\ntotal_commands_processed:{}\ntotal_command_usec:{}\ninstantaneous_ops_per_sec:0\nusec_per_call:{:.2}",
        total_connections, total_commands, total_command_usec, usec_per_call
    )
}

fn keyspace_section(keys: usize, expiring_keys: usize) -> String {
    format!("# Keyspace\ndb0:keys={},expires={}", keys, expiring_keys)
}

fn commandstats_section(commandstats: &[(String, u64, u64)]) -> String {
    let mut out = String::from("# Commandstats");
    for (command, calls, usec) in commandstats {
        let usec_per_call = if *calls == 0 {
            0.0
        } else {
            *usec as f64 / *calls as f64
        };
        out.push('\n');
        out.push_str(&format!(
            "cmdstat_{}:calls={},usec={},usec_per_call={:.2}",
            command, calls, usec, usec_per_call
        ));
    }
    out
}

fn persistence_section(metrics: &crate::store::PersistenceMetrics) -> String {
    format!(
        "# Persistence\naof_enabled:{}\naof_rewrite_in_progress:{}\naof_rewrites:{}\naof_rewrite_failures:{}\naof_last_rewrite_epoch_sec:{}",
        if metrics.aof_enabled { 1 } else { 0 },
        if metrics.rewrite_in_progress { 1 } else { 0 },
        metrics.rewrite_count,
        metrics.rewrite_fail_count,
        metrics.last_rewrite_epoch_sec,
    )
}

fn human_bytes(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2}G", b / GB)
    } else if b >= MB {
        format!("{:.2}M", b / MB)
    } else if b >= KB {
        format!("{:.2}K", b / KB)
    } else {
        format!("{}B", bytes)
    }
}
