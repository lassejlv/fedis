use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

pub struct ServerStats {
    started_at: Instant,
    connected_clients: AtomicUsize,
    total_connections: AtomicU64,
    total_commands: AtomicU64,
    command_calls: Mutex<HashMap<String, u64>>,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            connected_clients: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            command_calls: Mutex::new(HashMap::new()),
        }
    }

    pub fn on_connect(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn on_disconnect(&self) {
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn on_command(&self, command: &str) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut calls) = self.command_calls.lock() {
            let key = command.to_ascii_lowercase();
            let entry = calls.entry(key).or_insert(0);
            *entry += 1;
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    pub fn connected_clients(&self) -> usize {
        self.connected_clients.load(Ordering::Relaxed)
    }

    pub fn total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    pub fn total_commands(&self) -> u64 {
        self.total_commands.load(Ordering::Relaxed)
    }

    pub fn command_stats_snapshot(&self) -> Vec<(String, u64)> {
        if let Ok(calls) = self.command_calls.lock() {
            let mut out: Vec<(String, u64)> = calls.iter().map(|(k, v)| (k.clone(), *v)).collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            return out;
        }
        Vec::new()
    }
}
