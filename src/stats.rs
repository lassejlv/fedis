use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

pub struct ServerStats {
    started_at: Instant,
    connected_clients: AtomicUsize,
    total_connections: AtomicU64,
    total_commands: AtomicU64,
    total_command_usec: AtomicU64,
    ops_window: AtomicU64,
    ops_per_sec: AtomicU64,
    command_calls: Mutex<HashMap<String, CommandTiming>>,
}

#[derive(Clone, Copy)]
struct CommandTiming {
    calls: u64,
    usec: u64,
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            connected_clients: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            total_commands: AtomicU64::new(0),
            total_command_usec: AtomicU64::new(0),
            ops_window: AtomicU64::new(0),
            ops_per_sec: AtomicU64::new(0),
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

    pub fn record_command(&self, command: &str, elapsed_usec: u64) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
        self.total_command_usec
            .fetch_add(elapsed_usec, Ordering::Relaxed);
        self.ops_window.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut calls) = self.command_calls.lock() {
            let key = command.to_ascii_lowercase();
            let entry = calls
                .entry(key)
                .or_insert(CommandTiming { calls: 0, usec: 0 });
            entry.calls += 1;
            entry.usec = entry.usec.saturating_add(elapsed_usec);
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

    pub fn total_command_usec(&self) -> u64 {
        self.total_command_usec.load(Ordering::Relaxed)
    }

    pub fn instantaneous_ops_per_sec(&self) -> u64 {
        self.ops_per_sec.load(Ordering::Relaxed)
    }

    pub fn tick_ops_per_sec(&self) {
        let window = self.ops_window.swap(0, Ordering::Relaxed);
        self.ops_per_sec.store(window, Ordering::Relaxed);
    }

    pub fn command_stats_snapshot(&self) -> Vec<(String, u64, u64)> {
        if let Ok(calls) = self.command_calls.lock() {
            let mut out: Vec<(String, u64, u64)> = calls
                .iter()
                .map(|(k, v)| (k.clone(), v.calls, v.usec))
                .collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            return out;
        }
        Vec::new()
    }
}
