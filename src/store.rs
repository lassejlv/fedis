use std::collections::HashMap;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::persistence::{Aof, LogRecord};

#[derive(Clone)]
pub struct Store {
    state: std::sync::Arc<RwLock<HashMap<Vec<u8>, ValueEntry>>>,
    aof: Aof,
    rewrite_in_progress: std::sync::Arc<AtomicBool>,
    rewrite_count: std::sync::Arc<AtomicU64>,
    rewrite_fail_count: std::sync::Arc<AtomicU64>,
    last_rewrite_epoch_sec: std::sync::Arc<AtomicU64>,
    snapshot_path: Option<PathBuf>,
    snapshot_in_progress: std::sync::Arc<AtomicBool>,
    snapshot_count: std::sync::Arc<AtomicU64>,
    snapshot_fail_count: std::sync::Arc<AtomicU64>,
    last_snapshot_epoch_sec: std::sync::Arc<AtomicU64>,
}

pub struct StoreMetrics {
    pub keys: usize,
    pub expiring_keys: usize,
    pub approx_memory_bytes: usize,
}

pub struct ScanResult {
    pub next_cursor: u64,
    pub keys: Vec<Vec<u8>>,
}

pub struct PersistenceMetrics {
    pub aof_enabled: bool,
    pub rewrite_in_progress: bool,
    pub rewrite_count: u64,
    pub rewrite_fail_count: u64,
    pub last_rewrite_epoch_sec: u64,
    pub snapshot_in_progress: bool,
    pub snapshot_count: u64,
    pub snapshot_fail_count: u64,
    pub last_snapshot_epoch_sec: u64,
}

pub enum IncrByError {
    NotInteger,
    OutOfRange,
    Internal,
}

pub enum GetExMode {
    None,
    Ex(u64),
    Px(u64),
    Persist,
}

#[derive(Clone)]
struct ValueEntry {
    value: Vec<u8>,
    expires_at: Option<u64>,
}

pub enum SetCondition {
    None,
    Nx,
    Xx,
}

impl Store {
    pub async fn new(
        aof: Aof,
        snapshot_path: Option<PathBuf>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let store = Self {
            state: std::sync::Arc::new(RwLock::new(HashMap::new())),
            aof,
            rewrite_in_progress: std::sync::Arc::new(AtomicBool::new(false)),
            rewrite_count: std::sync::Arc::new(AtomicU64::new(0)),
            rewrite_fail_count: std::sync::Arc::new(AtomicU64::new(0)),
            last_rewrite_epoch_sec: std::sync::Arc::new(AtomicU64::new(0)),
            snapshot_path,
            snapshot_in_progress: std::sync::Arc::new(AtomicBool::new(false)),
            snapshot_count: std::sync::Arc::new(AtomicU64::new(0)),
            snapshot_fail_count: std::sync::Arc::new(AtomicU64::new(0)),
            last_snapshot_epoch_sec: std::sync::Arc::new(AtomicU64::new(0)),
        };
        store.load_snapshot().await?;
        store.replay().await?;
        Ok(store)
    }

    async fn load_snapshot(&self) -> Result<(), Box<dyn std::error::Error>> {
        let Some(path) = &self.snapshot_path else {
            return Ok(());
        };
        if !path.exists() {
            return Ok(());
        }

        let entries = read_snapshot(path)?;
        let mut state = self.state.write().await;
        state.clear();
        for (key, value, expires_at) in entries {
            if !is_expired(expires_at) {
                state.insert(key, ValueEntry { value, expires_at });
            }
        }
        Ok(())
    }

    async fn replay(&self) -> Result<(), Box<dyn std::error::Error>> {
        let records = self.aof.read_all()?;
        let mut state = self.state.write().await;
        for record in records {
            match record {
                LogRecord::Set {
                    key,
                    value,
                    expires_at,
                } => {
                    if !is_expired(expires_at) {
                        state.insert(key, ValueEntry { value, expires_at });
                    }
                }
                LogRecord::Del { key } => {
                    state.remove(&key);
                }
                LogRecord::Expire { key, expires_at } => {
                    if let Some(entry) = state.get_mut(&key) {
                        entry.expires_at = Some(expires_at);
                    }
                }
                LogRecord::Persist { key } => {
                    if let Some(entry) = state.get_mut(&key) {
                        entry.expires_at = None;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        {
            let state = self.state.read().await;
            if let Some(entry) = state.get(key) {
                if !is_expired(entry.expires_at) {
                    return Some(entry.value.clone());
                }
            } else {
                return None;
            }
        }

        let mut state = self.state.write().await;
        if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return None;
            }
            return Some(entry.value.clone());
        }
        None
    }

    pub async fn getdel(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        let value = if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                None
            } else {
                let value = entry.value.clone();
                state.remove(key);
                Some(value)
            }
        } else {
            None
        };
        drop(state);

        if value.is_some() {
            self.aof
                .append(LogRecord::Del { key: key.to_vec() })
                .await?;
        }

        Ok(value)
    }

    pub async fn set(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: Option<u64>,
        condition: SetCondition,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        let exists = state.get(&key).is_some_and(|e| !is_expired(e.expires_at));
        let allowed = match condition {
            SetCondition::None => true,
            SetCondition::Nx => !exists,
            SetCondition::Xx => exists,
        };

        if !allowed {
            return Ok(false);
        }

        state.insert(
            key.clone(),
            ValueEntry {
                value: value.clone(),
                expires_at,
            },
        );
        drop(state);

        self.aof
            .append(LogRecord::Set {
                key,
                value,
                expires_at,
            })
            .await?;
        Ok(true)
    }

    pub async fn msetnx(
        &self,
        pairs: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;

        for (key, _) in pairs {
            if let Some(entry) = state.get(key) {
                if is_expired(entry.expires_at) {
                    state.remove(key);
                } else {
                    return Ok(false);
                }
            }
        }

        for (key, value) in pairs {
            state.insert(
                key.clone(),
                ValueEntry {
                    value: value.clone(),
                    expires_at: None,
                },
            );
        }
        drop(state);

        for (key, value) in pairs {
            self.aof
                .append(LogRecord::Set {
                    key: key.clone(),
                    value: value.clone(),
                    expires_at: None,
                })
                .await?;
        }

        Ok(true)
    }

    pub async fn del(&self, keys: &[Vec<u8>]) -> Result<i64, Box<dyn std::error::Error>> {
        let mut removed = 0_i64;
        let mut state = self.state.write().await;
        for key in keys {
            if state.remove(key).is_some() {
                removed += 1;
            }
        }
        drop(state);

        for key in keys {
            self.aof.append(LogRecord::Del { key: key.clone() }).await?;
        }

        Ok(removed)
    }

    pub async fn exists(&self, keys: &[Vec<u8>]) -> i64 {
        let mut count = 0_i64;
        let mut state = self.state.write().await;
        for key in keys {
            if let Some(entry) = state.get(key) {
                if is_expired(entry.expires_at) {
                    state.remove(key);
                } else {
                    count += 1;
                }
            }
        }
        count
    }

    pub async fn expire(
        &self,
        key: &[u8],
        seconds: u64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let expires_at = now_ms().saturating_add(seconds.saturating_mul(1000));
        self.expire_at_ms(key, expires_at).await
    }

    pub async fn pexpire(
        &self,
        key: &[u8],
        milliseconds: u64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let expires_at = now_ms().saturating_add(milliseconds);
        self.expire_at_ms(key, expires_at).await
    }

    pub async fn expire_at(
        &self,
        key: &[u8],
        seconds_timestamp: u64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        self.expire_at_ms(key, seconds_timestamp.saturating_mul(1000))
            .await
    }

    pub async fn expire_at_ms(
        &self,
        key: &[u8],
        expires_at: u64,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get_mut(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return Ok(false);
            }
            entry.expires_at = Some(expires_at);
            drop(state);
            self.aof
                .append(LogRecord::Expire {
                    key: key.to_vec(),
                    expires_at,
                })
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn persist(&self, key: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get_mut(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return Ok(false);
            }
            if entry.expires_at.is_none() {
                return Ok(false);
            }
            entry.expires_at = None;
            drop(state);
            self.aof
                .append(LogRecord::Persist { key: key.to_vec() })
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn ttl(&self, key: &[u8]) -> i64 {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return -2;
            }
            if let Some(exp) = entry.expires_at {
                let now = now_ms();
                if exp <= now {
                    state.remove(key);
                    return -2;
                }
                return ((exp - now) / 1000) as i64;
            }
            return -1;
        }
        -2
    }

    pub async fn pttl(&self, key: &[u8]) -> i64 {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return -2;
            }
            if let Some(exp) = entry.expires_at {
                let now = now_ms();
                if exp <= now {
                    state.remove(key);
                    return -2;
                }
                return (exp - now) as i64;
            }
            return -1;
        }
        -2
    }

    pub async fn incr_by(&self, key: &[u8], amount: i64) -> Result<i64, IncrByError> {
        let mut state = self.state.write().await;
        let (current, expires_at) = if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                (0_i64, None)
            } else {
                let parsed = std::str::from_utf8(&entry.value)
                    .ok()
                    .and_then(|v| v.parse::<i64>().ok())
                    .ok_or(IncrByError::NotInteger)?;
                (parsed, entry.expires_at)
            }
        } else {
            (0_i64, None)
        };

        let next = current.checked_add(amount).ok_or(IncrByError::OutOfRange)?;
        let next_bytes = next.to_string().into_bytes();
        state.insert(
            key.to_vec(),
            ValueEntry {
                value: next_bytes.clone(),
                expires_at,
            },
        );
        drop(state);

        self.aof
            .append(LogRecord::Set {
                key: key.to_vec(),
                value: next_bytes,
                expires_at,
            })
            .await
            .map_err(|_| IncrByError::Internal)?;

        Ok(next)
    }

    pub async fn metrics(&self) -> StoreMetrics {
        let state = self.state.read().await;
        let mut expiring = 0_usize;
        let mut memory = 0_usize;

        for (key, entry) in state.iter() {
            if entry.expires_at.is_some() {
                expiring += 1;
            }
            memory = memory
                .saturating_add(key.len())
                .saturating_add(entry.value.len())
                .saturating_add(std::mem::size_of::<ValueEntry>());
        }

        StoreMetrics {
            keys: state.len(),
            expiring_keys: expiring,
            approx_memory_bytes: memory,
        }
    }

    pub async fn cleanup_expired(&self) {
        let mut state = self.state.write().await;
        let now = now_ms();
        state.retain(|_, v| v.expires_at.is_none_or(|exp| exp > now));
    }

    pub async fn dbsize(&self) -> i64 {
        let mut state = self.state.write().await;
        let now = now_ms();
        state.retain(|_, v| v.expires_at.is_none_or(|exp| exp > now));
        state.len() as i64
    }

    pub async fn key_type(&self, key: &[u8]) -> &'static str {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return "none";
            }
            return "string";
        }
        "none"
    }

    pub async fn memory_usage(&self, key: &[u8]) -> Option<i64> {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return None;
            }
            let bytes = key
                .len()
                .saturating_add(entry.value.len())
                .saturating_add(std::mem::size_of::<ValueEntry>());
            return Some(bytes as i64);
        }
        None
    }

    pub async fn object_encoding(&self, key: &[u8]) -> Option<&'static str> {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return None;
            }
            return Some("raw");
        }
        None
    }

    pub async fn strlen(&self, key: &[u8]) -> i64 {
        let mut state = self.state.write().await;
        if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                return 0;
            }
            return entry.value.len() as i64;
        }
        0
    }

    pub async fn append(
        &self,
        key: &[u8],
        suffix: &[u8],
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        let (mut value, expires_at) = if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                (Vec::new(), None)
            } else {
                (entry.value.clone(), entry.expires_at)
            }
        } else {
            (Vec::new(), None)
        };

        value.extend_from_slice(suffix);
        let new_len = value.len() as i64;
        state.insert(
            key.to_vec(),
            ValueEntry {
                value: value.clone(),
                expires_at,
            },
        );
        drop(state);

        self.aof
            .append(LogRecord::Set {
                key: key.to_vec(),
                value,
                expires_at,
            })
            .await?;

        Ok(new_len)
    }

    pub async fn getrange(&self, key: &[u8], start: i64, end: i64) -> Vec<u8> {
        let mut state = self.state.write().await;
        let Some(entry) = state.get(key) else {
            return Vec::new();
        };

        if is_expired(entry.expires_at) {
            state.remove(key);
            return Vec::new();
        }

        slice_range(&entry.value, start, end)
    }

    pub async fn setrange(
        &self,
        key: &[u8],
        offset: usize,
        value: &[u8],
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        let (mut current, expires_at) = if let Some(entry) = state.get(key) {
            if is_expired(entry.expires_at) {
                state.remove(key);
                (Vec::new(), None)
            } else {
                (entry.value.clone(), entry.expires_at)
            }
        } else {
            (Vec::new(), None)
        };

        if current.len() < offset {
            current.resize(offset, 0);
        }
        if current.len() < offset + value.len() {
            current.resize(offset + value.len(), 0);
        }
        current[offset..offset + value.len()].copy_from_slice(value);
        let new_len = current.len() as i64;

        state.insert(
            key.to_vec(),
            ValueEntry {
                value: current.clone(),
                expires_at,
            },
        );
        drop(state);

        self.aof
            .append(LogRecord::Set {
                key: key.to_vec(),
                value: current,
                expires_at,
            })
            .await?;

        Ok(new_len)
    }

    pub async fn getset(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        let previous = if let Some(entry) = state.get(&key) {
            if is_expired(entry.expires_at) {
                state.remove(&key);
                None
            } else {
                Some(entry.value.clone())
            }
        } else {
            None
        };

        state.insert(
            key.clone(),
            ValueEntry {
                value: value.clone(),
                expires_at: None,
            },
        );
        drop(state);

        self.aof
            .append(LogRecord::Set {
                key,
                value,
                expires_at: None,
            })
            .await?;

        Ok(previous)
    }

    pub async fn getex(
        &self,
        key: &[u8],
        mode: GetExMode,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut state = self.state.write().await;
        let Some(entry) = state.get_mut(key) else {
            return Ok(None);
        };

        if is_expired(entry.expires_at) {
            state.remove(key);
            return Ok(None);
        }

        let value = entry.value.clone();
        let key_owned = key.to_vec();
        let mut log_record = None;
        match mode {
            GetExMode::None => {}
            GetExMode::Ex(seconds) => {
                let expires_at = now_ms().saturating_add(seconds.saturating_mul(1000));
                entry.expires_at = Some(expires_at);
                log_record = Some(LogRecord::Expire {
                    key: key_owned,
                    expires_at,
                });
            }
            GetExMode::Px(milliseconds) => {
                let expires_at = now_ms().saturating_add(milliseconds);
                entry.expires_at = Some(expires_at);
                log_record = Some(LogRecord::Expire {
                    key: key_owned,
                    expires_at,
                });
            }
            GetExMode::Persist => {
                entry.expires_at = None;
                log_record = Some(LogRecord::Persist { key: key_owned });
            }
        }
        drop(state);

        if let Some(record) = log_record {
            self.aof.append(record).await?;
        }

        Ok(Some(value))
    }

    pub async fn keys(&self, pattern: &[u8]) -> Vec<Vec<u8>> {
        let mut state = self.state.write().await;
        let now = now_ms();
        state.retain(|_, v| v.expires_at.is_none_or(|exp| exp > now));

        let mut out: Vec<Vec<u8>> = state
            .keys()
            .filter(|k| glob_match(pattern, k))
            .cloned()
            .collect();
        out.sort();
        out
    }

    pub async fn scan(&self, cursor: u64, pattern: &[u8], count: usize) -> ScanResult {
        let mut state = self.state.write().await;
        let now = now_ms();
        state.retain(|_, v| v.expires_at.is_none_or(|exp| exp > now));

        let mut keys: Vec<Vec<u8>> = state
            .keys()
            .filter(|k| glob_match(pattern, k))
            .cloned()
            .collect();
        keys.sort();

        let start = cursor as usize;
        if start >= keys.len() {
            return ScanResult {
                next_cursor: 0,
                keys: Vec::new(),
            };
        }

        let step = count.max(1);
        let end = (start + step).min(keys.len());
        let next = if end >= keys.len() { 0 } else { end as u64 };

        ScanResult {
            next_cursor: next,
            keys: keys[start..end].to_vec(),
        }
    }

    pub async fn bgrewriteaof(&self) -> bool {
        if self
            .rewrite_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }

        let store = self.clone();
        tokio::spawn(async move {
            if store.rewrite_aof().await.is_ok() {
                store.rewrite_count.fetch_add(1, Ordering::SeqCst);
                store
                    .last_rewrite_epoch_sec
                    .store(now_ms() / 1000, Ordering::SeqCst);
            } else {
                store.rewrite_fail_count.fetch_add(1, Ordering::SeqCst);
            }
            store.rewrite_in_progress.store(false, Ordering::SeqCst);
        });
        true
    }

    async fn rewrite_aof(&self) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = {
            let mut state = self.state.write().await;
            let now = now_ms();
            state.retain(|_, v| v.expires_at.is_none_or(|exp| exp > now));
            state
                .iter()
                .map(|(key, entry)| (key.clone(), entry.value.clone(), entry.expires_at))
                .collect::<Vec<_>>()
        };

        self.aof.rewrite_from_snapshot(snapshot).await
    }

    pub fn persistence_metrics(&self) -> PersistenceMetrics {
        PersistenceMetrics {
            aof_enabled: true,
            rewrite_in_progress: self.rewrite_in_progress.load(Ordering::SeqCst),
            rewrite_count: self.rewrite_count.load(Ordering::SeqCst),
            rewrite_fail_count: self.rewrite_fail_count.load(Ordering::SeqCst),
            last_rewrite_epoch_sec: self.last_rewrite_epoch_sec.load(Ordering::SeqCst),
            snapshot_in_progress: self.snapshot_in_progress.load(Ordering::SeqCst),
            snapshot_count: self.snapshot_count.load(Ordering::SeqCst),
            snapshot_fail_count: self.snapshot_fail_count.load(Ordering::SeqCst),
            last_snapshot_epoch_sec: self.last_snapshot_epoch_sec.load(Ordering::SeqCst),
        }
    }

    pub async fn save_snapshot_now(&self) -> Result<(), Box<dyn std::error::Error>> {
        let Some(path) = &self.snapshot_path else {
            return Err("snapshot path is not configured".into());
        };

        let entries = {
            let mut state = self.state.write().await;
            let now = now_ms();
            state.retain(|_, v| v.expires_at.is_none_or(|exp| exp > now));
            state
                .iter()
                .map(|(k, v)| (k.clone(), v.value.clone(), v.expires_at))
                .collect::<Vec<_>>()
        };

        write_snapshot(path, entries)?;
        self.snapshot_count.fetch_add(1, Ordering::SeqCst);
        self.last_snapshot_epoch_sec
            .store(now_ms() / 1000, Ordering::SeqCst);
        Ok(())
    }

    pub async fn bgsave(&self) -> bool {
        if self.snapshot_path.is_none() {
            return false;
        }

        if self
            .snapshot_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }

        let store = self.clone();
        tokio::spawn(async move {
            if store.save_snapshot_now().await.is_err() {
                store.snapshot_fail_count.fetch_add(1, Ordering::SeqCst);
            }
            store.snapshot_in_progress.store(false, Ordering::SeqCst);
        });
        true
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_expired(exp: Option<u64>) -> bool {
    exp.is_some_and(|v| v <= now_ms())
}

fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0_usize;
    let mut t = 0_usize;
    let mut star_idx: Option<usize> = None;
    let mut match_idx = 0_usize;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
            continue;
        }

        if p < pattern.len() && pattern[p] == b'*' {
            star_idx = Some(p);
            p += 1;
            match_idx = t;
            continue;
        }

        if let Some(star) = star_idx {
            p = star + 1;
            match_idx += 1;
            t = match_idx;
            continue;
        }

        return false;
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

fn slice_range(value: &[u8], start: i64, end: i64) -> Vec<u8> {
    if value.is_empty() {
        return Vec::new();
    }

    let len = value.len() as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };

    if s < 0 {
        s = 0;
    }
    if e < 0 {
        return Vec::new();
    }
    if s >= len {
        return Vec::new();
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return Vec::new();
    }

    value[s as usize..=e as usize].to_vec()
}

const SNAP_MAGIC: &[u8] = b"FDSNP1";

fn write_snapshot(
    path: &Path,
    entries: Vec<(Vec<u8>, Vec<u8>, Option<u64>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut out = Vec::new();
    out.extend_from_slice(SNAP_MAGIC);
    for (key, value, expires_at) in entries {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(&key);
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(&value);
        let exp = expires_at.map(|v| v as i64).unwrap_or(-1);
        out.extend_from_slice(&exp.to_be_bytes());
    }
    let tmp = path.with_extension("snapshot.tmp");
    std::fs::write(&tmp, out)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn read_snapshot(
    path: &Path,
) -> Result<Vec<(Vec<u8>, Vec<u8>, Option<u64>)>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    let mut file = std::fs::File::open(path)?;
    file.read_to_end(&mut bytes)?;

    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.len() < SNAP_MAGIC.len() || &bytes[..SNAP_MAGIC.len()] != SNAP_MAGIC {
        return Err("invalid snapshot magic header".into());
    }

    let mut idx = SNAP_MAGIC.len();
    let mut out = Vec::new();
    while idx < bytes.len() {
        if idx + 4 > bytes.len() {
            return Err(
                std::io::Error::new(ErrorKind::InvalidData, "truncated snapshot key len").into(),
            );
        }
        let klen = u32::from_be_bytes(bytes[idx..idx + 4].try_into()?) as usize;
        idx += 4;
        if idx + klen > bytes.len() {
            return Err(
                std::io::Error::new(ErrorKind::InvalidData, "truncated snapshot key").into(),
            );
        }
        let key = bytes[idx..idx + klen].to_vec();
        idx += klen;

        if idx + 4 > bytes.len() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "truncated snapshot value len",
            )
            .into());
        }
        let vlen = u32::from_be_bytes(bytes[idx..idx + 4].try_into()?) as usize;
        idx += 4;
        if idx + vlen > bytes.len() {
            return Err(
                std::io::Error::new(ErrorKind::InvalidData, "truncated snapshot value").into(),
            );
        }
        let value = bytes[idx..idx + vlen].to_vec();
        idx += vlen;

        if idx + 8 > bytes.len() {
            return Err(
                std::io::Error::new(ErrorKind::InvalidData, "truncated snapshot expiry").into(),
            );
        }
        let exp = i64::from_be_bytes(bytes[idx..idx + 8].try_into()?);
        idx += 8;
        let expires_at = if exp < 0 { None } else { Some(exp as u64) };
        out.push((key, value, expires_at));
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::AofFsync;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_paths() -> (PathBuf, PathBuf) {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("fedis-store-test-{}-{}", std::process::id(), id));
        let _ = std::fs::create_dir_all(&root);
        (root.join("test.aof"), root.join("test.snapshot"))
    }

    #[tokio::test]
    async fn restart_recovers_from_aof_and_snapshot() {
        let (aof_path, snapshot_path) = temp_paths();

        let aof = Aof::open(&aof_path, AofFsync::Always)
            .await
            .expect("open aof");
        let store = Store::new(aof, Some(snapshot_path.clone()))
            .await
            .expect("new store");

        let _ = store
            .set(b"k".to_vec(), b"v1".to_vec(), None, SetCondition::None)
            .await
            .expect("set v1");
        store.save_snapshot_now().await.expect("save snapshot");
        let _ = store
            .set(b"k".to_vec(), b"v2".to_vec(), None, SetCondition::None)
            .await
            .expect("set v2");
        drop(store);

        let aof = Aof::open(&aof_path, AofFsync::Always)
            .await
            .expect("reopen aof");
        let store = Store::new(aof, Some(snapshot_path.clone()))
            .await
            .expect("reopen store");

        assert_eq!(store.get(b"k").await, Some(b"v2".to_vec()));

        let _ = std::fs::remove_file(&aof_path);
        let _ = std::fs::remove_file(&snapshot_path);
    }

    #[tokio::test]
    async fn restart_recovers_from_aof_without_snapshot() {
        let (aof_path, _) = temp_paths();

        let aof = Aof::open(&aof_path, AofFsync::Always)
            .await
            .expect("open aof");
        let store = Store::new(aof, None).await.expect("new store");
        let _ = store
            .set(b"a".to_vec(), b"1".to_vec(), None, SetCondition::None)
            .await
            .expect("set key");
        drop(store);

        let aof = Aof::open(&aof_path, AofFsync::Always)
            .await
            .expect("reopen aof");
        let store = Store::new(aof, None).await.expect("reopen store");
        assert_eq!(store.get(b"a").await, Some(b"1".to_vec()));

        let _ = std::fs::remove_file(&aof_path);
    }
}
