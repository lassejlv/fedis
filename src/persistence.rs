use std::io::{ErrorKind, Read};
use std::path::Path;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

const MAGIC: &[u8] = b"FDLOG1";
const OP_SET: u8 = 1;
const OP_DEL: u8 = 2;
const OP_EXPIRE: u8 = 3;
const OP_PERSIST: u8 = 4;

#[derive(Clone, Copy)]
pub enum AofFsync {
    Always,
    EverySec,
    No,
}

#[derive(Clone)]
pub struct Aof {
    inner: std::sync::Arc<Mutex<tokio::fs::File>>,
    path: std::path::PathBuf,
    fsync: AofFsync,
    tx: Option<mpsc::Sender<Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub enum LogRecord {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: Option<u64>,
    },
    Del {
        key: Vec<u8>,
    },
    Expire {
        key: Vec<u8>,
        expires_at: u64,
    },
    Persist {
        key: Vec<u8>,
    },
}

impl Aof {
    pub async fn open(path: &Path, fsync: AofFsync) -> Result<Self, Box<dyn std::error::Error>> {
        let exists = std::fs::metadata(path).is_ok();
        if !exists {
            std::fs::write(path, MAGIC)?;
        }

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await?;
        let mut tx = None;
        if matches!(fsync, AofFsync::EverySec | AofFsync::No) {
            let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(4096);
            let inner = std::sync::Arc::new(Mutex::new(
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(path)
                    .await?,
            ));
            let write_inner = inner.clone();
            tokio::spawn(async move {
                while let Some(mut batch) = receiver.recv().await {
                    let mut took = 1usize;
                    while took < 256 {
                        match receiver.try_recv() {
                            Ok(next) => {
                                batch.extend_from_slice(&next);
                                took += 1;
                            }
                            Err(_) => break,
                        }
                    }
                    let mut file = write_inner.lock().await;
                    let _ = file.write_all(&batch).await;
                }
            });
            tx = Some(sender);
            let aof = Self {
                inner,
                path: path.to_path_buf(),
                fsync,
                tx,
            };

            if matches!(fsync, AofFsync::EverySec) {
                let inner = aof.inner.clone();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                    loop {
                        interval.tick().await;
                        let mut file = inner.lock().await;
                        let _ = file.flush().await;
                        let _ = file.sync_data().await;
                    }
                });
            }

            return Ok(aof);
        }

        let aof = Self {
            inner: std::sync::Arc::new(Mutex::new(file)),
            path: path.to_path_buf(),
            fsync,
            tx,
        };

        if matches!(fsync, AofFsync::EverySec) {
            let inner = aof.inner.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    let mut file = inner.lock().await;
                    let _ = file.flush().await;
                    let _ = file.sync_data().await;
                }
            });
        }

        Ok(aof)
    }

    pub fn read_all(&self) -> Result<Vec<LogRecord>, Box<dyn std::error::Error>> {
        Self::read_all_from_path(&self.path)
    }

    pub async fn append(&self, record: LogRecord) -> Result<(), Box<dyn std::error::Error>> {
        let payload = encode_record(record);
        let mut wire = Vec::with_capacity(4 + payload.len());
        wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        wire.extend_from_slice(&payload);

        if let Some(tx) = &self.tx {
            tx.send(wire)
                .await
                .map_err(|_| "AOF writer task is not available")?;
            return Ok(());
        }

        let mut file = self.inner.lock().await;
        file.write_all(&wire).await?;
        match self.fsync {
            AofFsync::Always => {
                file.flush().await?;
                file.sync_data().await?;
            }
            AofFsync::EverySec | AofFsync::No => {}
        }
        Ok(())
    }

    pub async fn rewrite_from_snapshot(
        &self,
        entries: Vec<(Vec<u8>, Vec<u8>, Option<u64>)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_path = self.path.with_extension("aof.rewrite");
        let mut buf = Vec::with_capacity(1024 + entries.len() * 32);
        buf.extend_from_slice(MAGIC);

        for (key, value, expires_at) in entries {
            let payload = encode_record(LogRecord::Set {
                key,
                value,
                expires_at,
            });
            buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buf.extend_from_slice(&payload);
        }

        let mut file_guard = self.inner.lock().await;
        std::fs::write(&temp_path, &buf)?;
        std::fs::rename(&temp_path, &self.path)?;

        let replacement = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)
            .await?;
        *file_guard = replacement;

        Ok(())
    }

    fn read_all_from_path(path: &Path) -> Result<Vec<LogRecord>, Box<dyn std::error::Error>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let mut bytes = Vec::new();
        let mut file = std::fs::File::open(path)?;
        file.read_to_end(&mut bytes)?;

        if bytes.is_empty() {
            return Ok(Vec::new());
        }

        if bytes.len() < MAGIC.len() || &bytes[..MAGIC.len()] != MAGIC {
            return Err("invalid AOF magic header".into());
        }

        let mut idx = MAGIC.len();
        let mut out = Vec::new();
        while idx < bytes.len() {
            if idx + 4 > bytes.len() {
                return Err(
                    std::io::Error::new(ErrorKind::InvalidData, "truncated AOF size").into(),
                );
            }
            let size = u32::from_be_bytes(bytes[idx..idx + 4].try_into()?) as usize;
            idx += 4;
            if idx + size > bytes.len() {
                return Err(
                    std::io::Error::new(ErrorKind::InvalidData, "truncated AOF record").into(),
                );
            }
            let record = decode_record(&bytes[idx..idx + size])?;
            idx += size;
            out.push(record);
        }

        Ok(out)
    }
}

fn encode_record(record: LogRecord) -> Vec<u8> {
    let mut payload = Vec::new();
    match record {
        LogRecord::Set {
            key,
            value,
            expires_at,
        } => {
            payload.push(OP_SET);
            write_bytes(&mut payload, &key);
            write_bytes(&mut payload, &value);
            write_i64(&mut payload, expires_at.map(|v| v as i64).unwrap_or(-1));
        }
        LogRecord::Del { key } => {
            payload.push(OP_DEL);
            write_bytes(&mut payload, &key);
        }
        LogRecord::Expire { key, expires_at } => {
            payload.push(OP_EXPIRE);
            write_bytes(&mut payload, &key);
            write_i64(&mut payload, expires_at as i64);
        }
        LogRecord::Persist { key } => {
            payload.push(OP_PERSIST);
            write_bytes(&mut payload, &key);
        }
    }
    payload
}

fn write_bytes(dst: &mut Vec<u8>, value: &[u8]) {
    dst.extend_from_slice(&(value.len() as u32).to_be_bytes());
    dst.extend_from_slice(value);
}

fn write_i64(dst: &mut Vec<u8>, value: i64) {
    dst.extend_from_slice(&value.to_be_bytes());
}

fn read_bytes(input: &[u8], idx: &mut usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if *idx + 4 > input.len() {
        return Err("invalid record bytes length".into());
    }
    let len = u32::from_be_bytes(input[*idx..*idx + 4].try_into()?) as usize;
    *idx += 4;
    if *idx + len > input.len() {
        return Err("invalid record bytes payload".into());
    }
    let out = input[*idx..*idx + len].to_vec();
    *idx += len;
    Ok(out)
}

fn read_i64(input: &[u8], idx: &mut usize) -> Result<i64, Box<dyn std::error::Error>> {
    if *idx + 8 > input.len() {
        return Err("invalid record i64".into());
    }
    let value = i64::from_be_bytes(input[*idx..*idx + 8].try_into()?);
    *idx += 8;
    Ok(value)
}

fn decode_record(input: &[u8]) -> Result<LogRecord, Box<dyn std::error::Error>> {
    if input.is_empty() {
        return Err("empty record".into());
    }

    let op = input[0];
    let mut idx = 1;
    match op {
        OP_SET => {
            let key = read_bytes(input, &mut idx)?;
            let value = read_bytes(input, &mut idx)?;
            let exp = read_i64(input, &mut idx)?;
            Ok(LogRecord::Set {
                key,
                value,
                expires_at: if exp < 0 { None } else { Some(exp as u64) },
            })
        }
        OP_DEL => {
            let key = read_bytes(input, &mut idx)?;
            Ok(LogRecord::Del { key })
        }
        OP_EXPIRE => {
            let key = read_bytes(input, &mut idx)?;
            let exp = read_i64(input, &mut idx)?;
            if exp < 0 {
                return Err("expire timestamp cannot be negative".into());
            }
            Ok(LogRecord::Expire {
                key,
                expires_at: exp as u64,
            })
        }
        OP_PERSIST => {
            let key = read_bytes(input, &mut idx)?;
            Ok(LogRecord::Persist { key })
        }
        _ => Err("unknown AOF operation".into()),
    }
}
