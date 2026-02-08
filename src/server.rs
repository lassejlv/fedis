use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::auth::{Auth, SessionAuth};
use crate::command::{CommandExecutor, SessionAction};
use crate::config::Config;
use crate::persistence::Aof;
use crate::protocol::{RespValue, encode, frame_to_args, read_frame};
use crate::stats::ServerStats;
use crate::store::Store;

pub struct Server {
    config: Config,
    executor: Arc<CommandExecutor>,
    store: Store,
    stats: Arc<ServerStats>,
    next_connection_id: Arc<AtomicU64>,
}

impl Server {
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let aof = Aof::open(&config.aof_path, config.aof_fsync).await?;
        let store = Store::new(aof).await?;
        let auth = Auth::new(config.users.clone(), config.default_user.clone());
        let stats = Arc::new(ServerStats::new());
        let executor = Arc::new(CommandExecutor::new(
            auth,
            store.clone(),
            stats.clone(),
            config.listen_addr.clone(),
        ));
        Ok(Self {
            config,
            executor,
            store,
            stats,
            next_connection_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(
            listen_addr = %listener.local_addr()?,
            non_redis_mode = self.config.non_redis_mode,
            debug_response_ids = self.config.debug_response_ids,
            "server started"
        );

        if self.config.debug_response_ids && !self.config.non_redis_mode {
            warn!(
                "FEDIS_DEBUG_RESPONSE_ID is enabled but FEDIS_NON_REDIS_MODE is off; response IDs are disabled"
            );
        }
        let cleanup_store = self.store.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            loop {
                ticker.tick().await;
                cleanup_store.cleanup_expired().await;
            }
        });

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            let executor = self.executor.clone();
            let stats = self.stats.clone();
            let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
            let with_response_ids = self.config.non_redis_mode && self.config.debug_response_ids;
            stats.on_connect();
            info!(connection_id, peer = %peer_addr, "client connected");
            tokio::spawn(async move {
                if let Err(e) = handle_client(
                    socket,
                    executor,
                    connection_id,
                    peer_addr,
                    with_response_ids,
                )
                .await
                {
                    warn!(connection_id, peer = %peer_addr, error = %e, "client loop failed");
                }
                stats.on_disconnect();
                info!(connection_id, peer = %peer_addr, "client disconnected");
            });
        }
    }
}

async fn handle_client(
    socket: TcpStream,
    executor: Arc<CommandExecutor>,
    connection_id: u64,
    peer_addr: std::net::SocketAddr,
    with_response_ids: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader_half, writer_half) = socket.into_split();
    let mut reader = BufReader::new(reader_half);
    let mut writer = BufWriter::new(writer_half);
    let mut session = SessionAuth::default();
    let mut request_id = 0_u64;

    loop {
        let Some(frame) = read_frame(&mut reader).await? else {
            break;
        };

        let response = match frame_to_args(frame) {
            Ok(args) => {
                request_id = request_id.saturating_add(1);
                let command = command_name(&args);
                let arg_count = args.len();
                let started = Instant::now();
                let (resp, action) = executor.execute(args, &mut session).await;
                let elapsed_ms = started.elapsed().as_millis() as u64;
                let authed_user = session.user.as_deref().unwrap_or("-");
                if matches!(resp, RespValue::Error(_)) {
                    warn!(
                        connection_id,
                        request_id,
                        peer = %peer_addr,
                        user = authed_user,
                        command,
                        arg_count,
                        elapsed_ms,
                        "command failed"
                    );
                } else {
                    debug!(
                        connection_id,
                        request_id,
                        peer = %peer_addr,
                        user = authed_user,
                        command,
                        arg_count,
                        elapsed_ms,
                        "command handled"
                    );
                }
                let payload = if with_response_ids {
                    wrap_with_request_id(resp, request_id)
                } else {
                    resp
                };
                let encoded = encode(payload);
                writer.write_all(&encoded).await?;
                writer.flush().await?;
                if matches!(action, SessionAction::Close) {
                    break;
                }
                continue;
            }
            Err(e) => {
                request_id = request_id.saturating_add(1);
                warn!(connection_id, peer = %peer_addr, error = %e, "invalid client frame");
                let resp = RespValue::Error(e);
                if with_response_ids {
                    wrap_with_request_id(resp, request_id)
                } else {
                    resp
                }
            }
        };

        writer.write_all(&encode(response)).await?;
        writer.flush().await?;
    }

    Ok(())
}

fn wrap_with_request_id(response: RespValue, request_id: u64) -> RespValue {
    RespValue::Array(vec![
        RespValue::Simple("RID".to_string()),
        RespValue::Bulk(Some(request_id.to_string().into_bytes())),
        response,
    ])
}

fn command_name(args: &[Vec<u8>]) -> String {
    args.first()
        .map(|v| String::from_utf8_lossy(v).to_uppercase())
        .unwrap_or_else(|| "<empty>".to_string())
}
