use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

use tokio::io::{AsyncWriteExt, BufReader};
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
        let store = Store::new(aof, config.snapshot_path.clone()).await?;
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

        if let Some(metrics_addr) = &self.config.metrics_addr {
            let stats = self.stats.clone();
            let store = self.store.clone();
            let addr = metrics_addr.clone();
            tokio::spawn(async move {
                if let Err(e) = run_metrics_server(addr, stats, store).await {
                    warn!(error = %e, "metrics server failed");
                }
            });
        }
        let cleanup_store = self.store.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(500));
            loop {
                ticker.tick().await;
                cleanup_store.cleanup_expired().await;
            }
        });

        if let Some(interval_sec) = self.config.snapshot_interval_sec {
            let save_store = self.store.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(interval_sec.max(1)));
                loop {
                    ticker.tick().await;
                    let _ = save_store.bgsave().await;
                }
            });
        }

        let stats = self.stats.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                stats.tick_ops_per_sec();
            }
        });

        let mut shutdown = std::pin::pin!(tokio::signal::ctrl_c());
        loop {
            let accept_result = tokio::select! {
                _ = &mut shutdown => {
                    info!("shutdown signal received");
                    break;
                }
                accepted = listener.accept() => accepted,
            };

            let (socket, peer_addr) = accept_result?;
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

        info!("server stopped");
        Ok(())
    }
}

async fn run_metrics_server(
    metrics_addr: String,
    stats: Arc<ServerStats>,
    store: Store,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&metrics_addr).await?;
    info!(metrics_addr = %listener.local_addr()?, "metrics server started");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let metrics = format_metrics(&stats, &store).await;
        let body = metrics.into_bytes();
        let header = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(header.as_bytes()).await?;
        socket.write_all(&body).await?;
        socket.flush().await?;
    }
}

async fn format_metrics(stats: &ServerStats, store: &Store) -> String {
    let store_metrics = store.metrics().await;
    let persistence = store.persistence_metrics();
    let command_stats = stats.command_stats_snapshot();

    let mut out = String::new();
    out.push_str(&format!(
        "fedis_connected_clients {}\n",
        stats.connected_clients()
    ));
    out.push_str(&format!(
        "fedis_total_connections {}\n",
        stats.total_connections()
    ));
    out.push_str(&format!(
        "fedis_total_commands {}\n",
        stats.total_commands()
    ));
    out.push_str(&format!(
        "fedis_instantaneous_ops_per_sec {}\n",
        stats.instantaneous_ops_per_sec()
    ));
    out.push_str(&format!(
        "fedis_total_command_usec {}\n",
        stats.total_command_usec()
    ));
    out.push_str(&format!("fedis_keys {}\n", store_metrics.keys));
    out.push_str(&format!(
        "fedis_expiring_keys {}\n",
        store_metrics.expiring_keys
    ));
    out.push_str(&format!(
        "fedis_memory_bytes {}\n",
        store_metrics.approx_memory_bytes
    ));
    out.push_str(&format!(
        "fedis_aof_rewrite_in_progress {}\n",
        if persistence.rewrite_in_progress {
            1
        } else {
            0
        }
    ));
    out.push_str(&format!(
        "fedis_aof_rewrites {}\n",
        persistence.rewrite_count
    ));
    out.push_str(&format!(
        "fedis_aof_rewrite_failures {}\n",
        persistence.rewrite_fail_count
    ));
    out.push_str(&format!(
        "fedis_snapshot_in_progress {}\n",
        if persistence.snapshot_in_progress {
            1
        } else {
            0
        }
    ));
    out.push_str(&format!(
        "fedis_snapshot_saves {}\n",
        persistence.snapshot_count
    ));
    out.push_str(&format!(
        "fedis_snapshot_failures {}\n",
        persistence.snapshot_fail_count
    ));
    out.push_str(&format!(
        "fedis_snapshot_last_save_epoch_sec {}\n",
        persistence.last_snapshot_epoch_sec
    ));

    for (name, calls, usec) in command_stats {
        out.push_str(&format!(
            "fedis_command_calls{{command=\"{}\"}} {}\n",
            name, calls
        ));
        out.push_str(&format!(
            "fedis_command_usec{{command=\"{}\"}} {}\n",
            name, usec
        ));
    }
    out
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
    let mut writer = writer_half;
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
                let elapsed_usec = started.elapsed().as_micros() as u64;
                let elapsed_ms = elapsed_usec / 1000;
                executor.record_command_stats(&command, elapsed_usec);
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
