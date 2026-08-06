// iedb/src/main.rs
use clap::Parser;
use std::path::PathBuf;

#[cfg(any(feature = "agent", feature = "server"))]
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "iedb")]
struct Cli {
    #[arg(long, default_value = "mix")]
    mode: String,

    #[arg(long, default_value = "iedb.toml")]
    config: String,
}

/// Agent mode entry point: registers with the iedb server, runs the
/// in-memory buffer + WAL pipeline, snapshot scheduler, and serves
/// the local HTTP API (POST /write, GET /query, GET /health).
#[cfg(feature = "agent")]
#[cfg(feature = "agent")]
async fn run_agent(
    config: Arc<iedb::config::Config>,
    #[cfg_attr(not(feature = "server"), allow(unused_variables))]
    _on_local_flush: Option<Arc<iedb::agent::flush::scheduler::LocalFlushCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    use iedb::agent::buffer::Buffer;
    use iedb::agent::flush::scheduler::SnapshotScheduler;
    use iedb::agent::query::QueryHandler;
    use iedb::agent::wal::wal_core::{apply_write_batch, WalManager};
    use iedb::agent::wal::WalOp;
    use iedb::agent::write::WriteHandler;
    use iedb::agent::AgentClient;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, Notify};

    let data_dir = config.data.dir.clone();
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(data_dir.join("wal"))?;
    std::fs::create_dir_all(data_dir.join("meta"))?;
    std::fs::create_dir_all(data_dir.join("staging"))?;

    // 2. 初始化 Buffer + WAL
    let buffer = Arc::new(Mutex::new(Buffer::new()));
    let wal_manager = Arc::new(Mutex::new(WalManager::new(&data_dir, &config.wal).await?));
    wal_manager.lock().await.replay(&buffer).await?;

    // 3. HTTP client（供快照上传用）
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // 4. 关闭信号
    let shutdown_signal = Arc::new(Notify::new());
    let ctrl_shutdown = shutdown_signal.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!("Failed to listen for Ctrl-C: {}", e);
        }
        tracing::info!("Shutdown signal received");
        ctrl_shutdown.notify_waiters();
    });

    // In mix mode (_on_local_flush is Some), skip remote registration/heartbeat
    let is_mix = _on_local_flush.is_some();
    let agent_client = if is_mix { None } else { Some(Arc::new(AgentClient::new(config.clone()))) };

    if !is_mix {
        if let Some(ref ac) = agent_client {
            match ac.register().await {
                Ok((_cfg, ver)) => tracing::info!("Agent registered, config_version={}", ver),
                Err(e) => tracing::warn!("Registration failed: {}, using local/cached config", e),
            }
        }
    }

    // 5. 心跳后台任务 (agent mode only — mix skips)
    let mut config_version: u64 = 0;
    if !is_mix {
        let hb_client = agent_client.clone().unwrap();
        let hb_buffer = buffer.clone();
        let hb_shutdown = shutdown_signal.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            let mut last_schema: std::collections::HashMap<String, (Vec<String>, Vec<(String, String)>)> =
                std::collections::HashMap::new();
        loop {
            tokio::select! {
                _ = hb_shutdown.notified() => break,
                _ = interval.tick() => {}
            }
            // 检测 schema 变更（对比上次快照，逻辑见 iedb::agent::compute_schema_changes）
            let schema_changes = {
                let buf = hb_buffer.lock().await;
                iedb::agent::compute_schema_changes(&buf, &mut last_schema)
            };
            match hb_client.heartbeat(config_version, schema_changes).await {
                Ok(Some(_update)) => {
                    // 增量配置更新：Task 9 实现具体应用逻辑，此处仅推进版本号
                    config_version += 1;
                    tracing::info!("Config updated to version {}", config_version);
                }
                Ok(None) => {}
                Err(e) => tracing::warn!("Heartbeat failed: {}", e),
            }
        }
    });
    } // end if !is_mix

    // 6. WAL flush 后台任务
    let wal_flush = wal_manager.clone();
    let wal_buffer = buffer.clone();
    let wal_shutdown = shutdown_signal.clone();
    let wal_interval = config.wal.flush_interval_secs;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(wal_interval));
        loop {
            tokio::select! {
                _ = wal_shutdown.notified() => break,
                _ = interval.tick() => {}
            }
            // C1 fix: scope wal guard release before acquiring buffer lock
            let ops = {
                let mut wal_guard = wal_flush.lock().await;
                match wal_guard.flush().await {
                    Ok(ops) => ops,
                    Err(e) => { tracing::error!(%e, "WAL flush failed"); continue; }
                }
            };
            // wal guard dropped here — buffer lock acquired below
            if !ops.is_empty() {
                let wal_seq = wal_flush.lock().await.current_sequence().saturating_sub(1);
                let mut buf = wal_buffer.lock().await;
                for op in &ops {
                    if let WalOp::Write(batch) = op {
                        apply_write_batch(&mut buf, batch, wal_seq);
                    }
                }
            }
        }
    });

    // 7. Snapshot scheduler 后台任务
    let mut snapshot_scheduler = SnapshotScheduler::new(
        buffer.clone(),
        wal_manager.clone(),
        config.clone(),
        client.clone(),
    );
    #[cfg(feature = "server")]
    if let Some(cb) = _on_local_flush {
        snapshot_scheduler.on_local_flush = Some(cb);
        tracing::info!("Mix mode: local flush callback installed");
    }
    let snap_shutdown = shutdown_signal.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = snap_shutdown.notified() => {}
            _ = snapshot_scheduler.run() => {}
        }
    });

    // 8. HTTP server（单端口：POST /write, GET /query, GET /health）
    let write_handler = Arc::new(WriteHandler {
        buffer: buffer.clone(),
        wal: wal_manager.clone(),
        config: config.clone(),
    });
    let query_handler = Arc::new(QueryHandler {
        buffer: buffer.clone(),
    });

    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", config.server.port).parse()?;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Agent listening on {}", addr);

    let server_shutdown = shutdown_signal.clone();
    loop {
        tokio::select! {
            _ = server_shutdown.notified() => break,
            result = listener.accept() => {
                let (stream, _) = result?;
                let io = TokioIo::new(stream);
                let write_handler = write_handler.clone();
                let query_handler = query_handler.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let w = write_handler.clone();
                        let q = query_handler.clone();
                        async move {
                            match (req.method(), req.uri().path()) {
                                (&Method::POST, "/write") => w.handle(req).await,
                                (&Method::GET, "/query") => q.handle(req).await,
                                (&Method::GET, "/health") => Ok(Response::new("ok".into())),
                                _ => Ok(Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body("not found".into())
                                    .expect("valid response")),
                            }
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                });
            }
        }
    }

    // I6: Graceful shutdown — final flush + snapshot before notifying tasks
    tracing::info!("Agent shutting down, performing final WAL flush...");
    if let Ok(mut wal) = wal_manager.try_lock() {
        if let Ok(ops) = wal.flush().await {
            let seq = wal.current_sequence().saturating_sub(1);
            let mut buf = buffer.lock().await;
            for op in &ops {
                if let WalOp::Write(batch) = op {
                    apply_write_batch(&mut buf, batch, seq);
                }
            }
        }
    }
    tracing::info!("Final WAL flush done");

    shutdown_signal.notify_waiters();
    tracing::info!("Agent shutdown complete");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.mode.as_str() {
        #[cfg(feature = "agent")]
        "agent" => {
            let config = Arc::new(iedb::config::Config::from_file(&cli.config)?);
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run_agent(config, None))
        }

        #[cfg(feature = "server")]
        "server" => {
            let config = Arc::new(iedb::config::Config::from_file(&cli.config)?);
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(iedb::server::run_server(config, true))
        }

        #[cfg(all(feature = "agent", feature = "server"))]
        "mix" => {
            let config = Arc::new(iedb::config::Config::from_file(&cli.config)?);

            // agent 与 server 都需要绑定本地 HTTP 端口：agent 的本地 API
            // （POST /write, GET /query）使用 [server].port + 1，server 的
            // API 使用 [server].port。agent 通过 [agent].server_url 上报
            // 到 server 端口。
            let mut agent_config = (*config).clone();
            agent_config.server.port += 1;
            let server_config = config.clone();
            tracing::info!(
                "Mix mode: agent API on port {}, server API on port {}",
                agent_config.server.port,
                server_config.server.port
            );

            // Mix mode metadata callback: after agent flush writes a local
            // parquet file, update the shared SQLite metadata DB so the
            // server's DataFusion engine discovers the new table/data.
            use iedb::agent::buffer::chunk::Table;
            use iedb::agent::flush::scheduler::LocalFlushCallback;
            let db_path = server_config
                .metadata
                .as_ref()
                .map(|m| m.db_path.clone())
                .unwrap_or_else(|| PathBuf::from("/var/lib/iedb/iedb.db"));
            let on_local_flush: Arc<LocalFlushCallback> = Arc::new(
                move |db_name: &str, table_name: &str, file_path: &std::path::Path, table: &Table| {
                    if let Ok(conn) = rusqlite::Connection::open(&db_path) {
                        let _ = conn.execute_batch("PRAGMA journal_mode=WAL;");
                        let _ = conn.execute(
                            "INSERT OR IGNORE INTO databases (name) VALUES (?1)",
                            rusqlite::params![db_name],
                        );
                        let _ = conn.execute(
                            "INSERT OR IGNORE INTO tables (db_name, table_name) VALUES (?1, ?2)",
                            rusqlite::params![db_name, table_name],
                        );
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        // I13 fix: update time range and row count from flushed chunks
                        let (time_min, time_max, chunk_rows) = {
                            let (mut tmin, mut tmax, mut cnt) = (i64::MAX, i64::MIN, 0usize);
                            for c in &table.chunks {
                                tmin = tmin.min(c.time_min);
                                tmax = tmax.max(c.time_max);
                                cnt += c.rows.len();
                            }
                            (if tmin == i64::MAX { 0 } else { tmin },
                             if tmax == i64::MIN { 0 } else { tmax },
                             cnt)
                        };
                        let _ = conn.execute(
                            "UPDATE tables SET time_min=MIN(COALESCE(time_min,?1),?1), time_max=MAX(COALESCE(time_max,?2),?2), total_rows=total_rows+?3, updated_at=?4 WHERE db_name=?5 AND table_name=?6",
                            rusqlite::params![time_min, time_max, chunk_rows as i64, now_ms, db_name, table_name],
                        );
                        for f in &table.schema.field_defs {
                            let _ = conn.execute(
                                "INSERT OR IGNORE INTO fields (table_id, name, value_type, is_tag) \
                                 SELECT id, ?1, ?2, 0 FROM tables WHERE db_name=?3 AND table_name=?4",
                                rusqlite::params![f.name, format!("{:?}", f.value_type), db_name, table_name],
                            );
                        }
                        for tk in &table.schema.tag_keys {
                            let _ = conn.execute(
                                "INSERT OR IGNORE INTO fields (table_id, name, value_type, is_tag) \
                                 SELECT id, ?1, 'String', 1 FROM tables WHERE db_name=?2 AND table_name=?3",
                                rusqlite::params![tk, db_name, table_name],
                            );
                        }
                        tracing::info!(
                            db = db_name, table = table_name, file = %file_path.display(),
                            "Mix mode: local flush metadata updated"
                        );
                    }
                },
            );

            let rt = tokio::runtime::Runtime::new()?;
            // 两个长驻 future 并发执行（run_agent 的 future 非 Send，
            // 不能 spawn 到多线程 runtime，用 select! 在同一任务内轮询）。
            // 任一完成（如 agent 收到 Ctrl-C 优雅退出）时，另一个被 drop。
            rt.block_on(async {
                tokio::select! {
                    r = run_agent(Arc::new(agent_config), Some(on_local_flush)) => r,
                    r = iedb::server::run_server(server_config, false) => r,
                }
            })
        }

        _ => {
            eprintln!(
                "Unsupported mode: {}. Available modes depend on compiled features.",
                cli.mode
            );
            std::process::exit(1);
        }
    }
}
