// iedb/src/main.rs
use clap::Parser;

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
async fn run_agent(config: Arc<iedb::config::Config>) -> Result<(), Box<dyn std::error::Error>> {
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

    // 1. Agent 注册（获取运行配置）
    let agent_client = Arc::new(AgentClient::new(config.clone()));
    let (_remote_config, mut config_version) = match agent_client.register().await {
        Ok((cfg, ver)) => {
            tracing::info!("Agent registered, config_version={}", ver);
            (cfg, ver)
        }
        Err(e) => {
            tracing::warn!("Registration failed: {}, using local/cached config", e);
            (serde_json::Value::Null, 0u64)
        }
    };

    // 2. 初始化 Buffer + WAL
    let buffer = Arc::new(Mutex::new(Buffer::new()));
    let wal_manager = Arc::new(Mutex::new(WalManager::new(&data_dir, &config.wal).await?));
    wal_manager.lock().await.replay(&buffer).await?;

    // 3. HTTP client（供快照上传用）
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // 4. 关闭信号：Ctrl-C 触发 notify_waiters，所有后台任务与 HTTP server 退出
    let shutdown_signal = Arc::new(Notify::new());
    let ctrl_shutdown = shutdown_signal.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!("Failed to listen for Ctrl-C: {}", e);
        }
        tracing::info!("Shutdown signal received");
        ctrl_shutdown.notify_waiters();
    });

    // 5. 心跳后台任务
    let hb_client = agent_client.clone();
    let hb_buffer = buffer.clone();
    let hb_shutdown = shutdown_signal.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        // schema 快照对比（用于检测 schema 变更）
        let mut last_schema: std::collections::HashMap<String, (Vec<String>, Vec<(String, String)>)> =
            std::collections::HashMap::new();
        loop {
            tokio::select! {
                _ = hb_shutdown.notified() => break,
                _ = interval.tick() => {}
            }
            // 检测 schema 变更
            let mut schema_changes = Vec::new();
            {
                let buf = hb_buffer.lock().await;
                for (db_name, tables) in &buf.databases {
                    for (table_name, table) in tables {
                        let key = format!("{}.{}", db_name, table_name);
                        let tag_keys = table.schema.tag_keys.clone();
                        let field_defs: Vec<(String, String)> = table.schema
                            .field_defs
                            .iter()
                            .map(|f| (f.name.clone(), format!("{:?}", f.value_type)))
                            .collect();
                        let current = (tag_keys.clone(), field_defs.clone());
                        let changed = last_schema.get(&key).map_or(true, |prev| prev != &current);
                        if changed {
                            schema_changes.push(iedb::agent::SchemaChange {
                                db: db_name.clone(),
                                table: table_name.clone(),
                                tag_keys,
                                field_defs,
                            });
                            last_schema.insert(key, current);
                        }
                    }
                }
            }
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
            if let Ok(ops) = wal_flush.lock().await.flush().await {
                let mut buf = wal_buffer.lock().await;
                for op in &ops {
                    if let WalOp::Write(batch) = op {
                        apply_write_batch(&mut buf, batch, 0);
                    }
                }
            }
        }
    });

    // 7. Snapshot scheduler 后台任务
    let snapshot_scheduler = SnapshotScheduler::new(
        buffer.clone(),
        wal_manager.clone(),
        config.clone(),
        client.clone(),
    );
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

    // 优雅关闭：通知所有后台任务停止
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
            rt.block_on(run_agent(config))
        }

        #[cfg(feature = "server")]
        "server" => {
            let config = Arc::new(iedb::config::Config::from_file(&cli.config)?);
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(iedb::server::run_server(config))
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

            let rt = tokio::runtime::Runtime::new()?;
            // 两个长驻 future 并发执行（run_agent 的 future 非 Send，
            // 不能 spawn 到多线程 runtime，用 select! 在同一任务内轮询）。
            // 任一完成（如 agent 收到 Ctrl-C 优雅退出）时，另一个被 drop。
            rt.block_on(async {
                tokio::select! {
                    r = run_agent(Arc::new(agent_config)) => r,
                    r = iedb::server::run_server(server_config) => r,
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
