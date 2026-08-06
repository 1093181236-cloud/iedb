pub mod query_engine;
pub mod sql_api;
pub mod ingest_api;
pub mod agent_api;
pub mod agent_store;
pub mod metadata_api;
pub mod metadata_store;
pub mod table_provider;
pub mod compaction;
pub mod db;

#[cfg(test)]
mod test_util;

// Re-exports for the server wiring layer (main.rs / later tasks).
pub use agent_api::AgentApiHandler;
pub use ingest_api::IngestApiHandler;
pub use metadata_api::MetadataApiHandler;

use crate::config::Config;
use crate::server::agent_store::AgentStore;
use crate::server::compaction::CompactionScheduler;
use crate::server::db::Db;
use crate::server::metadata_store::MetadataStore;
use crate::server::query_engine::QueryEngine;
use crate::server::sql_api::SqlApiHandler;
use crate::server::table_provider::TableProvider;
use hyper::service::service_fn;
use hyper::Response;
use std::sync::Arc;

/// Server mode 入口：SQLite 元数据 → 存储层 → DataFusion 查询引擎 →
/// Parquet 表注册 → Compaction 后台任务 → HTTP API 服务。
pub async fn run_server(config: Arc<Config>, include_agent_api: bool) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = &config.data.dir;
    std::fs::create_dir_all(data_dir)?;

    // SQLite（agent / 元数据存储）
    let metadata_cfg = config
        .metadata
        .as_ref()
        .ok_or("missing [metadata] config section")?;
    let db = Arc::new(Db::open(&metadata_cfg.db_path)?);

    // 存储层
    let agent_store = Arc::new(AgentStore::new(db.clone()));
    let metadata = Arc::new(MetadataStore::new(db.clone()));

    // DataFusion 查询引擎 + 启动时注册已有 Parquet 表
    let query_cfg = config
        .query
        .as_ref()
        .ok_or("missing [query] config section")?;
    std::fs::create_dir_all(&query_cfg.data_dir)?;
    let engine = Arc::new(QueryEngine::new(
        query_cfg.max_rows,
        query_cfg.query_timeout_secs,
    ));
    TableProvider::register_all(&engine, &query_cfg.data_dir).await?;

    // Compaction 后台任务
    if let Some(compaction_cfg) = config.compaction.as_ref() {
        let scheduler = CompactionScheduler {
            data_dir: query_cfg.data_dir.clone(),
            metadata: metadata.clone(),
            config: compaction_cfg.clone(),
        };
        tokio::spawn(async move { scheduler.run().await; });
    }

    // HTTP handlers
    let agent_api: Option<Arc<AgentApiHandler>> = if include_agent_api {
        Some(Arc::new(AgentApiHandler { store: agent_store.clone() }))
    } else {
        None
    };
    let ingest_api = Arc::new(IngestApiHandler {
        data_dir: query_cfg.data_dir.clone(),
        metadata: metadata.clone(),
        max_body_bytes: config.server.max_body_bytes as usize,
        engine: Some(engine.clone()),
    });
    let sql_api = Arc::new(SqlApiHandler { engine: engine.clone(), data_dir: query_cfg.data_dir.clone() });
    let metadata_api = Arc::new(MetadataApiHandler {
        store: metadata.clone(),
    });

    let addr: std::net::SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .map_err(|e| format!("invalid bind address: {}", e))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Server listening on http://{}", addr);

    // I6: graceful shutdown via Ctrl-C
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_waiter = shutdown.notified();
    tokio::pin!(shutdown_waiter);
    {
        let s = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Server shutdown signal received");
            s.notify_waiters();
        });
    }

    loop {
        let (stream, _) = tokio::select! {
            _ = &mut shutdown_waiter => break,
            result = listener.accept() => result?,
        };
        let io = hyper_util::rt::TokioIo::new(stream);
        let agent_api = agent_api.clone();
        let ingest_api = ingest_api.clone();
        let sql_api = sql_api.clone();
        let metadata_api = metadata_api.clone();

        tokio::spawn(async move {
            let svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let agent = agent_api.clone();
                let ingest = ingest_api.clone();
                let sql = sql_api.clone();
                let metadata = metadata_api.clone();
                async move {
                    let path = req.uri().path().to_string();
                    match (req.method(), path.as_str()) {
                        (_, p) if p.starts_with("/api/v1/agents") => {
                            if let Some(ref a) = agent {
                                a.handle(req).await
                            } else {
                                Ok(Response::builder().status(404)
                                    .body(r#"{"error":"not found","code":"NOT_FOUND"}"#.into()).unwrap())
                            }
                        }
                        (_, "/api/v1/ingest/parquet") => ingest.handle(req).await,
                        (_, "/api/v1/query") => sql.handle(req).await,
                        (_, p) if p.starts_with("/api/v1/metadata") => metadata.handle(req).await,
                        (_, "/health") => Ok(hyper::Response::new("ok".into())),
                        _ => Ok(hyper::Response::builder()
                            .status(404)
                            .body("not found".into())
                            .unwrap()),
                    }
                }
            });
            let _ = hyper_util::server::conn::auto::Builder::new(
                hyper_util::rt::TokioExecutor::new(),
            )
            .serve_connection(io, svc)
            .await;
        });
    }

    // I6: graceful cleanup — let in-flight queries drain (bound by query_timeout_secs)
    tracing::info!("Server shutting down, draining in-flight requests...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    tracing::info!("Server shutdown complete");
    Ok(())
}
