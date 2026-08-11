// GET /api/v1/status — returns agent internal state as JSON.
use crate::agent::buffer::Buffer;
use crate::agent::flush::scheduler::SnapshotScheduler;
use crate::agent::system::SystemSampler;
use crate::agent::wal::wal_core::WalManager;
use crate::agent::AgentClient;
use crate::config::Config;
use hyper::{Request, Response, StatusCode};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

pub struct StatusHandler {
    pub buffer: Arc<Mutex<Buffer>>,
    pub wal: Arc<Mutex<WalManager>>,
    pub snapshot: Arc<SnapshotScheduler>,
    pub system: Arc<SystemSampler>,
    pub agent_client: Option<Arc<AgentClient>>,
    pub config: Arc<Config>,
    pub started_at: Instant,
}

impl StatusHandler {
    pub async fn handle<B>(&self, _req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: Send + Unpin + 'static,
    {
        let agent_cfg = self.config.agent.as_ref();
        let agent_id = agent_cfg.map(|c| c.id.as_str()).unwrap_or("unknown");
        let hostname = gethostname::gethostname().to_string_lossy().to_string();
        let uptime = self.started_at.elapsed().as_secs();

        // System info
        let sys = self.system.current().await;

        // Buffer info
        let (buf_json, db_count, total_rows, total_mem) = {
            let buf = self.buffer.lock().await;
            let mut databases = serde_json::Map::new();
            for (db_name, tables) in &buf.databases {
                let mut db_tables = serde_json::Map::new();
                for (tbl_name, table) in tables {
                    let (time_min, time_max, row_count) = {
                        let (mut tmin, mut tmax, mut cnt) = (i64::MAX, i64::MIN, 0usize);
                        for c in &table.chunks {
                            tmin = tmin.min(c.time_min);
                            tmax = tmax.max(c.time_max);
                            cnt += c.rows.len();
                        }
                        (
                            if tmin == i64::MAX { 0 } else { tmin },
                            if tmax == i64::MIN { 0 } else { tmax },
                            cnt,
                        )
                    };
                    let tag_keys = table.schema.tag_keys.clone();
                    let field_defs: Vec<Vec<String>> = table
                        .schema
                        .field_defs
                        .iter()
                        .map(|f| vec![f.name.clone(), format!("{:?}", f.value_type)])
                        .collect();
                    let mem_bytes = table.estimated_size();

                    db_tables.insert(tbl_name.clone(), serde_json::json!({
                        "row_count": row_count,
                        "chunk_count": table.chunks.len(),
                        "time_min": ms_to_iso(time_min),
                        "time_max": ms_to_iso(time_max),
                        "tag_keys": tag_keys,
                        "field_defs": field_defs,
                        "memory_bytes": mem_bytes,
                    }));
                }
                databases.insert(db_name.clone(), serde_json::json!({ "tables": db_tables }));
            }
            (
                serde_json::Value::Object(databases),
                buf.databases.len(),
                buf.databases.values().flat_map(|t| t.values()).map(|t| t.chunks.iter().map(|c| c.rows.len()).sum::<usize>()).sum::<usize>(),
                buf.total_estimated_size(),
            )
        };

        // WAL info
        let wal_info = {
            let wal = self.wal.lock().await;
            serde_json::json!({
                "current_sequence": wal.current_sequence(),
                "pending_ops": wal.pending_ops_count(),
                "data_dir_size_bytes": 0,
            })
        };

        // Snapshot info
        let snap = self.snapshot.last_snapshot_status().await;

        // Connection info
        let conn_info = if let Some(ref ac) = self.agent_client {
            let failures = ac.heartbeat_failures.load(std::sync::atomic::Ordering::Relaxed);
            serde_json::json!({
                "server_url": agent_cfg.map(|c| c.server_url.as_str()).unwrap_or(""),
                "registered": true,
                "config_version": 0,
                "last_heartbeat_at": null,
                "heartbeat_failures": failures,
            })
        } else {
            serde_json::json!({
                "server_url": null,
                "registered": false,
                "config_version": 0,
                "last_heartbeat_at": null,
                "heartbeat_failures": 0,
            })
        };

        let status = serde_json::json!({
            "agent": {
                "id": agent_id,
                "hostname": hostname,
                "arch": std::env::consts::ARCH,
                "version": env!("CARGO_PKG_VERSION"),
                "started_at": ms_to_iso(0),
                "uptime_secs": uptime,
            },
            "system": {
                "cpu_percent": (sys.cpu_percent * 10.0).round() / 10.0,
                "memory_total_mb": sys.memory_total_mb,
                "memory_used_mb": sys.memory_used_mb,
                "process_rss_mb": sys.process_rss_mb,
            },
            "buffer": {
                "database_count": db_count,
                "total_rows": total_rows,
                "total_memory_bytes": total_mem,
                "memory_limit_bytes": self.config.memory_limit_bytes(),
                "databases": buf_json,
            },
            "wal": wal_info,
            "snapshot": {
                "last_snapshot_at": snap.last_at.map(ms_to_iso),
                "last_upload_status": if snap.upload_ok { "ok" } else { "fail" },
                "last_upload_size_bytes": snap.last_size_bytes,
                "next_snapshot_in_secs": snap.next_in_secs,
            },
            "connection": conn_info,
        });

        let body = serde_json::to_string_pretty(&status).unwrap_or_else(|_| r#"{"error":"serialize"}"#.into());
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(body)
            .unwrap())
    }
}

fn ms_to_iso(ts_ms: i64) -> String {
    if ts_ms <= 0 {
        return "1970-01-01T00:00:00Z".into();
    }
    let secs = ts_ms / 1000;
    let nanos = ((ts_ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| format!("{}", ts_ms))
}
