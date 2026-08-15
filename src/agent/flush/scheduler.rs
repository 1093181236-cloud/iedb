use crate::agent::buffer::Buffer;
use crate::agent::buffer::chunk::Table;
use crate::config::Config;
use crate::agent::flush::http_upload::{self, UploadError};
use crate::agent::flush::parquet_writer::flush_chunks_to_parquet;
use crate::agent::flush::s3_upload;
use crate::agent::wal::wal_core::WalManager;
use reqwest::Client;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing;

/// Snapshot status exposed to the agent status endpoint.
#[derive(Debug, Clone)]
pub struct SnapshotStatus {
    pub last_at: Option<i64>,       // Unix ms timestamp of last snapshot
    pub upload_ok: bool,            // Whether last upload succeeded
    pub last_size_bytes: u64,       // Size of last uploaded parquet
    pub next_in_secs: i64,          // Seconds until next scheduled snapshot
}

/// Callback invoked after a parquet file is written locally (mix mode).
/// Arguments: db_name, table_name, file_path, table_schema
pub type LocalFlushCallback = dyn Fn(&str, &str, &Path, &Table) + Send + Sync;

pub struct SnapshotScheduler {
    pub buffer: Arc<Mutex<Buffer>>,
    pub wal: Arc<Mutex<WalManager>>,
    pub config: Arc<Config>,
    pub client: Client,
    pub staging_dir: PathBuf,
    pub on_local_flush: Option<Arc<LocalFlushCallback>>,
    last_snapshot: tokio::sync::Mutex<SnapshotStatus>,
    started_at: std::time::Instant,
    /// Optional runtime-hot config; when present its snapshot_interval /
    /// memory_limit override the static config (server changes them live).
    pub hot: Option<Arc<crate::agent::hot_config::HotConfig>>,
}

impl SnapshotScheduler {
    pub fn new(
        buffer: Arc<Mutex<Buffer>>,
        wal: Arc<Mutex<WalManager>>,
        config: Arc<Config>,
        client: Client,
    ) -> Self {
        let staging_dir = config.data.dir.join("staging");
        let snapshot_interval_secs = config.snapshot_interval_secs();
        SnapshotScheduler {
            buffer,
            wal,
            config,
            client,
            staging_dir,
            on_local_flush: None,
            last_snapshot: tokio::sync::Mutex::new(SnapshotStatus {
                last_at: None,
                upload_ok: true,
                last_size_bytes: 0,
                next_in_secs: snapshot_interval_secs,
            }),
            started_at: std::time::Instant::now(),
            hot: None,
        }
    }

    /// Effective snapshot interval: hot config when attached, else static.
    pub fn current_snapshot_interval(&self) -> i64 {
        self.hot
            .as_ref()
            .map_or_else(|| self.config.snapshot_interval_secs(), |h| h.snapshot_interval_secs())
    }

    /// Effective memory limit: hot config when attached, else static.
    pub fn current_memory_limit(&self) -> usize {
        self.hot
            .as_ref()
            .map_or_else(|| self.config.memory_limit_bytes(), |h| h.memory_limit_bytes())
    }

    /// Run the background snapshot + memory protection + staging retry loop.
    /// snapshot_interval / memory_limit are re-read every iteration so
    /// hot config updates take effect within one cycle (≤5s).
    pub async fn run(&self) {
        let memory_check_interval = Duration::from_secs(5);
        let staging_retry_interval_ticks = 6; // 6 × 5s = 30s
        let mut last_snapshot = Instant::now();
        let mut tick: u64 = 0;

        // Immediate staging retry at startup: after a crash the staged file
        // may be the only copy (chunk removed, WAL cleaned), so recover it
        // before the first snapshot cycle.
        if let Err(e) = self.retry_staging().await {
            tracing::warn!(error = %e, "Staging retry at startup failed");
        }

        loop {
            tokio::time::sleep(memory_check_interval).await;
            tick += 1;

            // Staging retry every 30s
            if tick % staging_retry_interval_ticks == 0 {
                if let Err(e) = self.retry_staging().await {
                    tracing::warn!(error = %e, "Staging retry failed");
                }
            }

            // Check memory pressure
            let total_bytes = {
                let buf = self.buffer.lock().await;
                buf.total_estimated_size()
            };

            let memory_limit = self.current_memory_limit();
            let snapshot_interval =
                Duration::from_secs(self.current_snapshot_interval().max(1) as u64);
            let should_force = total_bytes >= memory_limit;
            let should_timed = last_snapshot.elapsed() >= snapshot_interval;

            if should_force || should_timed {
                if should_force {
                    tracing::warn!(
                        total_bytes = total_bytes,
                        limit = memory_limit,
                        "Memory limit reached, forcing snapshot"
                    );
                }

                match self.do_snapshot().await {
                    Ok(n) => {
                        tracing::info!(chunks_flushed = n, "Snapshot complete");
                        last_snapshot = Instant::now();
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Snapshot failed");
                        // On failure the chunks were handed to staging (durable
                        // copy fsynced) and the WAL cleaned — retry_staging
                        // owns re-upload.
                    }
                }
            }
        }
    }

    /// Return the last snapshot status (for the status API endpoint).
    pub async fn last_snapshot_status(&self) -> SnapshotStatus {
        let mut status = self.last_snapshot.lock().await.clone();
        let elapsed = self.started_at.elapsed().as_secs() as i64;
        let interval = self.current_snapshot_interval().max(1);
        // Approximate seconds until next scheduled snapshot
        let since_last = status.last_at
            .map(|ts| (chrono::Utc::now().timestamp_millis() - ts) / 1000)
            .unwrap_or(elapsed);
        status.next_in_secs = (interval - since_last % interval).max(0);
        status
    }

    /// Execute one snapshot cycle.
    async fn do_snapshot(&self) -> Result<usize, String> {
        let snapshot_interval_ns = self.config.snapshot_interval_secs() * 1_000_000_000;
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let end_time_marker = ((now_ns - snapshot_interval_ns) / snapshot_interval_ns)
            * snapshot_interval_ns;

        // C5 fix: collect chunk_time values instead of positional indices
        let mut chunks_to_flush: Vec<(String, String, Vec<i64>)> = Vec::new();

        {
            let buf = self.buffer.lock().await;
            for (db_name, tables) in &buf.databases {
                for (table_name, table) in tables {
                    let chunk_times: Vec<i64> = table
                        .chunks
                        .iter()
                        .filter(|c| c.chunk_time < end_time_marker)
                        .map(|c| c.chunk_time)
                        .collect();
                    if !chunk_times.is_empty() {
                        chunks_to_flush.push((db_name.clone(), table_name.clone(), chunk_times));
                    }
                }
            }
        }

        let mut flushed_count = 0;

        for (db_name, table_name, chunk_times) in &chunks_to_flush {
            let chunks: Vec<crate::agent::buffer::chunk::Chunk> = {
                let buf = self.buffer.lock().await;
                let table = buf.get_table(db_name, table_name).ok_or("table not found")?;
                chunk_times
                    .iter()
                    .filter_map(|ct| table.chunks.iter().find(|c| c.chunk_time == *ct).cloned())
                    .collect()
            };

            let chunk_refs: Vec<&crate::agent::buffer::chunk::Chunk> = chunks.iter().collect();

            let table_for_schema = {
                let buf = self.buffer.lock().await;
                buf.get_table(db_name, table_name).cloned()
            };

            let table = table_for_schema.ok_or("table not found")?;
            let parquet_data =
                flush_chunks_to_parquet(&table, &chunk_refs, None, None)
                    .map_err(|e| format!("parquet write: {}", e))?;

            // Upload — shared helper so the staging retry loop re-uploads
            // with identical semantics (backend, chunk_time, agent id).
            let upload_result = self
                .upload_bytes(
                    db_name,
                    table_name,
                    chunks.first().map(|c| c.chunk_time).unwrap_or(0),
                    chunks.first().map(|c| c.time_min).unwrap_or(0),
                    &parquet_data,
                    Some(&table),
                )
                .await;

            match upload_result {
                Ok(()) => {
                    // Update status for monitoring
                    {
                        let mut status = self.last_snapshot.lock().await;
                        status.last_at = Some(chrono::Utc::now().timestamp_millis());
                        status.upload_ok = true;
                        status.last_size_bytes = parquet_data.len() as u64;
                    }
                    // Data is safe on the server — hand the chunks off.
                    self.complete_chunk_handoff(db_name, table_name, &chunk_times)
                        .await?;
                    flushed_count += 1;
                }
                Err(e) => {
                    // Update status
                    {
                        let mut status = self.last_snapshot.lock().await;
                        status.upload_ok = false;
                        status.last_size_bytes = parquet_data.len() as u64;
                    }
                    // Failure: save to staging (fsynced before returning), THEN
                    // hand the chunks off. The staging file is the durable copy,
                    // so the buffer can release the chunk and the WAL can be
                    // cleaned — the 30s staging retry loop owns re-upload.
                    tracing::warn!(
                        db = %db_name,
                        table = %table_name,
                        error = %e,
                        "Upload failed, handing chunk to staging"
                    );
                    http_upload::staging_save(
                        &self.staging_dir,
                        db_name,
                        table_name,
                        chunks.first().map(|c| c.chunk_time).unwrap_or(0),
                        &parquet_data,
                    )
                    .map_err(|e| format!("staging save: {}", e))?;
                    self.complete_chunk_handoff(db_name, table_name, &chunk_times)
                        .await?;
                }
            }
        }

        Ok(flushed_count)
    }

    /// Remove the flushed chunks from the buffer, clean the WAL up to the
    /// safe sequence, and persist the snapshot meta (fsynced). Shared by the
    /// success path (data lives on the server) and the failure path (data
    /// lives in staging). Must only be called after the data's durable copy
    /// exists elsewhere.
    async fn complete_chunk_handoff(
        &self,
        db_name: &str,
        table_name: &str,
        chunk_times: &[i64],
    ) -> Result<(), String> {
        // C5 fix: remove chunks by chunk_time, not positional index
        // I2 fix: track snapshot sequence for WAL cleanup
        let snapshot_wal_seq = {
            let mut buf = self.buffer.lock().await;
            // Remove chunks by chunk_time value
            if let Some(table) = buf.get_table_mut(db_name, table_name) {
                table.chunks.retain(|c| !chunk_times.contains(&c.chunk_time));
            }

            // Compute safe wal seq
            let mut min_wal = u64::MAX;
            for (_, tables) in &buf.databases {
                for (_, t) in tables {
                    for c in &t.chunks {
                        if c.min_wal_seq < min_wal {
                            min_wal = c.min_wal_seq;
                        }
                    }
                }
            }
            if min_wal == u64::MAX {
                // I2 fix: buffer is empty, use current_seq - 1 for cleanup
                // Fall back to computing from WAL state
                self.wal.lock().await.current_sequence().saturating_sub(1)
            } else {
                min_wal.saturating_sub(1)
            }
        };

        // I3 fix: write metadata with explicit fsync
        let meta = serde_json::json!({
            "flushed_wal_seq": snapshot_wal_seq,
            "snapshot_ts": chrono::Utc::now().to_rfc3339(),
        });
        let meta_path = self.config.data.dir.join("meta").join("last_snapshot.json");
        let meta_str = serde_json::to_string(&meta)
            .map_err(|e| format!("meta serialize: {}", e))?;

        // Open file explicitly, write, sync_all, then sync directory
        let mut f = std::fs::File::create(&meta_path)
            .map_err(|e| format!("meta create: {}", e))?;
        f.write_all(meta_str.as_bytes())
            .map_err(|e| format!("meta write: {}", e))?;
        f.sync_all()
            .map_err(|e| format!("meta fsync: {}", e))?;
        // fsync the directory for durability
        if let Some(parent) = meta_path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        // Clean WAL
        self.wal.lock().await.cleanup(snapshot_wal_seq).await;

        Ok(())
    }

    /// Upload parquet bytes through the configured backend (http / s3 /
    /// local). Shared by the snapshot path and the staging retry loop.
    /// `table` is needed only by the local backend's on_local_flush callback.
    async fn upload_bytes(
        &self,
        db_name: &str,
        table_name: &str,
        chunk_time: i64,
        time_min: i64,
        parquet_data: &[u8],
        table: Option<&crate::agent::buffer::chunk::Table>,
    ) -> Result<(), String> {
        let server_url = self
            .config
            .agent
            .as_ref()
            .map(|a| a.server_url.as_str())
            .unwrap_or_default();
        let agent_id = self
            .config
            .agent
            .as_ref()
            .map(|a| a.id.as_str())
            .unwrap_or("default");

        match self.config.flush.backend.as_str() {
            "local" => {
                // local backend: write parquet directly to disk, no upload
                let file_path = self
                    .config
                    .data
                    .dir
                    .join(db_name)
                    .join(table_name)
                    .join(format!("local_{}.parquet", chunk_time));
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("local mkdir: {}", e))?;
                }
                let result = std::fs::write(&file_path, parquet_data)
                    .map_err(|e| format!("local write: {}", e));
                if result.is_ok() {
                    if let (Some(cb), Some(t)) = (&self.on_local_flush, table) {
                        cb(db_name, table_name, &file_path, t);
                    }
                }
                result
            }
            "s3" => {
                let s3_cfg = self.config.s3.as_ref().ok_or("S3 config missing")?;
                let key = s3_upload::s3_key(agent_id, db_name, table_name, time_min);
                s3_upload::upload_to_s3(&self.client, s3_cfg, &key, parquet_data).await
            }
            _ => {
                // Default: HTTP upload
                match http_upload::upload_parquet(
                    &self.client,
                    server_url,
                    db_name,
                    table_name,
                    parquet_data,
                    None,
                    agent_id,
                    Some(chunk_time),
                )
                .await
                {
                    Ok(()) => Ok(()),
                    Err(UploadError::Http(e)) => Err(e),
                    Err(UploadError::ServerError { status, body }) => {
                        Err(format!("HTTP {} {}", status, body))
                    }
                }
            }
        }
    }

    /// Scan the staging directory and re-attempt every staged file's upload.
    /// Successful uploads delete the staging file; failures keep it for the
    /// next round. Returns the number of files successfully delivered.
    pub async fn retry_staging(&self) -> Result<usize, String> {
        let mut delivered = 0usize;
        if !self.staging_dir.exists() {
            return Ok(0);
        }
        for db_entry in std::fs::read_dir(&self.staging_dir)
            .map_err(|e| format!("read staging: {}", e))?
        {
            let db_entry = db_entry.map_err(|e| format!("staging entry: {}", e))?;
            if !db_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let db_name = db_entry.file_name().to_string_lossy().to_string();

            for table_entry in std::fs::read_dir(db_entry.path())
                .map_err(|e| format!("read staging db: {}", e))?
            {
                let table_entry = table_entry.map_err(|e| format!("staging table: {}", e))?;
                if !table_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let table_name = table_entry.file_name().to_string_lossy().to_string();

                for file_entry in std::fs::read_dir(table_entry.path())
                    .map_err(|e| format!("read staging table: {}", e))?
                {
                    let file_entry = file_entry.map_err(|e| format!("staging file: {}", e))?;
                    let name = file_entry.file_name().to_string_lossy().to_string();
                    let chunk_time = match name
                        .strip_suffix(".parquet")
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(ct) => ct,
                        None => continue, // not a staging file we recognize
                    };
                    let data = match std::fs::read(file_entry.path()) {
                        Ok(d) => d,
                        Err(e) => {
                            tracing::warn!(path = %file_entry.path().display(), "read staged file: {}", e);
                            continue;
                        }
                    };
                    match self
                        .upload_bytes(&db_name, &table_name, chunk_time, chunk_time, &data, None)
                        .await
                    {
                        Ok(()) => {
                            let _ = std::fs::remove_file(file_entry.path());
                            delivered += 1;
                            tracing::info!(
                                db = %db_name,
                                table = %table_name,
                                chunk_time = chunk_time,
                                "Staging file re-uploaded"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                db = %db_name,
                                table = %table_name,
                                error = %e,
                                "Staging retry failed, keeping file"
                            );
                        }
                    }
                }
            }
        }
        Ok(delivered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compute the end_time_marker the same way do_snapshot does.
    fn compute_end_time_marker(now_ns: i64, snapshot_interval_secs: i64) -> i64 {
        let interval_ns = snapshot_interval_secs * 1_000_000_000;
        ((now_ns - interval_ns) / interval_ns) * interval_ns
    }

    #[test]
    fn test_end_time_marker_10m_interval() {
        // snapshot_interval = 10m = 600s
        // Formula: floor((now - interval) / interval) * interval

        // now = 1200s: (1200-600)/600 * 600 = 1 * 600 = 600s
        let marker = compute_end_time_marker(1_200_000_000_000, 600);
        assert_eq!(marker, 600_000_000_000);

        // now = 1800s: (1800-600)/600 * 600 = 2 * 600 = 1200s
        let marker = compute_end_time_marker(1_800_000_000_000, 600);
        assert_eq!(marker, 1_200_000_000_000);

        // now = 590s: (590-600)/600 * 600 = 0 * 600 = 0 (not yet one full interval old)
        let marker = compute_end_time_marker(590_000_000_000, 600);
        assert_eq!(marker, 0);
    }

    #[test]
    fn test_end_time_marker_60s_interval() {
        // snapshot_interval = 60s

        // now = 120s: (120-60)/60 * 60 = 1 * 60 = 60s
        let marker = compute_end_time_marker(120_000_000_000, 60);
        assert_eq!(marker, 60_000_000_000);

        // now = 200s: (200-60)/60 * 60 = 2 * 60 = 120s
        let marker = compute_end_time_marker(200_000_000_000, 60);
        assert_eq!(marker, 120_000_000_000);
    }

    #[test]
    fn test_chunk_collection_only_selects_before_marker() {
        use crate::agent::buffer::Buffer;
        use crate::agent::buffer::chunk::{Chunk, Row};

        let mut buffer = Buffer::new();
        let table = buffer.get_or_create_table("testdb", "cpu");

        // Create chunks at 100s, 500s, 900s (in ns)
        let mut c1 = Chunk::new(100_000_000_000);
        c1.rows.push(Row { time: 100, tag_values: vec![], field_values: vec![] });
        table.chunks.push(c1);

        let mut c2 = Chunk::new(500_000_000_000);
        c2.rows.push(Row { time: 500, tag_values: vec![], field_values: vec![] });
        table.chunks.push(c2);

        let mut c3 = Chunk::new(900_000_000_000);
        c3.rows.push(Row { time: 900, tag_values: vec![], field_values: vec![] });
        table.chunks.push(c3);

        // end_time_marker = 600s = 600_000_000_000 ns
        let end_time_marker: i64 = 600_000_000_000;

        let selected: Vec<i64> = table
            .chunks
            .iter()
            .filter(|c| c.chunk_time < end_time_marker)
            .map(|c| c.chunk_time)
            .collect();

        // Only chunks at 100s and 500s should be selected
        assert_eq!(selected.len(), 2);
        assert_eq!(selected, vec![100_000_000_000, 500_000_000_000]);
    }

    #[test]
    fn test_chunk_collection_none_if_all_after_marker() {
        use crate::agent::buffer::Buffer;
        use crate::agent::buffer::chunk::{Chunk, Row};

        let mut buffer = Buffer::new();
        let table = buffer.get_or_create_table("testdb", "cpu");

        let mut c1 = Chunk::new(800_000_000_000);
        c1.rows.push(Row { time: 800, tag_values: vec![], field_values: vec![] });
        table.chunks.push(c1);

        let mut c2 = Chunk::new(900_000_000_000);
        c2.rows.push(Row { time: 900, tag_values: vec![], field_values: vec![] });
        table.chunks.push(c2);

        // end_time_marker = 600s
        let end_time_marker: i64 = 600_000_000_000;

        let selected: Vec<i64> = table
            .chunks
            .iter()
            .filter(|c| c.chunk_time < end_time_marker)
            .map(|c| c.chunk_time)
            .collect();

        assert!(selected.is_empty());
    }

    /// Upload failure must hand the chunk off to staging: the staging file
    /// is created (deterministic name), the chunk leaves the buffer, and the
    /// WAL is cleaned — freeing memory and leaving staging as the only copy.
    #[tokio::test]
    async fn test_failed_upload_stages_removes_chunk_and_cleans_wal() {
        use crate::agent::buffer::Buffer;
        use crate::agent::buffer::chunk::{FieldType, FieldValue, Row};
        use crate::agent::wal::wal_core::WalManager;
        use crate::agent::wal::{WalOp, WriteBatch};
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;
        use std::sync::Arc;

        // 1. mock server that always returns 500
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(|_req| async move {
                        Ok::<_, hyper::Error>(
                            hyper::Response::builder()
                                .status(500)
                                .body("boom".to_string())
                                .unwrap(),
                        )
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, svc)
                    .await;
                });
            }
        });

        // 2. temp data dir
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();

        // 3. WAL: one flushed file (seq 1) holding three write ops
        let wal_cfg = crate::config::WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 100_000,
        };
        let wal = Arc::new(Mutex::new(WalManager::new(&data_dir, &wal_cfg).await.unwrap()));
        {
            let mut w = wal.lock().await;
            for _ in 0..3 {
                w.buffer_op(WalOp::Write(WriteBatch {
                    db_name: "db1".into(),
                    table_name: "tbl1".into(),
                    chunk_time: 0,
                    field_names: vec!["v".into()],
                    tag_keys: vec![],
                    rows: vec![],
                }))
                .unwrap();
            }
            w.flush().await.unwrap(); // writes wal file seq 1
        }

        // 4. buffer with one chunk (chunk_time=0, three rows, wal seqs 1..=3)
        let buffer = Arc::new(Mutex::new(Buffer::new()));
        {
            let mut buf = buffer.lock().await;
            let table = buf.get_or_create_table("db1", "tbl1");
            table.schema.ensure_field("v", FieldType::F64);
            let chunk = table.get_or_create_chunk(0);
            for i in 1..=3i64 {
                chunk.insert(
                    Row {
                        time: i * 100,
                        tag_values: vec![],
                        field_values: vec![Some(FieldValue::F64(i as f64))],
                    },
                    i as u64,
                );
            }
        }

        // 5. config: 10m snapshot interval (chunk_time=0 is far in the past)
        let toml = format!(
            r#"
            [server]
            port = 8080
            [data]
            dir = "{}"
            [flush]
            snapshot_interval = "10m"
            backend = "http"
            memory_limit = "512MB"
            [agent]
            id = "test-agent"
            server_url = "http://{}"
            "#,
            data_dir.display(),
            addr
        );
        let config: crate::config::Config = toml::from_str(&toml).unwrap();
        let scheduler = SnapshotScheduler::new(
            buffer.clone(),
            wal.clone(),
            Arc::new(config),
            reqwest::Client::new(),
        );

        // 6. run one snapshot cycle
        scheduler.do_snapshot().await.unwrap();

        // asserts: staging file with deterministic chunk_time name
        let staging_file = scheduler.staging_dir.join("db1").join("tbl1").join("0.parquet");
        assert!(staging_file.exists(), "staging file must exist after failed upload");

        // chunk removed from buffer
        {
            let buf = buffer.lock().await;
            let table = buf.get_table("db1", "tbl1").unwrap();
            assert!(table.chunks.is_empty(), "chunk must leave the buffer after staging handoff");
        }

        // snapshot meta written with the cleaned WAL seq
        let meta_path = data_dir.join("meta").join("last_snapshot.json");
        assert!(meta_path.exists(), "snapshot meta must be written on the failure path too");
        let meta: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(meta["flushed_wal_seq"], 1);

        // WAL file removed — data now lives only in staging
        let wal_files = std::fs::read_dir(data_dir.join("wal"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
            .count();
        assert_eq!(wal_files, 0, "WAL must be cleaned after staging handoff");
    }

    /// Hot config must override the static config for the effective values.
    #[tokio::test]
    async fn test_current_values_follow_hot_config() {
        use crate::agent::buffer::Buffer;
        use std::sync::Arc;

        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let wal_cfg = crate::config::WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 100_000,
        };
        let wal = Arc::new(Mutex::new(crate::agent::wal::wal_core::WalManager::new(&data_dir, &wal_cfg).await.unwrap()));
        let buffer = Arc::new(Mutex::new(Buffer::new()));

        let toml = format!(
            r#"
            [server]
            port = 8080
            [data]
            dir = "{}"
            [flush]
            snapshot_interval = "10m"
            backend = "http"
            memory_limit = "512MB"
            "#,
            data_dir.display()
        );
        let config: crate::config::Config = toml::from_str(&toml).unwrap();
        let mut scheduler = SnapshotScheduler::new(
            buffer,
            wal,
            Arc::new(config),
            reqwest::Client::new(),
        );

        // Without hot config: static values
        assert_eq!(scheduler.current_snapshot_interval(), 600);
        assert_eq!(scheduler.current_memory_limit(), 512 * 1024 * 1024);

        // With hot config: hot values win
        let hot = Arc::new(crate::agent::hot_config::HotConfig::from_config(
            &scheduler.config,
        ));
        scheduler.hot = Some(hot.clone());
        hot.apply_update(&serde_json::json!({
            "flush": {"snapshot_interval": "30s", "memory_limit": "256MB"}
        }));
        assert_eq!(scheduler.current_snapshot_interval(), 30);
        assert_eq!(scheduler.current_memory_limit(), 256 * 1024 * 1024);

        // A further update is picked up immediately (no restart)
        hot.apply_update(&serde_json::json!({"flush": {"snapshot_interval": "2m"}}));
        assert_eq!(scheduler.current_snapshot_interval(), 120);

        // The status endpoint must reflect the hot interval too
        let status = scheduler.last_snapshot_status().await;
        assert!(status.next_in_secs <= 120 && status.next_in_secs >= 0,
            "next_snapshot_in_secs must follow the hot interval, got {}", status.next_in_secs);
    }

    /// A staging retry must upload the file and delete it on success.
    #[tokio::test]
    async fn test_staging_retry_uploads_and_deletes_on_success() {
        use crate::agent::buffer::Buffer;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;
        use std::sync::Arc;

        // mock server: 200 and record the request path
        let seen_path: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let seen = seen_path.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let seen = seen.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| {
                        let seen = seen.clone();
                        async move {
                            let uri = format!("{}?{}", req.uri().path(), req.uri().query().unwrap_or(""));
                            *seen.lock().await = Some(uri);
                            Ok::<_, hyper::Error>(
                                hyper::Response::builder()
                                    .status(200)
                                    .body("ok".to_string())
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, svc)
                    .await;
                });
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        // seed a staging file: {staging}/{db}/{table}/{chunk_time}.parquet
        let staging_file = data_dir.join("staging").join("db1").join("tbl1").join("123.parquet");
        std::fs::create_dir_all(staging_file.parent().unwrap()).unwrap();
        std::fs::write(&staging_file, b"parquet bytes").unwrap();

        let wal_cfg = crate::config::WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 100_000,
        };
        let wal = Arc::new(Mutex::new(crate::agent::wal::wal_core::WalManager::new(&data_dir, &wal_cfg).await.unwrap()));
        let buffer = Arc::new(Mutex::new(Buffer::new()));

        let toml = format!(
            r#"
            [server]
            port = 8080
            [data]
            dir = "{}"
            [flush]
            snapshot_interval = "10m"
            backend = "http"
            memory_limit = "512MB"
            [agent]
            id = "test-agent"
            server_url = "http://{}"
            "#,
            data_dir.display(),
            addr
        );
        let config: crate::config::Config = toml::from_str(&toml).unwrap();
        let scheduler = SnapshotScheduler::new(
            buffer,
            wal,
            Arc::new(config),
            reqwest::Client::new(),
        );

        let retried = scheduler.retry_staging().await.unwrap();

        assert_eq!(retried, 1, "one staging file must be retried");
        assert!(!staging_file.exists(), "staging file must be deleted after successful upload");
        let seen = seen_path.lock().await.clone().unwrap();
        assert!(seen.contains("chunk_time=123"), "retry must pass chunk_time, got: {}", seen);
        assert!(seen.contains("db=db1") && seen.contains("measurement=tbl1"));
    }

    /// A failed retry must keep the staging file for the next round.
    #[tokio::test]
    async fn test_staging_retry_skips_unrecognized_filenames() {
        use crate::agent::buffer::Buffer;
        use std::sync::Arc;

        // No server needed — the unrecognized file must be skipped without
        // any upload attempt, and the valid file would be attempted. Use a
        // mock server to observe only the valid file reaches it.
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let seen = seen_clone.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().await.push(req.uri().to_string());
                            Ok::<_, hyper::Error>(
                                hyper::Response::builder()
                                    .status(200)
                                    .body("ok".to_string())
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, svc)
                    .await;
                });
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let staging_dir = data_dir.join("staging").join("db1").join("tbl1");
        std::fs::create_dir_all(&staging_dir).unwrap();
        // valid chunk_time-named file
        let valid = staging_dir.join("123.parquet");
        std::fs::write(&valid, b"parquet bytes").unwrap();
        // old-format timestamp-named file (pre chunk_time naming)
        let legacy = staging_dir.join("20260809_052051_190585464.parquet");
        std::fs::write(&legacy, b"old data").unwrap();

        let wal_cfg = crate::config::WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 100_000,
        };
        let wal = Arc::new(Mutex::new(crate::agent::wal::wal_core::WalManager::new(&data_dir, &wal_cfg).await.unwrap()));
        let buffer = Arc::new(Mutex::new(Buffer::new()));

        let toml = format!(
            r#"
            [server]
            port = 8080
            [data]
            dir = "{}"
            [flush]
            snapshot_interval = "10m"
            backend = "http"
            memory_limit = "512MB"
            [agent]
            id = "test-agent"
            server_url = "http://{}"
            "#,
            data_dir.display(),
            addr
        );
        let config: crate::config::Config = toml::from_str(&toml).unwrap();
        let scheduler = SnapshotScheduler::new(
            buffer,
            wal,
            Arc::new(config),
            reqwest::Client::new(),
        );

        let retried = scheduler.retry_staging().await.unwrap();

        assert_eq!(retried, 1, "only the chunk_time-named file is retried");
        assert!(!valid.exists(), "valid file must be delivered and deleted");
        assert!(legacy.exists(), "legacy-named file must be left untouched");
        let seen = seen.lock().await;
        assert_eq!(seen.len(), 1, "only one upload attempt for the valid file");
        assert!(seen[0].contains("chunk_time=123"));
    }

    /// A failed retry must keep the staging file for the next round.
    #[tokio::test]
    async fn test_staging_retry_keeps_file_on_failure() {
        use crate::agent::buffer::Buffer;
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;
        use std::sync::Arc;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(|_req| async move {
                        Ok::<_, hyper::Error>(
                            hyper::Response::builder()
                                .status(500)
                                .body("boom".to_string())
                                .unwrap(),
                        )
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, svc)
                    .await;
                });
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let staging_file = data_dir.join("staging").join("db1").join("tbl1").join("123.parquet");
        std::fs::create_dir_all(staging_file.parent().unwrap()).unwrap();
        std::fs::write(&staging_file, b"parquet bytes").unwrap();

        let wal_cfg = crate::config::WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 100_000,
        };
        let wal = Arc::new(Mutex::new(crate::agent::wal::wal_core::WalManager::new(&data_dir, &wal_cfg).await.unwrap()));
        let buffer = Arc::new(Mutex::new(Buffer::new()));

        let toml = format!(
            r#"
            [server]
            port = 8080
            [data]
            dir = "{}"
            [flush]
            snapshot_interval = "10m"
            backend = "http"
            memory_limit = "512MB"
            [agent]
            id = "test-agent"
            server_url = "http://{}"
            "#,
            data_dir.display(),
            addr
        );
        let config: crate::config::Config = toml::from_str(&toml).unwrap();
        let scheduler = SnapshotScheduler::new(
            buffer,
            wal,
            Arc::new(config),
            reqwest::Client::new(),
        );

        let retried = scheduler.retry_staging().await.unwrap();

        assert_eq!(retried, 0, "failed retry must not count as retried");
        assert!(staging_file.exists(), "staging file must be kept on failure");
    }
}
