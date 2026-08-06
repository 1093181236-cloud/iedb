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
}

impl SnapshotScheduler {
    pub fn new(
        buffer: Arc<Mutex<Buffer>>,
        wal: Arc<Mutex<WalManager>>,
        config: Arc<Config>,
        client: Client,
    ) -> Self {
        let staging_dir = config.data.dir.join("staging");
        SnapshotScheduler {
            buffer,
            wal,
            config,
            client,
            staging_dir,
            on_local_flush: None,
        }
    }

    /// Run the background snapshot + memory protection loop.
    pub async fn run(&self) {
        let snapshot_interval = Duration::from_secs(self.config.snapshot_interval_secs() as u64);
        let memory_check_interval = Duration::from_secs(5);
        let mut last_snapshot = Instant::now();

        loop {
            tokio::time::sleep(memory_check_interval).await;

            // Check memory pressure
            let total_bytes = {
                let buf = self.buffer.lock().await;
                buf.total_estimated_size()
            };

            let memory_limit = self.config.memory_limit_bytes();
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
                        // On failure: chunks stay in memory, WAL stays, staging has parquet
                        // Memory protection will handle spill if needed
                    }
                }
            }
        }
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
                flush_chunks_to_parquet(&table, &chunk_refs).map_err(|e| format!("parquet write: {}", e))?;

            // Upload
            // In the new config, the agent registers against the iedb server via
            // `config.agent.server_url`, which is also the upload target.
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

            let upload_result = match self.config.flush.backend.as_str() {
                "local" => {
                    // local backend: write parquet directly to disk, no upload
                    let file_path = self.config.data.dir
                        .join(db_name)
                        .join(table_name)
                        .join(format!("local_{}.parquet", chunks.first().map(|c| c.chunk_time).unwrap_or(0)));
                    if let Some(parent) = file_path.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| format!("local mkdir: {}", e))?;
                    }
                    let result = std::fs::write(&file_path, &parquet_data)
                        .map_err(|e| format!("local write: {}", e));
                    if result.is_ok() {
                        if let Some(ref cb) = self.on_local_flush {
                            cb(db_name, table_name, &file_path, &table);
                        }
                    }
                    result
                }
                "s3" => {
                    let s3_cfg = self.config.s3.as_ref().ok_or("S3 config missing")?;
                    let key = s3_upload::s3_key(
                        agent_id,
                        db_name,
                        table_name,
                        chunks.first().map(|c| c.time_min).unwrap_or(0),
                    );
                    s3_upload::upload_to_s3(&self.client, s3_cfg, &key, &parquet_data).await
                }
                _ => {
                    // Default: HTTP upload
                    match http_upload::upload_parquet(
                        &self.client,
                        server_url,
                        db_name,
                        table_name,
                        &parquet_data,
                        None,
                        agent_id,
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
            };

            match upload_result {
                Ok(()) => {
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

                    flushed_count += 1;
                }
                Err(e) => {
                    // Failure: save to staging
                    tracing::warn!(
                        db = %db_name,
                        table = %table_name,
                        error = %e,
                        "Upload failed, saving to staging"
                    );
                    http_upload::staging_save(&self.staging_dir, db_name, table_name, &parquet_data)
                        .map_err(|e| format!("staging save: {}", e))?;
                    // chunk stays in memory, WAL stays
                }
            }
        }

        Ok(flushed_count)
    }
}

#[cfg(test)]
mod tests {

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
}
