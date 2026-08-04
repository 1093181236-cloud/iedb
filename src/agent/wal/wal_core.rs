use crate::agent::buffer::Buffer;
use crate::config::WalConfig;
use crate::agent::wal::{WalContents, WalFileSequenceNumber, WalOp, WriteBatch};
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tracing;

/// Manages WAL file creation, buffering, flushing, replay, and cleanup.
pub struct WalManager {
    wal_dir: PathBuf,
    meta_dir: PathBuf,
    current_seq: WalFileSequenceNumber,
    op_count: usize,
    op_limit: usize,
    pending_ops: Vec<WalOp>,
}

impl WalManager {
    /// Create a new WalManager with the given data directory and WAL config.
    ///
    /// Ensures the `wal` and `meta` subdirectories exist, then determines
    /// the next WAL sequence number by scanning on-disk files.
    pub async fn new(
        data_dir: &Path,
        config: &WalConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let wal_dir = data_dir.join("wal");
        let meta_dir = data_dir.join("meta");
        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&meta_dir)?;

        // Find max existing WAL seq and flushed seq
        let _flushed_wal_seq = Self::load_last_snapshot(&meta_dir);
        let max_existing = Self::max_wal_seq(&wal_dir);

        // Start from the next available sequence
        let next_seq = max_existing.map(|s| s + 1).unwrap_or(1);

        Ok(WalManager {
            wal_dir,
            meta_dir,
            current_seq: next_seq,
            op_count: 0,
            op_limit: config.max_write_buffer_ops,
            pending_ops: Vec::with_capacity(config.max_write_buffer_ops),
        })
    }

    /// Return the current WAL sequence number (next file to be written).
    pub fn current_sequence(&self) -> u64 {
        self.current_seq
    }

    /// Buffer a write op. Returns `BufferFull` error if over limit.
    pub fn buffer_op(&mut self, op: WalOp) -> Result<(), WalError> {
        if self.op_count >= self.op_limit {
            return Err(WalError::BufferFull(self.op_count));
        }
        self.op_count += 1;
        self.pending_ops.push(op);
        Ok(())
    }

    /// Block until the WAL file is persisted. Returns the ops to be applied to the Buffer.
    pub async fn flush(&mut self) -> Result<Vec<WalOp>, WalError> {
        if self.pending_ops.is_empty() {
            return Ok(Vec::new());
        }

        let ops = std::mem::take(&mut self.pending_ops);
        self.op_count = 0;

        let contents = WalContents {
            wal_file_number: self.current_seq,
            ops: ops.clone(),
            persist_timestamp_ms: chrono::Utc::now().timestamp_millis(),
        };

        let data = contents.serialize_to_file();
        let path = self.wal_file_path(self.current_seq);
        tokio::fs::write(&path, &data).await.map_err(|e| {
            WalError::WriteError(format!("write WAL {}: {}", self.current_seq, e))
        })?;

        tracing::debug!(
            seq = self.current_seq,
            ops = contents.ops.len(),
            bytes = data.len(),
            "WAL file flushed"
        );

        self.current_seq += 1;
        Ok(ops)
    }

    /// Replay WAL files after startup, applying their ops to the given buffer.
    ///
    /// Only replays WAL files with a sequence number greater than the
    /// flushed (snapshotted) sequence stored in `meta/last_snapshot.json`.
    pub async fn replay(
        &self,
        buffer: &Mutex<Buffer>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let flushed_seq = Self::load_last_snapshot(&self.meta_dir);
        let mut wal_files: Vec<(u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(&self.wal_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.ends_with(".wal") {
                if let Some(seq) = name_str
                    .strip_suffix(".wal")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    if seq > flushed_seq {
                        wal_files.push((seq, entry.path()));
                    }
                }
            }
        }
        wal_files.sort_by_key(|(seq, _)| *seq);

        for (seq, path) in &wal_files {
            let data = match tokio::fs::read(path).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(seq = seq, error = %e, "Skipping unreadable WAL file");
                    continue;
                }
            };
            let contents = match WalContents::deserialize_from_file(&data) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(seq = seq, error = %e, "Skipping corrupted WAL file");
                    continue;
                }
            };
            for op in &contents.ops {
                match op {
                    WalOp::Write(batch) => {
                        let mut buf = buffer.lock().await;
                        apply_write_batch(&mut buf, batch, *seq);
                    }
                    WalOp::Noop => {}
                }
            }
            tracing::info!(seq = seq, ops = contents.ops.len(), "WAL replayed");
        }

        tracing::info!(
            files = wal_files.len(),
            flushed_seq = flushed_seq,
            "WAL replay complete"
        );
        Ok(())
    }

    /// Clean up WAL files with a sequence number <= `through_seq`.
    pub async fn cleanup(&self, through_seq: WalFileSequenceNumber) {
        let entries = match fs::read_dir(&self.wal_dir) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Cannot read WAL dir for cleanup: {}", e);
                return;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(seq) = name_str
                .strip_suffix(".wal")
                .and_then(|s| s.parse::<u64>().ok())
            {
                if seq <= through_seq {
                    let _ = fs::remove_file(entry.path());
                    tracing::debug!(seq = seq, "WAL file cleaned up");
                }
            }
        }
    }

    fn wal_file_path(&self, seq: WalFileSequenceNumber) -> PathBuf {
        self.wal_dir.join(format!("{:020}.wal", seq))
    }

    fn load_last_snapshot(meta_dir: &Path) -> u64 {
        let path = meta_dir.join("last_snapshot.json");
        match fs::read_to_string(&path) {
            Ok(content) => {
                #[derive(serde::Deserialize)]
                struct SnapshotMeta {
                    flushed_wal_seq: u64,
                }
                serde_json::from_str::<SnapshotMeta>(&content)
                    .map(|m| m.flushed_wal_seq)
                    .unwrap_or(0)
            }
            Err(_) => 0,
        }
    }

    fn max_wal_seq(wal_dir: &Path) -> Option<u64> {
        let mut max_seq = None;
        if let Ok(entries) = fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if let Some(seq) = name_str
                    .strip_suffix(".wal")
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    max_seq = Some(max_seq.map_or(seq, |m: u64| m.max(seq)));
                }
            }
        }
        max_seq
    }
}

/// Errors that can occur during WAL operations.
#[derive(Debug)]
pub enum WalError {
    /// The WAL buffer has reached its configured capacity.
    BufferFull(usize),
    /// An I/O error occurred while writing a WAL file.
    WriteError(String),
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::BufferFull(n) => write!(f, "WAL buffer full with {} ops", n),
            WalError::WriteError(e) => write!(f, "WAL write error: {}", e),
        }
    }
}

impl std::error::Error for WalError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::buffer::chunk::{FieldType, FieldValue, Row};
    use crate::config::WalConfig;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_SEQ: AtomicU32 = AtomicU32::new(0);

    fn test_data_dir() -> PathBuf {
        let seq = TEST_SEQ.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("iedb_wal_test_{}_{}", std::process::id(), seq))
    }

    #[tokio::test]
    async fn test_buffer_op_count_and_limit() {
        let tmp = test_data_dir();
        let config = WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 5,
        };
        let mut wm = WalManager::new(&tmp, &config).await.unwrap();

        // Buffer 5 ops -- all should succeed
        for i in 0..5 {
            assert!(
                wm.buffer_op(WalOp::Noop).is_ok(),
                "op {} should succeed",
                i
            );
        }
        assert_eq!(wm.op_count, 5);
        assert_eq!(wm.pending_ops.len(), 5);

        // 6th op should return BufferFull
        let result = wm.buffer_op(WalOp::Noop);
        assert!(result.is_err());
        match result {
            Err(WalError::BufferFull(n)) => assert_eq!(n, 5),
            _ => panic!("expected BufferFull, got {:?}", result),
        }
        // op_count and pending_ops must not change on BufferFull
        assert_eq!(wm.op_count, 5);
        assert_eq!(wm.pending_ops.len(), 5);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_flush_resets_op_count_and_can_buffer_again() {
        let tmp = test_data_dir();
        let config = WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 5,
        };
        let mut wm = WalManager::new(&tmp, &config).await.unwrap();

        // Buffer to limit
        for _ in 0..5 {
            wm.buffer_op(WalOp::Noop).unwrap();
        }
        assert_eq!(wm.op_count, 5);

        // Flush returns the ops and resets state
        let ops = wm.flush().await.unwrap();
        assert_eq!(ops.len(), 5);
        // After flush, op_count and pending_ops are reset
        assert_eq!(wm.op_count, 0);
        assert!(wm.pending_ops.is_empty());

        // Can buffer again after flush
        assert!(wm.buffer_op(WalOp::Noop).is_ok());
        assert_eq!(wm.op_count, 1);
        assert_eq!(wm.pending_ops.len(), 1);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn test_flush_empty_buffer_returns_empty_vec() {
        let tmp = test_data_dir();
        let config = WalConfig {
            flush_interval_secs: 1,
            max_write_buffer_ops: 100,
        };
        let mut wm = WalManager::new(&tmp, &config).await.unwrap();

        let ops = wm.flush().await.unwrap();
        assert!(ops.is_empty());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ── Test 4: apply_write_batch ──────────────────────────────────────

    #[test]
    fn test_apply_write_batch_creates_table_in_buffer() {
        let mut buffer = Buffer::new();
        let batch = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 0,
            field_names: vec!["cpu".into()],
            tag_keys: vec!["host".into()],
            rows: vec![Row {
                time: 100,
                tag_values: vec!["srv01".into()],
                field_values: vec![Some(FieldValue::F64(0.5))],
            }],
        };

        apply_write_batch(&mut buffer, &batch, 1);

        // Table should exist
        assert!(buffer.get_table("testdb", "cpu").is_some());
        let table = buffer.get_table("testdb", "cpu").unwrap();
        assert_eq!(table.name, "cpu");
    }

    #[test]
    fn test_apply_write_batch_inserts_rows_into_correct_chunk() {
        let mut buffer = Buffer::new();
        let batch = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 42,
            field_names: vec!["cpu".into()],
            tag_keys: vec!["host".into()],
            rows: vec![
                Row {
                    time: 100,
                    tag_values: vec!["srv01".into()],
                    field_values: vec![Some(FieldValue::F64(0.5))],
                },
                Row {
                    time: 200,
                    tag_values: vec!["srv02".into()],
                    field_values: vec![Some(FieldValue::F64(0.8))],
                },
            ],
        };

        apply_write_batch(&mut buffer, &batch, 5);

        let table = buffer.get_table("testdb", "cpu").unwrap();
        assert_eq!(table.chunks.len(), 1);
        assert_eq!(table.chunks[0].chunk_time, 42);
        assert_eq!(table.chunks[0].row_count(), 2);
        assert_eq!(table.chunks[0].rows[0].time, 100);
        assert_eq!(table.chunks[0].rows[1].time, 200);

        // Check WAL seq bounds
        assert_eq!(table.chunks[0].min_wal_seq, 5);
        assert_eq!(table.chunks[0].max_wal_seq, 5);
    }

    #[test]
    fn test_apply_write_batch_evolves_schema_from_row_fields() {
        let mut buffer = Buffer::new();
        let batch = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 0,
            field_names: vec!["cpu".into(), "count".into(), "online".into()],
            tag_keys: vec!["host".into(), "region".into()],
            rows: vec![
                Row {
                    time: 100,
                    tag_values: vec!["srv01".into(), "us-east".into()],
                    field_values: vec![
                        Some(FieldValue::F64(0.5)),
                        Some(FieldValue::I64(100)),
                        Some(FieldValue::Bool(true)),
                    ],
                },
            ],
        };

        apply_write_batch(&mut buffer, &batch, 1);

        let table = buffer.get_table("testdb", "cpu").unwrap();

        // Schema should have field defs with real names (C2 fix)
        assert_eq!(table.schema.field_defs.len(), 3);
        assert_eq!(table.schema.field_defs[0].name, "cpu");
        assert_eq!(table.schema.field_defs[0].value_type, FieldType::F64);
        assert_eq!(table.schema.field_defs[1].name, "count");
        assert_eq!(table.schema.field_defs[1].value_type, FieldType::I64);
        assert_eq!(table.schema.field_defs[2].name, "online");
        assert_eq!(table.schema.field_defs[2].value_type, FieldType::Bool);

        // Tag keys should also be registered (I1 fix)
        assert_eq!(table.schema.tag_keys.len(), 2);
        assert_eq!(table.schema.tag_keys, vec!["host", "region"]);
    }

    #[test]
    fn test_apply_write_batch_builds_tag_index() {
        let mut buffer = Buffer::new();

        // Tag keys are passed through the batch now (I1 fix)
        let batch = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 0,
            field_names: vec![],
            tag_keys: vec!["host".into()],
            rows: vec![
                Row {
                    time: 100,
                    tag_values: vec!["srv01".into()],
                    field_values: vec![],
                },
                Row {
                    time: 200,
                    tag_values: vec!["srv02".into()],
                    field_values: vec![],
                },
                Row {
                    time: 300,
                    tag_values: vec!["srv01".into()],
                    field_values: vec![],
                },
            ],
        };

        apply_write_batch(&mut buffer, &batch, 1);

        let table = buffer.get_table("testdb", "cpu").unwrap();
        let chunk = &table.chunks[0];

        let host_index = chunk.tag_index.get("host").expect("tag_index should have 'host'");
        assert_eq!(host_index["srv01"], vec![0, 2]);
        assert_eq!(host_index["srv02"], vec![1]);
    }

    #[test]
    fn test_apply_write_batch_multiple_batches_same_table() {
        let mut buffer = Buffer::new();

        let batch1 = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 0,
            field_names: vec!["usage".into()],
            tag_keys: vec!["host".into()],
            rows: vec![Row {
                time: 100,
                tag_values: vec!["srv01".into()],
                field_values: vec![Some(FieldValue::F64(0.5))],
            }],
        };

        let batch2 = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 10,
            field_names: vec!["usage".into()],
            tag_keys: vec!["host".into()],
            rows: vec![Row {
                time: 200,
                tag_values: vec!["srv02".into()],
                field_values: vec![Some(FieldValue::F64(0.8))],
            }],
        };

        apply_write_batch(&mut buffer, &batch1, 1);
        apply_write_batch(&mut buffer, &batch2, 2);

        let table = buffer.get_table("testdb", "cpu").unwrap();
        assert_eq!(table.chunks.len(), 2);
        assert_eq!(table.chunks[0].chunk_time, 0);
        assert_eq!(table.chunks[1].chunk_time, 10);
        assert_eq!(table.chunks[0].row_count(), 1);
        assert_eq!(table.chunks[1].row_count(), 1);
    }

    #[test]
    fn test_apply_write_batch_tag_index_chunk_absolute_indices() {
        // C3 fix: verify tag index uses chunk-absolute indices when chunk already has rows
        let mut buffer = Buffer::new();

        // Batch 1: insert 3 rows into chunk_time=0
        let batch1 = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 0,
            field_names: vec!["usage".into()],
            tag_keys: vec!["host".into()],
            rows: vec![
                Row { time: 100, tag_values: vec!["srv01".into()], field_values: vec![Some(FieldValue::F64(0.5))] },
                Row { time: 200, tag_values: vec!["srv02".into()], field_values: vec![Some(FieldValue::F64(0.6))] },
                Row { time: 300, tag_values: vec!["srv01".into()], field_values: vec![Some(FieldValue::F64(0.7))] },
            ],
        };
        apply_write_batch(&mut buffer, &batch1, 1);

        // Batch 2: insert 2 more rows into the SAME chunk_time=0
        let batch2 = WriteBatch {
            db_name: "testdb".into(),
            table_name: "cpu".into(),
            chunk_time: 0,
            field_names: vec!["usage".into()],
            tag_keys: vec!["host".into()],
            rows: vec![
                Row { time: 400, tag_values: vec!["srv03".into()], field_values: vec![Some(FieldValue::F64(0.8))] },
                Row { time: 500, tag_values: vec!["srv01".into()], field_values: vec![Some(FieldValue::F64(0.9))] },
            ],
        };
        apply_write_batch(&mut buffer, &batch2, 2);

        let table = buffer.get_table("testdb", "cpu").unwrap();
        assert_eq!(table.chunks.len(), 1);
        let chunk = &table.chunks[0];
        assert_eq!(chunk.row_count(), 5);

        // Tag index must point to chunk-absolute indices
        let host_index = chunk.tag_index.get("host").expect("tag_index should have 'host'");

        // srv01 should be at indices 0, 2, 4 (absolute within chunk, not batch-relative)
        assert_eq!(host_index["srv01"], vec![0, 2, 4]);
        // srv02 at index 1
        assert_eq!(host_index["srv02"], vec![1]);
        // srv03 at index 3 (absolute, after the first 3 rows from batch1)
        assert_eq!(host_index["srv03"], vec![3]);
    }
}

/// Apply a `WriteBatch` to the in-memory buffer.
///
/// Ensures the target table and chunk exist, evolves the schema as needed,
/// inserts each row, and builds the tag index.
pub fn apply_write_batch(buffer: &mut Buffer, batch: &WriteBatch, wal_seq: u64) {
    let table = buffer.get_or_create_table(&batch.db_name, &batch.table_name);

    let chunk_time = batch.chunk_time;

    // Register tag keys in the table schema (I1 fix)
    for key in &batch.tag_keys {
        table.schema.ensure_tag_key(key);
    }

    // Ensure fields exist in the table schema with actual field names (C2 fix)
    for field_name in &batch.field_names {
        // Determine field type from the first row that has this field
        let field_idx = batch.field_names.iter().position(|n| n == field_name).unwrap_or(0);
        let field_type = batch.rows.iter()
            .find_map(|row| row.field_values.get(field_idx).and_then(|v| v.as_ref().map(|fv| fv.field_type())));
        if let Some(ft) = field_type {
            let _ = table.schema.ensure_field(field_name, ft);
        }
    }

    // Clone tag_keys to avoid concurrent mutable borrow of table and chunk
    let tag_keys = table.schema.tag_keys.clone();
    let chunk = table.get_or_create_chunk(chunk_time);

    // Compute base index for tag index entries (C3 fix)
    let base_idx = chunk.rows.len();

    for (row_idx, row) in batch.rows.iter().enumerate() {
        chunk.insert(row.clone(), wal_seq);

        // Build tag index inline (avoids borrow conflict with build_tag_index)
        for (key_idx, key) in tag_keys.iter().enumerate() {
            if let Some(value) = row.tag_values.get(key_idx) {
                chunk
                    .tag_index
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(base_idx + row_idx);
            }
        }
    }
}
