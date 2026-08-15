// Background compaction: merges small Parquet files into larger ones using
// DataFusion, then atomically swaps them into place.
use crate::config::CompactionConfig;
use crate::server::metadata_store::MetadataStore;
use datafusion::prelude::{col, SessionContext};
use std::path::PathBuf;
use std::sync::Arc;

pub struct CompactionScheduler {
    pub data_dir: PathBuf,
    pub metadata: Arc<MetadataStore>,
    pub config: CompactionConfig,
}

impl CompactionScheduler {
    /// 后台循环：从 schedule 解析间隔（cron 格式 "0 */N * * *"，提取 N 小时）。
    /// 响应 `shutdown`（server 的 Notify）——只在两个合并周期之间退出，
    /// 不打断进行中的 run_once。
    pub async fn run(&self, shutdown: std::sync::Arc<tokio::sync::Notify>) {
        if !self.config.enabled {
            return;
        }

        let hours = parse_schedule_hours(&self.config.schedule);
        let interval = tokio::time::Duration::from_secs((hours * 3600) as u64);
        tracing::info!("Compaction scheduler: every {}h", hours);
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    tracing::info!("Compaction scheduler shutting down");
                    break;
                }
                _ = tokio::time::sleep(interval) => {
                    if let Err(e) = self.run_once().await {
                        tracing::warn!("Compaction error: {}", e);
                    }
                }
            }
        }
    }

    /// 遍历所有表目录，把小于 `min_file_size_mb` 的 Parquet 文件合并为单个文件。
    /// 合并结果写入 `compacted_{hash}_{n}.parquet`（原文件删除，临时文件带
    /// `.parquet.tmp` 后缀避免被 ListingTable 扫描到）。合并后的原文件名
    /// 写入墓碑表，丢失响应的 ingest 重传据此跳过。
    pub async fn run_once(&self) -> Result<(), String> {
        // Prune tombstones older than 7 days on each run.
        let cutoff = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000;
        let _ = self.metadata.prune_tombstones(cutoff).await;
        let min_size = (self.config.min_file_size_mb * 1024 * 1024) as u64;
        let target_size = (self.config.target_file_size_mb * 1024 * 1024) as u64;

        for db_entry in std::fs::read_dir(&self.data_dir).map_err(|e| format!("read dir: {}", e))? {
            let db_entry = db_entry.map_err(|e| format!("entry: {}", e))?;
            if !db_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let db_name = db_entry.file_name().to_string_lossy().to_string();

            for table_entry in
                std::fs::read_dir(db_entry.path()).map_err(|e| format!("read table: {}", e))?
            {
                let table_entry = table_entry.map_err(|e| format!("entry: {}", e))?;
                if !table_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let table_name = table_entry.file_name().to_string_lossy().to_string();

                // 收集小文件（跳过已 compacted 的文件）
                let mut small_files: Vec<(u64, PathBuf)> = Vec::new();
                for file_entry in
                    std::fs::read_dir(table_entry.path()).map_err(|e| format!("read files: {}", e))?
                {
                    let file_entry = file_entry.map_err(|e| format!("file entry: {}", e))?;
                    let name = file_entry.file_name().to_string_lossy().to_string();
                    let is_parquet = file_entry
                        .path()
                        .extension()
                        .map_or(false, |e| e == "parquet");
                    if file_entry
                        .file_type()
                        .map(|t| t.is_file())
                        .unwrap_or(false)
                        && is_parquet
                        && !name.starts_with("compacted_")
                    {
                        let meta = file_entry.metadata().map_err(|e| format!("meta: {}", e))?;
                        small_files.push((meta.len(), file_entry.path()));
                    }
                }

                small_files.retain(|(size, _)| *size < min_size);
                let total_small: u64 = small_files.iter().map(|(s, _)| s).sum();
                // 文件太少，或合计已经达到目标大小 → 无需合并
                if small_files.len() <= 1 || total_small >= target_size {
                    continue;
                }
                small_files.sort_by_key(|(_, p)| p.to_string_lossy().to_string());

                // DataFusion 合并
                let ctx = SessionContext::new();
                let paths: Vec<String> = small_files
                    .iter()
                    .map(|(_, p)| format!("file://{}", p.display()))
                    .collect();
                let mut df = ctx
                    .read_parquet(paths, Default::default())
                    .await
                    .map_err(|e| format!("read parquet: {}", e))?;

                // 按 time 排序（表没有 time 列时跳过排序）
                if df.schema().field_with_name(None, "time").is_ok() {
                    df = df
                        .sort(vec![col("time").sort(true, true)])
                        .map_err(|e| format!("sort: {}", e))?;
                }

                // Deterministic output name: FNV-1a over the sorted input
                // filenames. If a crash happens between rename and deleting
                // the originals, the next cycle sees the same input set and
                // overwrites the same compacted file instead of stacking a
                // duplicate (which recount would then double-count).
                let input_hash = fnv1a_of_filenames(&small_files);
                let tmp_path = table_entry
                    .path()
                    .join(format!("compacted_{}_{}.parquet.tmp", input_hash, small_files.len()));
                let write_options =
                    datafusion::dataframe::DataFrameWriteOptions::new().with_single_file_output(true);
                df.write_parquet(&tmp_path.to_string_lossy(), write_options, None)
                    .await
                    .map_err(|e| format!("write parquet: {}", e))?;

                // I7 fix: rename first, then delete originals (crash-safe)
                let final_path = table_entry
                    .path()
                    .join(format!("compacted_{}_{}.parquet", input_hash, small_files.len()));
                std::fs::rename(&tmp_path, &final_path).map_err(|e| format!("rename: {}", e))?;
                for (_, old_path) in &small_files {
                    let _ = std::fs::remove_file(old_path);
                }

                // Tombstone the merged-away files: their rows now live only
                // in the compacted output, so a lost-response ingest retry
                // must not re-create them (it would duplicate the rows).
                let names: Vec<String> = small_files
                    .iter()
                    .map(|(_, p)| p.file_name().unwrap_or_default().to_string_lossy().into_owned())
                    .collect();
                if let Err(e) = self.metadata.tombstone_files(&names).await {
                    tracing::warn!(db = %db_name, table = %table_name, "tombstone merged files: {}", e);
                }

                tracing::info!(
                    db = %db_name,
                    table = %table_name,
                    files = small_files.len(),
                    "Compaction complete"
                );

                // 后置：合并后文件集合变化（删旧 + 写新），用替换语义重算
                // 该表的时间范围和总行数。update_stats 是累加语义，会重复计数。
                if let Err(e) = self.metadata.recount_table(&db_name, &table_name, &self.data_dir).await {
                    tracing::warn!(db = %db_name, table = %table_name, "compaction recount: {}", e);
                }
            }
        }
        Ok(())
    }
}

/// FNV-1a hash over the sorted input filenames — a deterministic fingerprint
/// of the merge's input set. Same inputs → same name (crash-replay
/// overwrites); different inputs → different name.
fn fnv1a_of_filenames(files: &[(u64, PathBuf)]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for (_, path) in files {
        for b in path.file_name().unwrap_or_default().to_string_lossy().bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0x7c; // separator between names
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Parse cron-like "0 */N * * *" to extract N hours. Falls back to 6.
fn parse_schedule_hours(schedule: &str) -> u64 {
    for part in schedule.split_whitespace() {
        if part.starts_with("*/") {
            return part[2..].parse::<u64>().unwrap_or(6);
        }
    }
    6
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schedule() {
        assert_eq!(parse_schedule_hours("0 */6 * * *"), 6);
        assert_eq!(parse_schedule_hours("0 */1 * * *"), 1);
        assert_eq!(parse_schedule_hours("0 */12 * * *"), 12);
        assert_eq!(parse_schedule_hours("bad"), 6);
    }
}

#[cfg(test)]
mod tests_compaction {
    use super::*;
    use crate::server::db::Db;
    use crate::server::test_util::write_test_parquet;
    use tempfile::tempdir;

    fn scheduler(data_dir: &std::path::Path, min_file_size_mb: u64) -> CompactionScheduler {
        let db_path = data_dir.join("test.db");
        let db = Db::open(&db_path).unwrap();
        CompactionScheduler {
            data_dir: data_dir.to_path_buf(),
            metadata: Arc::new(MetadataStore::new(Arc::new(db))),
            config: CompactionConfig {
                enabled: true,
                schedule: "0 */6 * * *".into(),
                min_file_size_mb,
                target_file_size_mb: 16,
                max_concurrent: 2,
            },
        }
    }

    /// 3 个文件 × 3 行 = 9 行
    fn seed_table(dir: &std::path::Path, db: &str, table: &str, n: usize) {
        let table_dir = dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir).unwrap();
        for i in 0..n {
            write_test_parquet(&table_dir.join(format!("file{}.parquet", i)));
        }
    }

    fn files_in(dir: &std::path::Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect()
    }

    #[tokio::test]
    async fn test_run_once_merges_small_files() {
        let dir = tempdir().unwrap();
        seed_table(dir.path(), "metrics", "cpu", 3);

        let s = scheduler(dir.path(), 1);
        // Pre-seed the metadata with the pre-compaction state (9 rows).
        s.metadata
            .update_stats(
                "metrics", "cpu",
                1000, 3000, 9,
                &[("usage".to_string(), "DOUBLE".to_string())],
                &[],
            )
            .await
            .unwrap();

        s.run_once().await.unwrap();

        let table_dir = dir.path().join("metrics").join("cpu");
        let files = files_in(&table_dir);
        assert_eq!(files.len(), 1);
        assert!(files[0].starts_with("compacted_") && files[0].ends_with(".parquet"));

        // 合并后的文件包含全部 9 行
        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(
                vec![format!("file://{}", table_dir.join(&files[0]).display())],
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(df.count().await.unwrap(), 9);

        // 后置步骤：metadata 用替换语义重算 — 行数必须仍是 9，
        // 累加语义会变成 18（重复计数）。
        let detail = s.metadata.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.row_count, 9, "compaction must not double-count rows");
        assert_eq!(detail.time_min, Some(1000));
        assert_eq!(detail.time_max, Some(3000));
    }

    /// Crash-replay: if the process dies between rename and deleting the
    /// originals, the next cycle sees the same input set — the deterministic
    /// name must make it overwrite the existing compacted file instead of
    /// stacking a duplicate (which recount would double-count).
    #[tokio::test]
    async fn test_run_once_crash_replay_does_not_duplicate() {
        let dir = tempdir().unwrap();
        seed_table(dir.path(), "metrics", "cpu", 3);

        let s = scheduler(dir.path(), 1);
        s.metadata
            .update_stats("metrics", "cpu", 1000, 3000, 9, &[], &[])
            .await
            .unwrap();

        s.run_once().await.unwrap();
        let table_dir = dir.path().join("metrics").join("cpu");
        let first: Vec<String> = files_in(&table_dir);
        assert_eq!(first.len(), 1);
        assert!(first[0].starts_with("compacted_"));

        // Simulate the crash window: the originals are still on disk.
        seed_table(dir.path(), "metrics", "cpu", 3);
        s.run_once().await.unwrap();

        let second: Vec<String> = files_in(&table_dir);
        assert_eq!(second.len(), 1, "replay must overwrite, not stack a duplicate");
        assert_eq!(second[0], first[0], "same input set must produce the same compacted name");

        // Stats must not double-count.
        let detail = s.metadata.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.row_count, 9);
    }

    #[tokio::test]
    async fn test_run_once_skips_large_files() {
        let dir = tempdir().unwrap();
        seed_table(dir.path(), "metrics", "cpu", 3);

        // min_file_size_mb = 0 → min_size = 0，任何文件都不算小 → 不合并
        scheduler(dir.path(), 0).run_once().await.unwrap();

        let table_dir = dir.path().join("metrics").join("cpu");
        let files = files_in(&table_dir);
        assert_eq!(files.len(), 3);
        assert!(files.iter().all(|f| f.starts_with("file")));
    }

    #[tokio::test]
    async fn test_run_once_single_file_noop() {
        let dir = tempdir().unwrap();
        seed_table(dir.path(), "metrics", "cpu", 1);

        scheduler(dir.path(), 1).run_once().await.unwrap();

        let table_dir = dir.path().join("metrics").join("cpu");
        let files = files_in(&table_dir);
        assert_eq!(files.len(), 1);
        assert!(files[0].starts_with("file"));
    }

    #[tokio::test]
    async fn test_run_once_missing_data_dir_is_error() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();
        let s = CompactionScheduler {
            data_dir: dir.path().join("nope"),
            metadata: Arc::new(MetadataStore::new(Arc::new(db))),
            config: CompactionConfig {
                enabled: true,
                schedule: "0 */6 * * *".into(),
                min_file_size_mb: 1,
                target_file_size_mb: 16,
                max_concurrent: 2,
            },
        };
        let err = s.run_once().await.unwrap_err();
        assert!(err.contains("read dir"));
    }

    #[test]
    fn test_run_disabled_returns_immediately() {
        let dir = tempdir().unwrap();
        let mut s = scheduler(dir.path(), 1);
        s.config.enabled = false;
        // run() 是 async 且永不返回，禁用时应立即结束
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(s.run(std::sync::Arc::new(tokio::sync::Notify::new())));
    }

    /// A shutdown signal must return the run loop promptly (it only
    /// responds between merge cycles, never mid-cycle).
    #[tokio::test]
    async fn test_run_returns_on_shutdown() {
        let dir = tempdir().unwrap();
        let mut s = scheduler(dir.path(), 1);
        // 1 小时间隔：若不响应 shutdown，测试会因无数据可合并而长期挂起
        s.config.schedule = "0 */1 * * *".into();
        let shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
        let s2 = shutdown.clone();
        let handle = tokio::spawn(async move { s.run(s2).await });

        // 给循环一点时间进入 sleep，然后发关闭信号
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        shutdown.notify_waiters();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        assert!(
            result.is_ok(),
            "run() must return after the shutdown signal"
        );
    }
}
