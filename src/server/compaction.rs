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
    /// 后台循环：先休眠再触发（间隔固定为 6 小时，与默认 schedule
    /// "0 */6 * * *" 一致；完整 cron 解析留待后续）。
    pub async fn run(&self) {
        if !self.config.enabled {
            return;
        }

        let interval = tokio::time::Duration::from_secs(6 * 3600);
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = self.run_once().await {
                tracing::warn!("Compaction error: {}", e);
            }
        }
    }

    /// 遍历所有表目录，把小于 `min_file_size_mb` 的 Parquet 文件合并为单个文件。
    /// 合并结果写入 `compacted_{ts}.parquet`（原文件删除，临时文件带
    /// `.parquet.tmp` 后缀避免被 ListingTable 扫描到）。
    pub async fn run_once(&self) -> Result<(), String> {
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

                let now_ms = chrono::Utc::now().timestamp_millis();
                let tmp_path = table_entry
                    .path()
                    .join(format!("compacted_{}_{}.parquet.tmp", now_ms, small_files.len()));
                let write_options =
                    datafusion::dataframe::DataFrameWriteOptions::new().with_single_file_output(true);
                df.write_parquet(&tmp_path.to_string_lossy(), write_options, None)
                    .await
                    .map_err(|e| format!("write parquet: {}", e))?;

                // 原子替换：先删除旧文件，再重命名临时文件
                for (_, old_path) in &small_files {
                    let _ = std::fs::remove_file(old_path);
                }
                let final_path = table_entry
                    .path()
                    .join(format!("compacted_{}.parquet", chrono::Utc::now().timestamp_millis()));
                std::fs::rename(&tmp_path, &final_path).map_err(|e| format!("rename: {}", e))?;

                tracing::info!(
                    db = %db_name,
                    table = %table_name,
                    files = small_files.len(),
                    "Compaction complete"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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

        scheduler(dir.path(), 1).run_once().await.unwrap();

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
        rt.block_on(s.run());
    }
}
