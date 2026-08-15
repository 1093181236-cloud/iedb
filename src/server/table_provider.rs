// Registers Parquet directories as external DataFusion tables.
// 目录布局: {data_dir}/{db}/{table}/*.parquet → 表名 `{db}.{table}`。
use crate::server::query_engine::QueryEngine;
use datafusion::catalog::MemorySchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use std::path::Path;
use std::sync::Arc;

pub struct TableProvider;

impl TableProvider {
    /// 启动时从 data_dir 注册所有已存在的表（{db}/{table} 目录）。
    /// 已注册的表会被跳过，因此可安全地重复调用（幂等）。
    pub async fn register_all(engine: &QueryEngine, data_dir: &Path) -> Result<(), String> {
        if !data_dir.exists() {
            return Ok(());
        }
        for db_entry in std::fs::read_dir(data_dir).map_err(|e| format!("read dir: {}", e))? {
            let db_entry = db_entry.map_err(|e| format!("dir entry: {}", e))?;
            if !db_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let db_name = db_entry.file_name().to_string_lossy().to_string();

            for table_entry in
                std::fs::read_dir(db_entry.path()).map_err(|e| format!("read table dir: {}", e))?
            {
                let table_entry = table_entry.map_err(|e| format!("table entry: {}", e))?;
                if !table_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let table_name = table_entry.file_name().to_string_lossy().to_string();
                let qualified = format!("{}.{}", db_name, table_name);

                if engine.ctx().table_exist(&qualified).unwrap_or(false) {
                    continue;
                }
                Self::register_table_dir(engine, &db_name, &table_name, &table_entry.path())
                    .await?;
            }
        }
        Ok(())
    }

    /// 写入后增量添加单个文件：注册其所在表目录。
    /// ListingTable 每次扫描都会重新列出目录，因此无需逐文件注册。
    pub async fn add_file(
        engine: &QueryEngine,
        db: &str,
        table: &str,
        file_path: &Path,
    ) -> Result<(), String> {
        let qualified = format!("{}.{}", db, table);
        if !engine.ctx().table_exist(&qualified).unwrap_or(false) {
            let dir = file_path.parent().ok_or("file has no parent dir")?;
            Self::register_table_dir(engine, db, table, dir).await?;
        }
        Ok(())
    }

    async fn register_table_dir(
        engine: &QueryEngine,
        db: &str,
        table: &str,
        dir: &Path,
    ) -> Result<(), String> {
        // DF 40 的 register_table 要求 schema 已存在于 catalog 中：
        // 先把 db 注册为默认 catalog 下的 schema（已存在则跳过，避免覆盖）
        let catalog_name = engine.ctx().copied_config().options().catalog.default_catalog.clone();
        let catalog = engine
            .ctx()
            .catalog(&catalog_name)
            .ok_or("default catalog missing")?;
        if !catalog.schema_names().iter().any(|s| s == db) {
            catalog
                .register_schema(db, Arc::new(MemorySchemaProvider::new()))
                .map_err(|e| format!("register schema {}: {}", db, e))?;
        }

        let qualified = format!("{}.{}", db, table);
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet");
        // 目录 URL 需要以 "/" 结尾，object store 才把它当作前缀来列出
        let table_path = ListingTableUrl::parse(&format!("file://{}/", dir.display()))
            .map_err(|e| format!("url: {}", e))?;
        let config = ListingTableConfig::new(table_path).with_listing_options(options);
        // DF 40 的 ListingTable 要求显式 schema：从目录中的文件推断
        let config = config
            .infer_schema(&engine.ctx().state())
            .await
            .map_err(|e| format!("infer schema: {}", e))?;
        let table = ListingTable::try_new(config)
            .map_err(|e| format!("listing table: {}", e))?;
        engine
            .ctx()
            .register_table(&qualified, Arc::new(table))
            .map_err(|e| format!("register {}: {}", qualified, e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::test_util::write_test_parquet;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_register_all_discovers_tables_and_is_idempotent() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("metrics").join("cpu");
        std::fs::create_dir_all(&table_dir).unwrap();
        write_test_parquet(&table_dir.join("a.parquet"));
        write_test_parquet(&table_dir.join("b.parquet"));

        let engine = QueryEngine::new(100, 10, 4);
        TableProvider::register_all(&engine, dir.path()).await.unwrap();
        assert!(engine.ctx().table_exist("metrics.cpu").unwrap());

        // 重复注册不会报错
        TableProvider::register_all(&engine, dir.path()).await.unwrap();
        assert!(engine.ctx().table_exist("metrics.cpu").unwrap());
    }

    #[tokio::test]
    async fn test_register_all_missing_dir_is_ok() {
        let dir = tempdir().unwrap();
        let engine = QueryEngine::new(100, 10, 4);
        TableProvider::register_all(&engine, &dir.path().join("nope")).await.unwrap();
    }

    #[tokio::test]
    async fn test_add_file_registers_parent_table() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("metrics").join("cpu").join("a.parquet");
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        write_test_parquet(&file_path);

        let engine = QueryEngine::new(100, 10, 4);
        TableProvider::add_file(&engine, "metrics", "cpu", &file_path).await.unwrap();
        assert!(engine.ctx().table_exist("metrics.cpu").unwrap());

        // 已注册 → 再次 add_file 是 no-op
        TableProvider::add_file(&engine, "metrics", "cpu", &file_path).await.unwrap();
    }
}
