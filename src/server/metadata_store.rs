// Database / table / field metadata aggregation, backed by SQLite.
use crate::server::db::Db;
use rusqlite::params;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct TableSummary {
    pub name: String,
    pub row_count: i64,
    pub time_min: Option<i64>,
    pub time_max: Option<i64>,
    pub tag_keys: Vec<String>,
    pub field_count: i64,
}

#[derive(Debug, Serialize)]
pub struct TableDetail {
    pub database: String,
    pub table: String,
    pub tag_keys: Vec<String>,
    pub fields: Vec<FieldMeta>,
    pub time_min: Option<i64>,
    pub time_max: Option<i64>,
    pub row_count: i64,
    pub sources: Vec<String>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct FieldMeta {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: String,
}

#[derive(Clone)]
pub struct MetadataStore {
    db: Arc<Db>,
}

/// Read Parquet footer statistics: overall time range of the `time` column
/// (INT64, little-endian min/max), row count, and field type definitions.
/// Shared by ingest (per-file stats on upload) and compaction (per-file
/// stats when recounting a table).
pub fn read_parquet_stats(
    path: &std::path::Path,
) -> Result<(i64, i64, usize, Vec<(String, String)>), Box<dyn std::error::Error>> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let meta = reader.metadata();
    let file_meta = meta.file_metadata();
    let row_count = file_meta.num_rows() as usize;

    let mut time_min = i64::MAX;
    let mut time_max = i64::MIN;
    let mut field_defs = Vec::new();

    for row_group in meta.row_groups() {
        for col in row_group.columns() {
            let col_desc = col.column_descr();
            let name = col_desc.name().to_string();
            let col_path = col_desc.path().string();
            if col_path == "time" {
                if let Some(stats) = col.statistics() {
                    // parquet 52 API: min_bytes()/max_bytes() panic when unset,
                    // so guard with has_min_max_set() (min_bytes_opt is 53+)
                    if stats.has_min_max_set() {
                        let min_val =
                            i64::from_le_bytes(stats.min_bytes().try_into().unwrap_or([0; 8]));
                        let max_val =
                            i64::from_le_bytes(stats.max_bytes().try_into().unwrap_or([0; 8]));
                        time_min = time_min.min(min_val);
                        time_max = time_max.max(max_val);
                    }
                }
            } else {
                let type_str = format!("{:?}", col_desc.physical_type());
                field_defs.push((name, type_str));
            }
        }
    }

    Ok((
        if time_min == i64::MAX { 0 } else { time_min },
        if time_max == i64::MIN { 0 } else { time_max },
        row_count,
        field_defs,
    ))
}

impl MetadataStore {
    pub fn new(db: Arc<Db>) -> Self {
        MetadataStore { db }
    }

    /// 更新表的数据统计（ingest/flush/compaction 调用）
    pub async fn update_stats(
        &self, db_name: &str, table_name: &str,
        time_min: i64, time_max: i64, row_count: usize,
        field_defs: &[(String, String)], tag_keys: &[String],
    ) -> Result<(), String> {
        let conn = self.db.conn().lock().await;
        // Ensure db + table exist
        conn.execute("INSERT OR IGNORE INTO databases (name) VALUES (?1)", params![db_name])
            .map_err(|e| format!("ensure db: {}", e))?;
        conn.execute("INSERT OR IGNORE INTO tables (db_name, table_name) VALUES (?1, ?2)", params![db_name, table_name])
            .map_err(|e| format!("ensure table: {}", e))?;
        let table_id: i64 = conn
            .query_row("SELECT id FROM tables WHERE db_name=?1 AND table_name=?2", params![db_name, table_name], |r| r.get(0))
            .map_err(|e| format!("get table id: {}", e))?;

        let now_ms = chrono::Utc::now().timestamp_millis();

        conn.execute(
            "UPDATE tables SET time_min=MIN(COALESCE(time_min, ?1), ?1), time_max=MAX(COALESCE(time_max, ?2), ?2), total_rows=total_rows+?3, updated_at=?4 WHERE id=?5",
            params![time_min, time_max, row_count as i64, now_ms, table_id],
        )
        .map_err(|e| format!("update stats: {}", e))?;

        // UPSERT fields
        for (name, value_type) in field_defs {
            conn.execute(
                "INSERT OR IGNORE INTO fields (table_id, name, value_type, is_tag) VALUES (?1, ?2, ?3, 0)",
                params![table_id, name, value_type],
            )
            .ok();
        }
        for tag_key in tag_keys {
            conn.execute(
                "INSERT OR IGNORE INTO fields (table_id, name, value_type, is_tag) VALUES (?1, ?2, 'String', 1)",
                params![table_id, tag_key],
            )
            .ok();
        }

        Ok(())
    }

    /// 合并心跳上报的 schema（仅 schema 变更时调用）
    pub async fn merge_schema(
        &self, db_name: &str, table_name: &str, agent_id: &str,
        tag_keys: &[String], field_defs: &[(String, String)],
    ) -> Result<(), String> {
        let conn = self.db.conn().lock().await;
        conn.execute("INSERT OR IGNORE INTO databases (name) VALUES (?1)", params![db_name])
            .map_err(|e| format!("ensure db: {}", e))?;
        conn.execute("INSERT OR IGNORE INTO tables (db_name, table_name) VALUES (?1, ?2)", params![db_name, table_name])
            .map_err(|e| format!("ensure table: {}", e))?;
        let table_id: i64 = conn
            .query_row("SELECT id FROM tables WHERE db_name=?1 AND table_name=?2", params![db_name, table_name], |r| r.get(0))
            .map_err(|e| format!("get table id: {}", e))?;

        // 记录 agent→table 关系。此前错误被 .ok() 吞掉：外键约束失败
        // （agent 未注册）会静默丢失映射，federation 因此看不到该 agent。
        // 现在向上传播，由调用方记录日志/跳过，而不是无声失败。
        conn.execute(
            "INSERT OR IGNORE INTO agent_tables (agent_id, table_id) VALUES (?1, ?2)",
            params![agent_id, table_id],
        )
        .map_err(|e| format!("record agent table: {}", e))?;

        for (name, value_type) in field_defs {
            conn.execute(
                "INSERT OR IGNORE INTO fields (table_id, name, value_type, is_tag) VALUES (?1, ?2, ?3, 0)",
                params![table_id, name, value_type],
            )
            .ok();
        }
        for tag_key in tag_keys {
            conn.execute(
                "INSERT OR IGNORE INTO fields (table_id, name, value_type, is_tag) VALUES (?1, ?2, 'String', 1)",
                params![table_id, tag_key],
            )
            .ok();
        }
        Ok(())
    }

    pub async fn list_databases(&self) -> Result<Vec<String>, String> {
        let conn = self.db.conn().lock().await;
        let mut stmt = conn
            .prepare("SELECT name FROM databases ORDER BY name")
            .map_err(|e| format!("list dbs: {}", e))?;
        let names = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| format!("query dbs: {}", e))?;
        names
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("collect dbs: {}", e))
    }

    pub async fn list_tables(&self, db_name: &str) -> Result<Vec<TableSummary>, String> {
        let conn = self.db.conn().lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT t.table_name, t.total_rows, t.time_min, t.time_max,
                        (SELECT COUNT(*) FROM fields WHERE table_id=t.id AND is_tag=1),
                        (SELECT COUNT(*) FROM fields WHERE table_id=t.id AND is_tag=0)
                 FROM tables t WHERE t.db_name=?1 ORDER BY t.table_name",
            )
            .map_err(|e| format!("list tables: {}", e))?;
        let rows = stmt
            .query_map(params![db_name], |r| {
                let tag_keys_json: String = conn
                    .query_row(
                        "SELECT COALESCE(json_group_array(name), '[]') FROM fields WHERE table_id=(SELECT id FROM tables WHERE db_name=?1 AND table_name=?2) AND is_tag=1",
                        params![db_name, r.get::<_, String>(0)?],
                        |jr| jr.get(0),
                    )
                    .unwrap_or_else(|_| "[]".to_string());

                let tag_keys: Vec<String> = serde_json::from_str(&tag_keys_json).unwrap_or_default();

                Ok(TableSummary {
                    name: r.get(0)?, row_count: r.get(1)?,
                    time_min: r.get(2)?, time_max: r.get(3)?,
                    tag_keys,
                    field_count: r.get(5)?,
                })
            })
            .map_err(|e| format!("query tables: {}", e))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("collect tables: {}", e))
    }

    pub async fn get_table(&self, db_name: &str, table_name: &str) -> Result<Option<TableDetail>, String> {
        let conn = self.db.conn().lock().await;
        let result = conn.query_row(
            "SELECT id, time_min, time_max, total_rows, updated_at FROM tables WHERE db_name=?1 AND table_name=?2",
            params![db_name, table_name],
            |row| Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
            )),
        );
        match result {
            Ok((table_id, time_min, time_max, total_rows, updated_at)) => {
                // Get tag keys
                let mut tag_stmt = conn
                    .prepare("SELECT name FROM fields WHERE table_id=?1 AND is_tag=1 ORDER BY name")
                    .map_err(|e| format!("query tags: {}", e))?;
                let tag_keys: Vec<String> = tag_stmt
                    .query_map(params![table_id], |r| r.get::<_, String>(0))
                    .map_err(|e| format!("tags: {}", e))?
                    .filter_map(|r| r.ok())
                    .collect();

                // Get field defs
                let mut field_stmt = conn
                    .prepare("SELECT name, value_type FROM fields WHERE table_id=?1 AND is_tag=0 ORDER BY name")
                    .map_err(|e| format!("query fields: {}", e))?;
                let fields: Vec<FieldMeta> = field_stmt
                    .query_map(params![table_id], |r| Ok(FieldMeta { name: r.get(0)?, value_type: r.get(1)? }))
                    .map_err(|e| format!("fields: {}", e))?
                    .filter_map(|r| r.ok())
                    .collect();

                // Get sources
                let mut src_stmt = conn
                    .prepare("SELECT agent_id FROM agent_tables WHERE table_id=?1 ORDER BY agent_id")
                    .map_err(|e| format!("query sources: {}", e))?;
                let sources: Vec<String> = src_stmt
                    .query_map(params![table_id], |r| r.get::<_, String>(0))
                    .map_err(|e| format!("sources: {}", e))?
                    .filter_map(|r| r.ok())
                    .collect();

                Ok(Some(TableDetail {
                    database: db_name.to_string(),
                    table: table_name.to_string(),
                    tag_keys,
                    fields,
                    time_min,
                    time_max,
                    row_count: total_rows,
                    sources,
                    updated_at,
                }))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(format!("get table: {}", e)),
        }
    }

    /// Return agent IDs that contribute data to the given table.
    pub async fn list_table_agents(&self, db: &str, table: &str) -> Result<Vec<String>, String> {
        let conn = self.db.conn().lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT at.agent_id FROM agent_tables at
                 JOIN tables t ON t.id = at.table_id
                 WHERE t.db_name = ?1 AND t.table_name = ?2",
            )
            .map_err(|e| format!("list table agents: {}", e))?;
        let ids = stmt
            .query_map(rusqlite::params![db, table], |row| row.get::<_, String>(0))
            .map_err(|e| format!("list table agents query: {}", e))?;
        ids.collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("list table agents collect: {}", e))
    }

    /// Recompute a table's time range and total_rows by scanning every
    /// Parquet file currently in `{data_dir}/{db}/{table}`. REPLACE
    /// semantics — unlike update_stats this does not accumulate, so it is
    /// the correct call after compaction merges files (the merged file
    /// contains the same rows as its inputs).
    pub async fn recount_table(
        &self,
        db: &str,
        table: &str,
        data_dir: &std::path::Path,
    ) -> Result<(), String> {
        let table_dir = data_dir.join(db).join(table);
        let mut time_min = i64::MAX;
        let mut time_max = i64::MIN;
        let mut total_rows: i64 = 0;

        if table_dir.exists() {
            for entry in std::fs::read_dir(&table_dir).map_err(|e| format!("read table dir: {}", e))? {
                let entry = entry.map_err(|e| format!("table entry: {}", e))?;
                let path = entry.path();
                if !path.extension().map_or(false, |e| e == "parquet") {
                    continue;
                }
                match read_parquet_stats(&path) {
                    Ok((tmin, tmax, rows, _)) => {
                        time_min = time_min.min(tmin);
                        time_max = time_max.max(tmax);
                        total_rows += rows as i64;
                    }
                    Err(e) => {
                        tracing::warn!(path = %path.display(), "recount: skip unreadable file: {}", e);
                    }
                }
            }
        }

        let conn = self.db.conn().lock().await;
        conn.execute("INSERT OR IGNORE INTO databases (name) VALUES (?1)", params![db])
            .map_err(|e| format!("ensure db: {}", e))?;
        conn.execute(
            "INSERT OR IGNORE INTO tables (db_name, table_name) VALUES (?1, ?2)",
            params![db, table],
        )
        .map_err(|e| format!("ensure table: {}", e))?;

        let now_ms = chrono::Utc::now().timestamp_millis();
        let time_min = if time_min == i64::MAX { None } else { Some(time_min) };
        let time_max = if time_max == i64::MIN { None } else { Some(time_max) };
        conn.execute(
            "UPDATE tables SET time_min=?1, time_max=?2, total_rows=?3, updated_at=?4 \
             WHERE db_name=?5 AND table_name=?6",
            params![time_min, time_max, total_rows, now_ms, db, table],
        )
        .map_err(|e| format!("recount update: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::db::Db;
    use tempfile::tempdir;

    fn test_store() -> (tempfile::TempDir, MetadataStore) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();
        (dir, MetadataStore::new(Arc::new(db)))
    }

    #[tokio::test]
    async fn test_update_stats_and_list() {
        let (_dir, store) = test_store();

        store
            .update_stats(
                "metrics", "cpu",
                1000, 2000, 10,
                &[("usage".to_string(), "Float".to_string()), ("hostname".to_string(), "String".to_string())],
                &["host".to_string()],
            )
            .await
            .unwrap();

        let dbs = store.list_databases().await.unwrap();
        assert_eq!(dbs, vec!["metrics".to_string()]);

        let tables = store.list_tables("metrics").await.unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "cpu");
        assert_eq!(tables[0].row_count, 10);
        assert_eq!(tables[0].time_min, Some(1000));
        assert_eq!(tables[0].time_max, Some(2000));
        assert_eq!(tables[0].tag_keys, vec!["host".to_string()]);
        assert_eq!(tables[0].field_count, 2);

        // 累积行数 + 时间范围合并
        store
            .update_stats(
                "metrics", "cpu",
                1500, 3000, 5,
                &[("usage".to_string(), "Float".to_string())],
                &["host".to_string()],
            )
            .await
            .unwrap();

        let detail = store.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.database, "metrics");
        assert_eq!(detail.table, "cpu");
        assert_eq!(detail.row_count, 15);
        assert_eq!(detail.time_min, Some(1000));
        assert_eq!(detail.time_max, Some(3000));
        assert_eq!(detail.tag_keys, vec!["host".to_string()]);
        assert_eq!(detail.fields.len(), 2);
        assert_eq!(detail.fields[0].name, "hostname");
        assert_eq!(detail.fields[0].value_type, "String");
        assert_eq!(detail.sources, Vec::<String>::new());
        assert!(detail.updated_at.is_some());
    }

    #[tokio::test]
    async fn test_merge_schema_sources() {
        // agent_tables.agent_id 有外键约束，需先注册 agent（心跳的正常路径）
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Arc::new(Db::open(&db_path).unwrap());
        let agents = crate::server::agent_store::AgentStore::new(db.clone());
        agents.register("agent-1", "h1", "x86", "1.0", "10.0.0.1:8080").await.unwrap();
        agents.register("agent-2", "h2", "arm", "1.0", "10.0.0.2:8080").await.unwrap();

        let store = MetadataStore::new(db);
        store
            .merge_schema("metrics", "cpu", "agent-1", &["host".to_string()], &[("usage".to_string(), "Float".to_string())])
            .await
            .unwrap();
        store
            .merge_schema("metrics", "cpu", "agent-2", &["host".to_string()], &[("usage".to_string(), "Float".to_string())])
            .await
            .unwrap();

        let detail = store.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.tag_keys, vec!["host".to_string()]);
        assert_eq!(detail.fields.len(), 1);
        assert_eq!(detail.fields[0].name, "usage");
        assert_eq!(detail.fields[0].value_type, "Float");
        assert_eq!(detail.sources, vec!["agent-1".to_string(), "agent-2".to_string()]);
    }

    /// Build a parquet file with the given time column values (usage fixed).
    fn parquet_with_times(times: &[i64]) -> Vec<u8> {
        use parquet::data_type::{DoubleType, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;
        let schema = Arc::new(
            parse_message_type("message schema { required int64 time; optional double usage; }")
                .unwrap(),
        );
        let mut buf = Vec::new();
        let props = Arc::new(WriterProperties::new());
        let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int64Type>().write_batch(times, None, None).unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            let defs: Vec<i16> = vec![1; times.len()];
            let usage: Vec<f64> = times.iter().map(|t| *t as f64 / 100.0).collect();
            col.typed::<DoubleType>().write_batch(&usage, Some(&defs), None).unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();
        buf
    }

    #[tokio::test]
    async fn test_recount_table_replaces_stats() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Arc::new(Db::open(&db_path).unwrap());
        let store = MetadataStore::new(db);
        let data_dir = dir.path().join("data");

        // Two files with different time ranges: 1000-2000 and 5000
        let table_dir = data_dir.join("metrics").join("cpu");
        std::fs::create_dir_all(&table_dir).unwrap();
        std::fs::write(table_dir.join("a.parquet"), parquet_with_times(&[1000, 2000])).unwrap();
        std::fs::write(table_dir.join("b.parquet"), parquet_with_times(&[5000])).unwrap();

        store.recount_table("metrics", "cpu", &data_dir).await.unwrap();
        let detail = store.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.row_count, 3);
        assert_eq!(detail.time_min, Some(1000));
        assert_eq!(detail.time_max, Some(5000));

        // Remove one file and recount: REPLACE semantics, not accumulate
        std::fs::remove_file(table_dir.join("a.parquet")).unwrap();
        store.recount_table("metrics", "cpu", &data_dir).await.unwrap();
        let detail = store.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.row_count, 1, "recount must replace, not accumulate");
        assert_eq!(detail.time_min, Some(5000));
        assert_eq!(detail.time_max, Some(5000));
    }

    #[tokio::test]
    async fn test_list_table_agents() {
        // agent_tables.agent_id 有外键约束，需先注册 agent
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Arc::new(Db::open(&db_path).unwrap());
        let agents = crate::server::agent_store::AgentStore::new(db.clone());
        agents.register("agent-1", "h1", "x86", "1.0", "10.0.0.1:8080").await.unwrap();
        agents.register("agent-2", "h2", "arm", "1.0", "10.0.0.2:8080").await.unwrap();

        let store = MetadataStore::new(db);
        store
            .merge_schema("metrics", "cpu", "agent-1", &["host".to_string()], &[("usage".to_string(), "Float".to_string())])
            .await
            .unwrap();
        store
            .merge_schema("metrics", "cpu", "agent-2", &["host".to_string()], &[("usage".to_string(), "Float".to_string())])
            .await
            .unwrap();

        let ids = store.list_table_agents("metrics", "cpu").await.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"agent-1".to_string()));
        assert!(ids.contains(&"agent-2".to_string()));

        let empty = store.list_table_agents("metrics", "nope").await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_get_table_missing() {
        let (_dir, store) = test_store();
        assert!(store.get_table("nope", "nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_databases_excludes_duplicates() {
        let (_dir, store) = test_store();
        let fd = vec![("v".to_string(), "I64".to_string())];

        store.update_stats("db1", "t1", 0, 100, 1, &fd, &[]).await.unwrap();
        store.update_stats("db1", "t2", 0, 100, 1, &fd, &[]).await.unwrap();
        store.update_stats("db2", "t1", 0, 100, 1, &fd, &[]).await.unwrap();

        let dbs = store.list_databases().await.unwrap();
        assert_eq!(dbs.len(), 2);
        assert!(dbs.contains(&"db1".to_string()));
        assert!(dbs.contains(&"db2".to_string()));

        // 同一 db 下两张表独立统计
        let tables = store.list_tables("db1").await.unwrap();
        assert_eq!(tables.len(), 2);
    }
}
