// HTTP handler for Parquet ingest: receives agent snapshot files, stores them
// under {data_dir}/{db}/{table}/, and updates table statistics from the
// Parquet footer (time range, row count, field defs).
use crate::server::metadata_store::MetadataStore;
use crate::server::query_engine::QueryEngine;
use crate::server::table_provider::TableProvider;
use hyper::{Method, Request, Response};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct IngestApiHandler {
    pub data_dir: PathBuf,
    pub metadata: Arc<MetadataStore>,
    pub max_body_bytes: usize,
    /// 可选：有查询引擎时，写入后把新表注册到 DataFusion（ListingTable 自动发现后续文件）
    pub engine: Option<Arc<QueryEngine>>,
}

impl IngestApiHandler {
    pub async fn handle<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
        hyper::Error: From<B::Error>,
    {
        if req.uri().path() != "/api/v1/ingest/parquet" {
            return Ok(json_err(404, "NOT_FOUND"));
        }
        if req.method() != Method::POST {
            return Ok(json_err(405, "METHOD_NOT_ALLOWED"));
        }

        // 解析 query params: db, measurement
        let query_string = req.uri().query().map(|q| q.to_string());
        let query: Vec<(String, String)> = query_string
            .as_deref()
            .map(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect()
            })
            .unwrap_or_default();
        let db = query
            .iter()
            .find(|(k, _)| k == "db")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "default".into());
        let table = query
            .iter()
            .find(|(k, _)| k == "measurement")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "unknown".into());
        // Optional chunk_time (ns): makes the filename deterministic across
        // upload retries, so a re-upload of the same chunk set is idempotent.
        // Absent → fall back to server time (backward compat with old agents).
        let chunk_time = query
            .iter()
            .find(|(k, _)| k == "chunk_time")
            .and_then(|(_, v)| v.parse::<i64>().ok());

        // 防止路径穿越：db/table 只允许单段名称
        if !is_safe_segment(&db) || !is_safe_segment(&table) {
            return Ok(json_err(400, "BAD_REQUEST"));
        }

        // 从 header 获取 agent_id（用于文件命名）
        let agent_id = req
            .headers()
            .get("x-agent-id")
            .and_then(|v| v.to_str().ok())
            .map(sanitize_filename_part)
            .unwrap_or_else(|| "unknown".into());

        // I5 fix: enforce body size limit before buffering
        let content_length = req.headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        if content_length > self.max_body_bytes {
            return Ok(json_err(413, "PAYLOAD_TOO_LARGE"));
        }
        // 读取 body (Parquet bytes)
        use http_body_util::BodyExt;
        let body = req.collect().await?.to_bytes();
        if body.len() > self.max_body_bytes {
            return Ok(json_err(413, "PAYLOAD_TOO_LARGE"));
        }

        // 写入文件 {data_dir}/{db}/{table}/{agent_id}_{ts}.parquet
        let table_dir = self.data_dir.join(&db).join(&table);
        if let Err(e) = std::fs::create_dir_all(&table_dir) {
            return Ok(json_err(500, &format!("mkdir: {}", e)));
        }

        let chunk_time = chunk_time
            .or_else(|| chrono::Utc::now().timestamp_nanos_opt())
            .unwrap_or(0);
        let filename = format!("{}_{}.parquet", agent_id, chunk_time);
        let filepath = table_dir.join(&filename);

        // Idempotence: a retry of the same chunk (staging re-upload after a
        // lost response) hits the same filename. The data is already on disk,
        // so skip the write AND the stats accumulation — otherwise
        // total_rows would double-count the same rows.
        if filepath.exists() {
            tracing::info!(path = %filepath.display(), "Duplicate ingest skipped (already exists)");
            return Ok(Response::builder().status(200).body("ok".into()).unwrap());
        }

        if let Err(e) = std::fs::write(&filepath, &body) {
            return Ok(json_err(500, &format!("write: {}", e)));
        }

        // 读取 Parquet footer stats 并更新元数据。
        // 注意：read_parquet_stats 的 Err 含 Box<dyn Error>（非 Send），
        // 必须在此处消化掉，避免非 Send 值跨越 .await（server 连接需要 Send future）。
        let stats = crate::server::metadata_store::read_parquet_stats(&filepath)
            .map_err(|e| {
                tracing::warn!("failed to read parquet stats from {}: {}", filepath.display(), e);
            })
            .ok();
        if let Some((time_min, time_max, row_count, field_defs)) = stats {
            self.metadata
                .update_stats(&db, &table, time_min, time_max, row_count, &field_defs, &[])
                .await
                .ok();
        }
        // Record agent→table relationship from ingest. The mapping can fail
        // (e.g. FK violation when the agent never registered); log it and
        // continue — the upload itself stays successful.
        if let Err(e) = self
            .metadata
            .merge_schema(&db, &table, &agent_id, &[], &[])
            .await
        {
            tracing::warn!(agent = %agent_id, db = %db, table = %table,
                "record agent table after ingest: {}", e);
        }

        // 新表注册到 DataFusion（首次上传时；已注册的表是 no-op）
        if let Some(engine) = &self.engine {
            if let Err(e) = TableProvider::add_file(engine, &db, &table, &filepath).await {
                tracing::warn!("register table after ingest: {}", e);
            }
        }

        Ok(Response::builder().status(200).body("ok".into()).unwrap())
    }
}

/// db / table names must be a single non-empty path segment.
fn is_safe_segment(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
}

/// Strip path separators from the agent id so it can be used in a filename.
fn sanitize_filename_part(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

fn json_err(status: u16, code: &str) -> Response<String> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(format!(r#"{{"error":"{}","code":"{}"}}"#, code.to_lowercase(), code))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::db::Db;
    use crate::server::test_util::TestBody;
    use bytes::Bytes;
    use parquet::data_type::{DoubleType, Int64Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use tempfile::tempdir;

    /// Build a minimal Parquet file: columns `time` (required int64) and `usage` (optional double).
    fn make_test_parquet() -> Vec<u8> {
        let schema = Arc::new(
            parse_message_type("message schema { required int64 time; optional double usage; }").unwrap(),
        );
        let mut buf = Vec::new();
        let props = Arc::new(WriterProperties::new());
        let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int64Type>()
                .write_batch(&[1000i64, 2000, 3000], None, None)
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            // optional double → max definition level 1, values require def levels
            col.typed::<DoubleType>()
                .write_batch(&[1.5, 2.5, 3.5], Some(&[1i16, 1, 1]), None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();
        buf
    }

    fn test_handler() -> (tempfile::TempDir, IngestApiHandler) {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();
        let handler = IngestApiHandler {
            data_dir: data_dir.clone(),
            metadata: Arc::new(MetadataStore::new(Arc::new(db))),
            engine: None,
            max_body_bytes: 10 * 1024 * 1024,
        };
        (dir, handler)
    }

    fn upload_req(uri: &str, body: Vec<u8>, agent_id: &str) -> Request<TestBody> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("x-agent-id", agent_id)
            .body(TestBody::from_bytes(Bytes::from(body)))
            .unwrap()
    }

    #[tokio::test]
    async fn test_upload_writes_file_and_metadata() {
        let (_dir, h) = test_handler();
        let parquet_bytes = make_test_parquet();

        let resp = h
            .handle(upload_req(
                "/api/v1/ingest/parquet?db=metrics&measurement=cpu",
                parquet_bytes,
                "agent-1",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.body(), "ok");

        // 文件落盘 {data_dir}/metrics/cpu/{agent-1}_{ts}.parquet
        let files: Vec<_> = std::fs::read_dir(h.data_dir.join("metrics").join("cpu"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].starts_with("agent-1_") && files[0].ends_with(".parquet"));

        // 元数据：行数 + 时间范围 + 字段
        let detail = h.metadata.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.row_count, 3);
        assert_eq!(detail.time_min, Some(1000));
        assert_eq!(detail.time_max, Some(3000));
        assert_eq!(detail.fields.len(), 1);
        assert_eq!(detail.fields[0].name, "usage");
        assert_eq!(detail.fields[0].value_type, "DOUBLE");
    }

    #[tokio::test]
    async fn test_upload_uses_chunk_time_in_filename() {
        let (_dir, h) = test_handler();

        let resp = h
            .handle(upload_req(
                "/api/v1/ingest/parquet?db=metrics&measurement=cpu&chunk_time=1700000000000000000",
                make_test_parquet(),
                "agent-1",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);

        let files: Vec<_> = std::fs::read_dir(h.data_dir.join("metrics").join("cpu"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "agent-1_1700000000000000000.parquet",
            "filename must use the agent-provided chunk_time");
    }

    #[tokio::test]
    async fn test_duplicate_upload_is_idempotent() {
        let (_dir, h) = test_handler();

        let uri = "/api/v1/ingest/parquet?db=metrics&measurement=cpu&chunk_time=1700000000000000000";
        let first = h
            .handle(upload_req(uri, make_test_parquet(), "agent-1"))
            .await
            .unwrap();
        assert_eq!(first.status().as_u16(), 200);

        // Retry of the same chunk (e.g. response lost, staging re-upload)
        let second = h
            .handle(upload_req(uri, make_test_parquet(), "agent-1"))
            .await
            .unwrap();
        assert_eq!(second.status().as_u16(), 200);

        // One file on disk
        let files: Vec<_> = std::fs::read_dir(h.data_dir.join("metrics").join("cpu"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 1, "duplicate chunk_time must not stack files");

        // Stats counted exactly once
        let detail = h.metadata.get_table("metrics", "cpu").await.unwrap().unwrap();
        assert_eq!(detail.row_count, 3, "duplicate upload must not double-count stats");
    }

    #[tokio::test]
    async fn test_upload_defaults_db_and_table() {
        let (_dir, h) = test_handler();

        // 缺省 db/measurement query 参数 → default/unknown
        let resp = h
            .handle(upload_req("/api/v1/ingest/parquet", make_test_parquet(), "agent-1"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);

        let files: Vec<_> = std::fs::read_dir(h.data_dir.join("default").join("unknown"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].starts_with("agent-1_") && files[0].ends_with(".parquet"));

        // 元数据同样落到 default.unknown
        let detail = h.metadata.get_table("default", "unknown").await.unwrap().unwrap();
        assert_eq!(detail.row_count, 3);
    }

    #[tokio::test]
    async fn test_upload_validation() {
        let (_dir, h) = test_handler();

        // 非 POST → 405
        let req = Request::builder()
            .method("GET")
            .uri("/api/v1/ingest/parquet")
            .body(TestBody::empty())
            .unwrap();
        let resp = h.handle(req).await.unwrap();
        assert_eq!(resp.status().as_u16(), 405);

        // 错误路径 → 404
        let resp = h
            .handle(upload_req("/api/v1/ingest/other", Vec::new(), "a1"))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 404);

        // 路径穿越 → 400
        let resp = h
            .handle(upload_req(
                "/api/v1/ingest/parquet?db=metrics&measurement=..%2F..%2Fevil",
                Vec::new(),
                "a1",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 400);
    }

    #[tokio::test]
    async fn test_upload_registers_table_in_query_engine() {
        use crate::server::query_engine::QueryEngine;

        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();
        let engine = Arc::new(QueryEngine::new(100, 10, 4));
        let handler = IngestApiHandler {
            data_dir: data_dir.clone(),
            metadata: Arc::new(MetadataStore::new(Arc::new(db))),
            engine: Some(engine.clone()),
            max_body_bytes: 10 * 1024 * 1024,
        };

        let resp = handler
            .handle(upload_req(
                "/api/v1/ingest/parquet?db=metrics&measurement=cpu",
                make_test_parquet(),
                "agent-1",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);

        // 上传后新表立即可查询（DataFusion 表自动注册）
        let result = engine.query(
            "SELECT * FROM metrics.cpu",
            crate::server::federation::QueryMode::History,
            None,
        ).await.unwrap();
        assert_eq!(result["returned_rows"], 3);
        assert_eq!(result["rows"][0]["time"], 1000);
    }

    #[test]
    fn test_sanitize_filename_part() {
        assert_eq!(sanitize_filename_part("agent-1"), "agent-1");
        assert_eq!(sanitize_filename_part("a/b"), "a_b");
        assert_eq!(sanitize_filename_part("a b"), "a_b");
    }
}
