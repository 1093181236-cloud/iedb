use crate::agent::buffer::Buffer;
use crate::agent::buffer::chunk::{Row, FieldValue as BFieldValue};
use crate::config::Config;
use crate::agent::wal::wal_core::{WalManager, apply_write_batch};
use crate::agent::wal::{WriteBatch, WalOp};
use hyper::{Request, Response, StatusCode, Method};
use http_body_util::BodyExt;
use bytes;
use std::sync::Arc;
use tokio::sync::Mutex;
use influxdb_line_protocol::parse_lines;

pub struct WriteHandler {
    pub buffer: Arc<Mutex<Buffer>>,
    pub wal: Arc<Mutex<WalManager>>,
    pub config: Arc<Config>,
}

impl WriteHandler {
    pub async fn handle<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body<Data = bytes::Bytes> + Send + Unpin,
        B::Error: Into<hyper::Error>,
    {
        if req.method() != Method::POST {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body("POST only".into())
                .expect("valid response"));
        }

        // Check body size limit (I7 fix)
        let max_body_bytes = self.config.max_body_bytes();
        let content_length = req.headers()
            .get(hyper::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        if content_length > max_body_bytes {
            return Ok(Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(format!("body exceeds {} bytes limit", max_body_bytes))
                .expect("valid response"));
        }

        // Parse query params
        let uri = req.uri();
        let query: Vec<(String, String)> = uri.query()
            .map(|q| {
                url::form_urlencoded::parse(q.as_bytes())
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect()
            })
            .unwrap_or_default();

        let db = query.iter()
            .find(|(k, _)| k == "db")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "default".into());

        // Read body
        let body_bytes = req.into_body().collect().await
            .map_err(Into::into)?
            .to_bytes();

        // Guard against bodies that exceeded the declared content-length (I7 defense-in-depth)
        if body_bytes.len() > max_body_bytes {
            return Ok(Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(format!("body exceeds {} bytes limit", max_body_bytes))
                .expect("valid response"));
        }

        let lp_str = match std::str::from_utf8(&body_bytes) {
            Ok(s) => s,
            Err(_) => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("invalid utf-8".into())
                    .expect("valid response"));
            }
        };

        // Parse line protocol
        let snapshot_interval_ns = self.config.snapshot_interval_secs().saturating_mul(1_000_000_000);
        let mut rows_by_table: std::collections::HashMap<String, (Vec<String>, Vec<String>, Vec<Row>)> = std::collections::HashMap::new();

        for line in parse_lines(lp_str) {
            match line {
                Ok(parsed) => {
                    let table_name = parsed.series.measurement.to_string();

                    // Build tag values in alphabetical order (sorted by key for consistency)
                    let mut tag_pairs: Vec<(String, String)> = Vec::new();
                    if let Some(ref tag_set) = parsed.series.tag_set {
                        for (k, v) in tag_set {
                            tag_pairs.push((k.to_string(), v.to_string()));
                        }
                    }
                    tag_pairs.sort_by(|a, b| a.0.cmp(&b.0));

                    let tag_keys: Vec<String> = tag_pairs.iter().map(|(k, _)| k.clone()).collect();
                    let tag_values: Vec<String> = tag_pairs.iter().map(|(_, v)| v.clone()).collect();

                    // Build field values (C2 fix: preserve field names)
                    let mut field_pairs: Vec<(String, BFieldValue)> = Vec::new();
                    for (key, value) in &parsed.field_set {
                        let val = match value {
                            influxdb_line_protocol::FieldValue::I64(v) => BFieldValue::I64(*v),
                            influxdb_line_protocol::FieldValue::F64(v) => BFieldValue::F64(*v),
                            influxdb_line_protocol::FieldValue::U64(v) => BFieldValue::U64(*v),
                            influxdb_line_protocol::FieldValue::Boolean(v) => BFieldValue::Bool(*v),
                            influxdb_line_protocol::FieldValue::String(v) => BFieldValue::String(v.to_string()),
                        };
                        field_pairs.push((key.to_string(), val));
                    }

                    let field_names: Vec<String> = field_pairs.iter().map(|(k, _)| k.clone()).collect();
                    let field_values: Vec<Option<BFieldValue>> = field_pairs.iter().map(|(_, v)| Some(v.clone())).collect();

                    let time_ns = parsed.timestamp.unwrap_or_else(|| {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as i64
                    });

                    let row = Row {
                        time: time_ns,
                        tag_values,
                        field_values,
                    };

                    let entry = rows_by_table
                        .entry(table_name)
                        .or_insert_with(|| (field_names.clone(), tag_keys.clone(), Vec::new()));
                    // Update field_names/tag_keys if they differ (unlikely but safe)
                    if entry.0.is_empty() { entry.0 = field_names; }
                    if entry.1.is_empty() { entry.1 = tag_keys; }
                    entry.2.push(row);
                }
                Err(e) => {
                    tracing::warn!("LP parse error (line skipped): {}", e);
                }
            }
        }

        if rows_by_table.is_empty() {
            return Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body("0 rows".into())
                .expect("valid response"));
        }

        // Build batches grouped by chunk_time (I4 fix)
        let mut batches: Vec<WriteBatch> = Vec::new();
        let mut total_rows = 0;

        for (table_name, (field_names, tag_keys, rows)) in rows_by_table {
            // Group rows by chunk_time
            let mut grouped: std::collections::BTreeMap<i64, Vec<Row>> = std::collections::BTreeMap::new();
            for row in rows {
                let chunk_time = (row.time / snapshot_interval_ns) * snapshot_interval_ns;
                grouped.entry(chunk_time).or_default().push(row);
            }

            for (chunk_time, chunk_rows) in grouped {
                total_rows += chunk_rows.len();
                batches.push(WriteBatch {
                    db_name: db.clone(),
                    table_name: table_name.clone(),
                    chunk_time,
                    field_names: field_names.clone(),
                    tag_keys: tag_keys.clone(),
                    rows: chunk_rows,
                });
            }
        }

        // Buffer all batches to WAL and flush synchronously (C1 fix: single write path)
        let (ops, wal_seq) = {
            let mut wal = self.wal.lock().await;
            for batch in &batches {
                if let Err(e) = wal.buffer_op(WalOp::Write(batch.clone())) {
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(format!("WAL buffer error: {}", e))
                        .expect("valid response"));
                }
            }
            let ops = match wal.flush().await {
                Ok(ops) => ops,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(format!("WAL flush error: {}", e))
                        .expect("valid response"));
                }
            };
            // The flushed ops live in the WAL file written with this seq —
            // track it on the buffer rows so snapshot WAL cleanup can
            // advance (a constant 0 here made cleanup a permanent no-op
            // and let .wal files accumulate unboundedly).
            let wal_seq = wal.current_sequence().saturating_sub(1);
            (ops, wal_seq)
        };

        // Apply flushed ops to memory buffer (sole path for buffer insertion)
        {
            let mut buf = self.buffer.lock().await;
            for op in &ops {
                if let WalOp::Write(batch) = op {
                    apply_write_batch(&mut buf, batch, wal_seq);
                }
            }
        }

        Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(format!("{} rows", total_rows))
            .expect("valid response"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::buffer::Buffer;
    use crate::agent::wal::wal_core::WalManager;
    use crate::config::Config;
    use bytes::Bytes;
    use http_body_util::Full;
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tempfile::tempdir;

    /// 与 server::test_util::TestBody 等价：Full<Bytes> 的 Error 是 Infallible，
    /// 而 WriteHandler 要求 B::Error: Into<hyper::Error>，所以包一层（agent-only
    /// 构建不能引用 server 模块）。
    struct TestBody(Full<Bytes>);

    impl TestBody {
        fn empty() -> Self {
            TestBody(Full::new(Bytes::new()))
        }

        fn from_bytes(bytes: Bytes) -> Self {
            TestBody(Full::new(bytes))
        }
    }

    impl hyper::body::Body for TestBody {
        type Data = Bytes;
        type Error = hyper::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            Pin::new(&mut self.get_mut().0)
                .poll_frame(cx)
                .map(|opt| opt.map(|res| res.map_err(|e: Infallible| match e {})))
        }

        fn is_end_stream(&self) -> bool {
            self.0.is_end_stream()
        }

        fn size_hint(&self) -> hyper::body::SizeHint {
            self.0.size_hint()
        }
    }

    async fn make_handler(
        max_body_bytes: usize,
        max_write_buffer_ops: usize,
    ) -> (WriteHandler, Arc<Mutex<Buffer>>, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let toml = format!(
            r#"
            [server]
            port = 0
            max_body_bytes = {max_body_bytes}

            [data]
            dir = "{}"

            [wal]
            flush_interval_secs = 1
            max_write_buffer_ops = {max_write_buffer_ops}

            [flush]
            snapshot_interval = "1s"
            "#,
            data_dir.display()
        );
        let config = Arc::new(toml::from_str::<Config>(&toml).unwrap());
        let buffer = Arc::new(Mutex::new(Buffer::new()));
        let wal = Arc::new(Mutex::new(
            WalManager::new(&data_dir, &config.wal).await.unwrap(),
        ));
        let handler = WriteHandler {
            buffer: buffer.clone(),
            wal,
            config,
        };
        (handler, buffer, dir)
    }

    fn post_req(uri: &str, body: &[u8]) -> Request<TestBody> {
        Request::builder()
            .method(Method::POST)
            .uri(uri)
            .body(TestBody::from_bytes(Bytes::from(body.to_vec())))
            .unwrap()
    }

    #[tokio::test]
    async fn test_write_applies_rows_with_real_wal_seq() {
        let (handler, buffer, _dir) = make_handler(1024, 100).await;

        let resp = handler
            .handle(post_req("/write?db=mydb", b"cpu,host=a usage=1.5"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let buf = buffer.lock().await;
        let table = buf.get_table("mydb", "cpu").unwrap();
        let min_seq = table
            .chunks
            .iter()
            .map(|c| c.min_wal_seq)
            .min()
            .unwrap_or(0);
        assert_eq!(
            min_seq, 1,
            "chunk min_wal_seq must carry the real WAL file seq (first file = 1), got {}",
            min_seq
        );
    }

    #[tokio::test]
    async fn test_non_post_returns_405() {
        let (handler, _buffer, _dir) = make_handler(1024, 100).await;
        let req = Request::builder()
            .method(Method::GET)
            .uri("/write?db=mydb")
            .body(TestBody::empty())
            .unwrap();
        let resp = handler.handle(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(resp.body(), "POST only");
    }

    #[tokio::test]
    async fn test_content_length_overflow_returns_413() {
        let (handler, _buffer, _dir) = make_handler(64, 100).await;

        // 声明 Content-Length 超限 → 不读 body 直接 413
        let req = Request::builder()
            .method(Method::POST)
            .uri("/write")
            .header(hyper::header::CONTENT_LENGTH, 1024u32)
            .body(TestBody::from_bytes(Bytes::from("small")))
            .unwrap();
        let resp = handler.handle(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(resp.body().contains("bytes limit"));
    }

    #[tokio::test]
    async fn test_actual_body_overflow_returns_413() {
        let (handler, _buffer, _dir) = make_handler(64, 100).await;

        // 无 Content-Length 头、实际 body 超限（纵深防御）→ 413
        let big_body = vec![b'a'; 100];
        let resp = handler.handle(post_req("/write", &big_body)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_invalid_utf8_returns_400() {
        let (handler, _buffer, _dir) = make_handler(1024, 100).await;

        // 非 UTF-8 body → 400，而不是 panic
        let resp = handler.handle(post_req("/write", &[0xFF, 0xFE, 0x00, 0x00])).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(resp.body(), "invalid utf-8");
    }

    #[tokio::test]
    async fn test_empty_body_returns_204() {
        let (handler, _buffer, _dir) = make_handler(1024, 100).await;

        // 空 body / 无法解析的行 → 0 行 204
        let resp = handler.handle(post_req("/write?db=mydb", b"")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(resp.body(), "0 rows");

        let resp = handler.handle(post_req("/write?db=mydb", b"not valid line protocol")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(resp.body(), "0 rows");
    }

    #[tokio::test]
    async fn test_valid_write_stores_rows_and_wal() {
        let (handler, buffer, dir) = make_handler(1024, 100).await;

        let lp = "cpu,host=srv01 cpu=75.5,mem=62.3 1700000000000000000\n";
        let resp = handler.handle(post_req("/write?db=mydb", lp.as_bytes())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(resp.body(), "1 rows");

        // 数据进入内存 buffer
        let buf = buffer.lock().await;
        let table = buf.get_table("mydb", "cpu").unwrap();
        assert_eq!(table.schema.tag_keys, vec!["host".to_string()]);
        let fields: Vec<String> = table.schema.field_defs.iter().map(|f| f.name.clone()).collect();
        assert_eq!(fields, vec!["cpu".to_string(), "mem".to_string()]);
        assert_eq!(table.chunks.len(), 1);
        assert_eq!(table.chunks[0].rows.len(), 1);
        assert_eq!(table.chunks[0].rows[0].time, 1700000000000000000);
        assert_eq!(table.chunks[0].rows[0].tag_values, vec!["srv01".to_string()]);
        // FieldValue 无 PartialEq，用匹配断言
        match &table.chunks[0].rows[0].field_values[0] {
            Some(BFieldValue::F64(v)) => assert!((v - 75.5).abs() < 1e-9),
            other => panic!("expected F64(75.5), got {:?}", other),
        }
        drop(buf);

        // WAL 文件已落盘
        let wal_dir = dir.path().join("data").join("wal");
        let wal_files: Vec<_> = std::fs::read_dir(&wal_dir).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .collect();
        assert_eq!(wal_files.len(), 1);
    }

    #[tokio::test]
    async fn test_wal_buffer_full_returns_503() {
        // max_write_buffer_ops = 1：单请求内两个 batch（不同 chunk_time）→ 第二个 buffer_op 失败
        let (handler, buffer, _dir) = make_handler(1024, 1).await;

        let lp = "cpu,host=srv01 cpu=75.5 1700000000000000000\n\
                  cpu,host=srv01 cpu=30.0 1700000001000000000\n";
        let resp = handler.handle(post_req("/write?db=mydb", lp.as_bytes())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(resp.body().contains("WAL buffer error"));

        // buffer 未收到任何行（WAL 拒绝后不应用）
        let buf = buffer.lock().await;
        assert!(buf.get_table("mydb", "cpu").is_none());
    }
}
