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
        let ops = {
            let mut wal = self.wal.lock().await;
            for batch in &batches {
                if let Err(e) = wal.buffer_op(WalOp::Write(batch.clone())) {
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(format!("WAL buffer error: {}", e))
                        .expect("valid response"));
                }
            }
            match wal.flush().await {
                Ok(ops) => ops,
                Err(e) => {
                    return Ok(Response::builder()
                        .status(StatusCode::SERVICE_UNAVAILABLE)
                        .body(format!("WAL flush error: {}", e))
                        .expect("valid response"));
                }
            }
        };

        // Apply flushed ops to memory buffer (sole path for buffer insertion)
        {
            let mut buf = self.buffer.lock().await;
            for op in &ops {
                if let WalOp::Write(batch) = op {
                    apply_write_batch(&mut buf, batch, 0);
                }
            }
        }

        Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(format!("{} rows", total_rows))
            .expect("valid response"))
    }
}
