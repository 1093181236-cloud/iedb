// GET /api/v1/query/parquet?db=&table=&start=&end=
// Returns the table's buffered rows as serialized Parquet bytes so the
// server can union them with its persisted Parquet files (federation).
use crate::agent::buffer::Buffer;
use hyper::{Request, Response, StatusCode};
use std::sync::Arc;
use tokio::sync::Mutex;
use url::form_urlencoded;

pub struct FederationQueryHandler {
    pub buffer: Arc<Mutex<Buffer>>,
}

impl FederationQueryHandler {
    pub async fn handle<B>(&self, req: Request<B>) -> Result<Response<Vec<u8>>, hyper::Error>
    where
        B: Send + Unpin + 'static,
    {
        let params: Vec<(String, String)> = form_urlencoded::parse(
            req.uri().query().unwrap_or("").as_bytes(),
        )
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
        let get = |key: &str| -> Option<String> {
            params.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
        };

        let db = get("db").unwrap_or_else(|| "default".into());
        let table_name = match get("table") {
            Some(t) => t,
            None => return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(b"{\"error\":\"missing table param\"}".to_vec())
                .expect("valid response")),
        };
        let start_ns = get("start").and_then(|s| s.parse::<i64>().ok());
        let end_ns = get("end").and_then(|s| s.parse::<i64>().ok());

        // Serialize under the buffer lock; chunks are &-referenced briefly.
        let buf = self.buffer.lock().await;
        let table = match buf.get_table(&db, &table_name) {
            Some(t) => t,
            None => return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(b"{\"error\":\"table not found\"}".to_vec())
                .expect("valid response")),
        };
        let chunk_refs: Vec<&crate::agent::buffer::chunk::Chunk> = table.chunks.iter().collect();

        // 204 No Content when the buffer has nothing for this range
        let any_rows = chunk_refs.iter().any(|c| {
            c.rows.iter().any(|r| {
                start_ns.map_or(true, |s| r.time >= s) && end_ns.map_or(true, |e| r.time <= e)
            })
        });
        if !any_rows {
            return Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Vec::new())
                .expect("valid response"));
        }

        match crate::agent::flush::parquet_writer::flush_chunks_to_parquet(
            table, &chunk_refs, start_ns, end_ns,
        ) {
            Ok(bytes) => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/octet-stream")
                .body(bytes)
                .expect("valid response")),
            Err(e) => Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(format!("{{\"error\":\"{}\"}}", e).into_bytes())
                .expect("valid response")),
        }
    }
}
