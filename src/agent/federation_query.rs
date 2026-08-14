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

        // Hold the buffer lock only long enough to CLONE the table and
        // decide whether any row falls in range; parquet serialization runs
        // after the guard is dropped so writes are not blocked for the
        // whole serialization.
        let (table, any_rows) = {
            let buf = self.buffer.lock().await;
            let t = match buf.get_table(&db, &table_name) {
                Some(t) => t,
                None => return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(b"{\"error\":\"table not found\"}".to_vec())
                    .expect("valid response")),
            };
            // 204 No Content when the buffer has nothing for this range
            let any_rows = t.chunks.iter().any(|c| {
                c.rows.iter().any(|r| {
                    start_ns.map_or(true, |s| r.time >= s) && end_ns.map_or(true, |e| r.time <= e)
                })
            });
            (t.clone(), any_rows)
        };
        if !any_rows {
            return Ok(Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Vec::new())
                .expect("valid response"));
        }

        let chunk_refs: Vec<&crate::agent::buffer::chunk::Chunk> = table.chunks.iter().collect();
        match crate::agent::flush::parquet_writer::flush_chunks_to_parquet(
            &table, &chunk_refs, start_ns, end_ns,
        ) {
            Ok(bytes) => Ok(Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/octet-stream")
                .body(bytes)
                .expect("valid response")),
            Err(e) => Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(serde_json::json!({"error": e}).to_string().into_bytes())
                .expect("valid response")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::buffer::chunk::{Chunk, FieldType, FieldValue, Row};
    use bytes::Bytes;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;

    /// Handler over a real Buffer seeded with `testdb.cpu`:
    /// tag [host], field [usage F64], rows at t=100 (srv01) and t=200 (srv02).
    fn seeded_handler() -> FederationQueryHandler {
        let mut buffer = Buffer::new();
        let table = buffer.get_or_create_table("testdb", "cpu");
        table.schema.ensure_tag_key("host");
        table.schema.ensure_field("usage", FieldType::F64);

        let mut chunk = Chunk::new(0);
        chunk.insert(
            Row {
                time: 100,
                tag_values: vec!["srv01".to_string()],
                field_values: vec![Some(FieldValue::F64(42.5))],
            },
            1,
        );
        chunk.insert(
            Row {
                time: 200,
                tag_values: vec!["srv02".to_string()],
                field_values: vec![Some(FieldValue::F64(7.25))],
            },
            2,
        );
        table.chunks.push(chunk);

        FederationQueryHandler {
            buffer: Arc::new(Mutex::new(buffer)),
        }
    }

    fn get(uri: &str) -> hyper::Request<()> {
        hyper::Request::builder().uri(uri).body(()).unwrap()
    }

    #[tokio::test]
    async fn test_ok_returns_parquet_with_two_rows() {
        let handler = seeded_handler();
        let resp = handler
            .handle(get("/api/v1/query/parquet?db=testdb&table=cpu"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").map(|v| v.as_bytes()),
            Some(b"application/octet-stream".as_slice())
        );

        let body = resp.into_body();
        assert!(!body.is_empty(), "parquet body should be non-empty");

        let reader = SerializedFileReader::new(Bytes::from(body)).expect("valid parquet");
        assert_eq!(
            reader.metadata().file_metadata().num_rows() as usize,
            2,
            "both seeded rows should be serialized"
        );
        let mut it = reader.get_row_iter(None).unwrap();
        let r0 = it.next().unwrap().unwrap();
        assert_eq!(r0.get_long(0).unwrap(), 100);
        assert_eq!(r0.get_string(1).unwrap(), "srv01");
        let r1 = it.next().unwrap().unwrap();
        assert_eq!(r1.get_long(0).unwrap(), 200);
        assert_eq!(r1.get_string(1).unwrap(), "srv02");
    }

    #[tokio::test]
    async fn test_no_rows_in_range_returns_204() {
        let handler = seeded_handler();
        // both rows are at t<=200, so start=500 matches nothing in range
        let resp = handler
            .handle(get("/api/v1/query/parquet?db=testdb&table=cpu&start=500"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(resp.into_body().is_empty());
    }

    #[tokio::test]
    async fn test_missing_table_param_returns_400() {
        let handler = seeded_handler();
        let resp = handler
            .handle(get("/api/v1/query/parquet?db=testdb"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_unknown_table_returns_404() {
        let handler = seeded_handler();
        let resp = handler
            .handle(get("/api/v1/query/parquet?db=testdb&table=nope"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_flush_error_returns_500_json() {
        // An empty field name breaks the parquet schema parser, so
        // flush_chunks_to_parquet errors and the handler must answer 500
        // with a JSON error body.
        let mut buffer = Buffer::new();
        let table = buffer.get_or_create_table("testdb", "err");
        table.schema.ensure_field("", FieldType::F64);
        let mut chunk = Chunk::new(0);
        chunk.insert(
            Row {
                time: 100,
                tag_values: vec![],
                field_values: vec![Some(FieldValue::F64(1.0))],
            },
            1,
        );
        table.chunks.push(chunk);
        let handler = FederationQueryHandler {
            buffer: Arc::new(Mutex::new(buffer)),
        };

        let resp = handler
            .handle(get("/api/v1/query/parquet?db=testdb&table=err"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = String::from_utf8(resp.into_body()).unwrap();
        assert!(
            body.contains("\"error\""),
            "500 body should be JSON: {}",
            body
        );
    }
}
