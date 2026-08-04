use crate::agent::buffer::Buffer;
use hyper::{Request, Response, StatusCode};
use std::sync::Arc;
use tokio::sync::Mutex;
use url::form_urlencoded;

pub struct QueryHandler {
    pub buffer: Arc<Mutex<Buffer>>,
}

impl QueryHandler {
    pub async fn handle<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: Send + Unpin + 'static,
    {
        let uri = req.uri();
        let query_str = uri.query().unwrap_or("");
        let params: Vec<(String, String)> = form_urlencoded::parse(query_str.as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        let get = |key: &str| -> Option<String> {
            params.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
        };

        let db = get("db").unwrap_or_else(|| "default".into());
        let table = match get("table") {
            Some(t) => t,
            None => {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(r#"{"error":"missing table param"}"#.into())
                    .expect("valid response"));
            }
        };

        let start_ns = get("start").and_then(|s| s.parse::<i64>().ok());
        let end_ns = get("end").and_then(|s| s.parse::<i64>().ok());

        let _tag_key: Option<String> = get("tag").and_then(|s| {
            let parts: Vec<&str> = s.splitn(2, '=').collect();
            if parts.len() == 2 {
                None
            } else {
                None
            }
        });

        // Parse tag filters: multiple tag=k=v params
        let mut tag_filters: Vec<(String, String)> = Vec::new();
        for (_k, v) in params.iter().filter(|(k, _)| k == "tag") {
            if let Some((tk, tv)) = v.split_once('=') {
                tag_filters.push((tk.to_string(), tv.to_string()));
            }
        }

        let buf = self.buffer.lock().await;
        let rows = buf.query(
            &db, &table,
            start_ns, end_ns,
            tag_filters.first().map(|(k, _)| k.as_str()),
            tag_filters.first().map(|(_, v)| v.as_str()),
        ).unwrap_or_default();

        let json = serde_json::to_string(&serde_json::json!({ "rows": rows }))
            .unwrap_or_else(|_| r#"{"rows":[]}"#.into());

        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(json)
            .expect("valid response"))
    }
}
