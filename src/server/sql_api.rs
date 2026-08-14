// HTTP handler for the SQL query API: POST /api/v1/query { "sql": "..." }.
use crate::server::query_engine::QueryEngine;
use hyper::{Method, Request, Response};
use std::sync::Arc;

pub struct SqlApiHandler {
    pub engine: Arc<QueryEngine>,
    pub data_dir: std::path::PathBuf,
    pub federator: Option<Arc<crate::server::federation::Federator>>,
}

impl SqlApiHandler {
    /// Routes one incoming request. `B` is any http body (the server layer
    /// passes `hyper::body::Incoming`; tests pass a test body).
    pub async fn handle<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
        hyper::Error: From<B::Error>,
    {
        if req.method() != Method::POST {
            return Ok(json_err(405, "METHOD_NOT_ALLOWED"));
        }
        use http_body_util::BodyExt;
        let body = req.collect().await?.to_bytes();
        let req_data: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let sql = req_data["sql"].as_str().unwrap_or("");
        let mode = crate::server::federation::parse_query_mode(
            req_data["mode"].as_str().unwrap_or("history"),
        );

        if sql.is_empty() {
            return Ok(json_err(400, "BAD_REQUEST"));
        }

        match self.engine.query(sql, mode, self.federator.as_deref()).await {
            Ok(result) => Ok(Response::builder()
                .status(200)
                .header("Content-Type", "application/json")
                .body(result.to_string())
                .unwrap()),
            Err(e)
                if mode != crate::server::federation::QueryMode::Buffer
                    && e.contains("table")
                    && e.contains("not found") =>
            {
                // 表尚未注册：尝试从 data_dir 按需注册。
                // Buffer mode 必须绕过：register_all 会把持久化表挂进
                // catalog，重试会让 buffer-only 查询看到历史 Parquet 行。
                // Buffer mode 的错误直接以 422 透传。
                use crate::server::table_provider::TableProvider;
                if let Err(re) = TableProvider::register_all(&self.engine, &self.data_dir).await {
                    tracing::warn!("Lazy table registration failed: {}", re);
                }
                // 重试一次
                match self.engine.query(sql, mode, self.federator.as_deref()).await {
                    Ok(result) => Ok(Response::builder()
                        .status(200)
                        .header("Content-Type", "application/json")
                        .body(result.to_string())
                        .unwrap()),
                    Err(e2) => Ok(json_err(422, &e2)),
                }
            }
            Err(e) => Ok(json_err(422, &e)),
        }
    }
}

fn json_err(status: u16, msg: &str) -> Response<String> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(format!(
            r#"{{"error":"{}","code":"{}"}}"#,
            msg,
            status_code_to_str(status)
        ))
        .unwrap()
}

fn status_code_to_str(s: u16) -> &'static str {
    match s {
        400 => "BAD_REQUEST",
        405 => "METHOD_NOT_ALLOWED",
        422 => "UNPROCESSABLE",
        _ => "INTERNAL",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::federation::QueryMode;
    use crate::server::table_provider::TableProvider;
    use crate::server::test_util::{write_test_parquet, TestBody};
    use bytes::Bytes;
    use tempfile::tempdir;

    async fn test_handler(max_rows: usize) -> (tempfile::TempDir, SqlApiHandler) {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("metrics").join("cpu");
        std::fs::create_dir_all(&table_dir).unwrap();
        write_test_parquet(&table_dir.join("a.parquet"));

        let data_dir = dir.path().to_path_buf();
        let engine = Arc::new(QueryEngine::new(max_rows, 10));
        TableProvider::register_all(&engine, &data_dir).await.unwrap();
        (dir, SqlApiHandler { engine, data_dir, federator: None })
    }

    fn json_req(method: &str, body: &str) -> Request<TestBody> {
        Request::builder()
            .method(method)
            .uri("/api/v1/query")
            .header("Content-Type", "application/json")
            .body(TestBody::from_bytes(Bytes::from(body.to_string())))
            .unwrap()
    }

    #[tokio::test]
    async fn test_query_ok() {
        let (_dir, h) = test_handler(100).await;
        let resp = h
            .handle(json_req("POST", r#"{"sql":"SELECT * FROM metrics.cpu"}"#))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["returned_rows"], 3);
        assert_eq!(v["rows"][0]["time"], 1000);
    }

    #[tokio::test]
    async fn test_method_not_allowed() {
        let (_dir, h) = test_handler(100).await;
        let resp = h.handle(json_req("GET", "")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 405);
        assert!(resp.body().contains("METHOD_NOT_ALLOWED"));
    }

    #[tokio::test]
    async fn test_empty_sql_bad_request() {
        let (_dir, h) = test_handler(100).await;
        let resp = h.handle(json_req("POST", r#"{}"#)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 400);
        assert!(resp.body().contains("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn test_sql_error_422() {
        let (_dir, h) = test_handler(100).await;
        let resp = h
            .handle(json_req("POST", r#"{"sql":"SELEC broken"}"#))
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 422);
        assert!(resp.body().contains("SQL error"));
    }

    #[tokio::test]
    async fn test_mode_param_parsed() {
        assert_eq!(crate::server::federation::parse_query_mode("buffer"), QueryMode::Buffer);
        assert_eq!(crate::server::federation::parse_query_mode("all"), QueryMode::All);
        assert_eq!(crate::server::federation::parse_query_mode("bogus"), QueryMode::History);
        assert_eq!(crate::server::federation::parse_query_mode(""), QueryMode::History);
    }
}
