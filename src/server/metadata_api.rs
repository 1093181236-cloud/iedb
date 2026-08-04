// HTTP handlers for metadata queries: list databases / list tables / get table detail.
use crate::server::metadata_store::MetadataStore;
use hyper::{Method, Request, Response};
use std::sync::Arc;

pub struct MetadataApiHandler {
    pub store: Arc<MetadataStore>,
}

impl MetadataApiHandler {
    pub async fn handle<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
        hyper::Error: From<B::Error>,
    {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let (method, path) = (method, path.as_str());
        match (method, path) {
            (Method::GET, "/api/v1/metadata/databases") => self.list_databases().await,
            (Method::GET, p) if p.starts_with("/api/v1/metadata/tables") => self.list_tables(req).await,
            (Method::GET, p) if p.starts_with("/api/v1/metadata/table") => self.get_table(req).await,
            _ => Ok(json_err(404, "NOT_FOUND")),
        }
    }

    async fn list_databases(&self) -> Result<Response<String>, hyper::Error> {
        match self.store.list_databases().await {
            Ok(names) => {
                let dbs: Vec<serde_json::Value> = names.iter().map(|n| serde_json::json!({"name": n})).collect();
                Ok(json_ok(&serde_json::json!({"databases": dbs}).to_string()))
            }
            Err(e) => Ok(json_err(500, &e)),
        }
    }

    async fn list_tables<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
    {
        let db = parse_query_param(req.uri().query(), "db").unwrap_or_default();
        match self.store.list_tables(&db).await {
            Ok(tables) => Ok(json_ok(&serde_json::json!({"database": db, "tables": tables}).to_string())),
            Err(e) => Ok(json_err(500, &e)),
        }
    }

    async fn get_table<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
    {
        let db = parse_query_param(req.uri().query(), "db").unwrap_or_default();
        let table = parse_query_param(req.uri().query(), "table").unwrap_or_default();
        match self.store.get_table(&db, &table).await {
            Ok(Some(t)) => Ok(json_ok(&serde_json::to_string(&t).unwrap_or_default())),
            Ok(None) => Ok(json_err(404, "NOT_FOUND")),
            Err(e) => Ok(json_err(500, &e)),
        }
    }
}

fn parse_query_param(query: Option<&str>, key: &str) -> Option<String> {
    query.and_then(|q| {
        url::form_urlencoded::parse(q.as_bytes())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
    })
}

fn json_ok(body: &str) -> Response<String> {
    Response::builder()
        .status(200)
        .header("Content-Type", "application/json")
        .body(body.into())
        .unwrap()
}

fn json_err(status: u16, msg: &str) -> Response<String> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(format!(
            r#"{{"error":"{}","code":"{}"}}"#,
            msg,
            if status == 404 { "NOT_FOUND" } else { "INTERNAL" }
        ))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::db::Db;
    use crate::server::test_util::TestBody;
    use tempfile::tempdir;

    fn test_handler() -> (tempfile::TempDir, MetadataApiHandler) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();
        let handler = MetadataApiHandler { store: Arc::new(MetadataStore::new(Arc::new(db))) };
        (dir, handler)
    }

    async fn seed(store: &MetadataStore) {
        store
            .update_stats(
                "metrics", "cpu",
                1000, 3000, 10,
                &[("usage".to_string(), "Float".to_string())],
                &["host".to_string()],
            )
            .await
            .unwrap();
        store
            .update_stats(
                "metrics", "mem",
                500, 2000, 8,
                &[("used".to_string(), "Int64".to_string())],
                &[],
            )
            .await
            .unwrap();
        store
            .update_stats(
                "logs", "events",
                0, 100, 5,
                &[("level".to_string(), "String".to_string())],
                &[],
            )
            .await
            .unwrap();
    }

    fn get_req(uri: &str) -> Request<TestBody> {
        Request::builder().method("GET").uri(uri).body(TestBody::empty()).unwrap()
    }

    #[tokio::test]
    async fn test_list_databases() {
        let (_dir, h) = test_handler();
        seed(&h.store).await;

        let resp = h.handle(get_req("/api/v1/metadata/databases")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        let names: Vec<&str> = v["databases"].as_array().unwrap().iter().map(|d| d["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["logs", "metrics"]);
    }

    #[tokio::test]
    async fn test_list_tables() {
        let (_dir, h) = test_handler();
        seed(&h.store).await;

        let resp = h.handle(get_req("/api/v1/metadata/tables?db=metrics")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["database"], "metrics");
        let tables = v["tables"].as_array().unwrap();
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0]["name"], "cpu");
        assert_eq!(tables[0]["row_count"], 10);
        assert_eq!(tables[0]["time_min"], 1000);
        assert_eq!(tables[0]["time_max"], 3000);
        assert_eq!(tables[0]["tag_keys"], serde_json::json!(["host"]));
        assert_eq!(tables[0]["field_count"], 1);

        // 未知 db → 空列表
        let resp = h.handle(get_req("/api/v1/metadata/tables?db=nope")).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["tables"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_get_table() {
        let (_dir, h) = test_handler();
        seed(&h.store).await;

        let resp = h.handle(get_req("/api/v1/metadata/table?db=metrics&table=cpu")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["database"], "metrics");
        assert_eq!(v["table"], "cpu");
        assert_eq!(v["row_count"], 10);
        assert_eq!(v["tag_keys"], serde_json::json!(["host"]));
        assert_eq!(v["fields"][0]["name"], "usage");
        assert_eq!(v["fields"][0]["type"], "Float");

        // 未知表 → 404
        let resp = h.handle(get_req("/api/v1/metadata/table?db=metrics&table=nope")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 404);
        assert!(resp.body().contains("NOT_FOUND"));

        // 未知路由 → 404
        let resp = h.handle(get_req("/api/v1/metadata/whatever")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 404);
    }
}
