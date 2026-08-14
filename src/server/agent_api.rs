// HTTP handlers for agent management: register / heartbeat / list / get / update-config / delete.
use crate::server::agent_store::AgentStore;
use crate::server::metadata_store::MetadataStore;
use hyper::{Method, Request, Response};
use std::sync::Arc;

pub struct AgentApiHandler {
    pub store: Arc<AgentStore>,
    pub metadata: Option<Arc<MetadataStore>>,
}

impl AgentApiHandler {
    /// Routes one incoming request. `B` is any http body (the server layer
    /// passes `hyper::body::Incoming`; tests pass `Full<Bytes>`).
    pub async fn handle<B>(
        &self,
        req: Request<B>,
    ) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
        hyper::Error: From<B::Error>,
    {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let (method, path) = (method, path.as_str());

        match (method, path) {
            (Method::POST, "/api/v1/agents/register") => self.handle_register(req).await,
            (Method::POST, "/api/v1/agents/heartbeat") => self.handle_heartbeat(req).await,
            (Method::GET, "/api/v1/agents") => self.handle_list().await,
            (Method::GET, p) if p.starts_with("/api/v1/agents/") => {
                let id = &p["/api/v1/agents/".len()..];
                self.handle_get(id).await
            }
            (Method::PUT, p) if p.starts_with("/api/v1/agents/") && p.ends_with("/config") => {
                let id = &p["/api/v1/agents/".len()..p.len() - "/config".len()];
                self.handle_update_config(id, req).await
            }
            (Method::DELETE, p) if p.starts_with("/api/v1/agents/") => {
                let id = &p["/api/v1/agents/".len()..];
                self.handle_delete(id).await
            }
            _ => Ok(json_response(404, r#"{"error":"not found","code":"NOT_FOUND"}"#)),
        }
    }

    async fn handle_register<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
        hyper::Error: From<B::Error>,
    {
        use http_body_util::BodyExt;
        let body = req.collect().await?.to_bytes();
        let req_data: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let id = req_data["id"].as_str().unwrap_or("");
        let hostname = req_data["hostname"].as_str().unwrap_or("unknown");
        let arch = req_data["arch"].as_str().unwrap_or("unknown");
        let version = req_data["version"].as_str().unwrap_or("0.1.0");
        let listen_addr = req_data["listen_addr"].as_str().unwrap_or("unknown");

        if id.is_empty() {
            return Ok(json_response(400, r#"{"error":"agent id required","code":"BAD_REQUEST"}"#));
        }

        match self.store.register(id, hostname, arch, version, listen_addr).await {
            Ok(agent) => {
                let resp = serde_json::json!({
                    "agent_id": agent.id,
                    "config": agent.config_json.as_ref().map(|c| serde_json::from_str::<serde_json::Value>(c).unwrap_or_default()).unwrap_or(serde_json::Value::Null),
                    "config_version": agent.config_version,
                });
                Ok(json_response(200, &resp.to_string()))
            }
            Err(e) => Ok(json_response(500, &format!(r#"{{"error":"{}","code":"INTERNAL"}}"#, e))),
        }
    }

    async fn handle_heartbeat<B>(&self, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
        hyper::Error: From<B::Error>,
    {
        use http_body_util::BodyExt;
        let body = req.collect().await?.to_bytes();
        let req_data: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let id = req_data["id"].as_str().unwrap_or("");
        let config_version = req_data["config_version"].as_u64().unwrap_or(0);

        // Try heartbeat — returns false if agent not found
        match self.store.heartbeat(id, config_version).await {
            Ok(true) => {}  // agent exists, heartbeat updated
            Ok(false) => {
                return Ok(json_response(404,
                    &format!(r#"{{"error":"agent {} not found","code":"NOT_FOUND"}}"#, id)));
            }
            Err(e) => {
                return Ok(json_response(500, &format!(r#"{{"error":"{}","code":"INTERNAL"}}"#, e)));
            }
        }

        // I4: extract and forward schema_changes to metadata store
        if let Some(ref md) = self.metadata {
            if let Some(changes) = req_data["schema_changes"].as_array() {
                for entry in changes {
                    let db = entry["db"].as_str().unwrap_or_default();
                    let table = entry["table"].as_str().unwrap_or_default();
                    let tag_keys: Vec<String> = entry["tag_keys"].as_array()
                        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                        .unwrap_or_default();
                    let field_defs: Vec<(String, String)> = entry["field_defs"].as_array()
                        .map(|a| a.iter().filter_map(|v| {
                            let arr = v.as_array()?;
                            Some((arr.get(0)?.as_str()?.to_string(), arr.get(1)?.as_str()?.to_string()))
                        }).collect())
                        .unwrap_or_default();
                    if !db.is_empty() && !table.is_empty() {
                        // The agent row exists (heartbeat required it), so
                        // the agent_tables insert normally succeeds; on the
                        // off chance it fails, log it instead of silently
                        // losing the mapping.
                        if let Err(e) = md.merge_schema(db, table, id, &tag_keys, &field_defs).await {
                            tracing::warn!(agent = %id, db = %db, table = %table,
                                "merge_schema from heartbeat: {}", e);
                        }
                    }
                }
            }
        }

        // 检查是否有配置更新
        let should_update = self.store.should_update_config(id, config_version).await.unwrap_or(false);
        let config_update: Option<serde_json::Value> = if should_update {
            let agent = self.store.get(id).await.ok();
            agent
                .and_then(|a| a.config_json)
                .and_then(|c| serde_json::from_str(&c).ok())
        } else {
            None
        };

        let resp = serde_json::json!({ "config_update": config_update });
        Ok(json_response(200, &resp.to_string()))
    }

    async fn handle_list(&self) -> Result<Response<String>, hyper::Error> {
        match self.store.list().await {
            Ok(agents) => {
                let now_ms = chrono::Utc::now().timestamp_millis();
                let result: Vec<serde_json::Value> = agents.iter().map(|a| {
                    let status = match a.last_seen_at {
                        None => "unknown",
                        Some(ts) if now_ms - ts <= 30000 => "online",
                        _ => "offline",
                    };
                    serde_json::json!({
                        "id": a.id, "hostname": a.hostname, "status": status,
                        "last_seen": a.last_seen_at.map(ms_to_iso8601),
                        "config_version": a.config_version,
                        "target_config_version": a.target_config_version,
                        "listen_addr": a.listen_addr,
                    })
                }).collect();
                Ok(json_response(200, &serde_json::json!({"agents": result}).to_string()))
            }
            Err(e) => Ok(json_response(500, &format!(r#"{{"error":"{}","code":"INTERNAL"}}"#, e))),
        }
    }

    async fn handle_get(&self, id: &str) -> Result<Response<String>, hyper::Error> {
        match self.store.get(id).await {
            Ok(agent) => Ok(json_response(200, &serde_json::to_string(&agent).unwrap_or_default())),
            Err(_) => Ok(json_response(404, r#"{"error":"agent not found","code":"NOT_FOUND"}"#)),
        }
    }

    async fn handle_update_config<B>(&self, id: &str, req: Request<B>) -> Result<Response<String>, hyper::Error>
    where
        B: hyper::body::Body,
        hyper::Error: From<B::Error>,
    {
        use http_body_util::BodyExt;
        let body = req.collect().await?.to_bytes();
        match self.store.update_config(id, &String::from_utf8_lossy(&body)).await {
            Ok(version) => {
                let resp = serde_json::json!({"target_version": version, "message": "config will apply on next heartbeat"});
                Ok(json_response(200, &resp.to_string()))
            }
            Err(e) => Ok(json_response(404, &format!(r#"{{"error":"{}","code":"NOT_FOUND"}}"#, e))),
        }
    }

    async fn handle_delete(&self, id: &str) -> Result<Response<String>, hyper::Error> {
        match self.store.delete(id).await {
            Ok(()) => Ok(json_response(200, r#"{"message":"deleted"}"#)),
            Err(e) => Ok(json_response(500, &format!(r#"{{"error":"{}","code":"INTERNAL"}}"#, e))),
        }
    }
}

/// I9 fix: convert Unix ms to ISO 8601 string
fn ms_to_iso8601(ts_ms: i64) -> String {
    let secs = ts_ms / 1000;
    let nanos = ((ts_ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| format!("{}", ts_ms))
}

fn json_response(status: u16, body: &str) -> Response<String> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::db::Db;
    use crate::server::test_util::TestBody;
    use bytes::Bytes;
    use tempfile::tempdir;

    fn test_handler() -> (tempfile::TempDir, AgentApiHandler) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();
        let handler = AgentApiHandler { store: Arc::new(AgentStore::new(Arc::new(db))), metadata: None };
        (dir, handler)
    }

    fn json_req(method: &str, uri: &str, body: &str) -> Request<TestBody> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("Content-Type", "application/json")
            .body(TestBody::from_bytes(Bytes::from(body.to_string())))
            .unwrap()
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let (_dir, h) = test_handler();

        let resp = h.handle(json_req("POST", "/api/v1/agents/register",
            r#"{"id":"agent-01","hostname":"edge-01","arch":"armv7","version":"0.1.0","listen_addr":"192.168.0.230:18080"}"#)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["agent_id"], "agent-01");
        assert_eq!(v["config_version"], 1);

        // 缺少 id → 400
        let resp = h.handle(json_req("POST", "/api/v1/agents/register", r#"{"hostname":"x"}"#)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 400);

        // GET 单个 agent
        let resp = h.handle(json_req("GET", "/api/v1/agents/agent-01", "")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["id"], "agent-01");
        assert_eq!(v["hostname"], "edge-01");

        // 未知 agent → 404
        let resp = h.handle(json_req("GET", "/api/v1/agents/nope", "")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 404);
        assert!(resp.body().contains("NOT_FOUND"));
    }

    #[tokio::test]
    async fn test_list_and_heartbeat() {
        let (_dir, h) = test_handler();

        h.handle(json_req("POST", "/api/v1/agents/register", r#"{"id":"a1","hostname":"h1","listen_addr":"10.0.0.1:8080"}"#)).await.unwrap();
        h.handle(json_req("POST", "/api/v1/agents/register", r#"{"id":"a2","hostname":"h2","listen_addr":"10.0.0.2:8080"}"#)).await.unwrap();

        // 刚注册 → online/unknown
        let resp = h.handle(json_req("GET", "/api/v1/agents", "")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        let agents = v["agents"].as_array().unwrap();
        assert_eq!(agents.len(), 2);
        assert!(agents.iter().all(|a| a["status"] == "online" || a["status"] == "unknown"));

        // heartbeat 推进 last_seen，无配置更新
        let resp = h.handle(json_req("POST", "/api/v1/agents/heartbeat", r#"{"id":"a1","config_version":1}"#)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["config_update"], serde_json::Value::Null);

        // 未知 agent heartbeat → 404
        let resp = h.handle(json_req("POST", "/api/v1/agents/heartbeat", r#"{"id":"ghost"}"#)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 404);
    }

    #[tokio::test]
    async fn test_update_config_then_heartbeat_picks_it_up() {
        let (_dir, h) = test_handler();

        h.handle(json_req("POST", "/api/v1/agents/register", r#"{"id":"a1","hostname":"h1","listen_addr":"10.0.0.1:8080"}"#)).await.unwrap();

        // 推送配置 → target_version 递增
        let resp = h.handle(json_req("PUT", "/api/v1/agents/a1/config", r#"{"batch":1024}"#)).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["target_version"], 2);

        // 客户端还停留在 version 1 → 返回 config_update
        let resp = h.handle(json_req("POST", "/api/v1/agents/heartbeat", r#"{"id":"a1","config_version":1}"#)).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["config_update"], serde_json::json!({"batch":1024}));

        // 客户端已应用 version 2 → 无更新
        let resp = h.handle(json_req("POST", "/api/v1/agents/heartbeat", r#"{"id":"a1","config_version":2}"#)).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(resp.body()).unwrap();
        assert_eq!(v["config_update"], serde_json::Value::Null);

        // 删除后 GET → 404
        let resp = h.handle(json_req("DELETE", "/api/v1/agents/a1", "")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let resp = h.handle(json_req("GET", "/api/v1/agents/a1", "")).await.unwrap();
        assert_eq!(resp.status().as_u16(), 404);
    }
}
