// iedb/src/agent/mod.rs
pub mod model;
pub mod buffer;
pub mod wal;
pub mod flush;
pub mod write;
pub mod query;

use crate::config::{AgentClientConfig, Config};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize)]
struct RegisterRequest {
    pub id: String,
    pub version: String,
    pub hostname: String,
    pub arch: String,
}

#[derive(Debug, Deserialize)]
struct RegisterResponse {
    // Not read yet: register() currently returns only config + config_version.
    #[allow(dead_code)]
    pub agent_id: String,
    pub config: serde_json::Value,
    pub config_version: u64,
}

#[derive(Debug, Serialize)]
struct HeartbeatRequest {
    pub id: String,
    pub config_version: u64,
    pub schema_changes: Vec<SchemaChange>,
}

#[derive(Debug, Serialize)]
pub struct SchemaChange {
    pub db: String,
    pub table: String,
    pub tag_keys: Vec<String>,
    pub field_defs: Vec<(String, String)>, // [(name, type), ...]
}

#[derive(Debug, Deserialize)]
struct HeartbeatResponse {
    pub config_update: Option<serde_json::Value>,
}

pub struct AgentClient {
    pub config: Arc<Config>,
    pub client: Client,
}

impl AgentClient {
    pub fn new(config: Arc<Config>) -> Self {
        AgentClient {
            config,
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("failed to create HTTP client"),
        }
    }

    pub async fn register(&self) -> Result<(serde_json::Value, u64), String> {
        let agent_cfg: &AgentClientConfig = self
            .config
            .agent
            .as_ref()
            .ok_or("missing [agent] config")?;
        let req = RegisterRequest {
            id: agent_cfg.id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            arch: std::env::consts::ARCH.to_string(),
        };
        let url = format!("{}/api/v1/agents/register", agent_cfg.server_url);
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("register request: {}", e))?;
        if !resp.status().is_success() {
            return Err(format!("register failed: {}", resp.status()));
        }
        let body: RegisterResponse = resp
            .json()
            .await
            .map_err(|e| format!("register response: {}", e))?;
        Ok((body.config, body.config_version))
    }

    pub async fn heartbeat(
        &self,
        config_version: u64,
        schema_changes: Vec<SchemaChange>,
    ) -> Result<Option<serde_json::Value>, String> {
        let agent_cfg: &AgentClientConfig = self
            .config
            .agent
            .as_ref()
            .ok_or("missing [agent] config")?;
        let req = HeartbeatRequest {
            id: agent_cfg.id.clone(),
            config_version,
            schema_changes,
        };
        let url = format!("{}/api/v1/agents/heartbeat", agent_cfg.server_url);
        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("heartbeat request: {}", e))?;
        if !resp.status().is_success() {
            return Err(format!("heartbeat failed: {}", resp.status()));
        }
        let body: HeartbeatResponse = resp
            .json()
            .await
            .map_err(|e| format!("heartbeat response: {}", e))?;
        Ok(body.config_update)
    }
}
