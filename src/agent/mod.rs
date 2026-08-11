// iedb/src/agent/mod.rs
pub mod model;
pub mod buffer;
pub mod wal;
pub mod flush;
pub mod write;
pub mod query;
pub mod system;

use crate::config::{AgentClientConfig, Config};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Serialize)]
struct RegisterRequest {
    pub id: String,
    pub version: String,
    pub hostname: String,
    pub arch: String,
    pub listen_addr: String,
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
    pub listen_addr: String,
    pub heartbeat_failures: AtomicU64,
}

impl AgentClient {
    pub fn new(config: Arc<Config>, listen_addr: String) -> Self {
        AgentClient {
            config,
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("failed to create HTTP client"),
            listen_addr,
            heartbeat_failures: AtomicU64::new(0),
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
            listen_addr: self.listen_addr.clone(),
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
            self.heartbeat_failures.fetch_add(1, Ordering::Relaxed);
            return Err(format!("heartbeat failed: {}", resp.status()));
        }
        self.heartbeat_failures.store(0, Ordering::Relaxed);
        let body: HeartbeatResponse = resp
            .json()
            .await
            .map_err(|e| format!("heartbeat response: {}", e))?;
        Ok(body.config_update)
    }
}

/// Compare the buffer's current schema against `last_state`, returning the
/// tables whose schema changed since the previous heartbeat. The first call
/// (empty `last_state`) reports every table as changed. Used by the agent
/// heartbeat loop; extracted here so the diff logic is unit-testable.
pub fn compute_schema_changes(
    buffer: &crate::agent::buffer::Buffer,
    last_state: &mut std::collections::HashMap<String, (Vec<String>, Vec<(String, String)>)>,
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();
    for (db_name, tables) in &buffer.databases {
        for (table_name, table) in tables {
            let key = format!("{}.{}", db_name, table_name);
            let tag_keys = table.schema.tag_keys.clone();
            let field_defs: Vec<(String, String)> = table.schema
                .field_defs
                .iter()
                .map(|f| (f.name.clone(), format!("{:?}", f.value_type)))
                .collect();
            let current = (tag_keys.clone(), field_defs.clone());
            let changed = last_state.get(&key).map_or(true, |prev| prev != &current);
            if changed {
                changes.push(SchemaChange {
                    db: db_name.clone(),
                    table: table_name.clone(),
                    tag_keys,
                    field_defs,
                });
                last_state.insert(key, current);
            }
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::buffer::Buffer;
    use crate::agent::buffer::chunk::{Chunk, FieldType, FieldValue, Row};

    /// Buffer with one table `mydb.cpu`: tags [host], fields [cpu, mem],
    /// one chunk containing a single row.
    fn buffer_with_schema() -> Buffer {
        let mut buffer = Buffer::new();
        let table = buffer.get_or_create_table("mydb", "cpu");
        table.schema.ensure_tag_key("host");
        table.schema.ensure_field("cpu", FieldType::F64);
        table.schema.ensure_field("mem", FieldType::F64);

        let mut chunk = Chunk::new(0);
        chunk.rows.push(Row {
            time: 100,
            tag_values: vec!["srv01".to_string()],
            field_values: vec![
                Some(FieldValue::F64(75.5)),
                Some(FieldValue::F64(62.3)),
            ],
        });
        table.chunks.push(chunk);
        buffer
    }

    #[test]
    fn test_first_call_reports_all_tables() {
        let buffer = buffer_with_schema();
        let mut last_schema = std::collections::HashMap::new();

        let changes = compute_schema_changes(&buffer, &mut last_schema);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].db, "mydb");
        assert_eq!(changes[0].table, "cpu");
        assert_eq!(changes[0].tag_keys, vec!["host".to_string()]);
        assert_eq!(changes[0].field_defs.len(), 2);
        assert!(changes[0].field_defs.iter().any(|(n, t)| n == "cpu" && t == "F64"));
    }

    #[test]
    fn test_unchanged_schema_yields_no_changes() {
        let buffer = buffer_with_schema();
        let mut last_schema = std::collections::HashMap::new();

        let _ = compute_schema_changes(&buffer, &mut last_schema);
        let changes2 = compute_schema_changes(&buffer, &mut last_schema);
        assert!(changes2.is_empty(), "unchanged schema should produce no changes");
    }

    #[test]
    fn test_detects_new_field() {
        let mut buffer = buffer_with_schema();
        let mut last_schema = std::collections::HashMap::new();
        let _ = compute_schema_changes(&buffer, &mut last_schema);

        buffer.get_table_mut("mydb", "cpu").unwrap()
            .schema.ensure_field("temp", FieldType::F64);

        let changes = compute_schema_changes(&buffer, &mut last_schema);
        assert_eq!(changes.len(), 1);
        assert!(changes[0].field_defs.iter().any(|(n, _)| n == "temp"),
            "should detect new field, got: {:?}", changes[0].field_defs);
    }

    #[test]
    fn test_detects_new_tag() {
        let mut buffer = buffer_with_schema();
        let mut last_schema = std::collections::HashMap::new();
        let _ = compute_schema_changes(&buffer, &mut last_schema);

        buffer.get_table_mut("mydb", "cpu").unwrap()
            .schema.ensure_tag_key("region");

        let changes = compute_schema_changes(&buffer, &mut last_schema);
        assert!(changes.iter().any(|c| c.tag_keys.contains(&"region".to_string())));
    }

    #[test]
    fn test_detects_new_table() {
        let mut buffer = buffer_with_schema();
        let mut last_schema = std::collections::HashMap::new();
        let _ = compute_schema_changes(&buffer, &mut last_schema);

        // 新表加入同一 db
        let table = buffer.get_or_create_table("mydb", "mem");
        table.schema.ensure_field("used", FieldType::F64);

        let changes = compute_schema_changes(&buffer, &mut last_schema);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].table, "mem");
        assert!(changes[0].field_defs.iter().any(|(n, _)| n == "used"));
    }

    #[tokio::test]
    async fn test_register_requires_agent_config() {
        let dir = tempfile::tempdir().unwrap();
        let toml = format!(
            r#"
            [server]
            [data]
            dir = "{}"
            "#,
            dir.path().display()
        );
        let config: crate::config::Config = toml::from_str(&toml).unwrap();
        let client = AgentClient::new(std::sync::Arc::new(config), "127.0.0.1:8080".into());

        let err = client.register().await;
        assert!(err.is_err(), "register without [agent] config should fail");
        assert!(err.unwrap_err().contains("missing [agent] config"));

        let err = client.heartbeat(1, Vec::new()).await;
        assert!(err.is_err(), "heartbeat without [agent] config should fail");
    }
}
