// Agent registration / heartbeat / config-version state, backed by SQLite.
use crate::server::db::Db;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub id: String,
    pub hostname: String,
    pub arch: Option<String>,
    pub version: Option<String>,
    pub config_json: Option<String>,
    pub config_version: i64,
    pub target_config_version: i64,
    pub registered_at: i64,
    pub last_seen_at: Option<i64>,
    pub listen_addr: Option<String>,
}

#[derive(Clone)]
pub struct AgentStore {
    db: Arc<Db>,
}

impl AgentStore {
    pub fn new(db: Arc<Db>) -> Self {
        AgentStore { db }
    }

    pub async fn register(&self, id: &str, hostname: &str, arch: &str, version: &str, listen_addr: &str) -> Result<AgentRecord, String> {
        {
            let conn = self.db.conn().lock().await;
            let now_ms = chrono::Utc::now().timestamp_millis();
            // 尝试 INSERT，冲突则 UPDATE（重复注册 = 重启）
            conn.execute(
                "INSERT INTO agents (id, hostname, arch, version, registered_at, last_seen_at, listen_addr)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)
                 ON CONFLICT(id) DO UPDATE SET
                   hostname=excluded.hostname, arch=excluded.arch, version=excluded.version,
                   registered_at=excluded.registered_at, last_seen_at=excluded.last_seen_at,
                   listen_addr=excluded.listen_addr",
                params![id, hostname, arch, version, now_ms, listen_addr],
            )
            .map_err(|e| format!("register: {}", e))?;
        }
        self.get(id).await
    }

    /// Returns Ok(true) if agent existed, Ok(false) if not found.
    pub async fn heartbeat(&self, id: &str, config_version: u64) -> Result<bool, String> {
        let conn = self.db.conn().lock().await;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let affected = conn.execute(
            "UPDATE agents SET last_seen_at=?1, config_version=MAX(config_version, ?2) WHERE id=?3",
            params![now_ms, config_version as i64, id],
        )
        .map_err(|e| format!("heartbeat: {}", e))?;
        Ok(affected > 0)
    }

    pub async fn get(&self, id: &str) -> Result<AgentRecord, String> {
        let conn = self.db.conn().lock().await;
        conn.query_row(
            "SELECT id, hostname, arch, version, config_json, config_version, target_config_version, registered_at, last_seen_at, listen_addr FROM agents WHERE id=?1",
            params![id],
            |row| {
                Ok(AgentRecord {
                    id: row.get(0)?, hostname: row.get(1)?, arch: row.get(2)?,
                    version: row.get(3)?, config_json: row.get(4)?, config_version: row.get(5)?,
                    target_config_version: row.get(6)?, registered_at: row.get(7)?,
                    last_seen_at: row.get(8)?, listen_addr: row.get(9)?,
                })
            },
        )
        .map_err(|e| format!("get agent: {}", e))
    }

    pub async fn list(&self) -> Result<Vec<AgentRecord>, String> {
        let conn = self.db.conn().lock().await;
        let mut stmt = conn
            .prepare(
                "SELECT id, hostname, arch, version, config_json, config_version, target_config_version, registered_at, last_seen_at, listen_addr FROM agents",
            )
            .map_err(|e| format!("list agents: {}", e))?;
        let records = stmt
            .query_map([], |row| {
                Ok(AgentRecord {
                    id: row.get(0)?, hostname: row.get(1)?, arch: row.get(2)?,
                    version: row.get(3)?, config_json: row.get(4)?, config_version: row.get(5)?,
                    target_config_version: row.get(6)?, registered_at: row.get(7)?,
                    last_seen_at: row.get(8)?, listen_addr: row.get(9)?,
                })
            })
            .map_err(|e| format!("list agents query: {}", e))?;
        records
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("list agents collect: {}", e))
    }

    pub async fn update_config(&self, id: &str, config_json: &str) -> Result<u64, String> {
        let conn = self.db.conn().lock().await;
        conn.execute(
            "UPDATE agents SET config_json=?1, target_config_version = target_config_version + 1 WHERE id=?2",
            params![config_json, id],
        )
        .map_err(|e| format!("update config: {}", e))?;
        let version: i64 = conn
            .query_row(
                "SELECT target_config_version FROM agents WHERE id=?1",
                params![id],
                |r| r.get(0),
            )
            .map_err(|e| format!("get target version: {}", e))?;
        Ok(version as u64)
    }

    pub async fn delete(&self, id: &str) -> Result<(), String> {
        let conn = self.db.conn().lock().await;
        conn.execute("DELETE FROM agent_tables WHERE agent_id=?1", params![id])
            .ok();
        conn.execute("DELETE FROM agents WHERE id=?1", params![id])
            .map_err(|e| format!("delete agent: {}", e))?;
        Ok(())
    }

    pub async fn should_update_config(&self, id: &str, current_client_version: u64) -> Result<bool, String> {
        let conn = self.db.conn().lock().await;
        let target: i64 = conn
            .query_row(
                "SELECT target_config_version FROM agents WHERE id=?1",
                params![id],
                |r| r.get(0),
            )
            .map_err(|e| format!("check version: {}", e))?;
        Ok((target as u64) > current_client_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::db::Db;
    use tempfile::tempdir;

    fn test_store() -> (tempfile::TempDir, AgentStore) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Db::open(&db_path).unwrap();
        (dir, AgentStore::new(Arc::new(db)))
    }

    #[tokio::test]
    async fn test_register_and_heartbeat() {
        let (_dir, store) = test_store();

        // 注册
        let agent = store.register("agent-01", "edge-01", "armv7", "0.1.0", "192.168.0.230:18080").await.unwrap();
        assert_eq!(agent.id, "agent-01");
        assert_eq!(agent.hostname, "edge-01");
        assert!(agent.last_seen_at.is_some());

        // 心跳
        store.heartbeat("agent-01", 1).await.unwrap();
        let agent2 = store.get("agent-01").await.unwrap();
        assert!(agent2.last_seen_at.unwrap() >= agent.last_seen_at.unwrap());

        // 重复注册（模拟重启）
        let agent3 = store.register("agent-01", "edge-01-v2", "armv7", "0.1.1", "192.168.0.230:18080").await.unwrap();
        assert_eq!(agent3.version.as_deref(), Some("0.1.1"));
    }

    #[tokio::test]
    async fn test_list_and_delete() {
        let (_dir, store) = test_store();

        store.register("a1", "h1", "x86", "1.0", "10.0.0.1:8080").await.unwrap();
        store.register("a2", "h2", "arm", "1.0", "10.0.0.2:8080").await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 2);

        store.delete("a1").await.unwrap();
        let list2 = store.list().await.unwrap();
        assert_eq!(list2.len(), 1);
    }

    #[tokio::test]
    async fn test_config_versions() {
        let (_dir, store) = test_store();

        store.register("a1", "h1", "x86", "1.0", "10.0.0.1:8080").await.unwrap();
        // 默认 target_config_version = 1（与客户端初始 config_version 一致）
        assert!(store.should_update_config("a1", 0).await.unwrap());
        assert!(!store.should_update_config("a1", 1).await.unwrap());

        let v = store.update_config("a1", "{\"batch\": 1024}").await.unwrap();
        assert_eq!(v, 2); // target_config_version 从 1 -> 2
        assert!(store.should_update_config("a1", 1).await.unwrap());
        assert!(!store.should_update_config("a1", 2).await.unwrap());

        let agent = store.get("a1").await.unwrap();
        assert_eq!(agent.config_json.as_deref(), Some("{\"batch\": 1024}"));
    }
}
