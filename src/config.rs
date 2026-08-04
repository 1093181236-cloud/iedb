use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub data: DataConfig,
    #[serde(default = "default_wal")]
    pub wal: WalConfig,
    #[serde(default = "default_flush")]
    pub flush: FlushConfig,
    #[serde(default)]
    pub s3: Option<S3Config>,
    #[cfg(feature = "agent")]
    #[serde(default)]
    pub agent: Option<AgentClientConfig>,
    #[cfg(feature = "server")]
    #[serde(default)]
    pub query: Option<QueryConfig>,
    #[cfg(feature = "server")]
    #[serde(default)]
    pub compaction: Option<CompactionConfig>,
    #[cfg(feature = "server")]
    #[serde(default)]
    pub agents: Option<AgentsConfig>,
    #[cfg(feature = "server")]
    #[serde(default)]
    pub metadata: Option<MetadataConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

fn default_host() -> String { "0.0.0.0".into() }
fn default_port() -> u16 { 8080 }
fn default_max_body_bytes() -> usize { 10 * 1024 * 1024 }

#[derive(Debug, Clone, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_data_dir")]
    pub dir: PathBuf,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/iedb-agent")
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalConfig {
    #[serde(default = "default_wal_flush_interval")]
    pub flush_interval_secs: u64,
    #[serde(default = "default_max_write_buffer_ops")]
    pub max_write_buffer_ops: usize,
}

fn default_wal_flush_interval() -> u64 { 1 }
fn default_max_write_buffer_ops() -> usize { 100_000 }

fn default_wal() -> WalConfig {
    WalConfig {
        flush_interval_secs: default_wal_flush_interval(),
        max_write_buffer_ops: default_max_write_buffer_ops(),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlushConfig {
    #[serde(default = "default_snapshot_interval")]
    pub snapshot_interval: String,  // e.g. "10m"
    #[serde(default = "default_backend")]
    pub backend: String,            // "http" or "s3"
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,       // e.g. "512MB"
}

fn default_snapshot_interval() -> String { "10m".into() }
fn default_backend() -> String { "http".into() }
fn default_memory_limit() -> String { "512MB".into() }

fn default_flush() -> FlushConfig {
    FlushConfig {
        snapshot_interval: default_snapshot_interval(),
        backend: default_backend(),
        memory_limit: default_memory_limit(),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

#[cfg(feature = "agent")]
#[derive(Debug, Clone, Deserialize)]
pub struct AgentClientConfig {
    pub id: String,
    pub server_url: String,
}

#[cfg(feature = "server")]
#[derive(Debug, Clone, Deserialize)]
pub struct QueryConfig {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: u64,
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,
}

#[cfg(feature = "server")]
fn default_query_timeout() -> u64 { 30 }
#[cfg(feature = "server")]
fn default_max_rows() -> usize { 10000 }
#[cfg(feature = "server")]
fn default_max_concurrent_queries() -> usize { 4 }

#[cfg(feature = "server")]
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_compaction_schedule")]
    pub schedule: String,
    #[serde(default = "default_min_file_size_mb")]
    pub min_file_size_mb: u64,
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: u64,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
}

#[cfg(feature = "server")]
fn default_true() -> bool { true }
#[cfg(feature = "server")]
fn default_compaction_schedule() -> String { "0 */6 * * *".into() }
#[cfg(feature = "server")]
fn default_min_file_size_mb() -> u64 { 10 }
#[cfg(feature = "server")]
fn default_target_file_size_mb() -> u64 { 128 }
#[cfg(feature = "server")]
fn default_max_concurrent() -> usize { 2 }

#[cfg(feature = "server")]
#[derive(Debug, Clone, Deserialize)]
pub struct AgentsConfig {
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
    #[serde(default = "default_offline_cleanup_days")]
    pub offline_cleanup_days: u64,
    #[serde(default)]
    pub default_config: Option<AgentDefaultConfig>,
}

#[cfg(feature = "server")]
fn default_heartbeat_timeout() -> u64 { 30 }
#[cfg(feature = "server")]
fn default_offline_cleanup_days() -> u64 { 7 }

#[cfg(feature = "server")]
#[derive(Debug, Clone, Deserialize)]
pub struct AgentDefaultConfig {
    pub flush: Option<FlushConfig>,
    pub wal: Option<WalConfig>,
    pub s3: Option<S3Config>,
}

#[cfg(feature = "server")]
#[derive(Debug, Clone, Deserialize)]
pub struct MetadataConfig {
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,
}

#[cfg(feature = "server")]
fn default_db_path() -> PathBuf {
    PathBuf::from("/var/lib/iedb/iedb.db")
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn snapshot_interval_secs(&self) -> i64 {
        parse_duration(&self.flush.snapshot_interval)
    }

    pub fn memory_limit_bytes(&self) -> usize {
        parse_bytes(&self.flush.memory_limit)
    }

    pub fn max_body_bytes(&self) -> usize {
        self.server.max_body_bytes
    }
}

fn parse_duration(s: &str) -> i64 {
    let s = s.trim();
    if s.ends_with('m') {
        s[..s.len()-1].parse::<i64>().unwrap_or(10) * 60
    } else if s.ends_with('s') {
        s[..s.len()-1].parse::<i64>().unwrap_or(600)
    } else {
        600
    }
}

fn parse_bytes(s: &str) -> usize {
    let s = s.trim().to_uppercase();
    if s.ends_with("MB") {
        s[..s.len()-2].parse::<usize>().unwrap_or(512) * 1024 * 1024
    } else if s.ends_with("GB") {
        s[..s.len()-2].parse::<usize>().unwrap_or(1) * 1024 * 1024 * 1024
    } else {
        512 * 1024 * 1024
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_toml(name: &str, content: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("iedb_cfg_{}", name));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    #[test]
    fn test_parse_duration_values() {
        assert_eq!(parse_duration("10m"), 600);
        assert_eq!(parse_duration("60s"), 60);
        assert_eq!(parse_duration("5m"), 300);
        assert_eq!(parse_duration("90s"), 90);
    }

    #[test]
    fn test_parse_duration_unknown_returns_default() {
        assert_eq!(parse_duration("abc"), 600);
        assert_eq!(parse_duration(""), 600);
    }

    #[test]
    fn test_parse_bytes_values() {
        assert_eq!(parse_bytes("512MB"), 536_870_912);
        assert_eq!(parse_bytes("1GB"), 1_073_741_824);
        assert_eq!(parse_bytes("256MB"), 268_435_456);
        assert_eq!(parse_bytes("2GB"), 2_147_483_648);
    }

    #[test]
    fn test_parse_bytes_unknown_returns_default() {
        assert_eq!(parse_bytes("abc"), 536_870_912);
        assert_eq!(parse_bytes(""), 536_870_912);
    }

    #[test]
    fn test_default_values_when_fields_missing() {
        let config: Config = toml::from_str(
            r#"
            [server]
            [data]
            [wal]
            [flush]
            "#,
        )
        .unwrap();

        assert_eq!(config.flush.snapshot_interval, "10m");
        assert_eq!(config.flush.memory_limit, "512MB");
        assert_eq!(config.flush.backend, "http");
        assert_eq!(config.wal.max_write_buffer_ops, 100_000);
        assert_eq!(config.wal.flush_interval_secs, 1);
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.max_body_bytes, 10 * 1024 * 1024);
        assert_eq!(config.max_body_bytes(), 10 * 1024 * 1024);
        assert_eq!(config.data.dir, std::path::PathBuf::from("/var/lib/iedb-agent"));
        #[cfg(feature = "agent")]
        assert!(config.agent.is_none());
    }

    #[test]
    fn test_full_config_from_toml_file() {
        let name = format!("full_cfg_{}", std::process::id());
        let path = write_temp_toml(
            &name,
            r#"
            [server]
            port = 9090

            [data]
            dir = "/tmp/test-data"

            [wal]
            flush_interval_secs = 5
            max_write_buffer_ops = 50000

            [flush]
            snapshot_interval = "2m"
            backend = "s3"
            memory_limit = "256MB"

            [s3]
            bucket = "my-bucket"
            region = "us-east-1"
            endpoint = "https://s3.example.com"
            access_key = "AKID"
            secret_key = "secret"

            [agent]
            id = "agent-01"
            server_url = "http://localhost:8080"

            [query]
            data_dir = "/tmp/query-data"
            query_timeout_secs = 60
            max_rows = 5000
            max_concurrent_queries = 8

            [compaction]
            enabled = false
            schedule = "0 */12 * * *"
            min_file_size_mb = 20
            target_file_size_mb = 256
            max_concurrent = 3

            [agents]
            heartbeat_timeout_secs = 60
            offline_cleanup_days = 14

            [agents.default_config]
            [agents.default_config.flush]
            snapshot_interval = "5m"

            [metadata]
            db_path = "/tmp/iedb.db"
            "#,
        );

        let config = Config::from_file(path.to_str().unwrap()).unwrap();
        assert_eq!(config.server.port, 9090);
        assert_eq!(config.data.dir, std::path::PathBuf::from("/tmp/test-data"));
        assert_eq!(config.wal.flush_interval_secs, 5);
        assert_eq!(config.wal.max_write_buffer_ops, 50000);
        assert_eq!(config.flush.snapshot_interval, "2m");
        assert_eq!(config.flush.backend, "s3");
        assert_eq!(config.flush.memory_limit, "256MB");
        #[cfg(feature = "agent")]
        {
            let agent = config.agent.as_ref().unwrap();
            assert_eq!(agent.id, "agent-01");
            assert_eq!(agent.server_url, "http://localhost:8080");
        }
        #[cfg(feature = "server")]
        {
            let query = config.query.as_ref().unwrap();
            assert_eq!(query.data_dir, std::path::PathBuf::from("/tmp/query-data"));
            assert_eq!(query.query_timeout_secs, 60);
            assert_eq!(query.max_rows, 5000);
            assert_eq!(query.max_concurrent_queries, 8);

            let compaction = config.compaction.as_ref().unwrap();
            assert!(!compaction.enabled);
            assert_eq!(compaction.schedule, "0 */12 * * *");
            assert_eq!(compaction.min_file_size_mb, 20);
            assert_eq!(compaction.target_file_size_mb, 256);
            assert_eq!(compaction.max_concurrent, 3);

            let agents = config.agents.as_ref().unwrap();
            assert_eq!(agents.heartbeat_timeout_secs, 60);
            assert_eq!(agents.offline_cleanup_days, 14);
            let default_config = agents.default_config.as_ref().unwrap();
            assert_eq!(
                default_config.flush.as_ref().unwrap().snapshot_interval,
                "5m"
            );

            let metadata = config.metadata.as_ref().unwrap();
            assert_eq!(metadata.db_path, std::path::PathBuf::from("/tmp/iedb.db"));
        }
        // Verify derived helpers
        assert_eq!(config.snapshot_interval_secs(), 120);
        assert_eq!(config.memory_limit_bytes(), 268_435_456);

        // Cleanup
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        // Verify default values for fields we didn't set
        assert_eq!(config.wal.flush_interval_secs, 5); // explicitly set
    }

    #[cfg(feature = "agent")]
    #[test]
    fn test_agent_minimal_config() {
        let config: Config = toml::from_str(r#"
            [server]
            port = 8080
            [data]
            dir = "/tmp/test"
            [agent]
            id = "test-agent"
            server_url = "http://localhost:8080"
        "#).unwrap();
        assert_eq!(config.agent.as_ref().unwrap().id, "test-agent");
        assert_eq!(config.agent.as_ref().unwrap().server_url, "http://localhost:8080");
    }

    #[cfg(feature = "server")]
    #[test]
    fn test_server_config_with_query_and_compaction() {
        let config: Config = toml::from_str(r#"
            [server]
            host = "0.0.0.0"
            port = 8080
            [data]
            dir = "/tmp/data"
            [query]
            max_rows = 5000
            [compaction]
            min_file_size_mb = 20
            [agents]
            heartbeat_timeout_secs = 60
            [metadata]
            db_path = "/tmp/iedb.db"
        "#).unwrap();
        assert_eq!(config.query.as_ref().unwrap().max_rows, 5000);
        assert_eq!(config.compaction.as_ref().unwrap().min_file_size_mb, 20);
        assert_eq!(config.agents.as_ref().unwrap().heartbeat_timeout_secs, 60);
        assert_eq!(config.metadata.as_ref().unwrap().db_path, std::path::PathBuf::from("/tmp/iedb.db"));
    }
}
