//! End-to-end smoke tests:
//! 1. All bundled example configs parse.
//! 2. Server mode boots and serves the full HTTP API surface
//!    (health / ingest parquet / SQL query / metadata) against real files.
//!
//! Note: the server-mode test is gated on `feature = "server"` so the suite
//! also compiles under agent-only builds.

use iedb::config::Config;

#[test]
fn test_example_configs_parse() {
    for name in [
        "agent.toml.example",
        "server.toml.example",
        "mix.toml.example",
    ] {
        let path = format!("{}/configs/{}", env!("CARGO_MANIFEST_DIR"), name);
        Config::from_file(&path).unwrap_or_else(|e| panic!("{} failed to parse: {}", name, e));
    }
}

#[cfg(feature = "server")]
mod server_tests {
    use iedb::config::Config;
    use iedb::server::run_server;
    use std::sync::Arc;
    use std::time::Duration;

    /// Minimal Parquet file: `time` (required int64: 1000/2000/3000) and
    /// `usage` (optional double: 1.5/2.5/3.5), 3 rows — same shape the agent
    /// snapshots use.
    fn make_test_parquet() -> Vec<u8> {
        use parquet::data_type::{DoubleType, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;
        use std::sync::Arc;

        let schema = Arc::new(
            parse_message_type("message schema { required int64 time; optional double usage; }")
                .unwrap(),
        );
        let mut buf = Vec::new();
        let props = Arc::new(WriterProperties::new());
        let mut writer = SerializedFileWriter::new(&mut buf, schema, props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<Int64Type>()
                .write_batch(&[1000i64, 2000, 3000], None, None)
                .unwrap();
            col.close().unwrap();
        }
        {
            let mut col = row_group.next_column().unwrap().unwrap();
            col.typed::<DoubleType>()
                .write_batch(&[1.5, 2.5, 3.5], Some(&[1i16, 1, 1]), None)
                .unwrap();
            col.close().unwrap();
        }
        row_group.close().unwrap();
        writer.close().unwrap();
        buf
    }

    fn random_port() -> u16 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        20000 + (nanos % 20000) as u16
    }

    fn make_config(dir: &tempfile::TempDir, port: u16) -> Arc<Config> {
        let data_dir = dir.path().join("data");
        let db_path = dir.path().join("iedb.db");
        let toml = format!(
            r#"
            [server]
            host = "127.0.0.1"
            port = {}

            [data]
            dir = "{}"

            [query]
            data_dir = "{}"
            query_timeout_secs = 10
            max_rows = 1000

            [compaction]
            enabled = true
            min_file_size_mb = 1
            target_file_size_mb = 16

            [metadata]
            db_path = "{}"
            "#,
            port,
            data_dir.display(),
            data_dir.display(),
            db_path.display(),
        );
        Arc::new(toml::from_str(&toml).unwrap())
    }

    /// 启动 server（绑定随机端口），等待 /health 就绪；端口冲突时换端口重试。
    async fn start_server(
        dir: &tempfile::TempDir,
    ) -> (tokio::task::JoinHandle<()>, String) {
        let client = reqwest::Client::new();
        for _ in 0..5 {
            let port = random_port();
            let base = format!("http://127.0.0.1:{}", port);
            // run_server 返回 Result<(), Box<dyn Error>>（非 Send），包装成 () 才能 spawn；
            // Config 是纯 owned 数据，可安全移入 'static 任务
            let config = make_config(dir, port);
            let handle = tokio::spawn(async move {
                let _ = run_server(config).await;
            });

            for _ in 0..100 {
                if handle.is_finished() {
                    break; // bind 失败或启动出错 → 换端口重试
                }
                if client
                    .get(format!("{}/health", base))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
                {
                    return (handle, base);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            handle.abort();
        }
        panic!("could not start iedb server on any port");
    }

    #[tokio::test]
    async fn test_server_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let (server, base) = start_server(&dir).await;
        let client = reqwest::Client::new();

        // health
        let resp = client.get(format!("{}/health", base)).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        assert_eq!(resp.text().await.unwrap(), "ok");

        // ingest parquet → 落盘 {data_dir}/metrics/cpu/
        let resp = client
            .post(format!("{}/api/v1/ingest/parquet?db=metrics&measurement=cpu", base))
            .header("x-agent-id", "agent-01")
            .body(make_test_parquet())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);

        // SQL 查询（上传后新表立即可查询）
        let resp = client
            .post(format!("{}/api/v1/query", base))
            .header("Content-Type", "application/json")
            .body(r#"{"sql":"SELECT * FROM metrics.cpu"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        let rows = v["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["time"], 1000);
        assert_eq!(rows[0]["usage"], 1.5);
        assert_eq!(v["truncated"], false);
        assert_eq!(v["returned_rows"], 3);

        // SQL 聚合
        let resp = client
            .post(format!("{}/api/v1/query", base))
            .header("Content-Type", "application/json")
            .body(r#"{"sql":"SELECT COUNT(*) AS cnt, MAX(usage) AS mx FROM metrics.cpu"}"#)
            .send()
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["rows"][0]["cnt"], 3);
        assert_eq!(v["rows"][0]["mx"], 3.5);

        // SQL 错误 → 422
        let resp = client
            .post(format!("{}/api/v1/query", base))
            .header("Content-Type", "application/json")
            .body(r#"{"sql":"SELEC broken"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 422);

        // metadata API：数据库与表
        let resp = client
            .get(format!("{}/api/v1/metadata/databases", base))
            .send()
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        let dbs: Vec<&str> = v["databases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["name"].as_str().unwrap())
            .collect();
        assert_eq!(dbs, vec!["metrics"]);

        let resp = client
            .get(format!("{}/api/v1/metadata/tables?db=metrics", base))
            .send()
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["tables"][0]["name"], "cpu");
        assert_eq!(v["tables"][0]["row_count"], 3);

        // agent API：注册 + 列表
        let resp = client
            .post(format!("{}/api/v1/agents/register", base))
            .header("Content-Type", "application/json")
            .body(r#"{"id":"agent-01","hostname":"edge-01"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let resp = client
            .get(format!("{}/api/v1/agents", base))
            .send()
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["agents"].as_array().unwrap().len(), 1);

        // agent heartbeat（当前版本无配置更新）
        let resp = client
            .post(format!("{}/api/v1/agents/heartbeat", base))
            .header("Content-Type", "application/json")
            .body(r#"{"id":"agent-01","config_version":1,"schema_changes":[]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["config_update"], serde_json::Value::Null);

        // 推送配置 → target_version 递增
        let resp = client
            .put(format!("{}/api/v1/agents/agent-01/config", base))
            .header("Content-Type", "application/json")
            .body(r#"{"flush":{"memory_limit":"128MB"}}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["target_version"], 2);

        // 旧版本心跳 → 返回 config_update
        let resp = client
            .post(format!("{}/api/v1/agents/heartbeat", base))
            .header("Content-Type", "application/json")
            .body(r#"{"id":"agent-01","config_version":1,"schema_changes":[]}"#)
            .send()
            .await
            .unwrap();
        let v: serde_json::Value = resp.json().await.unwrap();
        assert!(v["config_update"].is_object());

        // metadata table 详情：字段 + 行数
        let resp = client
            .get(format!("{}/api/v1/metadata/table?db=metrics&table=cpu", base))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["database"], "metrics");
        assert_eq!(v["table"], "cpu");
        assert_eq!(v["row_count"], 3);
        assert!(v["fields"].as_array().unwrap().iter().any(|f| f["name"] == "usage"));

        // 未知路由 → 404
        let resp = client
            .get(format!("{}/nope", base))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 404);

        server.abort();
    }
}
