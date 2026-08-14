// DataFusion-backed SQL query engine with result-size limits and timeouts.
use datafusion::prelude::*;
use serde_json::{Map, Value};
use std::time::Duration;

pub struct QueryEngine {
    ctx: SessionContext,
    max_rows: usize,
    query_timeout_secs: u64,
}

impl QueryEngine {
    pub fn new(max_rows: usize, query_timeout_secs: u64) -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.dialect = "Generic".to_string();
        let ctx = SessionContext::new_with_config(config);
        QueryEngine { ctx, max_rows, query_timeout_secs }
    }

    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// 执行一条 SQL，返回 `{rows, truncated, returned_rows, message}`。
    /// `mode`: history（仅 Parquet）/ buffer（仅 agent buffer）/ all（联合）。
    /// `federator` 为 None 时退化为 history。
    pub async fn query(
        &self,
        sql: &str,
        mode: crate::server::federation::QueryMode,
        federator: Option<&crate::server::federation::Federator>,
    ) -> Result<Value, String> {
        let mut outcome: Option<crate::server::federation::FederationOutcome> = None;
        let df = if mode != crate::server::federation::QueryMode::History {
            if let Some(f) = federator {
                match f.prepare_and_plan(self, sql, mode).await {
                    Ok((o, df)) => { outcome = o; df }
                    Err(e) => {
                        tracing::warn!("federation prepare failed: {}", e);
                        self.ctx
                            .sql(sql)
                            .await
                            .map_err(|e| format!("SQL error: {}", e))?
                    }
                }
            } else {
                self.ctx
                    .sql(sql)
                    .await
                    .map_err(|e| format!("SQL error: {}", e))?
            }
        } else {
            self.ctx
                .sql(sql)
                .await
                .map_err(|e| format!("SQL error: {}", e))?
        };

        // 应用 LIMIT 防止大结果集
        let df = df
            .limit(0, Some(self.max_rows))
            .map_err(|e| format!("SQL error: {}", e))?;

        let batches = tokio::time::timeout(
            Duration::from_secs(self.query_timeout_secs),
            df.collect(),
        )
        .await
        .map_err(|_| format!("Query timed out after {}s", self.query_timeout_secs))?
        .map_err(|e| format!("Query execution error: {}", e))?;

        let mut rows = Vec::new();
        for batch in &batches {
            let schema = batch.schema_ref();
            for row_idx in 0..batch.num_rows() {
                let mut json_row = Map::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let field = schema.field(col_idx);
                    json_row.insert(field.name().clone(), cell_to_json(col, row_idx));
                }
                rows.push(Value::Object(json_row));
            }
        }

        let total = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let truncated = total >= self.max_rows;

        let mut result = serde_json::json!({
            "rows": rows,
            "truncated": truncated,
            "returned_rows": rows.len(),
            "message": if truncated {
                format!("result truncated at {} rows", self.max_rows)
            } else {
                String::new()
            }
        });
        if let Some(o) = outcome {
            result["mode"] = serde_json::json!(mode_string(mode));
            result["federated"] = serde_json::json!(true);
            result["agents_queried"] = serde_json::json!(o.agents_queried);
            result["agents_skipped"] = serde_json::json!(o.agents_skipped);
            if o.agents_skipped > 0 {
                result["message"] = serde_json::json!(format!(
                    "{} agent(s) unreachable; result may be incomplete", o.agents_skipped));
            }
        }
        Ok(result)
    }
}

fn mode_string(mode: crate::server::federation::QueryMode) -> &'static str {
    match mode {
        crate::server::federation::QueryMode::History => "history",
        crate::server::federation::QueryMode::Buffer => "buffer",
        crate::server::federation::QueryMode::All => "all",
    }
}

/// 将单个 Arrow 单元格转换为 JSON 值。
/// 数值/布尔列输出原生 JSON 数字/布尔值，其余类型回退到显示字符串，
/// 避免对非字符串列调用 `as_string_array` 导致 panic。
fn cell_to_json(col: &datafusion::arrow::array::ArrayRef, idx: usize) -> Value {
    use datafusion::arrow::array::{as_boolean_array, as_primitive_array, as_string_array, Array};
    use datafusion::arrow::datatypes::{
        DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    };

    if col.is_null(idx) {
        return Value::Null;
    }
    match col.data_type() {
        DataType::Boolean => Value::Bool(as_boolean_array(col).value(idx)),
        DataType::Int8 => serde_json::json!(as_primitive_array::<Int8Type>(col).value(idx)),
        DataType::Int16 => serde_json::json!(as_primitive_array::<Int16Type>(col).value(idx)),
        DataType::Int32 => serde_json::json!(as_primitive_array::<Int32Type>(col).value(idx)),
        DataType::Int64 => serde_json::json!(as_primitive_array::<Int64Type>(col).value(idx)),
        DataType::UInt8 => serde_json::json!(as_primitive_array::<UInt8Type>(col).value(idx)),
        DataType::UInt16 => serde_json::json!(as_primitive_array::<UInt16Type>(col).value(idx)),
        DataType::UInt32 => serde_json::json!(as_primitive_array::<UInt32Type>(col).value(idx)),
        DataType::UInt64 => serde_json::json!(as_primitive_array::<UInt64Type>(col).value(idx)),
        DataType::Float32 => {
            let v = as_primitive_array::<Float32Type>(col).value(idx) as f64;
            if v.is_finite() {
                serde_json::json!(v)
            } else {
                Value::String(v.to_string())
            }
        }
        DataType::Float64 => {
            let v = as_primitive_array::<Float64Type>(col).value(idx);
            if v.is_finite() {
                serde_json::json!(v)
            } else {
                Value::String(v.to_string())
            }
        }
        DataType::Utf8 => Value::String(as_string_array(col).value(idx).to_string()),
        // 时间戳、LargeUtf8、Decimal、二进制等 → 显示字符串（完整列表见 arrow 的 display 实现）
        _ => datafusion::arrow::util::display::array_value_to_string(col, idx)
            .map(Value::String)
            .unwrap_or(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::federation::QueryMode;
    use crate::server::table_provider::TableProvider;
    use crate::server::test_util::write_test_parquet;
    use tempfile::tempdir;

    /// 创建一个带一张 `metrics.cpu` 表（3 行：time=1000/2000/3000, usage=1.5/2.5/3.5）的引擎。
    fn engine_with_data(max_rows: usize) -> (tempfile::TempDir, QueryEngine) {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("metrics").join("cpu");
        std::fs::create_dir_all(&table_dir).unwrap();
        write_test_parquet(&table_dir.join("a.parquet"));
        let engine = QueryEngine::new(max_rows, 10);
        (dir, engine)
    }

    #[tokio::test]
    async fn test_query_returns_rows_as_json() {
        let (dir, engine) = engine_with_data(100);
        TableProvider::register_all(&engine, dir.path()).await.unwrap();

        let result = engine.query("SELECT * FROM metrics.cpu", QueryMode::History, None).await.unwrap();
        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        // 数值列输出原生 JSON 数字，而不是字符串
        assert_eq!(rows[0]["time"], 1000);
        assert_eq!(rows[0]["usage"], 1.5);
        assert_eq!(rows[1]["time"], 2000);
        assert_eq!(result["truncated"], false);
        assert_eq!(result["returned_rows"], 3);
        assert_eq!(result["message"], "");
    }

    #[tokio::test]
    async fn test_query_truncates_at_max_rows() {
        let (dir, engine) = engine_with_data(2);
        TableProvider::register_all(&engine, dir.path()).await.unwrap();

        let result = engine.query("SELECT * FROM metrics.cpu", QueryMode::History, None).await.unwrap();
        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(result["truncated"], true);
        assert!(result["message"].as_str().unwrap().contains("truncated at 2 rows"));
    }

    #[tokio::test]
    async fn test_query_aggregate() {
        let (dir, engine) = engine_with_data(100);
        TableProvider::register_all(&engine, dir.path()).await.unwrap();

        let result = engine.query("SELECT COUNT(*) AS cnt, MIN(time) AS mn FROM metrics.cpu", QueryMode::History, None).await.unwrap();
        let rows = result["rows"].as_array().unwrap();
        assert_eq!(rows[0]["cnt"], 3);
        assert_eq!(rows[0]["mn"], 1000);
    }

    #[tokio::test]
    async fn test_query_sql_error() {
        let engine = QueryEngine::new(100, 10);
        let err = engine.query("SELEC broken", QueryMode::History, None).await.unwrap_err();
        assert!(err.contains("SQL error"));
    }

    #[tokio::test]
    async fn test_query_table_not_exists_returns_error() {
        let engine = QueryEngine::new(100, 10);
        let err = engine.query("SELECT * FROM nonexistent.table", QueryMode::History, None).await.unwrap_err();
        assert!(err.contains("SQL error"), "unexpected error: {}", err);
    }

    #[tokio::test]
    async fn test_query_times_out() {
        let (dir, _) = engine_with_data(100);
        let engine = QueryEngine::new(100, 0); // 0 秒超时 → 立即失败
        TableProvider::register_all(&engine, dir.path()).await.unwrap();

        let err = engine.query("SELECT * FROM metrics.cpu", QueryMode::History, None).await.unwrap_err();
        assert!(err.contains("timed out"), "unexpected error: {}", err);
    }

    #[tokio::test]
    async fn test_query_history_default_mode_works_without_federator() {
        let (dir, engine) = engine_with_data(100);
        TableProvider::register_all(&engine, dir.path()).await.unwrap();
        let result = engine.query("SELECT * FROM metrics.cpu", QueryMode::History, None).await.unwrap();
        assert_eq!(result["returned_rows"], 3);
        assert!(result.get("federated").is_none(), "history mode must not federate");
    }
}
