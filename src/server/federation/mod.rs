// Query federation: fetch agent buffer data at query time and union it
// with persisted Parquet at scan time.
pub mod time_filter;
pub mod provider;

use crate::server::agent_store::AgentStore;
use crate::server::metadata_store::MetadataStore;
use crate::server::query_engine::QueryEngine;
use datafusion::arrow::array::new_null_array;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlparser::ast::{SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Extract qualified `db.table` references from a SQL statement.
/// Returns (db, table) pairs in first-appearance order.
pub fn extract_table_names(sql: &str) -> Result<Vec<(String, String)>, String> {
    let stmts = Parser::parse_sql(&GenericDialect, sql).map_err(|e| format!("parse: {}", e))?;
    let ctes: Vec<String> = Vec::new();
    let mut tables: Vec<(String, String)> = Vec::new();

    fn walk_table_with_joins(
        tw: &TableWithJoins,
        ctes: &[String],
        tables: &mut Vec<(String, String)>,
    ) {
        walk_table_factor(&tw.relation, ctes, tables);
        for j in &tw.joins {
            walk_table_factor(&j.relation, ctes, tables);
        }
    }

    fn walk_table_factor(
        factor: &TableFactor,
        ctes: &[String],
        tables: &mut Vec<(String, String)>,
    ) {
        match factor {
            TableFactor::Table { name, .. } => {
                let parts: Vec<String> = name.0.iter().map(|i| i.value.clone()).collect();
                if parts.len() == 2 && !ctes.contains(&parts[1]) {
                    let pair = (parts[0].clone(), parts[1].clone());
                    if !tables.contains(&pair) {
                        tables.push(pair);
                    }
                }
            }
            TableFactor::Derived { subquery, .. } => walk_query(subquery, ctes, tables),
            TableFactor::NestedJoin { table_with_joins, .. } => {
                walk_table_with_joins(table_with_joins, ctes, tables)
            }
            _ => {}
        }
    }

    fn walk_set_expr(
        set_expr: &SetExpr,
        ctes: &[String],
        tables: &mut Vec<(String, String)>,
    ) {
        match set_expr {
            SetExpr::Select(select) => {
                for tw in &select.from {
                    walk_table_with_joins(tw, ctes, tables);
                }
            }
            SetExpr::Query(q) => walk_query(q, ctes, tables),
            SetExpr::SetOperation { left, right, .. } => {
                walk_set_expr(left, ctes, tables);
                walk_set_expr(right, ctes, tables);
            }
            _ => {}
        }
    }

    fn walk_query(q: &sqlparser::ast::Query, ctes: &[String], tables: &mut Vec<(String, String)>) {
        let mut all_ctes = ctes.to_vec();
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                all_ctes.push(cte.alias.name.value.clone());
                walk_query(&cte.query, &all_ctes, tables);
            }
        }
        walk_set_expr(&q.body, &all_ctes, tables);
    }

    for stmt in stmts {
        if let Statement::Query(q) = stmt {
            walk_query(&q, &ctes, &mut tables);
        }
    }
    Ok(tables)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    History,
    Buffer,
    All,
}

pub fn parse_query_mode(s: &str) -> QueryMode {
    match s {
        "buffer" => QueryMode::Buffer,
        "all" => QueryMode::All,
        _ => QueryMode::History,
    }
}

pub struct FederationOutcome {
    pub agents_queried: usize,
    pub agents_skipped: usize,
}

pub struct Federator {
    pub agent_store: Arc<AgentStore>,
    pub metadata: Arc<MetadataStore>,
    pub client: reqwest::Client,
    pub lock: Mutex<()>,
}

impl Federator {
    /// Fetch buffers for every table in `sql`, register federated providers,
    /// and plan the SQL — all under the federation lock, so a concurrent
    /// query cannot swap providers between registration and planning.
    /// Execution happens after the lock is released (the DataFrame owns
    /// Arc references to its providers).
    pub async fn prepare_and_plan(
        &self,
        engine: &QueryEngine,
        sql: &str,
        mode: QueryMode,
    ) -> Result<(Option<FederationOutcome>, datafusion::prelude::DataFrame), String> {
        let tables = extract_table_names(sql)?;
        if tables.is_empty() {
            let df = engine.ctx().sql(sql).await.map_err(|e| format!("SQL error: {}", e))?;
            return Ok((None, df));
        }

        let _guard = self.lock.lock().await;
        // The time range is per-SQL, not per-table: extract once.
        let (start_ns, end_ns) = engine_time_range(engine, sql);
        let mut queried = 0usize;
        let mut skipped = 0usize;

        for (db, table) in &tables {
            let qualified = format!("{}.{}", db, table);
            let agent_ids = match self.metadata.list_table_agents(db, table).await {
                Ok(ids) => ids,
                Err(e) => {
                    tracing::warn!("list_table_agents failed: {}", e);
                    continue;
                }
            };
            if agent_ids.is_empty() {
                continue;
            }

            // Resolve online agents with listen addresses
            let mut targets = Vec::new();
            let now_ms = chrono::Utc::now().timestamp_millis();
            for id in &agent_ids {
                if let Ok(rec) = self.agent_store.get(id).await {
                    let online = rec
                        .last_seen_at
                        .map_or(false, |ts| now_ms.saturating_sub(ts) <= 30_000);
                    if online {
                        if let Some(addr) = rec.listen_addr {
                            targets.push(addr);
                            continue;
                        }
                    }
                    tracing::warn!(agent = %id, "agent offline or missing listen_addr, skipping");
                    skipped += 1;
                } else {
                    skipped += 1;
                }
            }
            if targets.is_empty() {
                continue;
            }

            // Fetch buffers in parallel (2s timeout each) — sequential
            // await over an already-created future list keeps this simple
            // and still lets reqwest drive each connection concurrently.
            let mut fetches = Vec::new();
            for addr in &targets {
                let url = build_fetch_url(addr, db, table, start_ns, end_ns);
                fetches.push((
                    addr.clone(),
                    self.client.get(url).timeout(Duration::from_secs(2)).send(),
                ));
            }
            let mut results = Vec::new();
            for (addr, fut) in fetches {
                results.push((addr, fut.await));
            }

            let mut batches: Vec<RecordBatch> = Vec::new();
            for (addr, resp) in results {
                match resp {
                    Ok(r) if r.status() == 204 => {} // empty buffer, fine
                    Ok(r) if r.status().is_success() => {
                        queried += 1;
                        let bytes = match r.bytes().await {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::warn!(agent = %addr, "body read: {}", e);
                                skipped += 1;
                                continue;
                            }
                        };
                        match parquet_bytes_to_batches(bytes).await {
                            Ok(bs) => batches.extend(bs),
                            Err(e) => {
                                tracing::warn!(agent = %addr, "parquet decode: {}", e);
                                skipped += 1;
                            }
                        }
                    }
                    Ok(r) => {
                        tracing::warn!(agent = %addr, status = %r.status(), "buffer fetch failed");
                        skipped += 1;
                    }
                    Err(e) => {
                        tracing::warn!(agent = %addr, "buffer fetch: {}", e);
                        skipped += 1;
                    }
                }
            }

            // No data fetched for this table: leave whatever provider is
            // currently registered (the persisted listing, or a previous
            // registration). In buffer-only mode with no persisted data,
            // planning below fails with "table not found", which the
            // caller surfaces as a normal SQL error.
            if batches.is_empty() {
                continue;
            }

            let original = engine.ctx().table_provider(&qualified).await.ok();
            match mode {
                QueryMode::All => {
                    let listing = match original {
                        Some(l) => l,
                        // No persisted data yet — buffer only, same as Buffer mode
                        None => {
                            let mem = MemTable::try_new(batches[0].schema(), vec![batches])
                                .map_err(|e| format!("mem table: {}", e))?;
                            engine.ctx().register_table(&qualified, Arc::new(mem))
                                .map_err(|e| format!("register {}: {}", qualified, e))?;
                            continue;
                        }
                    };
                    let schema = listing.schema();
                    let aligned = align_to_schema(&batches, &schema)?;
                    let provider = provider::FederatedTableProvider {
                        listing,
                        memory: aligned,
                        schema,
                    };
                    engine.ctx().register_table(&qualified, Arc::new(provider))
                        .map_err(|e| format!("register {}: {}", qualified, e))?;
                }
                QueryMode::Buffer => {
                    let mem = MemTable::try_new(batches[0].schema(), vec![batches])
                        .map_err(|e| format!("mem table: {}", e))?;
                    engine.ctx().register_table(&qualified, Arc::new(mem))
                        .map_err(|e| format!("register {}: {}", qualified, e))?;
                }
                QueryMode::History => unreachable!("history mode never calls prepare_and_plan"),
            }
        }

        // Plan while still holding the lock — the plan binds Arc references
        // to the providers we just registered; later swaps cannot affect it.
        let df = engine.ctx().sql(sql).await.map_err(|e| format!("SQL error: {}", e))?;
        Ok((Some(FederationOutcome { agents_queried: queried, agents_skipped: skipped }), df))
    }
}

fn build_fetch_url(addr: &str, db: &str, table: &str, start: Option<i64>, end: Option<i64>) -> String {
    let mut url = format!("http://{}/api/v1/query/parquet?db={}&table={}", addr, db, table);
    if let Some(s) = start {
        url.push_str(&format!("&start={}", s));
    }
    if let Some(e) = end {
        url.push_str(&format!("&end={}", e));
    }
    url
}

/// Decode Parquet bytes into RecordBatches.
///
/// Uses the synchronous arrow reader. The async stream builder cannot be
/// used here: parquet 53's `ParquetRecordBatchStreamBuilder::new` requires
/// an `AsyncFileReader` and `bytes::Bytes` implements none, and the async
/// stream's `StreamExt` helpers would need `futures-util` as a direct
/// dependency. The sync reader produces the same arrow-52 `RecordBatch`
/// type datafusion 40 uses, so batches feed `MemTable::try_new` directly.
/// Decode is offloaded to the blocking pool since it is CPU-bound.
async fn parquet_bytes_to_batches(bytes: bytes::Bytes) -> Result<Vec<RecordBatch>, String> {
    tokio::task::spawn_blocking(move || {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| format!("parquet builder: {}", e))?;
        let reader = builder.build().map_err(|e| format!("parquet reader: {}", e))?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parquet read: {}", e))
    })
    .await
    .map_err(|e| format!("parquet task join: {}", e))?
}

/// Project raw batches onto the target schema by column name.
/// Missing columns become NULL columns; unknown columns are dropped.
///
/// Takes the raw batches rather than a `MemTable`: datafusion 40's
/// `MemTable` exposes no batch accessor (`batches` is `pub(crate)`), so
/// alignment must happen before the `MemTable` is constructed.
fn align_to_schema(batches: &[RecordBatch], target: &Schema) -> Result<Arc<MemTable>, String> {
    let mut aligned = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut cols = Vec::with_capacity(target.fields().len());
        let n = batch.num_rows();
        for field in target.fields() {
            match batch.column_by_name(field.name()) {
                Some(col) => cols.push(col.clone()),
                None => cols.push(new_null_array(field.data_type(), n)),
            }
        }
        aligned.push(
            RecordBatch::try_new(Arc::new(target.clone()), cols)
                .map_err(|e| format!("align batch: {}", e))?,
        );
    }
    MemTable::try_new(Arc::new(target.clone()), vec![aligned])
        .map(|m| Arc::new(m))
        .map_err(|e| format!("aligned mem table: {}", e))
}

/// Extract the time range for pushdown from the SQL's WHERE clause.
/// Returns (start, end) in ns; (None, None) when nothing extractable.
///
/// An inverted range (start > end) is neutralized to (None, None) — never
/// pass an inverted range to the agent endpoint; fetch the full buffer and
/// let DataFusion re-apply the WHERE instead.
fn engine_time_range(_engine: &QueryEngine, sql: &str) -> (Option<i64>, Option<i64>) {
    let stmts = match Parser::parse_sql(&GenericDialect, sql) {
        Ok(s) => s,
        Err(_) => return (None, None),
    };
    let range = stmts
        .iter()
        .find_map(|s| match s {
            Statement::Query(q) => match &*q.body {
                SetExpr::Select(sel) => sel.selection.as_ref(),
                _ => None,
            },
            _ => None,
        })
        .and_then(|e| time_filter::extract_time_range(e));
    match range {
        Some(r) => match (r.start_ns, r.end_ns) {
            (Some(s), Some(e)) if s > e => (None, None),
            _ => (r.start_ns, r.end_ns),
        },
        None => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_qualified() {
        let t = extract_table_names("SELECT * FROM mydb.cpu").unwrap();
        assert_eq!(t, vec![("mydb".to_string(), "cpu".to_string())]);
    }

    #[test]
    fn test_join_two_tables() {
        let t = extract_table_names("SELECT * FROM a.t1 JOIN b.t2 ON t1.x = t2.x").unwrap();
        assert_eq!(t.len(), 2);
        assert!(t.contains(&("a".to_string(), "t1".to_string())));
        assert!(t.contains(&("b".to_string(), "t2".to_string())));
    }

    #[test]
    fn test_unqualified_skipped() {
        let t = extract_table_names("SELECT * FROM cpu").unwrap();
        assert!(t.is_empty());
    }

    #[test]
    fn test_cte_shadowed_skipped() {
        let sql = "WITH cpu AS (SELECT * FROM otherdb.src) SELECT * FROM cpu JOIN mydb.real ON 1=1";
        let t = extract_table_names(sql).unwrap();
        assert!(!t.contains(&("mydb".to_string(), "cpu".to_string())),
            "CTE named cpu must not be mistaken for mydb.cpu; got {:?}", t);
        assert!(t.contains(&("otherdb".to_string(), "src".to_string())));
        assert!(t.contains(&("mydb".to_string(), "real".to_string())));
    }

    #[test]
    fn test_cte_shadowed_qualified_main_body() {
        let t = extract_table_names(
            "WITH cpu AS (SELECT * FROM otherdb.src) SELECT * FROM mydb.cpu",
        )
        .unwrap();
        assert!(t.contains(&("otherdb".to_string(), "src".to_string())));
        assert!(!t.contains(&("mydb".to_string(), "cpu".to_string())),
            "qualified mydb.cpu must not be collected while cpu is CTE-shadowed; got {:?}", t);
    }

    #[test]
    fn test_cte_shadowed_multiple_ctes() {
        let t = extract_table_names(
            "WITH a AS (SELECT 1), b AS (SELECT 1) SELECT * FROM mydb.b",
        )
        .unwrap();
        assert!(!t.contains(&("mydb".to_string(), "b".to_string())),
            "CTE-shadowed mydb.b must not be collected; got {:?}", t);
    }

    #[test]
    fn test_subquery_tables_found() {
        let t = extract_table_names("SELECT * FROM (SELECT * FROM mydb.cpu) x").unwrap();
        assert_eq!(t, vec![("mydb".to_string(), "cpu".to_string())]);
    }

    #[test]
    fn test_parse_query_mode_defaults() {
        assert_eq!(parse_query_mode("buffer"), QueryMode::Buffer);
        assert_eq!(parse_query_mode("all"), QueryMode::All);
        assert_eq!(parse_query_mode("history"), QueryMode::History);
        assert_eq!(parse_query_mode("bogus"), QueryMode::History);
        assert_eq!(parse_query_mode(""), QueryMode::History);
    }

    #[test]
    fn test_engine_time_range() {
        let engine = QueryEngine::new(100, 30);
        // Normal range passes through in ns.
        assert_eq!(
            engine_time_range(&engine, "SELECT * FROM metrics.cpu WHERE time >= 100 AND time <= 200"),
            (Some(100), Some(200))
        );
        // Single bound passes through.
        assert_eq!(
            engine_time_range(&engine, "SELECT * FROM metrics.cpu WHERE time >= 100"),
            (Some(100), None)
        );
        // Inverted range (start > end) must be neutralized — never pushed
        // to the agent endpoint.
        assert_eq!(
            engine_time_range(&engine, "SELECT * FROM metrics.cpu WHERE time >= 200 AND time <= 100"),
            (None, None)
        );
        // No WHERE clause → nothing extractable.
        assert_eq!(engine_time_range(&engine, "SELECT * FROM metrics.cpu"), (None, None));
        // Unparseable SQL → nothing extractable.
        assert_eq!(engine_time_range(&engine, "not sql at all"), (None, None));
    }
}
