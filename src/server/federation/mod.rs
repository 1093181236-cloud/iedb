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
use datafusion::catalog::MemorySchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
use futures_util::future::join_all;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlparser::ast::{SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

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
    /// Write-locked during prepare_and_plan (provider registration +
    /// planning); History queries take a read lock through planning so a
    /// concurrent federation pass cannot swap providers mid-plan, while
    /// multiple history queries (or non-federated planning) proceed
    /// concurrently.
    pub lock: RwLock<()>,
}

impl Federator {
    /// Fetch buffers for every table in `sql`, register federated providers,
    /// and plan the SQL — all under the federation lock, so a concurrent
    /// query cannot swap providers between registration and planning.
    /// Execution happens after the lock is released (the DataFrame owns
    /// Arc references to its providers).
    ///
    /// The returned `outcome` is `Some` whenever the SQL references
    /// qualified `db.table` tables — i.e. a fetch was ATTEMPTED —
    /// regardless of whether any agent was reachable or returned data.
    /// Consequently `federated: true` in the query response means a fetch
    /// was attempted, NOT that rows were merged; an offline/empty fleet
    /// still reports `federated: true` with an empty (Buffer) or
    /// persisted-only (All) result set.
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

        let _guard = self.lock.write().await;
        // The time range is per-SQL, not per-table: extract once.
        let (start_ns, end_ns) = engine_time_range(engine, sql);
        let mut queried = 0usize;
        let mut skipped = 0usize;

        // Providers replaced by this query's registrations, so they can be
        // restored once planning is done. Buffer mode (and All mode without
        // persisted data) temporarily clobbers the persisted listing in the
        // shared SessionContext; the returned plan holds Arc references to
        // whatever it used, so restoring right after planning leaves the
        // catalog as it was for the next query.
        let mut replaced: Vec<(String, Option<Arc<dyn TableProvider>>)> = Vec::new();

        let plan_result = async {
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
                    // No agents contribute this table: in Buffer mode the
                    // listing must not leak persisted rows into the result.
                    self.shadow_empty_in_buffer(engine, &qualified, &mut replaced, mode)
                        .await?;
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
                    // All agents offline / unreachable: in Buffer mode the
                    // listing must not leak persisted rows into the result.
                    self.shadow_empty_in_buffer(engine, &qualified, &mut replaced, mode)
                        .await?;
                    continue;
                }

                // Fetch buffers in parallel (2s timeout each): build every
                // request future first, then poll them all concurrently with
                // join_all, so connections overlap and the lock-hold window is
                // bounded by the slowest agent, not the sum of all agents.
                let fetches = targets
                    .iter()
                    .map(|addr| {
                        let url = build_fetch_url(addr, db, table, start_ns, end_ns);
                        (
                            addr.clone(),
                            self.client.get(url).timeout(Duration::from_secs(2)).send(),
                        )
                    })
                    .collect::<Vec<_>>();
                let results =
                    join_all(fetches.into_iter().map(|(addr, fut)| async move { (addr, fut.await) }))
                        .await;

                let mut batches: Vec<RecordBatch> = Vec::new();
                for (addr, resp) in results {
                    match resp {
                        Ok(r) if r.status() == 204 => {} // empty buffer, fine
                        Ok(r) if r.status().is_success() => {
                            let bytes = match r.bytes().await {
                                Ok(b) => b,
                                Err(e) => {
                                    tracing::warn!(agent = %addr, "body read: {}", e);
                                    skipped += 1;
                                    continue;
                                }
                            };
                            match parquet_bytes_to_batches(bytes).await {
                                Ok(bs) => {
                                    // Count only after a successful body read
                                    // AND parquet decode; a failed decode
                                    // counts as skipped, not queried.
                                    queried += 1;
                                    batches.extend(bs);
                                }
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

                // No data fetched for this table. All mode keeps whatever
                // provider is registered (the persisted listing — Parquet-only
                // degradation is in-spec there). Buffer mode must never leak
                // persisted rows: shadow the listing with an empty transient
                // provider, or leave the table unregistered when no listing
                // exists (planning then fails with "table not found", which
                // the caller surfaces as a normal SQL error).
                if batches.is_empty() {
                    self.shadow_empty_in_buffer(engine, &qualified, &mut replaced, mode)
                        .await?;
                    continue;
                }

                let original = engine.ctx().table_provider(&qualified).await.ok();
                // Remember the pre-query provider (if any) so the catalog can
                // be restored after planning.
                replaced.push((qualified.clone(), original.clone()));
                match mode {
                    QueryMode::All => {
                        let listing = match original {
                            Some(l) => l,
                            // No persisted data yet — buffer only, same as Buffer mode
                            None => {
                                let mem = MemTable::try_new(batches[0].schema(), vec![batches])
                                    .map_err(|e| format!("mem table: {}", e))?;
                                swap_provider(engine, &qualified, Arc::new(mem))?;
                                continue;
                            }
                        };
                        let schema = listing.schema();
                        let aligned = align_batches_to_schema(&batches, &schema)?;
                        let memory = Arc::new(
                            MemTable::try_new(schema.clone(), vec![aligned])
                                .map_err(|e| format!("mem table: {}", e))?,
                        );
                        let provider = provider::FederatedTableProvider {
                            listing,
                            memory,
                            schema,
                        };
                        swap_provider(engine, &qualified, Arc::new(provider))?;
                    }
                    QueryMode::Buffer => {
                        // Agents may serve heterogeneous field sets; align all
                        // batches to the first batch's schema so MemTable
                        // construction cannot fail on mismatched schemas.
                        let target = batches[0].schema();
                        let aligned = align_batches_to_schema(&batches, &target)?;
                        let mem = MemTable::try_new(target.clone(), vec![aligned])
                            .map_err(|e| format!("mem table: {}", e))?;
                        swap_provider(engine, &qualified, Arc::new(mem))?;
                    }
                    QueryMode::History => unreachable!("history mode never calls prepare_and_plan"),
                }
            }

            // Plan while still holding the lock — the plan binds Arc references
            // to the providers we just registered; later swaps cannot affect it.
            engine.ctx().sql(sql).await.map_err(|e| format!("SQL error: {}", e))
        }
        .await;

        // Restore the pre-query providers in the shared catalog (log-only on
        // failure — the plan we return is already bound to its own providers
        // either way, and the caller falls back to plain SQL on error). The
        // transient provider must be removed first: datafusion 40's
        // register_table rejects an existing name.
        for (name, original) in replaced {
            let res = match original {
                Some(p) => {
                    let _ = engine.ctx().deregister_table(&name);
                    engine.ctx().register_table(&name, p)
                }
                None => engine.ctx().deregister_table(&name),
            };
            if let Err(e) = res {
                tracing::warn!(table = %name, "restore provider: {}", e);
            }
        }

        let df = plan_result?;
        Ok((Some(FederationOutcome { agents_queried: queried, agents_skipped: skipped }), df))
    }

    /// Buffer mode must never bind a plan to the persisted listing: when no
    /// buffer data arrived for `qualified` (no agents registered, all
    /// offline, or empty buffers), the listing would otherwise leak persisted
    /// rows into the buffer-only result set. Shadow it with an empty
    /// transient MemTable over the listing's schema; the caller's
    /// `replaced`/restore machinery puts the original back after planning.
    /// When nothing is registered, leave the table unregistered (planning
    /// then surfaces "table not found" to the caller).
    async fn shadow_empty_in_buffer(
        &self,
        engine: &QueryEngine,
        qualified: &str,
        replaced: &mut Vec<(String, Option<Arc<dyn TableProvider>>)>,
        mode: QueryMode,
    ) -> Result<(), String> {
        if mode != QueryMode::Buffer {
            return Ok(());
        }
        if let Ok(original) = engine.ctx().table_provider(qualified).await {
            let empty = MemTable::try_new(original.schema(), vec![Vec::new()])
                .map_err(|e| format!("empty mem table: {}", e))?;
            replaced.push((qualified.to_string(), Some(original)));
            swap_provider(engine, qualified, Arc::new(empty))?;
        }
        Ok(())
    }
}

/// Replace (or install) the provider registered under `name`.
/// datafusion 40's MemorySchemaProvider rejects re-registration of an
/// existing name, so a swap must deregister first (deregistering a name
/// that is not registered is a no-op).
///
/// Two catalog quirks handled here:
/// - deregister_table on a name whose SCHEMA was never registered fails
///   with "failed to resolve schema" — guard with table_exist (same
///   pattern as table_provider.rs).
/// - register_table also requires the schema to exist; register a
///   MemorySchemaProvider for the db first when missing (mirrors
///   table_provider.rs::register_table_dir).
fn swap_provider(
    engine: &QueryEngine,
    name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<(), String> {
    if engine.ctx().table_exist(name).unwrap_or(false) {
        engine
            .ctx()
            .deregister_table(name)
            .map_err(|e| format!("deregister {}: {}", name, e))?;
    }
    // DF 40's register_table requires the schema in the catalog: create the
    // db schema first if it does not exist (skip if present, never overwrite).
    let db = name
        .split_once('.')
        .map(|(db, _)| db)
        .ok_or_else(|| format!("unqualified table name: {}", name))?;
    let catalog_name = engine
        .ctx()
        .copied_config()
        .options()
        .catalog
        .default_catalog
        .clone();
    let catalog = engine
        .ctx()
        .catalog(&catalog_name)
        .ok_or("default catalog missing")?;
    if !catalog.schema_names().iter().any(|s| s == db) {
        catalog
            .register_schema(db, Arc::new(MemorySchemaProvider::new()))
            .map_err(|e| format!("register schema {}: {}", db, e))?;
    }
    engine
        .ctx()
        .register_table(name, provider)
        .map_err(|e| format!("register {}: {}", name, e))?;
    Ok(())
}

fn build_fetch_url(addr: &str, db: &str, table: &str, start: Option<i64>, end: Option<i64>) -> String {
    // db/table can contain characters that need percent-encoding (spaces,
    // slashes, non-ASCII); encode both before splicing into the query.
    let enc = |s: &str| url::form_urlencoded::byte_serialize(s.as_bytes()).collect::<String>();
    let mut url = format!("http://{}/api/v1/query/parquet?db={}&table={}", addr, enc(db), enc(table));
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
/// Returns the aligned batches, all sharing `target` as their schema.
///
/// Takes the raw batches rather than a `MemTable`: datafusion 40's
/// `MemTable` exposes no batch accessor (`batches` is `pub(crate)`), so
/// alignment must happen before the `MemTable` is constructed. Reused by
/// both the All path (align buffer rows to the persisted listing schema)
/// and the Buffer path (align heterogeneous agent schemas to the first
/// batch's schema).
fn align_batches_to_schema(
    batches: &[RecordBatch],
    target: &Schema,
) -> Result<Vec<RecordBatch>, String> {
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
    Ok(aligned)
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
        let engine = QueryEngine::new(100, 30, 4);
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

    /// Buffer mode must leave the shared catalog with the ORIGINAL provider:
    /// the MemTable registered for the query is transient and is restored
    /// right after planning.
    #[tokio::test]
    async fn test_buffer_mode_restores_original_provider() {
        use crate::server::db::Db;
        use datafusion::arrow::array::{Float64Array, Int64Array};
        use datafusion::arrow::datatypes::{DataType, Field};
        use datafusion::catalog::MemorySchemaProvider;
        use tempfile::tempdir;

        // --- Mock agent: serves a small parquet buffer over HTTP ---
        let buf_schema = Arc::new(Schema::new(vec![Field::new("time", DataType::Int64, false)]));
        let buf_batch = RecordBatch::try_new(
            buf_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let mut parquet_buf = Vec::new();
        {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(&mut parquet_buf, buf_schema, None).unwrap();
            writer.write(&buf_batch).unwrap();
            writer.close().unwrap();
        }
        let parquet_bytes = bytes::Bytes::from(parquet_buf);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serve_bytes = parquet_bytes.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => break,
                };
                let io = hyper_util::rt::TokioIo::new(stream);
                let resp_bytes = serve_bytes.clone();
                let svc = hyper::service::service_fn(move |_req| {
                    let b = resp_bytes.clone();
                    async move {
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .status(200)
                                .body(http_body_util::Full::new(b))
                                .unwrap(),
                        )
                    }
                });
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await;
                });
            }
        });

        // --- Stores: one online agent contributing metrics.cpu ---
        let dir = tempdir().unwrap();
        let db = Arc::new(Db::open(&dir.path().join("test.db")).unwrap());
        let agent_store = Arc::new(AgentStore::new(db.clone()));
        let metadata = Arc::new(MetadataStore::new(db));
        agent_store
            .register("agent-1", "host-a", "x86_64", "1", &addr.to_string())
            .await
            .unwrap();
        metadata
            .merge_schema(
                "metrics",
                "cpu",
                "agent-1",
                &[],
                &[("time".to_string(), "Int64".to_string())],
            )
            .await
            .unwrap();

        // --- Engine with an original (persisted-style) provider ---
        let engine = QueryEngine::new(100, 30, 4);
        let catalog_name = engine
            .ctx()
            .copied_config()
            .options()
            .catalog
            .default_catalog
            .clone();
        let catalog = engine.ctx().catalog(&catalog_name).unwrap();
        catalog
            .register_schema("metrics", Arc::new(MemorySchemaProvider::new()))
            .unwrap();

        let orig_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("usage", DataType::Float64, false),
        ]));
        let orig_batch = RecordBatch::try_new(
            orig_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10i64, 20])),
                Arc::new(Float64Array::from(vec![0.5, 0.6])),
            ],
        )
        .unwrap();
        let original_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(orig_schema, vec![vec![orig_batch]]).unwrap());
        engine
            .ctx()
            .register_table("metrics.cpu", original_provider.clone())
            .unwrap();

        // --- Federate in Buffer mode ---
        let federator = Federator {
            agent_store,
            metadata,
            client: reqwest::Client::new(),
            lock: RwLock::new(()),
        };
        let (outcome, _df) = federator
            .prepare_and_plan(&engine, "SELECT * FROM metrics.cpu", QueryMode::Buffer)
            .await
            .unwrap();
        let outcome = outcome.unwrap();
        assert_eq!(outcome.agents_queried, 1);
        assert_eq!(outcome.agents_skipped, 0);

        // The shared catalog must hold the ORIGINAL provider again — the
        // buffer MemTable was transient.
        let now = engine.ctx().table_provider("metrics.cpu").await.unwrap();
        assert!(
            Arc::ptr_eq(&original_provider, &now),
            "buffer provider must be transient; catalog must keep the original"
        );
    }
}

// End-to-end federation tests: real QueryEngine + real Federator + a mock
// agent HTTP server, proving the three modes return correct merged data.
#[cfg(test)]
mod tests_federation {
    use super::*;
    use crate::server::db::Db;
    use crate::server::query_engine::QueryEngine;
    use crate::server::table_provider::TableProvider as Reg;
    use crate::server::test_util::write_test_parquet;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::net::SocketAddr;
    use tempfile::tempdir;

    /// Buffer rows served by the mock agent: time 4000..4002, usage 4.5/5.5/6.5.
    /// Written with the server-side parquet writer (the `agent` module is not
    /// compiled under `--features server`): schema time + host (tag) + usage,
    /// matching what the agent's flush_chunks_to_parquet would produce.
    fn buffer_parquet_bytes() -> Vec<u8> {
        use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field};
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("usage", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4000i64, 4001, 4002])),
                Arc::new(StringArray::from(vec!["srv01", "srv01", "srv01"])),
                Arc::new(Float64Array::from(vec![4.5f64, 5.5, 6.5])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        buf
    }

    /// Spawn a mock agent answering 204 No Content (empty buffer) — the
    /// response a real agent gives when it has no rows in the requested
    /// time range.
    async fn spawn_mock_agent_204() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await { Ok(x) => x, Err(_) => return };
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |_req| {
                        async move {
                            Ok::<_, std::convert::Infallible>(
                                hyper::Response::builder()
                                    .status(204)
                                    .body(http_body_util::Full::new(bytes::Bytes::new()))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new()).serve_connection(io, svc).await;
                });
            }
        });
        addr
    }

    /// Spawn a mock agent on an ephemeral port returning the given bytes.
    async fn spawn_mock_agent(bytes: Vec<u8>) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await { Ok(x) => x, Err(_) => return };
                let bytes = bytes.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |_req| {
                        let b = bytes.clone();
                        async move {
                            Ok::<_, std::convert::Infallible>(hyper::Response::builder()
                                .status(200)
                                .header("Content-Type", "application/octet-stream")
                                .body(http_body_util::Full::new(bytes::Bytes::from(b))).unwrap())
                        }
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new()).serve_connection(io, svc).await;
                });
            }
        });
        addr
    }

    async fn setup() -> (tempfile::TempDir, QueryEngine, Federator) {
        let dir = tempdir().unwrap();
        // Persisted parquet: 3 rows time 1000..3000 usage 1.5/2.5/3.5
        let table_dir = dir.path().join("metrics").join("cpu");
        std::fs::create_dir_all(&table_dir).unwrap();
        write_test_parquet(&table_dir.join("a.parquet"));

        let engine = QueryEngine::new(100, 10, 4);
        Reg::register_all(&engine, dir.path()).await.unwrap();

        // Metadata: agent a1 -> metrics.cpu
        let db = Db::open(&dir.path().join("meta.db")).unwrap();
        // agent_tables.agent_id has REFERENCES agents(id) with
        // foreign_keys=ON, and merge_schema swallows the insert error — so
        // the mapping is silently dropped unless the agent row exists in the
        // metadata DB first. Seed it (the federator's agent_store still uses
        // its own separate agents.db, per the brief's setup).
        {
            let conn = db.conn().lock().await;
            let now_ms = chrono::Utc::now().timestamp_millis();
            conn.execute(
                "INSERT OR IGNORE INTO agents (id, hostname, registered_at, last_seen_at) VALUES ('a1', 'mock', ?1, ?1)",
                rusqlite::params![now_ms],
            )
            .unwrap();
        }
        let metadata = Arc::new(MetadataStore::new(Arc::new(db)));
        let _ = metadata.merge_schema("metrics", "cpu", "a1",
            &["host".to_string()],
            &[("usage".to_string(), "F64".to_string())]).await;

        let federator = Federator {
            agent_store: Arc::new(AgentStore::new(Arc::new(Db::open(&dir.path().join("agents.db")).unwrap()))),
            metadata,
            client: reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)).build().unwrap(),
            lock: RwLock::new(()),
        };
        (dir, engine, federator)
    }

    async fn register_agent(f: &Federator, addr: &SocketAddr) {
        let _ = f.agent_store.register("a1", "mock", "x86", "0.1.0", &format!("{}", addr)).await;
        let _ = f.agent_store.heartbeat("a1", 1).await;
    }

    #[tokio::test]
    async fn test_history_mode_no_federation() {
        let (_dir, engine, f) = setup().await;
        let r = engine.query("SELECT * FROM metrics.cpu", QueryMode::History, Some(&f)).await.unwrap();
        assert_eq!(r["returned_rows"], 3);
        assert!(r.get("federated").is_none());
    }

    #[tokio::test]
    async fn test_all_mode_unions_buffer_and_parquet() {
        let (_dir, engine, f) = setup().await;
        let addr = spawn_mock_agent(buffer_parquet_bytes()).await;
        register_agent(&f, &addr).await;

        let r = engine.query("SELECT * FROM metrics.cpu", QueryMode::All, Some(&f)).await.unwrap();
        assert_eq!(r["returned_rows"], 6, "3 parquet + 3 buffer rows; got: {}", r);
        assert_eq!(r["federated"], true);
        assert_eq!(r["agents_queried"], 1);
        assert_eq!(r["agents_skipped"], 0);
    }

    #[tokio::test]
    async fn test_all_mode_aggregate_includes_buffer() {
        let (_dir, engine, f) = setup().await;
        let addr = spawn_mock_agent(buffer_parquet_bytes()).await;
        register_agent(&f, &addr).await;

        let r = engine.query("SELECT COUNT(*) AS cnt, MAX(time) AS mx FROM metrics.cpu", QueryMode::All, Some(&f)).await.unwrap();
        assert_eq!(r["rows"][0]["cnt"], 6);
        assert_eq!(r["rows"][0]["mx"], 4002);
    }

    #[tokio::test]
    async fn test_buffer_mode_only_memory() {
        let (_dir, engine, f) = setup().await;
        let addr = spawn_mock_agent(buffer_parquet_bytes()).await;
        register_agent(&f, &addr).await;

        let r = engine.query("SELECT * FROM metrics.cpu", QueryMode::Buffer, Some(&f)).await.unwrap();
        assert_eq!(r["returned_rows"], 3);
        assert_eq!(r["rows"][0]["time"], 4000, "buffer-only must exclude parquet rows");
    }

    /// Buffer mode on a BRAND-NEW table (no parquet written, no listing,
    /// schema never registered in the catalog). swap_provider must not
    /// choke on the missing schema — regression for live ARM32:
    /// "deregister fed3.test: Error during planning: failed to resolve
    /// schema: fed3".
    #[tokio::test]
    async fn test_buffer_mode_fresh_table_no_listing() {
        let dir = tempdir().unwrap();
        // No write_test_parquet, no TableProvider::register_all: the
        // metrics schema does not exist in the catalog at all.
        let engine = QueryEngine::new(100, 10, 4);

        // Metadata: agent a1 -> metrics.fresh (agent row seeded first, as in
        // setup(), because agent_tables has a FK on agents)
        let db = Db::open(&dir.path().join("meta.db")).unwrap();
        {
            let conn = db.conn().lock().await;
            let now_ms = chrono::Utc::now().timestamp_millis();
            conn.execute(
                "INSERT OR IGNORE INTO agents (id, hostname, registered_at, last_seen_at) VALUES ('a1', 'mock', ?1, ?1)",
                rusqlite::params![now_ms],
            )
            .unwrap();
        }
        let metadata = Arc::new(MetadataStore::new(Arc::new(db)));
        metadata
            .merge_schema(
                "metrics",
                "fresh",
                "a1",
                &["host".to_string()],
                &[("v".to_string(), "F64".to_string())],
            )
            .await
            .unwrap();

        let federator = Federator {
            agent_store: Arc::new(AgentStore::new(Arc::new(Db::open(&dir.path().join("agents.db")).unwrap()))),
            metadata,
            client: reqwest::Client::builder().timeout(std::time::Duration::from_secs(2)).build().unwrap(),
            lock: RwLock::new(()),
        };
        let addr = spawn_mock_agent(buffer_parquet_bytes()).await;
        register_agent(&federator, &addr).await;

        let r = engine
            .query("SELECT COUNT(*) AS c FROM metrics.fresh", QueryMode::Buffer, Some(&federator))
            .await
            .unwrap();
        assert_eq!(r["rows"][0]["c"], 3, "buffer rows on a fresh table must count, got: {}", r);
        assert_eq!(r["federated"], true);
        assert_eq!(r["agents_queried"], 1);

        // The transient registration must not linger in the catalog.
        assert!(!engine.ctx().table_exist("metrics.fresh").unwrap_or(false),
            "transient provider must be restored (removed) after planning");
    }

    #[tokio::test]
    async fn test_offline_agent_skipped() {
        let (_dir, engine, f) = setup().await;
        // Agent "a1" registered in metadata but no agent row → offline → skipped
        let r = engine.query("SELECT * FROM metrics.cpu", QueryMode::All, Some(&f)).await.unwrap();
        assert_eq!(r["returned_rows"], 3, "only parquet data");
        assert_eq!(r["agents_skipped"], 1);
    }

    /// Buffer mode with an unreachable agent must return an EMPTY result
    /// set — never the persisted Parquet rows the listing would otherwise
    /// leak into the plan.
    #[tokio::test]
    async fn test_buffer_mode_offline_agent_returns_empty() {
        let (_dir, engine, f) = setup().await;
        // a1 is in metadata but has no agent-store row → offline → no fetch
        let r = engine.query("SELECT * FROM metrics.cpu", QueryMode::Buffer, Some(&f)).await.unwrap();
        assert_eq!(r["returned_rows"], 0, "buffer-only with no reachable agent must be empty, got: {}", r);
        assert_eq!(r["federated"], true);
        assert_eq!(r["agents_skipped"], 1);
    }

    /// Buffer mode with an online agent whose buffer is empty (HTTP 204)
    /// must return an empty result set, not the persisted Parquet rows.
    #[tokio::test]
    async fn test_buffer_mode_empty_buffer_returns_empty() {
        let (_dir, engine, f) = setup().await;
        let addr = spawn_mock_agent_204().await;
        register_agent(&f, &addr).await;

        let r = engine.query("SELECT * FROM metrics.cpu", QueryMode::Buffer, Some(&f)).await.unwrap();
        assert_eq!(r["returned_rows"], 0, "empty agent buffer must not leak parquet rows, got: {}", r);
        // A 204 carries no decoded rows, so it is neither queried nor skipped.
        assert_eq!(r["agents_queried"], 0);
        assert_eq!(r["agents_skipped"], 0);
    }

    #[tokio::test]
    async fn test_time_pushdown_passed_to_agent() {
        let (_dir, engine, f) = setup().await;
        let addr = spawn_mock_agent(buffer_parquet_bytes()).await;
        register_agent(&f, &addr).await;

        // WHERE time <= 1500: the mock agent ignores start/end and returns all
        // buffer rows, but DataFusion re-applies the WHERE after the union —
        // only the parquet row at time=1000 survives. cnt must be 1.
        let r = engine.query("SELECT COUNT(*) AS cnt FROM metrics.cpu WHERE time <= 1500", QueryMode::All, Some(&f)).await.unwrap();
        assert_eq!(r["rows"][0]["cnt"], 1);
    }
}
