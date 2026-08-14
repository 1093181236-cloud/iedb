// Query federation: fetch agent buffer data at query time and union it
// with persisted Parquet at scan time.
pub mod time_filter;
pub mod provider;

use sqlparser::ast::{SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                let mut all_ctes = ctes.to_vec();
                all_ctes.push(cte.alias.name.value.clone());
                walk_query(&cte.query, &all_ctes, tables);
                // CTE body tables are in scope at the outer level too (recursion aside)
                walk_set_expr(&q.body, &all_ctes, tables);
            }
        }
        walk_set_expr(&q.body, ctes, tables);
    }

    for stmt in stmts {
        if let Statement::Query(q) = stmt {
            walk_query(&q, &ctes, &mut tables);
        }
    }
    Ok(tables)
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
    fn test_subquery_tables_found() {
        let t = extract_table_names("SELECT * FROM (SELECT * FROM mydb.cpu) x").unwrap();
        assert_eq!(t, vec![("mydb".to_string(), "cpu".to_string())]);
    }
}
