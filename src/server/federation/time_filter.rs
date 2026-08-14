// Narrow-scope time predicate extraction from SQL WHERE expressions.
// Only `time` compared to constant literals is extractable. Anything else
// yields no bound — the caller then fetches the full buffer, which is
// always correct because DataFusion re-applies the WHERE after the union.
use sqlparser::ast::{BinaryOperator, Expr, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
}

/// Extract a [start, end] range from a WHERE expression.
/// Returns None if nothing extractable; Some with at least one bound otherwise.
pub fn extract_time_range(expr: &Expr) -> Option<TimeRange> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let l = extract_time_range(left).unwrap_or(TimeRange { start_ns: None, end_ns: None });
                let r = extract_time_range(right).unwrap_or(TimeRange { start_ns: None, end_ns: None });
                let merged = TimeRange {
                    start_ns: merge_max(l.start_ns, r.start_ns),
                    end_ns: merge_min(l.end_ns, r.end_ns),
                };
                if merged.start_ns.is_none() && merged.end_ns.is_none() {
                    None
                } else {
                    Some(merged)
                }
            }
            // OR: cannot combine bounds safely → nothing extractable
            BinaryOperator::Or => None,
            BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq => {
                if !is_time_column(left) {
                    return None;
                }
                let value = parse_const(right)?;
                match op {
                    // saturating: value +/- 1 at i64::MAX/MIN would panic in
                    // debug builds; clamp instead of overflowing
                    BinaryOperator::Gt => Some(TimeRange { start_ns: Some(value.saturating_add(1)), end_ns: None }),
                    BinaryOperator::GtEq => Some(TimeRange { start_ns: Some(value), end_ns: None }),
                    BinaryOperator::Lt => Some(TimeRange { start_ns: None, end_ns: Some(value.saturating_sub(1)) }),
                    BinaryOperator::LtEq => Some(TimeRange { start_ns: None, end_ns: Some(value) }),
                    _ => unreachable!(),
                }
            }
            _ => None,
        },
        _ => None,
    }
}

/// Column must be `time`, `table.time`, or `db.table.time`.
fn is_time_column(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(id) => id.value == "time",
        Expr::CompoundIdentifier(parts) => {
            parts.last().map(|p| p.value == "time").unwrap_or(false)
        }
        _ => false,
    }
}

/// Right side must be a constant: integer literal or quoted string that
/// parses as an integer (ns timestamps are large).
fn parse_const(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(Value::Number(s, _)) => s.parse::<i64>().ok(),
        Expr::Value(Value::SingleQuotedString(s)) | Expr::Value(Value::DoubleQuotedString(s)) => {
            s.parse::<i64>().ok()
        }
        // Typed string literal: TIMESTAMP '...' — not supported in v1, treat as unknown
        _ => None,
    }
}

fn merge_max(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

fn merge_min(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn where_of(sql: &str) -> Expr {
        let stmts = Parser::parse_sql(&GenericDialect, sql).unwrap();
        match &stmts[0] {
            sqlparser::ast::Statement::Query(q) => match &*q.body {
                sqlparser::ast::SetExpr::Select(s) => s.selection.clone().unwrap(),
                _ => panic!("not a select"),
            },
            _ => panic!("not a query"),
        }
    }

    #[test]
    fn test_both_bounds() {
        let w = where_of("SELECT * FROM t WHERE time >= 100 AND time <= 200");
        let r = extract_time_range(&w).unwrap();
        assert_eq!(r.start_ns, Some(100));
        assert_eq!(r.end_ns, Some(200));
    }

    #[test]
    fn test_exclusive_bounds_adjusted() {
        let w = where_of("SELECT * FROM t WHERE time > 100 AND time < 200");
        let r = extract_time_range(&w).unwrap();
        assert_eq!(r.start_ns, Some(101));
        assert_eq!(r.end_ns, Some(199));
    }

    /// `time > i64::MAX` must not panic: the +1 adjustment saturates and
    /// yields an i64::MAX start bound.
    #[test]
    fn test_gt_i64_max_saturates() {
        let w = where_of("SELECT * FROM t WHERE time > 9223372036854775807");
        let r = extract_time_range(&w).unwrap();
        assert_eq!(r.start_ns, Some(i64::MAX));
        assert_eq!(r.end_ns, None);
    }

    /// `time < i64::MIN` must not panic: the -1 adjustment saturates and
    /// yields an i64::MIN end bound. (Bare `-9223372036854775808` does not
    /// parse as a single literal, so use the quoted-string form, which
    /// parse_const accepts.)
    #[test]
    fn test_lt_i64_min_saturates() {
        let w = where_of("SELECT * FROM t WHERE time < '-9223372036854775808'");
        let r = extract_time_range(&w).unwrap();
        assert_eq!(r.start_ns, None);
        assert_eq!(r.end_ns, Some(i64::MIN));
    }

    #[test]
    fn test_qualified_column() {
        let w = where_of("SELECT * FROM t WHERE db.t.time >= 5");
        let r = extract_time_range(&w).unwrap();
        assert_eq!(r.start_ns, Some(5));
        assert_eq!(r.end_ns, None);
    }

    #[test]
    fn test_string_literal_bound() {
        let w = where_of("SELECT * FROM t WHERE time <= '1000'");
        let r = extract_time_range(&w).unwrap();
        assert_eq!(r.end_ns, Some(1000));
    }

    #[test]
    fn test_or_yields_nothing() {
        let w = where_of("SELECT * FROM t WHERE time > 100 OR host = 'a'");
        assert!(extract_time_range(&w).is_none());
    }

    #[test]
    fn test_and_with_unrelated_condition_partial() {
        let w = where_of("SELECT * FROM t WHERE time > 100 AND usage > 0.5");
        let r = extract_time_range(&w).unwrap();
        assert_eq!(r.start_ns, Some(101));
        assert_eq!(r.end_ns, None);
    }

    #[test]
    fn test_non_time_column_yields_nothing() {
        let w = where_of("SELECT * FROM t WHERE usage > 0.5");
        assert!(extract_time_range(&w).is_none());
    }

    #[test]
    fn test_function_yields_nothing() {
        let w = where_of("SELECT * FROM t WHERE time > now() - 300");
        assert!(extract_time_range(&w).is_none());
    }

    #[test]
    fn test_no_where_yields_nothing() {
        assert!(extract_time_range(&Expr::Value(Value::Boolean(true))).is_none());
    }
}
