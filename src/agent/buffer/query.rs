use crate::agent::buffer::chunk::Table;
use crate::agent::buffer::Buffer;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct QueryRow {
    pub time: i64,
    pub tags: std::collections::HashMap<String, String>,
    pub fields: std::collections::HashMap<String, serde_json::Value>,
}

/// Query a table across all chunks with optional time range and tag filter.
pub fn query_table(
    table: &Table,
    start_ns: Option<i64>,
    end_ns: Option<i64>,
    tag_key: Option<&str>,
    tag_value: Option<&str>,
) -> Vec<QueryRow> {
    let mut results = Vec::new();

    for chunk in &table.chunks {
        // Get candidate row indices
        let candidates: Vec<usize> = if let (Some(key), Some(val)) = (tag_key, tag_value) {
            chunk.tag_index
                .get(key)
                .and_then(|vmap| vmap.get(val))
                .cloned()
                .unwrap_or_default()
        } else {
            (0..chunk.rows.len()).collect()
        };

        for idx in candidates {
            let row = &chunk.rows[idx];

            // Time filter
            if let Some(start) = start_ns {
                if row.time < start { continue; }
            }
            if let Some(end) = end_ns {
                if row.time > end { continue; }
            }

            // Build response row with schema
            let mut tags = std::collections::HashMap::new();
            for (i, key) in table.schema.tag_keys.iter().enumerate() {
                if let Some(val) = row.tag_values.get(i) {
                    tags.insert(key.clone(), val.clone());
                }
            }

            let mut fields = std::collections::HashMap::new();
            for (i, fdef) in table.schema.field_defs.iter().enumerate() {
                if let Some(Some(val)) = row.field_values.get(i) {
                    let json_val = match val {
                        crate::agent::buffer::chunk::FieldValue::I64(v) => serde_json::json!(v),
                        crate::agent::buffer::chunk::FieldValue::F64(v) => serde_json::json!(v),
                        crate::agent::buffer::chunk::FieldValue::U64(v) => serde_json::json!(v),
                        crate::agent::buffer::chunk::FieldValue::Bool(v) => serde_json::json!(v),
                        crate::agent::buffer::chunk::FieldValue::String(v) => serde_json::json!(v),
                    };
                    fields.insert(fdef.name.clone(), json_val);
                } else {
                    fields.insert(fdef.name.clone(), serde_json::Value::Null);
                }
            }

            results.push(QueryRow {
                time: row.time,
                tags,
                fields,
            });
        }
    }

    results
}

impl Buffer {
    pub fn query(
        &self,
        db: &str,
        table_name: &str,
        start_ns: Option<i64>,
        end_ns: Option<i64>,
        tag_key: Option<&str>,
        tag_value: Option<&str>,
    ) -> Option<Vec<QueryRow>> {
        self.get_table(db, table_name)
            .map(|table| query_table(table, start_ns, end_ns, tag_key, tag_value))
    }
}
