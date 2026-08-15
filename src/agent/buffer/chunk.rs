use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A field value in a time-series row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    I64(i64),
    F64(f64),
    U64(u64),
    Bool(bool),
    String(String),
}

/// The type of a field column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    I64,
    F64,
    U64,
    Bool,
    String,
}

impl FieldValue {
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldValue::I64(_) => FieldType::I64,
            FieldValue::F64(_) => FieldType::F64,
            FieldValue::U64(_) => FieldType::U64,
            FieldValue::Bool(_) => FieldType::Bool,
            FieldValue::String(_) => FieldType::String,
        }
    }
}

/// A field definition in the table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub value_type: FieldType,
}

/// Table-level schema shared across all rows and chunks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub tag_keys: Vec<String>,
    pub field_defs: Vec<FieldDef>,
}

impl TableSchema {
    pub fn new() -> Self {
        TableSchema {
            tag_keys: Vec::new(),
            field_defs: Vec::new(),
        }
    }

    /// Return the index of a field, adding it if new (schema evolution).
    pub fn ensure_field(&mut self, name: &str, value_type: FieldType) -> usize {
        if let Some(pos) = self.field_defs.iter().position(|f| f.name == name) {
            return pos;
        }
        self.field_defs.push(FieldDef {
            name: name.to_string(),
            value_type,
        });
        self.field_defs.len() - 1
    }

    /// Return the index of a tag key, adding it if new.
    pub fn ensure_tag_key(&mut self, key: &str) -> usize {
        if let Some(pos) = self.tag_keys.iter().position(|k| k == key) {
            return pos;
        }
        self.tag_keys.push(key.to_string());
        self.tag_keys.len() - 1
    }
}

/// A row stores only values; keys come from TableSchema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub time: i64,
    pub tag_values: Vec<String>,
    pub field_values: Vec<Option<FieldValue>>,
}

/// A time-partitioned chunk of rows.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub chunk_time: i64,
    pub rows: Vec<Row>,
    pub tag_index: HashMap<String, HashMap<String, Vec<usize>>>,
    pub time_min: i64,
    pub time_max: i64,
    pub avg_row_bytes: usize,
    pub min_wal_seq: u64,
    pub max_wal_seq: u64,
}

impl Chunk {
    pub fn new(chunk_time: i64) -> Self {
        Chunk {
            chunk_time,
            rows: Vec::new(),
            tag_index: HashMap::new(),
            time_min: i64::MAX,
            time_max: i64::MIN,
            avg_row_bytes: 0,
            min_wal_seq: u64::MAX,
            max_wal_seq: 0,
        }
    }

    pub fn estimated_size(&self) -> usize {
        self.rows.len() * self.avg_row_bytes.max(64)
    }

    /// Insert a row into this chunk.
    pub fn insert(&mut self, row: Row, wal_seq: u64) {
        let _row_idx = self.rows.len();

        // Update time bounds
        if row.time < self.time_min { self.time_min = row.time; }
        if row.time > self.time_max { self.time_max = row.time; }

        // Update WAL seq bounds
        if wal_seq < self.min_wal_seq { self.min_wal_seq = wal_seq; }
        if wal_seq > self.max_wal_seq { self.max_wal_seq = wal_seq; }

        // Update tag index
        // We know tag keys from the caller, not stored per-row.
        // The caller (Table) updates the index after adjusting schema.

        // I10: update estimated row size for memory-limit protection
        let row_bytes = 8  // time (i64)
            + row.tag_values.iter().map(|t| t.len()).sum::<usize>()
            + row.field_values.len() * 16;  // field overhead estimate
        self.avg_row_bytes = if self.avg_row_bytes == 0 {
            row_bytes
        } else {
            (self.avg_row_bytes + row_bytes) / 2
        };

        self.rows.push(row);
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    /// Rebuild the tag index from scratch. Needed after rows are drained
    /// from the front (snapshot handoff keeps later-arriving rows): the
    /// index stores row positions, which shift by the drained count.
    pub fn rebuild_tag_index(&mut self, tag_keys: &[String]) {
        self.tag_index.clear();
        for (idx, row) in self.rows.iter().enumerate() {
            for (key_idx, key) in tag_keys.iter().enumerate() {
                if let Some(value) = row.tag_values.get(key_idx) {
                    self.tag_index
                        .entry(key.clone())
                        .or_default()
                        .entry(value.clone())
                        .or_default()
                        .push(idx);
                }
            }
        }
    }
}

/// A table holds a schema and time-ordered chunks (usually 1-2).
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub schema: TableSchema,
    pub chunks: Vec<Chunk>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Table {
            name,
            schema: TableSchema::new(),
            chunks: Vec::new(),
        }
    }

    /// Find or create the chunk for the given chunk_time.
    pub fn get_or_create_chunk(&mut self, chunk_time: i64) -> &mut Chunk {
        match self.chunks.binary_search_by(|c| c.chunk_time.cmp(&chunk_time)) {
            Ok(idx) => &mut self.chunks[idx],
            Err(idx) => {
                self.chunks.insert(idx, Chunk::new(chunk_time));
                &mut self.chunks[idx]
            }
        }
    }

    /// Total estimated memory size of all chunks.
    pub fn estimated_size(&self) -> usize {
        self.chunks.iter().map(|c| c.estimated_size()).sum()
    }

    /// Build tag_index entries for a row's tag values given the schema.
    pub fn build_tag_index(&mut self, chunk: &mut Chunk, row_idx: usize, tag_values: &[String]) {
        let tag_keys = &self.schema.tag_keys;
        for (key_idx, key) in tag_keys.iter().enumerate() {
            if let Some(value) = tag_values.get(key_idx) {
                chunk.tag_index
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(row_idx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::agent::buffer::chunk::{Chunk, FieldDef, FieldType, FieldValue, Row, Table, TableSchema};
    use crate::agent::buffer::query::query_table;

    fn make_test_table() -> Table {
        let schema = TableSchema {
            tag_keys: vec!["host".to_string()],
            field_defs: vec![
                FieldDef {
                    name: "cpu".to_string(),
                    value_type: FieldType::F64,
                },
                FieldDef {
                    name: "mem".to_string(),
                    value_type: FieldType::F64,
                },
            ],
        };
        let mut table = Table {
            name: "metrics".to_string(),
            schema,
            chunks: Vec::new(),
        };

        let mut chunk = Chunk::new(0);

        // Row 0: host=srv01, time=100, cpu=0.5, mem=0.8
        let row0 = Row {
            time: 100,
            tag_values: vec!["srv01".to_string()],
            field_values: vec![Some(FieldValue::F64(0.5)), Some(FieldValue::F64(0.8))],
        };
        chunk.rows.push(row0);
        table.build_tag_index(&mut chunk, 0, &["srv01".to_string()]);

        // Row 1: host=srv02, time=200, cpu=0.3, mem=0.6
        let row1 = Row {
            time: 200,
            tag_values: vec!["srv02".to_string()],
            field_values: vec![Some(FieldValue::F64(0.3)), Some(FieldValue::F64(0.6))],
        };
        chunk.rows.push(row1);
        table.build_tag_index(&mut chunk, 1, &["srv02".to_string()]);

        // Row 2: host=srv01, time=300, cpu=0.9, mem=0.95
        let row2 = Row {
            time: 300,
            tag_values: vec!["srv01".to_string()],
            field_values: vec![Some(FieldValue::F64(0.9)), Some(FieldValue::F64(0.95))],
        };
        chunk.rows.push(row2);
        table.build_tag_index(&mut chunk, 2, &["srv01".to_string()]);

        // Row 3: host=srv03, time=400, cpu=0.1, mem=0.2
        let row3 = Row {
            time: 400,
            tag_values: vec!["srv03".to_string()],
            field_values: vec![Some(FieldValue::F64(0.1)), Some(FieldValue::F64(0.2))],
        };
        chunk.rows.push(row3);
        table.build_tag_index(&mut chunk, 3, &["srv03".to_string()]);

        // Set time bounds on the chunk explicitly
        chunk.time_min = 100;
        chunk.time_max = 400;

        table.chunks.push(chunk);
        table
    }

    #[test]
    fn test_tag_filter_srv01() {
        let table = make_test_table();
        let results = query_table(&table, None, None, Some("host"), Some("srv01"));
        // Should match rows 0 and 2 (host=srv01)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].time, 100);
        assert_eq!(results[1].time, 300);
        assert_eq!(results[0].tags.get("host").unwrap(), "srv01");
        assert_eq!(results[1].tags.get("host").unwrap(), "srv01");
    }

    #[test]
    fn test_time_range_filter() {
        let table = make_test_table();
        let results = query_table(&table, Some(150), Some(350), None, None);
        // Should match rows 1 and 2 (time 200 and 300)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].time, 200);
        assert_eq!(results[1].time, 300);
    }

    #[test]
    fn test_tag_and_time_filter() {
        let table = make_test_table();
        let results = query_table(&table, Some(50), Some(250), Some("host"), Some("srv01"));
        // Should match only row 0: host=srv01, time=100 (row 2 has time=300, out of range)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].time, 100);
        assert_eq!(results[0].tags.get("host").unwrap(), "srv01");
    }

    #[test]
    fn test_nonexistent_tag() {
        let table = make_test_table();
        let results = query_table(&table, None, None, Some("host"), Some("nonexistent"));
        assert!(results.is_empty());
    }

    #[test]
    fn test_no_filters_returns_all() {
        let table = make_test_table();
        let results = query_table(&table, None, None, None, None);
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_time_range_no_match() {
        let table = make_test_table();
        let results = query_table(&table, Some(1000), Some(2000), None, None);
        assert!(results.is_empty());
    }

    // ── Test 2: Schema evolution + Chunk basics ──────────────────────────

    #[test]
    fn test_schema_ensure_field_new_field_added() {
        let mut schema = TableSchema::new();
        let idx = schema.ensure_field("cpu", FieldType::F64);
        assert_eq!(idx, 0);
        assert_eq!(schema.field_defs.len(), 1);
        assert_eq!(schema.field_defs[0].name, "cpu");
        assert_eq!(schema.field_defs[0].value_type, FieldType::F64);
    }

    #[test]
    fn test_schema_ensure_field_existing_returns_same_index() {
        let mut schema = TableSchema::new();
        let idx1 = schema.ensure_field("cpu", FieldType::F64);
        let idx2 = schema.ensure_field("mem", FieldType::F64);
        let idx3 = schema.ensure_field("cpu", FieldType::I64); // different type, same name
        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(idx3, 0, "existing field should return original index");
        assert_eq!(schema.field_defs.len(), 2, "should not add duplicate field");
        // Type should still be the original (F64), not I64
        assert_eq!(schema.field_defs[0].value_type, FieldType::F64);
    }

    #[test]
    fn test_schema_ensure_tag_key_new_key_added() {
        let mut schema = TableSchema::new();
        let idx = schema.ensure_tag_key("host");
        assert_eq!(idx, 0);
        assert_eq!(schema.tag_keys.len(), 1);
        assert_eq!(schema.tag_keys[0], "host");
    }

    #[test]
    fn test_schema_ensure_tag_key_existing_returns_same_index() {
        let mut schema = TableSchema::new();
        let idx1 = schema.ensure_tag_key("host");
        let idx2 = schema.ensure_tag_key("region");
        let idx3 = schema.ensure_tag_key("host");
        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(idx3, 0);
        assert_eq!(schema.tag_keys.len(), 2);
        assert_eq!(schema.tag_keys, vec!["host", "region"]);
    }

    #[test]
    fn test_chunk_new_initializes_correctly() {
        let chunk = Chunk::new(42);
        assert!(chunk.rows.is_empty());
        assert_eq!(chunk.chunk_time, 42);
        assert_eq!(chunk.time_min, i64::MAX);
        assert_eq!(chunk.time_max, i64::MIN);
        assert_eq!(chunk.min_wal_seq, u64::MAX);
        assert_eq!(chunk.max_wal_seq, 0);
        assert_eq!(chunk.row_count(), 0);
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_chunk_insert_updates_time_bounds_and_row_count() {
        let mut chunk = Chunk::new(0);
        let row = Row {
            time: 100,
            tag_values: vec![],
            field_values: vec![],
        };
        chunk.insert(row.clone(), 1);

        assert_eq!(chunk.time_min, 100);
        assert_eq!(chunk.time_max, 100);
        assert_eq!(chunk.row_count(), 1);

        // Insert a later time
        chunk.insert(
            Row {
                time: 200,
                tag_values: vec![],
                field_values: vec![],
            },
            2,
        );
        assert_eq!(chunk.time_min, 100);
        assert_eq!(chunk.time_max, 200);
        assert_eq!(chunk.row_count(), 2);

        // Insert an earlier time
        chunk.insert(
            Row {
                time: 50,
                tag_values: vec![],
                field_values: vec![],
            },
            3,
        );
        assert_eq!(chunk.time_min, 50);
        assert_eq!(chunk.time_max, 200);
        assert_eq!(chunk.row_count(), 3);
    }

    #[test]
    fn test_chunk_insert_updates_wal_seq_bounds() {
        let mut chunk = Chunk::new(0);
        let row = Row {
            time: 100,
            tag_values: vec![],
            field_values: vec![],
        };

        chunk.insert(row.clone(), 5);
        assert_eq!(chunk.min_wal_seq, 5);
        assert_eq!(chunk.max_wal_seq, 5);

        chunk.insert(row.clone(), 3);
        assert_eq!(chunk.min_wal_seq, 3);
        assert_eq!(chunk.max_wal_seq, 5);

        chunk.insert(row.clone(), 10);
        assert_eq!(chunk.min_wal_seq, 3);
        assert_eq!(chunk.max_wal_seq, 10);
    }

    #[test]
    fn test_chunk_estimated_size_grows_with_rows() {
        let mut chunk = Chunk::new(0);
        let row = Row {
            time: 100,
            tag_values: vec![],
            field_values: vec![],
        };

        // avg_row_bytes defaults to 0, so estimated_size uses max(0, 64) = 64 per row
        assert_eq!(chunk.estimated_size(), 0);

        chunk.insert(row.clone(), 1);
        assert_eq!(chunk.estimated_size(), 64);

        chunk.insert(row, 2);
        assert_eq!(chunk.estimated_size(), 128);

        // Set a higher avg_row_bytes
        chunk.avg_row_bytes = 200;
        assert_eq!(chunk.estimated_size(), 400);
    }

    #[test]
    fn test_build_tag_index_with_multiple_tags() {
        let mut schema = TableSchema::new();
        schema.ensure_tag_key("host");
        schema.ensure_tag_key("region");

        let mut table = Table {
            name: "metrics".to_string(),
            schema,
            chunks: Vec::new(),
        };

        let mut chunk = Chunk::new(0);

        // Insert rows and build tag index
        chunk.rows.push(Row {
            time: 100,
            tag_values: vec!["srv01".to_string(), "us-east".to_string()],
            field_values: vec![],
        });
        table.build_tag_index(&mut chunk, 0, &["srv01".to_string(), "us-east".to_string()]);

        chunk.rows.push(Row {
            time: 200,
            tag_values: vec!["srv02".to_string(), "us-west".to_string()],
            field_values: vec![],
        });
        table.build_tag_index(&mut chunk, 1, &["srv02".to_string(), "us-west".to_string()]);

        chunk.rows.push(Row {
            time: 300,
            tag_values: vec!["srv01".to_string(), "us-west".to_string()],
            field_values: vec![],
        });
        table.build_tag_index(&mut chunk, 2, &["srv01".to_string(), "us-west".to_string()]);

        // Check host index
        let host_idx = &chunk.tag_index["host"];
        assert_eq!(host_idx["srv01"], vec![0, 2]);
        assert_eq!(host_idx["srv02"], vec![1]);

        // Check region index
        let region_idx = &chunk.tag_index["region"];
        assert_eq!(region_idx["us-east"], vec![0]);
        assert_eq!(region_idx["us-west"], vec![1, 2]);
    }

    // ── Test 5: Multi-chunk scenarios ──────────────────────────────────

    #[test]
    fn test_table_get_or_create_chunk_returns_existing_for_same_time() {
        let mut table = Table::new("test".to_string());
        {
            let _ = table.get_or_create_chunk(100);
        } // borrow dropped

        // Calling get_or_create_chunk again with same time should return existing
        {
            let _c2 = table.get_or_create_chunk(100);
        }

        assert_eq!(table.chunks.len(), 1, "should not create duplicate chunk");
        assert_eq!(table.chunks[0].chunk_time, 100);
    }

    #[test]
    fn test_table_get_or_create_chunk_creates_new_for_different_time() {
        let mut table = Table::new("test".to_string());
        let _c1 = table.get_or_create_chunk(100);
        let _c2 = table.get_or_create_chunk(200);
        let _c3 = table.get_or_create_chunk(50);

        assert_eq!(table.chunks.len(), 3);
        // Chunks must be sorted by chunk_time (binary_search_by requires sorted vec)
        assert_eq!(table.chunks[0].chunk_time, 50);
        assert_eq!(table.chunks[1].chunk_time, 100);
        assert_eq!(table.chunks[2].chunk_time, 200);
    }

    #[test]
    fn test_table_chunks_stay_sorted_by_chunk_time() {
        let mut table = Table::new("test".to_string());

        // Insert in non-sorted order
        let times = vec![300, 100, 500, 200, 400];
        for t in &times {
            table.get_or_create_chunk(*t);
        }

        // Verify sorted order is maintained
        assert_eq!(table.chunks.len(), 5);
        for i in 1..table.chunks.len() {
            assert!(
                table.chunks[i - 1].chunk_time < table.chunks[i].chunk_time,
                "chunks not sorted: chunk[{}].chunk_time={} >= chunk[{}].chunk_time={}",
                i - 1, table.chunks[i - 1].chunk_time,
                i, table.chunks[i].chunk_time,
            );
        }
        assert_eq!(table.chunks[0].chunk_time, 100);
        assert_eq!(table.chunks[4].chunk_time, 500);
    }

    #[test]
    fn test_chunk_removal_from_vec_simulating_flush_cleanup() {
        let mut table = Table::new("test".to_string());

        // Create 3 chunks with data
        for ct in [0, 10, 20] {
            let chunk = table.get_or_create_chunk(ct);
            let row = Row {
                time: ct * 10,
                tag_values: vec![],
                field_values: vec![],
            };
            chunk.insert(row, ct as u64);
        }
        assert_eq!(table.chunks.len(), 3);

        // Simulate flushing: remove the first chunk (chunk_time=0)
        table.chunks.remove(0);
        assert_eq!(table.chunks.len(), 2);
        assert_eq!(table.chunks[0].chunk_time, 10);
        assert_eq!(table.chunks[1].chunk_time, 20);

        // Remove remaining chunks one by one
        table.chunks.remove(1);
        assert_eq!(table.chunks.len(), 1);
        assert_eq!(table.chunks[0].chunk_time, 10);

        table.chunks.clear();
        assert_eq!(table.chunks.len(), 0);
        assert_eq!(table.estimated_size(), 0);
    }
}
