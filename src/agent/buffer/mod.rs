pub mod chunk;
pub mod query;

use chunk::Table;
use std::collections::HashMap;

/// Stores all tables across all databases.
#[derive(Debug)]
pub struct Buffer {
    /// db_name => table_name => Table
    pub databases: HashMap<String, HashMap<String, Table>>,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer {
            databases: HashMap::new(),
        }
    }

    pub fn get_or_create_table(&mut self, db: &str, table: &str) -> &mut Table {
        self.databases
            .entry(db.to_string())
            .or_default()
            .entry(table.to_string())
            .or_insert_with(|| Table::new(table.to_string()))
    }

    pub fn get_table(&self, db: &str, table: &str) -> Option<&Table> {
        self.databases.get(db).and_then(|tables| tables.get(table))
    }

    pub fn get_table_mut(&mut self, db: &str, table: &str) -> Option<&mut Table> {
        self.databases.get_mut(db).and_then(|tables| tables.get_mut(table))
    }

    /// Total estimated memory usage across all tables and chunks.
    pub fn total_estimated_size(&self) -> usize {
        self.databases
            .values()
            .flat_map(|tables| tables.values())
            .map(|t| t.estimated_size())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::buffer::chunk::{FieldValue, Row};

    #[test]
    fn test_get_or_create_same_db_table_returns_same_table() {
        let mut buffer = Buffer::new();

        // Create the table and mutate it
        {
            let table = buffer.get_or_create_table("db1", "tbl1");
            table.schema.ensure_tag_key("host");
            table.schema.ensure_field("cpu", crate::agent::buffer::chunk::FieldType::F64);
        }

        // Get it again — must reflect the previous mutations
        {
            let table = buffer.get_or_create_table("db1", "tbl1");
            assert_eq!(table.schema.tag_keys.len(), 1);
            assert_eq!(table.schema.tag_keys[0], "host");
            assert_eq!(table.schema.field_defs.len(), 1);
            assert_eq!(table.schema.field_defs[0].name, "cpu");
        }

        // Internal map should have only one db and one table entry
        assert_eq!(buffer.databases.len(), 1);
        assert_eq!(buffer.databases.get("db1").unwrap().len(), 1);
    }

    #[test]
    fn test_get_or_create_different_tables_independent() {
        let mut buffer = Buffer::new();

        {
            let t1 = buffer.get_or_create_table("db1", "tbl1");
            t1.schema.ensure_tag_key("tag1");
        }
        {
            let t2 = buffer.get_or_create_table("db1", "tbl2");
            t2.schema.ensure_tag_key("tag2");
        }
        {
            let t3 = buffer.get_or_create_table("db2", "tbl1");
            t3.schema.ensure_tag_key("tag3");
        }

        // Each table keeps its own schema
        let t1 = buffer.get_table("db1", "tbl1").unwrap();
        let t2 = buffer.get_table("db1", "tbl2").unwrap();
        let t3 = buffer.get_table("db2", "tbl1").unwrap();

        assert_eq!(t1.schema.tag_keys, vec!["tag1"]);
        assert_eq!(t2.schema.tag_keys, vec!["tag2"]);
        assert_eq!(t3.schema.tag_keys, vec!["tag3"]);
    }

    #[test]
    fn test_get_table_existing_and_nonexistent() {
        let mut buffer = Buffer::new();
        buffer.get_or_create_table("db1", "tbl1");

        assert!(buffer.get_table("db1", "tbl1").is_some());
        assert!(buffer.get_table("db1", "nonexistent").is_none());
        assert!(buffer.get_table("nonexistent_db", "tbl1").is_none());
    }

    #[test]
    fn test_get_table_mut_can_mutate_through_reference() {
        let mut buffer = Buffer::new();
        buffer.get_or_create_table("db1", "tbl1");

        // Mutate through get_table_mut
        if let Some(table) = buffer.get_table_mut("db1", "tbl1") {
            table.schema.ensure_tag_key("host");
            table.schema.ensure_field("mem", crate::agent::buffer::chunk::FieldType::F64);
        }

        // Verify mutation persisted
        let table = buffer.get_table("db1", "tbl1").unwrap();
        assert_eq!(table.schema.tag_keys.len(), 1);
        assert_eq!(table.schema.tag_keys[0], "host");
        assert_eq!(table.schema.field_defs.len(), 1);
        assert_eq!(table.schema.field_defs[0].name, "mem");
    }

    #[test]
    fn test_get_table_mut_returns_none_for_nonexistent() {
        let mut buffer = Buffer::new();
        assert!(buffer.get_table_mut("db1", "tbl1").is_none());
    }

    #[test]
    fn test_total_estimated_size_empty_buffer_is_zero() {
        let buffer = Buffer::new();
        assert_eq!(buffer.total_estimated_size(), 0);
    }

    #[test]
    fn test_total_estimated_size_grows_with_data() {
        let mut buffer = Buffer::new();

        // Create a table with a chunk and insert rows
        let table = buffer.get_or_create_table("db1", "metrics");
        let chunk = table.get_or_create_chunk(0);
        chunk.avg_row_bytes = 128;

        for i in 0..10 {
            let row = Row {
                time: i * 100,
                tag_values: vec!["srv01".to_string()],
                field_values: vec![Some(FieldValue::F64(i as f64))],
            };
            chunk.insert(row, i as u64);
        }

        let size = buffer.total_estimated_size();
        assert!(size > 0, "size should be > 0, got {size}");
        // 10 rows, avg_row_bytes=128, so estimated >= max(avg_row_bytes, 64) * 10 = 128 * 10 = 1280
        assert!(size >= 1280);
    }

    #[test]
    fn test_total_estimated_size_shrinks_when_chunk_removed() {
        let mut buffer = Buffer::new();

        // Add data
        {
            let table = buffer.get_or_create_table("db1", "metrics");
            let chunk = table.get_or_create_chunk(0);
            chunk.avg_row_bytes = 200;

            let row = Row {
                time: 100,
                tag_values: vec![],
                field_values: vec![],
            };
            chunk.insert(row, 1);
        }

        let size_before = buffer.total_estimated_size();
        assert!(size_before > 0);

        // Remove all chunks (simulating flush cleanup)
        if let Some(table) = buffer.get_table_mut("db1", "metrics") {
            table.chunks.clear();
        }

        let size_after = buffer.total_estimated_size();
        assert!(size_after < size_before);
        assert_eq!(size_after, 0);
    }
}
