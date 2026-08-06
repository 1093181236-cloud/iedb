use std::sync::Arc;

use parquet::data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::agent::buffer::chunk::{Chunk, FieldValue, Row, Table};

fn sanitize_column_name(name: &str) -> String {
    name.replace('-', "_").replace(' ', "_")
}

/// Merge-sort rows from multiple chunks, dedup, and write as a single Parquet file.
/// Returns the serialized Parquet bytes.
pub fn flush_chunks_to_parquet(table: &Table, chunks: &[&Chunk]) -> Result<Vec<u8>, String> {
    if chunks.is_empty() {
        return Err("no chunks to flush".into());
    }

    // Step 1: Build Parquet schema from TableSchema
    let mut schema_fields = vec!["required int64 time;".to_string()];
    for tag_key in &table.schema.tag_keys {
        let safe_name = sanitize_column_name(tag_key);
        schema_fields.push(format!("optional binary {} (STRING);", safe_name));
    }
    for fdef in &table.schema.field_defs {
        let safe_name = sanitize_column_name(&fdef.name);
        let pq_type = match fdef.value_type {
            crate::agent::buffer::chunk::FieldType::I64 => "INT64",
            crate::agent::buffer::chunk::FieldType::F64 => "DOUBLE",
            crate::agent::buffer::chunk::FieldType::U64 => "INT64",
            crate::agent::buffer::chunk::FieldType::Bool => "BOOLEAN",
            crate::agent::buffer::chunk::FieldType::String => "BINARY (STRING)",
        };
        schema_fields.push(format!("optional {} {};", pq_type, safe_name));
    }

    let message_type = format!("message schema {{ {} }}", schema_fields.join(" "));
    let schema = Arc::new(
        parse_message_type(&message_type)
            .map_err(|e| format!("parquet schema error: {}", e))?,
    );

    // Step 2: Collect and merge-sort all rows
    let mut all_rows: Vec<&Row> = chunks.iter().flat_map(|c| c.rows.iter()).collect();
    all_rows.sort_by_key(|r| r.time);
    // C2 fix: dedup by full row identity (time + tags + fields), not just time+tags
    all_rows.dedup_by(|a, b| a.time == b.time && a.tag_values == b.tag_values && a.field_values == b.field_values);

    // Step 3: Write to Parquet using the column-oriented writer API
    let mut buf = Vec::new();
    let props = Arc::new(WriterProperties::new());
    let mut writer = SerializedFileWriter::new(&mut buf, schema, props)
        .map_err(|e| format!("parquet writer: {}", e))?;

    let mut row_group = writer
        .next_row_group()
        .map_err(|e| format!("row group: {}", e))?;

    let num_tags = table.schema.tag_keys.len();
    let total_cols = 1 + num_tags + table.schema.field_defs.len();
    let num_rows = all_rows.len();

    for col_idx in 0..total_cols {
        let mut col_writer = row_group
            .next_column()
            .map_err(|e| format!("next column: {}", e))?
            .ok_or_else(|| "unexpected end of columns".to_string())?;

        match col_idx {
            // Column 0: time (required int64, non-nullable)
            0 => {
                let vals: Vec<i64> = all_rows.iter().map(|r| r.time).collect();
                col_writer
                    .typed::<Int64Type>()
                    .write_batch(&vals, None, None)
                    .map_err(|e| format!("write time column: {}", e))?;
            }
            // Tag columns (optional STRING)
            i if i <= num_tags => {
                let tag_idx = i - 1;
                let mut vals: Vec<ByteArray> = Vec::new();
                let mut def_levels: Vec<i16> = Vec::with_capacity(num_rows);
                for row in &all_rows {
                    match row.tag_values.get(tag_idx) {
                        Some(v) if !v.is_empty() => {
                            vals.push(ByteArray::from(v.as_str()));
                            def_levels.push(1);
                        }
                        _ => {
                            def_levels.push(0);
                        }
                    }
                }
                col_writer
                    .typed::<ByteArrayType>()
                    .write_batch(&vals, Some(&def_levels), None)
                    .map_err(|e| format!("write tag column {}: {}", tag_idx, e))?;
            }
            // Field columns (optional, typed)
            _ => {
                let field_idx = col_idx - 1 - num_tags;
                if let Some(fdef) = table.schema.field_defs.get(field_idx) {
                    match fdef.value_type {
                        crate::agent::buffer::chunk::FieldType::I64 | crate::agent::buffer::chunk::FieldType::U64 => {
                            let mut vals: Vec<i64> = Vec::new();
                            let mut def_levels: Vec<i16> = Vec::with_capacity(num_rows);
                            for row in &all_rows {
                                match row.field_values.get(field_idx) {
                                    Some(Some(FieldValue::I64(v))) => {
                                        vals.push(*v);
                                        def_levels.push(1);
                                    }
                                    Some(Some(FieldValue::U64(v))) => {
                                        let val = i64::try_from(*v)
                                            .map_err(|_| format!("u64 value {} exceeds i64 range", v))?;
                                        vals.push(val);
                                        def_levels.push(1);
                                    }
                                    _ => {
                                        def_levels.push(0);
                                    }
                                }
                            }
                            col_writer
                                .typed::<Int64Type>()
                                .write_batch(&vals, Some(&def_levels), None)
                                .map_err(|e| format!("write field {}: {}", field_idx, e))?;
                        }
                        crate::agent::buffer::chunk::FieldType::F64 => {
                            let mut vals: Vec<f64> = Vec::new();
                            let mut def_levels: Vec<i16> = Vec::with_capacity(num_rows);
                            for row in &all_rows {
                                match row.field_values.get(field_idx) {
                                    Some(Some(FieldValue::F64(v))) => {
                                        vals.push(*v);
                                        def_levels.push(1);
                                    }
                                    _ => {
                                        def_levels.push(0);
                                    }
                                }
                            }
                            col_writer
                                .typed::<DoubleType>()
                                .write_batch(&vals, Some(&def_levels), None)
                                .map_err(|e| format!("write field {}: {}", field_idx, e))?;
                        }
                        crate::agent::buffer::chunk::FieldType::Bool => {
                            let mut vals: Vec<bool> = Vec::new();
                            let mut def_levels: Vec<i16> = Vec::with_capacity(num_rows);
                            for row in &all_rows {
                                match row.field_values.get(field_idx) {
                                    Some(Some(FieldValue::Bool(v))) => {
                                        vals.push(*v);
                                        def_levels.push(1);
                                    }
                                    _ => {
                                        def_levels.push(0);
                                    }
                                }
                            }
                            col_writer
                                .typed::<BoolType>()
                                .write_batch(&vals, Some(&def_levels), None)
                                .map_err(|e| format!("write field {}: {}", field_idx, e))?;
                        }
                        crate::agent::buffer::chunk::FieldType::String => {
                            let mut vals: Vec<ByteArray> = Vec::new();
                            let mut def_levels: Vec<i16> = Vec::with_capacity(num_rows);
                            for row in &all_rows {
                                match row.field_values.get(field_idx) {
                                    Some(Some(FieldValue::String(v))) => {
                                        vals.push(ByteArray::from(v.as_str()));
                                        def_levels.push(1);
                                    }
                                    _ => {
                                        def_levels.push(0);
                                    }
                                }
                            }
                            col_writer
                                .typed::<ByteArrayType>()
                                .write_batch(&vals, Some(&def_levels), None)
                                .map_err(|e| format!("write field {}: {}", field_idx, e))?;
                        }
                    }
                }
            }
        }

        col_writer
            .close()
            .map_err(|e| format!("close column writer: {}", e))?;
    }

    row_group
        .close()
        .map_err(|e| format!("close row group: {}", e))?;
    writer.close().map_err(|e| format!("close writer: {}", e))?;

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::buffer::chunk::{Chunk, FieldDef, FieldType, FieldValue, Row, TableSchema};
    use bytes::Bytes;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;

    fn make_test_table_with_data() -> (Table, Chunk) {
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

        let table = Table {
            name: "metrics".to_string(),
            schema,
            chunks: Vec::new(),
        };

        let mut chunk = Chunk::new(0);
        chunk.rows.push(Row {
            time: 100,
            tag_values: vec!["srv01".to_string()],
            field_values: vec![Some(FieldValue::F64(0.5)), Some(FieldValue::F64(0.8))],
        });
        chunk.rows.push(Row {
            time: 200,
            tag_values: vec!["srv02".to_string()],
            field_values: vec![Some(FieldValue::F64(0.3)), Some(FieldValue::F64(0.6))],
        });
        chunk.rows.push(Row {
            time: 300,
            tag_values: vec!["srv01".to_string()],
            field_values: vec![Some(FieldValue::F64(0.9)), Some(FieldValue::F64(0.95))],
        });

        (table, chunk)
    }

    #[test]
    fn test_parquet_round_trip() {
        let (table, chunk) = make_test_table_with_data();
        let expected_row_count = chunk.rows.len();

        let parquet_bytes =
            flush_chunks_to_parquet(&table, &[&chunk]).expect("flush_chunks_to_parquet");

        // Read back the parquet file
        let bytes = Bytes::from(parquet_bytes);
        let reader = SerializedFileReader::new(bytes).expect("SerializedFileReader");

        let metadata = reader.metadata();
        assert!(metadata.num_row_groups() > 0);
        assert_eq!(metadata.file_metadata().num_rows() as usize, expected_row_count);

        // Iterate rows and verify
        let mut row_iter = reader.get_row_iter(None).expect("get_row_iter");
        let mut row_count = 0;

        while let Some(row) = row_iter.next() {
            let row = row.expect("read row");
            if row_count == 0 {
                // time=100, host=srv01, cpu=0.5, mem=0.8
                assert_eq!(row.get_long(0).expect("get_long"), 100);
                let host = row.get_string(1).expect("host");
                assert_eq!(host.as_str(), "srv01");
                assert!((row.get_double(2).expect("cpu") - 0.5).abs() < 0.001);
                assert!((row.get_double(3).expect("mem") - 0.8).abs() < 0.001);
            } else if row_count == 1 {
                assert_eq!(row.get_long(0).expect("get_long"), 200);
                let host = row.get_string(1).expect("host");
                assert_eq!(host.as_str(), "srv02");
                assert!((row.get_double(2).expect("cpu") - 0.3).abs() < 0.001);
                assert!((row.get_double(3).expect("mem") - 0.6).abs() < 0.001);
            } else if row_count == 2 {
                assert_eq!(row.get_long(0).expect("get_long"), 300);
                let host = row.get_string(1).expect("host");
                assert_eq!(host.as_str(), "srv01");
                assert!((row.get_double(2).expect("cpu") - 0.9).abs() < 0.001);
                assert!((row.get_double(3).expect("mem") - 0.95).abs() < 0.001);
            }
            row_count += 1;
        }

        assert_eq!(row_count, expected_row_count);
    }

    #[test]
    fn test_parquet_empty_chunks_error() {
        let table = Table {
            name: "empty".to_string(),
            schema: TableSchema {
                tag_keys: vec![],
                field_defs: vec![FieldDef {
                    name: "v".to_string(),
                    value_type: FieldType::F64,
                }],
            },
            chunks: Vec::new(),
        };
        let empty_chunks: &[&Chunk] = &[];
        let result = flush_chunks_to_parquet(&table, empty_chunks);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no chunks"));
    }
}
