use super::WalContents;
use bitcode;

const FILE_TYPE_IDENTIFIER: &[u8; 8] = b"iedb.a01";

impl WalContents {
    /// Serialize to the WAL file format: identifier + CRC32 + bitcode bytes.
    pub fn serialize_to_file(&self) -> Vec<u8> {
        let payload = bitcode::serialize(self).expect("WAL serialize");
        let crc = crc32fast::hash(&payload);
        let mut out = Vec::with_capacity(8 + 4 + payload.len());
        out.extend_from_slice(FILE_TYPE_IDENTIFIER);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Deserialize from WAL file bytes.
    pub fn deserialize_from_file(data: &[u8]) -> Result<Self, String> {
        if data.len() < 12 {
            return Err("WAL file too short".into());
        }
        if &data[..8] != FILE_TYPE_IDENTIFIER {
            return Err("invalid WAL file identifier".into());
        }
        let stored_crc = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let payload = &data[12..];
        let actual_crc = crc32fast::hash(payload);
        if stored_crc != actual_crc {
            return Err(format!(
                "WAL CRC mismatch: stored={:08x} actual={:08x}",
                stored_crc, actual_crc
            ));
        }
        bitcode::deserialize(payload).map_err(|e| format!("WAL deserialize: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::buffer::chunk::{FieldValue, Row};
    use crate::agent::wal::{WalOp, WriteBatch};

    fn make_test_wal_contents() -> WalContents {
        WalContents {
            wal_file_number: 42,
            ops: vec![
                WalOp::Write(WriteBatch {
                    db_name: "testdb".into(),
                    table_name: "cpu".into(),
                    chunk_time: 1000,
                    field_names: vec!["usage".into()],
                    tag_keys: vec!["host".into()],
                    rows: vec![
                        Row {
                            time: 1000,
                            tag_values: vec!["srv01".into()],
                            field_values: vec![Some(FieldValue::F64(0.5))],
                        },
                        Row {
                            time: 2000,
                            tag_values: vec!["srv02".into()],
                            field_values: vec![Some(FieldValue::F64(0.8))],
                        },
                    ],
                }),
                WalOp::Noop,
            ],
            persist_timestamp_ms: 1700000000000,
        }
    }

    #[test]
    fn test_round_trip() {
        let original = make_test_wal_contents();
        let data = original.serialize_to_file();
        let restored = WalContents::deserialize_from_file(&data).expect("deserialize");

        assert_eq!(restored.wal_file_number, original.wal_file_number);
        assert_eq!(restored.persist_timestamp_ms, original.persist_timestamp_ms);
        assert_eq!(restored.ops.len(), original.ops.len());

        match (&restored.ops[0], &original.ops[0]) {
            (WalOp::Write(r), WalOp::Write(o)) => {
                assert_eq!(r.db_name, o.db_name);
                assert_eq!(r.table_name, o.table_name);
                assert_eq!(r.chunk_time, o.chunk_time);
                assert_eq!(r.rows.len(), o.rows.len());
                for (rr, or) in r.rows.iter().zip(o.rows.iter()) {
                    assert_eq!(rr.time, or.time);
                    assert_eq!(rr.tag_values, or.tag_values);
                    assert_eq!(rr.field_values.len(), or.field_values.len());
                }
            }
            _ => panic!("expected WalOp::Write"),
        }
        assert!(matches!(restored.ops[1], WalOp::Noop));
    }

    #[test]
    fn test_crc_integrity() {
        let original = make_test_wal_contents();
        let mut data = original.serialize_to_file();

        // Corrupt a byte in the payload (after the 12-byte header)
        data[15] ^= 0xFF;

        let result = WalContents::deserialize_from_file(&data);
        assert!(result.is_err(), "should fail on CRC mismatch");
        assert!(result.unwrap_err().contains("CRC mismatch"));
    }

    #[test]
    fn test_too_short_data() {
        let result = WalContents::deserialize_from_file(&[0u8; 5]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too short"));
    }

    #[test]
    fn test_bad_identifier() {
        let mut data = vec![0u8; 20];
        // Use a wrong identifier
        data[..8].copy_from_slice(b"BAD.ID  ");
        let result = WalContents::deserialize_from_file(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid WAL file identifier"));
    }

    #[test]
    fn test_exactly_12_bytes() {
        // 12 bytes is the header but no payload — deserialize should fail at bitcode
        let mut data = vec![0u8; 12];
        data[..8].copy_from_slice(b"iedb.a01");
        // crc will be 0 since payload is empty (no bytes after header)
        // but bitcode deserialize of empty slice should fail
        let result = WalContents::deserialize_from_file(&data);
        assert!(result.is_err());
    }
}
