// Runtime-hot configuration: the four agent parameters that the server can
// change without a restart. Backed by atomics so the hot read paths
// (snapshot loop every ~5s, WAL flush every ~1s, buffer_op per write) stay
// lock-free; heartbeat updates are single atomic stores.
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize};

pub struct HotConfig {
    snapshot_interval_secs: AtomicI64,
    memory_limit_bytes: AtomicUsize,
    wal_flush_interval: AtomicU64,
    wal_max_buffer_ops: AtomicUsize,
}

impl HotConfig {
    /// Initialize from the static config (local file / cached server config).
    pub fn from_config(c: &crate::config::Config) -> Self {
        HotConfig {
            snapshot_interval_secs: AtomicI64::new(c.snapshot_interval_secs()),
            memory_limit_bytes: AtomicUsize::new(c.memory_limit_bytes()),
            wal_flush_interval: AtomicU64::new(c.wal.flush_interval_secs),
            wal_max_buffer_ops: AtomicUsize::new(c.wal.max_write_buffer_ops),
        }
    }

    /// Best-effort apply of a server-provided config update JSON:
    /// `{"flush": {"snapshot_interval": "10m", "memory_limit": "512MB"},
    ///   "wal": {"flush_interval_secs": 1, "max_write_buffer_ops": 100000}}`.
    /// Missing sections and invalid values leave the current value untouched.
    pub fn apply_update(&self, update: &serde_json::Value) {
        if let Some(flush) = update.get("flush") {
            if let Some(s) = flush.get("snapshot_interval").and_then(|v| v.as_str()) {
                if let Some(secs) = crate::config::parse_duration_opt(s) {
                    if secs > 0 {
                        self.snapshot_interval_secs.store(secs, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
            if let Some(m) = flush.get("memory_limit").and_then(|v| v.as_str()) {
                if let Some(bytes) = crate::config::parse_bytes_opt(m) {
                    if bytes > 0 {
                        self.memory_limit_bytes.store(bytes, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }
        if let Some(wal) = update.get("wal") {
            if let Some(v) = wal.get("flush_interval_secs").and_then(|v| v.as_u64()) {
                if v > 0 {
                    self.wal_flush_interval.store(v, std::sync::atomic::Ordering::Relaxed);
                }
            }
            if let Some(v) = wal.get("max_write_buffer_ops").and_then(|v| v.as_u64()) {
                if v > 0 {
                    self.wal_max_buffer_ops.store(v as usize, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    pub fn snapshot_interval_secs(&self) -> i64 {
        self.snapshot_interval_secs.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn memory_limit_bytes(&self) -> usize {
        self.memory_limit_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn wal_flush_interval(&self) -> u64 {
        self.wal_flush_interval.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn wal_max_buffer_ops(&self) -> usize {
        self.wal_max_buffer_ops.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> crate::config::Config {
        toml::from_str(
            r#"
            [server]
            [data]
            dir = "/tmp/iedb-hot-test"
            [flush]
            snapshot_interval = "10m"
            memory_limit = "512MB"
            [wal]
            flush_interval_secs = 1
            max_write_buffer_ops = 100000
            "#,
        )
        .unwrap()
    }

    #[test]
    fn test_from_config_initializes_all_fields() {
        let c = test_config();
        let hot = HotConfig::from_config(&c);
        assert_eq!(hot.snapshot_interval_secs(), 600);
        assert_eq!(hot.memory_limit_bytes(), 512 * 1024 * 1024);
        assert_eq!(hot.wal_flush_interval(), 1);
        assert_eq!(hot.wal_max_buffer_ops(), 100_000);
    }

    #[test]
    fn test_apply_update_changes_all_four_fields() {
        let hot = HotConfig::from_config(&test_config());
        let update = serde_json::json!({
            "flush": {"snapshot_interval": "2m", "memory_limit": "256MB"},
            "wal": {"flush_interval_secs": 5, "max_write_buffer_ops": 5000}
        });
        hot.apply_update(&update);
        assert_eq!(hot.snapshot_interval_secs(), 120);
        assert_eq!(hot.memory_limit_bytes(), 256 * 1024 * 1024);
        assert_eq!(hot.wal_flush_interval(), 5);
        assert_eq!(hot.wal_max_buffer_ops(), 5000);
    }

    #[test]
    fn test_apply_update_partial_leaves_others_untouched() {
        let hot = HotConfig::from_config(&test_config());
        hot.apply_update(&serde_json::json!({"flush": {"snapshot_interval": "30s"}}));
        assert_eq!(hot.snapshot_interval_secs(), 30);
        assert_eq!(hot.memory_limit_bytes(), 512 * 1024 * 1024, "untouched field must keep old value");
        assert_eq!(hot.wal_flush_interval(), 1);
        assert_eq!(hot.wal_max_buffer_ops(), 100_000);
    }

    #[test]
    fn test_apply_update_invalid_values_are_skipped() {
        let hot = HotConfig::from_config(&test_config());
        // First apply a valid change so "skipped" is distinguishable from
        // "reset to fallback" (both would look like the original value).
        hot.apply_update(&serde_json::json!({
            "flush": {"snapshot_interval": "30s", "memory_limit": "256MB"},
            "wal": {"flush_interval_secs": 7, "max_write_buffer_ops": 7000}
        }));
        assert_eq!(hot.snapshot_interval_secs(), 30);

        hot.apply_update(&serde_json::json!({
            "flush": {"snapshot_interval": "garbage", "memory_limit": ""},
            "wal": {"flush_interval_secs": 0, "max_write_buffer_ops": 0}
        }));
        assert_eq!(hot.snapshot_interval_secs(), 30, "invalid duration must be skipped, not reset");
        assert_eq!(hot.memory_limit_bytes(), 256 * 1024 * 1024, "invalid size must be skipped");
        assert_eq!(hot.wal_flush_interval(), 7, "zero interval must be skipped");
        assert_eq!(hot.wal_max_buffer_ops(), 7000, "zero limit must be skipped");
    }

    #[test]
    fn test_apply_update_missing_sections_are_ignored() {
        let hot = HotConfig::from_config(&test_config());
        hot.apply_update(&serde_json::json!({"other": true}));
        assert_eq!(hot.snapshot_interval_secs(), 600);
        assert_eq!(hot.wal_flush_interval(), 1);
    }
}
