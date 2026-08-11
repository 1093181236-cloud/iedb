// Lightweight CPU / memory sampling via /proc. No external crates.
// A background task calls SystemSampler::tick() every ~5s; the status
// handler reads `sampler.current()` to serve the latest cached values.

/// A snapshot of system resource usage.
#[derive(Debug, Clone)]
pub struct SystemInfo {
    pub cpu_percent: f32,
    pub memory_total_mb: u64,
    pub memory_used_mb: u64,
    pub process_rss_mb: u64,
}

/// Holds the last sampled SystemInfo behind a tokio::sync::RwLock.
pub struct SystemSampler {
    info: tokio::sync::RwLock<SystemInfo>,
}

impl SystemSampler {
    /// Create a new sampler with initial zero values.
    pub fn new() -> Self {
        SystemSampler {
            info: tokio::sync::RwLock::new(SystemInfo {
                cpu_percent: 0.0,
                memory_total_mb: 0,
                memory_used_mb: 0,
                process_rss_mb: 0,
            }),
        }
    }

    /// Sample current system state. Call periodically (every 5s).
    pub async fn tick(&self) {
        let cpu = sample_cpu();
        let mem = sample_memory();
        let rss = sample_process_rss_mb();

        let mut info = self.info.write().await;
        info.cpu_percent = cpu;
        info.memory_total_mb = mem.0;
        info.memory_used_mb = mem.1;
        info.process_rss_mb = rss;
    }

    /// Return the most recently sampled values.
    pub async fn current(&self) -> SystemInfo {
        self.info.read().await.clone()
    }
}

// ── /proc readers ──

/// Read /proc/stat, compute CPU usage as delta from previous call.
/// Returns 0.0 on first call (no baseline).
fn sample_cpu() -> f32 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static PREV_IDLE: AtomicU64 = AtomicU64::new(0);
    static PREV_TOTAL: AtomicU64 = AtomicU64::new(0);
    static HAS_BASELINE: AtomicU64 = AtomicU64::new(0);

    let content = match std::fs::read_to_string("/proc/stat") {
        Ok(c) => c,
        Err(_) => return 0.0,
    };
    let cpu_line = match content.lines().find(|l| l.starts_with("cpu ")) {
        Some(l) => l,
        None => return 0.0,
    };

    let fields: Vec<u64> = cpu_line
        .split_whitespace()
        .skip(1) // skip "cpu"
        .filter_map(|s| s.parse::<u64>().ok())
        .collect();

    if fields.len() < 4 {
        return 0.0;
    }

    // idle = idle + iowait
    let idle = fields.get(3).copied().unwrap_or(0) + fields.get(4).copied().unwrap_or(0);
    let total: u64 = fields.iter().sum();

    let prev_idle = PREV_IDLE.swap(idle, Ordering::Relaxed);
    let prev_total = PREV_TOTAL.swap(total, Ordering::Relaxed);

    if HAS_BASELINE.swap(1, Ordering::Relaxed) == 0 {
        return 0.0; // first sample, no delta yet
    }

    let delta_total = total.saturating_sub(prev_total);
    let delta_idle = idle.saturating_sub(prev_idle);

    if delta_total == 0 {
        return 0.0;
    }

    let used = delta_total.saturating_sub(delta_idle);
    let pct = (used as f32 / delta_total as f32) * 100.0;

    // Clamp to sane range
    pct.clamp(0.0, 100.0)
}

/// Read /proc/meminfo, return (total_mb, used_mb).
fn sample_memory() -> (u64, u64) {
    let content = match std::fs::read_to_string("/proc/meminfo") {
        Ok(c) => c,
        Err(_) => return (0, 0),
    };

    let mut total_kb: u64 = 0;
    let mut avail_kb: u64 = 0;

    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            total_kb = parse_kb(line);
        } else if line.starts_with("MemAvailable:") {
            avail_kb = parse_kb(line);
        }
    }

    let total_mb = total_kb / 1024;
    let used_mb = if total_kb > avail_kb {
        (total_kb - avail_kb) / 1024
    } else {
        0
    };
    (total_mb, used_mb)
}

fn parse_kb(line: &str) -> u64 {
    line.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Read /proc/self/status, return VmRSS in MB.
fn sample_process_rss_mb() -> u64 {
    let content = match std::fs::read_to_string("/proc/self/status") {
        Ok(c) => c,
        Err(_) => return 0,
    };
    for line in content.lines() {
        if line.starts_with("VmRSS:") {
            let kb = parse_kb(line);
            return kb / 1024;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kb_typical_line() {
        assert_eq!(parse_kb("MemTotal:        2048000 kB"), 2048000);
        assert_eq!(parse_kb("VmRSS:       12345 kB"), 12345);
    }

    #[test]
    fn test_parse_kb_empty_returns_zero() {
        assert_eq!(parse_kb(""), 0);
        assert_eq!(parse_kb("garbage"), 0);
    }

    #[test]
    fn test_sample_memory_and_rss_do_not_panic() {
        // These may return 0 if /proc is unavailable (e.g. macOS), but must not panic
        let (total, used) = sample_memory();
        assert!(total >= used, "total {total} >= used {used}");

        let rss = sample_process_rss_mb();
        // RSS could be 0 on platforms without /proc — just verify it's not absurd
        assert!(rss < 1_000_000, "RSS {rss} MB is implausibly large");
    }

    #[test]
    fn test_sampler_initial_values_are_zero() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let sampler = SystemSampler::new();
        let info = rt.block_on(sampler.current());
        assert_eq!(info.cpu_percent, 0.0);
        assert_eq!(info.process_rss_mb, 0);
    }

    #[tokio::test]
    async fn test_sampler_tick_updates_values() {
        let sampler = SystemSampler::new();
        sampler.tick().await;

        let info = sampler.current().await;
        // After tick, values should be populated (on Linux) or remain 0 (macOS/other).
        // We only assert they're in valid ranges.
        assert!(info.cpu_percent >= 0.0 && info.cpu_percent <= 100.0,
            "cpu_percent {} out of range", info.cpu_percent);
        assert!(info.memory_total_mb < 1_000_000,
            "memory_total_mb {} implausible", info.memory_total_mb);
    }
}
