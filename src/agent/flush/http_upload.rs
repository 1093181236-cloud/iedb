use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;
use tracing;

/// Build the ingest endpoint URL. `chunk_time` (ns) makes the server-side
/// filename deterministic across retries; None keeps backward compat.
pub fn build_ingest_url(
    server_url: &str,
    db: &str,
    table: &str,
    chunk_time: Option<i64>,
) -> String {
    let mut url = format!(
        "{}/api/v1/ingest/parquet?db={}&measurement={}",
        server_url.trim_end_matches('/'),
        urlencoding(db),
        urlencoding(table)
    );
    if let Some(ct) = chunk_time {
        url.push_str(&format!("&chunk_time={}", ct));
    }
    url
}

/// Upload Parquet bytes to iedb server via HTTP.
pub async fn upload_parquet(
    client: &Client,
    server_url: &str,
    db: &str,
    table: &str,
    data: &[u8],
    auth_header: Option<&str>,
    agent_id: &str,
    chunk_time: Option<i64>,
) -> Result<(), UploadError> {
    let url = build_ingest_url(server_url, db, table, chunk_time);

    let mut req = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .header("x-agent-id", agent_id)
        .body(data.to_vec());
    if let Some(h) = auth_header {
        req = req.header("Authorization", h);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| UploadError::Http(e.to_string()))?;

    if resp.status().is_success() {
        tracing::info!(db = db, table = table, bytes = data.len(), "Parquet uploaded");
        Ok(())
    } else {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        Err(UploadError::ServerError { status, body })
    }
}

/// Save Parquet bytes to local staging on upload failure. The filename is
/// deterministic per chunk_time so repeated failures of the same chunk set
/// overwrite the same file. fsyncs the file and its directory before
/// returning — callers remove chunks / clean WAL only after this succeeds.
pub fn staging_save(
    staging_dir: &Path,
    db: &str,
    table: &str,
    chunk_time: i64,
    data: &[u8],
) -> Result<PathBuf, std::io::Error> {
    let dir = staging_dir.join(db).join(table);
    fs::create_dir_all(&dir)?;

    let path = dir.join(format!("{}.parquet", chunk_time));
    let mut f = fs::File::create(&path)?;
    std::io::Write::write_all(&mut f, data)?;
    f.sync_all()?;
    // fsync the directory so the file entry itself is durable
    if let Ok(d) = fs::File::open(&dir) {
        let _ = d.sync_all();
    }
    tracing::info!(path = %path.display(), bytes = data.len(), "Parquet saved to staging");
    Ok(path)
}

fn urlencoding(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

#[derive(Debug)]
pub enum UploadError {
    Http(String),
    ServerError { status: u16, body: String },
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UploadError::Http(e) => write!(f, "HTTP error: {}", e),
            UploadError::ServerError { status, body } => {
                write!(f, "server error {}: {}", status, body)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_SEQ: AtomicU32 = AtomicU32::new(0);

    fn test_staging_dir() -> PathBuf {
        let seq = TEST_SEQ.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("iedb_staging_test_{}_{}", std::process::id(), seq))
    }

    #[test]
    fn test_staging_save_creates_correct_directory_structure() {
        let tmp = test_staging_dir();
        let data = b"mock parquet binary data";

        let result = staging_save(&tmp, "metrics_db", "cpu_usage", 1700000000000000000, data);
        assert!(result.is_ok());

        let path = result.unwrap();

        // Directory structure: {dir}/{db}/{table}/{chunk_time}.parquet
        assert!(path.starts_with(&tmp.join("metrics_db").join("cpu_usage")));
        assert_eq!(path.extension().unwrap(), "parquet");

        // Verify file contents match what we wrote
        let read_back = fs::read(&path).unwrap();
        assert_eq!(read_back, data);

        // Verify directory exists
        let dir = tmp.join("metrics_db").join("cpu_usage");
        assert!(dir.is_dir());

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_staging_save_multiple_files() {
        let tmp = test_staging_dir();

        let data1 = b"first file";
        let data2 = b"second file data";

        let path1 = staging_save(&tmp, "mydb", "table_a", 1700000000000000000, data1).unwrap();
        let path2 = staging_save(&tmp, "mydb", "table_b", 1700000060000000000, data2).unwrap();

        assert!(path1.exists());
        assert!(path2.exists());
        assert_eq!(fs::read(&path1).unwrap(), data1);
        assert_eq!(fs::read(&path2).unwrap(), data2);

        // Different tables get different dirs
        assert!(path1.to_str().unwrap().contains("table_a"));
        assert!(path2.to_str().unwrap().contains("table_b"));

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_staging_save_same_chunk_time_overwrites_same_file() {
        let tmp = test_staging_dir();

        let path1 = staging_save(&tmp, "db", "tbl", 1700000000000000000, b"first attempt").unwrap();
        let path2 = staging_save(&tmp, "db", "tbl", 1700000000000000000, b"second attempt").unwrap();

        // Same chunk_time → same deterministic filename, later write overwrites
        assert_eq!(path1, path2);
        let dir = tmp.join("db").join("tbl");
        let files: Vec<_> = fs::read_dir(&dir).unwrap().collect();
        assert_eq!(files.len(), 1, "same chunk_time must not stack files");
        assert_eq!(fs::read(&path1).unwrap(), b"second attempt");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_staging_save_different_chunk_time_different_files() {
        let tmp = test_staging_dir();

        let path1 = staging_save(&tmp, "db", "tbl", 1700000000000000000, b"one").unwrap();
        let path2 = staging_save(&tmp, "db", "tbl", 1700000060000000000, b"two").unwrap();

        assert_ne!(path1, path2);
        assert_eq!(path1.file_name().unwrap(), "1700000000000000000.parquet");
        assert_eq!(path2.file_name().unwrap(), "1700000060000000000.parquet");

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_ingest_url_includes_chunk_time() {
        let url = build_ingest_url("http://server:8080", "mydb", "cpu", Some(1700000000000000000));
        assert!(url.contains("/api/v1/ingest/parquet?"));
        assert!(url.contains("db=mydb"));
        assert!(url.contains("measurement=cpu"));
        assert!(url.contains("chunk_time=1700000000000000000"));
    }

    #[test]
    fn test_ingest_url_without_chunk_time_for_backward_compat() {
        let url = build_ingest_url("http://server:8080/", "mydb", "cpu", None);
        assert!(!url.contains("chunk_time"));
        assert!(url.starts_with("http://server:8080/api/v1/ingest/parquet?"));
    }
}
