use reqwest::Client;
use std::path::{Path, PathBuf};
use std::fs;
use tracing;

/// Upload Parquet bytes to iotededb via HTTP.
pub async fn upload_parquet(
    client: &Client,
    iotededb_url: &str,
    db: &str,
    table: &str,
    data: &[u8],
    auth_header: Option<&str>,
) -> Result<(), UploadError> {
    let url = format!(
        "{}/api/v1/ingest/parquet?db={}&measurement={}",
        iotededb_url.trim_end_matches('/'),
        urlencoding(db),
        urlencoding(table)
    );

    let mut req = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
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

/// Save Parquet bytes to local staging on upload failure.
pub fn staging_save(
    staging_dir: &Path,
    db: &str,
    table: &str,
    data: &[u8],
) -> Result<PathBuf, std::io::Error> {
    let dir = staging_dir.join(db).join(table);
    fs::create_dir_all(&dir)?;

    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S_%f");
    let path = dir.join(format!("{}.parquet", ts));
    fs::write(&path, data)?;
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

        let result = staging_save(&tmp, "metrics_db", "cpu_usage", data);
        assert!(result.is_ok());

        let path = result.unwrap();

        // Directory structure: {dir}/{db}/{table}/{timestamp}.parquet
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

        let path1 = staging_save(&tmp, "mydb", "table_a", data1).unwrap();
        let path2 = staging_save(&tmp, "mydb", "table_b", data2).unwrap();

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
    fn test_staging_save_timestamp_is_unique() {
        let tmp = test_staging_dir();

        let data = b"some data";
        let path1 = staging_save(&tmp, "db", "tbl", data).unwrap();
        // Small sleep to get a different timestamp
        std::thread::sleep(std::time::Duration::from_millis(2));
        let path2 = staging_save(&tmp, "db", "tbl", data).unwrap();

        assert_ne!(path1, path2);
        assert!(path1.exists());
        assert!(path2.exists());

        let _ = fs::remove_dir_all(&tmp);
    }
}
