use crate::config::S3Config;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use reqwest::Client;
use std::time::SystemTime;

/// Upload Parquet bytes to S3 using SigV4 signing.
pub async fn upload_to_s3(
    client: &Client,
    config: &S3Config,
    key: &str,
    data: &[u8],
) -> Result<(), String> {
    let host = format!("{}.s3.{}.amazonaws.com", config.bucket, config.region);
    let uri = format!("https://{}/{}", host, key);

    // Set up credentials and identity for signing
    let creds = Credentials::new(
        &config.access_key,
        &config.secret_key,
        None,
        None,
        "iedb-agent",
    );
    let identity: Identity = Identity::new(creds, None);

    // Build signing params
    let settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&config.region)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|e| format!("Build signing params: {e}"))?
        .into();

    // Create signable request
    let signable = SignableRequest::new(
        "PUT",
        &uri,
        std::iter::once(("host", host.as_str()))
            .chain(std::iter::once(("content-type", "application/octet-stream"))),
        SignableBody::Bytes(data),
    )
    .map_err(|e| format!("Build signable request: {e}"))?;

    // Sign
    let (signing_instructions, _signature) = sign(signable, &signing_params)
        .map_err(|e| format!("Sign: {e}"))?
        .into_parts();

    // Build the actual HTTP request and apply signing instructions
    let mut http_req = http::Request::builder()
        .method("PUT")
        .uri(&uri)
        .header("host", &host)
        .header("content-type", "application/octet-stream")
        .body(data.to_vec())
        .map_err(|e| format!("Build request: {e}"))?;

    signing_instructions.apply_to_request_http1x(&mut http_req);

    // Execute the signed request
    let reqwest_req = reqwest::Request::try_from(http_req)
        .map_err(|e| format!("Convert to reqwest: {e}"))?;

    let resp = client
        .execute(reqwest_req)
        .await
        .map_err(|e| format!("S3 PUT: {e}"))?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!(
            "S3 error {}: {}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        ))
    }
}

/// Build S3 object key from db/table/timestamp.
pub fn s3_key(agent_id: &str, db: &str, table: &str, ts_nanos: i64) -> String {
    let dt = chrono::DateTime::from_timestamp_nanos(ts_nanos);
    let year = dt.format("%Y");
    let month = dt.format("%m");
    let day = dt.format("%d");
    let hour = dt.format("%H");
    let ts_str = dt.format("%Y%m%d_%H%M%S");
    let nanos = ts_nanos % 1_000_000_000;

    format!(
        "{}/{}/{}/{}/{}/{}/{}_{}_{:09}.parquet",
        db, table, year, month, day, hour, agent_id, ts_str, nanos
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_key_generates_correct_path_format() {
        // 2024-01-01 00:00:00.000000000 UTC = 1704067200 seconds since epoch
        // So in nanos: 1704067200 * 1_000_000_000 = 1_704_067_200_000_000_000
        let ts_nanos: i64 = 1_704_067_200_000_000_000i64;
        let key = s3_key("agent-01", "mydb", "cpu", ts_nanos);

        let expected = "mydb/cpu/2024/01/01/00/agent-01_20240101_000000_000000000.parquet";
        assert_eq!(key, expected);
    }

    #[test]
    fn test_s3_key_verify_path_components_from_known_timestamp() {
        // 2025-12-31 23:59:59.999999999 UTC = a known timestamp
        // Let's compute it: 2025-12-31 23:59:59 UTC
        let dt = chrono::NaiveDate::from_ymd_opt(2025, 12, 31)
            .unwrap()
            .and_hms_nano_opt(23, 59, 59, 999999999)
            .unwrap();
        let ts_nanos = dt.and_utc().timestamp_nanos_opt().unwrap();

        let key = s3_key("agent-x", "testdb", "temperature", ts_nanos);

        // Verify it starts with db/table and ends with .parquet
        assert!(key.starts_with("testdb/temperature/"), "unexpected key: {key}");

        // Verify the path contains the expected date/time parts
        let parts: Vec<&str> = key.split('/').collect();
        // Format: db/table/year/month/day/hour/agent_ts_nanos.parquet = 7 components
        assert_eq!(parts.len(), 7, "expected 7 path components, got {}: {parts:?}", parts.len());
        // parts[0] = "testdb", parts[1] = "temperature", parts[2] = "2025"
        // parts[3] = "12", parts[4] = "31", parts[5] = "23"
        assert_eq!(parts[2], "2025", "year should be 2025");
        assert_eq!(parts[3], "12", "month should be 12");
        assert_eq!(parts[4], "31", "day should be 31");
        assert_eq!(parts[5], "23", "hour should be 23");

        // The last segment should contain agent-x and end with .parquet
        let last_part = parts[6];
        assert!(last_part.starts_with("agent-x_"), "last part = {last_part}");
        assert!(last_part.ends_with(".parquet"));
    }

    #[test]
    fn test_s3_key_different_agent_ids_produce_different_paths() {
        let ts_nanos: i64 = 1_704_067_200_000_000_000i64; // 2024-01-01 00:00:00

        let key1 = s3_key("agent-alpha", "db1", "tbl1", ts_nanos);
        let key2 = s3_key("agent-beta", "db1", "tbl1", ts_nanos);

        assert_ne!(key1, key2);
        assert!(key1.contains("agent-alpha"));
        assert!(key2.contains("agent-beta"));

        // All other components should be the same
        let key1_no_agent = key1.replace("agent-alpha", "AGENT");
        let key2_no_agent = key2.replace("agent-beta", "AGENT");
        assert_eq!(key1_no_agent, key2_no_agent);
    }

    #[test]
    fn test_s3_key_different_timestamps_produce_different_paths() {
        let ts1: i64 = 1_704_067_200_000_000_000i64; // 2024-01-01
        let ts2: i64 = 1_735_689_600_000_000_000i64; // 2025-01-01 (approx)

        let key1 = s3_key("agent-01", "db1", "tbl1", ts1);
        let key2 = s3_key("agent-01", "db1", "tbl1", ts2);

        assert_ne!(key1, key2);
        assert!(key1.contains("2024"));
        assert!(key2.contains("2025"));
    }
}
