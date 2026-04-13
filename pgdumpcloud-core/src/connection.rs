use crate::error::{PgDumpCloudError, Result};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub pg_version: String,
    pub latency_ms: u64,
}

pub async fn test_connection(url: &str) -> Result<ConnectionInfo> {
    let start = Instant::now();

    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PG connection error: {e}");
        }
    });

    let row = client
        .query_one("SELECT version()", &[])
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    let version: String = row.get(0);
    let latency = start.elapsed().as_millis() as u64;

    Ok(ConnectionInfo {
        pg_version: version,
        latency_ms: latency,
    })
}

pub fn parse_db_name(url: &str) -> Option<String> {
    url.rsplit('/').next().map(|s| {
        s.split('?').next().unwrap_or(s).to_string()
    })
}
