use pgdumpcloud_core::compress;
use pgdumpcloud_core::dump::{self, DumpFormat, DumpOptions};
use pgdumpcloud_core::progress::ProgressEvent;
use pgdumpcloud_core::storage::s3::S3Storage;
use pgdumpcloud_core::storage::{BackupEntry, CloudStorage};
use serde::{Deserialize, Serialize};
use tauri::{Emitter, Window};

struct TauriProgressSender {
    window: Window,
}

impl pgdumpcloud_core::progress::ProgressSender for TauriProgressSender {
    fn send(&self, event: ProgressEvent) {
        let _ = self.window.emit("backup:progress", &event);
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "kind")]
pub enum HivePartitioning {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "year")]
    Year { column: String },
    #[serde(rename = "year_month")]
    YearMonth { column: String },
}

impl Default for HivePartitioning {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ParquetOptions {
    pub storage_mode: String,
    pub max_rows_per_file: Option<u64>,
    pub hive_partitioning: HivePartitioning,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            storage_mode: "archive".into(),
            max_rows_per_file: None,
            hive_partitioning: HivePartitioning::None,
        }
    }
}

#[derive(Deserialize)]
pub struct BackupRequest {
    pub connection_url: String,
    pub format: String,
    pub compression: String,
    pub schemas: Vec<String>,
    pub tables: Vec<String>,
    pub no_owner: bool,
    pub no_acl: bool,
    pub storage_endpoint: String,
    pub storage_bucket: String,
    pub storage_region: String,
    pub storage_access_key: String,
    pub storage_secret_key: String,
    pub storage_prefix: String,
    pub filename_prefix: String,
    pub retention: u32,
    pub keep_local: bool,
    #[serde(default)]
    #[allow(dead_code)]
    pub streaming: bool,
    #[serde(default)]
    pub parquet_options: Option<ParquetOptions>,
}

#[tauri::command]
pub async fn run_backup(window: Window, request: BackupRequest) -> Result<String, String> {
    let progress = TauriProgressSender {
        window: window.clone(),
    };

    let dump_format = DumpFormat::from_str(&request.format);

    let opts = DumpOptions {
        database_url: request.connection_url,
        format: dump_format,
        schemas: request.schemas,
        tables: request.tables,
        no_owner: request.no_owner,
        no_acl: request.no_acl,
        output_dir: std::env::temp_dir(),
        filename_prefix: request.filename_prefix,
        ..Default::default()
    };

    let dump_path = dump::run_dump(&opts, &progress).map_err(|e| e.to_string())?;

    let upload_path = if request.compression.starts_with("gzip") {
        let level = compress::compression_level(
            request.compression.strip_prefix("gzip-").unwrap_or("default"),
        );
        let compressed = compress::compress_gzip(&dump_path, level, &progress).map_err(|e| e.to_string())?;
        let _ = std::fs::remove_file(&dump_path);
        compressed
    } else {
        dump_path.clone()
    };

    let remote_key = upload_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("backup")
        .to_string();

    let s3 = S3Storage::new(
        &request.storage_endpoint,
        &request.storage_bucket,
        &request.storage_region,
        &request.storage_access_key,
        &request.storage_secret_key,
        &request.storage_prefix,
    );

    s3.upload(&upload_path, &remote_key, &progress)
        .await
        .map_err(|e| e.to_string())?;

    if request.retention > 0 {
        if let Ok(entries) = s3.list("").await {
            if entries.len() > request.retention as usize {
                for old in &entries[request.retention as usize..] {
                    let _ = s3.delete(&old.key).await;
                }
            }
        }
    }

    if !request.keep_local {
        let _ = std::fs::remove_file(&upload_path);
    }

    let _ = window.emit(
        "backup:progress",
        ProgressEvent::Finished {
            message: format!("Backup uploaded: {remote_key}"),
        },
    );

    Ok(remote_key)
}

#[tauri::command]
pub async fn list_backups(
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    prefix: String,
) -> Result<Vec<BackupEntry>, String> {
    let s3 = S3Storage::new(&endpoint, &bucket, &region, &access_key, &secret_key, &prefix);
    s3.list("").await.map_err(|e| e.to_string())
}
