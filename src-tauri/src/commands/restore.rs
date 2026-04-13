use pgdumpcloud_core::compress;
use pgdumpcloud_core::progress::ProgressEvent;
use pgdumpcloud_core::restore::{self, RestoreOptions};
use pgdumpcloud_core::storage::s3::S3Storage;
use pgdumpcloud_core::storage::CloudStorage;
use serde::Deserialize;
use tauri::{Emitter, Window};

struct TauriProgressSender {
    window: Window,
}

impl pgdumpcloud_core::progress::ProgressSender for TauriProgressSender {
    fn send(&self, event: ProgressEvent) {
        let _ = self.window.emit("restore:progress", &event);
    }
}

#[derive(Deserialize)]
pub struct RestoreRequest {
    pub backup_key: String,
    pub target_url: String,
    pub clean: bool,
    pub no_owner: bool,
    pub no_acl: bool,
    #[serde(default)]
    pub data_only: bool,
    pub storage_endpoint: String,
    pub storage_bucket: String,
    pub storage_region: String,
    pub storage_access_key: String,
    pub storage_secret_key: String,
}

#[tauri::command]
pub async fn run_restore(window: Window, request: RestoreRequest) -> Result<String, String> {
    let progress = TauriProgressSender {
        window: window.clone(),
    };

    let s3 = S3Storage::new(
        &request.storage_endpoint,
        &request.storage_bucket,
        &request.storage_region,
        &request.storage_access_key,
        &request.storage_secret_key,
        "",
    );

    let local_path = std::env::temp_dir().join(&request.backup_key);

    s3.download(&request.backup_key, &local_path, &progress)
        .await
        .map_err(|e| e.to_string())?;

    let restore_path = if request.backup_key.ends_with(".gz") {
        let decompressed =
            compress::decompress_gzip(&local_path, &progress).map_err(|e| e.to_string())?;
        let _ = std::fs::remove_file(&local_path);
        decompressed
    } else {
        local_path.clone()
    };

    let opts = RestoreOptions {
        database_url: request.target_url,
        clean: request.clean,
        no_owner: request.no_owner,
        no_acl: request.no_acl,
        if_exists: request.clean,
        data_only: request.data_only,
    };

    restore::run_restore(&restore_path, &opts, &progress).map_err(|e| e.to_string())?;

    let _ = std::fs::remove_file(&restore_path);

    let _ = window.emit(
        "restore:progress",
        ProgressEvent::Finished {
            message: "Restore completed successfully".into(),
        },
    );

    Ok("Restore completed".into())
}
