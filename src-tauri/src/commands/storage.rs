use crate::state::AppState;
use pgdumpcloud_core::config::StorageConfig;
use pgdumpcloud_core::storage::s3::S3Storage;
use pgdumpcloud_core::storage::CloudStorage;
use tauri::State;

#[tauri::command]
pub fn list_storage_targets(state: State<'_, AppState>) -> Result<Vec<StorageConfig>, String> {
    let config = state.config.lock().map_err(|e| e.to_string())?;
    Ok(config.storage.clone())
}

#[tauri::command]
pub fn add_storage_target(
    state: State<'_, AppState>,
    target: StorageConfig,
) -> Result<(), String> {
    {
        let mut config = state.config.lock().map_err(|e| e.to_string())?;
        config.storage.retain(|s| s.id != target.id);
        config.storage.push(target);
    }
    state.save()
}

#[tauri::command]
pub fn delete_storage_target(state: State<'_, AppState>, id: String) -> Result<(), String> {
    {
        let mut config = state.config.lock().map_err(|e| e.to_string())?;
        config.storage.retain(|s| s.id != id);
    }
    state.save()
}

#[tauri::command]
pub async fn test_storage_cmd(
    endpoint: String,
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
) -> Result<String, String> {
    let s3 = S3Storage::new(&endpoint, &bucket, &region, &access_key, &secret_key, "");
    s3.test_connection()
        .await
        .map_err(|e| e.to_string())?;
    Ok("Connection successful".into())
}
