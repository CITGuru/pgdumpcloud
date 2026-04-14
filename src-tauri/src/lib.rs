mod commands;
mod jobs;
mod state;

use std::sync::Arc;

use jobs::JobManager;
use state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let config_path = pgdumpcloud_core::config::AppConfig::default_config_path();
    let app_state = AppState::load(config_path.clone());

    let db_dir = config_path.parent().unwrap_or(std::path::Path::new("."));
    let job_manager = Arc::new(JobManager::new(db_dir.join("history.db")));

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(app_state)
        .manage(job_manager)
        .invoke_handler(tauri::generate_handler![
            commands::connections::list_connections,
            commands::connections::add_connection,
            commands::connections::update_connection,
            commands::connections::delete_connection,
            commands::connections::test_connection_cmd,
            commands::connections::parse_connection_url,
            commands::connections::build_connection_url_for_db,
            commands::storage::list_storage_targets,
            commands::storage::add_storage_target,
            commands::storage::delete_storage_target,
            commands::storage::test_storage_cmd,
            commands::introspect::list_databases,
            commands::introspect::list_schemas,
            commands::introspect::list_tables,
            commands::introspect::list_datetime_columns,
            commands::backup::run_backup,
            commands::backup::list_backups,
            commands::restore::run_restore,
            commands::jobs::create_backup_job,
            commands::jobs::create_restore_job,
            commands::jobs::list_jobs,
            commands::jobs::get_job,
            commands::jobs::get_job_logs,
            commands::jobs::cancel_job,
            commands::jobs::delete_job,
            commands::jobs::retry_job,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
