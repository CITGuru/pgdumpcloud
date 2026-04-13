use pgdumpcloud_core::introspect::{self, DatabaseInfo, SchemaInfo, TableInfo};

#[tauri::command]
pub async fn list_databases(url: String) -> Result<Vec<DatabaseInfo>, String> {
    introspect::list_databases(&url)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_schemas(url: String) -> Result<Vec<SchemaInfo>, String> {
    introspect::list_schemas(&url)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_tables(url: String, schema: String) -> Result<Vec<TableInfo>, String> {
    introspect::list_tables(&url, &schema)
        .await
        .map_err(|e| e.to_string())
}
