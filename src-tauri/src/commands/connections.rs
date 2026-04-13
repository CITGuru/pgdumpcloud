use crate::state::AppState;
use pgdumpcloud_core::config::ConnectionConfig;
use pgdumpcloud_core::connection::{self, ConnectionInfo};
use serde::Serialize;
use tauri::State;

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> Result<Vec<ConnectionConfig>, String> {
    let config = state.config.lock().map_err(|e| e.to_string())?;
    Ok(config.connections.clone())
}

#[tauri::command]
pub fn add_connection(
    state: State<'_, AppState>,
    connection: ConnectionConfig,
) -> Result<(), String> {
    {
        let mut config = state.config.lock().map_err(|e| e.to_string())?;
        config.connections.retain(|c| c.id != connection.id);
        config.connections.push(connection);
    }
    state.save()
}

#[tauri::command]
pub fn update_connection(
    state: State<'_, AppState>,
    connection: ConnectionConfig,
) -> Result<(), String> {
    {
        let mut config = state.config.lock().map_err(|e| e.to_string())?;
        if let Some(existing) = config.connections.iter_mut().find(|c| c.id == connection.id) {
            *existing = connection;
        } else {
            return Err(format!("Connection '{}' not found", connection.id));
        }
    }
    state.save()
}

#[tauri::command]
pub fn delete_connection(state: State<'_, AppState>, id: String) -> Result<(), String> {
    {
        let mut config = state.config.lock().map_err(|e| e.to_string())?;
        config.connections.retain(|c| c.id != id);
    }
    state.save()
}

#[tauri::command]
pub async fn test_connection_cmd(url: String) -> Result<ConnectionInfo, String> {
    connection::test_connection(&url)
        .await
        .map_err(|e| e.to_string())
}

#[derive(Serialize)]
pub struct ParsedConnection {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

#[tauri::command]
pub fn parse_connection_url(url: String) -> ParsedConnection {
    let parsed = ConnectionConfig::from_url(&url);
    ParsedConnection {
        host: parsed.host,
        port: parsed.port,
        username: parsed.username,
        password: parsed.password,
        database: parsed.database,
    }
}

#[tauri::command]
pub fn build_connection_url_for_db(
    state: State<'_, AppState>,
    connection_id: String,
    database: String,
) -> Result<String, String> {
    let config = state.config.lock().map_err(|e| e.to_string())?;
    let conn = config
        .connections
        .iter()
        .find(|c| c.id == connection_id)
        .ok_or_else(|| format!("Connection '{}' not found", connection_id))?;
    Ok(conn.build_url_for_db(&database))
}
