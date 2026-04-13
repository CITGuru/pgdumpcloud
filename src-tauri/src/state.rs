use pgdumpcloud_core::config::AppConfig;
use std::path::PathBuf;
use std::sync::Mutex;

pub struct AppState {
    pub config: Mutex<AppConfig>,
    pub config_path: PathBuf,
}

impl AppState {
    pub fn load(config_path: PathBuf) -> Self {
        let config = AppConfig::load(&config_path).unwrap_or_default();
        Self {
            config: Mutex::new(config),
            config_path,
        }
    }

    pub fn save(&self) -> Result<(), String> {
        let config = self.config.lock().map_err(|e| e.to_string())?;
        config.save(&self.config_path).map_err(|e| e.to_string())
    }
}
