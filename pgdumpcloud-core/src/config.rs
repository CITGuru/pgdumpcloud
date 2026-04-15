use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub defaults: DefaultsConfig,
    #[serde(default)]
    pub connections: Vec<ConnectionConfig>,
    #[serde(default)]
    pub storage: Vec<StorageConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultsConfig {
    pub dump_format: String,
    pub compression: String,
    pub no_owner: bool,
    pub no_acl: bool,
    pub keep_local: bool,
    pub retention: u32,
    pub filename_prefix: String,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            dump_format: "custom".into(),
            compression: "gzip".into(),
            no_owner: true,
            no_acl: true,
            keep_local: false,
            retention: 7,
            filename_prefix: "backup".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub ssl_mode: Option<String>,
}

fn default_port() -> u16 {
    5432
}

impl ConnectionConfig {
    pub fn build_url(&self) -> String {
        let password_part = if self.password.is_empty() {
            String::new()
        } else {
            format!(":{}", urlencoding::encode(&self.password))
        };

        let user_part = if self.username.is_empty() {
            String::new()
        } else {
            format!("{}{password_part}@", urlencoding::encode(&self.username))
        };

        let host = if self.host.is_empty() {
            "localhost"
        } else {
            &self.host
        };
        let db = if self.database.is_empty() {
            "postgres"
        } else {
            &self.database
        };

        format!("postgres://{user_part}{host}:{}/{db}", self.port)
    }

    pub fn build_url_for_db(&self, db_name: &str) -> String {
        let password_part = if self.password.is_empty() {
            String::new()
        } else {
            format!(":{}", urlencoding::encode(&self.password))
        };

        let user_part = if self.username.is_empty() {
            String::new()
        } else {
            format!("{}{password_part}@", urlencoding::encode(&self.username))
        };

        let host = if self.host.is_empty() {
            "localhost"
        } else {
            &self.host
        };

        format!("postgres://{user_part}{host}:{}/{db_name}", self.port)
    }

    pub fn from_url(url: &str) -> Self {
        let mut config = Self {
            id: String::new(),
            name: String::new(),
            host: "localhost".into(),
            port: 5432,
            username: String::new(),
            password: String::new(),
            database: "postgres".into(),
            ssl_mode: None,
        };

        if let Ok(parsed) = url::Url::parse(url) {
            config.host = parsed.host_str().unwrap_or("localhost").to_string();
            config.port = parsed.port().unwrap_or(5432);
            config.username = urlencoding::decode(parsed.username())
                .unwrap_or_default()
                .into_owned();
            config.password = parsed
                .password()
                .map(|p| urlencoding::decode(p).unwrap_or_default().into_owned())
                .unwrap_or_default();
            let path = parsed.path().trim_start_matches('/');
            if !path.is_empty() {
                config.database = path.to_string();
            }
        }

        config
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub id: String,
    pub name: String,
    pub provider: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub remote: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
}

impl AppConfig {
    pub fn load(path: &std::path::Path) -> crate::error::Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)
                .map_err(|e| crate::error::PgDumpCloudError::Config(e.to_string()))?;
            toml::from_str(&content)
                .map_err(|e| crate::error::PgDumpCloudError::Config(e.to_string()))
        } else {
            Ok(Self::default())
        }
    }

    pub fn save(&self, path: &std::path::Path) -> crate::error::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)
            .map_err(|e| crate::error::PgDumpCloudError::Config(e.to_string()))?;
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn default_config_path() -> std::path::PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join("pgdumpcloud")
            .join("config.toml")
    }

    pub fn find_connection(&self, id_or_name: &str) -> Option<&ConnectionConfig> {
        self.connections
            .iter()
            .find(|c| c.id == id_or_name || c.name == id_or_name)
    }

    pub fn find_storage(&self, id_or_name: &str) -> Option<&StorageConfig> {
        self.storage
            .iter()
            .find(|s| s.id == id_or_name || s.name == id_or_name)
    }
}
