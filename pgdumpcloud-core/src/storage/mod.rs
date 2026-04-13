pub mod s3;

use crate::error::Result;
use crate::progress::ProgressSender;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupEntry {
    pub key: String,
    pub size: i64,
    pub last_modified: Option<String>,
}

#[async_trait::async_trait]
pub trait CloudStorage: Send + Sync {
    async fn upload(
        &self,
        local_path: &Path,
        remote_key: &str,
        progress: &dyn ProgressSender,
    ) -> Result<()>;

    async fn download(
        &self,
        remote_key: &str,
        local_path: &Path,
        progress: &dyn ProgressSender,
    ) -> Result<()>;

    async fn list(&self, prefix: &str) -> Result<Vec<BackupEntry>>;

    async fn delete(&self, remote_key: &str) -> Result<()>;

    async fn test_connection(&self) -> Result<()>;
}
