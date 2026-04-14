use thiserror::Error;

#[derive(Error, Debug)]
pub enum PgDumpCloudError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Dump failed: {0}")]
    Dump(String),

    #[error("Restore failed: {0}")]
    Restore(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Binary not found: {0} — install PostgreSQL client tools")]
    BinaryNotFound(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Parquet export error: {0}")]
    ParquetExport(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, PgDumpCloudError>;
