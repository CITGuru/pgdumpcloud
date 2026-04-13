use crate::error::{PgDumpCloudError, Result};
use crate::progress::{Phase, ProgressEvent, ProgressSender};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DumpFormat {
    Custom,
    Plain,
    Tar,
}

impl DumpFormat {
    pub fn pg_flag(&self) -> &str {
        match self {
            DumpFormat::Custom => "c",
            DumpFormat::Plain => "p",
            DumpFormat::Tar => "t",
        }
    }

    pub fn extension(&self) -> &str {
        match self {
            DumpFormat::Custom => "dump",
            DumpFormat::Plain => "sql",
            DumpFormat::Tar => "tar",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "plain" | "sql" | "p" => DumpFormat::Plain,
            "tar" | "t" => DumpFormat::Tar,
            _ => DumpFormat::Custom,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DumpOptions {
    pub database_url: String,
    pub format: DumpFormat,
    pub schemas: Vec<String>,
    pub tables: Vec<String>,
    pub no_owner: bool,
    pub no_acl: bool,
    pub data_only: bool,
    pub schema_only: bool,
    pub clean: bool,
    pub if_exists: bool,
    pub verbose: bool,
    pub jobs: Option<u32>,
    pub output_dir: PathBuf,
    pub filename_prefix: String,
}

impl Default for DumpOptions {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            format: DumpFormat::Custom,
            schemas: Vec::new(),
            tables: Vec::new(),
            no_owner: true,
            no_acl: true,
            data_only: false,
            schema_only: false,
            clean: false,
            if_exists: false,
            verbose: false,
            jobs: None,
            output_dir: std::env::temp_dir(),
            filename_prefix: "backup".into(),
        }
    }
}

pub fn check_pg_dump() -> Result<String> {
    let output = Command::new("pg_dump").arg("--version").output();
    match output {
        Ok(o) if o.status.success() => {
            Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
        }
        _ => Err(PgDumpCloudError::BinaryNotFound("pg_dump".into())),
    }
}

/// Quotes a `schema.table` identifier for pg_dump's `-t` / `-n` flags so that
/// mixed-case names (e.g. Prisma's `"Workspace"`) are matched correctly.
/// Input `public.Workspace` becomes `"public"."Workspace"`.
/// A bare name without a dot (e.g. `public`) becomes `"public"`.
fn quote_pg_identifier(name: &str) -> String {
    if let Some((schema, table)) = name.split_once('.') {
        format!("\"{schema}\".\"{table}\"")
    } else {
        format!("\"{name}\"")
    }
}

pub fn generate_filename(prefix: &str, db_name: &str, format: &DumpFormat) -> String {
    let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    format!("{prefix}_{db_name}_{timestamp}.{ext}", ext = format.extension())
}

/// Derives the companion `.types.sql` path from a dump path or remote key.
/// Strips `.gz` and the format extension (`.dump`, `.sql`, `.tar`), then
/// appends `.types.sql`.
/// Example: `backup_mydb_20260412.dump.gz` -> `backup_mydb_20260412.types.sql`
pub fn types_sql_path(dump_path: &Path) -> PathBuf {
    let stem = dump_path.to_string_lossy();
    let s = stem.strip_suffix(".gz").unwrap_or(&stem);
    let s = s.strip_suffix(".dump")
        .or_else(|| s.strip_suffix(".sql"))
        .or_else(|| s.strip_suffix(".tar"))
        .unwrap_or(s);
    PathBuf::from(format!("{s}.types.sql"))
}

/// Same as `types_sql_path` but operates on a remote key string.
pub fn types_sql_key(remote_key: &str) -> String {
    let s = remote_key.strip_suffix(".gz").unwrap_or(remote_key);
    let s = s.strip_suffix(".dump")
        .or_else(|| s.strip_suffix(".sql"))
        .or_else(|| s.strip_suffix(".tar"))
        .unwrap_or(s);
    format!("{s}.types.sql")
}

pub fn run_dump(options: &DumpOptions, progress: &dyn ProgressSender) -> Result<PathBuf> {
    check_pg_dump()?;

    let db_name = crate::connection::parse_db_name(&options.database_url)
        .unwrap_or_else(|| "unknown".into());

    let filename = generate_filename(&options.filename_prefix, &db_name, &options.format);
    let output_path = options.output_dir.join(&filename);

    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Dumping,
    });

    let mut cmd = Command::new("pg_dump");
    cmd.arg(format!("--dbname={}", options.database_url));
    cmd.args(["-F", options.format.pg_flag()]);
    cmd.args(["-f", output_path.to_str().unwrap_or("")]);

    if options.no_owner {
        cmd.arg("--no-owner");
    }
    if options.no_acl {
        cmd.arg("--no-acl");
    }
    if options.data_only {
        cmd.arg("--data-only");
    }
    if options.schema_only {
        cmd.arg("--schema-only");
    }
    if options.clean {
        cmd.arg("--clean");
    }
    if options.if_exists {
        cmd.arg("--if-exists");
    }
    if options.verbose {
        cmd.arg("--verbose");
    }
    if let Some(j) = options.jobs {
        cmd.args(["-j", &j.to_string()]);
    }

    for schema in &options.schemas {
        cmd.args(["-n", &quote_pg_identifier(schema)]);
    }
    for table in &options.tables {
        cmd.args(["-t", &quote_pg_identifier(table)]);
    }

    let result = cmd.output()?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        progress.send(ProgressEvent::Error {
            message: stderr.to_string(),
        });
        return Err(PgDumpCloudError::Dump(stderr.to_string()));
    }

    progress.send(ProgressEvent::PhaseCompleted {
        phase: Phase::Dumping,
    });

    Ok(output_path)
}

/// Spawns `pg_dump` writing to a file on disk. Returns the child process and
/// the output path so the caller can poll the file size for progress while
/// pg_dump is still running.
pub fn spawn_dump_to_file(options: &DumpOptions, progress: &dyn ProgressSender) -> Result<(std::process::Child, PathBuf)> {
    check_pg_dump()?;

    let db_name = crate::connection::parse_db_name(&options.database_url)
        .unwrap_or_else(|| "unknown".into());

    let filename = generate_filename(&options.filename_prefix, &db_name, &options.format);
    let output_path = options.output_dir.join(&filename);

    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Dumping,
    });

    let mut cmd = Command::new("pg_dump");
    cmd.arg(format!("--dbname={}", options.database_url));
    cmd.args(["-F", options.format.pg_flag()]);
    cmd.args(["-f", output_path.to_str().unwrap_or("")]);

    if options.no_owner {
        cmd.arg("--no-owner");
    }
    if options.no_acl {
        cmd.arg("--no-acl");
    }
    if options.data_only {
        cmd.arg("--data-only");
    }
    if options.schema_only {
        cmd.arg("--schema-only");
    }
    if options.clean {
        cmd.arg("--clean");
    }
    if options.if_exists {
        cmd.arg("--if-exists");
    }
    if options.verbose {
        cmd.arg("--verbose");
    }
    if let Some(j) = options.jobs {
        cmd.args(["-j", &j.to_string()]);
    }

    for schema in &options.schemas {
        cmd.args(["-n", &quote_pg_identifier(schema)]);
    }
    for table in &options.tables {
        cmd.args(["-t", &quote_pg_identifier(table)]);
    }

    cmd.stderr(Stdio::piped());

    let child = cmd.spawn()?;
    Ok((child, output_path))
}

/// Spawns `pg_dump` with stdout piped (no local file). Returns the child
/// process whose stdout can be streamed directly into an upload.
/// The caller is responsible for waiting on the child and checking its exit status.
///
/// NOTE: Directory format (`-Fd`) is not supported here because it writes
/// to a directory, not stdout. The `-j` (parallel jobs) flag also requires
/// directory format and is therefore ignored in streaming mode.
pub fn spawn_dump_stream(options: &DumpOptions, progress: &dyn ProgressSender) -> Result<std::process::Child> {
    check_pg_dump()?;

    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Dumping,
    });

    let mut cmd = Command::new("pg_dump");
    cmd.arg(format!("--dbname={}", options.database_url));
    cmd.args(["-F", options.format.pg_flag()]);
    // No -f flag: output goes to stdout for streaming

    if options.no_owner {
        cmd.arg("--no-owner");
    }
    if options.no_acl {
        cmd.arg("--no-acl");
    }
    if options.data_only {
        cmd.arg("--data-only");
    }
    if options.schema_only {
        cmd.arg("--schema-only");
    }
    if options.clean {
        cmd.arg("--clean");
    }
    if options.if_exists {
        cmd.arg("--if-exists");
    }
    if options.verbose {
        cmd.arg("--verbose");
    }

    for schema in &options.schemas {
        cmd.args(["-n", &quote_pg_identifier(schema)]);
    }
    for table in &options.tables {
        cmd.args(["-t", &quote_pg_identifier(table)]);
    }

    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let child = cmd.spawn()?;
    Ok(child)
}
