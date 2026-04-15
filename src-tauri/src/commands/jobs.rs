use std::sync::Arc;

use pgdumpcloud_core::compress;
use pgdumpcloud_core::dump::{self, DumpFormat, DumpOptions};
use pgdumpcloud_core::introspect;
use pgdumpcloud_core::parquet_export::{self, FetchStrategy, ParquetExportOptions, StorageMode, HivePartitioning as CoreHivePartitioning};
use pgdumpcloud_core::progress::{Phase, ProgressEvent, ProgressSender, ThrottledProgressSender};
use pgdumpcloud_core::restore::{self, RestoreOptions};
use pgdumpcloud_core::storage::s3::S3Storage;
use pgdumpcloud_core::storage::CloudStorage;
use serde::Deserialize;
use tauri::{AppHandle, Emitter, State};
use tokio_util::sync::CancellationToken;

use super::backup::ParquetOptions;
use crate::jobs::{
    Job, JobKind, JobManager, JobProgressSender, JobStatus, JobStatusEvent, JobSummary, LogEntry,
};

#[derive(Deserialize, serde::Serialize)]
pub struct BackupJobRequest {
    pub connection_url: String,
    pub format: String,
    pub compression: String,
    pub schemas: Vec<String>,
    pub tables: Vec<String>,
    pub no_owner: bool,
    pub no_acl: bool,
    pub storage_endpoint: String,
    pub storage_bucket: String,
    pub storage_region: String,
    pub storage_access_key: String,
    pub storage_secret_key: String,
    pub storage_prefix: String,
    pub filename_prefix: String,
    pub retention: u32,
    pub keep_local: bool,
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub parquet_options: Option<ParquetOptions>,
}

#[derive(Deserialize, serde::Serialize)]
pub struct RestoreJobRequest {
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
pub async fn create_backup_job(
    app: AppHandle,
    manager: State<'_, Arc<JobManager>>,
    request: BackupJobRequest,
) -> Result<String, String> {
    let request_json = serde_json::to_value(&request).map_err(|e| e.to_string())?;
    let job_id = manager.create_job(JobKind::Backup, request_json);
    let cancel_token = manager.create_cancel_token(&job_id);

    let mgr = Arc::clone(&manager.inner());
    let app_handle = app.clone();
    let jid = job_id.clone();

    tokio::spawn(async move {
        run_backup_task(jid, request, mgr, app_handle, cancel_token).await;
    });

    Ok(job_id)
}

async fn run_backup_task(
    job_id: String,
    request: BackupJobRequest,
    manager: Arc<JobManager>,
    app: AppHandle,
    cancel: CancellationToken,
) {
    manager.update_status(&job_id, JobStatus::Running);
    let _ = app.emit(
        "job:status_changed",
        JobStatusEvent {
            job_id: job_id.clone(),
            status: JobStatus::Running,
        },
    );

    if request.format == "parquet" {
        run_parquet_backup(job_id.clone(), request, manager.clone(), app.clone(), cancel).await;
    } else if request.streaming {
        run_streaming_backup(job_id.clone(), request, manager.clone(), app.clone(), cancel).await;
    } else {
        run_local_backup(job_id.clone(), request, manager.clone(), app.clone(), cancel).await;
    }

    manager.remove_cancel_token(&job_id);
}

async fn run_local_backup(
    job_id: String,
    request: BackupJobRequest,
    manager: Arc<JobManager>,
    app: AppHandle,
    cancel: CancellationToken,
) {
    let raw_progress = JobProgressSender {
        job_id: job_id.clone(),
        app_handle: app.clone(),
        manager: Arc::clone(&manager),
    };

    let dump_format = DumpFormat::from_str(&request.format);
    let db_url = request.connection_url.clone();
    let schemas = request.schemas.clone();

    let opts = DumpOptions {
        database_url: request.connection_url,
        format: dump_format,
        schemas: request.schemas,
        tables: request.tables,
        no_owner: request.no_owner,
        no_acl: request.no_acl,
        output_dir: std::env::temp_dir(),
        filename_prefix: request.filename_prefix,
        ..Default::default()
    };

    if cancel.is_cancelled() { return; }

    // Estimate total size for dump progress
    let estimated_total = estimate_db_size(&db_url).await;

    let (mut child, dump_path) = match dump::spawn_dump_to_file(&opts, &raw_progress) {
        Ok(pair) => pair,
        Err(e) => {
            fail_job(&manager, &app, &job_id, e.to_string());
            return;
        }
    };

    // Capture stderr concurrently
    let stderr_handle = {
        let stderr = child.stderr.take();
        tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            if let Some(mut s) = stderr {
                let _ = std::io::Read::read_to_end(&mut s, &mut buf);
            }
            buf
        })
    };

    // Poll file size for progress while pg_dump runs
    let poll_path = dump_path.clone();
    let poll_cancel = cancel.clone();
    let poll_progress = JobProgressSender {
        job_id: job_id.clone(),
        app_handle: app.clone(),
        manager: Arc::clone(&manager),
    };
    let poll_handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            if poll_cancel.is_cancelled() {
                break;
            }
            if let Ok(meta) = tokio::fs::metadata(&poll_path).await {
                poll_progress.send(ProgressEvent::Progress {
                    phase: Phase::Dumping,
                    bytes: meta.len(),
                    total: estimated_total,
                });
            }
        }
    });

    // Wait for pg_dump to finish
    let exit_status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap_or_else(|e| Err(std::io::Error::new(std::io::ErrorKind::Other, e)));

    poll_handle.abort();

    let stderr_bytes = stderr_handle.await.unwrap_or_default();

    if cancel.is_cancelled() {
        let _ = std::fs::remove_file(&dump_path);
        return;
    }

    match &exit_status {
        Ok(status) if !status.success() => {
            let stderr_str = String::from_utf8_lossy(&stderr_bytes);
            let msg = if stderr_str.trim().is_empty() {
                format!("pg_dump failed with exit code: {status}")
            } else {
                format!("pg_dump failed: {}", stderr_str.trim())
            };
            let _ = std::fs::remove_file(&dump_path);
            fail_job(&manager, &app, &job_id, msg);
            return;
        }
        Err(e) => {
            let _ = std::fs::remove_file(&dump_path);
            fail_job(&manager, &app, &job_id, format!("pg_dump wait error: {e}"));
            return;
        }
        _ => {}
    }

    raw_progress.send(ProgressEvent::PhaseCompleted {
        phase: Phase::Dumping,
    });

    let dump_size = std::fs::metadata(&dump_path).map(|m| m.len()).ok();
    let progress = Arc::new(ThrottledProgressSender::new(raw_progress, dump_size));

    let upload_path = if request.compression.starts_with("gzip") {
        let level = compress::compression_level(
            request.compression.strip_prefix("gzip-").unwrap_or("default"),
        );
        let compress_dump = dump_path.clone();
        let compress_progress = Arc::clone(&progress);
        let compress_result = tokio::task::spawn_blocking(move || {
            compress::compress_gzip(&compress_dump, level, compress_progress.as_ref())
        })
        .await
        .unwrap_or_else(|e| Err(pgdumpcloud_core::error::PgDumpCloudError::Other(e.to_string())));

        match compress_result {
            Ok(compressed) => {
                let _ = std::fs::remove_file(&dump_path);
                compressed
            }
            Err(e) => {
                fail_job(&manager, &app, &job_id, e.to_string());
                return;
            }
        }
    } else {
        dump_path.clone()
    };

    if cancel.is_cancelled() {
        let _ = std::fs::remove_file(&upload_path);
        return;
    }

    let remote_key = upload_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("backup")
        .to_string();

    let s3 = S3Storage::new(
        &request.storage_endpoint,
        &request.storage_bucket,
        &request.storage_region,
        &request.storage_access_key,
        &request.storage_secret_key,
        &request.storage_prefix,
    );

    if let Err(e) = s3.upload(&upload_path, &remote_key, progress.as_ref()).await {
        fail_job(&manager, &app, &job_id, e.to_string());
        return;
    }

    if cancel.is_cancelled() {
        if !request.keep_local {
            let _ = std::fs::remove_file(&upload_path);
        }
        return;
    }

    extract_and_upload_types(&db_url, &schemas, &remote_key, &s3, progress.as_ref()).await;

    if request.retention > 0 {
        if let Ok(entries) = s3.list("").await {
            if entries.len() > request.retention as usize {
                for old in &entries[request.retention as usize..] {
                    let _ = s3.delete(&old.key).await;
                }
            }
        }
    }

    if !request.keep_local {
        let _ = std::fs::remove_file(&upload_path);
    }

    if !complete_job(&manager, &app, &job_id, remote_key.clone(), progress.as_ref()) {
        return;
    }
}

async fn run_parquet_backup(
    job_id: String,
    request: BackupJobRequest,
    manager: Arc<JobManager>,
    app: AppHandle,
    cancel: CancellationToken,
) {
    let raw_progress = JobProgressSender {
        job_id: job_id.clone(),
        app_handle: app.clone(),
        manager: Arc::clone(&manager),
    };
    let progress = Arc::new(ThrottledProgressSender::new(raw_progress, None));

    let parquet_opts = request.parquet_options.unwrap_or_default();

    let storage_mode = match parquet_opts.storage_mode.as_str() {
        "individual" => StorageMode::Individual,
        _ => StorageMode::Archive,
    };

    let hive = match parquet_opts.hive_partitioning {
        super::backup::HivePartitioning::Year { column } => CoreHivePartitioning::Year { column },
        super::backup::HivePartitioning::YearMonth { column } => CoreHivePartitioning::YearMonth { column },
        super::backup::HivePartitioning::None => CoreHivePartitioning::None,
    };

    let fetch_strategy = match parquet_opts.fetch_strategy.as_str() {
        "copy" => FetchStrategy::Copy,
        _ => FetchStrategy::Cursor,
    };

    let export_opts = ParquetExportOptions {
        database_url: request.connection_url,
        schemas: request.schemas,
        tables: request.tables,
        output_dir: std::env::temp_dir(),
        filename_prefix: request.filename_prefix,
        max_rows_per_file: parquet_opts.max_rows_per_file,
        hive_partitioning: hive,
        storage_mode,
        fetch_strategy,
    };

    if cancel.is_cancelled() { return; }

    let result = match parquet_export::run_parquet_export(&export_opts, progress.as_ref()).await {
        Ok(r) => r,
        Err(e) => {
            fail_job(&manager, &app, &job_id, e.to_string());
            return;
        }
    };

    if cancel.is_cancelled() { return; }

    let s3 = S3Storage::new(
        &request.storage_endpoint,
        &request.storage_bucket,
        &request.storage_region,
        &request.storage_access_key,
        &request.storage_secret_key,
        &request.storage_prefix,
    );

    let remote_key = match result.mode {
        StorageMode::Archive => {
            let archive_path = match &result.archive_path {
                Some(p) => p,
                None => {
                    fail_job(&manager, &app, &job_id, "No archive file produced".into());
                    return;
                }
            };

            let remote_key = archive_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("export.tar.gz")
                .to_string();

            if let Err(e) = s3.upload(archive_path, &remote_key, progress.as_ref()).await {
                fail_job(&manager, &app, &job_id, e.to_string());
                return;
            }

            if !request.keep_local {
                let _ = std::fs::remove_file(archive_path);
            }

            remote_key
        }
        StorageMode::Individual => {
            let mut last_key = String::new();
            for file_path in &result.individual_files {
                if cancel.is_cancelled() { return; }

                let relative = file_path
                    .strip_prefix(&result.base_dir)
                    .unwrap_or(file_path);
                let remote_key = format!("{}/{}", result.db_name, relative.to_string_lossy());

                if let Err(e) = s3.upload(file_path, &remote_key, progress.as_ref()).await {
                    fail_job(&manager, &app, &job_id, e.to_string());
                    return;
                }
                last_key = remote_key;
            }

            if !request.keep_local {
                let _ = std::fs::remove_dir_all(&result.base_dir);
            }

            if result.individual_files.len() == 1 {
                last_key
            } else {
                format!("{} files uploaded", result.individual_files.len())
            }
        }
    };

    if cancel.is_cancelled() { return; }

    if request.retention > 0 {
        if let Ok(entries) = s3.list("").await {
            if entries.len() > request.retention as usize {
                for old in &entries[request.retention as usize..] {
                    let _ = s3.delete(&old.key).await;
                }
            }
        }
    }

    complete_job(&manager, &app, &job_id, remote_key, progress.as_ref());
}

/// Streams pg_dump stdout -> optional gzip -> S3 multipart upload.
/// No temporary file is written to disk.
async fn run_streaming_backup(
    job_id: String,
    request: BackupJobRequest,
    manager: Arc<JobManager>,
    app: AppHandle,
    cancel: CancellationToken,
) {
    let raw_progress = JobProgressSender {
        job_id: job_id.clone(),
        app_handle: app.clone(),
        manager: Arc::clone(&manager),
    };
    let progress = ThrottledProgressSender::new(raw_progress, None);

    let dump_format = DumpFormat::from_str(&request.format);
    let db_url = request.connection_url.clone();
    let schemas = request.schemas.clone();

    // Estimate DB size for progress reporting
    let estimated_total = estimate_db_size(&db_url).await;

    let opts = DumpOptions {
        database_url: request.connection_url,
        format: dump_format,
        schemas: request.schemas,
        tables: request.tables,
        no_owner: request.no_owner,
        no_acl: request.no_acl,
        filename_prefix: request.filename_prefix.clone(),
        ..Default::default()
    };

    if cancel.is_cancelled() { return; }

    let mut child = match dump::spawn_dump_stream(&opts, &progress) {
        Ok(c) => c,
        Err(e) => {
            fail_job(&manager, &app, &job_id, e.to_string());
            return;
        }
    };

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => {
            fail_job(&manager, &app, &job_id, "Failed to capture pg_dump stdout".into());
            return;
        }
    };

    let db_name = pgdumpcloud_core::connection::parse_db_name(&opts.database_url)
        .unwrap_or_else(|| "unknown".into());
    let remote_key = dump::generate_filename(&request.filename_prefix, &db_name, &opts.format);
    let remote_key = if request.compression.starts_with("gzip") {
        format!("{remote_key}.gz")
    } else {
        remote_key
    };

    let s3 = S3Storage::new(
        &request.storage_endpoint,
        &request.storage_bucket,
        &request.storage_region,
        &request.storage_access_key,
        &request.storage_secret_key,
        &request.storage_prefix,
    );

    let mut async_stdout = tokio::process::ChildStdout::from_std(stdout)
        .unwrap_or_else(|_| panic!("failed to convert stdout to async"));

    // Capture stderr concurrently so it's not lost when pg_dump fails
    let stderr_handle = {
        let stderr = child.stderr.take();
        tokio::task::spawn_blocking(move || {
            let mut buf = Vec::new();
            if let Some(mut s) = stderr {
                let _ = std::io::Read::read_to_end(&mut s, &mut buf);
            }
            buf
        })
    };

    let upload_result = if request.compression.starts_with("gzip") {
        let level = compress::compression_level(
            request.compression.strip_prefix("gzip-").unwrap_or("default"),
        );
        let mut gz_stream = compress::AsyncGzipEncoder::new(async_stdout, level);
        s3.upload_stream(&mut gz_stream, &remote_key, &progress, Some(&cancel), estimated_total).await
    } else {
        s3.upload_stream(&mut async_stdout, &remote_key, &progress, Some(&cancel), estimated_total).await
    };

    let stderr_bytes = stderr_handle.await.unwrap_or_default();

    if cancel.is_cancelled() {
        let _ = child.kill();
        let _ = child.wait();
        return;
    }

    let exit_status = child.wait();

    // Determine whether pg_dump died from SIGPIPE. SIGPIPE means *we*
    // closed the read end of the pipe (upload error / cancellation),
    // so the upload error is the real root cause — not pg_dump itself.
    let died_from_sigpipe = {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            matches!(&exit_status, Ok(s) if s.signal() == Some(13))
        }
        #[cfg(not(unix))]
        { false }
    };

    // If the upload failed, report that error.
    // When pg_dump died from SIGPIPE it was a side-effect of the upload
    // closing the pipe, so the upload error is more informative.
    if let Err(e) = &upload_result {
        if died_from_sigpipe || matches!(&exit_status, Ok(s) if s.success()) {
            fail_job(&manager, &app, &job_id, format!("Upload failed: {e}"));
            return;
        }
    }

    // pg_dump failed on its own (not SIGPIPE) — report its stderr.
    match &exit_status {
        Ok(status) if !status.success() && !died_from_sigpipe => {
            let stderr_str = String::from_utf8_lossy(&stderr_bytes);
            let msg = if stderr_str.trim().is_empty() {
                format!("pg_dump failed with exit code: {status}")
            } else {
                format!("pg_dump failed: {}", stderr_str.trim())
            };
            fail_job(&manager, &app, &job_id, msg);
            return;
        }
        Err(e) => {
            fail_job(&manager, &app, &job_id, format!("pg_dump wait error: {e}"));
            return;
        }
        _ => {}
    }

    // Upload also failed but pg_dump succeeded — shouldn't normally happen
    // but handle it gracefully.
    if let Err(e) = upload_result {
        fail_job(&manager, &app, &job_id, format!("Upload failed: {e}"));
        return;
    }

    if cancel.is_cancelled() { return; }

    extract_and_upload_types(&db_url, &schemas, &remote_key, &s3, &progress).await;

    if request.retention > 0 {
        if let Ok(entries) = s3.list("").await {
            if entries.len() > request.retention as usize {
                for old in &entries[request.retention as usize..] {
                    let _ = s3.delete(&old.key).await;
                }
            }
        }
    }

    complete_job(&manager, &app, &job_id, remote_key.clone(), &progress);
}

async fn estimate_db_size(db_url: &str) -> Option<u64> {
    let db_name = pgdumpcloud_core::connection::parse_db_name(db_url)?;
    let dbs = introspect::list_databases(db_url).await.ok()?;
    dbs.iter()
        .find(|d| d.name == db_name)
        .and_then(|d| d.size_bytes.map(|b| b as u64))
}

async fn extract_and_upload_types(
    db_url: &str,
    schemas: &[String],
    remote_key: &str,
    s3: &S3Storage,
    progress: &dyn ProgressSender,
) {
    let types_sql = match introspect::extract_enum_types(db_url, schemas).await {
        Ok(sql) if !sql.is_empty() => sql,
        _ => return,
    };

    let types_key = dump::types_sql_key(remote_key);
    let types_path = std::env::temp_dir().join(&types_key);

    if std::fs::write(&types_path, &types_sql).is_ok() {
        let _ = s3.upload(&types_path, &types_key, progress).await;
        let _ = std::fs::remove_file(&types_path);
    }
}

/// Attempts to mark a job as completed. Returns false if the job was
/// already cancelled (avoids overwriting the cancelled status).
fn complete_job(
    manager: &JobManager,
    app: &AppHandle,
    job_id: &str,
    result: String,
    progress: &dyn ProgressSender,
) -> bool {
    if let Some(status) = manager.get_status(job_id) {
        if status == JobStatus::Cancelled {
            return false;
        }
    }

    progress.send(ProgressEvent::Finished {
        message: format!("Backup uploaded: {result}"),
    });

    manager.set_result(job_id, result);
    manager.update_status(job_id, JobStatus::Completed);
    let _ = app.emit(
        "job:status_changed",
        JobStatusEvent {
            job_id: job_id.to_string(),
            status: JobStatus::Completed,
        },
    );
    true
}

fn fail_job(manager: &JobManager, app: &AppHandle, job_id: &str, error: String) {
    if let Some(status) = manager.get_status(job_id) {
        if status == JobStatus::Cancelled {
            return;
        }
    }
    manager.set_error(job_id, error);
    let _ = app.emit(
        "job:status_changed",
        JobStatusEvent {
            job_id: job_id.to_string(),
            status: JobStatus::Failed,
        },
    );
}

#[tauri::command]
pub async fn create_restore_job(
    app: AppHandle,
    manager: State<'_, Arc<JobManager>>,
    request: RestoreJobRequest,
) -> Result<String, String> {
    let request_json = serde_json::to_value(&request).map_err(|e| e.to_string())?;
    let job_id = manager.create_job(JobKind::Restore, request_json);
    let cancel_token = manager.create_cancel_token(&job_id);

    let mgr = Arc::clone(&manager.inner());
    let app_handle = app.clone();
    let jid = job_id.clone();

    tokio::spawn(async move {
        run_restore_task(jid, request, mgr, app_handle, cancel_token).await;
    });

    Ok(job_id)
}

async fn run_restore_task(
    job_id: String,
    request: RestoreJobRequest,
    manager: Arc<JobManager>,
    app: AppHandle,
    cancel: CancellationToken,
) {
    manager.update_status(&job_id, JobStatus::Running);
    let _ = app.emit(
        "job:status_changed",
        JobStatusEvent {
            job_id: job_id.clone(),
            status: JobStatus::Running,
        },
    );

    let raw_progress = JobProgressSender {
        job_id: job_id.clone(),
        app_handle: app.clone(),
        manager: Arc::clone(&manager),
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

    let progress = ThrottledProgressSender::new(raw_progress, None);

    if cancel.is_cancelled() {
        manager.remove_cancel_token(&job_id);
        return;
    }

    if let Err(e) = s3.download(&request.backup_key, &local_path, &progress).await {
        fail_job(&manager, &app, &job_id, e.to_string());
        manager.remove_cancel_token(&job_id);
        return;
    }

    if cancel.is_cancelled() {
        let _ = std::fs::remove_file(&local_path);
        manager.remove_cancel_token(&job_id);
        return;
    }

    let restore_path = if request.backup_key.ends_with(".gz") {
        match compress::decompress_gzip(&local_path, &progress) {
            Ok(decompressed) => {
                let _ = std::fs::remove_file(&local_path);
                decompressed
            }
            Err(e) => {
                fail_job(&manager, &app, &job_id, e.to_string());
                manager.remove_cancel_token(&job_id);
                return;
            }
        }
    } else {
        local_path.clone()
    };

    if cancel.is_cancelled() {
        let _ = std::fs::remove_file(&restore_path);
        manager.remove_cancel_token(&job_id);
        return;
    }

    let types_key = dump::types_sql_key(&request.backup_key);
    let types_local = std::env::temp_dir().join(&types_key);
    if s3.download(&types_key, &types_local, &progress).await.is_ok() {
        if let Err(e) = restore::apply_types_sql(&types_local, &request.target_url) {
            progress.send(ProgressEvent::Error {
                message: format!("Types SQL warning: {e}"),
            });
        }
        let _ = std::fs::remove_file(&types_local);
    }

    let opts = RestoreOptions {
        database_url: request.target_url,
        clean: request.clean,
        no_owner: request.no_owner,
        no_acl: request.no_acl,
        if_exists: request.clean,
        data_only: request.data_only,
    };

    let mut child = match restore::spawn_restore(&restore_path, &opts, &progress) {
        Ok(c) => c,
        Err(e) => {
            fail_job(&manager, &app, &job_id, e.to_string());
            let _ = std::fs::remove_file(&restore_path);
            manager.remove_cancel_token(&job_id);
            return;
        }
    };

    // Read stderr lines for verbose progress and capture errors
    let stderr_handle = {
        let stderr = child.stderr.take();
        let p = JobProgressSender {
            job_id: job_id.clone(),
            app_handle: app.clone(),
            manager: Arc::clone(&manager),
        };
        tokio::task::spawn_blocking(move || {
            use std::io::{BufRead, BufReader};
            let mut error_lines = Vec::new();
            let mut line_count: u64 = 0;
            if let Some(s) = stderr {
                let reader = BufReader::new(s);
                for line in reader.lines() {
                    let line = match line {
                        Ok(l) => l,
                        Err(_) => break,
                    };
                    if line.starts_with("pg_restore:") && line.contains("error") {
                        error_lines.push(line);
                    } else {
                        line_count += 1;
                        if line_count % 5 == 0 {
                            p.send(ProgressEvent::Progress {
                                phase: Phase::Restoring,
                                bytes: line_count,
                                total: None,
                            });
                        }
                    }
                }
            }
            (error_lines, line_count)
        })
    };

    let exit_status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .unwrap_or_else(|e| Err(std::io::Error::new(std::io::ErrorKind::Other, e)));

    let (error_lines, _) = stderr_handle.await.unwrap_or_default();

    let _ = std::fs::remove_file(&restore_path);

    if cancel.is_cancelled() {
        manager.remove_cancel_token(&job_id);
        return;
    }

    match &exit_status {
        Ok(status) if !status.success() => {
            let msg = if !error_lines.is_empty() {
                if error_lines.iter().any(|l| l.contains("errors ignored on restore")) {
                    progress.send(ProgressEvent::Error {
                        message: format!("Restore completed with warnings ({} errors ignored)", error_lines.len()),
                    });
                    // Fall through to success
                    String::new()
                } else {
                    error_lines.join("\n")
                }
            } else {
                format!("Restore failed with exit code: {status}")
            };
            if !msg.is_empty() {
                fail_job(&manager, &app, &job_id, msg);
                manager.remove_cancel_token(&job_id);
                return;
            }
        }
        Err(e) => {
            fail_job(&manager, &app, &job_id, format!("Restore wait error: {e}"));
            manager.remove_cancel_token(&job_id);
            return;
        }
        _ => {}
    }

    progress.send(ProgressEvent::PhaseCompleted {
        phase: Phase::Restoring,
    });

    if let Some(status) = manager.get_status(&job_id) {
        if status == JobStatus::Cancelled {
            manager.remove_cancel_token(&job_id);
            return;
        }
    }

    progress.send(ProgressEvent::Finished {
        message: "Restore completed successfully".into(),
    });

    manager.set_result(&job_id, "Restore completed".into());
    manager.update_status(&job_id, JobStatus::Completed);
    let _ = app.emit(
        "job:status_changed",
        JobStatusEvent {
            job_id: job_id.clone(),
            status: JobStatus::Completed,
        },
    );

    manager.remove_cancel_token(&job_id);
}

#[tauri::command]
pub async fn list_jobs(manager: State<'_, Arc<JobManager>>) -> Result<Vec<JobSummary>, String> {
    Ok(manager.list_jobs())
}

#[tauri::command]
pub async fn get_job(manager: State<'_, Arc<JobManager>>, id: String) -> Result<Job, String> {
    manager
        .get_job(&id)
        .ok_or_else(|| format!("Job not found: {id}"))
}

#[derive(serde::Serialize)]
pub struct JobLogsResponse {
    pub logs: Vec<LogEntry>,
    pub total: u32,
}

#[tauri::command]
pub async fn get_job_logs(
    manager: State<'_, Arc<JobManager>>,
    id: String,
    offset: u32,
    limit: u32,
) -> Result<JobLogsResponse, String> {
    let total = manager.get_job_log_count(&id);
    let logs = manager.get_job_logs(&id, offset, limit);
    Ok(JobLogsResponse { logs, total })
}

#[tauri::command]
pub async fn cancel_job(
    app: AppHandle,
    manager: State<'_, Arc<JobManager>>,
    id: String,
) -> Result<(), String> {
    let job = manager
        .get_job(&id)
        .ok_or_else(|| format!("Job not found: {id}"))?;

    if job.status != JobStatus::Running && job.status != JobStatus::Queued {
        return Err("Can only cancel running or queued jobs".into());
    }

    manager.cancel(&id);
    manager.update_status(&id, JobStatus::Cancelled);
    let _ = app.emit(
        "job:status_changed",
        JobStatusEvent {
            job_id: id,
            status: JobStatus::Cancelled,
        },
    );
    Ok(())
}

#[tauri::command]
pub async fn delete_job(manager: State<'_, Arc<JobManager>>, id: String) -> Result<(), String> {
    let job = manager
        .get_job(&id)
        .ok_or_else(|| format!("Job not found: {id}"))?;

    if job.status == JobStatus::Running {
        return Err("Cannot delete a running job".into());
    }

    manager.delete_job(&id);
    Ok(())
}

#[tauri::command]
pub async fn retry_job(
    app: AppHandle,
    manager: State<'_, Arc<JobManager>>,
    id: String,
) -> Result<String, String> {
    let (kind, request_value) = manager
        .get_job_request(&id)
        .ok_or_else(|| format!("Job not found: {id}"))?;

    match kind {
        JobKind::Backup => {
            let request: BackupJobRequest =
                serde_json::from_value(request_value.clone()).map_err(|e| e.to_string())?;
            let new_job_id = manager.create_job(JobKind::Backup, request_value);
            let cancel_token = manager.create_cancel_token(&new_job_id);

            let mgr = Arc::clone(&manager.inner());
            let app_handle = app.clone();
            let jid = new_job_id.clone();

            tokio::spawn(async move {
                run_backup_task(jid, request, mgr, app_handle, cancel_token).await;
            });

            Ok(new_job_id)
        }
        JobKind::Restore => {
            let request: RestoreJobRequest =
                serde_json::from_value(request_value.clone()).map_err(|e| e.to_string())?;
            let new_job_id = manager.create_job(JobKind::Restore, request_value);
            let cancel_token = manager.create_cancel_token(&new_job_id);

            let mgr = Arc::clone(&manager.inner());
            let app_handle = app.clone();
            let jid = new_job_id.clone();

            tokio::spawn(async move {
                run_restore_task(jid, request, mgr, app_handle, cancel_token).await;
            });

            Ok(new_job_id)
        }
    }
}
