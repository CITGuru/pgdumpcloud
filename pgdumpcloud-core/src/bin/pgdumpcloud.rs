use clap::{Parser, Subcommand, ValueEnum};
use pgdumpcloud_core::{
    compress, config, dump, introspect, parquet_export, progress, restore, storage,
};
use progress::ProgressSender;
use std::path::PathBuf;
use std::process;

#[derive(Parser)]
#[command(name = "pgdumpcloud", version, about = "PostgreSQL backup/restore to S3-compatible cloud storage")]
struct Cli {
    #[arg(long, global = true, help = "Path to config file")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a database backup
    Backup {
        #[arg(long, help = "Database URL (postgres://...)")]
        url: Option<String>,

        #[arg(long, help = "Saved connection name or ID")]
        connection: Option<String>,

        #[arg(long, value_delimiter = ',', help = "Tables to include (schema.table,...)")]
        tables: Vec<String>,

        #[arg(long, value_delimiter = ',', help = "Schemas to include")]
        schemas: Vec<String>,

        #[arg(long, default_value = "custom", help = "Dump format")]
        format: FormatArg,

        #[arg(long, default_value = "gzip", help = "Compression method")]
        compression: CompressionArg,

        #[arg(long, help = "Saved storage target name or ID")]
        storage: Option<String>,

        #[arg(long, help = "S3 endpoint URL")]
        endpoint: Option<String>,

        #[arg(long, help = "S3 bucket name")]
        bucket: Option<String>,

        #[arg(long, help = "S3 access key")]
        access_key: Option<String>,

        #[arg(long, help = "S3 secret key")]
        secret_key: Option<String>,

        #[arg(long, default_value = "us-east-1", help = "S3 region")]
        region: String,

        #[arg(long, default_value = "", help = "Remote key prefix")]
        prefix: String,

        #[arg(long, default_value_t = 7, help = "Number of backups to retain")]
        retention: u32,

        #[arg(long, help = "Keep local backup file after upload")]
        keep_local: bool,

        #[arg(long, default_value = "backup", help = "Filename prefix")]
        filename_prefix: String,

        #[arg(long, help = "Exclude owner statements")]
        no_owner: bool,

        #[arg(long, help = "Exclude ACL statements")]
        no_acl: bool,

        #[arg(long, help = "Output directory for local dump")]
        output_dir: Option<PathBuf>,

        #[arg(long, help = "Stream pg_dump directly to S3 without local temp file (for large databases)")]
        streaming: bool,

        #[arg(long, default_value = "archive", help = "Parquet storage mode (archive or individual)")]
        storage_mode: StorageModeArg,

        #[arg(long, help = "Max rows per parquet file")]
        max_rows_per_file: Option<u64>,

        #[arg(long, default_value = "none", help = "Hive partitioning strategy (none, year, year-month)")]
        partition_by: PartitionByArg,

        #[arg(long, help = "Column name for Hive partitioning (required when partition-by != none)")]
        partition_column: Option<String>,

        #[arg(long, default_value = "cursor", help = "Fetch strategy for flat export (cursor or copy)")]
        fetch_strategy: FetchStrategyArg,
    },

    /// Restore a backup from cloud storage
    Restore {
        #[arg(long, help = "Remote backup key to restore")]
        backup: String,

        #[arg(long, help = "Saved storage target name or ID")]
        storage: Option<String>,

        #[arg(long, help = "S3 endpoint URL")]
        endpoint: Option<String>,

        #[arg(long, help = "S3 bucket name")]
        bucket: Option<String>,

        #[arg(long, help = "S3 access key")]
        access_key: Option<String>,

        #[arg(long, help = "S3 secret key")]
        secret_key: Option<String>,

        #[arg(long, default_value = "us-east-1", help = "S3 region")]
        region: String,

        #[arg(long, help = "Target database URL")]
        target_url: String,

        #[arg(long, help = "Drop existing objects before restore")]
        clean: bool,

        #[arg(long, help = "Exclude owner statements")]
        no_owner: bool,

        #[arg(long, help = "Exclude ACL statements")]
        no_acl: bool,

        #[arg(long, help = "Restore data only (skip schema creation)")]
        data_only: bool,
    },

    /// Check environment and dependencies
    Doctor,

    /// List databases, schemas, and tables
    Introspect {
        #[arg(long, help = "Database URL")]
        url: Option<String>,

        #[arg(long, help = "Saved connection name or ID")]
        connection: Option<String>,

        #[arg(long, help = "Schema to list tables for")]
        schema: Option<String>,
    },

    /// List backups on a storage target
    ListBackups {
        #[arg(long, help = "Saved storage target name or ID")]
        storage: Option<String>,

        #[arg(long, help = "S3 endpoint URL")]
        endpoint: Option<String>,

        #[arg(long, help = "S3 bucket name")]
        bucket: Option<String>,

        #[arg(long, help = "S3 access key")]
        access_key: Option<String>,

        #[arg(long, help = "S3 secret key")]
        secret_key: Option<String>,

        #[arg(long, default_value = "us-east-1", help = "S3 region")]
        region: String,

        #[arg(long, default_value = "", help = "Key prefix filter")]
        prefix: String,
    },
}

#[derive(Clone, ValueEnum, PartialEq)]
enum FormatArg {
    Custom,
    Plain,
    Tar,
    Parquet,
}

#[derive(Clone, ValueEnum)]
enum StorageModeArg {
    Archive,
    Individual,
}

#[derive(Clone, ValueEnum)]
enum PartitionByArg {
    None,
    Year,
    YearMonth,
}

#[derive(Clone, ValueEnum)]
enum FetchStrategyArg {
    Cursor,
    Copy,
}

#[derive(Clone, ValueEnum)]
enum CompressionArg {
    Gzip,
    None,
}

fn load_config(path: Option<&PathBuf>) -> config::AppConfig {
    let config_path = path
        .cloned()
        .unwrap_or_else(config::AppConfig::default_config_path);
    config::AppConfig::load(&config_path).unwrap_or_default()
}

fn resolve_db_url(
    url: &Option<String>,
    conn_name: &Option<String>,
    cfg: &config::AppConfig,
) -> String {
    if let Some(u) = url {
        return u.clone();
    }
    if let Some(name) = conn_name {
        if let Some(c) = cfg.find_connection(name) {
            return c.build_url();
        }
        eprintln!("Connection '{name}' not found in config");
        process::exit(1);
    }
    if let Ok(u) = std::env::var("DATABASE_URL") {
        return u;
    }
    eprintln!("No database URL provided. Use --url, --connection, or set DATABASE_URL");
    process::exit(1);
}

fn resolve_s3_storage(
    storage_name: &Option<String>,
    endpoint: &Option<String>,
    bucket: &Option<String>,
    access_key: &Option<String>,
    secret_key: &Option<String>,
    region: &str,
    prefix: &str,
    cfg: &config::AppConfig,
) -> storage::s3::S3Storage {
    if let Some(name) = storage_name {
        if let Some(s) = cfg.find_storage(name) {
            return storage::s3::S3Storage::new(
                s.endpoint.as_deref().unwrap_or(""),
                s.bucket.as_deref().unwrap_or(""),
                s.region.as_deref().unwrap_or("us-east-1"),
                s.access_key.as_deref().unwrap_or(""),
                s.secret_key.as_deref().unwrap_or(""),
                s.prefix.as_deref().unwrap_or(""),
            );
        }
        eprintln!("Storage target '{name}' not found in config");
        process::exit(1);
    }

    let ep = endpoint.clone()
        .or_else(|| std::env::var("S3_ENDPOINT").ok())
        .unwrap_or_else(|| { eprintln!("No storage endpoint. Use --endpoint or --storage"); process::exit(1); });
    let bk = bucket.clone()
        .or_else(|| std::env::var("S3_BUCKET").ok())
        .unwrap_or_else(|| { eprintln!("No bucket name. Use --bucket or --storage"); process::exit(1); });
    let ak = access_key.clone()
        .or_else(|| std::env::var("S3_ACCESS_KEY").ok())
        .unwrap_or_else(|| { eprintln!("No access key. Use --access-key or --storage"); process::exit(1); });
    let sk = secret_key.clone()
        .or_else(|| std::env::var("S3_SECRET_KEY").ok())
        .unwrap_or_else(|| { eprintln!("No secret key. Use --secret-key or --storage"); process::exit(1); });

    storage::s3::S3Storage::new(&ep, &bk, region, &ak, &sk, prefix)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let cfg = load_config(cli.config.as_ref());
    let progress_sender = progress::CliProgressSender;

    match cli.command {
        Commands::Doctor => cmd_doctor(),

        Commands::Backup {
            url,
            connection: conn,
            tables,
            schemas,
            format,
            compression,
            storage: storage_name,
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            prefix,
            retention,
            keep_local,
            filename_prefix,
            no_owner,
            no_acl,
            output_dir,
            streaming,
            storage_mode,
            max_rows_per_file,
            partition_by,
            partition_column,
            fetch_strategy,
        } => {
            let db_url = resolve_db_url(&url, &conn, &cfg);
            let s3 = resolve_s3_storage(
                &storage_name, &endpoint, &bucket, &access_key, &secret_key,
                &region, &prefix, &cfg,
            );

            if format == FormatArg::Parquet && streaming {
                eprintln!("[ERROR] --streaming is incompatible with --format parquet");
                process::exit(1);
            }

            if format == FormatArg::Parquet {
                run_parquet_backup_cli(
                    &db_url, schemas, tables, &filename_prefix,
                    output_dir.unwrap_or_else(std::env::temp_dir),
                    storage_mode, max_rows_per_file, partition_by, partition_column,
                    fetch_strategy,
                    keep_local, retention, s3, &progress_sender,
                ).await;
                return;
            }

            let dump_format = match format {
                FormatArg::Custom => dump::DumpFormat::Custom,
                FormatArg::Plain => dump::DumpFormat::Plain,
                FormatArg::Tar => dump::DumpFormat::Tar,
                FormatArg::Parquet => unreachable!(),
            };

            let db_url_for_types = db_url.clone();
            let schemas_for_types = schemas.clone();

            let opts = dump::DumpOptions {
                database_url: db_url,
                format: dump_format,
                schemas,
                tables,
                no_owner,
                no_acl,
                output_dir: output_dir.unwrap_or_else(std::env::temp_dir),
                filename_prefix: filename_prefix.clone(),
                ..Default::default()
            };

            if streaming {
                run_streaming_backup(opts, compression, s3, retention, &filename_prefix, &progress_sender).await;
            } else {
                let dump_path = match dump::run_dump(&opts, &progress_sender) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("[ERROR] {e}");
                        process::exit(1);
                    }
                };

                let upload_path = match compression {
                    CompressionArg::Gzip => {
                        match compress::compress_gzip(&dump_path, flate2::Compression::default(), &progress_sender) {
                            Ok(p) => {
                                let _ = std::fs::remove_file(&dump_path);
                                p
                            }
                            Err(e) => {
                                eprintln!("[ERROR] Compression failed: {e}");
                                process::exit(1);
                            }
                        }
                    }
                    CompressionArg::None => dump_path.clone(),
                };

                let remote_key = upload_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("backup")
                    .to_string();

                use storage::CloudStorage;
                if let Err(e) = s3.upload(&upload_path, &remote_key, &progress_sender).await {
                    eprintln!("[ERROR] Upload failed: {e}");
                    process::exit(1);
                }

                extract_and_upload_types_cli(
                    &db_url_for_types, &schemas_for_types, &remote_key, &s3, &progress_sender,
                ).await;

                progress_sender.send(progress::ProgressEvent::Finished {
                    message: format!("Backup uploaded as {remote_key}"),
                });

                if retention > 0 {
                    if let Ok(entries) = s3.list("").await {
                        if entries.len() > retention as usize {
                            for old in &entries[retention as usize..] {
                                if let Err(e) = s3.delete(&old.key).await {
                                    eprintln!("[WARN] Failed to delete old backup {}: {e}", old.key);
                                } else {
                                    eprintln!("[INFO] Deleted old backup: {}", old.key);
                                }
                            }
                        }
                    }
                }

                if !keep_local {
                    let _ = std::fs::remove_file(&upload_path);
                }
            }
        }

        Commands::Restore {
            backup,
            storage: storage_name,
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            target_url,
            clean,
            no_owner,
            no_acl,
            data_only,
        } => {
            let s3 = resolve_s3_storage(
                &storage_name, &endpoint, &bucket, &access_key, &secret_key,
                &region, "", &cfg,
            );

            let local_path = std::env::temp_dir().join(&backup);

            use storage::CloudStorage;
            if let Err(e) = s3.download(&backup, &local_path, &progress_sender).await {
                eprintln!("[ERROR] Download failed: {e}");
                process::exit(1);
            }

            let restore_path = if backup.ends_with(".gz") {
                match compress::decompress_gzip(&local_path, &progress_sender) {
                    Ok(p) => {
                        let _ = std::fs::remove_file(&local_path);
                        p
                    }
                    Err(e) => {
                        eprintln!("[ERROR] Decompression failed: {e}");
                        process::exit(1);
                    }
                }
            } else {
                local_path.clone()
            };

            // Try to download and apply the companion .types.sql (best-effort)
            let types_key = dump::types_sql_key(&backup);
            let types_local = std::env::temp_dir().join(&types_key);
            if s3.download(&types_key, &types_local, &progress_sender).await.is_ok() {
                match restore::apply_types_sql(&types_local, &target_url) {
                    Ok(()) => eprintln!("[INFO] Applied types file: {types_key}"),
                    Err(e) => eprintln!("[WARN] Types SQL warning: {e}"),
                }
                let _ = std::fs::remove_file(&types_local);
            }

            let opts = restore::RestoreOptions {
                database_url: target_url,
                clean,
                no_owner,
                no_acl,
                if_exists: clean,
                data_only,
            };

            if let Err(e) = restore::run_restore(&restore_path, &opts, &progress_sender) {
                eprintln!("[ERROR] Restore failed: {e}");
                let _ = std::fs::remove_file(&restore_path);
                process::exit(1);
            }

            let _ = std::fs::remove_file(&restore_path);

            progress_sender.send(progress::ProgressEvent::Finished {
                message: "Restore completed successfully".into(),
            });
        }

        Commands::Introspect {
            url,
            connection: conn,
            schema,
        } => {
            let db_url = resolve_db_url(&url, &conn, &cfg);

            if let Some(schema_name) = schema {
                match introspect::list_tables(&db_url, &schema_name).await {
                    Ok(tables) => {
                        println!("{:<30} {:>12} {:>12}", "TABLE", "ROWS", "SIZE");
                        println!("{}", "-".repeat(56));
                        for t in &tables {
                            println!(
                                "{:<30} {:>12} {:>12}",
                                format!("{}.{}", t.schema, t.name),
                                t.row_estimate,
                                t.size_pretty
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("[ERROR] {e}");
                        process::exit(1);
                    }
                }
            } else {
                match introspect::list_schemas(&db_url).await {
                    Ok(schemas) => {
                        println!("{:<30} {:>10}", "SCHEMA", "TABLES");
                        println!("{}", "-".repeat(42));
                        for s in &schemas {
                            println!("{:<30} {:>10}", s.name, s.table_count);
                        }
                    }
                    Err(e) => {
                        eprintln!("[ERROR] {e}");
                        process::exit(1);
                    }
                }
            }
        }

        Commands::ListBackups {
            storage: storage_name,
            endpoint,
            bucket,
            access_key,
            secret_key,
            region,
            prefix,
        } => {
            let s3 = resolve_s3_storage(
                &storage_name, &endpoint, &bucket, &access_key, &secret_key,
                &region, &prefix, &cfg,
            );

            use storage::CloudStorage;
            match s3.list("").await {
                Ok(entries) => {
                    println!("{:<50} {:>12} {}", "KEY", "SIZE", "LAST MODIFIED");
                    println!("{}", "-".repeat(80));
                    for e in &entries {
                        let size_str = format_size(e.size);
                        println!(
                            "{:<50} {:>12} {}",
                            e.key,
                            size_str,
                            e.last_modified.as_deref().unwrap_or("-")
                        );
                    }
                }
                Err(e) => {
                    eprintln!("[ERROR] {e}");
                    process::exit(1);
                }
            }
        }
    }
}

async fn extract_and_upload_types_cli(
    db_url: &str,
    schemas: &[String],
    remote_key: &str,
    s3: &storage::s3::S3Storage,
    progress: &dyn progress::ProgressSender,
) {
    let types_sql = match introspect::extract_enum_types(db_url, schemas).await {
        Ok(sql) if !sql.is_empty() => sql,
        Ok(_) => return,
        Err(e) => {
            eprintln!("[WARN] Failed to extract enum types: {e}");
            return;
        }
    };

    let types_key = dump::types_sql_key(remote_key);
    let types_path = std::env::temp_dir().join(&types_key);

    if let Err(e) = std::fs::write(&types_path, &types_sql) {
        eprintln!("[WARN] Failed to write types file: {e}");
        return;
    }

    use storage::CloudStorage;
    if let Err(e) = s3.upload(&types_path, &types_key, progress).await {
        eprintln!("[WARN] Failed to upload types file: {e}");
    } else {
        eprintln!("[INFO] Uploaded types file: {types_key}");
    }

    let _ = std::fs::remove_file(&types_path);
}

fn cmd_doctor() {
    println!("pgdumpcloud doctor\n");

    match dump::check_pg_dump() {
        Ok(v) => println!("[OK] {v}"),
        Err(_) => println!("[FAIL] pg_dump not found in PATH"),
    }

    match restore::check_pg_restore() {
        Ok(v) => println!("[OK] {v}"),
        Err(_) => println!("[FAIL] pg_restore not found in PATH"),
    }

    match restore::check_psql() {
        Ok(v) => println!("[OK] {v}"),
        Err(_) => println!("[FAIL] psql not found in PATH"),
    }

    match std::process::Command::new("rclone").arg("--version").output() {
        Ok(o) if o.status.success() => {
            let v = String::from_utf8_lossy(&o.stdout);
            let first_line = v.lines().next().unwrap_or("rclone");
            println!("[OK] {first_line}");
        }
        _ => println!("[INFO] rclone not found (optional)"),
    }

    println!("\nDoctor check complete.");
}

async fn run_streaming_backup(
    opts: dump::DumpOptions,
    compression: CompressionArg,
    s3: storage::s3::S3Storage,
    retention: u32,
    filename_prefix: &str,
    progress_sender: &dyn progress::ProgressSender,
) {
    let db_name = pgdumpcloud_core::connection::parse_db_name(&opts.database_url)
        .unwrap_or_else(|| "unknown".into());
    let remote_key = dump::generate_filename(filename_prefix, &db_name, &opts.format);
    let remote_key = match compression {
        CompressionArg::Gzip => format!("{remote_key}.gz"),
        CompressionArg::None => remote_key,
    };

    let mut child = match dump::spawn_dump_stream(&opts, progress_sender) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[ERROR] {e}");
            process::exit(1);
        }
    };

    let stdout = child.stdout.take().unwrap_or_else(|| {
        eprintln!("[ERROR] Failed to capture pg_dump stdout");
        process::exit(1);
    });

    let mut async_stdout = tokio::process::ChildStdout::from_std(stdout)
        .unwrap_or_else(|e| {
            eprintln!("[ERROR] Failed to convert stdout: {e}");
            process::exit(1);
        });

    let upload_result = match compression {
        CompressionArg::Gzip => {
            let mut gz = compress::AsyncGzipEncoder::new(async_stdout, flate2::Compression::default());
            s3.upload_stream(&mut gz, &remote_key, progress_sender, None, None).await
        }
        CompressionArg::None => {
            s3.upload_stream(&mut async_stdout, &remote_key, progress_sender, None, None).await
        }
    };

    let mut stderr_bytes = Vec::new();
    if let Some(mut stderr) = child.stderr.take() {
        use std::io::Read;
        let _ = stderr.read_to_end(&mut stderr_bytes);
    }

    let exit_status = child.wait();

    if let Err(e) = upload_result {
        eprintln!("[ERROR] Upload failed: {e}");
        process::exit(1);
    }

    match exit_status {
        Ok(status) if !status.success() => {
            let stderr_str = String::from_utf8_lossy(&stderr_bytes);
            eprintln!("[ERROR] pg_dump failed: {stderr_str}");
            process::exit(1);
        }
        Err(e) => {
            eprintln!("[ERROR] pg_dump wait error: {e}");
            process::exit(1);
        }
        _ => {}
    }

    progress_sender.send(progress::ProgressEvent::PhaseCompleted {
        phase: progress::Phase::Dumping,
    });

    extract_and_upload_types_cli(
        &opts.database_url, &opts.schemas, &remote_key, &s3, progress_sender,
    ).await;

    if retention > 0 {
        use storage::CloudStorage;
        if let Ok(entries) = s3.list("").await {
            if entries.len() > retention as usize {
                for old in &entries[retention as usize..] {
                    if let Err(e) = s3.delete(&old.key).await {
                        eprintln!("[WARN] Failed to delete old backup {}: {e}", old.key);
                    } else {
                        eprintln!("[INFO] Deleted old backup: {}", old.key);
                    }
                }
            }
        }
    }

    progress_sender.send(progress::ProgressEvent::Finished {
        message: format!("Backup streamed as {remote_key}"),
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_parquet_backup_cli(
    db_url: &str,
    schemas: Vec<String>,
    tables: Vec<String>,
    filename_prefix: &str,
    output_dir: PathBuf,
    storage_mode: StorageModeArg,
    max_rows_per_file: Option<u64>,
    partition_by: PartitionByArg,
    partition_column: Option<String>,
    fetch_strategy: FetchStrategyArg,
    keep_local: bool,
    retention: u32,
    s3: storage::s3::S3Storage,
    progress_sender: &dyn progress::ProgressSender,
) {
    let mode = match storage_mode {
        StorageModeArg::Archive => parquet_export::StorageMode::Archive,
        StorageModeArg::Individual => parquet_export::StorageMode::Individual,
    };

    let strategy = match fetch_strategy {
        FetchStrategyArg::Copy => parquet_export::FetchStrategy::Copy,
        FetchStrategyArg::Cursor => parquet_export::FetchStrategy::Cursor,
    };

    let hive = match partition_by {
        PartitionByArg::None => parquet_export::HivePartitioning::None,
        PartitionByArg::Year => {
            let col = partition_column.unwrap_or_else(|| {
                eprintln!("[ERROR] --partition-column is required when --partition-by is year");
                process::exit(1);
            });
            parquet_export::HivePartitioning::Year { column: col }
        }
        PartitionByArg::YearMonth => {
            let col = partition_column.unwrap_or_else(|| {
                eprintln!("[ERROR] --partition-column is required when --partition-by is year-month");
                process::exit(1);
            });
            parquet_export::HivePartitioning::YearMonth { column: col }
        }
    };

    let opts = parquet_export::ParquetExportOptions {
        database_url: db_url.to_string(),
        schemas,
        tables,
        output_dir,
        filename_prefix: filename_prefix.to_string(),
        max_rows_per_file,
        hive_partitioning: hive,
        storage_mode: mode,
        fetch_strategy: strategy,
    };

    let result = match parquet_export::run_parquet_export(&opts, progress_sender).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[ERROR] Parquet export failed: {e}");
            process::exit(1);
        }
    };

    use storage::CloudStorage;
    match result.mode {
        parquet_export::StorageMode::Archive => {
            if let Some(archive_path) = &result.archive_path {
                let remote_key = archive_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("export.tar.gz")
                    .to_string();

                if let Err(e) = s3.upload(archive_path, &remote_key, progress_sender).await {
                    eprintln!("[ERROR] Upload failed: {e}");
                    process::exit(1);
                }

                if !keep_local {
                    let _ = std::fs::remove_file(archive_path);
                }

                progress_sender.send(progress::ProgressEvent::Finished {
                    message: format!("Parquet export uploaded as {remote_key}"),
                });
            }
        }
        parquet_export::StorageMode::Individual => {
            for file_path in &result.individual_files {
                let relative = file_path
                    .strip_prefix(&result.base_dir)
                    .unwrap_or(file_path);
                let remote_key = format!("{}/{}", result.db_name, relative.to_string_lossy());

                if let Err(e) = s3.upload(file_path, &remote_key, progress_sender).await {
                    eprintln!("[ERROR] Upload failed for {remote_key}: {e}");
                    process::exit(1);
                }
            }

            if !keep_local {
                let _ = std::fs::remove_dir_all(&result.base_dir);
            }

            progress_sender.send(progress::ProgressEvent::Finished {
                message: format!("{} parquet files uploaded", result.individual_files.len()),
            });
        }
    }

    if retention > 0 {
        if let Ok(entries) = s3.list("").await {
            if entries.len() > retention as usize {
                for old in &entries[retention as usize..] {
                    if let Err(e) = s3.delete(&old.key).await {
                        eprintln!("[WARN] Failed to delete old backup {}: {e}", old.key);
                    } else {
                        eprintln!("[INFO] Deleted old backup: {}", old.key);
                    }
                }
            }
        }
    }
}

fn format_size(bytes: i64) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
