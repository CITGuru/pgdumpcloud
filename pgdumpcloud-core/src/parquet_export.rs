use crate::connection;
use crate::error::{PgDumpCloudError, Result};
use crate::introspect;
use crate::progress::{Phase, ProgressEvent, ProgressSender};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_MAX_ROWS: u64 = 500_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageMode {
    Archive,
    Individual,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HivePartitioning {
    None,
    Year { column: String },
    YearMonth { column: String },
}

pub struct ParquetExportOptions {
    pub database_url: String,
    pub schemas: Vec<String>,
    pub tables: Vec<String>,
    pub output_dir: PathBuf,
    pub filename_prefix: String,
    pub max_rows_per_file: Option<u64>,
    pub hive_partitioning: HivePartitioning,
    pub storage_mode: StorageMode,
}

pub struct ExportResult {
    pub mode: StorageMode,
    pub archive_path: Option<PathBuf>,
    pub individual_files: Vec<PathBuf>,
    pub base_dir: PathBuf,
    pub db_name: String,
}

#[derive(Debug, Clone, Serialize)]
struct ManifestColumn {
    name: String,
    pg_type: String,
    arrow_type: String,
}

#[derive(Debug, Clone, Serialize)]
struct ManifestEntry {
    schema: String,
    table: String,
    columns: Vec<ManifestColumn>,
    files: Vec<String>,
    row_count: u64,
}

#[derive(Debug, Clone, Serialize)]
struct Manifest {
    version: u32,
    created_at: String,
    tables: Vec<ManifestEntry>,
}

pub async fn run_parquet_export(
    options: &ParquetExportOptions,
    progress: &dyn ProgressSender,
) -> Result<ExportResult> {
    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Exporting,
    });

    let db_name = connection::parse_db_name(&options.database_url)
        .unwrap_or_else(|| "export".to_string());

    let client = introspect::connect(&options.database_url).await?;

    let tables = resolve_tables(&client, &options.schemas, &options.tables).await?;

    let base_dir = options.output_dir.join(format!(
        "{}_parquet_{}",
        options.filename_prefix,
        chrono::Utc::now().format("%Y%m%d_%H%M%S")
    ));
    std::fs::create_dir_all(&base_dir)?;

    let max_rows = options.max_rows_per_file.unwrap_or(DEFAULT_MAX_ROWS);
    let mut manifest_entries = Vec::new();
    let mut all_files = Vec::new();

    for (idx, (schema, table)) in tables.iter().enumerate() {
        progress.send(ProgressEvent::TableProgress {
            schema: schema.clone(),
            table: table.clone(),
            index: idx,
            total_tables: tables.len(),
        });

        let table_dir = base_dir.join(format!("{schema}.{table}"));
        std::fs::create_dir_all(&table_dir)?;

        let (files, row_count, columns) = match &options.hive_partitioning {
            HivePartitioning::None => {
                export_table_flat(&client, schema, table, &table_dir, max_rows).await?
            }
            HivePartitioning::Year { column } => {
                export_table_hive(&client, schema, table, &table_dir, column, false, max_rows)
                    .await?
            }
            HivePartitioning::YearMonth { column } => {
                export_table_hive(&client, schema, table, &table_dir, column, true, max_rows)
                    .await?
            }
        };

        let relative_files: Vec<String> = files
            .iter()
            .filter_map(|f| f.strip_prefix(&base_dir).ok())
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        manifest_entries.push(ManifestEntry {
            schema: schema.clone(),
            table: table.clone(),
            columns,
            files: relative_files,
            row_count,
        });

        all_files.extend(files);
    }

    write_manifest(&base_dir, &manifest_entries)?;

    let result = match options.storage_mode {
        StorageMode::Archive => {
            let archive_path = tar_gz_directory(&base_dir)?;
            let _ = std::fs::remove_dir_all(&base_dir);
            ExportResult {
                mode: StorageMode::Archive,
                archive_path: Some(archive_path),
                individual_files: Vec::new(),
                base_dir: base_dir.clone(),
                db_name: db_name.clone(),
            }
        }
        StorageMode::Individual => {
            all_files.push(base_dir.join("_manifest.json"));
            ExportResult {
                mode: StorageMode::Individual,
                archive_path: None,
                individual_files: all_files,
                base_dir: base_dir.clone(),
                db_name: db_name.clone(),
            }
        }
    };

    progress.send(ProgressEvent::PhaseCompleted {
        phase: Phase::Exporting,
    });

    Ok(result)
}

async fn resolve_tables(
    client: &tokio_postgres::Client,
    schemas: &[String],
    tables: &[String],
) -> Result<Vec<(String, String)>> {
    if !tables.is_empty() {
        return Ok(tables
            .iter()
            .map(|t| {
                let parts: Vec<&str> = t.splitn(2, '.').collect();
                if parts.len() == 2 {
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    ("public".to_string(), parts[0].to_string())
                }
            })
            .collect());
    }

    let effective_schemas = if schemas.is_empty() {
        vec!["public".to_string()]
    } else {
        schemas.to_vec()
    };

    let rows = client
        .query(
            "SELECT table_schema, table_name \
             FROM information_schema.tables \
             WHERE table_schema = ANY($1) AND table_type = 'BASE TABLE' \
             ORDER BY table_schema, table_name",
            &[&effective_schemas],
        )
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| {
            let s: String = row.get(0);
            let t: String = row.get(1);
            (s, t)
        })
        .collect())
}

async fn get_table_schema(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> Result<(Arc<Schema>, Vec<String>, Vec<ManifestColumn>)> {
    let rows = client
        .query(
            "SELECT column_name, udt_name \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    let mut fields = Vec::new();
    let mut pg_types = Vec::new();
    let mut manifest_columns = Vec::new();
    for row in &rows {
        let col_name: String = row.get(0);
        let udt_name: String = row.get(1);
        let arrow_type = pg_type_to_arrow(&udt_name);
        manifest_columns.push(ManifestColumn {
            name: col_name.clone(),
            pg_type: udt_name.clone(),
            arrow_type: format!("{arrow_type:?}"),
        });
        fields.push(Field::new(&col_name, arrow_type, true));
        pg_types.push(udt_name);
    }

    Ok((Arc::new(Schema::new(fields)), pg_types, manifest_columns))
}

fn pg_type_to_arrow(udt_name: &str) -> DataType {
    match udt_name {
        "bool" => DataType::Boolean,
        "int2" => DataType::Int16,
        "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "numeric" | "money" => DataType::Utf8,
        "text" | "varchar" | "bpchar" | "name" | "citext" => DataType::Utf8,
        "bytea" => DataType::Binary,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "json" | "jsonb" => DataType::Utf8,
        "uuid" => DataType::Utf8,
        "inet" | "cidr" | "macaddr" | "macaddr8" => DataType::Utf8,
        "interval" => DataType::Utf8,
        "time" | "timetz" => DataType::Utf8,
        "oid" => DataType::UInt32,
        "_text" | "_int4" | "_int8" | "_float8" | "_varchar" | "_bool" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

fn rows_to_record_batch(
    rows: &[tokio_postgres::Row],
    arrow_schema: &Arc<Schema>,
    pg_types: &[String],
) -> Result<RecordBatch> {
    let num_cols = arrow_schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = arrow_schema.field(col_idx);
        let pg_type = &pg_types[col_idx];
        let array = build_column_array(rows, col_idx, field.data_type(), pg_type)?;
        columns.push(array);
    }

    RecordBatch::try_new(arrow_schema.clone(), columns)
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to create RecordBatch: {e}")))
}

fn build_column_array(
    rows: &[tokio_postgres::Row],
    col_idx: usize,
    data_type: &DataType,
    pg_type: &str,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                let val: Option<bool> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i16> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i32> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<i64> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt32 => {
            let mut builder = UInt32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<u32> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<f32> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<f64> = row.try_get(col_idx).ok().flatten();
                builder.append_option(val);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(rows.len());
            for row in rows {
                let val: Option<NaiveDate> = row.try_get(col_idx).ok().flatten();
                match val {
                    Some(d) => {
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let days = (d - epoch).num_days() as i32;
                        builder.append_value(days);
                    }
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                let val: Option<NaiveDateTime> = row.try_get(col_idx).ok().flatten();
                match val {
                    Some(ts) => {
                        builder.append_value(ts.and_utc().timestamp_micros());
                    }
                    None => builder.append_null(),
                }
            }
            let arr = builder.finish();
            if matches!(pg_type, "timestamptz") {
                Ok(Arc::new(arr.with_timezone("UTC")))
            } else {
                Ok(Arc::new(arr))
            }
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(rows.len(), 256);
            for row in rows {
                let val: Option<Vec<u8>> = row.try_get(col_idx).ok().flatten();
                match val {
                    Some(b) => builder.append_value(&b),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 | _ => {
            let mut builder = StringBuilder::with_capacity(rows.len(), 64);
            for row in rows {
                let val: Option<String> = row_to_string(row, col_idx, pg_type);
                match val {
                    Some(s) => builder.append_value(&s),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

fn row_to_string(row: &tokio_postgres::Row, col_idx: usize, pg_type: &str) -> Option<String> {
    match pg_type {
        "bool" => row.try_get::<_, Option<bool>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "int2" => row.try_get::<_, Option<i16>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "int4" => row.try_get::<_, Option<i32>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "int8" => row.try_get::<_, Option<i64>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "float4" => row.try_get::<_, Option<f32>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "float8" => row.try_get::<_, Option<f64>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "oid" => row.try_get::<_, Option<u32>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "date" => row.try_get::<_, Option<NaiveDate>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "timestamp" => row.try_get::<_, Option<NaiveDateTime>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "timestamptz" => row.try_get::<_, Option<NaiveDateTime>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "json" | "jsonb" => row.try_get::<_, Option<serde_json::Value>>(col_idx).ok().flatten().map(|v| v.to_string()),
        "bytea" => row.try_get::<_, Option<Vec<u8>>>(col_idx).ok().flatten().map(|v| {
            v.iter().fold(String::with_capacity(v.len() * 2), |mut s, b| {
                use std::fmt::Write;
                let _ = write!(s, "{b:02x}");
                s
            })
        }),
        _ => row.try_get::<_, Option<String>>(col_idx).ok().flatten(),
    }
}

fn write_parquet_file(
    path: &Path,
    batch: &RecordBatch,
) -> Result<()> {
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to create writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to write batch: {e}")))?;
    writer
        .close()
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to close writer: {e}")))?;
    Ok(())
}

async fn export_table_flat(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    output_dir: &Path,
    max_rows: u64,
) -> Result<(Vec<PathBuf>, u64, Vec<ManifestColumn>)> {
    let (arrow_schema, pg_types, manifest_columns) = get_table_schema(client, schema, table).await?;

    let quoted = format!("\"{}\".\"{}\"", schema, table);
    let count_row = client
        .query_one(&format!("SELECT COUNT(*) FROM {quoted}"), &[])
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
    let total_rows: i64 = count_row.get(0);

    if total_rows == 0 {
        let path = output_dir.join("part-00000.parquet");
        let batch = RecordBatch::new_empty(arrow_schema);
        write_parquet_file(&path, &batch)?;
        return Ok((vec![path], 0, manifest_columns));
    }

    let mut files = Vec::new();
    let mut offset: u64 = 0;
    let mut part_num: u32 = 0;

    while offset < total_rows as u64 {
        let query = format!(
            "SELECT * FROM {quoted} LIMIT {max_rows} OFFSET {offset}"
        );
        let rows = client
            .query(&query, &[])
            .await
            .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

        if rows.is_empty() {
            break;
        }

        let batch = rows_to_record_batch(&rows, &arrow_schema, &pg_types)?;
        let path = output_dir.join(format!("part-{part_num:05}.parquet"));
        write_parquet_file(&path, &batch)?;
        files.push(path);

        offset += rows.len() as u64;
        part_num += 1;
    }

    Ok((files, total_rows as u64, manifest_columns))
}

async fn export_table_hive(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    output_dir: &Path,
    column: &str,
    include_month: bool,
    max_rows: u64,
) -> Result<(Vec<PathBuf>, u64, Vec<ManifestColumn>)> {
    let (arrow_schema, pg_types, manifest_columns) = get_table_schema(client, schema, table).await?;

    let quoted_table = format!("\"{}\".\"{}\"", schema, table);
    let quoted_col = format!("\"{}\"", column);

    let partition_query = if include_month {
        format!(
            "SELECT DISTINCT EXTRACT(YEAR FROM {quoted_col})::int AS yr, \
             EXTRACT(MONTH FROM {quoted_col})::int AS mo \
             FROM {quoted_table} WHERE {quoted_col} IS NOT NULL \
             ORDER BY yr, mo"
        )
    } else {
        format!(
            "SELECT DISTINCT EXTRACT(YEAR FROM {quoted_col})::int AS yr \
             FROM {quoted_table} WHERE {quoted_col} IS NOT NULL \
             ORDER BY yr"
        )
    };

    let partition_rows = client
        .query(&partition_query, &[])
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

    let mut all_files = Vec::new();
    let mut total_row_count: u64 = 0;

    for prow in &partition_rows {
        let year: i32 = prow.get(0);
        let (partition_dir, filter) = if include_month {
            let month: i32 = prow.get(1);
            let dir = output_dir.join(format!("year={year}")).join(format!("month={month:02}"));
            let filter = format!(
                "EXTRACT(YEAR FROM {quoted_col})::int = {year} \
                 AND EXTRACT(MONTH FROM {quoted_col})::int = {month}"
            );
            (dir, filter)
        } else {
            let dir = output_dir.join(format!("year={year}"));
            let filter = format!("EXTRACT(YEAR FROM {quoted_col})::int = {year}");
            (dir, filter)
        };

        std::fs::create_dir_all(&partition_dir)?;

        let count_query = format!("SELECT COUNT(*) FROM {quoted_table} WHERE {filter}");
        let count_row = client
            .query_one(&count_query, &[])
            .await
            .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
        let partition_count: i64 = count_row.get(0);
        total_row_count += partition_count as u64;

        let mut offset: u64 = 0;
        let mut part_num: u32 = 0;

        while offset < partition_count as u64 {
            let query = format!(
                "SELECT * FROM {quoted_table} WHERE {filter} LIMIT {max_rows} OFFSET {offset}"
            );
            let rows = client
                .query(&query, &[])
                .await
                .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

            if rows.is_empty() {
                break;
            }

            let batch = rows_to_record_batch(&rows, &arrow_schema, &pg_types)?;
            let path = partition_dir.join(format!("part-{part_num:05}.parquet"));
            write_parquet_file(&path, &batch)?;
            all_files.push(path);

            offset += rows.len() as u64;
            part_num += 1;
        }
    }

    // Handle NULL partition values
    let null_count_query =
        format!("SELECT COUNT(*) FROM {quoted_table} WHERE {quoted_col} IS NULL");
    let null_count_row = client
        .query_one(&null_count_query, &[])
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
    let null_count: i64 = null_count_row.get(0);

    if null_count > 0 {
        let null_dir = output_dir.join("__HIVE_DEFAULT_PARTITION__");
        std::fs::create_dir_all(&null_dir)?;
        total_row_count += null_count as u64;

        let mut offset: u64 = 0;
        let mut part_num: u32 = 0;

        while offset < null_count as u64 {
            let query = format!(
                "SELECT * FROM {quoted_table} WHERE {quoted_col} IS NULL \
                 LIMIT {max_rows} OFFSET {offset}"
            );
            let rows = client
                .query(&query, &[])
                .await
                .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

            if rows.is_empty() {
                break;
            }

            let batch = rows_to_record_batch(&rows, &arrow_schema, &pg_types)?;
            let path = null_dir.join(format!("part-{part_num:05}.parquet"));
            write_parquet_file(&path, &batch)?;
            all_files.push(path);

            offset += rows.len() as u64;
            part_num += 1;
        }
    }

    Ok((all_files, total_row_count, manifest_columns))
}

fn write_manifest(base_dir: &Path, entries: &[ManifestEntry]) -> Result<()> {
    let manifest = Manifest {
        version: 1,
        created_at: chrono::Utc::now().to_rfc3339(),
        tables: entries.to_vec(),
    };
    let json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to serialize manifest: {e}")))?;
    std::fs::write(base_dir.join("_manifest.json"), json)?;
    Ok(())
}

pub fn tar_gz_directory(dir: &Path) -> Result<PathBuf> {
    let archive_path = dir.with_extension("tar.gz");
    let file = File::create(&archive_path)?;
    let gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut tar_builder = tar::Builder::new(gz);

    let dir_name = dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("export");

    tar_builder
        .append_dir_all(dir_name, dir)
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to create tar archive: {e}")))?;
    tar_builder
        .into_inner()
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to finish tar: {e}")))?
        .finish()
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to finish gzip: {e}")))?;

    Ok(archive_path)
}
