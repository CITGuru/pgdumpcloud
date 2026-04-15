use crate::connection;
use crate::error::{PgDumpCloudError, Result};
use crate::introspect;
use crate::progress::{Phase, ProgressEvent, ProgressSender};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use futures_util::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_MAX_ROWS: u64 = 500_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FetchStrategy {
    Cursor,
    Copy,
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
    pub fetch_strategy: FetchStrategy,
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
                export_table_flat(&client, schema, table, &table_dir, max_rows, &options.fetch_strategy).await?
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

/// Fetch rows via a server-side cursor and write Parquet part files into `output_dir`.
/// Returns the list of files written and the total row count.
async fn fetch_cursor_to_parquet(
    client: &tokio_postgres::Client,
    cursor_name: &str,
    cursor_query: &str,
    output_dir: &Path,
    max_rows: u64,
    arrow_schema: &Arc<Schema>,
    pg_types: &[String],
) -> Result<(Vec<PathBuf>, u64)> {
    client
        .batch_execute("BEGIN")
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

    let declare = format!(
        "DECLARE {cursor_name} NO SCROLL CURSOR FOR {cursor_query}"
    );
    client
        .batch_execute(&declare)
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

    let fetch_sql = format!("FETCH {max_rows} FROM {cursor_name}");
    let mut files = Vec::new();
    let mut total_rows: u64 = 0;
    let mut part_num: u32 = 0;

    loop {
        let rows = client
            .query(&fetch_sql, &[])
            .await
            .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

        if rows.is_empty() {
            break;
        }

        total_rows += rows.len() as u64;
        let batch = rows_to_record_batch(&rows, arrow_schema, pg_types)?;
        let path = output_dir.join(format!("part-{part_num:05}.parquet"));
        write_parquet_file(&path, &batch)?;
        files.push(path);
        part_num += 1;
    }

    client
        .batch_execute(&format!("CLOSE {cursor_name}"))
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
    client
        .batch_execute("ROLLBACK")
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

    if files.is_empty() {
        let path = output_dir.join("part-00000.parquet");
        let batch = RecordBatch::new_empty(arrow_schema.clone());
        write_parquet_file(&path, &batch)?;
        files.push(path);
    }

    Ok((files, total_rows))
}

async fn copy_text_to_parquet(
    client: &tokio_postgres::Client,
    quoted_table: &str,
    output_dir: &Path,
    max_rows: u64,
    arrow_schema: &Arc<Schema>,
    pg_types: &[String],
) -> Result<(Vec<PathBuf>, u64)> {
    client
        .batch_execute("SET timezone = 'UTC'")
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

    let copy_query = format!("COPY {quoted_table} TO STDOUT (FORMAT text)");
    let stream = client
        .copy_out(copy_query.as_str())
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

    tokio::pin!(stream);

    let num_cols = arrow_schema.fields().len();
    let mut buf = Vec::new();
    let mut row_buf: Vec<Vec<Option<String>>> = Vec::new();
    let mut files = Vec::new();
    let mut total_rows: u64 = 0;
    let mut part_num: u32 = 0;
    let max = max_rows as usize;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result
            .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
        buf.extend_from_slice(&chunk);

        let mut start = 0;
        while let Some(rel_pos) = buf[start..].iter().position(|&b| b == b'\n') {
            let end = start + rel_pos;
            let line = std::str::from_utf8(&buf[start..end])
                .map_err(|e| PgDumpCloudError::ParquetExport(format!("Invalid UTF-8 in COPY stream: {e}")))?;

            let fields: Vec<Option<String>> = line
                .split('\t')
                .take(num_cols)
                .map(|f| {
                    if f == "\\N" {
                        None
                    } else {
                        Some(unescape_copy_text(f))
                    }
                })
                .collect();
            row_buf.push(fields);
            start = end + 1;

            if row_buf.len() >= max {
                let batch = text_rows_to_record_batch(&row_buf, arrow_schema, pg_types)?;
                let path = output_dir.join(format!("part-{part_num:05}.parquet"));
                write_parquet_file(&path, &batch)?;
                files.push(path);
                total_rows += row_buf.len() as u64;
                row_buf.clear();
                part_num += 1;
            }
        }

        if start > 0 {
            buf.drain(..start);
        }
    }

    if !row_buf.is_empty() {
        let batch = text_rows_to_record_batch(&row_buf, arrow_schema, pg_types)?;
        let path = output_dir.join(format!("part-{part_num:05}.parquet"));
        write_parquet_file(&path, &batch)?;
        files.push(path);
        total_rows += row_buf.len() as u64;
    }

    if files.is_empty() {
        let path = output_dir.join("part-00000.parquet");
        let batch = RecordBatch::new_empty(arrow_schema.clone());
        write_parquet_file(&path, &batch)?;
        files.push(path);
    }

    Ok((files, total_rows))
}

fn unescape_copy_text(s: &str) -> String {
    if !s.contains('\\') {
        return s.to_string();
    }
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('r') => result.push('\r'),
                Some('\\') => result.push('\\'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn text_rows_to_record_batch(
    rows: &[Vec<Option<String>>],
    arrow_schema: &Arc<Schema>,
    pg_types: &[String],
) -> Result<RecordBatch> {
    let num_cols = arrow_schema.fields().len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let field = arrow_schema.field(col_idx);
        let pg_type = &pg_types[col_idx];
        let col_values: Vec<Option<&str>> = rows
            .iter()
            .map(|row| row.get(col_idx).and_then(|v| v.as_deref()))
            .collect();
        let array = parse_text_column(&col_values, field.data_type(), pg_type)?;
        columns.push(array);
    }

    RecordBatch::try_new(arrow_schema.clone(), columns)
        .map_err(|e| PgDumpCloudError::ParquetExport(format!("Failed to create RecordBatch: {e}")))
}

fn parse_text_column(
    values: &[Option<&str>],
    data_type: &DataType,
    _pg_type: &str,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    Some("t") => b.append_value(true),
                    Some("f") => b.append_value(false),
                    _ => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| s.parse::<i16>().ok()) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| s.parse::<i32>().ok()) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| s.parse::<i64>().ok()) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt32 => {
            let mut b = UInt32Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| s.parse::<u32>().ok()) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| s.parse::<f32>().ok()) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| s.parse::<f64>().ok()) {
                    Some(n) => b.append_value(n),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Date32 => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let mut b = Date32Builder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()) {
                    Some(d) => b.append_value((d - epoch).num_days() as i32),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(values.len());
            for v in values {
                match v.and_then(|s| parse_pg_timestamp(s)) {
                    Some(ts) => b.append_value(ts.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
            let arr = b.finish();
            if tz.is_some() {
                Ok(Arc::new(arr.with_timezone("UTC")))
            } else {
                Ok(Arc::new(arr))
            }
        }
        DataType::Binary => {
            let mut b = BinaryBuilder::with_capacity(values.len(), 256);
            for v in values {
                match v {
                    Some(s) => {
                        let hex_str = s.strip_prefix("\\x").unwrap_or(s);
                        let bytes: Vec<u8> = (0..hex_str.len())
                            .step_by(2)
                            .filter_map(|i| hex_str.get(i..i + 2).and_then(|h| u8::from_str_radix(h, 16).ok()))
                            .collect();
                        b.append_value(&bytes);
                    }
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        _ => {
            let mut b = StringBuilder::with_capacity(values.len(), 64);
            for v in values {
                match v {
                    Some(s) => b.append_value(s),
                    None => b.append_null(),
                }
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

/// Parse Postgres text-format timestamp, stripping any timezone suffix.
fn parse_pg_timestamp(s: &str) -> Option<NaiveDateTime> {
    let base = if let Some(pos) = s.rfind('+') {
        if pos > 10 { &s[..pos] } else { s }
    } else if let Some(pos) = s.rfind('-') {
        if pos > 10 { &s[..pos] } else { s }
    } else {
        s
    };
    NaiveDateTime::parse_from_str(base, "%Y-%m-%d %H:%M:%S%.f")
        .ok()
        .or_else(|| NaiveDateTime::parse_from_str(base, "%Y-%m-%d %H:%M:%S").ok())
}

async fn export_table_flat(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
    output_dir: &Path,
    max_rows: u64,
    strategy: &FetchStrategy,
) -> Result<(Vec<PathBuf>, u64, Vec<ManifestColumn>)> {
    let (arrow_schema, pg_types, manifest_columns) = get_table_schema(client, schema, table).await?;

    let quoted = format!("\"{}\".\"{}\"", schema, table);
    let (files, total_rows) = match strategy {
        FetchStrategy::Copy => {
            copy_text_to_parquet(
                client, &quoted, output_dir, max_rows, &arrow_schema, &pg_types,
            ).await?
        }
        FetchStrategy::Cursor => {
            let cursor_query = format!("SELECT * FROM {quoted}");
            fetch_cursor_to_parquet(
                client, "export_cur", &cursor_query, output_dir, max_rows,
                &arrow_schema, &pg_types,
            ).await?
        }
    };

    Ok((files, total_rows, manifest_columns))
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

        let cursor_query = format!("SELECT * FROM {quoted_table} WHERE {filter}");
        let (files, row_count) = fetch_cursor_to_parquet(
            client, "hive_cur", &cursor_query, &partition_dir, max_rows,
            &arrow_schema, &pg_types,
        ).await?;
        all_files.extend(files);
        total_row_count += row_count;
    }

    let null_cursor_query = format!(
        "SELECT * FROM {quoted_table} WHERE {quoted_col} IS NULL"
    );

    client
        .batch_execute("BEGIN")
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
    client
        .batch_execute(&format!(
            "DECLARE null_check_cur NO SCROLL CURSOR FOR {null_cursor_query}"
        ))
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
    let peek = client
        .query("FETCH 1 FROM null_check_cur", &[])
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;
    client
        .batch_execute("CLOSE null_check_cur; ROLLBACK")
        .await
        .map_err(|e| PgDumpCloudError::ParquetExport(e.to_string()))?;

    if !peek.is_empty() {
        let null_dir = output_dir.join("__HIVE_DEFAULT_PARTITION__");
        std::fs::create_dir_all(&null_dir)?;

        let (files, row_count) = fetch_cursor_to_parquet(
            client, "null_cur", &null_cursor_query, &null_dir, max_rows,
            &arrow_schema, &pg_types,
        ).await?;
        all_files.extend(files);
        total_row_count += row_count;
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
