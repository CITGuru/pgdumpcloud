use crate::error::{PgDumpCloudError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub size_bytes: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub name: String,
    pub table_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub row_estimate: i64,
    pub size_bytes: i64,
    pub size_pretty: String,
}

pub(crate) async fn connect(url: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PG connection error: {e}");
        }
    });

    Ok(client)
}

pub async fn list_databases(url: &str) -> Result<Vec<DatabaseInfo>> {
    let client = connect(url).await?;

    let rows = client
        .query(
            "SELECT d.datname, pg_database_size(d.datname) as size_bytes \
             FROM pg_database d \
             WHERE d.datistemplate = false \
             ORDER BY d.datname",
            &[],
        )
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| DatabaseInfo {
            name: row.get(0),
            size_bytes: row.try_get(1).ok(),
        })
        .collect())
}

pub async fn list_schemas(url: &str) -> Result<Vec<SchemaInfo>> {
    let client = connect(url).await?;

    let rows = client
        .query(
            "SELECT s.schema_name, \
                    (SELECT count(*) FROM information_schema.tables t \
                     WHERE t.table_schema = s.schema_name AND t.table_type = 'BASE TABLE') as table_count \
             FROM information_schema.schemata s \
             WHERE s.schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
             ORDER BY s.schema_name",
            &[],
        )
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| SchemaInfo {
            name: row.get(0),
            table_count: row.get(1),
        })
        .collect())
}

/// Extracts all custom ENUM type definitions from the given schemas and returns
/// idempotent SQL statements that can be applied before a pg_restore to ensure
/// the types exist. When `schemas` is empty, defaults to `["public"]`.
pub async fn extract_enum_types(database_url: &str, schemas: &[String]) -> Result<String> {
    let client = connect(database_url).await?;

    let effective_schemas: Vec<String> = if schemas.is_empty() {
        vec!["public".to_string()]
    } else {
        schemas.to_vec()
    };

    let rows = client
        .query(
            "SELECT n.nspname, t.typname, \
                    string_agg(quote_literal(e.enumlabel), ', ' ORDER BY e.enumsortorder) \
             FROM pg_type t \
             JOIN pg_namespace n ON t.typnamespace = n.oid \
             JOIN pg_enum e ON e.enumtypid = t.oid \
             WHERE n.nspname = ANY($1) \
             GROUP BY n.nspname, t.typname \
             ORDER BY n.nspname, t.typname",
            &[&effective_schemas],
        )
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    if rows.is_empty() {
        return Ok(String::new());
    }

    let mut sql = String::from("-- Auto-generated enum type definitions\n");
    for row in &rows {
        let schema_name: String = row.get(0);
        let type_name: String = row.get(1);
        let labels: String = row.get(2);
        sql.push_str(&format!(
            "DO $$ BEGIN\n  CREATE TYPE \"{schema_name}\".\"{type_name}\" AS ENUM ({labels});\n\
             EXCEPTION WHEN duplicate_object THEN NULL;\nEND $$;\n\n"
        ));
    }

    Ok(sql)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

pub async fn list_datetime_columns(
    url: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let client = connect(url).await?;

    let rows = client
        .query(
            "SELECT column_name, udt_name \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
               AND udt_name IN ('timestamp', 'timestamptz', 'date') \
             ORDER BY ordinal_position",
            &[&schema, &table],
        )
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| ColumnInfo {
            name: row.get(0),
            data_type: row.get(1),
        })
        .collect())
}

pub async fn list_tables(url: &str, schema: &str) -> Result<Vec<TableInfo>> {
    let client = connect(url).await?;

    let rows = client
        .query(
            "SELECT t.table_schema, t.table_name, \
                    COALESCE(s.n_live_tup, 0) as row_estimate, \
                    COALESCE(pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)), 0) as size_bytes, \
                    COALESCE(pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))), '0 bytes') as size_pretty \
             FROM information_schema.tables t \
             LEFT JOIN pg_stat_user_tables s ON s.schemaname = t.table_schema AND s.relname = t.table_name \
             WHERE t.table_schema = $1 AND t.table_type = 'BASE TABLE' \
             ORDER BY t.table_name",
            &[&schema],
        )
        .await
        .map_err(|e| PgDumpCloudError::Connection(e.to_string()))?;

    Ok(rows
        .iter()
        .map(|row| TableInfo {
            schema: row.get(0),
            name: row.get(1),
            row_estimate: row.get(2),
            size_bytes: row.get(3),
            size_pretty: row.get(4),
        })
        .collect())
}
