# pgdumpcloud-core

PostgreSQL backup and restore to S3-compatible cloud storage. Supports standard pg_dump formats (custom, plain, tar) and direct-to-Parquet export with optional Hive partitioning.

## Installation

```bash
cargo install --path .
```

Requires `pg_dump` and `pg_restore` on your `PATH`. Run `pgdumpcloud doctor` to verify.

## Quick Start

```bash
# Back up a database to S3
pgdumpcloud backup \
  --url postgres://user:pass@localhost/mydb \
  --endpoint https://s3.us-east-1.amazonaws.com \
  --bucket my-backups \
  --access-key AKIA... \
  --secret-key wJal...

# Restore the latest backup
pgdumpcloud restore \
  --backup backup-mydb-20260415T120000.dump.gz \
  --endpoint https://s3.us-east-1.amazonaws.com \
  --bucket my-backups \
  --access-key AKIA... \
  --secret-key wJal... \
  --target-url postgres://user:pass@localhost/mydb_restored
```

## Global Options

| Flag | Description |
|------|-------------|
| `--config <path>` | Path to config file (default: `~/.config/pgdumpcloud/config.toml`) |

## Commands

### `backup`

Dump a PostgreSQL database and upload to S3-compatible storage.

```bash
pgdumpcloud backup [OPTIONS]
```

**Database connection** (one required):

| Flag | Description |
|------|-------------|
| `--url <url>` | Database URL (`postgres://...`) |
| `--connection <name>` | Saved connection name or ID from config file |

Falls back to the `DATABASE_URL` environment variable if neither is provided.

**Dump options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--format <fmt>` | `custom` | Dump format: `custom`, `plain`, `tar`, or `parquet` |
| `--compression <method>` | `gzip` | Compression: `gzip` or `none` |
| `--tables <t1,t2,...>` | | Comma-separated tables to include (`schema.table`) |
| `--schemas <s1,s2,...>` | | Comma-separated schemas to include |
| `--no-owner` | | Exclude owner statements from dump |
| `--no-acl` | | Exclude ACL/grant statements from dump |
| `--output-dir <path>` | system temp dir | Local directory for dump files |
| `--filename-prefix <pfx>` | `backup` | Prefix for the generated backup filename |
| `--streaming` | | Stream pg_dump directly to S3 without a local temp file (not compatible with `--format parquet`) |

**Storage target** (provide `--storage` or individual flags):

| Flag | Default | Description |
|------|---------|-------------|
| `--storage <name>` | | Saved storage target name or ID from config file |
| `--endpoint <url>` | | S3 endpoint URL |
| `--bucket <name>` | | S3 bucket name |
| `--access-key <key>` | | S3 access key ID |
| `--secret-key <key>` | | S3 secret access key |
| `--region <region>` | `us-east-1` | S3 region |
| `--prefix <prefix>` | | Remote key prefix for uploaded files |

Falls back to `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY` environment variables.

**Retention & cleanup:**

| Flag | Default | Description |
|------|---------|-------------|
| `--retention <n>` | `7` | Number of backups to retain; older ones are deleted |
| `--keep-local` | | Keep the local dump file after upload |

**Parquet-specific options** (apply when `--format parquet`):

| Flag | Default | Description |
|------|---------|-------------|
| `--storage-mode <mode>` | `archive` | `archive` bundles all tables into a `.tar.gz`; `individual` uploads one file per table |
| `--max-rows-per-file <n>` | | Split large tables across multiple Parquet files |
| `--partition-by <strategy>` | `none` | Hive partitioning: `none`, `year`, or `year-month` |
| `--partition-column <col>` | | Column to partition on (required when `--partition-by` is not `none`) |
| `--fetch-strategy <strat>` | `cursor` | Row fetch strategy: `cursor` (server-side cursor) or `copy` (COPY protocol) |

**Examples:**

```bash
# Basic backup with gzip compression
pgdumpcloud backup --url postgres://localhost/mydb \
  --storage my-s3-target

# Back up specific tables as plain SQL
pgdumpcloud backup --url postgres://localhost/mydb \
  --tables public.users,public.orders \
  --format plain --storage my-s3-target

# Export to Parquet with Hive partitioning by year
pgdumpcloud backup --url postgres://localhost/mydb \
  --format parquet \
  --storage-mode individual \
  --partition-by year \
  --partition-column created_at \
  --storage my-s3-target

# Stream a large database directly to S3 (no local temp file)
pgdumpcloud backup --url postgres://localhost/bigdb \
  --streaming --storage my-s3-target
```

### `restore`

Download a backup from S3 and restore it into a PostgreSQL database.

```bash
pgdumpcloud restore [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--backup <key>` | *(required)* | Remote key of the backup to restore |
| `--target-url <url>` | *(required)* | Target database URL |
| `--clean` | | Drop existing objects before restore |
| `--no-owner` | | Exclude owner statements |
| `--no-acl` | | Exclude ACL statements |
| `--data-only` | | Restore data only, skip schema creation |

Storage flags (`--storage`, `--endpoint`, `--bucket`, `--access-key`, `--secret-key`, `--region`) work the same as in `backup`.

Companion `.types.sql` files (containing enum type definitions) are automatically downloaded and applied before the main restore when available.

**Examples:**

```bash
# Restore using a saved storage target
pgdumpcloud restore \
  --backup backup-mydb-20260415T120000.dump.gz \
  --target-url postgres://localhost/mydb_copy \
  --storage my-s3-target

# Clean restore (drop + recreate objects)
pgdumpcloud restore \
  --backup backup-mydb-20260415T120000.dump.gz \
  --target-url postgres://localhost/mydb \
  --storage my-s3-target \
  --clean
```

### `introspect`

List schemas and tables in a database.

```bash
# List all schemas
pgdumpcloud introspect --url postgres://localhost/mydb

# List tables in a specific schema
pgdumpcloud introspect --url postgres://localhost/mydb --schema public
```

| Flag | Description |
|------|-------------|
| `--url <url>` | Database URL |
| `--connection <name>` | Saved connection name or ID |
| `--schema <name>` | Schema to list tables for (omit to list schemas) |

### `list-backups`

List backups stored on a remote storage target.

```bash
pgdumpcloud list-backups --storage my-s3-target
```

Storage flags (`--storage`, `--endpoint`, `--bucket`, `--access-key`, `--secret-key`, `--region`, `--prefix`) work the same as in `backup`.

### `doctor`

Check that required dependencies are available.

```bash
pgdumpcloud doctor
```

Checks for `pg_dump`, `pg_restore`, `psql`, and optionally `rclone`.

## Configuration File

The config file lives at `~/.config/pgdumpcloud/config.toml` (override with `--config`). It stores saved connections, storage targets, and default options so you don't have to pass them on every invocation.

```toml
[defaults]
dump_format = "custom"
compression = "gzip"
no_owner = false
no_acl = false
keep_local = false

[[connections]]
id = "prod"
name = "Production DB"
host = "db.example.com"
port = 5432
username = "backup_user"
password = "s3cret"
database = "myapp"
ssl_mode = "require"

[[storage]]
id = "my-s3-target"
name = "Primary S3"
provider = "s3"
endpoint = "https://s3.us-east-1.amazonaws.com"
bucket = "my-backups"
region = "us-east-1"
access_key = "AKIA..."
secret_key = "wJal..."
prefix = "daily/"
```

With a config file in place, commands simplify to:

```bash
pgdumpcloud backup --connection prod --storage my-s3-target
pgdumpcloud restore --backup backup-myapp-20260415.dump.gz \
  --target-url postgres://localhost/restore_test \
  --storage my-s3-target
```

## Environment Variables

| Variable | Used for |
|----------|----------|
| `DATABASE_URL` | Fallback database URL when `--url` and `--connection` are not provided |
| `S3_ENDPOINT` | Fallback S3 endpoint |
| `S3_BUCKET` | Fallback S3 bucket name |
| `S3_ACCESS_KEY` | Fallback S3 access key |
| `S3_SECRET_KEY` | Fallback S3 secret key |

## License

MIT
