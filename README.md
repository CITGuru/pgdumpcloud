# PG Dump Cloud

A desktop app and CLI for backing up and restoring PostgreSQL databases directly to S3-compatible cloud storage. Built with Tauri, React, and Rust.

![PGDumpCloud](./assets/pgdumpcloud.png)

## Features

- **Streaming backups** — pipe `pg_dump` output straight to S3 without consuming local disk space, critical for large databases (100GB+)
- **Local backups** — traditional dump-to-file then upload, with multipart upload and progress tracking
- **Restore** — download backups from S3 and restore via `pg_restore` or `psql` with progress visibility
- **Multiple formats** — custom, plain SQL, and tar dump formats
- **Compression** — gzip compression with configurable levels (fast, default, best, none)
- **Database introspection** — browse schemas and tables, select specific objects to back up
- **Job management** — run multiple backup/restore jobs concurrently, monitor progress, cancel, retry
- **Connection & storage management** — save and reuse PostgreSQL connections and S3 storage targets
- **CLI** — headless operation for scripting and cron jobs


## Getting Started

### Prerequisites

- [Rust](https://rustup.rs/) (stable)
- [Bun](https://bun.sh/) (or Node.js)
- PostgreSQL client tools (`pg_dump`, `pg_restore`, `psql`) installed and on your `PATH`
- System dependencies for [Tauri 2](https://v2.tauri.app/start/prerequisites/)

### Development

```bash
bun install
bun tauri dev
```

### Production Build

```bash
bun tauri build
```

### CLI

The core library includes a standalone CLI binary:

```bash
cargo run -p pgdumpcloud-core --bin pgdumpcloud -- --help
```

#### CLI Examples

```bash
# Streaming backup to S3
pgdumpcloud backup --url postgres://user:pass@host/db \
  --streaming --compression gzip --format custom

# Restore from a backup
pgdumpcloud restore --url postgres://user:pass@host/db \
  --backup-key backups/db-2026-04-12.dump

# List backups in storage
pgdumpcloud list-backups

# Check pg_dump/pg_restore availability
pgdumpcloud doctor
```

## Configuration

The app stores config at `~/.config/pgdumpcloud/config.toml` (or the platform equivalent via `dirs::config_dir`). This includes saved connections, storage targets, and default backup options.

The CLI accepts `--config <path>` to use an alternate config file.

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Fallback PostgreSQL connection string |
| `R2_ENDPOINT` | Fallback S3 endpoint |
| `R2_BUCKET_NAME` | Fallback S3 bucket |
| `R2_ACCESS_KEY` | Fallback S3 access key |
| `R2_SECRET_KEY` | Fallback S3 secret key |

## License

MIT
