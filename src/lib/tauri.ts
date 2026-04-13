import { invoke } from "@tauri-apps/api/core";
import type {
  ConnectionConfig,
  ConnectionInfo,
  ParsedConnection,
  StorageConfig,
  DatabaseInfo,
  SchemaInfo,
  TableInfo,
  BackupEntry,
  BackupRequest,
  RestoreRequest,
  Job,
  JobSummary,
  JobLogsResponse,
} from "./types";

export function buildConnectionUrl(conn: {
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
}): string {
  const host = conn.host || "localhost";
  const db = conn.database || "postgres";
  const passEnc = conn.password ? `:${encodeURIComponent(conn.password)}` : "";
  const userPart = conn.username
    ? `${encodeURIComponent(conn.username)}${passEnc}@`
    : "";
  return `postgres://${userPart}${host}:${conn.port}/${db}`;
}

export function buildUrlForDb(
  conn: { host: string; port: number; username: string; password: string },
  dbName: string
): string {
  const host = conn.host || "localhost";
  const passEnc = conn.password ? `:${encodeURIComponent(conn.password)}` : "";
  const userPart = conn.username
    ? `${encodeURIComponent(conn.username)}${passEnc}@`
    : "";
  return `postgres://${userPart}${host}:${conn.port}/${dbName}`;
}

export const api = {
  connections: {
    list: () => invoke<ConnectionConfig[]>("list_connections"),
    add: (connection: ConnectionConfig) =>
      invoke<void>("add_connection", { connection }),
    update: (connection: ConnectionConfig) =>
      invoke<void>("update_connection", { connection }),
    delete: (id: string) => invoke<void>("delete_connection", { id }),
    test: (url: string) =>
      invoke<ConnectionInfo>("test_connection_cmd", { url }),
    parseUrl: (url: string) =>
      invoke<ParsedConnection>("parse_connection_url", { url }),
    buildUrlForDb: (connectionId: string, database: string) =>
      invoke<string>("build_connection_url_for_db", { connectionId, database }),
  },

  storage: {
    list: () => invoke<StorageConfig[]>("list_storage_targets"),
    add: (target: StorageConfig) =>
      invoke<void>("add_storage_target", { target }),
    delete: (id: string) => invoke<void>("delete_storage_target", { id }),
    test: (
      endpoint: string,
      bucket: string,
      region: string,
      accessKey: string,
      secretKey: string
    ) =>
      invoke<string>("test_storage_cmd", {
        endpoint,
        bucket,
        region,
        accessKey,
        secretKey,
      }),
  },

  introspect: {
    databases: (url: string) =>
      invoke<DatabaseInfo[]>("list_databases", { url }),
    schemas: (url: string) => invoke<SchemaInfo[]>("list_schemas", { url }),
    tables: (url: string, schema: string) =>
      invoke<TableInfo[]>("list_tables", { url, schema }),
  },

  backup: {
    run: (request: BackupRequest) =>
      invoke<string>("run_backup", { request }),
    list: (
      endpoint: string,
      bucket: string,
      region: string,
      accessKey: string,
      secretKey: string,
      prefix: string
    ) =>
      invoke<BackupEntry[]>("list_backups", {
        endpoint,
        bucket,
        region,
        accessKey,
        secretKey,
        prefix,
      }),
  },

  restore: {
    run: (request: RestoreRequest) =>
      invoke<string>("run_restore", { request }),
  },

  jobs: {
    createBackup: (request: BackupRequest) =>
      invoke<string>("create_backup_job", { request }),
    createRestore: (request: RestoreRequest) =>
      invoke<string>("create_restore_job", { request }),
    list: () => invoke<JobSummary[]>("list_jobs"),
    get: (id: string) => invoke<Job>("get_job", { id }),
    getLogs: (id: string, offset: number, limit: number) =>
      invoke<JobLogsResponse>("get_job_logs", { id, offset, limit }),
    cancel: (id: string) => invoke<void>("cancel_job", { id }),
    delete: (id: string) => invoke<void>("delete_job", { id }),
    retry: (id: string) => invoke<string>("retry_job", { id }),
  },
};
