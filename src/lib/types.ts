export interface ConnectionConfig {
  id: string;
  name: string;
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
  ssl_mode?: string;
}

export interface ParsedConnection {
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
}

export interface StorageConfig {
  id: string;
  name: string;
  provider: string;
  endpoint?: string;
  bucket?: string;
  region?: string;
  access_key?: string;
  secret_key?: string;
  prefix?: string;
  remote?: string;
  path?: string;
}

export interface ConnectionInfo {
  pg_version: string;
  latency_ms: number;
}

export interface DatabaseInfo {
  name: string;
  size_bytes: number | null;
}

export interface SchemaInfo {
  name: string;
  table_count: number;
}

export interface TableInfo {
  schema: string;
  name: string;
  row_estimate: number;
  size_bytes: number;
  size_pretty: string;
}

export interface BackupEntry {
  key: string;
  size: number;
  last_modified: string | null;
}

export type Phase =
  | "Dumping"
  | "Compressing"
  | "Uploading"
  | "Downloading"
  | "Decompressing"
  | "Restoring"
  | "StreamingUpload";

export type ProgressEvent =
  | { PhaseStarted: { phase: Phase } }
  | { Progress: { phase: Phase; bytes: number; total: number | null } }
  | { PhaseCompleted: { phase: Phase } }
  | { Error: { message: string } }
  | { Finished: { message: string } };

export interface BackupRequest {
  connection_url: string;
  format: string;
  compression: string;
  schemas: string[];
  tables: string[];
  no_owner: boolean;
  no_acl: boolean;
  storage_endpoint: string;
  storage_bucket: string;
  storage_region: string;
  storage_access_key: string;
  storage_secret_key: string;
  storage_prefix: string;
  filename_prefix: string;
  retention: number;
  keep_local: boolean;
  streaming: boolean;
}

export interface RestoreRequest {
  backup_key: string;
  target_url: string;
  clean: boolean;
  no_owner: boolean;
  no_acl: boolean;
  data_only: boolean;
  storage_endpoint: string;
  storage_bucket: string;
  storage_region: string;
  storage_access_key: string;
  storage_secret_key: string;
}

export type JobKind = "Backup" | "Restore";
export type JobStatus = "Queued" | "Running" | "Completed" | "Failed" | "Cancelled";

export interface LogEntry {
  timestamp: string;
  message: string;
}

export interface JobSummary {
  id: string;
  kind: JobKind;
  status: JobStatus;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
  result: string | null;
}

export interface Job extends JobSummary {
  progress: ProgressEvent | null;
  logs: LogEntry[];
  request: BackupRequest | RestoreRequest;
}

export interface JobLogsResponse {
  logs: LogEntry[];
  total: number;
}

export interface JobEvent {
  job_id: string;
  event: ProgressEvent;
}

export interface JobStatusEvent {
  job_id: string;
  status: JobStatus;
}
