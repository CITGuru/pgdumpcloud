use chrono::Utc;
use pgdumpcloud_core::progress::{ProgressEvent, ProgressSender};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tauri::{AppHandle, Emitter};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobKind {
    Backup,
    Restore,
}

impl std::fmt::Display for JobKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobKind::Backup => write!(f, "Backup"),
            JobKind::Restore => write!(f, "Restore"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobStatus::Queued => write!(f, "Queued"),
            JobStatus::Running => write!(f, "Running"),
            JobStatus::Completed => write!(f, "Completed"),
            JobStatus::Failed => write!(f, "Failed"),
            JobStatus::Cancelled => write!(f, "Cancelled"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub kind: JobKind,
    pub status: JobStatus,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub progress: Option<ProgressEvent>,
    pub logs: Vec<LogEntry>,
    pub request: serde_json::Value,
    pub error: Option<String>,
    pub result: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSummary {
    pub id: String,
    pub kind: JobKind,
    pub status: JobStatus,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub result: Option<String>,
}

impl From<&Job> for JobSummary {
    fn from(job: &Job) -> Self {
        Self {
            id: job.id.clone(),
            kind: job.kind.clone(),
            status: job.status.clone(),
            created_at: job.created_at.clone(),
            started_at: job.started_at.clone(),
            finished_at: job.finished_at.clone(),
            error: job.error.clone(),
            result: job.result.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobEvent {
    pub job_id: String,
    pub event: ProgressEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStatusEvent {
    pub job_id: String,
    pub status: JobStatus,
}

pub struct JobManager {
    jobs: Mutex<HashMap<String, Job>>,
    cancel_tokens: Mutex<HashMap<String, CancellationToken>>,
    db_path: PathBuf,
}

impl JobManager {
    pub fn new(db_path: PathBuf) -> Self {
        let manager = Self {
            jobs: Mutex::new(HashMap::new()),
            cancel_tokens: Mutex::new(HashMap::new()),
            db_path,
        };
        manager.init_db();
        manager.load_history();
        manager
    }

    fn init_db(&self) {
        if let Some(parent) = self.db_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(conn) = Connection::open(&self.db_path) {
            let _ = conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request TEXT NOT NULL,
                    logs TEXT,
                    error TEXT,
                    result TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS job_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    message TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);",
            );
            self.migrate_legacy_logs(&conn);
        }
    }

    fn migrate_legacy_logs(&self, conn: &Connection) {
        let has_data: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE logs IS NOT NULL AND logs != '[]' AND logs != '')",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if !has_data {
            return;
        }

        let mut stmt = match conn.prepare("SELECT id, logs FROM jobs WHERE logs IS NOT NULL AND logs != '[]' AND logs != ''") {
            Ok(s) => s,
            Err(_) => return,
        };

        let rows: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .ok()
            .into_iter()
            .flatten()
            .flatten()
            .collect();

        for (job_id, logs_json) in &rows {
            let entries: Vec<LogEntry> = match serde_json::from_str(logs_json) {
                Ok(e) => e,
                Err(_) => continue,
            };
            for entry in &entries {
                let _ = conn.execute(
                    "INSERT INTO job_logs (job_id, timestamp, message) VALUES (?1, ?2, ?3)",
                    rusqlite::params![job_id, entry.timestamp, entry.message],
                );
            }
            let _ = conn.execute("UPDATE jobs SET logs = NULL WHERE id = ?1", [job_id]);
        }
    }

    fn load_history(&self) {
        let conn = match Connection::open(&self.db_path) {
            Ok(c) => c,
            Err(_) => return,
        };

        let mut stmt = match conn.prepare(
            "SELECT id, kind, status, request, error, result, created_at, started_at, finished_at FROM jobs ORDER BY created_at DESC",
        ) {
            Ok(s) => s,
            Err(_) => return,
        };

        let rows = match stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let kind_str: String = row.get(1)?;
            let status_str: String = row.get(2)?;
            let request_str: String = row.get(3)?;
            let error: Option<String> = row.get(4)?;
            let result: Option<String> = row.get(5)?;
            let created_at: String = row.get(6)?;
            let started_at: Option<String> = row.get(7)?;
            let finished_at: Option<String> = row.get(8)?;

            let kind = match kind_str.as_str() {
                "Restore" => JobKind::Restore,
                _ => JobKind::Backup,
            };
            let status = match status_str.as_str() {
                "Running" => JobStatus::Failed,
                "Queued" => JobStatus::Failed,
                "Completed" => JobStatus::Completed,
                "Cancelled" => JobStatus::Cancelled,
                _ => JobStatus::Failed,
            };
            let request: serde_json::Value =
                serde_json::from_str(&request_str).unwrap_or(serde_json::Value::Null);

            Ok(Job {
                id,
                kind,
                status,
                created_at,
                started_at,
                finished_at,
                progress: None,
                logs: Vec::new(),
                request,
                error,
                result,
            })
        }) {
            Ok(r) => r,
            Err(_) => return,
        };

        let mut jobs = self.jobs.lock().unwrap();
        for row in rows.flatten() {
            jobs.insert(row.id.clone(), row);
        }
    }

    pub fn create_job(&self, kind: JobKind, request: serde_json::Value) -> String {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();

        let job = Job {
            id: id.clone(),
            kind,
            status: JobStatus::Queued,
            created_at: now,
            started_at: None,
            finished_at: None,
            progress: None,
            logs: Vec::new(),
            request,
            error: None,
            result: None,
        };

        self.persist_job(&job);

        let mut jobs = self.jobs.lock().unwrap();
        jobs.insert(id.clone(), job);
        id
    }

    pub fn update_status(&self, id: &str, status: JobStatus) {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(id) {
            let now = Utc::now().to_rfc3339();
            match &status {
                JobStatus::Running => {
                    job.started_at = Some(now);
                }
                JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => {
                    job.finished_at = Some(now);
                }
                _ => {}
            }
            job.status = status;
            self.persist_job(job);
        }
    }

    pub fn set_error(&self, id: &str, error: String) {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(id) {
            job.error = Some(error);
            job.status = JobStatus::Failed;
            job.finished_at = Some(Utc::now().to_rfc3339());
            self.persist_job(job);
        }
    }

    pub fn set_result(&self, id: &str, result: String) {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(id) {
            job.result = Some(result);
            self.persist_job(job);
        }
    }

    pub fn append_log(&self, id: &str, message: &str) {
        let timestamp = Utc::now().to_rfc3339();
        {
            let mut jobs = self.jobs.lock().unwrap();
            if let Some(job) = jobs.get_mut(id) {
                job.logs.push(LogEntry {
                    timestamp: timestamp.clone(),
                    message: message.to_string(),
                });
            }
        }
        if let Ok(conn) = Connection::open(&self.db_path) {
            let _ = conn.execute(
                "INSERT INTO job_logs (job_id, timestamp, message) VALUES (?1, ?2, ?3)",
                rusqlite::params![id, timestamp, message],
            );
        }
    }

    pub fn update_progress(&self, id: &str, event: &ProgressEvent) {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(id) {
            job.progress = Some(event.clone());
        }
    }

    pub fn get_job(&self, id: &str) -> Option<Job> {
        let jobs = self.jobs.lock().unwrap();
        jobs.get(id).map(|job| Job {
            logs: Vec::new(),
            ..job.clone()
        })
    }

    pub fn list_jobs(&self) -> Vec<JobSummary> {
        let jobs = self.jobs.lock().unwrap();
        let mut summaries: Vec<JobSummary> = jobs.values().map(JobSummary::from).collect();
        summaries.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        summaries
    }

    pub fn delete_job(&self, id: &str) {
        let mut jobs = self.jobs.lock().unwrap();
        jobs.remove(id);
        drop(jobs);

        if let Ok(conn) = Connection::open(&self.db_path) {
            let _ = conn.execute("DELETE FROM job_logs WHERE job_id = ?1", [id]);
            let _ = conn.execute("DELETE FROM jobs WHERE id = ?1", [id]);
        }
    }

    pub fn get_job_request(&self, id: &str) -> Option<(JobKind, serde_json::Value)> {
        let jobs = self.jobs.lock().unwrap();
        jobs.get(id).map(|j| (j.kind.clone(), j.request.clone()))
    }

    pub fn create_cancel_token(&self, id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        let mut tokens = self.cancel_tokens.lock().unwrap();
        tokens.insert(id.to_string(), token.clone());
        token
    }

    pub fn cancel(&self, id: &str) {
        let tokens = self.cancel_tokens.lock().unwrap();
        if let Some(token) = tokens.get(id) {
            token.cancel();
        }
    }

    pub fn remove_cancel_token(&self, id: &str) {
        let mut tokens = self.cancel_tokens.lock().unwrap();
        tokens.remove(id);
    }

    pub fn get_status(&self, id: &str) -> Option<JobStatus> {
        let jobs = self.jobs.lock().unwrap();
        jobs.get(id).map(|j| j.status.clone())
    }

    pub fn get_job_logs(&self, id: &str, offset: u32, limit: u32) -> Vec<LogEntry> {
        let jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get(id) {
            if job.status == JobStatus::Running || job.status == JobStatus::Queued {
                let start = offset as usize;
                return job.logs.iter().skip(start).take(limit as usize).cloned().collect();
            }
        }
        drop(jobs);

        let conn = match Connection::open(&self.db_path) {
            Ok(c) => c,
            Err(_) => return Vec::new(),
        };

        let mut stmt = match conn.prepare(
            "SELECT timestamp, message FROM job_logs WHERE job_id = ?1 ORDER BY id ASC LIMIT ?2 OFFSET ?3",
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };

        stmt.query_map(rusqlite::params![id, limit, offset], |row| {
            Ok(LogEntry {
                timestamp: row.get(0)?,
                message: row.get(1)?,
            })
        })
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .collect()
    }

    pub fn get_job_log_count(&self, id: &str) -> u32 {
        let jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get(id) {
            if job.status == JobStatus::Running || job.status == JobStatus::Queued {
                return job.logs.len() as u32;
            }
        }
        drop(jobs);

        let conn = match Connection::open(&self.db_path) {
            Ok(c) => c,
            Err(_) => return 0,
        };

        conn.query_row(
            "SELECT COUNT(*) FROM job_logs WHERE job_id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap_or(0)
    }

    fn persist_job(&self, job: &Job) {
        let conn = match Connection::open(&self.db_path) {
            Ok(c) => c,
            Err(_) => return,
        };

        let _ = conn.execute(
            "INSERT OR REPLACE INTO jobs (id, kind, status, request, error, result, created_at, started_at, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                job.id,
                job.kind.to_string(),
                job.status.to_string(),
                serde_json::to_string(&job.request).unwrap_or_default(),
                job.error,
                job.result,
                job.created_at,
                job.started_at,
                job.finished_at,
            ],
        );
    }
}

pub struct JobProgressSender {
    pub job_id: String,
    pub app_handle: AppHandle,
    pub manager: Arc<JobManager>,
}

impl ProgressSender for JobProgressSender {
    fn send(&self, event: ProgressEvent) {
        self.manager.update_progress(&self.job_id, &event);

        let log_message = match &event {
            ProgressEvent::PhaseStarted { phase } => Some(format!("{phase} started")),
            ProgressEvent::Progress { .. } => None,
            ProgressEvent::PhaseCompleted { phase } => Some(format!("{phase} completed")),
            ProgressEvent::Error { message } => Some(format!("Error: {message}")),
            ProgressEvent::Finished { message } => Some(format!("Finished: {message}")),
        };
        if let Some(msg) = &log_message {
            self.manager.append_log(&self.job_id, msg);
        }

        let _ = self.app_handle.emit(
            "job:progress",
            JobEvent {
                job_id: self.job_id.clone(),
                event,
            },
        );
    }
}
