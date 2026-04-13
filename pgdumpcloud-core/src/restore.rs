use crate::error::{PgDumpCloudError, Result};
use crate::progress::{Phase, ProgressEvent, ProgressSender};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreOptions {
    pub database_url: String,
    pub clean: bool,
    pub no_owner: bool,
    pub no_acl: bool,
    pub if_exists: bool,
    pub data_only: bool,
}

impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            clean: false,
            no_owner: true,
            no_acl: true,
            if_exists: false,
            data_only: false,
        }
    }
}

pub fn check_pg_restore() -> Result<String> {
    let output = Command::new("pg_restore").arg("--version").output();
    match output {
        Ok(o) if o.status.success() => {
            Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
        }
        _ => Err(PgDumpCloudError::BinaryNotFound("pg_restore".into())),
    }
}

pub fn check_psql() -> Result<String> {
    let output = Command::new("psql").arg("--version").output();
    match output {
        Ok(o) if o.status.success() => {
            Ok(String::from_utf8_lossy(&o.stdout).trim().to_string())
        }
        _ => Err(PgDumpCloudError::BinaryNotFound("psql".into())),
    }
}

/// Applies a companion `.types.sql` file via `psql` before the main restore.
/// The SQL uses `EXCEPTION WHEN duplicate_object` wrappers so it's safe to
/// run even if the types already exist.
pub fn apply_types_sql(types_path: &Path, database_url: &str) -> Result<()> {
    check_psql()?;

    let mut cmd = Command::new("psql");
    cmd.arg(database_url);
    cmd.args(["-f", types_path.to_str().unwrap_or("")]);
    cmd.arg("--no-psqlrc");

    let output = cmd.output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PgDumpCloudError::Restore(
            format!("Types SQL apply failed: {stderr}"),
        ));
    }

    Ok(())
}

fn is_plain_sql(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    name.ends_with(".sql")
}

pub fn run_restore(
    backup_path: &Path,
    options: &RestoreOptions,
    progress: &dyn ProgressSender,
) -> Result<()> {
    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Restoring,
    });

    let result = if is_plain_sql(backup_path) {
        check_psql()?;
        run_psql_restore(backup_path, options)
    } else {
        check_pg_restore()?;
        run_pg_restore(backup_path, options, progress)
    };

    match &result {
        Ok(()) => {
            progress.send(ProgressEvent::PhaseCompleted {
                phase: Phase::Restoring,
            });
        }
        Err(e) => {
            progress.send(ProgressEvent::Error {
                message: e.to_string(),
            });
        }
    }

    result
}

fn run_pg_restore(backup_path: &Path, options: &RestoreOptions, progress: &dyn ProgressSender) -> Result<()> {
    let mut cmd = Command::new("pg_restore");
    cmd.arg(format!("--dbname={}", options.database_url));

    if options.data_only {
        cmd.arg("--data-only");
    } else if options.clean {
        cmd.arg("--clean");
        cmd.arg("--if-exists");
    }
    if options.no_owner {
        cmd.arg("--no-owner");
    }
    if options.no_acl {
        cmd.arg("--no-acl");
    }
    if !options.data_only && options.if_exists {
        cmd.arg("--if-exists");
    }

    cmd.arg(backup_path);

    let output = cmd.output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("errors ignored on restore") {
            progress.send(ProgressEvent::Error {
                message: format!("Restore completed with warnings: {stderr}"),
            });
            return Ok(());
        }
        return Err(PgDumpCloudError::Restore(stderr.to_string()));
    }

    Ok(())
}

fn run_psql_restore(backup_path: &Path, options: &RestoreOptions) -> Result<()> {
    let mut cmd = Command::new("psql");
    cmd.arg(&options.database_url);
    cmd.args(["-f", backup_path.to_str().unwrap_or("")]);

    if options.no_owner {
        cmd.arg("--no-psqlrc");
    }

    let output = cmd.output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PgDumpCloudError::Restore(stderr.to_string()));
    }

    Ok(())
}

/// Spawns the restore command as a child process with `--verbose` so the
/// caller can read stderr lines for per-table progress.
/// Returns the child process; the caller is responsible for waiting and
/// checking exit status.
pub fn spawn_restore(
    backup_path: &Path,
    options: &RestoreOptions,
    progress: &dyn ProgressSender,
) -> Result<std::process::Child> {
    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Restoring,
    });

    if is_plain_sql(backup_path) {
        check_psql()?;
        let mut cmd = Command::new("psql");
        cmd.arg(&options.database_url);
        cmd.args(["-f", backup_path.to_str().unwrap_or("")]);

        if options.no_owner {
            cmd.arg("--no-psqlrc");
        }

        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());

        let child = cmd.spawn()?;
        Ok(child)
    } else {
        check_pg_restore()?;
        let mut cmd = Command::new("pg_restore");
        cmd.arg(format!("--dbname={}", options.database_url));
        cmd.arg("--verbose");

        if options.data_only {
            cmd.arg("--data-only");
        } else if options.clean {
            cmd.arg("--clean");
            cmd.arg("--if-exists");
        }
        if options.no_owner {
            cmd.arg("--no-owner");
        }
        if options.no_acl {
            cmd.arg("--no-acl");
        }
        if !options.data_only && options.if_exists {
            cmd.arg("--if-exists");
        }

        cmd.arg(backup_path);
        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::piped());

        let child = cmd.spawn()?;
        Ok(child)
    }
}
