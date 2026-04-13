use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Phase {
    Dumping,
    Compressing,
    Uploading,
    Downloading,
    Decompressing,
    Restoring,
    StreamingUpload,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Phase::Dumping => write!(f, "Dumping"),
            Phase::Compressing => write!(f, "Compressing"),
            Phase::Uploading => write!(f, "Uploading"),
            Phase::Downloading => write!(f, "Downloading"),
            Phase::Decompressing => write!(f, "Decompressing"),
            Phase::Restoring => write!(f, "Restoring"),
            Phase::StreamingUpload => write!(f, "Streaming to cloud"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProgressEvent {
    PhaseStarted { phase: Phase },
    Progress { phase: Phase, bytes: u64, total: Option<u64> },
    PhaseCompleted { phase: Phase },
    Error { message: String },
    Finished { message: String },
}

pub trait ProgressSender: Send + Sync {
    fn send(&self, event: ProgressEvent);
}

pub struct CliProgressSender;

impl ProgressSender for CliProgressSender {
    fn send(&self, event: ProgressEvent) {
        match &event {
            ProgressEvent::PhaseStarted { phase } => {
                eprintln!("[INFO] {phase} started...");
            }
            ProgressEvent::Progress { phase, bytes, total } => {
                if let Some(t) = total {
                    let pct = (*bytes as f64 / *t as f64 * 100.0) as u32;
                    eprint!("\r[INFO] {phase}: {bytes} / {t} bytes ({pct}%)    ");
                } else {
                    eprint!("\r[INFO] {phase}: {bytes} bytes    ");
                }
            }
            ProgressEvent::PhaseCompleted { phase } => {
                eprintln!("\n[OK] {phase} completed");
            }
            ProgressEvent::Error { message } => {
                eprintln!("[ERROR] {message}");
            }
            ProgressEvent::Finished { message } => {
                eprintln!("[DONE] {message}");
            }
        }
    }
}

pub struct NoopProgressSender;

impl ProgressSender for NoopProgressSender {
    fn send(&self, _event: ProgressEvent) {}
}

struct ThrottleState {
    last_emitted_bytes: u64,
    last_emitted_time: Instant,
    byte_interval: u64,
}

/// Wraps any `ProgressSender` and rate-limits `Progress` events based on
/// total file size. Phase transitions, errors, and finish always pass through.
pub struct ThrottledProgressSender<S: ProgressSender> {
    inner: S,
    state: Mutex<ThrottleState>,
}

impl<S: ProgressSender> ThrottledProgressSender<S> {
    pub fn new(inner: S, total_size: Option<u64>) -> Self {
        let byte_interval = total_size
            .map(|t| (t / 200).max(64 * 1024))
            .unwrap_or(256 * 1024);

        Self {
            inner,
            state: Mutex::new(ThrottleState {
                last_emitted_bytes: 0,
                last_emitted_time: Instant::now(),
                byte_interval,
            }),
        }
    }
}

impl<S: ProgressSender> ProgressSender for ThrottledProgressSender<S> {
    fn send(&self, event: ProgressEvent) {
        match &event {
            ProgressEvent::Progress { bytes, total, .. } => {
                let mut state = self.state.lock().unwrap();
                let bytes_delta = bytes.saturating_sub(state.last_emitted_bytes);
                let time_delta = state.last_emitted_time.elapsed();
                let is_final = total.map_or(false, |t| *bytes >= t);

                if is_final
                    || (bytes_delta >= state.byte_interval
                        && time_delta >= Duration::from_millis(100))
                {
                    state.last_emitted_bytes = *bytes;
                    state.last_emitted_time = Instant::now();
                    drop(state);
                    self.inner.send(event);
                }
            }
            _ => self.inner.send(event),
        }
    }
}
