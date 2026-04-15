use crate::error::{PgDumpCloudError, Result};
use crate::progress::{Phase, ProgressEvent, ProgressSender};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const BUF_SIZE: usize = 1024 * 1024;
const ASYNC_BUF_SIZE: usize = 1024 * 1024; // 1 MB for streaming encoder

pub fn compression_level(level: &str) -> Compression {
    match level {
        "fast" => Compression::fast(),
        "best" => Compression::best(),
        "none" => Compression::none(),
        _ => Compression::default(),
    }
}

/// An `AsyncRead` adapter that gzip-compresses data from an inner `AsyncRead`
/// on the fly. This avoids writing any intermediate file to disk.
pub struct AsyncGzipEncoder<R> {
    inner: R,
    encoder: Option<GzEncoder<Vec<u8>>>,
    read_buf: Vec<u8>,
    out_buf: Vec<u8>,
    out_pos: usize,
    finished: bool,
}

impl<R> AsyncGzipEncoder<R> {
    pub fn new(inner: R, level: Compression) -> Self {
        Self {
            inner,
            encoder: Some(GzEncoder::new(Vec::new(), level)),
            read_buf: vec![0u8; ASYNC_BUF_SIZE],
            out_buf: Vec::new(),
            out_pos: 0,
            finished: false,
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for AsyncGzipEncoder<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;

        let this = self.get_mut();

        // Drain any buffered compressed output first.
        if this.out_pos < this.out_buf.len() {
            let avail = &this.out_buf[this.out_pos..];
            let n = avail.len().min(buf.remaining());
            buf.put_slice(&avail[..n]);
            this.out_pos += n;
            if this.out_pos >= this.out_buf.len() {
                this.out_buf.clear();
                this.out_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        if this.finished {
            return Poll::Ready(Ok(()));
        }

        // Bounded loop: read from the inner source up to 16 times per
        // poll_read call. This lets gzip accumulate enough input to
        // produce a compressed block (usually 2-4 reads of ~64KB)
        // without the overhead of returning to the tokio scheduler
        // each time, and without spinning unboundedly.
        for _ in 0..16 {
            let mut read_buf_handle = tokio::io::ReadBuf::new(&mut this.read_buf);
            match std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut read_buf_handle) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let filled_len = read_buf_handle.filled().len();
                    if filled_len == 0 {
                        if let Some(enc) = this.encoder.take() {
                            let trailing = enc.finish()?;
                            if !trailing.is_empty() {
                                this.out_buf = trailing;
                                this.out_pos = 0;
                                this.finished = true;
                                let n = this.out_buf.len().min(buf.remaining());
                                buf.put_slice(&this.out_buf[..n]);
                                this.out_pos = n;
                                return Poll::Ready(Ok(()));
                            }
                        }
                        this.finished = true;
                        return Poll::Ready(Ok(()));
                    }

                    let enc = this.encoder.as_mut().expect("encoder already finished");
                    enc.write_all(&this.read_buf[..filled_len])?;
                    let compressed = enc.get_mut();
                    if !compressed.is_empty() {
                        this.out_buf = std::mem::take(compressed);
                        this.out_pos = 0;
                        let n = this.out_buf.len().min(buf.remaining());
                        buf.put_slice(&this.out_buf[..n]);
                        this.out_pos = n;
                        return Poll::Ready(Ok(()));
                    }
                    // No output yet — loop to feed more data.
                }
            }
        }

        // 16 reads with no output (very rare). Yield and retry.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

pub fn compress_gzip(
    input_path: &Path,
    level: Compression,
    progress: &dyn ProgressSender,
) -> Result<PathBuf> {
    let output_path = PathBuf::from(format!("{}.gz", input_path.display()));

    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Compressing,
    });

    let input_file = File::open(input_path)?;
    let total = input_file.metadata()?.len();
    let mut reader = std::io::BufReader::new(input_file);

    let output_file = File::create(&output_path)?;
    let mut encoder = GzEncoder::new(output_file, level);

    let mut buf = vec![0u8; BUF_SIZE];
    let mut written: u64 = 0;

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        encoder
            .write_all(&buf[..n])
            .map_err(|e| PgDumpCloudError::Compression(e.to_string()))?;
        written += n as u64;
        progress.send(ProgressEvent::Progress {
            phase: Phase::Compressing,
            bytes: written,
            total: Some(total),
        });
    }

    encoder
        .finish()
        .map_err(|e| PgDumpCloudError::Compression(e.to_string()))?;

    progress.send(ProgressEvent::PhaseCompleted {
        phase: Phase::Compressing,
    });

    Ok(output_path)
}

pub fn decompress_gzip(input_path: &Path, progress: &dyn ProgressSender) -> Result<PathBuf> {
    let output_path = input_path.with_extension("");

    progress.send(ProgressEvent::PhaseStarted {
        phase: Phase::Decompressing,
    });

    let input_file = File::open(input_path)?;
    let total = input_file.metadata()?.len();
    let mut decoder = GzDecoder::new(std::io::BufReader::new(input_file));

    let mut output_file = File::create(&output_path)?;
    let mut buf = vec![0u8; BUF_SIZE];
    let mut read_total: u64 = 0;

    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| PgDumpCloudError::Compression(e.to_string()))?;
        if n == 0 {
            break;
        }
        output_file.write_all(&buf[..n])?;
        read_total += n as u64;
        progress.send(ProgressEvent::Progress {
            phase: Phase::Decompressing,
            bytes: read_total,
            total: Some(total),
        });
    }

    progress.send(ProgressEvent::PhaseCompleted {
        phase: Phase::Decompressing,
    });

    Ok(output_path)
}
