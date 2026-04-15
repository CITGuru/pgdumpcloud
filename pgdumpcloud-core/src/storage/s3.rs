use super::{BackupEntry, CloudStorage};
use crate::error::{PgDumpCloudError, Result};
use crate::progress::{Phase, ProgressEvent, ProgressSender};
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use std::path::Path;
use tokio::io::AsyncReadExt;

pub struct S3Storage {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Storage {
    pub fn new(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        prefix: &str,
    ) -> Self {
        let creds = Credentials::new(access_key, secret_key, None, None, "pgdumpcloud");

        let config = S3ConfigBuilder::new()
            .behavior_version_latest()
            .endpoint_url(endpoint)
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(config);

        Self {
            client,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        }
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }
}

#[async_trait::async_trait]
impl CloudStorage for S3Storage {
    async fn upload(
        &self,
        local_path: &Path,
        remote_key: &str,
        progress: &dyn ProgressSender,
    ) -> Result<()> {
        let key = self.full_key(remote_key);
        let file_size = std::fs::metadata(local_path)?.len();

        progress.send(ProgressEvent::PhaseStarted {
            phase: Phase::Uploading,
        });

        // Small files (< 10 MB): single put_object
        if file_size < MIN_PART_SIZE as u64 {
            let body = ByteStream::from_path(local_path)
                .await
                .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(body)
                .send()
                .await
                .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

            progress.send(ProgressEvent::Progress {
                phase: Phase::Uploading,
                bytes: file_size,
                total: Some(file_size),
            });
            progress.send(ProgressEvent::PhaseCompleted {
                phase: Phase::Uploading,
            });
            return Ok(());
        }

        // Large files: multipart upload with per-part progress
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        let upload_id = create
            .upload_id()
            .ok_or_else(|| PgDumpCloudError::Storage("No upload_id returned".into()))?
            .to_string();

        let mut file = tokio::fs::File::open(local_path)
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        let chunk_size = choose_part_size(Some(file_size));
        let mut part_number: i32 = 1;
        let mut completed_parts: Vec<(i32, String)> = Vec::new();
        let mut uploaded_bytes: u64 = 0;
        let mut failed = false;
        let mut join_set = tokio::task::JoinSet::new();

        loop {
            let mut buf = vec![0u8; chunk_size];
            let mut buf_len: usize = 0;
            loop {
                let n = file
                    .read(&mut buf[buf_len..])
                    .await
                    .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;
                if n == 0 {
                    break;
                }
                buf_len += n;
                if buf_len >= chunk_size {
                    break;
                }
            }

            if buf_len == 0 {
                break;
            }

            while join_set.len() >= MAX_CONCURRENT_PARTS {
                match join_set.join_next().await {
                    Some(Ok(Ok((pn, etag, size)))) => {
                        completed_parts.push((pn, etag));
                        uploaded_bytes += size;
                        progress.send(ProgressEvent::Progress {
                            phase: Phase::Uploading,
                            bytes: uploaded_bytes,
                            total: Some(file_size),
                        });
                    }
                    Some(Ok(Err(e))) => {
                        failed = true;
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload failed: {e}"),
                        });
                        break;
                    }
                    Some(Err(e)) => {
                        failed = true;
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload task panicked: {e}"),
                        });
                        break;
                    }
                    None => break,
                }
            }

            if failed {
                break;
            }

            buf.truncate(buf_len);
            let bytes_in_part = buf_len as u64;
            let pn = part_number;
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let k = key.clone();
            let uid = upload_id.clone();

            join_set.spawn(async move {
                let body = ByteStream::from(buf);
                let resp = client
                    .upload_part()
                    .bucket(bucket)
                    .key(k)
                    .upload_id(uid)
                    .part_number(pn)
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

                let etag = resp
                    .e_tag()
                    .map(|s| s.to_string())
                    .ok_or_else(|| PgDumpCloudError::Storage("No ETag".into()))?;
                Ok::<_, PgDumpCloudError>((pn, etag, bytes_in_part))
            });

            part_number += 1;
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((pn, etag, size))) => {
                    completed_parts.push((pn, etag));
                    uploaded_bytes += size;
                    progress.send(ProgressEvent::Progress {
                        phase: Phase::Uploading,
                        bytes: uploaded_bytes,
                        total: Some(file_size),
                    });
                }
                Ok(Err(e)) => {
                    if !failed {
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload failed: {e}"),
                        });
                    }
                    failed = true;
                }
                Err(e) => {
                    if !failed {
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload task panicked: {e}"),
                        });
                    }
                    failed = true;
                }
            }
        }

        if failed {
            self.abort_upload(&key, &upload_id).await;
            return Err(PgDumpCloudError::Storage("Upload failed".into()));
        }

        completed_parts.sort_by_key(|(pn, _)| *pn);

        let parts: Vec<CompletedPart> = completed_parts
            .iter()
            .map(|(pn, etag)| {
                CompletedPart::builder()
                    .part_number(*pn)
                    .e_tag(etag)
                    .build()
            })
            .collect();

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        progress.send(ProgressEvent::Progress {
            phase: Phase::Uploading,
            bytes: file_size,
            total: Some(file_size),
        });

        progress.send(ProgressEvent::PhaseCompleted {
            phase: Phase::Uploading,
        });

        Ok(())
    }

    async fn download(
        &self,
        remote_key: &str,
        local_path: &Path,
        progress: &dyn ProgressSender,
    ) -> Result<()> {
        let key = self.full_key(remote_key);

        progress.send(ProgressEvent::PhaseStarted {
            phase: Phase::Downloading,
        });

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        let total = resp.content_length().map(|l| l as u64);
        let mut stream = resp.body.into_async_read();
        let mut file = tokio::fs::File::create(local_path)
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        let mut downloaded: u64 = 0;
        let mut buf = vec![0u8; 64 * 1024];

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let n = stream
                .read(&mut buf)
                .await
                .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])
                .await
                .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;
            downloaded += n as u64;
            progress.send(ProgressEvent::Progress {
                phase: Phase::Downloading,
                bytes: downloaded,
                total,
            });
        }

        progress.send(ProgressEvent::PhaseCompleted {
            phase: Phase::Downloading,
        });

        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BackupEntry>> {
        let full_prefix = self.full_key(prefix);

        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        let mut entries = Vec::new();
        for obj in resp.contents() {
            entries.push(BackupEntry {
                key: obj.key().unwrap_or_default().to_string(),
                size: obj.size().unwrap_or(0),
                last_modified: obj.last_modified().map(|t| format!("{t}")),
            });
        }

        entries.sort_by(|a, b| b.last_modified.cmp(&a.last_modified));
        Ok(entries)
    }

    async fn delete(&self, remote_key: &str) -> Result<()> {
        let key = self.full_key(remote_key);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn test_connection(&self) -> Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(format!("Cannot reach bucket: {e}")))?;

        Ok(())
    }
}

const MIN_PART_SIZE: usize = 10 * 1024 * 1024; // 10 MB minimum per part
const MAX_CONCURRENT_PARTS: usize = 8;

/// Pick a part size based on the estimated total. Larger files use larger
/// parts to reduce per-part overhead and improve throughput, but capped
/// to keep memory usage reasonable (MAX_CONCURRENT_PARTS * part_size in RAM).
fn choose_part_size(estimated_total: Option<u64>) -> usize {
    match estimated_total {
        Some(t) if t > 5 * 1024 * 1024 * 1024 => 50 * 1024 * 1024, // >5 GB → 50 MB parts
        Some(t) if t > 1024 * 1024 * 1024 => 25 * 1024 * 1024,     // >1 GB → 25 MB parts
        _ => MIN_PART_SIZE,                                        // else  → 10 MB parts
    }
}

impl S3Storage {
    /// Stream data directly from an `AsyncRead` source into S3 using
    /// multipart upload with parallel part uploads. No local file is
    /// written — data flows from the reader (e.g. pg_dump stdout)
    /// through memory buffers into S3.
    ///
    /// `cancel` is checked between part uploads. If cancelled, the
    /// multipart upload is aborted and `Err` is returned.
    ///
    /// `estimated_total` is an optional hint for progress reporting.
    pub async fn upload_stream<R: tokio::io::AsyncRead + Unpin + Send>(
        &self,
        reader: &mut R,
        remote_key: &str,
        progress: &dyn ProgressSender,
        cancel: Option<&tokio_util::sync::CancellationToken>,
        estimated_total: Option<u64>,
    ) -> Result<()> {
        let key = self.full_key(remote_key);
        let phase = Phase::StreamingUpload;

        progress.send(ProgressEvent::PhaseStarted {
            phase: phase.clone(),
        });

        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        let upload_id = create
            .upload_id()
            .ok_or_else(|| PgDumpCloudError::Storage("No upload_id returned".into()))?
            .to_string();

        let part_size = choose_part_size(estimated_total);
        let mut part_number: i32 = 1;
        let mut completed_parts: Vec<(i32, String)> = Vec::new();
        let mut bytes_read: u64 = 0;
        let mut join_set = tokio::task::JoinSet::new();
        let mut failed = false;

        loop {
            if let Some(ct) = cancel {
                if ct.is_cancelled() {
                    failed = true;
                    break;
                }
            }

            // Non-blocking drain: collect any parts that finished while
            // we were reading, so progress stays up to date and memory
            // from completed uploads is freed promptly.
            loop {
                match join_set.try_join_next() {
                    Some(Ok(Ok((pn, etag)))) => {
                        completed_parts.push((pn, etag));
                    }
                    Some(Ok(Err(e))) => {
                        failed = true;
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload failed: {e}"),
                        });
                        break;
                    }
                    Some(Err(e)) => {
                        failed = true;
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload task panicked: {e}"),
                        });
                        break;
                    }
                    None => break,
                }
            }

            if failed {
                break;
            }

            // Fill a part-sized buffer from the reader
            let mut buf = vec![0u8; part_size];
            let mut buf_len: usize = 0;
            loop {
                let n = reader
                    .read(&mut buf[buf_len..])
                    .await
                    .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;
                if n == 0 {
                    break;
                }
                buf_len += n;
                if buf_len >= part_size {
                    break;
                }
            }

            if buf_len == 0 {
                break;
            }

            bytes_read += buf_len as u64;
            progress.send(ProgressEvent::Progress {
                phase: phase.clone(),
                bytes: bytes_read,
                total: estimated_total,
            });

            // If at max concurrency, wait for one upload to finish
            while join_set.len() >= MAX_CONCURRENT_PARTS {
                match join_set.join_next().await {
                    Some(Ok(Ok((pn, etag)))) => {
                        completed_parts.push((pn, etag));
                    }
                    Some(Ok(Err(e))) => {
                        failed = true;
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload failed: {e}"),
                        });
                        break;
                    }
                    Some(Err(e)) => {
                        failed = true;
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload task panicked: {e}"),
                        });
                        break;
                    }
                    None => break,
                }
            }

            if failed {
                break;
            }

            buf.truncate(buf_len);
            let pn = part_number;
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let k = key.clone();
            let uid = upload_id.clone();

            join_set.spawn(async move {
                let body = ByteStream::from(buf);
                let resp = client
                    .upload_part()
                    .bucket(bucket)
                    .key(k)
                    .upload_id(uid)
                    .part_number(pn)
                    .body(body)
                    .send()
                    .await
                    .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

                let etag = resp
                    .e_tag()
                    .map(|s| s.to_string())
                    .ok_or_else(|| PgDumpCloudError::Storage("No ETag".into()))?;
                Ok::<_, PgDumpCloudError>((pn, etag))
            });

            part_number += 1;
        }

        // Drain remaining in-flight uploads
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((pn, etag))) => {
                    completed_parts.push((pn, etag));
                }
                Ok(Err(e)) => {
                    if !failed {
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload failed: {e}"),
                        });
                    }
                    failed = true;
                }
                Err(e) => {
                    if !failed {
                        progress.send(ProgressEvent::Error {
                            message: format!("Part upload task panicked: {e}"),
                        });
                    }
                    failed = true;
                }
            }
        }

        if failed || (cancel.is_some() && cancel.unwrap().is_cancelled()) {
            self.abort_upload(&key, &upload_id).await;
            return Err(PgDumpCloudError::Storage(
                "Upload cancelled or failed".into(),
            ));
        }

        if completed_parts.is_empty() {
            self.abort_upload(&key, &upload_id).await;
            return Err(PgDumpCloudError::Storage(
                "No data was read from source".into(),
            ));
        }

        completed_parts.sort_by_key(|(pn, _)| *pn);

        let parts: Vec<CompletedPart> = completed_parts
            .iter()
            .map(|(pn, etag)| {
                CompletedPart::builder()
                    .part_number(*pn)
                    .e_tag(etag)
                    .build()
            })
            .collect();

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| PgDumpCloudError::Storage(e.to_string()))?;

        progress.send(ProgressEvent::Progress {
            phase: phase.clone(),
            bytes: bytes_read,
            total: Some(bytes_read),
        });

        progress.send(ProgressEvent::PhaseCompleted { phase });

        Ok(())
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) {
        let _ = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
    }
}
