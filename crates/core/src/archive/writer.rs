// Project:   dfe-archiver
// File:      crates/core/src/archive/writer.rs
// Purpose:   Archive writer with rolling support
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::Result;
use crate::compression::Compressor;
use crate::config::ArchiveConfig;
use crate::storage::StorageBackend;
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info};

/// Rolling policy for archive files
#[derive(Debug, Clone)]
pub struct RollingPolicy {
    /// Roll when final compressed file size exceeds this (bytes)
    pub max_size_bytes: u64,

    /// Roll when file age exceeds this (seconds)
    pub max_age_secs: u64,
}

impl Default for RollingPolicy {
    fn default() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024,
            max_age_secs: 3600,
        }
    }
}

/// Archive writer state
#[derive(Debug)]
pub struct ArchiveState {
    pub path: String,
    pub compressed_bytes: AtomicU64,
    pub uncompressed_bytes: AtomicU64,
    pub records_written: AtomicU64,
    pub created_at: DateTime<Utc>,
}

/// Archive writer with compression and rolling
pub struct ArchiveWriter {
    config: ArchiveConfig,
    policy: RollingPolicy,
    compressor: Box<dyn Compressor + Send + Sync>,
    storage: Box<dyn StorageBackend + Send + Sync>,
    state: Option<ArchiveState>,
    buffer: Vec<u8>,
    file_seq: u64,
}

impl ArchiveWriter {
    /// Create new archive writer
    pub fn new(
        config: ArchiveConfig,
        policy: RollingPolicy,
        compressor: Box<dyn Compressor + Send + Sync>,
        storage: Box<dyn StorageBackend + Send + Sync>,
    ) -> Self {
        Self {
            config,
            policy,
            compressor,
            storage,
            state: None,
            buffer: Vec::with_capacity(1024 * 1024),
            file_seq: 0,
        }
    }

    /// Write data to archive
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        if self.should_roll() {
            self.roll().await?;
        }

        if self.state.is_none() {
            self.open_new_file().await?;
        }

        self.buffer.extend_from_slice(data);

        if let Some(ref state) = self.state {
            state
                .uncompressed_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Write a single record (adds newline)
    pub async fn write_record(&mut self, record: &[u8]) -> Result<()> {
        self.write(record).await?;
        self.write(b"\n").await?;

        if let Some(ref state) = self.state {
            state.records_written.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Flush buffer to storage
    pub async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let compressed = self.compressor.compress(&self.buffer)?;

        if let Some(ref state) = self.state {
            self.storage.append(&state.path, &compressed).await?;
            state
                .compressed_bytes
                .fetch_add(compressed.len() as u64, Ordering::Relaxed);
            debug!(
                path = %state.path,
                uncompressed = self.buffer.len(),
                compressed = compressed.len(),
                total_file_size = state.compressed_bytes.load(Ordering::Relaxed),
                "Flushed buffer to storage"
            );
        }

        self.buffer.clear();
        Ok(())
    }

    /// Check if file should be rolled (based on final compressed file size)
    fn should_roll(&self) -> bool {
        let Some(ref state) = self.state else {
            return false;
        };

        let file_size = state.compressed_bytes.load(Ordering::Relaxed);
        if file_size >= self.policy.max_size_bytes {
            debug!(
                file_size,
                max = self.policy.max_size_bytes,
                "Rolling: final file size exceeded"
            );
            return true;
        }

        let age = Utc::now().signed_duration_since(state.created_at);
        #[allow(clippy::cast_possible_wrap)]
        if age.num_seconds() >= self.policy.max_age_secs as i64 {
            debug!(
                age_secs = age.num_seconds(),
                max = self.policy.max_age_secs,
                "Rolling: age exceeded"
            );
            return true;
        }

        false
    }

    /// Roll to a new file
    async fn roll(&mut self) -> Result<()> {
        self.flush().await?;

        if let Some(state) = self.state.take() {
            self.storage.close(&state.path).await?;
            info!(
                path = %state.path,
                file_size = state.compressed_bytes.load(Ordering::Relaxed),
                uncompressed = state.uncompressed_bytes.load(Ordering::Relaxed),
                records = state.records_written.load(Ordering::Relaxed),
                "Closed archive file"
            );
        }

        self.open_new_file().await
    }

    /// Open a new archive file
    async fn open_new_file(&mut self) -> Result<()> {
        self.file_seq += 1;
        let now = Utc::now();
        let path = self.generate_path(&now);

        self.storage.create(&path).await?;

        self.state = Some(ArchiveState {
            path: path.clone(),
            compressed_bytes: AtomicU64::new(0),
            uncompressed_bytes: AtomicU64::new(0),
            records_written: AtomicU64::new(0),
            created_at: now,
        });

        info!(path = %path, "Opened new archive file");
        Ok(())
    }

    /// Generate path from template
    fn generate_path(&self, timestamp: &DateTime<Utc>) -> String {
        let mut path = self.config.path_template.clone();

        path = path.replace("{year}", &timestamp.format("%Y").to_string());
        path = path.replace("{month}", &timestamp.format("%m").to_string());
        path = path.replace("{day}", &timestamp.format("%d").to_string());
        path = path.replace("{hour}", &timestamp.format("%H").to_string());
        path = path.replace("{minute}", &timestamp.format("%M").to_string());
        path = path.replace("{timestamp}", &timestamp.timestamp().to_string());
        path = path.replace("{seq}", &format!("{:04}", self.file_seq));

        let ext = &self.config.file_extension;
        let compression_ext = self.compressor.extension();

        if compression_ext.is_empty() {
            format!("{path}-{:04}.{ext}", self.file_seq)
        } else {
            format!("{path}-{:04}.{ext}.{compression_ext}", self.file_seq)
        }
    }

    /// Close the writer
    pub async fn close(&mut self) -> Result<()> {
        self.flush().await?;

        if let Some(state) = self.state.take() {
            self.storage.close(&state.path).await?;
            info!(
                path = %state.path,
                file_size = state.compressed_bytes.load(Ordering::Relaxed),
                uncompressed = state.uncompressed_bytes.load(Ordering::Relaxed),
                records = state.records_written.load(Ordering::Relaxed),
                "Closed archive writer"
            );
        }

        Ok(())
    }
}
