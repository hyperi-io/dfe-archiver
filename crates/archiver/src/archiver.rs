// Project:   dfe-archiver
// File:      crates/archiver/src/archiver.rs
// Purpose:   Main archiver pipeline orchestrator
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

//! # Archiver Pipeline
//!
//! Orchestrates the complete Kafka → Buffer → Archive → Storage pipeline.
//!
//! ```text
//! Kafka Consumer → Router → Buffer Manager → Archive Writer → Storage
//!       ↓             ↓           ↓               ↓            ↓
//!   Batch recv    Dest key    Per-dest       Compressed    S3/MinIO/
//!   (10K msgs)    routing     buffering      rolling       File/etc
//! ```

use crate::config::Config;
use crate::metrics::ArchiverMetrics;
use dfe_archiver_core::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver_core::buffer::TieredBufferManager;
use dfe_archiver_core::compression::create_compressor;
use dfe_archiver_core::routing::Router;
use dfe_archiver_core::types::KafkaOffset;
use dfe_archiver_core::{Error, Result};
use dfe_archiver_io::storage::create_backend;
use dfe_archiver_io::TransportAdapter;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Main archiver pipeline
pub struct Archiver {
    config: Config,
    transport: Mutex<TransportAdapter>,
    router: Router,
    buffer: TieredBufferManager,
    metrics: Arc<ArchiverMetrics>,
    /// Active archive writers per destination
    writers: Mutex<HashMap<String, ArchiveWriter>>,
    /// Shutdown flag
    shutdown: AtomicBool,
}

impl Archiver {
    /// Create new archiver from configuration
    pub async fn new(config: Config) -> Result<Self> {
        let transport = TransportAdapter::new(&config.kafka).await?;

        let router = Router::new(config.routing.clone());

        let buffer_config = dfe_archiver_core::buffer::TieredBufferConfig {
            max_hot_buffers: 64,
            hot_buffer_size: 1024 * 1024, // 1MB
            hot_buffer_age_secs: config.buffer.flush_age_secs,
            spool_dir: ".tmp/archiver-spool".into(),
            max_writers: config.buffer.writer_parallelism,
            staging_batch_size: config.buffer.flush_bytes,
            max_spool_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            min_free_disk_bytes: 1024 * 1024 * 1024,  // 1GB
            spool_compression: true,
        };
        let buffer = TieredBufferManager::new(buffer_config)?;

        let metrics = ArchiverMetrics::new();

        info!(
            brokers = %config.kafka.brokers.join(","),
            topics = %config.kafka.topics.join(","),
            destination = %config.archive.destination,
            "Archiver initialized"
        );

        Ok(Self {
            config,
            transport: Mutex::new(transport),
            router,
            buffer,
            metrics,
            writers: Mutex::new(HashMap::new()),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Check Kafka connection (transport connects on creation)
    pub async fn check_connection(&self) -> Result<()> {
        let transport = self.transport.lock().await;
        if !transport.is_healthy() {
            return Err(Error::Kafka("Kafka transport is not healthy".to_string()));
        }
        info!("Kafka connection verified");
        Ok(())
    }

    /// Run the main archiver loop
    ///
    /// This runs until shutdown is signaled.
    pub async fn run(&self) -> Result<()> {
        info!("Starting archiver main loop");

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Shutdown requested, exiting main loop");
                break;
            }

            let messages = {
                let transport = self.transport.lock().await;
                match transport.recv(self.config.kafka.batch_size).await {
                    Ok(msgs) => msgs,
                    Err(e) => {
                        error!(error = %e, "Failed to receive from Kafka");
                        self.metrics.record_error();
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };

            if messages.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            self.metrics.record_received(messages.len() as u64);
            debug!(count = messages.len(), "Received batch from Kafka");

            let mut offsets_to_commit: Vec<KafkaOffset> = Vec::new();
            let mut backpressure = false;

            for message in messages {
                let destination = match self.router.route(&message) {
                    Ok(dest) => dest,
                    Err(e) => {
                        warn!(error = %e, "Routing failed, using topic as destination");
                        message.topic.clone()
                    }
                };

                match self.buffer.push(&destination, message) {
                    Ok(staged_batches) => {
                        for batch in staged_batches {
                            if let Err(e) = self.write_batch(&batch).await {
                                error!(
                                    error = %e,
                                    destination = %batch.destination,
                                    "Failed to write batch"
                                );
                                self.metrics.record_error();
                                continue;
                            }

                            offsets_to_commit.extend(batch.offsets);
                            self.metrics.record_archived(batch.record_count as u64);
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Buffer push failed (backpressure)");
                        self.metrics.record_disk_pressure();
                        backpressure = true;
                        break;
                    }
                }
            }

            if !backpressure {
                let aged_batches = self.buffer.flush_aged();
                for batch in aged_batches {
                    if let Err(e) = self.write_batch(&batch).await {
                        error!(
                            error = %e,
                            destination = %batch.destination,
                            "Failed to write aged batch"
                        );
                        self.metrics.record_error();
                    } else {
                        offsets_to_commit.extend(batch.offsets);
                    }
                }
            }

            if !offsets_to_commit.is_empty() {
                let transport = self.transport.lock().await;
                if let Err(e) = transport.commit(&offsets_to_commit).await {
                    error!(error = %e, "Failed to commit offsets");
                }
            }

            if backpressure {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            let stats = self.buffer.stats();
            self.metrics
                .set_hot_buffer_stats(stats.current_hot_buffers, stats.current_hot_bytes);
            self.metrics.set_spool_bytes(stats.current_spool_bytes);
        }

        Ok(())
    }

    /// Write a staged batch to archive storage
    async fn write_batch(
        &self,
        batch: &dfe_archiver_core::buffer::StagedBatch,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        let mut writers = self.writers.lock().await;
        let writer = if let Some(w) = writers.get_mut(batch.destination.as_str()) {
            w
        } else {
            let writer = self.create_writer(&batch.destination)?;
            writers.insert(batch.destination.to_string(), writer);
            writers
                .get_mut(batch.destination.as_str())
                .ok_or_else(|| Error::Storage("writer not found after insert".to_string()))?
        };

        writer.write(&batch.data).await?;
        writer.flush().await?;

        let duration = start.elapsed();
        self.metrics.record_flush();
        self.metrics.record_flush_duration(duration.as_secs_f64());
        self.metrics
            .record_bytes(batch.data.len() as u64, batch.data.len() as u64);
        self.metrics.record_batch_size(batch.data.len() as u64);

        debug!(
            destination = %batch.destination,
            records = batch.record_count,
            bytes = batch.data.len(),
            duration_ms = duration.as_millis(),
            "Wrote batch to archive"
        );

        Ok(())
    }

    /// Create a new archive writer for a destination
    fn create_writer(&self, destination: &str) -> Result<ArchiveWriter> {
        let policy = RollingPolicy {
            max_size_bytes: self.config.archive.roll_size_bytes,
            max_age_secs: self.config.archive.roll_interval_secs,
        };

        let compressor = create_compressor(
            &self.config.compression.codec,
            self.config.compression.level,
        )?;

        let mut archive_config = self.config.archive.clone();
        archive_config.path_template =
            format!("{}/{}", destination, self.config.archive.path_template);

        let storage = create_backend(&archive_config)?;

        Ok(ArchiveWriter::new(
            archive_config,
            policy,
            compressor,
            storage,
        ))
    }

    /// Request graceful shutdown
    pub fn shutdown(&self) {
        info!("Shutdown requested");
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Drain all buffers and close writers (for shutdown)
    pub async fn drain(&self) -> Result<()> {
        info!("Draining buffers...");

        let batches = self.buffer.flush_all();
        for batch in batches {
            if let Err(e) = self.write_batch(&batch).await {
                error!(
                    error = %e,
                    destination = %batch.destination,
                    "Failed to write final batch"
                );
            }
        }

        let mut writers = self.writers.lock().await;
        for (dest, mut writer) in writers.drain() {
            if let Err(e) = writer.close().await {
                error!(error = %e, destination = %dest, "Failed to close writer");
            }
        }

        let transport = self.transport.lock().await;
        transport.close().await?;

        info!("Drain complete");
        Ok(())
    }

    /// Get metrics handle
    pub fn metrics(&self) -> Arc<ArchiverMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Check if archiver is healthy
    pub async fn is_healthy(&self) -> bool {
        let transport = self.transport.lock().await;
        transport.is_healthy()
    }
}

#[cfg(test)]
mod tests {
    // Integration tests require running infrastructure (Kafka, MinIO)
    // See tests/ directory for integration test files
}
