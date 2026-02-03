// Project:   dfe-archiver
// File:      src/archiver.rs
// Purpose:   Main archiver pipeline orchestrator
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

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

use crate::archive::{ArchiveWriter, RollingPolicy};
use crate::buffer::TieredBufferManager;
use crate::compression::create_compressor;
use crate::config::Config;
use crate::kafka::{KafkaOffset, TransportAdapter};
use crate::metrics::ArchiverMetrics;
use crate::routing::Router;
use crate::storage::create_backend;
use crate::{Error, Result};
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
        // Create Kafka transport
        let transport = TransportAdapter::new(&config.kafka).await?;

        // Create router
        let router = Router::new(config.routing.clone());

        // Create tiered buffer manager
        let buffer_config = crate::buffer::TieredBufferConfig {
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

        // Create metrics
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

            // Receive batch from Kafka
            let messages = {
                let mut transport = self.transport.lock().await;
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
                // No messages, sleep briefly and continue
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            self.metrics.record_received(messages.len() as u64);
            debug!(count = messages.len(), "Received batch from Kafka");

            // Process each message
            let mut offsets_to_commit: Vec<KafkaOffset> = Vec::new();

            for message in messages {
                // Route message to destination
                let destination = match self.router.route(&message) {
                    Ok(dest) => dest,
                    Err(e) => {
                        warn!(error = %e, "Routing failed, using topic as destination");
                        message.topic.clone()
                    }
                };

                // Buffer the message
                let offset = KafkaOffset::from(&message);
                match self.buffer.push(&destination, message) {
                    Ok(staged_batches) => {
                        // Process any batches ready for archive
                        for batch in staged_batches {
                            if let Err(e) = self.write_batch(&batch).await {
                                error!(
                                    error = %e,
                                    destination = %batch.destination,
                                    "Failed to write batch"
                                );
                                self.metrics.record_error();
                                // Don't commit these offsets
                                continue;
                            }

                            // Batch written successfully, track offsets
                            offsets_to_commit.extend(batch.offsets);
                            self.metrics.record_archived(batch.record_count as u64);
                        }
                    }
                    Err(e) => {
                        // Disk pressure - backpressure
                        warn!(error = %e, "Buffer push failed (backpressure)");
                        self.metrics.record_disk_pressure();
                        // Sleep to allow disk to clear
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }

                offsets_to_commit.push(offset);
            }

            // Commit processed offsets
            if !offsets_to_commit.is_empty() {
                let mut transport = self.transport.lock().await;
                if let Err(e) = transport.commit(&offsets_to_commit).await {
                    error!(error = %e, "Failed to commit offsets");
                }
            }

            // Check for aged buffers that need flushing
            let aged_batches = self.buffer.flush_aged();
            for batch in aged_batches {
                if let Err(e) = self.write_batch(&batch).await {
                    error!(
                        error = %e,
                        destination = %batch.destination,
                        "Failed to write aged batch"
                    );
                    self.metrics.record_error();
                }
            }

            // Update buffer metrics
            let stats = self.buffer.stats();
            self.metrics
                .set_hot_buffer_stats(stats.current_hot_buffers, stats.current_hot_bytes);
            self.metrics.set_spool_bytes(stats.current_spool_bytes);
        }

        Ok(())
    }

    /// Write a staged batch to archive storage
    async fn write_batch(&self, batch: &crate::buffer::StagedBatch) -> Result<()> {
        let start = std::time::Instant::now();

        // Get or create writer for this destination
        let mut writers = self.writers.lock().await;
        let writer = if let Some(w) = writers.get_mut(batch.destination.as_str()) {
            w
        } else {
            // Create new writer
            let writer = self.create_writer(&batch.destination)?;
            writers.insert(batch.destination.to_string(), writer);
            writers
                .get_mut(batch.destination.as_str())
                .ok_or_else(|| Error::Storage("writer not found after insert".to_string()))?
        };

        // Write batch data
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

        // Build destination-specific archive config
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

        // Flush all remaining buffers
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

        // Close all writers
        let mut writers = self.writers.lock().await;
        for (dest, mut writer) in writers.drain() {
            if let Err(e) = writer.close().await {
                error!(error = %e, destination = %dest, "Failed to close writer");
            }
        }

        // Close Kafka transport
        let mut transport = self.transport.lock().await;
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
    use super::*;

    // Integration tests would go here but require running infrastructure
    // See tests/integration/ for full integration tests
}
