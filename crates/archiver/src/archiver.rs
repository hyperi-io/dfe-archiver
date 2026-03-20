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

use crate::config::{Config, SharedConfig};
use crate::metrics::ArchiverMetrics;
use dfe_archiver_core::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver_core::buffer::TieredBufferManager;
use dfe_archiver_core::compression::create_compressor;
use dfe_archiver_core::routing::Router;
use dfe_archiver_core::types::KafkaOffset;
use dfe_archiver_core::{Error, Result};
use dfe_archiver_io::TransportAdapter;
use dfe_archiver_io::storage::create_backend;
use hyperi_rustlib::logger::helpers::{log_debounced, log_sampled, log_state_change};
use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};
use hyperi_rustlib::scaling::{ScalingComponent, ScalingPressure};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

// Log spam guards
static RECV_ERROR_LAST: AtomicU64 = AtomicU64::new(0);
static ROUTE_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);
static BACKPRESSURE_ACTIVE: AtomicBool = AtomicBool::new(false);
static MEMORY_PRESSURE_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Main archiver pipeline
pub struct Archiver {
    /// Startup config snapshot (for restart-required fields: transport, archive, routing, etc.)
    startup_config: Config,
    /// Shared config for hot-reloadable fields (buffer, memory, scaling tunables)
    shared_config: SharedConfig<Config>,
    transport: Mutex<TransportAdapter>,
    router: Router,
    buffer: TieredBufferManager,
    metrics: Arc<ArchiverMetrics>,
    /// Active archive writers per destination
    writers: Mutex<HashMap<String, ArchiveWriter>>,
    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
    /// KEDA scaling pressure calculator
    scaling: ScalingPressure,
    /// Cgroup-aware memory guard for backpressure
    memory_guard: MemoryGuard,
}

impl Archiver {
    /// Create new archiver from shared configuration.
    ///
    /// Takes a snapshot of the config for startup-bound fields (transport,
    /// archive, routing, compression). Hot-reloadable fields (buffer thresholds,
    /// memory limits, scaling tunables) are read from `shared_config` each iteration.
    ///
    /// `metrics` must be the pre-initialised `ArchiverMetrics` from `init_metrics()`,
    /// which shares the same `MetricsManager` as the HTTP server.
    pub async fn new(
        shared_config: SharedConfig<Config>,
        metrics: Arc<ArchiverMetrics>,
    ) -> Result<Self> {
        let config = shared_config.get();
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

        let scaling = ScalingPressure::new(
            config.scaling.clone(),
            vec![
                ScalingComponent::new("kafka_lag", 0.40, 100_000.0),
                ScalingComponent::new("buffer_depth", 0.30, 10_000.0),
                ScalingComponent::new("memory", 0.30, 1.0),
            ],
        );

        let memory_guard = MemoryGuard::new(MemoryGuardConfig::from_env("DFE_ARCHIVER"));

        info!(
            brokers = %config.kafka.brokers.join(","),
            topics = %config.kafka.topics.join(","),
            destination = %config.archive.destination,
            "Archiver initialized"
        );

        Ok(Self {
            startup_config: config,
            shared_config,
            transport: Mutex::new(transport),
            router,
            buffer,
            metrics,
            writers: Mutex::new(HashMap::new()),
            cancel: CancellationToken::new(),
            scaling,
            memory_guard,
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
    /// This runs until shutdown is signaled via the cancellation token.
    #[instrument(skip(self))]
    pub async fn run(&self) -> Result<()> {
        info!("Starting archiver main loop");

        loop {
            // Pattern B: pause consumption when under memory pressure
            // Consumer lag rises, KEDA scales up replicas. No data loss.
            if self.memory_guard.under_pressure() {
                if log_state_change(&MEMORY_PRESSURE_ACTIVE, true) {
                    warn!(
                        current_bytes = self.memory_guard.current_bytes(),
                        limit_bytes = self.memory_guard.limit_bytes(),
                        ratio = format!("{:.1}%", self.memory_guard.pressure_ratio() * 100.0),
                        "Memory pressure HIGH — pausing Kafka consumption"
                    );
                }
                self.metrics.set_memory_usage(
                    self.memory_guard.current_bytes(),
                    self.memory_guard.limit_bytes(),
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            if log_state_change(&MEMORY_PRESSURE_ACTIVE, false) {
                info!("Memory pressure recovered — resuming Kafka consumption");
            }

            // Race recv against cancellation — exit immediately on shutdown
            let messages = tokio::select! {
                () = self.cancel.cancelled() => {
                    info!("Shutdown requested, exiting main loop");
                    return Ok(());
                }
                result = async {
                    let batch_size = self.shared_config.with(|c| c.kafka.batch_size);
                    let transport = self.transport.lock().await;
                    transport.recv(batch_size).await
                } => {
                    match result {
                        Ok(msgs) => msgs,
                        Err(e) => {
                            if log_debounced(&RECV_ERROR_LAST, 5000) {
                                error!(error = %e, "Failed to receive from Kafka (debounced, max 1/5s)");
                            }
                            self.metrics.record_error();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    }
                }
            };

            if messages.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            self.process_messages(messages).await;
        }
    }

    /// Process a batch of messages: route, buffer, write, commit offsets, update metrics
    async fn process_messages(&self, messages: Vec<dfe_archiver_core::KafkaMessage>) {
        let batch_len = messages.len();
        self.metrics.record_received(batch_len as u64);
        debug!(count = batch_len, "Received batch from Kafka");

        // Track incoming bytes in memory guard
        let batch_bytes: u64 = messages.iter().map(|m| m.payload.len() as u64).sum();
        self.memory_guard.add_bytes(batch_bytes);

        // Update kafka_lag scaling component — full batches indicate lag
        self.scaling.set_component("kafka_lag", batch_len as f64);

        let mut offsets_to_commit: Vec<KafkaOffset> = Vec::new();
        let mut backpressure = false;

        for message in messages {
            let destination = match self.router.route(&message) {
                Ok(dest) => dest,
                Err(e) => {
                    if log_sampled(&ROUTE_ERROR_COUNT, 1000) {
                        warn!(error = %e, total = ROUTE_ERROR_COUNT.load(std::sync::atomic::Ordering::Relaxed), "Routing failed, using topic (sampled 1/1000)");
                    }
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
                    if log_state_change(&BACKPRESSURE_ACTIVE, true) {
                        warn!(error = %e, "Buffer push failed — backpressure active");
                    }
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
        } else {
            // Backpressure cleared — log state transition
            if log_state_change(&BACKPRESSURE_ACTIVE, false) {
                info!("Backpressure cleared — normal processing resumed");
            }
        }

        let stats = self.buffer.stats();
        self.metrics
            .set_hot_buffer_stats(stats.current_hot_buffers, stats.current_hot_bytes);
        self.metrics.set_spool_bytes(stats.current_spool_bytes);

        // Update scaling pressure components using MemoryGuard for cgroup-aware tracking
        self.scaling
            .set_component("buffer_depth", stats.current_hot_buffers as f64);
        self.scaling
            .set_component("memory", self.memory_guard.pressure_ratio());
        let pressure = self.scaling.calculate();
        self.metrics.set_scaling_pressure(pressure);
        self.metrics
            .set_scaling_memory_pressure(self.memory_guard.pressure_ratio());
        self.metrics
            .set_scaling_circuit_open(self.scaling.snapshot().circuit_open);
        self.metrics.set_memory_usage(
            self.memory_guard.current_bytes(),
            self.memory_guard.limit_bytes(),
        );
    }

    /// Write a staged batch to archive storage
    #[instrument(skip(self, batch), fields(destination = %batch.destination, records = batch.record_count))]
    async fn write_batch(&self, batch: &dfe_archiver_core::buffer::StagedBatch) -> Result<()> {
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

        // Release bytes from memory guard after successful write
        self.memory_guard.release(batch.data.len() as u64);

        let duration = start.elapsed();
        self.metrics.record_flush();
        self.metrics.record_flush_duration(duration.as_secs_f64());
        self.metrics.record_bytes_written(batch.data.len() as u64);
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

    /// Create a new archive writer for a destination.
    ///
    /// Uses `startup_config` for archive/compression settings (restart-required).
    fn create_writer(&self, destination: &str) -> Result<ArchiveWriter> {
        let policy = RollingPolicy {
            max_size_bytes: self.startup_config.archive.roll_size_bytes,
            max_age_secs: self.startup_config.archive.roll_interval_secs,
        };

        let compressor = create_compressor(
            &self.startup_config.compression.codec,
            self.startup_config.compression.level,
        )?;

        let mut archive_config = self.startup_config.archive.clone();
        archive_config.path_template = format!(
            "{}/{}",
            destination, self.startup_config.archive.path_template
        );

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
        self.cancel.cancel();
    }

    /// Drain all buffers and close writers (for shutdown)
    #[instrument(skip(self))]
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

    /// Check if archiver is healthy (transport up and not under memory pressure)
    pub async fn is_healthy(&self) -> bool {
        let transport = self.transport.lock().await;
        transport.is_healthy() && !self.memory_guard.under_pressure()
    }
}

#[cfg(test)]
mod tests {
    // Integration tests require running infrastructure (Kafka, MinIO)
    // See tests/ directory for integration test files
}
