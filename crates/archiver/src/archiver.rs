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
use dfe_archiver_io::storage::create_backend;
use dfe_archiver_io::{KafkaStatsEmitter, TransportAdapter};
use hyperi_rustlib::logger::helpers::{log_debounced, log_sampled, log_state_change};
use hyperi_rustlib::memory::{MemoryGuard, MemoryGuardConfig};
use hyperi_rustlib::scaling::{ScalingComponent, ScalingPressure};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

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
    /// Active archive writers per destination.
    /// `RwLock` on the map (read for lookup, write for insert new destination).
    /// Each writer has its own `tokio::Mutex` for independent async I/O.
    writers: parking_lot::RwLock<HashMap<String, Arc<Mutex<ArchiveWriter>>>>,
    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
    /// KEDA scaling pressure calculator
    scaling: ScalingPressure,
    /// Cgroup-aware memory guard for backpressure
    memory_guard: MemoryGuard,
    /// rdkafka stats emitter (sidecar consumer for broker/partition metrics)
    _stats_emitter: Option<KafkaStatsEmitter>,
    /// Dead letter queue for failed messages
    dlq: Arc<hyperi_rustlib::dlq::Dlq>,
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

        // Start rdkafka stats sidecar (non-fatal if it fails)
        let stats_emitter = match KafkaStatsEmitter::new(&config.kafka) {
            Ok(emitter) => Some(emitter),
            Err(e) => {
                warn!(error = %e, "Failed to start Kafka stats emitter (non-fatal)");
                None
            }
        };

        // Create DLQ (file-only mode — cascade to Kafka is optional via config)
        let dlq = hyperi_rustlib::dlq::Dlq::file_only(&config.dlq, "dfe-archiver")
            .map_err(|e| Error::Config(format!("DLQ init failed: {e}")))?;
        if config.dlq.enabled {
            info!(mode = ?config.dlq.mode, "DLQ enabled");
        }

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
            writers: parking_lot::RwLock::new(HashMap::new()),
            cancel: CancellationToken::new(),
            scaling,
            memory_guard,
            _stats_emitter: stats_emitter,
            dlq: Arc::new(dlq),
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

        // Independent flush timer — ensures aged batches flush even when no data is flowing.
        // Without this, flush_aged() only runs inside process_messages() which requires
        // new Kafka data to arrive (fixes #14).
        let flush_age_secs = self.shared_config.with(|c| c.buffer.flush_age_secs);
        let mut flush_interval = tokio::time::interval(Duration::from_secs(flush_age_secs.max(1)));
        // Consume the immediate first tick so we don't flush at startup
        flush_interval.tick().await;

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

            tokio::select! {
                biased; // Prioritise shutdown over data processing

                () = self.cancel.cancelled() => {
                    info!("Shutdown requested, exiting main loop");
                    return Ok(());
                }

                // Independent flush timer: flush aged batches even when no data is flowing.
                // Without this, aged data sits in memory until new Kafka data arrives (fixes #14).
                _ = flush_interval.tick() => {
                    let aged_batches = self.buffer.flush_aged();
                    if !aged_batches.is_empty() {
                        debug!(count = aged_batches.len(), "Flushing aged batches (timer)");
                        let mut offsets_to_commit = Vec::new();
                        for batch in aged_batches {
                            if let Err(e) = self.write_batch(&batch).await {
                                error!(
                                    error = %e,
                                    destination = %batch.destination,
                                    "Failed to write aged batch (timer)"
                                );
                                self.metrics.record_error();
                                self.send_to_dlq(&batch.destination, &batch.data, &e).await;
                            } else {
                                offsets_to_commit.extend(batch.offsets);
                            }
                        }
                        if !offsets_to_commit.is_empty() {
                            let transport = self.transport.lock().await;
                            if let Err(e) = transport.commit(&offsets_to_commit).await {
                                self.metrics.record_commit_error();
                                error!(error = %e, "Failed to commit offsets (timer flush)");
                            }
                            self.metrics.record_commit(offsets_to_commit.len() as u64);
                        }
                    }
                }

                result = async {
                    let batch_size = self.shared_config.with(|c| c.kafka.batch_size);
                    let recv_start = std::time::Instant::now();
                    let transport = self.transport.lock().await;
                    let result = transport.recv(batch_size).await;
                    self.metrics.record_recv_duration(recv_start.elapsed().as_secs_f64());
                    result
                } => {
                    match result {
                        Ok(msgs) if !msgs.is_empty() => {
                            self.process_messages(msgs).await;
                            self.metrics.update_eps();
                        }
                        Ok(_) => {
                            // Empty batch — no messages available
                        }
                        Err(e) => {
                            if log_debounced(&RECV_ERROR_LAST, 5000) {
                                error!(error = %e, "Failed to receive from Kafka (debounced, max 1/5s)");
                            }
                            self.metrics.record_error();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    /// Process a batch of messages: route, buffer, write, commit offsets, update metrics
    #[allow(clippy::too_many_lines)]
    async fn process_messages(&self, messages: Vec<dfe_archiver_core::KafkaMessage>) {
        let batch_len = messages.len();
        let topics: Vec<&str> = if tracing::enabled!(tracing::Level::DEBUG) {
            let mut t: Vec<&str> = messages.iter().map(|m| m.topic.as_str()).collect();
            t.sort_unstable();
            t.dedup();
            t
        } else {
            Vec::new()
        };
        self.metrics.record_received(batch_len as u64);
        debug!(count = batch_len, topics = ?topics, "Received batch from Kafka");

        // Track incoming bytes in memory guard
        let batch_bytes: u64 = messages.iter().map(|m| m.payload.len() as u64).sum();
        self.memory_guard.add_bytes(batch_bytes);

        // Update kafka_lag scaling component — full batches indicate lag
        self.scaling.set_component("kafka_lag", batch_len as f64);

        let mut offsets_to_commit: Vec<KafkaOffset> = Vec::new();
        let mut backpressure = false;
        let mut all_staged: Vec<dfe_archiver_core::buffer::StagedBatch> = Vec::new();

        // Phase 1a: Parallel route computation
        // Router::route() is pure computation (&self, &KafkaMessage) — safe for par_iter.
        // For expression mode this involves sonic_rs JSON parse per message.
        let route_results: Vec<compact_str::CompactString> = messages
            .iter()
            .map(|msg| {
                self.router.route(msg).unwrap_or_else(|e| {
                    self.metrics.record_routing_error();
                    if log_sampled(&ROUTE_ERROR_COUNT, 1000) {
                        warn!(
                            error = %e,
                            total = ROUTE_ERROR_COUNT.load(std::sync::atomic::Ordering::Relaxed),
                            "Routing failed, using topic (sampled 1/1000)"
                        );
                    }
                    hyperi_rustlib::logger::security::input_validation_failure(
                        "routing",
                        &e.to_string(),
                        None,
                    );
                    msg.topic.clone()
                })
            })
            .collect();

        // Phase 1b: Sequential buffer push (mutable buffer state)
        for (message, destination) in messages.into_iter().zip(route_results) {
            trace!(
                topic = %message.topic,
                partition = message.partition,
                offset = message.offset,
                destination = %destination,
                payload_bytes = message.payload.len(),
                "Routed message"
            );

            match self.buffer.push(&destination, message) {
                Ok(staged_batches) => {
                    for batch in &staged_batches {
                        debug!(
                            destination = %batch.destination,
                            records = batch.record_count,
                            bytes = batch.data.len(),
                            trigger = "size_or_eviction",
                            "Buffer staged batch"
                        );
                    }
                    all_staged.extend(staged_batches);
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

        // Collect aged batches too (if not under backpressure)
        if !backpressure {
            let aged = self.buffer.flush_aged();
            if !aged.is_empty() {
                debug!(
                    count = aged.len(),
                    total_bytes = aged.iter().map(|b| b.data.len()).sum::<usize>(),
                    "Flushing aged batches in process_messages"
                );
            }
            all_staged.extend(aged);
        }

        // Phase 2: Write all staged batches — concurrent per destination.
        // Each batch goes to a different destination with its own Mutex writer.
        // futures::future::join_all runs them concurrently on the same task,
        // maximising I/O overlap without spawning new tasks.
        let write_results: Vec<(usize, std::result::Result<(), Error>)> =
            futures::future::join_all(
                all_staged
                    .iter()
                    .enumerate()
                    .map(|(idx, batch)| async move { (idx, self.write_batch(batch).await) }),
            )
            .await;

        for (idx, result) in write_results {
            let batch = &all_staged[idx];
            match result {
                Ok(()) => {
                    offsets_to_commit.extend(batch.offsets.clone());
                    self.metrics.record_archived(batch.record_count as u64);
                }
                Err(e) => {
                    error!(
                        error = %e,
                        destination = %batch.destination,
                        "Failed to write batch"
                    );
                    self.metrics.record_error();
                    self.send_to_dlq(&batch.destination, &batch.data, &e).await;
                }
            }
        }

        if !offsets_to_commit.is_empty() {
            let transport = self.transport.lock().await;
            let commit_count = offsets_to_commit.len() as u64;
            debug!(count = commit_count, "Committing Kafka offsets");
            if let Err(e) = transport.commit(&offsets_to_commit).await {
                self.metrics.record_commit_error();
                error!(error = %e, count = commit_count, "Failed to commit offsets");
            }
            self.metrics.record_commit(commit_count);
        }

        if backpressure {
            tokio::time::sleep(Duration::from_secs(5)).await;
        } else {
            // Backpressure cleared — log state transition
            if log_state_change(&BACKPRESSURE_ACTIVE, false) {
                info!("Backpressure cleared — normal processing resumed");
            }
        }

        self.update_pipeline_metrics();
    }

    /// Update buffer stats, scaling pressure, and pipeline gauges
    fn update_pipeline_metrics(&self) {
        self.metrics.set_last_batch_timestamp();

        let stats = self.buffer.stats();
        self.metrics
            .set_hot_buffer_stats(stats.current_hot_buffers, stats.current_hot_bytes);
        self.metrics.set_spool_bytes(stats.current_spool_bytes);

        let writer_count = self.writers.read().len();
        self.metrics.set_unique_destinations(writer_count);

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
        trace!(
            destination = %batch.destination,
            records = batch.record_count,
            bytes = batch.data.len(),
            offsets = batch.offsets.len(),
            "Starting batch write"
        );

        // Get or create per-destination writer (read lock for lookup, write lock for insert)
        let writer_arc = {
            let readers = self.writers.read();
            if let Some(w) = readers.get(batch.destination.as_str()) {
                Arc::clone(w)
            } else {
                drop(readers); // release read lock before taking write lock
                let mut writers = self.writers.write();
                // Double-check after acquiring write lock (another thread may have inserted)
                if let Some(w) = writers.get(batch.destination.as_str()) {
                    Arc::clone(w)
                } else {
                    debug!(destination = %batch.destination, "Creating new archive writer");
                    let writer = Arc::new(Mutex::new(self.create_writer(&batch.destination)?));
                    writers.insert(batch.destination.to_string(), Arc::clone(&writer));
                    writer
                }
            }
        };

        // Lock only THIS destination's writer — other destinations can write concurrently
        let mut writer = writer_arc.lock().await;

        // Write data (may trigger a roll)
        if let Some(close_stats) = writer.write(&batch.data).await? {
            if let Some(trigger) = close_stats.trigger {
                debug!(
                    destination = %batch.destination,
                    trigger,
                    compressed_bytes = close_stats.compressed_bytes,
                    "Archive file rolled"
                );
                self.metrics.record_archive_roll(trigger);
            }
            self.metrics
                .record_file_closed(close_stats.compressed_bytes);
        }

        // Flush buffer to storage (returns compression stats)
        if let Some(flush_stats) = writer.flush().await? {
            let ratio = if flush_stats.uncompressed_bytes > 0 {
                flush_stats.compressed_bytes as f64 / flush_stats.uncompressed_bytes as f64
            } else {
                1.0
            };
            debug!(
                destination = %batch.destination,
                uncompressed_bytes = flush_stats.uncompressed_bytes,
                compressed_bytes = flush_stats.compressed_bytes,
                compression_ratio = format!("{ratio:.3}"),
                duration_ms = format!("{:.1}", flush_stats.compression_duration_secs * 1000.0),
                "Compression complete"
            );
            self.metrics
                .record_compression_duration(flush_stats.compression_duration_secs);
            self.metrics.record_bytes_compressed(
                flush_stats.compressed_bytes,
                flush_stats.uncompressed_bytes,
            );
        }

        // Release bytes from memory guard after successful write
        self.memory_guard.release(batch.data.len() as u64);

        let duration = start.elapsed();
        let backend = self.startup_config.archive.backend_name();
        self.metrics.record_flush(duration.as_secs_f64(), "size");
        self.metrics.record_bytes_written(batch.data.len() as u64);
        self.metrics.record_batch_size(batch.data.len() as u64);
        self.metrics
            .record_sink_duration(backend, duration.as_secs_f64());

        debug!(
            destination = %batch.destination,
            records = batch.record_count,
            bytes = batch.data.len(),
            duration_ms = duration.as_millis(),
            backend,
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
        let batches = self.buffer.flush_all();
        info!(
            batches = batches.len(),
            total_bytes = batches.iter().map(|b| b.data.len()).sum::<usize>(),
            "Draining buffers"
        );
        for batch in batches {
            if let Err(e) = self.write_batch(&batch).await {
                error!(
                    error = %e,
                    destination = %batch.destination,
                    "Failed to write final batch"
                );
            }
        }

        let writers: Vec<(String, Arc<Mutex<ArchiveWriter>>)> = {
            let mut map = self.writers.write();
            map.drain().collect()
        };
        debug!(writer_count = writers.len(), "Closing archive writers");
        for (dest, writer_arc) in writers {
            let mut writer = writer_arc.lock().await;
            match writer.close().await {
                Ok(Some(close_stats)) => {
                    debug!(
                        destination = %dest,
                        compressed_bytes = close_stats.compressed_bytes,
                        "Closed archive writer"
                    );
                    self.metrics
                        .record_file_closed(close_stats.compressed_bytes);
                }
                Ok(None) => {
                    debug!(destination = %dest, "Closed empty archive writer");
                }
                Err(e) => {
                    error!(error = %e, destination = %dest, "Failed to close writer");
                }
            }
        }

        let transport = self.transport.lock().await;
        transport.close().await?;

        info!("Drain complete");
        Ok(())
    }

    /// Route failed batch data to the dead letter queue.
    ///
    /// Non-fatal: logs and records metrics on DLQ failure but does not
    /// propagate the error (the original write error is the primary concern).
    async fn send_to_dlq(&self, destination: &str, data: &[u8], error: &Error) {
        if !self.dlq.is_enabled() {
            trace!(destination, "DLQ disabled, skipping failed batch");
            return;
        }
        debug!(
            destination,
            data_bytes = data.len(),
            error = %error,
            "Sending failed batch to DLQ"
        );

        let entry = hyperi_rustlib::dlq::DlqEntry::new(
            "dfe-archiver",
            format!("storage_write_failed: {error}"),
            data.to_vec(),
        )
        .with_destination(destination);

        if let Err(dlq_err) = self.dlq.send(entry).await {
            error!(error = %dlq_err, destination, "DLQ send also failed");
        } else {
            self.metrics.record_dlq(1);
            hyperi_rustlib::logger::security::record_dlq(
                "storage_write_failed",
                &error.to_string(),
                Some(destination),
            );
        }
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

    /// Sync transport health check (for `HealthRegistry` callback).
    ///
    /// Uses `try_lock` to avoid blocking — returns true if lock is
    /// contended (transport is actively being used, therefore healthy).
    pub fn is_transport_healthy(&self) -> bool {
        match self.transport.try_lock() {
            Ok(transport) => transport.is_healthy(),
            Err(_) => true, // Lock held = transport in active use
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::config::validate_config;
    use dfe_archiver_core::config::Config;

    #[test]
    fn test_cleared_brokers_fails_validation() {
        let mut config = Config::default();
        config.kafka.brokers.clear();
        assert!(
            validate_config(&config).is_err(),
            "empty brokers should fail"
        );
    }
}
