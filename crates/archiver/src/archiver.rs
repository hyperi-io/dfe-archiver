// Project:   dfe-archiver
// File:      crates/archiver/src/archiver.rs
// Purpose:   Main archiver pipeline orchestrator
// Language:  Rust
//
// License:      BUSL-1.1
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
use dfe_archiver_core::compression::compressor_for;
use dfe_archiver_core::routing::Router;
use dfe_archiver_core::types::KafkaOffset;
use dfe_archiver_core::{Error, Result};
use dfe_archiver_io::storage::create_backend;
use dfe_archiver_io::{KafkaStatsEmitter, TransportAdapter};
use lru::LruCache;
use rayon::prelude::*;
use scalo::SelfRegulationGovernor;
use scalo::logger::helpers::{log_debounced, log_sampled, log_state_change};
use scalo::memory::MemoryGuard;
use scalo::metrics::FlushTrigger;
use scalo::scaling::ScalingPressure;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

/// Per-instance log-spam guards. Live on `Archiver` (not as module statics)
/// so state does not leak across nextest test runs that share a process.
#[derive(Default)]
struct LogSpamGuards {
    recv_error_last: AtomicU64,
    route_error_count: AtomicU64,
    backpressure_active: AtomicBool,
}

/// Main archiver pipeline
pub struct Archiver {
    /// Startup config snapshot (for restart-required fields: transport, archive, routing, etc.)
    startup_config: Config,
    /// Shared config for hot-reloadable fields (buffer, memory, scaling tunables)
    shared_config: SharedConfig<Config>,
    /// Kafka transport. `TransportAdapter` methods take `&self` and scalo's
    /// `KafkaTransport` is `Send+Sync` with internal locking on the recv hot
    /// path, so no outer `Mutex` is required. This lets recv and commit run
    /// concurrently from different async tasks.
    transport: TransportAdapter,
    router: Router,
    buffer: TieredBufferManager,
    metrics: Arc<ArchiverMetrics>,
    /// Active archive writers per destination, capped LRU cache.
    /// On overflow the least-recently-used writer is evicted and closed in
    /// a spawned task. Each writer has its own `tokio::Mutex` for
    /// independent async I/O — the outer `parking_lot::Mutex` only guards
    /// map ops (lookup / insert / evict-pop), which are O(1).
    writers: parking_lot::Mutex<LruCache<String, Arc<Mutex<ArchiveWriter>>>>,
    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
    /// The unified KEDA `ScalingPressure` engine, reaching KEDA as the
    /// `dfe_scaling_pressure` gauge on the metrics listener -- the runtime
    /// starts that listener without the `/scaling/pressure` route, so nothing
    /// serves the composite over HTTP. scalo 2.9 collapsed the old dual model (the app's
    /// weighted pressure + a separate runtime signal cell) into this single
    /// engine: the components are registered via `ServiceApp::scaling_components`
    /// and the archiver's loops drive their values directly (`kafka_lag` from
    /// assigned-partition lag, `buffer_depth` from hot-buffer count, `memory`
    /// from the cgroup guard) plus the circuit gate. Shared from the runtime when
    /// `scaling` is enabled; a standalone fallback otherwise.
    scaling: Arc<ScalingPressure>,
    /// Object-store sink circuit latch driving `ScalingPressure::set_circuit_open`.
    /// "Open" (dead) when a whole write cycle failed with zero successes;
    /// recovers the moment any write succeeds. The engine zeroes the scaling
    /// composite while open (more pods cannot relieve a dead sink) -- the right
    /// behaviour for a non-Kafka outbound (object store).
    sink_circuit_open: AtomicBool,
    /// Cgroup-aware memory guard. This is the SAME guard the self-regulation
    /// governor reads, so the bytes accounted here (`add_bytes` on recv,
    /// `release` after write) drive the inbound pause-partitions brake.
    memory_guard: Arc<MemoryGuard>,
    /// rdkafka stats emitter (sidecar consumer for broker/partition metrics)
    _stats_emitter: Option<KafkaStatsEmitter>,
    /// Dead letter queue for failed messages
    dlq: Arc<scalo::dlq::Dlq>,
    /// Log-spam guards (per-instance — see `LogSpamGuards` doc)
    log_guards: LogSpamGuards,
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
    ///
    /// `memory_guard` is the runtime's cgroup-aware guard (the one feeding the
    /// self-regulation governor); the archiver accounts in-flight bytes on it so
    /// the inbound brake reflects real archiver pressure. `governor`, when
    /// `Some`, attaches the Kafka pause-partitions inbound gate to the receiver
    /// (default-on self-regulation); `None` means self-regulation is disabled.
    ///
    /// `scaling` is the runtime's unified `ScalingPressure` engine, shared so
    /// the archiver's loops drive the `dfe_scaling_pressure` gauge KEDA scales
    /// on. `None` when the runtime's `scaling` feature is off; a standalone
    /// engine (registering the same components) is built as a fallback so the
    /// local pressure gauge keeps working.
    pub async fn new(
        shared_config: SharedConfig<Config>,
        metrics: Arc<ArchiverMetrics>,
        memory_guard: Arc<MemoryGuard>,
        governor: Option<&SelfRegulationGovernor>,
        scaling: Option<Arc<ScalingPressure>>,
    ) -> Result<Self> {
        let config = shared_config.get();
        let transport = TransportAdapter::new(&config.kafka, governor).await?;

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

        // The runtime's unified ScalingPressure, so the archiver's loops drive
        // the engine that emits the KEDA gauge. Its thresholds come from the
        // `scaling` cascade section (`ARCHIVER_SCALING__*`), not from this
        // file's config. `None` only when the `scaling` feature is compiled
        // out, where a standalone engine keeps the components registered.
        let scaling = scaling.unwrap_or_else(|| {
            Arc::new(ScalingPressure::new(
                scalo::scaling::ScalingPressureConfig::from_cascade(),
                crate::scaling_components(),
            ))
        });

        // Start rdkafka stats sidecar (non-fatal if it fails)
        let stats_emitter = match KafkaStatsEmitter::new(&config.kafka) {
            Ok(emitter) => Some(emitter),
            Err(e) => {
                warn!(error = %e, "Failed to start Kafka stats emitter (non-fatal)");
                None
            }
        };

        // Shutdown token — created early so the DLQ drain task can be tied to it.
        let cancel = CancellationToken::new();

        // Create DLQ. The Kafka backend rides the same config conversion as
        // the consumer transport -- dead-letters land on the broker the data
        // came from.
        let dlq_kafka = dfe_archiver_io::kafka::convert_config(&config.kafka);
        let dlq = scalo::dlq::Dlq::spawn(
            &config.dlq,
            "dfe-archiver",
            Some(&dlq_kafka),
            cancel.clone(),
        )
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

        // Writer LRU cache — clamp to at least 1 to satisfy NonZeroUsize.
        let writer_cap = NonZeroUsize::new(config.archive.max_writers).unwrap_or(NonZeroUsize::MIN);

        Ok(Self {
            startup_config: config,
            shared_config,
            transport,
            router,
            buffer,
            metrics,
            writers: parking_lot::Mutex::new(LruCache::new(writer_cap)),
            cancel,
            scaling,
            sink_circuit_open: AtomicBool::new(false),
            memory_guard,
            _stats_emitter: stats_emitter,
            dlq: Arc::new(dlq),
            log_guards: LogSpamGuards::default(),
        })
    }

    /// Check Kafka connection (transport connects on creation)
    pub fn check_connection(&self) -> Result<()> {
        if !self.transport.is_healthy() {
            return Err(Error::kafka("Kafka transport is not healthy"));
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

        // Memory backpressure is handled by the self-regulation governor: the
        // inbound pause-partitions gate (attached to the Kafka receiver) pauses
        // the consumer's ASSIGNED partitions under pressure -- the member stays
        // in the group (no rebalance), consumer lag rises, KEDA scales up. The
        // gate is evaluated automatically inside `recv`, so the old hand-rolled
        // Pattern-B pause loop here is gone. We never gate the outbound archive
        // drain -- gating the sink would deadlock the pipeline.
        loop {
            tokio::select! {
                biased; // Prioritise shutdown over data processing

                () = self.cancel.cancelled() => {
                    info!("Shutdown requested, exiting main loop");
                    return Ok(());
                }

                // Independent flush timer: flush aged batches even when no data is flowing.
                // Without this, aged data sits in memory until new Kafka data arrives (fixes #14).
                _ = flush_interval.tick() => {
                    // Refresh the per-pod Kafka inbound scaling signal even when no
                    // data is flowing -- an idle-but-lagging pod (e.g. paused under
                    // memory pressure) must still surface its assigned-partition lag
                    // so KEDA can scale the group out. Cheaper + fresher than the
                    // engine's own 15s tick.
                    self.push_kafka_lag_signal();
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
                            let commit_count = offsets_to_commit.len() as u64;
                            if let Err(e) = self.transport.commit(offsets_to_commit).await {
                                self.metrics.record_commit_error();
                                error!(error = %e, "Failed to commit offsets (timer flush)");
                            }
                            self.metrics.record_commit(commit_count);
                        }
                    }
                }

                result = async {
                    let batch_size = self.shared_config.with(|c| c.kafka.batch_size);
                    let recv_start = std::time::Instant::now();
                    let result = self.transport.recv(batch_size).await;
                    self.metrics.record_recv_duration(recv_start.elapsed().as_secs_f64());
                    result
                } => {
                    // Push the per-pod Kafka inbound scaling signal each poll
                    // (mirrors dfe-loader). assigned_lag() sums lag over THIS pod's
                    // ASSIGNED partitions -- scale-invariant.
                    self.push_kafka_lag_signal();
                    match result {
                        Ok(batch) if !batch.messages.is_empty() || !batch.dlq_entries.is_empty() => {
                            self.route_filter_dlq(batch.dlq_entries).await;
                            if !batch.messages.is_empty() {
                                self.process_messages(batch.messages).await;
                                self.metrics.update_eps();
                            }
                        }
                        Ok(_) => {
                            // Empty batch — no messages available (or partitions paused)
                        }
                        Err(e) => {
                            if log_debounced(&self.log_guards.recv_error_last, 5000) {
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

    /// Process a batch of messages: route → write → commit → update metrics.
    /// Each phase lives in its own helper for testability and clarity.
    async fn process_messages(&self, messages: Vec<dfe_archiver_core::KafkaMessage>) {
        let batch_len = messages.len();
        if tracing::enabled!(tracing::Level::DEBUG) {
            let mut topics: Vec<&str> = messages.iter().map(|m| m.topic.as_str()).collect();
            topics.sort_unstable();
            topics.dedup();
            debug!(count = batch_len, topics = ?topics, "Received batch from Kafka");
        }
        self.metrics.record_received(batch_len as u64);

        // Track incoming bytes in the memory guard (the inbound brake watches
        // this). The `kafka_lag` scaling component is driven by the pod's
        // assigned-partition lag in `push_kafka_lag_signal` (scale-invariant),
        // NOT by per-batch throughput, so nothing is set here.
        let batch_bytes: u64 = messages.iter().map(|m| m.payload.len() as u64).sum();
        self.memory_guard.add_bytes(batch_bytes);

        // Phase 1: route + buffer accumulate (returns staged batches and a
        // backpressure flag set when the buffer rejects a push).
        let (staged, backpressure) = self.route_batch(messages);

        // Phase 2: concurrent per-destination writes; collect commit offsets.
        let offsets_to_commit = self.write_staged(staged).await;

        // Phase 3: commit Kafka offsets (release point for at-least-once).
        self.commit_offsets(offsets_to_commit).await;

        if backpressure {
            let pause_secs = self
                .shared_config
                .with(|c| c.buffer.backpressure_pause_secs);
            tokio::time::sleep(Duration::from_secs(pause_secs)).await;
        } else if log_state_change(&self.log_guards.backpressure_active, false) {
            info!("Backpressure cleared — normal processing resumed");
        }

        self.update_pipeline_metrics();
    }

    /// Phase 1: parallel route + sequential buffer push.
    ///
    /// Returns the staged batches (including any aged buffers picked up
    /// while we're here) and a `backpressure` flag — set if the buffer
    /// rejected a push, in which case aged-flush is skipped and the caller
    /// is expected to pause.
    fn route_batch(
        &self,
        messages: Vec<dfe_archiver_core::KafkaMessage>,
    ) -> (Vec<dfe_archiver_core::buffer::StagedBatch>, bool) {
        // Phase 1a: Parallel route computation. `Router::route` is pure
        // (`&self, &KafkaMessage`) so par_iter is sound. Expression-routed
        // configs do a sonic-rs JSON parse per message here.
        let route_results: Vec<compact_str::CompactString> = messages
            .par_iter()
            .map(|msg| {
                self.router.route(msg).unwrap_or_else(|e| {
                    self.metrics.record_routing_error();
                    if log_sampled(&self.log_guards.route_error_count, 1000) {
                        warn!(
                            error = %e,
                            total = self
                                .log_guards
                                .route_error_count
                                .load(std::sync::atomic::Ordering::Relaxed),
                            "Routing failed, using topic (sampled 1/1000)"
                        );
                    }
                    scalo::logger::security::input_validation_failure(
                        "routing",
                        &e.to_string(),
                        None,
                    );
                    msg.topic.clone()
                })
            })
            .collect();

        // Phase 1b: Sequential buffer push (mutable buffer state).
        let mut all_staged: Vec<dfe_archiver_core::buffer::StagedBatch> = Vec::new();
        let mut backpressure = false;
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
                    if log_state_change(&self.log_guards.backpressure_active, true) {
                        warn!(error = %e, "Buffer push failed — backpressure active");
                    }
                    self.metrics.record_disk_pressure();
                    backpressure = true;
                    break;
                }
            }
        }

        // Picking up aged batches here keeps them moving even when no fresh
        // batch is closing. Skipped under backpressure so we don't pile more
        // I/O onto an already-saturated downstream.
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

        (all_staged, backpressure)
    }

    /// Phase 2: write staged batches concurrently, one task per
    /// destination (each writer has its own Mutex so destinations never
    /// contend). Returns offsets ready to commit.
    async fn write_staged(
        &self,
        staged: Vec<dfe_archiver_core::buffer::StagedBatch>,
    ) -> Vec<KafkaOffset> {
        // futures::future::join_all runs writes concurrently on this task —
        // maximises I/O overlap without spawning new tasks.
        let write_results: Vec<std::result::Result<(), Error>> = futures::future::join_all(
            staged
                .iter()
                .map(|batch| async move { self.write_batch(batch).await }),
        )
        .await;

        let mut offsets_to_commit: Vec<KafkaOffset> = Vec::new();
        let (mut cycle_ok, mut cycle_err) = (0usize, 0usize);
        for (batch, result) in staged.into_iter().zip(write_results) {
            match result {
                Ok(()) => {
                    cycle_ok += 1;
                    self.metrics.record_archived(batch.record_count as u64);
                    offsets_to_commit.extend(batch.offsets); // MOVE, no clone
                }
                Err(e) => {
                    cycle_err += 1;
                    error!(
                        error = %e,
                        destination = %batch.destination,
                        "Failed to write batch"
                    );
                    self.metrics.record_error();
                    // Surface storage-write failures per backend so the
                    // per-backend error counter is non-empty (metrics-gap audit).
                    self.metrics
                        .record_sink_error(self.startup_config.archive.backend_name());
                    self.send_to_dlq(&batch.destination, &batch.data, &e).await;
                }
            }
        }
        // Drive the sink circuit gate from this cycle's outcome. Skipped when the
        // cycle had no staged batches at all (both counters 0 -> latch unchanged).
        self.update_sink_circuit(cycle_ok, cycle_err);
        offsets_to_commit
    }

    /// Phase 3: commit Kafka offsets — the at-least-once release point.
    /// Errors are logged but not propagated; rebalance retries will
    /// re-deliver the messages and writes are idempotent at the file level
    /// (rolling on size + content hash makes duplicates harmless).
    async fn commit_offsets(&self, offsets_to_commit: Vec<KafkaOffset>) {
        if offsets_to_commit.is_empty() {
            return;
        }
        let commit_count = offsets_to_commit.len() as u64;
        debug!(count = commit_count, "Committing Kafka offsets");
        if let Err(e) = self.transport.commit(offsets_to_commit).await {
            self.metrics.record_commit_error();
            error!(error = %e, count = commit_count, "Failed to commit offsets");
        }
        self.metrics.record_commit(commit_count);
    }

    /// Push this pod's Kafka assigned-partition lag into the unified
    /// `ScalingPressure` `kafka_lag` component AND the `dfe_archiver_kafka_lag`
    /// gauge.
    ///
    /// The component is the Kafka inbound term of the composite KEDA scales on,
    /// weighted + saturated per
    /// `scaling_components()`. `assigned_lag()` sums lag over THIS pod's ASSIGNED
    /// partitions, so it is scale-invariant: as the consumer group grows each
    /// pod's lag falls and the term relaxes.
    fn push_kafka_lag_signal(&self) {
        let lag = self.transport.assigned_lag();
        // assigned_lag() is >= 0 (clamped in the adapter).
        let lag = u64::try_from(lag).unwrap_or(0);
        self.metrics.set_kafka_lag(lag);
        self.scaling.set_component("kafka_lag", lag as f64);
    }

    /// Drive the object-store sink circuit-open scaling gate from a write
    /// cycle's outcome (mirrors dfe-loader's `ClickHouse` sink latch). The sink is
    /// "dead" when a whole cycle wrote nothing but saw errors; it recovers the
    /// moment any write succeeds. The engine zeroes the composite while the
    /// circuit is open (more pods cannot relieve a dead object store).
    fn update_sink_circuit(&self, cycle_ok: usize, cycle_err: usize) {
        if cycle_ok > 0 {
            self.sink_circuit_open
                .store(false, std::sync::atomic::Ordering::Relaxed);
        } else if cycle_err > 0 {
            self.sink_circuit_open
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        let open = self
            .sink_circuit_open
            .load(std::sync::atomic::Ordering::Relaxed);
        // Feed the unified ScalingPressure circuit gate directly -- KEDA reads
        // the resulting /scaling/pressure (0.0 while open).
        self.scaling.set_circuit_open(open);
        self.metrics.set_scaling_circuit_open(open);
    }

    /// Update buffer stats, scaling pressure, and pipeline gauges
    fn update_pipeline_metrics(&self) {
        self.metrics.set_last_batch_timestamp();

        let stats = self.buffer.stats();
        self.metrics
            .set_hot_buffer_stats(stats.current_hot_buffers, stats.current_hot_bytes);
        self.metrics.set_spool_bytes(stats.current_spool_bytes);

        let writer_count = self.writers.lock().len();
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

        // Get-or-create per-destination writer under the LRU cache lock.
        // The lock guards only the cache map ops (O(1)); the actual write
        // happens on the writer's own Mutex below. If insertion overflows
        // the cap, the oldest writer is evicted and closed asynchronously.
        let (writer_arc, evicted) = {
            let mut cache = self.writers.lock();
            if let Some(w) = cache.get(batch.destination.as_str()) {
                (Arc::clone(w), None)
            } else {
                debug!(destination = %batch.destination, "Creating new archive writer");
                let writer = Arc::new(Mutex::new(self.create_writer(&batch.destination)?));
                let evicted = cache.push(batch.destination.to_string(), Arc::clone(&writer));
                // `push` only returns Some when the inserted key REPLACED an
                // existing key, OR when capacity overflowed. The replacement
                // case is impossible here because we just confirmed the key
                // wasn't present.
                let overflow = evicted.filter(|(k, _)| k != batch.destination.as_str());
                (writer, overflow)
            }
        };

        if let Some((evicted_key, evicted_writer)) = evicted {
            self.spawn_evicted_writer_close(evicted_key, evicted_writer);
        }

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
        self.metrics
            .record_flush(duration.as_secs_f64(), FlushTrigger::Size);
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

    /// Close an LRU-evicted writer asynchronously so it never blocks the
    /// hot path. The writer's own Mutex is held during close, so any
    /// concurrent writes targeting the same destination (which would have
    /// re-inserted a fresh writer into the cache) will not contend.
    fn spawn_evicted_writer_close(
        &self,
        destination: String,
        writer_arc: Arc<Mutex<ArchiveWriter>>,
    ) {
        let metrics = Arc::clone(&self.metrics);
        debug!(destination = %destination, "Evicting LRU archive writer");
        metrics.record_writer_eviction();
        tokio::spawn(async move {
            let mut writer = writer_arc.lock().await;
            match writer.close().await {
                Ok(Some(close_stats)) => {
                    debug!(
                        destination = %destination,
                        compressed_bytes = close_stats.compressed_bytes,
                        "Closed evicted writer"
                    );
                    metrics.record_file_closed(close_stats.compressed_bytes);
                }
                Ok(None) => {
                    debug!(destination = %destination, "Closed empty evicted writer");
                }
                Err(e) => {
                    error!(error = %e, destination = %destination, "Failed to close evicted writer");
                    metrics.record_error();
                }
            }
        });
    }

    /// Create a new archive writer for a destination.
    ///
    /// Uses `startup_config` for archive/compression settings (restart-required).
    fn create_writer(&self, destination: &str) -> Result<ArchiveWriter> {
        let policy = RollingPolicy {
            max_size_bytes: self.startup_config.archive.roll_size_bytes,
            max_age_secs: self.startup_config.archive.roll_interval_secs,
        };

        let compressor = compressor_for(&self.startup_config.compression)?;

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
            let mut cache = self.writers.lock();
            let mut all = Vec::with_capacity(cache.len());
            while let Some(entry) = cache.pop_lru() {
                all.push(entry);
            }
            all
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

        self.transport.close().await?;

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

        let entry = scalo::dlq::DlqEntry::new(
            "dfe-archiver",
            format!("storage_write_failed: {error}"),
            data.to_vec(),
        )
        .with_destination(destination);

        if let Err(dlq_err) = self.dlq.send(entry).await {
            error!(error = %dlq_err, destination, "DLQ send also failed");
        } else {
            self.metrics.record_dlq(1);
            scalo::logger::security::record_dlq(
                "storage_write_failed",
                &error.to_string(),
                Some(destination),
            );
        }
    }

    /// Route inbound-filter DLQ entries surfaced by the transport.
    ///
    /// These are records the scalo inbound filter dead-lettered before they
    /// reached the archiver. The archiver configures no inbound filters, so this
    /// is normally empty -- but the no-silent-drop contract means we route any
    /// entries that do arrive instead of dropping them after a metric.
    async fn route_filter_dlq(&self, entries: Vec<scalo::transport::filter::FilteredDlqEntry>) {
        if entries.is_empty() || !self.dlq.is_enabled() {
            return;
        }
        for entry in entries {
            let dest = entry.key.as_deref().unwrap_or("filter");
            let dlq_entry = scalo::dlq::DlqEntry::new("dfe-archiver", entry.reason, entry.payload)
                .with_destination(dest);
            if let Err(dlq_err) = self.dlq.send(dlq_entry).await {
                error!(error = %dlq_err, "Inbound-filter DLQ send failed");
            } else {
                self.metrics.record_dlq(1);
            }
        }
    }

    /// Get metrics handle
    pub fn metrics(&self) -> Arc<ArchiverMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Check if archiver is healthy (transport up and not under memory pressure)
    pub fn is_healthy(&self) -> bool {
        self.transport.is_healthy() && !self.memory_guard.under_pressure()
    }

    /// Sync transport health check (for `HealthRegistry` callback).
    pub fn is_transport_healthy(&self) -> bool {
        self.transport.is_healthy()
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
