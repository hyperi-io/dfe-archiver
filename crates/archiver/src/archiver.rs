// Project:   dfe-archiver
// File:      crates/archiver/src/archiver.rs
// Purpose:   Main archiver pipeline orchestrator
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! # Archiver Pipeline
//!
//! Orchestrates the complete Kafka -> Buffer -> Archive -> Storage pipeline.
//!
//! ```text
//! Kafka Consumer -> Router -> Buffer Manager -> Archive Writer -> Storage
//!       |             |           |                |              |
//!   Batch recv    Dest key    Per-dest        Compressed      S3/MinIO/
//!   (10K msgs)    routing     buffering       rolling         File/etc
//! ```
//!
//! A record's offset is released only when the archive file holding it is
//! complete: until then the file is an upload in progress that a crash
//! abandons. On Kafka the armed consumer commits each partition up to its
//! lowest offset not yet released, so one destination's roll never commits
//! past a record another destination still holds.

use crate::config::{Config, SharedConfig};
use crate::metrics::ArchiverMetrics;
use dfe_archiver_core::archive::{ArchiveWriter, RollingPolicy, Settled};
use dfe_archiver_core::buffer::{StagedBatch, TieredBufferManager};
use dfe_archiver_core::compression::compressor_for;
use dfe_archiver_core::config::ArchiveConfig;
use dfe_archiver_core::routing::Router;
use dfe_archiver_core::storage::probe_sink;
use dfe_archiver_core::types::KafkaOffset;
use dfe_archiver_core::{Error, Result};
use dfe_archiver_io::storage::create_backend;
use dfe_archiver_io::{KafkaStatsEmitter, ReceivedBatch, SourceTransport};
use lru::LruCache;
use rayon::prelude::*;
use scalo::SelfRegulationGovernor;
use scalo::logger::helpers::{log_debounced, log_sampled, log_state_change};
use scalo::memory::MemoryGuard;
use scalo::metrics::FlushTrigger;
use scalo::scaling::ScalingPressure;
use scalo::transport::filter::FilteredDlqEntry;
use scalo::transport::{DeliveryStatus, KafkaToken, SinkConfirmation};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

/// How long the shutdown drain waits on a closed source that returns nothing
/// and never reports it is empty.
const DRAIN_IDLE_LIMIT: Duration = Duration::from_secs(5);

/// Per-instance log-spam guards. Live on `Archiver` (not as module statics)
/// so state does not leak across nextest test runs that share a process.
#[derive(Default)]
struct LogSpamGuards {
    recv_error_last: AtomicU64,
    route_error_count: AtomicU64,
    backpressure_active: AtomicBool,
}

/// How one staged batch's write ended.
enum Written {
    /// In its destination's open file, which holds the offsets until it
    /// completes.
    Held { records: usize },
    /// No file took it, so the batch comes back for the dead-letter path.
    Failed(StagedBatch, Error),
}

/// Main archiver pipeline
pub struct Archiver {
    /// Startup config snapshot (for restart-required fields: transport, archive, routing, etc.)
    startup_config: Config,
    /// Shared config for hot-reloadable fields (buffer, memory, scaling tunables)
    shared_config: SharedConfig<Config>,
    /// The inbound transport, bus or direct. Its methods take `&self` and the
    /// scalo transports are `Send+Sync` with internal locking on the recv hot
    /// path, so no outer `Mutex` is required. This lets recv and release run
    /// concurrently from different async tasks.
    transport: SourceTransport,
    router: Router,
    buffer: TieredBufferManager,
    metrics: Arc<ArchiverMetrics>,
    /// Active archive writers per destination, capped LRU cache.
    /// On overflow the least-recently-used writer is evicted and closed in
    /// a spawned task. Each writer has its own `tokio::Mutex` for
    /// independent async I/O -- the outer `parking_lot::Mutex` only guards
    /// map ops (lookup / insert / evict-pop), which are O(1).
    writers: parking_lot::Mutex<LruCache<String, Arc<Mutex<ArchiveWriter>>>>,
    /// Closes of LRU-evicted writers, each returning what its file settled.
    /// Collected every cycle and awaited at drain, so an evicted file's
    /// offsets are released once it completes and never before.
    evictions: parking_lot::Mutex<JoinSet<Settled>>,
    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
    /// The unified KEDA `ScalingPressure` engine, reaching KEDA as the
    /// `dfe_scaling_pressure` gauge on the metrics listener -- the runtime
    /// starts that listener without the `/scaling/pressure` route, so nothing
    /// serves the composite over HTTP. scalo 2.9 collapsed the old dual model (the app's
    /// weighted pressure + a separate runtime signal cell) into this single
    /// engine: the components are registered via `ServiceApp::scaling_components`
    /// and the archiver's loops drive their values directly (`kafka_lag` from
    /// the consumer's position lag, `buffer_depth` from hot-buffer count,
    /// `memory` from the cgroup guard) plus the circuit gate. Shared from the
    /// runtime when `scaling` is enabled, a standalone fallback otherwise.
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
    /// Log-spam guards (per-instance -- see `LogSpamGuards` doc)
    log_guards: LogSpamGuards,
    /// One debounce cell per configured expression field, indexed as
    /// `routing.expression_fields`. A field no source carries falls through on
    /// every record, so the warn is debounced per field rather than per record.
    routing_fallback_guards: Vec<AtomicU64>,
}

/// The config sections a reload cannot reach, listed for the operator.
///
/// `Archiver::new` snapshots the config into `startup_config` and builds the
/// transport, router, buffer, DLQ and writers from it, so only
/// `kafka.batch_size` and `buffer.backpressure_pause_secs` are re-read while
/// the process runs. Everything else keeps its startup value until a restart,
/// and the reloader must say so rather than report a reload that was applied to
/// nothing.
#[must_use]
pub fn restart_required_changes(old: &Config, new: &Config) -> Vec<&'static str> {
    let mut changed = Vec::new();

    if old.transport != new.transport {
        changed.push("transport");
    }

    // The recv loop re-reads kafka.batch_size, so hold it equal and compare the
    // rest of the section.
    let mut kafka = new.kafka.clone();
    kafka.batch_size = old.kafka.batch_size;
    if old.kafka != kafka {
        changed.push("kafka");
    }

    if old.grpc != new.grpc {
        changed.push("grpc");
    }
    if old.archive != new.archive {
        changed.push("archive");
    }

    // Same treatment for the one buffer field the backpressure pause re-reads.
    let mut buffer = new.buffer.clone();
    buffer.backpressure_pause_secs = old.buffer.backpressure_pause_secs;
    if old.buffer != buffer {
        changed.push("buffer");
    }

    if old.routing != new.routing {
        changed.push("routing");
    }
    if old.compression != new.compression {
        changed.push("compression");
    }

    // scalo's DlqConfig has no PartialEq, so the section is compared as JSON --
    // exact here because it carries no redacted field. A serialisation failure
    // reports the change rather than swallowing it.
    match (
        serde_json::to_value(&old.dlq),
        serde_json::to_value(&new.dlq),
    ) {
        (Ok(before), Ok(after)) if before == after => {}
        _ => changed.push("dlq"),
    }

    changed
}

/// Drain the files a writer has opened into `files_created_total`.
///
/// Every production write path goes through here, so the success counter has
/// the same coverage as `files_closed_total` -- it is the denominator the
/// sink-failure alert divides the error rate by.
fn record_files_opened(metrics: &ArchiverMetrics, writer: &mut ArchiveWriter) {
    let opened = writer.take_files_opened();
    if opened > 0 {
        metrics.record_files_created(opened);
    }
}

/// Drain the rolls a writer completed into `archive_roll_total` and
/// `files_closed_total`.
///
/// Drained rather than returned because one `write_record` rolls on either
/// half, so a single returned `CloseStats` would report one of two closes.
fn record_rolls(metrics: &ArchiverMetrics, writer: &mut ArchiveWriter, destination: &str) {
    for stats in writer.take_rolls() {
        if let Some(trigger) = stats.trigger {
            debug!(
                destination,
                trigger,
                compressed_bytes = stats.compressed_bytes,
                "Archive file rolled"
            );
            metrics.record_archive_roll(trigger);
        }
        metrics.record_file_closed(stats.compressed_bytes);
    }
}

/// Count the expression-routing fields a record did not carry, and warn once
/// per field per minute.
///
/// `crates/core` owns no metrics, so the router reports the fallback and this
/// records it: read against `messages_received_total`, a sustained ratio of 1
/// is a field name no source in the deployment sets, which otherwise looks
/// exactly like a tenant called `unknown`.
fn record_routing_fallbacks(
    metrics: &ArchiverMetrics,
    fields: &[String],
    guards: &[AtomicU64],
    fallbacks: &[usize],
) {
    for &index in fallbacks {
        let Some(field) = fields.get(index) else {
            continue;
        };
        metrics.record_routing_fallback(field);
        if guards
            .get(index)
            .is_some_and(|guard| log_debounced(guard, 60_000))
        {
            warn!(
                field = %field,
                "Expression-routing field missing from the record -- archiving under the default segment (debounced, max 1/60s)"
            );
        }
    }
}

/// The commit tokens of `offsets`.
fn tokens_of(offsets: Vec<KafkaOffset>) -> Vec<KafkaToken> {
    offsets.into_iter().map(KafkaOffset::into_token).collect()
}

/// What a completed archive file proves: a completion the object store
/// answered, or a file written on this node.
///
/// Decided by the destination scheme, as `create_backend` decides the backend.
fn sink_confirmation(archive: &ArchiveConfig) -> SinkConfirmation {
    let destination = archive.destination.as_str();
    if destination.starts_with("file://") || !destination.contains("://") {
        SinkConfirmation::Local
    } else {
        SinkConfirmation::Remote
    }
}

/// Map the operator's `buffer` section onto the tiered buffer's own config.
fn buffer_config(config: &Config) -> dfe_archiver_core::buffer::TieredBufferConfig {
    dfe_archiver_core::buffer::TieredBufferConfig {
        max_hot_buffers: 64,
        hot_buffer_size: 1024 * 1024, // 1MB
        hot_buffer_age_secs: config.buffer.flush_age_secs,
        spool_dir: config.buffer.spool_dir.clone().into(),
        max_writers: config.buffer.writer_parallelism,
        staging_batch_size: config.buffer.flush_bytes,
        max_spool_bytes: 10 * 1024 * 1024 * 1024, // 10GB
        min_free_disk_bytes: 1024 * 1024 * 1024,  // 1GB
        spool_compression: true,
    }
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
    /// `Some`, attaches the inbound brake to the receiver -- the Kafka
    /// pause-partitions gate, or the Push listener's `Unavailable` shed
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
        let transport = SourceTransport::from_config(&config, governor).await?;

        let (guarantee, reason) = transport.guarantee(sink_confirmation(&config.archive));
        metrics.set_delivery_guarantee(guarantee.as_str(), reason.as_str());
        info!(
            guarantee = guarantee.as_str(),
            reason = reason.as_str(),
            "pipeline delivery guarantee"
        );

        let router = Router::new(config.routing.clone());

        let buffer = TieredBufferManager::new(buffer_config(&config))?;

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

        // Start rdkafka stats sidecar (non-fatal if it fails). Skipped on the
        // direct transport, which reaches no broker to collect stats from.
        let stats_emitter = if config.is_direct() {
            None
        } else {
            match KafkaStatsEmitter::new(&config.kafka) {
                Ok(emitter) => Some(emitter),
                Err(e) => {
                    warn!(error = %e, "Failed to start Kafka stats emitter (non-fatal)");
                    None
                }
            }
        };

        let cancel = CancellationToken::new();

        // Create DLQ. The Kafka backend rides the same config conversion as
        // the consumer transport -- dead-letters land on the broker the data
        // came from.
        let mut dlq_config = config.dlq.clone();
        let dlq_kafka = if config.is_direct() {
            // No broker to dead-letter to, and the file backend is an EROFS
            // no-op on a read-only rootfs, so there is no backend to offer.
            if dlq_config.enabled {
                warn!("no broker on the direct transport -- the DLQ is disabled");
                dlq_config.enabled = false;
            }
            None
        } else {
            Some(dfe_archiver_io::kafka::convert_config(&config.kafka))
        };
        // Not tied to `cancel`: the shutdown drain still dead-letters after the
        // loop stops, and `drain` shuts the DLQ down last.
        let dlq = scalo::dlq::Dlq::spawn(
            &dlq_config,
            "dfe-archiver",
            dlq_kafka.as_ref(),
            CancellationToken::new(),
        )
        .map_err(|e| Error::Config(format!("DLQ init failed: {e}")))?;
        if dlq_config.enabled {
            info!(mode = ?dlq_config.mode, "DLQ enabled");
        }

        info!(
            transport = %config.transport,
            brokers = %config.kafka.brokers.join(","),
            topics = %config.kafka.topics.join(","),
            destination = %config.archive.destination,
            "Archiver initialized"
        );

        // Writer LRU cache -- clamp to at least 1 to satisfy NonZeroUsize.
        let writer_cap = NonZeroUsize::new(config.archive.max_writers).unwrap_or(NonZeroUsize::MIN);

        let routing_fallback_guards = config
            .routing
            .expression_fields
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect();

        Ok(Self {
            startup_config: config,
            shared_config,
            transport,
            router,
            buffer,
            metrics,
            writers: parking_lot::Mutex::new(LruCache::new(writer_cap)),
            evictions: parking_lot::Mutex::new(JoinSet::new()),
            cancel,
            scaling,
            sink_circuit_open: AtomicBool::new(false),
            memory_guard,
            _stats_emitter: stats_emitter,
            dlq: Arc::new(dlq),
            log_guards: LogSpamGuards::default(),
            routing_fallback_guards,
        })
    }

    /// Check the inbound transport (it connects or binds on creation) and prove
    /// the archive sink answers.
    ///
    /// # Errors
    /// Returns an error when the inbound transport is not serving.
    pub async fn check_connection(&self) -> Result<()> {
        if !self.transport.is_healthy() {
            return Err(Error::transport("inbound transport is not healthy"));
        }
        info!(transport = %self.startup_config.transport, "Inbound transport verified");
        self.verify_sink().await;
        Ok(())
    }

    /// List one object under the configured prefix and latch what came back.
    ///
    /// A sink that is out never blocks startup -- refusing to start brings no
    /// object store back, and the archiver keeps consuming, buffering and
    /// dead-lettering while it recovers.
    async fn verify_sink(&self) {
        let destination = &self.startup_config.archive.destination;
        let backend = self.startup_config.archive.backend_name();
        let probe = match create_backend(&self.startup_config.archive) {
            Ok(client) => probe_sink(client.as_ref()).await,
            Err(e) => Err(e),
        };
        match probe {
            Ok(()) => {
                info!(backend, destination = %destination, "Archive sink verified");
                self.set_sink_circuit(false);
            }
            Err(e) => {
                warn!(
                    error = %e,
                    backend,
                    destination = %destination,
                    "Archive sink did not answer -- degraded until a write succeeds"
                );
                self.set_sink_circuit(true);
            }
        }
    }

    /// Run the main archiver loop
    ///
    /// This runs until shutdown is signaled via the cancellation token.
    #[instrument(skip(self))]
    pub async fn run(&self) -> Result<()> {
        info!("Starting archiver main loop");

        // Independent flush timer -- ensures aged batches flush even when no data is flowing.
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
                    // memory pressure) must still surface its backlog so KEDA can
                    // scale the group out. Cheaper + fresher than the engine's own
                    // 15s tick.
                    self.push_kafka_lag_signal();
                    let aged = self.buffer.flush_aged();
                    if !aged.is_empty() {
                        debug!(count = aged.len(), "Flushing aged batches (timer)");
                    }
                    let mut settled = self.write_staged(aged).await;
                    // Runs after the aged batches so an active destination rolls
                    // through the write path, leaving only idle files here.
                    settled.absorb(self.close_aged_writers().await);
                    settled.absorb(self.collect_evictions());
                    self.release_settled(settled).await;
                }

                result = async {
                    let batch_size = self.shared_config.with(|c| c.kafka.batch_size);
                    let recv_start = Instant::now();
                    let result = self.transport.recv(batch_size).await;
                    self.metrics.record_recv_duration(recv_start.elapsed().as_secs_f64());
                    result
                } => {
                    // Push the per-pod Kafka inbound scaling signal each poll
                    // (mirrors dfe-loader). position_lag() sums records past THIS
                    // pod's read position -- scale-invariant.
                    self.push_kafka_lag_signal();
                    match result {
                        Ok(batch) if !batch.is_empty() => self.handle_batch(batch).await,
                        Ok(_) => {
                            // Empty batch -- no messages available (or partitions paused)
                        }
                        Err(Error::Shutdown) => {
                            info!("The inbound transport closed, exiting main loop");
                            return Ok(());
                        }
                        Err(e) => {
                            if log_debounced(&self.log_guards.recv_error_last, 5000) {
                                error!(error = %e, "Failed to receive from the inbound transport (debounced, max 1/5s)");
                            }
                            self.metrics.record_error();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    /// Route, write and release one received block.
    async fn handle_batch(&self, batch: ReceivedBatch) {
        let ReceivedBatch {
            messages,
            filtered,
            dlq_entries,
        } = batch;
        self.route_filter_dlq(dlq_entries, filtered).await;
        if !messages.is_empty() {
            self.process_messages(messages).await;
            self.metrics.update_eps();
        }
    }

    /// Process a batch of messages: route -> write -> release -> update metrics.
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
        // position lag in `push_kafka_lag_signal` (scale-invariant), NOT by
        // per-batch throughput, so nothing is set here.
        let batch_bytes: u64 = messages.iter().map(|m| m.payload.len() as u64).sum();
        self.memory_guard.add_bytes(batch_bytes);

        // Phase 1: route + buffer accumulate (returns staged batches and a
        // backpressure flag set when the buffer rejects a push).
        let (staged, backpressure) = self.route_batch(messages);

        // Phase 2: write into each destination's open file. A roll completes
        // the previous file, which settles its offsets.
        let mut settled = self.write_staged(staged).await;
        settled.absorb(self.collect_evictions());

        // Phase 3: release the offsets of completed files -- the at-least-once
        // release point.
        self.release_settled(settled).await;

        if backpressure {
            let pause_secs = self
                .shared_config
                .with(|c| c.buffer.backpressure_pause_secs);
            tokio::time::sleep(Duration::from_secs(pause_secs)).await;
        } else if log_state_change(&self.log_guards.backpressure_active, false) {
            info!("Backpressure cleared -- normal processing resumed");
        }

        self.update_pipeline_metrics();
    }

    /// Phase 1: parallel route + sequential buffer push.
    ///
    /// Returns the staged batches (including any aged buffers picked up
    /// while we're here) and a `backpressure` flag -- set if the buffer
    /// rejected a push, in which case aged-flush is skipped and the caller
    /// is expected to pause.
    fn route_batch(
        &self,
        messages: Vec<dfe_archiver_core::KafkaMessage>,
    ) -> (Vec<StagedBatch>, bool) {
        // Phase 1a: Parallel route computation. `Router::route` is pure
        // (`&self, &KafkaMessage`) so par_iter is sound. Expression-routed
        // configs do a sonic-rs JSON parse per message here.
        let route_results: Vec<dfe_archiver_core::routing::Routed> = messages
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
                    dfe_archiver_core::routing::Routed {
                        destination: msg.topic.clone(),
                        fallback_fields: Vec::new(),
                    }
                })
            })
            .collect();

        // Phase 1b: Sequential buffer push (mutable buffer state).
        let mut all_staged: Vec<StagedBatch> = Vec::new();
        let mut backpressure = false;
        for (message, routed) in messages.into_iter().zip(route_results) {
            let dfe_archiver_core::routing::Routed {
                destination,
                fallback_fields,
            } = routed;
            record_routing_fallbacks(
                &self.metrics,
                self.router.expression_fields(),
                &self.routing_fallback_guards,
                &fallback_fields,
            );
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
                        warn!(error = %e, "Buffer push failed -- backpressure active");
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

    /// Phase 2: write staged batches concurrently, one task per destination
    /// (each writer has its own Mutex so destinations never contend), and
    /// dead-letter the ones no file took. Returns what the writes settled.
    async fn write_staged(&self, staged: Vec<StagedBatch>) -> Settled {
        // futures::future::join_all runs writes concurrently on this task --
        // maximises I/O overlap without spawning new tasks.
        let written = futures::future::join_all(
            staged
                .into_iter()
                .map(|batch| async move { self.write_batch(batch).await }),
        )
        .await;

        let mut settled = Settled::default();
        let (mut cycle_ok, mut cycle_err) = (0usize, 0usize);
        for (outcome, rolled) in written {
            settled.absorb(rolled);
            match outcome {
                Written::Held { records } => {
                    cycle_ok += 1;
                    self.metrics.record_archived(records as u64);
                }
                Written::Failed(batch, e) => {
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
                    self.dead_letter(batch, &e).await;
                }
            }
        }
        // Drive the sink circuit gate from this cycle's outcome. Skipped when the
        // cycle had no staged batches at all (both counters 0 -> latch unchanged).
        self.update_sink_circuit(cycle_ok, cycle_err);
        settled
    }

    /// Release `tokens` with `status`. A failed commit is counted and logged
    /// and not retried here: the records are settled, and the next commit on
    /// the partition covers them.
    async fn release(&self, tokens: Vec<KafkaToken>, status: DeliveryStatus) {
        if tokens.is_empty() || !self.transport.holds_offsets() {
            return;
        }
        let count = tokens.len() as u64;
        match self.transport.release(&tokens, status).await {
            Ok(()) => {
                if status.should_commit() {
                    self.metrics.record_commit(count);
                }
            }
            Err(e) => {
                self.metrics.record_commit_error();
                error!(error = %e, count, ?status, "Failed to release offsets");
            }
        }
    }

    /// Release what the writers settled: the offsets of completed files
    /// `Delivered`, of files whose completion failed `Errored`, which keeps
    /// them below every later commit until a restart reads them again.
    async fn release_settled(&self, settled: Settled) {
        self.release(settled.delivered.tokens(), DeliveryStatus::Delivered)
            .await;
        self.release(settled.errored.tokens(), DeliveryStatus::Errored)
            .await;
    }

    /// Push this pod's Kafka position lag into the unified `ScalingPressure`
    /// `kafka_lag` component AND the `dfe_archiver_kafka_lag` gauge.
    ///
    /// The component is the Kafka inbound term of the composite KEDA scales on,
    /// weighted + saturated per `scaling_components()`. Position lag counts the
    /// records past THIS pod's read position, so it is scale-invariant -- as
    /// the consumer group grows each pod's lag falls -- and the commit an open
    /// file holds does not inflate it. The committed-offset lag would, by up to
    /// a roll interval of intake, and scale the archiver out on the hold rather
    /// than on backlog. The direct transport keeps no backlog this pod can
    /// read, so it leaves the component at whatever the engine last held
    /// rather than writing a false zero.
    fn push_kafka_lag_signal(&self) {
        let Some(lag) = self.transport.position_lag() else {
            return;
        };
        // position_lag() is >= 0 (clamped in the adapter).
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
        let open = if cycle_ok > 0 {
            false
        } else if cycle_err > 0 {
            true
        } else {
            self.sink_circuit_open
                .load(std::sync::atomic::Ordering::Relaxed)
        };
        self.set_sink_circuit(open);
    }

    /// Latch the sink circuit and publish it everywhere it is read.
    fn set_sink_circuit(&self, open: bool) {
        self.sink_circuit_open
            .store(open, std::sync::atomic::Ordering::Relaxed);
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

    /// The destination's writer, created on first use.
    ///
    /// The LRU lock guards only the map ops (O(1)), and the write happens on
    /// the writer's own Mutex. If an insertion overflows the cap, the oldest
    /// writer is evicted and closed in the background.
    fn writer_for(&self, destination: &str) -> Result<Arc<Mutex<ArchiveWriter>>> {
        let (writer, evicted) = {
            let mut cache = self.writers.lock();
            if let Some(w) = cache.get(destination) {
                (Arc::clone(w), None)
            } else {
                debug!(destination, "Creating new archive writer");
                let writer = Arc::new(Mutex::new(self.create_writer(destination)?));
                let evicted = cache.push(destination.to_string(), Arc::clone(&writer));
                // `push` only returns Some when the inserted key REPLACED an
                // existing key, OR when capacity overflowed. The replacement
                // case is impossible here because we just confirmed the key
                // wasn't present.
                let overflow = evicted.filter(|(k, _)| k != destination);
                (writer, overflow)
            }
        };

        if let Some((evicted_key, evicted_writer)) = evicted {
            self.spawn_evicted_writer_close(evicted_key, evicted_writer);
        }
        Ok(writer)
    }

    /// Write one staged batch into its destination's open file.
    ///
    /// Once written, the batch's offsets are held against that file and
    /// released when it completes. A roll inside the write completes the
    /// previous file, whose offsets come back settled either way.
    #[instrument(skip(self, batch), fields(destination = %batch.destination, records = batch.record_count))]
    async fn write_batch(&self, batch: StagedBatch) -> (Written, Settled) {
        let start = Instant::now();
        trace!(
            destination = %batch.destination,
            records = batch.record_count,
            bytes = batch.data.len(),
            offsets = batch.offsets.len(),
            "Starting batch write"
        );

        let writer_arc = match self.writer_for(batch.destination.as_str()) {
            Ok(writer) => writer,
            Err(e) => return (Written::Failed(batch, e), Settled::default()),
        };

        // Lock only THIS destination's writer -- other destinations can write concurrently
        let mut writer = writer_arc.lock().await;

        // Write data (may trigger a roll), then flush it into the file.
        let written = match writer.write(&batch.data).await {
            Ok(()) => writer.flush().await,
            Err(e) => Err(e),
        };
        record_files_opened(&self.metrics, &mut writer);
        record_rolls(&self.metrics, &mut writer, batch.destination.as_str());
        // The batch leaves memory whether a file took it or the DLQ does.
        self.memory_guard.release(batch.data.len() as u64);

        let flush_stats = match written {
            Ok(flush_stats) => flush_stats,
            Err(e) => return (Written::Failed(batch, e), writer.take_settled()),
        };
        let StagedBatch {
            destination,
            data,
            offsets,
            record_count,
        } = batch;
        if self.transport.holds_offsets() {
            writer.hold(offsets);
        }
        let settled = writer.take_settled();
        drop(writer);

        if let Some(flush_stats) = flush_stats {
            let ratio = if flush_stats.uncompressed_bytes > 0 {
                flush_stats.compressed_bytes as f64 / flush_stats.uncompressed_bytes as f64
            } else {
                1.0
            };
            debug!(
                destination = %destination,
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

        let duration = start.elapsed();
        let backend = self.startup_config.archive.backend_name();
        self.metrics
            .record_flush(duration.as_secs_f64(), FlushTrigger::Size);
        self.metrics.record_bytes_written(data.len() as u64);
        self.metrics.record_batch_size(data.len() as u64);
        self.metrics
            .record_sink_duration(backend, duration.as_secs_f64());

        debug!(
            destination = %destination,
            records = record_count,
            bytes = data.len(),
            duration_ms = duration.as_millis(),
            backend,
            "Wrote batch to archive"
        );

        (
            Written::Held {
                records: record_count,
            },
            settled,
        )
    }

    /// Close every writer whose open file has outlived the rolling policy,
    /// and return what the closed files settled.
    ///
    /// `write` is the only other thing that consults the policy, so without
    /// this a destination that stops receiving data holds its file open until
    /// shutdown, and an active one overruns the interval by however long it
    /// waits for the next batch.
    async fn close_aged_writers(&self) -> Settled {
        // The parking_lot lock guards the map only, so the writers are
        // snapshotted and it is released before awaiting any writer's mutex.
        let writers: Vec<(String, Arc<Mutex<ArchiveWriter>>)> = {
            let cache = self.writers.lock();
            cache
                .iter()
                .map(|(dest, writer)| (dest.clone(), Arc::clone(writer)))
                .collect()
        };

        let mut settled = Settled::default();
        for (dest, writer_arc) in writers {
            let mut writer = writer_arc.lock().await;
            match writer.close_if_aged().await {
                Ok(Some(close_stats)) => {
                    debug!(
                        destination = %dest,
                        trigger = close_stats.trigger.unwrap_or("age"),
                        compressed_bytes = close_stats.compressed_bytes,
                        "Closed aged archive file (timer)"
                    );
                    if let Some(trigger) = close_stats.trigger {
                        self.metrics.record_archive_roll(trigger);
                    }
                    self.metrics
                        .record_file_closed(close_stats.compressed_bytes);
                }
                Ok(None) => {}
                Err(e) => {
                    error!(error = %e, destination = %dest, "Failed to close aged writer");
                    self.metrics.record_error();
                }
            }
            settled.absorb(writer.take_settled());
        }
        settled
    }

    /// Close an LRU-evicted writer in the background so it never blocks the
    /// hot path. The task returns what the file settled, which
    /// [`collect_evictions`](Self::collect_evictions) and
    /// [`await_evictions`](Self::await_evictions) release. The writer's own
    /// Mutex is held during close, and a write to the same destination
    /// meanwhile goes to a fresh writer, whose file names never collide with
    /// this one's.
    fn spawn_evicted_writer_close(
        &self,
        destination: String,
        writer_arc: Arc<Mutex<ArchiveWriter>>,
    ) {
        let metrics = Arc::clone(&self.metrics);
        debug!(destination = %destination, "Evicting LRU archive writer");
        metrics.record_writer_eviction();
        self.evictions.lock().spawn(async move {
            let mut writer = writer_arc.lock().await;
            // A write that errored after rolling left its stats undrained.
            record_rolls(&metrics, &mut writer, &destination);
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
            writer.take_settled()
        });
    }

    /// What the evicted writers that finished closing settled, without
    /// waiting on the ones still closing.
    fn collect_evictions(&self) -> Settled {
        let mut settled = Settled::default();
        let mut closing = self.evictions.lock();
        while let Some(joined) = closing.try_join_next() {
            match joined {
                Ok(closed) => settled.absorb(closed),
                Err(e) => {
                    error!(error = %e, "An evicted writer's close did not finish; its offsets stay held until a restart");
                }
            }
        }
        settled
    }

    /// Wait for every evicted writer still closing, and return what they
    /// settled.
    async fn await_evictions(&self) -> Settled {
        let mut closing = std::mem::take(&mut *self.evictions.lock());
        let mut settled = Settled::default();
        while let Some(joined) = closing.join_next().await {
            match joined {
                Ok(closed) => settled.absorb(closed),
                Err(e) => {
                    error!(error = %e, "An evicted writer's close did not finish; its offsets stay held until a restart");
                }
            }
        }
        settled
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

    /// Drain the pipeline for shutdown: stop the source, write everything it
    /// already accepted, complete every file, then release.
    ///
    /// Call once [`run`](Self::run) has returned. The source closes first, so
    /// the Push listener stops answering new pushes while every push it already
    /// answered is received and written. On Kafka the consumer stops fetching
    /// and its commit still lands.
    #[instrument(skip(self))]
    pub async fn drain(&self) {
        if let Err(e) = self.transport.close().await {
            warn!(error = %e, "Closing the inbound transport failed; draining what it holds");
        }
        let received = self.drain_source().await;

        let batches = self.buffer.flush_all();
        info!(
            received,
            batches = batches.len(),
            total_bytes = batches.iter().map(|b| b.data.len()).sum::<usize>(),
            "Draining buffers"
        );
        let mut settled = self.write_staged(batches).await;
        settled.absorb(self.await_evictions().await);
        settled.absorb(self.close_all_writers().await);
        self.release_settled(settled).await;

        if let Err(e) = self.dlq.shutdown().await {
            warn!(error = %e, "DLQ shutdown failed");
        }
        info!("Drain complete");
    }

    /// Receive from the closed source until it reports it is empty, pushing
    /// each block through the normal path. Returns the records received.
    async fn drain_source(&self) -> usize {
        let batch_size = self.shared_config.with(|c| c.kafka.batch_size);
        let mut received = 0usize;
        let mut idle_since = Instant::now();
        loop {
            match self.transport.recv(batch_size).await {
                Ok(batch) if !batch.is_empty() => {
                    received += batch.messages.len();
                    self.handle_batch(batch).await;
                    idle_since = Instant::now();
                }
                Ok(_) if idle_since.elapsed() < DRAIN_IDLE_LIMIT => {}
                Ok(_) => {
                    warn!(
                        received,
                        "Shutdown drain gave up: the source returned nothing and never reported it was empty"
                    );
                    return received;
                }
                Err(Error::Shutdown) => return received,
                Err(e) => {
                    warn!(error = %e, received, "Shutdown drain stopped on a receive error");
                    return received;
                }
            }
        }
    }

    /// Complete every open file, concurrently, and return what they settled.
    async fn close_all_writers(&self) -> Settled {
        let writers: Vec<(String, Arc<Mutex<ArchiveWriter>>)> = {
            let mut cache = self.writers.lock();
            let mut all = Vec::with_capacity(cache.len());
            while let Some(entry) = cache.pop_lru() {
                all.push(entry);
            }
            all
        };
        debug!(writer_count = writers.len(), "Closing archive writers");

        let closed = futures::future::join_all(writers.into_iter().map(|(dest, writer_arc)| {
            let metrics = &self.metrics;
            async move {
                let mut writer = writer_arc.lock().await;
                // A write that errored after rolling left its stats undrained.
                record_rolls(metrics, &mut writer, &dest);
                match writer.close().await {
                    Ok(Some(close_stats)) => {
                        debug!(
                            destination = %dest,
                            compressed_bytes = close_stats.compressed_bytes,
                            "Closed archive writer"
                        );
                        metrics.record_file_closed(close_stats.compressed_bytes);
                    }
                    Ok(None) => {
                        debug!(destination = %dest, "Closed empty archive writer");
                    }
                    Err(e) => {
                        error!(error = %e, destination = %dest, "Failed to close writer");
                    }
                }
                writer.take_settled()
            }
        }))
        .await;

        let mut settled = Settled::default();
        for one in closed {
            settled.absorb(one);
        }
        settled
    }

    /// Dead-letter a batch no file took, and release its offsets: `Rejected`
    /// once the DLQ confirms it holds the batch, `Errored` when the DLQ
    /// refuses it or is off, which keeps them below every later commit until
    /// a restart reads the records again.
    async fn dead_letter(&self, batch: StagedBatch, error: &Error) {
        let StagedBatch {
            destination,
            data,
            offsets,
            record_count,
        } = batch;
        let status = if self.dlq.is_enabled() {
            debug!(
                destination = %destination,
                data_bytes = data.len(),
                error = %error,
                "Sending failed batch to DLQ"
            );
            let entry = scalo::dlq::DlqEntry::new(
                "dfe-archiver",
                format!("storage_write_failed: {error}"),
                data,
            )
            .with_destination(destination.as_str());
            match self.dlq.write_confirmed(vec![entry]).await {
                Ok(()) => {
                    self.metrics.record_dlq(record_count as u64);
                    scalo::logger::security::record_dlq(
                        "storage_write_failed",
                        &error.to_string(),
                        Some(destination.as_str()),
                    );
                    DeliveryStatus::Rejected
                }
                Err(dlq_err) => {
                    error!(
                        error = %dlq_err,
                        destination = %destination,
                        "The DLQ refused a batch no file took; its offsets stay held until a restart reads it again"
                    );
                    DeliveryStatus::Errored
                }
            }
        } else {
            trace!(
                destination = %destination,
                "DLQ disabled; a batch no file took keeps its offsets held until a restart reads it again"
            );
            DeliveryStatus::Errored
        };
        self.release(tokens_of(offsets), status).await;
    }

    /// Route inbound-filter DLQ entries surfaced by the transport, then release
    /// the offsets of the records the filter removed with the outcome.
    ///
    /// These are records the scalo inbound filter took out before they reached
    /// the archiver. The archiver configures no inbound filters, so both are
    /// normally empty -- but the no-silent-drop contract means we route any
    /// entries that do arrive, and a held source waits on every offset it
    /// handed out.
    async fn route_filter_dlq(&self, entries: Vec<FilteredDlqEntry>, filtered: Vec<KafkaOffset>) {
        if entries.is_empty() && filtered.is_empty() {
            return;
        }
        let status = if entries.is_empty() {
            // Only a drop filter matched: removed by policy, not lost.
            DeliveryStatus::Dropped
        } else {
            let count = entries.len() as u64;
            let dead_letters = entries
                .into_iter()
                .map(|entry| {
                    let destination = entry.key.as_deref().unwrap_or("filter").to_string();
                    scalo::dlq::DlqEntry::new("dfe-archiver", entry.reason, entry.payload)
                        .with_destination(&destination)
                })
                .collect();
            match self.dlq.write_confirmed(dead_letters).await {
                Ok(()) if self.dlq.is_enabled() => {
                    self.metrics.record_dlq(count);
                    DeliveryStatus::Rejected
                }
                // A disabled DLQ counts what it drops in dlq_dropped_total.
                Ok(()) => DeliveryStatus::Dropped,
                Err(dlq_err) => {
                    error!(
                        error = %dlq_err,
                        "The DLQ refused inbound-filter dead letters; their offsets stay held until a restart"
                    );
                    DeliveryStatus::Errored
                }
            }
        };
        self.release(tokens_of(filtered), status).await;
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

    /// Archive-sink status for the `HealthRegistry` callback, seeded by the
    /// startup probe and driven thereafter by each write cycle's outcome.
    ///
    /// Degraded rather than Unhealthy: taking this pod out of readiness brings
    /// no object store back, and the archiver keeps consuming, buffering and
    /// dead-lettering while the sink is out.
    pub fn sink_health(&self) -> scalo::health::HealthStatus {
        if self
            .sink_circuit_open
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            scalo::health::HealthStatus::Degraded
        } else {
            scalo::health::HealthStatus::Healthy
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{
        buffer_config, record_files_opened, record_rolls, record_routing_fallbacks,
        restart_required_changes, sink_confirmation,
    };
    use crate::config::validate_config;
    use crate::metrics::ArchiverMetrics;
    use dfe_archiver_core::archive::{ArchiveWriter, RollingPolicy};
    use dfe_archiver_core::buffer::DEFAULT_SPOOL_DIR;
    use dfe_archiver_core::compression::compressor_for;
    use dfe_archiver_core::config::{ArchiveConfig, CompressionConfig, Config, RoutingConfig};
    use dfe_archiver_core::routing::Router;
    use dfe_archiver_core::types::KafkaMessage;
    use dfe_archiver_io::storage::create_backend;
    use scalo::transport::SinkConfirmation;
    use std::path::Path;
    use std::sync::atomic::AtomicU64;

    /// A completed object-store upload is the store's own answer, while a
    /// local file is written on this node only.
    #[test]
    fn the_destination_scheme_decides_what_a_completed_file_proves() {
        let archive = |destination: &str| ArchiveConfig {
            destination: destination.to_string(),
            ..ArchiveConfig::default()
        };
        for remote in [
            "s3://bucket/archive",
            "minio://bucket/archive",
            "gs://bucket/archive",
            "az://container/archive",
        ] {
            assert_eq!(
                sink_confirmation(&archive(remote)),
                SinkConfirmation::Remote,
                "{remote}"
            );
        }
        for local in ["file:///srv/archive", "/srv/archive"] {
            assert_eq!(
                sink_confirmation(&archive(local)),
                SinkConfirmation::Local,
                "{local}"
            );
        }
    }

    #[test]
    fn test_cleared_brokers_fails_validation() {
        let mut config = Config::default();
        config.kafka.brokers.clear();
        assert!(
            validate_config(&config).is_err(),
            "empty brokers should fail"
        );
    }

    /// The configured spool has to be the directory the buffer creates, not a
    /// path compiled into the binary.
    #[test]
    fn test_configured_spool_dir_reaches_the_buffer() {
        let mut config = Config::default();
        config.buffer.spool_dir = "/srv/dfe/spool".to_string();

        assert_eq!(
            buffer_config(&config).spool_dir,
            Path::new("/srv/dfe/spool"),
        );
    }

    /// A default deployment writes under the path the image pre-creates, and
    /// an absolute one, because the container working directory is root-owned.
    #[test]
    fn test_default_spool_dir_is_the_absolute_image_path() {
        let config = Config::default();

        assert_eq!(config.buffer.spool_dir, DEFAULT_SPOOL_DIR);
        assert_eq!(
            buffer_config(&config).spool_dir,
            Path::new("/var/spool/dfe/archiver"),
        );
    }

    /// The success counter is the denominator the sink-failure alert divides
    /// the error rate by, so it has to move when the pipeline opens a file.
    #[tokio::test]
    async fn test_opening_a_file_moves_files_created_total() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = ArchiverMetrics::register(&manager, "test");

        let dir = tempfile::TempDir::new().expect("temp dir");
        let archive = ArchiveConfig {
            destination: format!("file://{}", dir.path().display()),
            path_template: "{year}/{month}/{day}/{hour}/archive".to_string(),
            ..ArchiveConfig::default()
        };
        let compressor = compressor_for(&CompressionConfig::default()).expect("compressor");
        let storage = create_backend(&archive).expect("backend");
        let mut writer = ArchiveWriter::new(archive, RollingPolicy::default(), compressor, storage);

        writer.write(b"one\n").await.expect("write");
        record_files_opened(&metrics, &mut writer);

        let rendered = manager.render();
        assert!(
            rendered
                .lines()
                .any(|line| line == "archiver_files_created_total 1"),
            "files_created_total did not move after a write:\n{rendered}"
        );
    }

    /// A second write to the same open file must not count another creation.
    #[tokio::test]
    async fn test_writing_to_an_open_file_creates_nothing() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let archive = ArchiveConfig {
            destination: format!("file://{}", dir.path().display()),
            path_template: "{year}/{month}/{day}/{hour}/archive".to_string(),
            ..ArchiveConfig::default()
        };
        let compressor = compressor_for(&CompressionConfig::default()).expect("compressor");
        let storage = create_backend(&archive).expect("backend");
        let mut writer = ArchiveWriter::new(archive, RollingPolicy::default(), compressor, storage);

        writer.write(b"one\n").await.expect("write");
        assert_eq!(writer.take_files_opened(), 1);

        writer.write(b"two\n").await.expect("write");
        assert_eq!(writer.take_files_opened(), 0);
    }

    /// A field no source carries is otherwise indistinguishable from a tenant
    /// named `unknown`, so the counter has to move on the router's own output
    /// rather than on a hand-built index list.
    #[test]
    fn test_a_missing_routing_field_moves_routing_fallback_total() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = ArchiverMetrics::register(&manager, "test");

        let fields = vec!["org_id".to_string()];
        let router = Router::new(RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: fields.clone(),
            default_segment: "unknown".to_string(),
        });
        let guards: Vec<AtomicU64> = fields.iter().map(|_| AtomicU64::new(0)).collect();

        let carried =
            KafkaMessage::for_test(br#"{"org_id":"acme"}"#.to_vec(), "default_land", 0, 0);
        let missing =
            KafkaMessage::for_test(br#"{"customer":"acme"}"#.to_vec(), "default_land", 0, 1);

        for message in [&carried, &missing] {
            let outcome = router.route(message).expect("route");
            record_routing_fallbacks(&metrics, &fields, &guards, &outcome.fallback_fields);
        }

        let rendered = manager.render();
        assert!(
            rendered
                .lines()
                .any(|line| line == r#"archiver_routing_fallback_total{field="org_id"} 1"#),
            "the record carrying org_id must not count as a fallback:\n{rendered}"
        );
    }

    /// Both halves of a `write_record` can roll, so the close counter is
    /// drained rather than returned -- returning one dropped the other.
    #[tokio::test]
    async fn test_every_roll_reaches_files_closed_total() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = ArchiverMetrics::register(&manager, "test");

        let dir = tempfile::TempDir::new().expect("temp dir");
        let archive = ArchiveConfig {
            destination: format!("file://{}", dir.path().display()),
            path_template: "{year}/{month}/{day}/{hour}/archive".to_string(),
            ..ArchiveConfig::default()
        };
        let compressor = compressor_for(&CompressionConfig::default()).expect("compressor");
        let storage = create_backend(&archive).expect("backend");
        // A zero-second interval rolls on each half of a write_record.
        let policy = RollingPolicy {
            max_size_bytes: 1024 * 1024,
            max_age_secs: 0,
        };
        let mut writer = ArchiveWriter::new(archive, policy, compressor, storage);

        writer.write_record(b"first").await.expect("write");
        writer.write_record(b"second").await.expect("write");
        record_rolls(&metrics, &mut writer, "default_land");

        let rendered = manager.render();
        assert!(
            rendered
                .lines()
                .any(|line| line == "archiver_files_closed_total 3"),
            "files_closed_total dropped a roll:\n{rendered}"
        );
        assert!(
            rendered
                .lines()
                .any(|line| line == r#"archiver_archive_roll_total{trigger="age"} 3"#),
            "archive_roll_total dropped a roll:\n{rendered}"
        );
    }

    /// A new destination is bound at construction, so the reloader reports it
    /// rather than claiming the running archiver picked it up.
    #[test]
    fn test_changed_destination_is_restart_required() {
        let old = Config::default();
        let mut new = old.clone();
        new.archive.destination = "file:///srv/dfe/archive".to_string();

        assert_eq!(restart_required_changes(&old, &new), vec!["archive"]);
    }

    /// The two fields the running pipeline re-reads are reported as applied.
    #[test]
    fn test_hot_reloaded_fields_are_not_restart_required() {
        let old = Config::default();
        let mut new = old.clone();
        new.kafka.batch_size = old.kafka.batch_size + 1;
        new.buffer.backpressure_pause_secs = old.buffer.backpressure_pause_secs + 1;

        assert!(restart_required_changes(&old, &new).is_empty());
    }

    /// The consumer is armed at construction, so turning acknowledgements off
    /// on a running archiver changes nothing until it restarts.
    #[test]
    fn test_acknowledgements_are_restart_required() {
        let old = Config::default();
        let mut new = old.clone();
        new.kafka.acknowledgements = scalo::transport::AcknowledgementsConfig::new(false);

        assert_eq!(restart_required_changes(&old, &new), vec!["kafka"]);
    }

    /// Every section the archiver snapshots has to be named, including the DLQ
    /// that is compared as JSON.
    #[test]
    fn test_every_startup_bound_section_is_reported() {
        let old = Config::default();
        let mut new = old.clone();
        new.transport = "grpc".to_string();
        new.kafka.topics = vec!["other".to_string()];
        new.grpc.listen = Some("0.0.0.0:6000".to_string());
        new.archive.destination = "file:///srv/dfe/archive".to_string();
        new.buffer.flush_bytes += 1;
        new.routing.mode = "topic".to_string();
        new.compression.codec = "gzip".to_string();
        new.dlq.enabled = !old.dlq.enabled;

        assert_eq!(
            restart_required_changes(&old, &new),
            vec![
                "transport",
                "kafka",
                "grpc",
                "archive",
                "buffer",
                "routing",
                "compression",
                "dlq"
            ]
        );
    }
}
