// Project:   dfe-archiver
// File:      crates/archiver/src/metrics.rs
// Purpose:   Prometheus metrics using hyperi-rustlib DFE metric groups
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver_core::Result;
use dfe_archiver_core::config::MetricsConfig;
use hyperi_rustlib::metrics::dfe_groups::{
    AppMetrics, BackpressureMetrics, BufferMetrics, ConsumerMetrics, SinkMetrics,
};
use hyperi_rustlib::metrics::{DfeMetrics, MetricsManager};
use metrics::{counter, gauge, histogram};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

/// Archiver metrics -- combines rustlib DFE metric groups with
/// archiver-specific counters/gauges/histograms.
///
/// Layer 1: `DfeMetrics` (platform `dfe_*` metrics)
/// Layer 2: Group structs (`AppMetrics`, `BufferMetrics`, `ConsumerMetrics`,
///          `SinkMetrics`, `BackpressureMetrics`)
/// Layer 3: Archiver-specific metrics (compression, archive roll, etc.)
pub struct ArchiverMetrics {
    /// Standard DFE metrics (None in tests without a metrics exporter)
    dfe: Option<DfeMetrics>,
    /// Pipeline readiness flag (shared with `MetricsManager` readiness check)
    ready: Arc<AtomicBool>,

    // Layer 2: Metric groups from rustlib
    pub app: Option<AppMetrics>,
    pub buffer: Option<BufferMetrics>,
    pub consumer: Option<ConsumerMetrics>,
    pub sink: Option<SinkMetrics>,
    pub backpressure: Option<BackpressureMetrics>,
}

impl Default for ArchiverMetrics {
    fn default() -> Self {
        Self {
            dfe: None,
            ready: Arc::new(AtomicBool::new(false)),
            app: None,
            buffer: None,
            consumer: None,
            sink: None,
            backpressure: None,
        }
    }
}

impl ArchiverMetrics {
    /// Register all archiver metrics on the given manager.
    ///
    /// Uses a single `MetricsManager` for both metric registration and the
    /// HTTP server. Combines rustlib DFE groups with archiver-specific metrics.
    fn register(manager: &MetricsManager, commit: &str) -> Self {
        // Layer 2: Metric groups (auto-prefixed with dfe_archiver_)
        let app = AppMetrics::new(manager, env!("CARGO_PKG_VERSION"), commit);
        let buffer = BufferMetrics::new(manager);
        let consumer = ConsumerMetrics::new(manager);
        let sink = SinkMetrics::new(manager);
        let backpressure = BackpressureMetrics::new(manager);

        // Layer 3: Archiver-specific metrics
        // Existing metrics (kept for backwards compatibility)
        let _ = manager.counter(
            "messages_received_total",
            "Total messages received from Kafka",
        );
        let _ = manager.counter(
            "messages_archived_total",
            "Total messages successfully archived",
        );
        let _ = manager.counter("messages_dlq_total", "Total messages sent to DLQ");
        let _ = manager.counter("files_created_total", "Total archive files created");
        let _ = manager.counter("files_closed_total", "Total archive files closed (rolled)");
        let _ = manager.counter(
            "bytes_written_total",
            "Total bytes written (uncompressed input)",
        );
        let _ = manager.counter("flush_operations_total", "Total flush operations");
        let _ = manager.counter("archive_errors_total", "Total archive errors");
        let _ = manager.counter(
            "disk_pressure_events_total",
            "Total disk pressure backpressure events",
        );

        let _ = manager.gauge("kafka_lag", "Kafka consumer lag (sum across partitions)");
        let _ = manager.gauge("hot_buffers_active", "Number of active hot buffers");
        let _ = manager.gauge("hot_buffers_bytes", "Total bytes in hot buffers");

        let _ = manager.histogram("batch_size_bytes", "Archive batch size in bytes");
        let _ = manager.histogram("flush_duration_seconds", "Time to flush buffer to storage");

        // New archiver-specific metrics
        let _ = manager.counter(
            "bytes_compressed_total",
            "Total bytes written (post-compression)",
        );
        let _ = manager.counter(
            "routing_errors_total",
            "Routing failures (fell back to topic)",
        );
        let _ = manager.counter(
            "hot_buffer_evictions_total",
            "LRU hot buffer eviction events",
        );
        let _ = manager.gauge(
            "compression_ratio",
            "Running compression ratio (compressed/uncompressed)",
        );
        let _ = manager.gauge("unique_destinations", "Current distinct destination count");
        let _ = manager.gauge(
            "pipeline_last_batch_timestamp_seconds",
            "Unix timestamp of last completed batch",
        );
        let _ = manager.histogram(
            "compression_duration_seconds",
            "Time to compress a buffer flush",
        );
        let _ = manager.histogram(
            "archive_file_size_bytes",
            "Final compressed archive file sizes at roll time",
        );

        // Labelled metrics (described manually for label dimensions)
        metrics::describe_counter!(
            "dfe_archiver_archive_roll_total",
            "Archive file roll events by trigger"
        );
        metrics::describe_counter!(
            "dfe_archiver_kafka_commit_errors_total",
            "Failed Kafka offset commits"
        );

        let dfe = DfeMetrics::register();

        Self {
            dfe: Some(dfe),
            ready: Arc::new(AtomicBool::new(false)),
            app: Some(app),
            buffer: Some(buffer),
            consumer: Some(consumer),
            sink: Some(sink),
            backpressure: Some(backpressure),
        }
    }

    // ── Layer 1: DfeMetrics pass-throughs ────────────────────────────

    /// Record messages received (dual-emit: archiver + `DfeMetrics`)
    pub fn record_received(&self, count: u64) {
        counter!("dfe_archiver_messages_received_total").increment(count);
        if let Some(ref app) = self.app {
            app.record_received(count);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.records_received(count);
        }
    }

    /// Record messages archived (dual-emit: archiver + `DfeMetrics`)
    pub fn record_archived(&self, count: u64) {
        counter!("dfe_archiver_messages_archived_total").increment(count);
        if let Some(ref app) = self.app {
            app.record_processed(count);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.records_delivered(count);
        }
    }

    /// Record messages sent to DLQ
    pub fn record_dlq(&self, count: u64) {
        counter!("dfe_archiver_messages_dlq_total").increment(count);
        if let Some(ref dfe) = self.dfe {
            dfe.records_dlq(count);
        }
    }

    // ── Layer 3: Archiver-specific ───────────────────────────────────

    /// Record file created
    pub fn record_file_created(&self) {
        counter!("dfe_archiver_files_created_total").increment(1);
    }

    /// Record file closed (rolled) with final compressed size
    pub fn record_file_closed(&self, compressed_bytes: u64) {
        counter!("dfe_archiver_files_closed_total").increment(1);
        histogram!("dfe_archiver_archive_file_size_bytes").record(compressed_bytes as f64);
    }

    /// Record archive roll with trigger reason
    pub fn record_archive_roll(&self, trigger: &str) {
        counter!("dfe_archiver_archive_roll_total", "trigger" => trigger.to_string()).increment(1);
    }

    /// Record bytes written (uncompressed input to writer)
    pub fn record_bytes_written(&self, bytes: u64) {
        counter!("dfe_archiver_bytes_written_total").increment(bytes);
        if let Some(ref app) = self.app {
            app.record_bytes_written(bytes);
        }
    }

    /// Record bytes after compression
    pub fn record_bytes_compressed(&self, compressed: u64, uncompressed: u64) {
        counter!("dfe_archiver_bytes_compressed_total").increment(compressed);
        if uncompressed > 0 {
            gauge!("dfe_archiver_compression_ratio").set(compressed as f64 / uncompressed as f64);
        }
    }

    /// Record compression duration
    pub fn record_compression_duration(&self, duration_secs: f64) {
        histogram!("dfe_archiver_compression_duration_seconds").record(duration_secs);
    }

    /// Update Kafka lag
    pub fn set_kafka_lag(&self, lag: u64) {
        gauge!("dfe_archiver_kafka_lag").set(lag as f64);
    }

    /// Record flush operation with duration and trigger
    pub fn record_flush(&self, duration_secs: f64, trigger: &str) {
        counter!("dfe_archiver_flush_operations_total").increment(1);
        histogram!("dfe_archiver_flush_duration_seconds").record(duration_secs);
        if let Some(ref buffer) = self.buffer {
            buffer.record_flush(duration_secs, trigger);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.transport_send_duration("storage", duration_secs);
        }
    }

    /// Record archive error
    pub fn record_error(&self) {
        counter!("dfe_archiver_archive_errors_total").increment(1);
        if let Some(ref app) = self.app {
            app.record_error(1);
        }
    }

    /// Record storage write error with backend label
    pub fn record_sink_error(&self, backend: &str) {
        counter!("dfe_archiver_archive_errors_total").increment(1);
        if let Some(ref sink) = self.sink {
            sink.record_error(backend);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.transport_send_errors("storage", 1);
        }
    }

    /// Record storage write duration with backend label
    pub fn record_sink_duration(&self, backend: &str, duration_secs: f64) {
        if let Some(ref sink) = self.sink {
            sink.record_duration(backend, duration_secs);
        }
    }

    /// Record disk pressure event
    pub fn record_disk_pressure(&self) {
        counter!("dfe_archiver_disk_pressure_events_total").increment(1);
        if let Some(ref bp) = self.backpressure {
            bp.record_event();
        }
    }

    /// Record backpressure pause duration
    pub fn record_backpressure_duration(&self, duration_secs: f64) {
        if let Some(ref bp) = self.backpressure {
            bp.record_duration(duration_secs);
        }
    }

    /// Update hot buffer stats
    pub fn set_hot_buffer_stats(&self, count: usize, bytes: usize) {
        gauge!("dfe_archiver_hot_buffers_active").set(count as f64);
        gauge!("dfe_archiver_hot_buffers_bytes").set(bytes as f64);
        if let Some(ref buffer) = self.buffer {
            buffer.set_buffer(bytes, count);
        }
    }

    /// Update spool size
    pub fn set_spool_bytes(&self, bytes: u64) {
        if let Some(ref dfe) = self.dfe {
            dfe.spool_bytes(bytes as f64);
        }
    }

    /// Record batch size for histogram
    pub fn record_batch_size(&self, bytes: u64) {
        histogram!("dfe_archiver_batch_size_bytes").record(bytes as f64);
    }

    /// Record Kafka recv duration
    pub fn record_recv_duration(&self, duration_secs: f64) {
        if let Some(ref consumer) = self.consumer {
            consumer.record_poll_duration(duration_secs);
        }
    }

    /// Record Kafka offset commit
    pub fn record_commit(&self, count: u64) {
        if let Some(ref consumer) = self.consumer {
            consumer.record_offsets_committed(count);
        }
    }

    /// Record Kafka commit error
    pub fn record_commit_error(&self) {
        counter!("dfe_archiver_kafka_commit_errors_total").increment(1);
    }

    /// Record routing error
    pub fn record_routing_error(&self) {
        counter!("dfe_archiver_routing_errors_total").increment(1);
    }

    /// Record hot buffer eviction
    pub fn record_eviction(&self) {
        counter!("dfe_archiver_hot_buffer_evictions_total").increment(1);
    }

    /// Update unique destination count
    pub fn set_unique_destinations(&self, count: usize) {
        gauge!("dfe_archiver_unique_destinations").set(count as f64);
    }

    /// Update last batch timestamp (staleness detection)
    pub fn set_last_batch_timestamp(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        gauge!("dfe_archiver_pipeline_last_batch_timestamp_seconds").set(now);
    }

    /// Update KEDA scaling pressure gauge
    pub fn set_scaling_pressure(&self, value: f64) {
        if let Some(ref dfe) = self.dfe {
            dfe.scaling_pressure(value);
        }
    }

    /// Update pipeline readiness
    pub fn set_pipeline_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
        if let Some(ref dfe) = self.dfe {
            dfe.pipeline_ready(ready);
        }
    }

    /// Update scaling circuit breaker state
    pub fn set_scaling_circuit_open(&self, open: bool) {
        if let Some(ref dfe) = self.dfe {
            dfe.scaling_circuit_open(open);
        }
    }

    /// Update scaling memory pressure ratio
    pub fn set_scaling_memory_pressure(&self, ratio: f64) {
        if let Some(ref dfe) = self.dfe {
            dfe.scaling_memory_pressure(ratio);
        }
    }

    /// Update memory usage from `MemoryGuard` (cgroup-aware)
    pub fn set_memory_usage(&self, current_bytes: u64, limit_bytes: u64) {
        if let Some(ref app) = self.app {
            app.set_memory(current_bytes, limit_bytes);
        }
    }

    /// Record a successful config reload
    pub fn record_config_reload(&self, success: bool) {
        if let Some(ref app) = self.app {
            app.record_config_reload(success);
        }
    }
}

/// Initialise the metrics subsystem: register metrics, start HTTP server,
/// wire `/readyz` to pipeline readiness.
///
/// Returns the `ArchiverMetrics` handle (wrapped in `Arc`) and the
/// `MetricsManager` (which owns the server task).
///
/// # Errors
/// Returns error if the HTTP server fails to bind.
pub async fn init_metrics(
    config: &MetricsConfig,
    commit: &str,
) -> Result<(Arc<ArchiverMetrics>, MetricsManager)> {
    let mut manager = MetricsManager::new("dfe_archiver");

    let metrics = ArchiverMetrics::register(&manager, commit);

    // Wire /readyz to the pipeline readiness flag
    let ready_flag = Arc::clone(&metrics.ready);
    manager.set_readiness_check(move || ready_flag.load(Ordering::Acquire));

    if config.enabled {
        manager
            .start_server(&config.address)
            .await
            .map_err(|e| dfe_archiver_core::Error::Config(format!("metrics server failed: {e}")))?;

        info!(
            address = %config.address,
            path = %config.path,
            "Metrics server started (hyperi-rustlib)"
        );
    } else {
        info!("Metrics server disabled");
    }

    Ok((Arc::new(metrics), manager))
}
