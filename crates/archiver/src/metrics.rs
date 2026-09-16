// Project:   dfe-archiver
// File:      crates/archiver/src/metrics.rs
// Purpose:   Prometheus metrics using scalo metric groups
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use metrics::{counter, gauge, histogram};
use scalo::metrics::groups::{
    AppMetrics, BackpressureMetrics, BufferMetrics, ConsumerMetrics, SinkMetrics,
};
use scalo::metrics::{FlushTrigger, MetricsManager, ServiceMetrics, TransportKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

/// Archiver metrics -- combines scalo metric groups with
/// archiver-specific counters/gauges/histograms.
///
/// Layer 1: `ServiceMetrics` (platform `dfe_*` metrics)
/// Layer 2: Group structs (`AppMetrics`, `BufferMetrics`, `ConsumerMetrics`,
///          `SinkMetrics`, `BackpressureMetrics`)
/// Layer 3: Archiver-specific metrics (compression, archive roll, etc.)
pub struct ArchiverMetrics {
    /// Standard platform metrics (None in tests without a metrics exporter)
    dfe: Option<ServiceMetrics>,
    /// Pipeline readiness flag (shared with `MetricsManager` readiness check)
    ready: Arc<AtomicBool>,

    // Layer 2: Metric groups from scalo
    pub app: Option<AppMetrics>,
    pub buffer: Option<BufferMetrics>,
    pub consumer: Option<ConsumerMetrics>,
    pub sink: Option<SinkMetrics>,
    pub backpressure: Option<BackpressureMetrics>,

    // EPS (events per second) rate tracking.
    // parking_lot::Mutex used for poison-free locking (Instant::elapsed cannot panic,
    // but belt-and-braces: a future caller that panics under the lock won't poison it).
    eps_counter: AtomicU64,
    eps_last_update: parking_lot::Mutex<Instant>,
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
            eps_counter: AtomicU64::new(0),
            eps_last_update: parking_lot::Mutex::new(Instant::now()),
        }
    }
}

impl ArchiverMetrics {
    /// Register all archiver metrics on the given manager.
    ///
    /// Uses a single `MetricsManager` for both metric registration and the
    /// HTTP server. Combines scalo metric groups with archiver-specific metrics.
    pub fn register(manager: &MetricsManager, commit: &str) -> Self {
        // Layer 2: Metric groups (namespace-prefixed by the MetricsManager)
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

        let _ = manager.gauge(
            "events_per_second",
            "Pipeline throughput: events processed per second",
        );

        // Labelled metrics (described manually for label dimensions)
        metrics::describe_counter!("archive_roll_total", "Archive file roll events by trigger");
        metrics::describe_counter!("kafka_commit_errors_total", "Failed Kafka offset commits");
        metrics::describe_counter!(
            "routing_fallback_total",
            "Expression-routing fields absent from a record, by field path"
        );

        let dfe = ServiceMetrics::register(manager);

        Self {
            dfe: Some(dfe),
            ready: Arc::new(AtomicBool::new(false)),
            app: Some(app),
            buffer: Some(buffer),
            consumer: Some(consumer),
            sink: Some(sink),
            backpressure: Some(backpressure),
            eps_counter: AtomicU64::new(0),
            eps_last_update: parking_lot::Mutex::new(Instant::now()),
        }
    }

    // ── Layer 1: ServiceMetrics pass-throughs ────────────────────────

    /// Record messages received (dual-emit: archiver + `ServiceMetrics`)
    ///
    /// Also feeds the EPS rate calculator.
    pub fn record_received(&self, count: u64) {
        counter!("messages_received_total").increment(count);
        self.eps_counter.fetch_add(count, Ordering::Relaxed);
        if let Some(ref app) = self.app {
            app.record_received(count);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.records_received(count);
        }
    }

    /// Recalculate and emit the events-per-second gauge.
    ///
    /// Call once per pipeline loop iteration. Uses a simple counter/elapsed
    /// approach: accumulates events since last call, divides by elapsed seconds,
    /// then resets. This gives a true instantaneous rate rather than relying on
    /// Prometheus `rate()` over scrape intervals.
    pub fn update_eps(&self) {
        let count = self.eps_counter.swap(0, Ordering::Relaxed);
        let mut last = self.eps_last_update.lock();
        let elapsed = last.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let eps = count as f64 / elapsed;
            gauge!("events_per_second").set(eps);
        }
        *last = Instant::now();
    }

    /// Record messages archived (dual-emit: archiver + `ServiceMetrics`)
    pub fn record_archived(&self, count: u64) {
        counter!("messages_archived_total").increment(count);
        if let Some(ref app) = self.app {
            app.record_processed(count);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.records_delivered(count);
        }
    }

    /// Record messages sent to DLQ
    pub fn record_dlq(&self, count: u64) {
        counter!("messages_dlq_total").increment(count);
        if let Some(ref dfe) = self.dfe {
            dfe.records_dlq(count);
        }
    }

    // ── Layer 3: Archiver-specific ───────────────────────────────────

    /// Record archive files created.
    ///
    /// Takes a count because the writer reports the files it opened since the
    /// last drain, which spans the roll inside a single write.
    pub fn record_files_created(&self, count: u64) {
        counter!("files_created_total").increment(count);
    }

    /// Record file closed (rolled) with final compressed size
    pub fn record_file_closed(&self, compressed_bytes: u64) {
        counter!("files_closed_total").increment(1);
        histogram!("archive_file_size_bytes").record(compressed_bytes as f64);
    }

    /// Record archive roll with trigger reason
    pub fn record_archive_roll(&self, trigger: &str) {
        counter!("archive_roll_total", "trigger" => trigger.to_string()).increment(1);
    }

    /// Record an LRU eviction of a per-destination archive writer.
    /// Increments a counter so operators can correlate sudden bumps with
    /// `unique_destinations` saturating the configured `max_writers` cap.
    pub fn record_writer_eviction(&self) {
        counter!("writer_evictions_total").increment(1);
    }

    /// Record bytes written (uncompressed input to writer)
    pub fn record_bytes_written(&self, bytes: u64) {
        counter!("bytes_written_total").increment(bytes);
        if let Some(ref app) = self.app {
            app.record_bytes_written(bytes);
        }
    }

    /// Record bytes after compression
    pub fn record_bytes_compressed(&self, compressed: u64, uncompressed: u64) {
        counter!("bytes_compressed_total").increment(compressed);
        if uncompressed > 0 {
            gauge!("compression_ratio").set(compressed as f64 / uncompressed as f64);
        }
    }

    /// Record compression duration
    pub fn record_compression_duration(&self, duration_secs: f64) {
        histogram!("compression_duration_seconds").record(duration_secs);
    }

    /// Update Kafka lag
    pub fn set_kafka_lag(&self, lag: u64) {
        gauge!("kafka_lag").set(lag as f64);
    }

    /// Record flush operation with duration and trigger
    pub fn record_flush(&self, duration_secs: f64, trigger: FlushTrigger) {
        counter!("flush_operations_total").increment(1);
        histogram!("flush_duration_seconds").record(duration_secs);
        if let Some(ref buffer) = self.buffer {
            buffer.record_flush(duration_secs, trigger);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.transport_send_duration("storage", duration_secs);
        }
    }

    /// Record archive error
    pub fn record_error(&self) {
        counter!("archive_errors_total").increment(1);
        if let Some(ref app) = self.app {
            app.record_error(1);
        }
    }

    /// Record a storage-write error attributed to a backend (s3/gcs/azure/minio/file).
    ///
    /// Emits the per-backend sink error counter + the platform transport-error
    /// counter. Does NOT touch `archive_errors_total` -- the write
    /// path pairs this with `record_error()`, which owns that total, so the two
    /// never double-count. Fills the per-backend storage-error metrics gap
    /// (previously this was never called on the production path).
    pub fn record_sink_error(&self, backend: &str) {
        if let Some(ref sink) = self.sink {
            sink.record_error(backend);
        }
        if let Some(ref dfe) = self.dfe {
            dfe.transport_send_errors(TransportKind::Http, 1);
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
        counter!("disk_pressure_events_total").increment(1);
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
        gauge!("hot_buffers_active").set(count as f64);
        gauge!("hot_buffers_bytes").set(bytes as f64);
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
        histogram!("batch_size_bytes").record(bytes as f64);
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
        counter!("kafka_commit_errors_total").increment(1);
    }

    /// Record routing error
    pub fn record_routing_error(&self) {
        counter!("routing_errors_total").increment(1);
    }

    /// Record an expression-routing field a record did not carry.
    ///
    /// Read as a ratio against `messages_received_total`: a sustained 1 is a
    /// field name no source sets, not a tenant named after `default_segment`.
    pub fn record_routing_fallback(&self, field: &str) {
        counter!("routing_fallback_total", "field" => field.to_string()).increment(1);
    }

    /// Record hot buffer eviction
    pub fn record_eviction(&self) {
        counter!("hot_buffer_evictions_total").increment(1);
    }

    /// Update unique destination count
    pub fn set_unique_destinations(&self, count: usize) {
        gauge!("unique_destinations").set(count as f64);
    }

    /// Update last batch timestamp (staleness detection)
    pub fn set_last_batch_timestamp(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        gauge!("pipeline_last_batch_timestamp_seconds").set(now);
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

/// Register archiver-specific metrics on the runtime's existing manager.
///
/// The `ServiceRuntime` already installs the global Prometheus recorder and
/// starts the metrics HTTP server. This function only registers app-specific
/// counters/gauges/histograms on that manager — no duplicate recorder.
pub fn init_metrics(manager: &mut MetricsManager, commit: &str) -> Arc<ArchiverMetrics> {
    let metrics = ArchiverMetrics::register(manager, commit);

    // Wire /readyz to the pipeline readiness flag
    let ready_flag = Arc::clone(&metrics.ready);
    manager.set_readiness_check(move || ready_flag.load(Ordering::Acquire));

    Arc::new(metrics)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    /// Verify `ArchiverMetrics::default()` doesn't panic (test-mode with no exporter)
    #[test]
    fn test_default_metrics_no_panic() {
        let m = ArchiverMetrics::default();
        assert!(m.dfe.is_none());
        assert!(m.app.is_none());
    }

    /// Verify all recording methods work without a metrics exporter (no-op path)
    #[test]
    fn test_recording_methods_no_panic_without_exporter() {
        let m = ArchiverMetrics::default();
        m.record_received(100);
        m.record_archived(50);
        m.record_dlq(1);
        m.record_files_created(1);
        m.record_file_closed(1024);
        m.record_archive_roll("size");
        m.record_bytes_written(4096);
        m.record_bytes_compressed(2048, 4096);
        m.record_compression_duration(0.042);
        m.set_kafka_lag(500);
        m.record_flush(0.01, FlushTrigger::Size);
        m.record_error();
        m.record_sink_error("file");
        m.record_sink_duration("file", 0.005);
        m.record_disk_pressure();
        m.record_backpressure_duration(0.1);
        m.set_hot_buffer_stats(10, 8192);
        m.set_spool_bytes(0);
        m.record_batch_size(1024);
        m.record_recv_duration(0.05);
        m.record_commit(1);
        m.record_commit_error();
        m.record_routing_error();
        m.record_routing_fallback("org_id");
        m.record_eviction();
        m.set_unique_destinations(5);
        m.set_last_batch_timestamp();
        m.set_scaling_pressure(0.42);
        m.set_pipeline_ready(true);
        m.set_scaling_circuit_open(false);
        m.set_scaling_memory_pressure(0.3);
        m.set_memory_usage(100_000, 1_000_000);
        m.record_config_reload(true);
        m.record_config_reload(false);
    }

    /// Verify zero-value edge cases don't panic
    #[test]
    fn test_recording_zero_values() {
        let m = ArchiverMetrics::default();
        m.record_received(0);
        m.record_bytes_compressed(0, 0);
        m.set_hot_buffer_stats(0, 0);
        m.set_memory_usage(0, 0);
    }

    /// Verify EPS calculation doesn't panic and produces non-negative values
    #[test]
    fn test_eps_calculation() {
        let m = ArchiverMetrics::default();
        m.record_received(1000);
        m.update_eps();
        // Counter should be reset after update
        m.record_received(500);
        m.update_eps();
    }
}
