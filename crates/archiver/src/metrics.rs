// Project:   dfe-archiver
// File:      crates/archiver/src/metrics.rs
// Purpose:   Prometheus metrics using hyperi-rustlib
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver_core::Result;
use dfe_archiver_core::config::MetricsConfig;
use hyperi_rustlib::metrics::{DfeMetrics, MetricsManager};
use metrics::{counter, gauge, histogram};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::info;

/// Archiver metrics — dual-emits legacy `dfe_archiver_*` names and
/// standard `dfe_*` names via `DfeMetrics`.
pub struct ArchiverMetrics {
    /// Standard DFE metrics (None in tests without a metrics exporter)
    dfe: Option<DfeMetrics>,
    /// Pipeline readiness flag (shared with `MetricsManager` readiness check)
    ready: Arc<AtomicBool>,
}

impl Default for ArchiverMetrics {
    fn default() -> Self {
        Self {
            dfe: None,
            ready: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ArchiverMetrics {
    /// Register all archiver metrics on the given manager.
    ///
    /// Uses a single `MetricsManager` for both metric registration and the
    /// HTTP server, avoiding the previous double-instantiation.
    fn register(manager: &MetricsManager) -> Self {
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

        let _ = manager.gauge("buffer_bytes", "Current buffer size in bytes");
        let _ = manager.gauge("buffer_records", "Current buffer record count");
        let _ = manager.gauge("kafka_lag", "Kafka consumer lag (sum across partitions)");
        let _ = manager.gauge("hot_buffers_active", "Number of active hot buffers");
        let _ = manager.gauge("hot_buffers_bytes", "Total bytes in hot buffers");
        let _ = manager.gauge("spool_bytes", "Current spool size in bytes");

        let _ = manager.histogram("batch_size_bytes", "Archive batch size in bytes");
        let _ = manager.histogram("flush_duration_seconds", "Time to flush buffer to storage");

        let _ = manager.gauge("scaling_pressure", "KEDA scaling pressure (0-100)");
        let _ = manager.gauge(
            "memory_used_bytes",
            "Current tracked memory usage (cgroup-aware)",
        );
        let _ = manager.gauge(
            "memory_limit_bytes",
            "Effective memory limit (cgroup-aware)",
        );

        let dfe = DfeMetrics::register();

        Self {
            dfe: Some(dfe),
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Record messages received
    pub fn record_received(&self, count: u64) {
        counter!("dfe_archiver_messages_received_total").increment(count);
        if let Some(ref dfe) = self.dfe {
            dfe.records_received(count);
        }
    }

    /// Record messages archived
    pub fn record_archived(&self, count: u64) {
        counter!("dfe_archiver_messages_archived_total").increment(count);
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

    /// Record file created
    pub fn record_file_created(&self) {
        counter!("dfe_archiver_files_created_total").increment(1);
    }

    /// Record file closed
    pub fn record_file_closed(&self) {
        counter!("dfe_archiver_files_closed_total").increment(1);
    }

    /// Record bytes written (uncompressed input to writer)
    pub fn record_bytes_written(&self, bytes: u64) {
        counter!("dfe_archiver_bytes_written_total").increment(bytes);
    }

    /// Update buffer stats
    pub fn set_buffer_stats(&self, bytes: u64, records: u64) {
        gauge!("dfe_archiver_buffer_bytes").set(bytes as f64);
        gauge!("dfe_archiver_buffer_records").set(records as f64);
    }

    /// Update Kafka lag
    pub fn set_kafka_lag(&self, lag: u64) {
        gauge!("dfe_archiver_kafka_lag").set(lag as f64);
    }

    /// Record flush operation
    pub fn record_flush(&self) {
        counter!("dfe_archiver_flush_operations_total").increment(1);
    }

    /// Record archive error
    pub fn record_error(&self) {
        counter!("dfe_archiver_archive_errors_total").increment(1);
    }

    /// Record disk pressure event
    pub fn record_disk_pressure(&self) {
        counter!("dfe_archiver_disk_pressure_events_total").increment(1);
    }

    /// Update hot buffer stats
    pub fn set_hot_buffer_stats(&self, count: usize, bytes: usize) {
        gauge!("dfe_archiver_hot_buffers_active").set(count as f64);
        gauge!("dfe_archiver_hot_buffers_bytes").set(bytes as f64);
    }

    /// Update spool size
    pub fn set_spool_bytes(&self, bytes: u64) {
        gauge!("dfe_archiver_spool_bytes").set(bytes as f64);
        if let Some(ref dfe) = self.dfe {
            dfe.spool_bytes(bytes as f64);
        }
    }

    /// Record batch size for histogram
    pub fn record_batch_size(&self, bytes: u64) {
        histogram!("dfe_archiver_batch_size_bytes").record(bytes as f64);
    }

    /// Record flush duration for histogram
    pub fn record_flush_duration(&self, duration_secs: f64) {
        histogram!("dfe_archiver_flush_duration_seconds").record(duration_secs);
        if let Some(ref dfe) = self.dfe {
            dfe.transport_send_duration("storage", duration_secs);
        }
    }

    /// Update KEDA scaling pressure gauge
    pub fn set_scaling_pressure(&self, value: f64) {
        gauge!("dfe_archiver_scaling_pressure").set(value);
        if let Some(ref dfe) = self.dfe {
            dfe.scaling_pressure(value);
        }
    }

    /// Update pipeline readiness.
    ///
    /// This sets the `DfeMetrics` gauge AND the shared `AtomicBool` that
    /// the `MetricsManager` readiness check reads for `/readyz`.
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
        gauge!("dfe_archiver_memory_used_bytes").set(current_bytes as f64);
        gauge!("dfe_archiver_memory_limit_bytes").set(limit_bytes as f64);
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
) -> Result<(Arc<ArchiverMetrics>, MetricsManager)> {
    let mut manager = MetricsManager::new("dfe_archiver");

    // Register all archiver-specific metrics on this single manager
    let metrics = ArchiverMetrics::register(&manager);

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
