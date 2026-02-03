// Project:   dfe-archiver
// File:      src/metrics/mod.rs
// Purpose:   Prometheus metrics using hs-rustlib
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

use crate::config::MetricsConfig;
use crate::Result;
use hs_rustlib::metrics::MetricsManager;
use metrics::{counter, gauge, histogram};
use std::sync::Arc;
use tracing::info;

/// Archiver metrics using hs-rustlib's Prometheus exporter
///
/// This struct holds the metrics manager and provides methods for recording metrics.
/// The actual MetricsManager should be started separately via `start_metrics_server`.
pub struct ArchiverMetrics {
    _namespace: &'static str,
}

impl ArchiverMetrics {
    /// Create new metrics instance and register all metrics
    #[must_use]
    pub fn new() -> Arc<Self> {
        // Create a manager just to register metrics with descriptions
        let manager = MetricsManager::new("dfe_archiver");

        // Register standard metrics with descriptions
        // Note: The returned handles are intentionally ignored as hs-rustlib
        // registers metrics globally and we use the metrics crate macros directly
        let _ = manager.counter("messages_received_total", "Total messages received from Kafka");
        let _ = manager.counter("messages_archived_total", "Total messages successfully archived");
        let _ = manager.counter("messages_dlq_total", "Total messages sent to DLQ");
        let _ = manager.counter("files_created_total", "Total archive files created");
        let _ = manager.counter("files_closed_total", "Total archive files closed (rolled)");
        let _ = manager.counter("bytes_written_total", "Total bytes written (uncompressed)");
        let _ = manager.counter("bytes_compressed_total", "Total bytes written (compressed)");
        let _ = manager.counter("flush_operations_total", "Total flush operations");
        let _ = manager.counter("archive_errors_total", "Total archive errors");
        let _ = manager.counter("disk_pressure_events_total", "Total disk pressure backpressure events");

        // Gauges for current state
        let _ = manager.gauge("buffer_bytes", "Current buffer size in bytes");
        let _ = manager.gauge("buffer_records", "Current buffer record count");
        let _ = manager.gauge("kafka_lag", "Kafka consumer lag (sum across partitions)");
        let _ = manager.gauge("hot_buffers_active", "Number of active hot buffers");
        let _ = manager.gauge("hot_buffers_bytes", "Total bytes in hot buffers");
        let _ = manager.gauge("spool_bytes", "Current spool size in bytes");

        // Histograms for latency/size distribution
        let _ = manager.histogram("batch_size_bytes", "Archive batch size in bytes");
        let _ = manager.histogram("flush_duration_seconds", "Time to flush buffer to storage");

        // The manager is not stored - metrics crate uses a global registry
        // and hs-rustlib::metrics registers with it
        Arc::new(Self {
            _namespace: "dfe_archiver",
        })
    }

    /// Record messages received
    pub fn record_received(&self, count: u64) {
        counter!("dfe_archiver_messages_received_total").increment(count);
    }

    /// Record messages archived
    pub fn record_archived(&self, count: u64) {
        counter!("dfe_archiver_messages_archived_total").increment(count);
    }

    /// Record messages sent to DLQ
    pub fn record_dlq(&self, count: u64) {
        counter!("dfe_archiver_messages_dlq_total").increment(count);
    }

    /// Record file created
    pub fn record_file_created(&self) {
        counter!("dfe_archiver_files_created_total").increment(1);
    }

    /// Record file closed
    pub fn record_file_closed(&self) {
        counter!("dfe_archiver_files_closed_total").increment(1);
    }

    /// Record bytes written
    pub fn record_bytes(&self, uncompressed: u64, compressed: u64) {
        counter!("dfe_archiver_bytes_written_total").increment(uncompressed);
        counter!("dfe_archiver_bytes_compressed_total").increment(compressed);
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
    }

    /// Record batch size for histogram
    pub fn record_batch_size(&self, bytes: u64) {
        histogram!("dfe_archiver_batch_size_bytes").record(bytes as f64);
    }

    /// Record flush duration for histogram
    pub fn record_flush_duration(&self, duration_secs: f64) {
        histogram!("dfe_archiver_flush_duration_seconds").record(duration_secs);
    }
}

impl Default for ArchiverMetrics {
    fn default() -> Self {
        Self {
            _namespace: "dfe_archiver",
        }
    }
}

/// Snapshot of metrics for reporting (computed from Prometheus metrics)
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub messages_received: u64,
    pub messages_archived: u64,
    pub messages_dlq: u64,
    pub files_created: u64,
    pub files_closed: u64,
    pub bytes_written: u64,
    pub bytes_compressed: u64,
    pub buffer_bytes: u64,
    pub buffer_records: u64,
    pub kafka_lag: u64,
    pub flush_count: u64,
    pub archive_errors: u64,
}

impl MetricsSnapshot {
    /// Get compression ratio
    #[must_use]
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_written == 0 {
            1.0
        } else {
            self.bytes_compressed as f64 / self.bytes_written as f64
        }
    }
}

/// Start metrics HTTP server using hs-rustlib
///
/// This creates a new MetricsManager and starts the server.
/// The server provides /metrics, /healthz, /readyz endpoints.
///
/// # Errors
/// Returns error if server fails to start
pub async fn start_metrics_server(config: &MetricsConfig) -> Result<MetricsManager> {
    if !config.enabled {
        info!("Metrics server disabled");
        // Return a manager anyway for consistency, just don't start the server
        return Ok(MetricsManager::new("dfe_archiver"));
    }

    let mut manager = MetricsManager::new("dfe_archiver");

    // Start the hs-rustlib metrics server
    // This provides /metrics, /healthz, /readyz endpoints
    manager
        .start_server(&config.address)
        .await
        .map_err(|e| crate::Error::Config(format!("metrics server failed: {e}")))?;

    info!(
        address = %config.address,
        path = %config.path,
        "Metrics server started (hs-rustlib)"
    );

    Ok(manager)
}
