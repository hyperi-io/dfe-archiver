// Project:   dfe-archiver
// File:      crates/archiver/src/lib.rs
// Purpose:   Library root - high-volume Kafka-to-storage archiver
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! # DFE Archiver
//!
//! High-volume Kafka-to-storage archiver designed for PB/s scale data pipelines.
//!
//! ## Features
//!
//! - **Multiple destinations**: File, `MinIO`, S3, GCS, Azure Blob
//! - **Compression**: Zstd, LZ4, Snappy, Gzip (configurable)
//! - **Smart routing**: By topic or JSON field expressions
//! - **Rolling archives**: By size or time interval
//! - **At-least-once delivery**: Kafka offset commit after successful archive
//! - **Memory-capped**: Configurable memory limits with backpressure
//!
//! ## Architecture
//!
//! ```text
//! Kafka Consumer → Buffer Manager → Archive Writer → Storage Backend
//!       ↓                ↓               ↓
//!   Batch recv      Per-dest       Compressed
//!   (10K msgs)      buffering      rolling files
//! ```

mod archiver;
pub mod config;
pub mod contract;
pub mod metrics;

pub use archiver::Archiver;

// Re-export core types for convenience
pub use dfe_archiver_core::error::{Error, Result};
pub use dfe_archiver_core::{archive, buffer, compression, routing, storage, types};
pub use dfe_archiver_io as io;

// Re-export key config types
pub use dfe_archiver_core::config::{
    ArchiveConfig, BufferConfig, CompressionConfig, KafkaConfig, MemoryConfig, MetricsConfig,
    RoutingConfig,
};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The archiver's weighted KEDA scaling components.
///
/// Single source of truth shared by the `ServiceApp::scaling_components`
/// override (which registers them on the runtime's unified `ScalingPressure`,
/// the engine `/scaling/pressure` serves) and the `Archiver`'s standalone
/// fallback engine (built only when the runtime has `scaling` disabled). Both
/// MUST register the same component set so the gauge and the served pressure
/// agree. Weights/saturations mirror the legacy archiver model:
/// `kafka_lag` (assigned-partition lag), `buffer_depth` (hot-buffer count), and
/// `memory` (cgroup guard pressure ratio).
#[must_use]
pub fn scaling_components() -> Vec<scalo::scaling::ScalingComponent> {
    use scalo::scaling::ScalingComponent;
    vec![
        ScalingComponent::new("kafka_lag", 0.40, 100_000.0),
        ScalingComponent::new("buffer_depth", 0.30, 10_000.0),
        ScalingComponent::new("memory", 0.30, 1.0),
    ]
}
