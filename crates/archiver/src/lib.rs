// Project:   dfe-archiver
// File:      crates/archiver/src/lib.rs
// Purpose:   Library root - high-volume Kafka-to-storage archiver
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

//! # DFE Archiver
//!
//! Writes a DFE deployment's streams, read from Kafka or a gRPC push, to
//! compressed rolling files in a local path, `MinIO`, S3, GCS or Azure Blob.
//! On Kafka an offset is committed once the archive file holding its record is
//! durable. The pipeline is in [`Archiver`].

mod archiver;
pub mod config;
pub mod contract;
pub mod metrics;
mod spool_limits;
mod writers;

pub use archiver::{Archiver, restart_required_changes};

// Re-export core types for convenience
pub use dfe_archiver_core::error::{Error, Result};
pub use dfe_archiver_core::{archive, buffer, compression, routing, storage, types};
pub use dfe_archiver_io as io;

// Re-export key config types
pub use dfe_archiver_core::config::{
    ArchiveConfig, BufferConfig, CompressionConfig, KafkaConfig, RoutingConfig,
};

/// The archiver's version-check defaults: the releases endpoint the check
/// targets, with scalo's own defaults for everything else.
///
/// Sits UNDER the config cascade, so any `version_check.*` key a deployment
/// sets wins -- `enabled: false` in the config file, or the
/// `<PREFIX>_VERSION_CHECK__ENABLED=false` the chart renders for an air-gapped
/// install. That overlay only happens because `config::load_config`
/// initialises the cascade with the config file as its settings layer.
#[must_use]
pub fn version_check_defaults() -> scalo::version_check::VersionCheckConfig {
    scalo::version_check::VersionCheckConfig {
        api_url: "https://releases.hyperi.io/api/v1/check".into(),
        ..Default::default()
    }
}

/// The archiver's weighted KEDA scaling components.
///
/// Registered on the runtime's `ScalingPressure` by
/// `ServiceApp::scaling_components`, and on the standalone engine `Archiver`
/// builds for a caller with no runtime: `kafka_lag` (records past the
/// consumer's read position, which the commit an open archive file holds does
/// not inflate), `buffer_depth` (hot-buffer count), and `memory` (cgroup guard
/// pressure ratio).
#[must_use]
pub fn scaling_components() -> Vec<scalo::scaling::ScalingComponent> {
    use scalo::scaling::ScalingComponent;
    vec![
        ScalingComponent::new("kafka_lag", 0.40, 100_000.0),
        ScalingComponent::new("buffer_depth", 0.30, 10_000.0),
        ScalingComponent::new("memory", 0.30, 1.0),
    ]
}
