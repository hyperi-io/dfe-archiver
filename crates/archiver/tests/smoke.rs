// Project:   dfe-archiver
// File:      crates/archiver/tests/smoke.rs
// Purpose:   Mandatory startup smoke test (runs on every push)
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! Startup smoke test.
//!
//! Constructs all pipeline components with default config (no external deps).
//! Catches init panics, broken defaults, and missing wiring before production does.
//!
//! Run with: `cargo nextest run --test smoke`

#![allow(clippy::expect_used, clippy::no_effect_underscore_binding)]

use dfe_archiver::archive::RollingPolicy;
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::{Config, validate_config};
use dfe_archiver::io::{Staging, create_backend};
use dfe_archiver::metrics::ArchiverMetrics;
use dfe_archiver::routing::Router;
use dfe_archiver_core::buffer::{TieredBufferConfig, TieredBufferManager};

/// Full startup smoke test: construct all pipeline components with default config.
///
/// No Kafka, no storage credentials, no network -- just verify nothing panics.
#[test]
fn smoke_startup_boots_with_default_config() {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".to_string()];

    validate_config(&config).expect("default config must validate");

    // Router
    let _router = Router::new(config.routing.clone());

    // Hot buffers, spooled under a temp dir instead of the image's absolute path
    let spool = tempfile::TempDir::new().expect("spool");
    let _buffer = TieredBufferManager::new(TieredBufferConfig {
        hot_buffer_size: config.buffer.flush_bytes,
        hot_buffer_records: config.buffer.flush_records,
        hot_buffer_age_secs: config.buffer.flush_age_secs,
        spool_dir: spool.path().to_path_buf(),
        ..TieredBufferConfig::default()
    })
    .expect("hot buffers");

    // Compressor
    let _compressor = create_compressor(&config.compression.codec, config.compression.level)
        .expect("default compressor");

    // Metrics (no exporter)
    let _metrics = ArchiverMetrics::default();

    // RollingPolicy
    let _policy = RollingPolicy {
        max_size_bytes: config.archive.roll_size_bytes,
        max_age_secs: config.roll_interval_secs(),
    };

    // File backend (default destination is file://)
    let staging = Staging::open(spool.path().join("uploads")).expect("staging");
    let _backend = create_backend(&config.archive, &staging).expect("default file backend");
}

/// Verify the deployment contract is valid and produces JSON without panic.
#[test]
fn smoke_deployment_contract() {
    use dfe_archiver::contract::deployment_contract;
    let contract = deployment_contract();
    let json = contract.to_json();
    assert!(!json.is_empty(), "contract JSON should not be empty");
    assert!(
        json.contains("dfe-archiver"),
        "contract should reference app name"
    );
}
