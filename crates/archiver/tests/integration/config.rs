// Project:   dfe-archiver
// File:      crates/archiver/tests/integration/config.rs
// Purpose:   Config loading and validation integration tests
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver::config::{Config, validate_config};

/// Load the fixture YAML and validate it parses + validates
#[test]
fn test_fixture_config_loads_and_validates() {
    let yaml = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/valid_config.yaml"
    ))
    .expect("read fixture");

    let config: Config = serde_yaml_ng::from_str(&yaml).expect("parse YAML");
    validate_config(&config).expect("fixture config should validate");

    assert_eq!(config.kafka.brokers, vec!["localhost:9092"]);
    assert_eq!(config.kafka.group_id, "dfe-archiver");
    assert_eq!(config.compression.codec, "zstd");
    assert_eq!(config.archive.destination, "file:///var/data/archive");
}

/// Default config (with brokers populated) validates
#[test]
fn test_default_config_validates() {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".to_string()];
    validate_config(&config).expect("default config should validate");
}
