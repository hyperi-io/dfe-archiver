// Project:   dfe-archiver
// File:      crates/archiver/src/config/loader.rs
// Purpose:   Configuration loading with cascade priority
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver_core::config::Config;
use dfe_archiver_core::{Error, Result};
use scalo::config::flat_env::{ApplyFlatEnv, Normalize};
use std::path::Path;
use tracing::info;

/// Load configuration with cascade: CLI → ENV → .env → file → defaults
///
/// Priority (highest to lowest):
/// 1. CLI arguments (handled by caller, merged after)
/// 2. Environment variables (flat env overrides)
/// 3. .env file (loaded by dotenvy in main)
/// 4. Config file (YAML)
/// 5. Hard-coded defaults
pub fn load_config(config_path: Option<&str>) -> Result<Config> {
    let mut config = Config::default();

    if let Some(path) = config_path {
        config = load_from_file(path)?;
        info!(path = %path, "Loaded configuration from file");
    } else if let Ok(path) = std::env::var("ARCHIVER_CONFIG") {
        config = load_from_file(&path)?;
        info!(path = %path, "Loaded configuration from ARCHIVER_CONFIG");
    } else {
        for default_path in &[
            "config.yaml",
            "config/settings.yaml",
            "/etc/dfe-archiver/config.yaml",
        ] {
            if Path::new(default_path).exists() {
                config = load_from_file(default_path)?;
                info!(path = %default_path, "Loaded configuration from default location");
                break;
            }
        }
    }

    // Apply flat env overrides (preserves existing env var contract)
    config.apply_flat_env("UNUSED");
    config.normalize();

    validate_config(&config)?;

    // Register all sections in the config registry for /config endpoint
    config.register_in_registry();

    Ok(config)
}

/// Load configuration from YAML file
fn load_from_file(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| Error::Config(format!("failed to read config file '{path}': {e}")))?;

    serde_yaml_ng::from_str(&content)
        .map_err(|e| Error::Config(format!("failed to parse config file '{path}': {e}")))
}

/// Validate configuration
///
/// Called by `load_config` and by the `ConfigReloader` on hot-reload.
pub fn validate_config(config: &Config) -> Result<()> {
    if config.kafka.brokers.is_empty() {
        return Err(Error::Config("kafka.brokers cannot be empty".to_string()));
    }

    if config.kafka.group_id.is_empty() {
        return Err(Error::Config("kafka.group_id cannot be empty".to_string()));
    }

    if config.kafka.sasl_mechanism.is_some()
        && (config.kafka.sasl_username.is_none() || config.kafka.sasl_password.is_none())
    {
        return Err(Error::Config(
            "kafka.sasl_username and kafka.sasl_password required when sasl_mechanism is set"
                .to_string(),
        ));
    }

    if config.archive.destination.is_empty() {
        return Err(Error::Config(
            "archive.destination cannot be empty".to_string(),
        ));
    }

    if config.buffer.flush_bytes == 0 {
        return Err(Error::Config(
            "buffer.flush_bytes must be greater than 0".to_string(),
        ));
    }

    if config.memory.limit_bytes == 0 {
        return Err(Error::Config(
            "memory.limit_bytes must be greater than 0".to_string(),
        ));
    }

    if config.memory.pressure_threshold <= 0.0 || config.memory.pressure_threshold > 1.0 {
        return Err(Error::Config(
            "memory.pressure_threshold must be between 0.0 and 1.0".to_string(),
        ));
    }

    if config.archive.multipart_chunk_size > 0
        && config.archive.multipart_chunk_size < 5 * 1024 * 1024
    {
        return Err(Error::Config(
            "archive.multipart_chunk_size must be at least 5MB (5242880 bytes)".to_string(),
        ));
    }

    let valid_codecs = ["none", "zstd", "lz4", "snappy", "gzip"];
    if !valid_codecs.contains(&config.compression.codec.as_str()) {
        return Err(Error::Config(format!(
            "compression.codec must be one of: {}",
            valid_codecs.join(", ")
        )));
    }

    // `Router::route` honours "expression" and treats every other value as
    // "topic", so without this check a misspelt mode is indistinguishable from
    // the default at runtime and the configured path layout never appears.
    let valid_routing_modes = ["topic", "expression"];
    if !valid_routing_modes.contains(&config.routing.mode.as_str()) {
        return Err(Error::Config(format!(
            "routing.mode must be one of: {} (got '{}')",
            valid_routing_modes.join(", "),
            config.routing.mode
        )));
    }

    // Expression mode with no fields yields the same path as topic mode, and
    // additionally rejects non-JSON payloads. Never what was intended.
    if config.routing.mode == "expression" && config.routing.expression_fields.is_empty() {
        return Err(Error::Config(
            "routing.expression_fields cannot be empty when routing.mode is 'expression'"
                .to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];

        validate_config(&config).expect("default config should be valid");
    }

    #[test]
    fn test_default_dlq_routes_common_to_standard_topic() {
        let config = Config::default();
        assert_eq!(config.dlq.kafka.routing, scalo::dlq::DlqRouting::Common);
        assert_eq!(config.dlq.kafka.common_topic, "dfe_archiver_dlq");
    }

    #[test]
    fn test_empty_brokers_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec![];

        let result = validate_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_compression_codec_fails() {
        let mut config = Config::default();
        config.compression.codec = "invalid".to_string();

        let result = validate_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_memory_limit_fails() {
        let mut config = Config::default();
        config.memory.limit_bytes = 0;

        let result = validate_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_normalize_infers_sasl_mechanism() {
        let mut config = Config::default();
        config.kafka.sasl_username = Some("user".to_string());
        config.kafka.sasl_password = Some(
            dfe_archiver_core::config::sensitive::SensitiveString::from("pass"),
        );
        config.normalize();

        assert_eq!(config.kafka.sasl_mechanism, Some("PLAIN".to_string()));
    }

    #[test]
    fn test_empty_group_id_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.kafka.group_id = String::new();

        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_empty_destination_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.archive.destination = String::new();

        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_sasl_mechanism_without_credentials_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.kafka.sasl_mechanism = Some("PLAIN".to_string());
        config.kafka.sasl_username = None;
        config.kafka.sasl_password = None;

        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_zero_flush_bytes_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.buffer.flush_bytes = 0;

        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_pressure_threshold_out_of_range() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];

        config.memory.pressure_threshold = 0.0;
        assert!(validate_config(&config).is_err());

        config.memory.pressure_threshold = 1.5;
        assert!(validate_config(&config).is_err());

        config.memory.pressure_threshold = 0.85;
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_multipart_chunk_size_too_small() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.archive.multipart_chunk_size = 1024; // 1KB < 5MB minimum

        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_codec_case_sensitive() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];

        config.compression.codec = "Zstd".to_string();
        assert!(
            validate_config(&config).is_err(),
            "uppercase Zstd should fail"
        );

        config.compression.codec = "GZIP".to_string();
        assert!(
            validate_config(&config).is_err(),
            "uppercase GZIP should fail"
        );
    }

    /// `Router::route` matches `routing.mode` on the exact string "expression"
    /// and treats everything else as topic routing. An unvalidated `mode:
    /// expresion` (or `Expression`) therefore archives by topic: the configured
    /// per-org path layout never appears, and no log, metric or startup check
    /// reports the mode as ignored.
    #[test]
    fn test_unknown_routing_mode_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.routing.mode = "expresion".to_string();

        assert!(
            validate_config(&config).is_err(),
            "a misspelt routing.mode must be rejected, not silently treated as 'topic'"
        );
    }

    /// `mode: expression` with no `expression_fields` produces the same path as
    /// topic routing, so the routing it names cannot fire -- and it is strictly
    /// worse than topic mode, because expression routing parses every payload as
    /// JSON and errors on anything that is not.
    #[test]
    fn test_expression_routing_without_fields_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.routing.mode = "expression".to_string();
        config.routing.expression_fields = vec![];

        assert!(
            validate_config(&config).is_err(),
            "expression routing with an empty field list can never route on anything"
        );
    }

    #[test]
    fn test_valid_routing_modes_pass() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];

        config.routing.mode = "topic".to_string();
        assert!(validate_config(&config).is_ok(), "topic mode is valid");

        config.routing.mode = "expression".to_string();
        config.routing.expression_fields = vec!["org_id".to_string()];
        assert!(validate_config(&config).is_ok(), "expression mode is valid");
    }

    #[test]
    fn test_s3_config_default_allow_http_is_false() {
        // Security invariant: S3 config must default to HTTPS-only.
        let cfg = dfe_archiver_core::config::S3Config::default();
        assert!(
            !cfg.allow_http,
            "S3Config::default().allow_http must be false (HTTPS required)"
        );
    }

    #[test]
    fn test_s3_config_serde_round_trip_with_allow_http() {
        use dfe_archiver_core::config::S3Config;
        // Round-trip via YAML to ensure allow_http persists.
        let mut cfg = S3Config {
            bucket: "test-bucket".into(),
            allow_http: true,
            ..Default::default()
        };
        let yaml = serde_yaml_ng::to_string(&cfg).expect("serialise");
        let parsed: S3Config = serde_yaml_ng::from_str(&yaml).expect("deserialise");
        assert!(parsed.allow_http);
        assert_eq!(parsed.bucket, "test-bucket");

        // Absent field should deserialise to false (#[serde(default)]).
        cfg.allow_http = false;
        let yaml_no_flag = "bucket: test-bucket\n";
        let parsed: S3Config = serde_yaml_ng::from_str(yaml_no_flag).expect("deserialise no flag");
        assert!(!parsed.allow_http);
    }
}
