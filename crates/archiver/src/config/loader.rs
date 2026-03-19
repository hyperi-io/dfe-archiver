// Project:   dfe-archiver
// File:      crates/archiver/src/config/loader.rs
// Purpose:   Configuration loading with cascade priority
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver_core::config::Config;
use dfe_archiver_core::{Error, Result};
use hyperi_rustlib::config::flat_env::{ApplyFlatEnv, Normalize};
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
        config.kafka.sasl_password = Some("pass".to_string());
        config.normalize();

        assert_eq!(config.kafka.sasl_mechanism, Some("PLAIN".to_string()));
    }
}
