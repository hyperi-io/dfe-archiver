// Project:   dfe-archiver
// File:      src/config/loader.rs
// Purpose:   Configuration loading with cascade priority
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

use super::Config;
use crate::{Error, Result};
use std::path::Path;
use tracing::{debug, info};

/// Load configuration with cascade: CLI → ENV → .env → file → defaults
///
/// Priority (highest to lowest):
/// 1. CLI arguments (handled by caller, merged after)
/// 2. Environment variables (ARCHIVER_ prefix)
/// 3. .env file (loaded by dotenvy in main)
/// 4. Config file (YAML)
/// 5. Hard-coded defaults
pub fn load_config(config_path: Option<&str>) -> Result<Config> {
    // Start with defaults
    let mut config = Config::default();

    // Layer 4: Load from config file if specified
    if let Some(path) = config_path {
        config = load_from_file(path)?;
        info!(path = %path, "Loaded configuration from file");
    } else if let Ok(path) = std::env::var("ARCHIVER_CONFIG") {
        config = load_from_file(&path)?;
        info!(path = %path, "Loaded configuration from ARCHIVER_CONFIG");
    } else {
        // Try default locations
        for default_path in &["config.yaml", "config/settings.yaml", "/etc/dfe-archiver/config.yaml"] {
            if Path::new(default_path).exists() {
                config = load_from_file(default_path)?;
                info!(path = %default_path, "Loaded configuration from default location");
                break;
            }
        }
    }

    // Layer 2: Override with environment variables
    apply_env_overrides(&mut config);

    // Validate configuration
    validate_config(&config)?;

    Ok(config)
}

/// Load configuration from YAML file
fn load_from_file(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| Error::Config(format!("failed to read config file '{path}': {e}")))?;

    serde_yaml::from_str(&content)
        .map_err(|e| Error::Config(format!("failed to parse config file '{path}': {e}")))
}

/// Apply environment variable overrides (ARCHIVER_ prefix)
fn apply_env_overrides(config: &mut Config) {
    // Kafka overrides
    if let Ok(brokers) = std::env::var("KAFKA_BROKERS") {
        config.kafka.brokers = brokers.split(',').map(|s| s.trim().to_string()).collect();
        debug!(brokers = %brokers, "Override: kafka.brokers from env");
    }

    if let Ok(group_id) = std::env::var("KAFKA_GROUP_ID") {
        config.kafka.group_id = group_id;
        debug!("Override: kafka.group_id from env");
    }

    if let Ok(topics) = std::env::var("KAFKA_TOPICS") {
        config.kafka.topics = topics.split(',').map(|s| s.trim().to_string()).collect();
        debug!(topics = %topics, "Override: kafka.topics from env");
    }

    if let Ok(mechanism) = std::env::var("KAFKA_SASL_MECHANISM") {
        config.kafka.sasl_mechanism = Some(mechanism);
        debug!("Override: kafka.sasl_mechanism from env");
    }

    if let Ok(protocol) = std::env::var("KAFKA_SECURITY_PROTOCOL") {
        config.kafka.security_protocol = protocol;
        debug!("Override: kafka.security_protocol from env");
    }

    if let Ok(user) = std::env::var("KAFKA_SASL_USER") {
        config.kafka.sasl_username = Some(user);
        debug!("Override: kafka.sasl_username from env");
    }

    if let Ok(password) = std::env::var("KAFKA_SASL_PASSWORD") {
        config.kafka.sasl_password = Some(password);
        debug!("Override: kafka.sasl_password from env (redacted)");
    }

    // Archive overrides
    if let Ok(dest) = std::env::var("ARCHIVER_DESTINATION") {
        config.archive.destination = dest;
        debug!("Override: archive.destination from env");
    }

    if let Ok(template) = std::env::var("ARCHIVER_PATH_TEMPLATE") {
        config.archive.path_template = template;
        debug!("Override: archive.path_template from env");
    }

    // Compression overrides
    if let Ok(codec) = std::env::var("ARCHIVER_COMPRESSION_CODEC") {
        config.compression.codec = codec;
        debug!("Override: compression.codec from env");
    }

    // Buffer overrides
    if let Ok(bytes) = std::env::var("ARCHIVER_FLUSH_BYTES") {
        if let Ok(bytes) = bytes.parse() {
            config.buffer.flush_bytes = bytes;
            debug!("Override: buffer.flush_bytes from env");
        }
    }

    if let Ok(secs) = std::env::var("ARCHIVER_FLUSH_INTERVAL_SECS") {
        if let Ok(secs) = secs.parse() {
            config.buffer.flush_age_secs = secs;
            debug!("Override: buffer.flush_age_secs from env");
        }
    }

    // Metrics overrides
    if let Ok(addr) = std::env::var("METRICS_ADDRESS") {
        config.metrics.address = addr;
        debug!("Override: metrics.address from env");
    }

    // Memory overrides
    if let Ok(limit) = std::env::var("ARCHIVER_MEMORY_LIMIT_BYTES") {
        if let Ok(limit) = limit.parse() {
            config.memory.limit_bytes = limit;
            debug!("Override: memory.limit_bytes from env");
        }
    }
}

/// Validate configuration
fn validate_config(config: &Config) -> Result<()> {
    // Kafka validation
    if config.kafka.brokers.is_empty() {
        return Err(Error::Config("kafka.brokers cannot be empty".to_string()));
    }

    if config.kafka.group_id.is_empty() {
        return Err(Error::Config("kafka.group_id cannot be empty".to_string()));
    }

    // SASL validation
    if config.kafka.sasl_mechanism.is_some() {
        if config.kafka.sasl_username.is_none() || config.kafka.sasl_password.is_none() {
            return Err(Error::Config(
                "kafka.sasl_username and kafka.sasl_password required when sasl_mechanism is set"
                    .to_string(),
            ));
        }
    }

    // Archive validation
    if config.archive.destination.is_empty() {
        return Err(Error::Config(
            "archive.destination cannot be empty".to_string(),
        ));
    }

    // Buffer validation
    if config.buffer.flush_bytes == 0 {
        return Err(Error::Config(
            "buffer.flush_bytes must be greater than 0".to_string(),
        ));
    }

    // Memory validation
    if config.memory.pressure_threshold <= 0.0 || config.memory.pressure_threshold > 1.0 {
        return Err(Error::Config(
            "memory.pressure_threshold must be between 0.0 and 1.0".to_string(),
        ));
    }

    // Compression validation
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
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let mut config = Config::default();
        // Add minimal required fields
        config.kafka.brokers = vec!["localhost:9092".to_string()];

        // Should not panic
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
}
