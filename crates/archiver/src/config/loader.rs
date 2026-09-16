// Project:   dfe-archiver
// File:      crates/archiver/src/config/loader.rs
// Purpose:   Configuration loading with cascade priority
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver_core::archive::{PATH_TEMPLATE_PLACEHOLDERS, unknown_placeholders};
use dfe_archiver_core::config::{Config, TRANSPORT_GRPC, TRANSPORT_KAFKA};
use dfe_archiver_core::{Error, Result};
use scalo::config::flat_env::{ApplyFlatEnv, Normalize};
use std::path::Path;
use tracing::info;

/// The env prefix for the scalo config cascade. Must equal the contract's
/// `env_prefix` and `ServiceApp::env_prefix` -- it is the prefix the charts
/// build their `<PREFIX>_SECTION__KEY` variables from.
const ENV_PREFIX: &str = "ARCHIVER";

/// Initialise scalo's global config cascade.
///
/// `ServiceRuntime` resolves `version_check`, `metrics`, `logger`,
/// `self_regulation`, `scaling` and `worker_pool` through `from_cascade()`,
/// which returns each type's own default when the cascade was never set up --
/// indistinguishable from a cascade that says "default", and silent. Without
/// this call no `ARCHIVER_*__*` variable a chart renders reaches anything.
///
/// The config reloader re-enters on SIGHUP and on every file change, and the
/// cascade is a `OnceLock`, so a repeat setup is a no-op rather than an error.
fn init_cascade() {
    let opts = scalo::config::ConfigOptions {
        env_prefix: ENV_PREFIX.to_string(),
        // `main` runs dotenvy before arg parsing, so `.env` is already in the
        // process environment; re-loading it here would tie every test that
        // loads config to whatever `.env` sits in the working tree.
        load_dotenv: false,
        ..Default::default()
    };
    if let Err(e) = scalo::config::setup(opts)
        && !matches!(e, scalo::config::ConfigError::AlreadyInitialised)
    {
        // Every reader falls back to its own default, outranking the deployment.
        tracing::warn!(error = %e, "config cascade setup failed; deployment overrides will not apply");
    }
}

/// Load configuration with cascade: CLI → ENV → .env → file → defaults
///
/// Priority (highest to lowest):
/// 1. CLI arguments (handled by caller, merged after)
/// 2. Environment variables (flat env overrides)
/// 3. .env file (loaded by dotenvy in main)
/// 4. Config file (YAML)
/// 5. Hard-coded defaults
///
/// The app's own sections are read straight from `config_path`. Sections scalo
/// owns (`version_check`, `metrics`, `logger`, `self_regulation`, `scaling`,
/// `worker_pool`) come from the cascade `init_cascade` sets up, so they are set
/// by `<PREFIX>_SECTION__KEY` env vars or a `settings.yaml`, NOT by this file --
/// scalo's cascade discovers config files by name and cannot ingest an
/// arbitrary `--config` path (scalo-rs#50).
pub fn load_config(config_path: Option<&str>) -> Result<Config> {
    init_cascade();

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
/// STRUCTURAL faults only -- a contradictory or unusable setting, which the
/// service refuses loudly. A config that is valid and merely EMPTY of work
/// (no topics, no destination) is `Config::idle_reason`'s to answer, and the
/// scalo idle gate keeps the service Ready while it waits for one that names
/// work.
///
/// Called by `load_config` and by the `ConfigReloader` on hot-reload.
pub fn validate_config(config: &Config) -> Result<()> {
    let known_transports = [TRANSPORT_KAFKA, TRANSPORT_GRPC];
    if !known_transports.contains(&config.transport.as_str()) {
        return Err(Error::Config(format!(
            "transport must be one of: {} (got '{}')",
            known_transports.join(", "),
            config.transport
        )));
    }

    if config.is_direct() {
        if config
            .grpc
            .listen
            .as_ref()
            .is_none_or(|l| l.trim().is_empty())
        {
            return Err(Error::Config(
                "grpc.listen is required when transport is 'grpc'".to_string(),
            ));
        }
    } else {
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
    }

    if config.buffer.flush_bytes == 0 {
        return Err(Error::Config(
            "buffer.flush_bytes must be greater than 0".to_string(),
        ));
    }

    // The tiered buffer creates this directory at construction, so an empty
    // value fails the boot with an errno the operator cannot place.
    if config.buffer.spool_dir.trim().is_empty() {
        return Err(Error::Config(
            "buffer.spool_dir cannot be empty".to_string(),
        ));
    }

    // An unsupported placeholder is not substituted and not dropped: it reaches
    // the object store as literal braces in the key, baked into every object
    // already written, so it is refused at startup instead.
    let unknown = unknown_placeholders(&config.archive.path_template);
    if !unknown.is_empty() {
        return Err(Error::Config(format!(
            "archive.path_template has unsupported placeholders: {} (supported: {})",
            unknown.join(", "),
            PATH_TEMPLATE_PLACEHOLDERS.join(", ")
        )));
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
    fn test_empty_spool_dir_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.buffer.spool_dir = String::new();

        let err = validate_config(&config).expect_err("empty spool_dir should fail");
        assert!(
            err.to_string().contains("buffer.spool_dir"),
            "error must name the setting: {err}"
        );
    }

    #[test]
    fn test_empty_group_id_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.kafka.group_id = String::new();

        assert!(validate_config(&config).is_err());
    }

    /// An archiver with nowhere to write is valid and IDLE, never a refusal:
    /// the deployment stands it up before an operator names a destination, and
    /// a refusal there is a crash loop with no probe surface.
    #[test]
    fn test_empty_destination_is_idle_not_invalid() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.kafka.topics = vec!["default_land".to_string()];
        config.archive.destination = String::new();

        validate_config(&config).expect("an empty destination is not a structural fault");
        assert_eq!(
            config.idle_reason(),
            Some("archive.destination is empty -- nowhere to write")
        );
    }

    /// The bus form with neither an explicit topic list nor a discovery
    /// pattern would subscribe to every topic on the broker, which is never
    /// what an unconfigured archiver should start doing.
    #[test]
    fn test_no_topics_and_no_discovery_pattern_is_idle() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.archive.destination = "file:///tmp/archive".to_string();

        validate_config(&config).expect("an empty topic list is not a structural fault");
        assert_eq!(
            config.idle_reason(),
            Some("no kafka.topics and no kafka.topic_include to discover with")
        );
    }

    #[test]
    fn test_explicit_topics_or_a_discovery_pattern_is_work() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.archive.destination = "file:///tmp/archive".to_string();

        config.kafka.topics = vec!["default_land".to_string()];
        assert_eq!(config.idle_reason(), None);

        config.kafka.topics = vec![];
        config.kafka.topic_include = vec!["_land$".to_string()];
        assert_eq!(config.idle_reason(), None);
    }

    /// A bound listener has work the moment a sender dials it, and nothing in
    /// the config says whether one will.
    #[test]
    fn test_a_bound_listener_is_never_idle_for_want_of_topics() {
        let mut config = Config {
            transport: TRANSPORT_GRPC.to_string(),
            ..Config::default()
        };
        config.grpc.listen = Some("0.0.0.0:6000".to_string());
        config.archive.destination = "file:///tmp/archive".to_string();
        config.kafka.brokers = vec![];
        config.kafka.topics = vec![];

        validate_config(&config).expect("the direct form needs no broker");
        assert_eq!(config.idle_reason(), None);
    }

    #[test]
    fn test_direct_transport_without_a_listen_address_fails() {
        let mut config = Config {
            transport: TRANSPORT_GRPC.to_string(),
            ..Config::default()
        };
        config.grpc.listen = None;

        assert!(
            validate_config(&config).is_err(),
            "transport: grpc with no listen address can never receive anything"
        );
    }

    #[test]
    fn test_unknown_transport_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.transport = "rabbit".to_string();

        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_default_transport_is_the_bus() {
        let config = Config::default();
        assert_eq!(config.transport, TRANSPORT_KAFKA);
        assert!(!config.is_direct());
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

    /// `{topic}` and `{date}` were advertised by the field's own doc comment
    /// and neither exists, so a template carrying one wrote literal braces into
    /// every object key.
    #[test]
    fn test_unsupported_path_template_placeholder_fails() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.archive.path_template = "{topic}/{year}/{month}/{day}/{hour}".to_string();

        let err = validate_config(&config).expect_err("{topic} is not substituted");
        assert!(
            err.to_string().contains("{topic}"),
            "error must name the placeholder: {err}"
        );

        config.archive.path_template = "{date}/{hour}".to_string();
        let err = validate_config(&config).expect_err("{date} is not substituted");
        assert!(
            err.to_string().contains("{date}"),
            "error must name the placeholder: {err}"
        );
    }

    #[test]
    fn test_supported_path_template_placeholders_pass() {
        let mut config = Config::default();
        config.kafka.brokers = vec!["localhost:9092".to_string()];
        config.archive.path_template =
            "{year}/{month}/{day}/{hour}/{minute}/{timestamp}/{seq}".to_string();

        validate_config(&config).expect("every supported placeholder is valid");
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
