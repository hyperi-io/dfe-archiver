// Project:   dfe-archiver
// File:      crates/core/src/config.rs
// Purpose:   Configuration type definitions
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use hyperi_rustlib::config::flat_env::{self, ApplyFlatEnv, Normalize};
use serde::{Deserialize, Serialize};

pub use hyperi_rustlib::scaling::ScalingPressureConfig;

/// Root configuration for dfe-archiver.
///
/// ## Hot-reload behavior
///
/// **Hot-reloaded (takes effect on next batch):**
/// - `kafka.batch_size`
/// - `buffer.flush_bytes` / `flush_age_secs` / `flush_records`
/// - `memory.limit_bytes` / `pressure_threshold` / `tracking_enabled`
/// - `scaling.enabled` / `memory_gate_threshold`
///
/// **Requires pod restart:**
/// - `kafka.*` (except `batch_size`) — transport connection established at startup
/// - `archive.*` — storage backend and rolling policy bound at startup
/// - `routing.*` — archive path structure, must be atomic
/// - `compression.*` — file format consistency across rolling set
/// - `metrics.*` — HTTP server binds at startup
/// - `buffer.writer_parallelism` — structural buffer manager config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    /// Kafka consumer configuration
    pub kafka: KafkaConfig,

    /// Archive output configuration
    pub archive: ArchiveConfig,

    /// Buffer management configuration
    pub buffer: BufferConfig,

    /// Memory limits configuration
    pub memory: MemoryConfig,

    /// Routing configuration
    pub routing: RoutingConfig,

    /// Metrics configuration
    pub metrics: MetricsConfig,

    /// Compression configuration
    pub compression: CompressionConfig,

    /// Scaling pressure configuration for KEDA autoscaling
    pub scaling: ScalingPressureConfig,
}

/// Kafka consumer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// Broker addresses
    pub brokers: Vec<String>,

    /// Consumer group ID
    pub group_id: String,

    /// Topics to consume
    pub topics: Vec<String>,

    /// SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
    pub sasl_mechanism: Option<String>,

    /// Security protocol (PLAINTEXT, `SASL_PLAINTEXT`, SSL, `SASL_SSL`)
    pub security_protocol: String,

    /// SASL username
    pub sasl_username: Option<String>,

    /// SASL password
    pub sasl_password: Option<String>,

    /// Batch size for `recv()`
    pub batch_size: usize,

    /// Maximum poll interval (ms)
    pub max_poll_interval_ms: u32,

    /// Session timeout (ms)
    pub session_timeout_ms: u32,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            group_id: "dfe-archiver".to_string(),
            topics: vec![],
            sasl_mechanism: None,
            security_protocol: "PLAINTEXT".to_string(),
            sasl_username: None,
            sasl_password: None,
            batch_size: 10_000,
            max_poll_interval_ms: 300_000,
            session_timeout_ms: 30_000,
        }
    }
}

/// Archive output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ArchiveConfig {
    /// Destination URL (file://, s3://, gs://, az://, minio://)
    pub destination: String,

    /// Path template with placeholders: {topic}, {date}, {hour}, etc.
    pub path_template: String,

    /// File extension (without compression suffix)
    pub file_extension: String,

    /// Rolling trigger: final compressed file size in bytes (not inbound data)
    pub roll_size_bytes: u64,

    /// Rolling trigger: interval in seconds
    pub roll_interval_secs: u64,

    /// Multipart upload chunk size in bytes (min 5MB for S3 compatibility)
    pub multipart_chunk_size: usize,

    /// S3-specific configuration
    pub s3: Option<S3Config>,

    /// GCS-specific configuration
    pub gcs: Option<GcsConfig>,

    /// Azure-specific configuration
    pub azure: Option<AzureConfig>,

    /// MinIO-specific configuration
    pub minio: Option<MinioConfig>,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            destination: "file:///var/data/archive".to_string(),
            path_template: "{topic}/{year}/{month}/{day}/{hour}".to_string(),
            file_extension: "jsonl".to_string(),
            roll_size_bytes: 1024 * 1024 * 1024, // 1GB final compressed file size
            roll_interval_secs: 3600,            // 1 hour
            multipart_chunk_size: 8 * 1024 * 1024, // 8MB
            s3: None,
            gcs: None,
            azure: None,
            minio: None,
        }
    }
}

/// S3 configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub bucket: String,
}

/// GCS configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GcsConfig {
    pub project_id: Option<String>,
    pub service_account_key: Option<String>,
    pub credentials_path: Option<String>,
    pub bucket: String,
}

/// Azure Blob configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AzureConfig {
    pub account_name: String,
    pub account_key: Option<String>,
    pub sas_token: Option<String>,
    pub container: String,
    pub use_emulator: bool,
    pub endpoint: Option<String>,
}

/// `MinIO` configuration (S3-compatible)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MinioConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: String,
    pub use_ssl: bool,
}

/// Buffer management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BufferConfig {
    /// Flush when buffer exceeds this size (bytes)
    pub flush_bytes: usize,

    /// Flush when buffer exceeds this age (seconds)
    pub flush_age_secs: u64,

    /// Maximum records per buffer before flush
    pub flush_records: usize,

    /// Number of concurrent archive writers
    pub writer_parallelism: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            flush_bytes: 64 * 1024 * 1024, // 64MB
            flush_age_secs: 60,
            flush_records: 100_000,
            writer_parallelism: 4,
        }
    }
}

/// Memory limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    /// Hard limit on total memory usage (bytes)
    pub limit_bytes: usize,

    /// Pressure threshold (0.0-1.0) - trigger aggressive flush
    pub pressure_threshold: f64,

    /// Enable memory tracking
    pub tracking_enabled: bool,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            limit_bytes: 512 * 1024 * 1024, // 512MB
            pressure_threshold: 0.8,
            tracking_enabled: true,
        }
    }
}

/// Routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    /// Routing mode: "topic" (default) or "expression"
    pub mode: String,

    /// Field paths for expression-based routing (dot notation)
    /// e.g., [`org_id`, `tags.event_type`]
    pub expression_fields: Vec<String>,

    /// Default path segment when field not found
    pub default_segment: String,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            mode: "topic".to_string(),
            expression_fields: vec![],
            default_segment: "unknown".to_string(),
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Enable metrics server
    pub enabled: bool,

    /// Bind address for metrics HTTP server
    pub address: String,

    /// Metrics path
    pub path: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: "0.0.0.0:9090".to_string(),
            path: "/metrics".to_string(),
        }
    }
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    /// Compression codec: none, zstd, lz4, snappy, gzip
    pub codec: String,

    /// Compression level (codec-specific)
    pub level: i32,

    /// Enable compression
    pub enabled: bool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            codec: "zstd".to_string(),
            level: 3, // zstd default
            enabled: true,
        }
    }
}

/// Apply flat environment variable overrides.
///
/// Uses multiple prefixes to preserve the existing env var contract:
/// `KAFKA_*`, `ARCHIVER_*`, `METRICS_*`, `S3_*`.
///
/// CRITICAL: These env var names are the contract with dfe-engine.
/// Do NOT rename them.
impl ApplyFlatEnv for Config {
    fn apply_flat_env(&mut self, _prefix: &str) {
        // Kafka transport
        if let Some(v) = flat_env::flat_env_list("KAFKA", "BROKERS") {
            self.kafka.brokers = v;
        }
        if let Some(v) = flat_env::flat_env_string("KAFKA", "GROUP_ID") {
            self.kafka.group_id = v;
        }
        if let Some(v) = flat_env::flat_env_list("KAFKA", "TOPICS") {
            self.kafka.topics = v;
        }
        if let Some(v) = flat_env::flat_env_string("KAFKA", "SASL_MECHANISM") {
            self.kafka.sasl_mechanism = Some(v);
        }
        if let Some(v) = flat_env::flat_env_string("KAFKA", "SECURITY_PROTOCOL") {
            self.kafka.security_protocol = v;
        }
        if let Some(v) = flat_env::flat_env_string("KAFKA", "SASL_USER") {
            self.kafka.sasl_username = Some(v);
        }
        if let Some(v) = flat_env::flat_env_string_sensitive("KAFKA", "SASL_PASSWORD") {
            self.kafka.sasl_password = Some(v);
        }

        // Archive
        if let Some(v) = flat_env::flat_env_string("ARCHIVER", "DESTINATION") {
            self.archive.destination = v;
        }
        if let Some(v) = flat_env::flat_env_string("ARCHIVER", "PATH_TEMPLATE") {
            self.archive.path_template = v;
        }
        if let Some(v) = flat_env::flat_env_string("ARCHIVER", "COMPRESSION_CODEC") {
            self.compression.codec = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>("ARCHIVER", "FLUSH_BYTES") {
            self.buffer.flush_bytes = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<u64>("ARCHIVER", "FLUSH_INTERVAL_SECS") {
            self.buffer.flush_age_secs = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>("ARCHIVER", "MEMORY_LIMIT_BYTES") {
            self.memory.limit_bytes = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<usize>("ARCHIVER", "MULTIPART_CHUNK_SIZE") {
            self.archive.multipart_chunk_size = v;
        }

        // Metrics
        if let Some(v) = flat_env::flat_env_string("METRICS", "ADDRESS") {
            self.metrics.address = v;
        }

        // S3
        if let Some(v) = flat_env::flat_env_string("S3", "BUCKET") {
            let s3 = self.archive.s3.get_or_insert_with(S3Config::default);
            s3.bucket = v;
        }
        if let Some(v) = flat_env::flat_env_string("S3", "REGION") {
            let s3 = self.archive.s3.get_or_insert_with(S3Config::default);
            s3.region = Some(v);
        }
        if let Some(v) = flat_env::flat_env_string_sensitive("S3", "ACCESS_KEY_ID") {
            let s3 = self.archive.s3.get_or_insert_with(S3Config::default);
            s3.access_key_id = Some(v);
        }
        if let Some(v) = flat_env::flat_env_string_sensitive("S3", "SECRET_ACCESS_KEY") {
            let s3 = self.archive.s3.get_or_insert_with(S3Config::default);
            s3.secret_access_key = Some(v);
        }
        if let Some(v) = flat_env::flat_env_string("S3", "ENDPOINT") {
            let s3 = self.archive.s3.get_or_insert_with(S3Config::default);
            s3.endpoint = Some(v);
        }
    }
}

/// Normalisation: infer implied settings after all config sources merge.
impl Normalize for Config {
    fn normalize(&mut self) {
        // SASL credentials present → ensure mechanism is set
        if self.kafka.sasl_username.is_some()
            && self.kafka.sasl_password.is_some()
            && self.kafka.sasl_mechanism.is_none()
        {
            self.kafka.sasl_mechanism = Some("PLAIN".to_string());
        }
    }
}
