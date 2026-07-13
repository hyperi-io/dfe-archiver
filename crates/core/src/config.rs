// Project:   dfe-archiver
// File:      crates/core/src/config.rs
// Purpose:   Configuration type definitions
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use scalo::config::flat_env::{self, ApplyFlatEnv, Normalize};
use scalo::config::sensitive::SensitiveString;
use serde::{Deserialize, Serialize};

pub use scalo::config::sensitive;
pub use scalo::dlq::DlqConfig;
pub use scalo::scaling::ScalingPressureConfig;

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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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

    /// Dead letter queue configuration
    pub dlq: DlqConfig,
}

/// Kafka consumer configuration
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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

    /// SASL password (never serialised in plaintext)
    pub sasl_password: Option<SensitiveString>,

    /// Path to a CA certificate bundle for verifying the broker's TLS cert
    /// (private-CA trust). Maps to librdkafka `ssl.ca.location`.
    pub ssl_ca_location: Option<String>,

    /// Deliberately opt into an unencrypted transport (PLAINTEXT /
    /// `SASL_PLAINTEXT`) in production. Defaults to false: scalo's
    /// Kafka transport REJECTS plaintext in production unless this is set
    /// (e.g. mesh-encrypted in-cluster traffic). Outside production it has no
    /// effect.
    pub allow_insecure_transport: bool,

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
            ssl_ca_location: None,
            allow_insecure_transport: false,
            batch_size: 10_000,
            max_poll_interval_ms: 300_000,
            session_timeout_ms: 30_000,
        }
    }
}

/// Archive output configuration
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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

    /// Maximum number of concurrent archive writers (one per unique routing
    /// destination). When the cap is reached, the least-recently-used writer
    /// is evicted and closed asynchronously. Caps file-handle use and bounds
    /// the `dfe_archiver_unique_destinations` metric for expression-routed
    /// pods on high-cardinality keys (e.g. per-org-id routing).
    pub max_writers: usize,

    /// S3-specific configuration
    pub s3: Option<S3Config>,

    /// GCS-specific configuration
    pub gcs: Option<GcsConfig>,

    /// Azure-specific configuration
    pub azure: Option<AzureConfig>,

    /// MinIO-specific configuration
    pub minio: Option<MinioConfig>,
}

impl ArchiveConfig {
    /// Derive the storage backend name from the destination URL scheme.
    ///
    /// Used as the `backend` label on sink metrics.
    #[must_use]
    pub fn backend_name(&self) -> &'static str {
        if self.destination.starts_with("s3://") {
            "s3"
        } else if self.destination.starts_with("gs://") || self.destination.starts_with("gcs://") {
            "gcs"
        } else if self.destination.starts_with("az://") || self.destination.starts_with("azure://")
        {
            "azure"
        } else if self.minio.is_some() {
            "minio"
        } else {
            "file"
        }
    }
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            destination: "file:///var/data/archive".to_string(),
            // Topic is already prepended as a directory by the archiver (per-topic writers).
            // Do not include {topic} here — it would result in double topic paths.
            path_template: "{year}/{month}/{day}/{hour}".to_string(),
            file_extension: "jsonl".to_string(),
            roll_size_bytes: 1024 * 1024 * 1024, // 1GB final compressed file size
            roll_interval_secs: 3600,            // 1 hour
            multipart_chunk_size: 8 * 1024 * 1024, // 8MB
            max_writers: 1024,
            s3: None,
            gcs: None,
            azure: None,
            minio: None,
        }
    }
}

/// S3 configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<SensitiveString>,
    pub bucket: String,
    /// Allow plaintext HTTP (for local/dev endpoints only). Defaults to false (HTTPS required).
    #[serde(default)]
    pub allow_http: bool,
}

/// GCS configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct GcsConfig {
    pub project_id: Option<String>,
    pub service_account_key: Option<SensitiveString>,
    pub credentials_path: Option<String>,
    pub bucket: String,
}

/// Azure Blob configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct AzureConfig {
    pub account_name: String,
    pub account_key: Option<SensitiveString>,
    pub sas_token: Option<SensitiveString>,
    pub container: String,
    pub use_emulator: bool,
    pub endpoint: Option<String>,
}

/// `MinIO` configuration (S3-compatible)
#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct MinioConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: SensitiveString,
    pub bucket: String,
    pub use_ssl: bool,
}

/// Buffer management configuration
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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

    /// How long to pause Kafka consumption after a backpressure trigger.
    /// Lower values cycle faster but burn more CPU when downstream is slow;
    /// higher values let buffers drain but increase tail latency.
    pub backpressure_pause_secs: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            flush_bytes: 64 * 1024 * 1024, // 64MB
            flush_age_secs: 60,
            flush_records: 100_000,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
        }
    }
}

/// Memory limits configuration
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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
            self.kafka.sasl_password = Some(SensitiveString::from(v));
        }
        if let Some(v) = flat_env::flat_env_string("KAFKA", "SSL_CA_LOCATION") {
            self.kafka.ssl_ca_location = Some(v);
        }
        if let Some(v) = flat_env::flat_env_string("KAFKA", "ALLOW_INSECURE_TRANSPORT") {
            self.kafka.allow_insecure_transport =
                matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes");
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
            s3.secret_access_key = Some(SensitiveString::from(v));
        }
        if let Some(v) = flat_env::flat_env_string("S3", "ENDPOINT") {
            let s3 = self.archive.s3.get_or_insert_with(S3Config::default);
            s3.endpoint = Some(v);
        }
        if let Some(v) = flat_env::flat_env_string("S3", "ALLOW_HTTP") {
            let s3 = self.archive.s3.get_or_insert_with(S3Config::default);
            s3.allow_http = matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes");
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

impl Config {
    /// Register all config sections in the scalo config registry.
    ///
    /// Enables the `/config` admin endpoint to dump effective config
    /// (with automatic redaction of sensitive fields like `sasl_password`).
    pub fn register_in_registry(&self) {
        use scalo::config::registry;
        registry::register("kafka", &self.kafka);
        registry::register("archive", &self.archive);
        registry::register("buffer", &self.buffer);
        registry::register("memory", &self.memory);
        registry::register("routing", &self.routing);
        registry::register("metrics", &self.metrics);
        registry::register("compression", &self.compression);
        registry::register("scaling", &self.scaling);
    }
}
