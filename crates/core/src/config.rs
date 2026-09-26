// Project:   dfe-archiver
// File:      crates/core/src/config.rs
// Purpose:   Configuration type definitions
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::buffer::DEFAULT_SPOOL_DIR;
use scalo::config::flat_env::{self, ApplyFlatEnv, Normalize};
use scalo::config::sensitive::SensitiveString;
use scalo::transport::AcknowledgementsConfig;
use serde::{Deserialize, Serialize};

pub use scalo::config::sensitive;
pub use scalo::dlq::DlqConfig;

/// Root configuration for dfe-archiver.
///
/// Covers the archiver's own sections only. The memory guard, the metrics
/// listener and the scaling-pressure engine are scalo's, built by
/// `ServiceRuntime` before the archiver exists, and are configured through the
/// cascade (`ARCHIVER_MEMORY_*` for the guard, `--metrics-addr` /
/// `METRICS_ADDR` / `ARCHIVER_METRICS__ADDRESS` for the listener,
/// `ARCHIVER_SCALING__*` for the engine). A parallel `memory:` / `metrics:` /
/// `scaling:` block here parsed, validated and reached nothing.
///
/// ## Hot-reload behavior
///
/// **Hot-reloaded**, because the pipeline re-reads them from the shared config:
/// - `kafka.batch_size` -- once per receive
/// - `buffer.backpressure_pause_secs` -- on each backpressure pause
///
/// **Requires pod restart** -- everything else. `Archiver` snapshots the config
/// at construction into `startup_config`, so a reload of `transport`,
/// `kafka.*`, `grpc.*`, `archive.*`, `routing.*`, `compression.*`, `dlq.*` or
/// the `buffer.*` flush thresholds is accepted and validated but does not reach
/// the running pipeline. `restart_required_changes` names those sections, and
/// the reloader warns instead of reporting a reload.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct Config {
    /// Which transport carries records IN: `kafka` (a broker holds them between
    /// stages) or `grpc` (the scalo Push listener, point to point). Same two
    /// spellings as dfe-loader's `transport`, because a deployment sets both
    /// from the one `kafka.mode` dial.
    pub transport: String,

    /// Kafka consumer configuration
    pub kafka: KafkaConfig,

    /// Push listener configuration, read when `transport` is `grpc`.
    pub grpc: GrpcConfig,

    /// Archive output configuration
    pub archive: ArchiveConfig,

    /// Buffer management configuration
    pub buffer: BufferConfig,

    /// Routing configuration
    pub routing: RoutingConfig,

    /// Compression configuration
    pub compression: CompressionConfig,

    /// Dead letter queue configuration
    pub dlq: DlqConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            transport: default_transport(),
            kafka: KafkaConfig::default(),
            grpc: GrpcConfig::default(),
            archive: ArchiveConfig::default(),
            buffer: BufferConfig::default(),
            routing: RoutingConfig::default(),
            compression: CompressionConfig::default(),
            // Fleet DLQ standard defaults: fixed per-app topic, routing=common.
            // Archiver entry destinations are archive PATHS (slashes), so
            // scalo's per-table default would build invalid topic names.
            // Applies when the config file has no `dlq:` key; a partial `dlq:`
            // block reverts nested fields to scalo's own defaults.
            dlq: {
                let mut dlq = DlqConfig::default();
                dlq.kafka.routing = scalo::dlq::DlqRouting::Common;
                dlq.kafka.common_topic = "dfe_archiver_dlq".to_string();
                dlq
            },
        }
    }
}

/// `transport` value selecting the bus: a broker holds records between stages.
pub const TRANSPORT_KAFKA: &str = "kafka";

/// `transport` value selecting the direct form: the scalo Push listener.
pub const TRANSPORT_GRPC: &str = "grpc";

fn default_transport() -> String {
    TRANSPORT_KAFKA.to_string()
}

/// Roll interval when none is configured and offsets are held: at 10k
/// records/s that keeps about 96 MB of held offsets per pod.
pub const HELD_OFFSETS_ROLL_INTERVAL_SECS: u64 = 300;

/// Roll interval when none is configured and no offset waits on a file.
pub const DEFAULT_ROLL_INTERVAL_SECS: u64 = 3600;

/// A roll interval above this, with offsets held, is warned about at startup.
pub const HELD_OFFSETS_ROLL_WARN_SECS: u64 = 900;

/// Approximate memory each held record costs: scalo's offset tracking plus the
/// archive writer's own copy.
pub const HELD_OFFSET_BYTES: u64 = 32;

impl Config {
    /// Whether records arrive on the Push listener rather than a broker.
    #[must_use]
    pub fn is_direct(&self) -> bool {
        self.transport == TRANSPORT_GRPC
    }

    /// Whether the Kafka consumer holds each offset until the archive file
    /// holding its record completes.
    #[must_use]
    pub fn holds_offsets(&self) -> bool {
        !self.is_direct() && self.kafka.acknowledgements.enabled
    }

    /// The roll interval in force: `archive.roll_interval_secs` when set,
    /// otherwise a default short enough to bound held-offset memory.
    #[must_use]
    pub fn roll_interval_secs(&self) -> u64 {
        self.archive
            .roll_interval_secs
            .unwrap_or(if self.holds_offsets() {
                HELD_OFFSETS_ROLL_INTERVAL_SECS
            } else {
                DEFAULT_ROLL_INTERVAL_SECS
            })
    }

    /// The roll interval when offsets are held for longer than
    /// [`HELD_OFFSETS_ROLL_WARN_SECS`], or `None` when it needs no warning.
    #[must_use]
    pub fn long_held_roll_interval(&self) -> Option<u64> {
        let secs = self.roll_interval_secs();
        (self.holds_offsets() && secs > HELD_OFFSETS_ROLL_WARN_SECS).then_some(secs)
    }

    /// Why this configuration gives the archiver nothing to do, or `None` when
    /// it has work.
    ///
    /// THE emptiness predicate -- the one place that decides idle, read by
    /// `ServiceApp::work_state` and by the tests. Structural faults are
    /// `validate_config`'s; this answers only "valid, but nothing to archive".
    ///
    /// A bound listener always has work: a sender can arrive at any moment and
    /// nothing in the config says whether one will. So the direct form idles
    /// only for a missing destination.
    #[must_use]
    pub fn idle_reason(&self) -> Option<&'static str> {
        if self.archive.destination.trim().is_empty() {
            return Some("archive.destination is empty -- nowhere to write");
        }
        if !self.is_direct() && self.kafka.topics.is_empty() && self.kafka.topic_include.is_empty()
        {
            return Some("no kafka.topics and no kafka.topic_include to discover with");
        }
        None
    }
}

/// Push listener configuration for the direct transport.
///
/// Mirrors dfe-loader's `grpc` block key for key, because both stages receive
/// the same scalo Push RPC and an operator reading one config should not have
/// to learn a second set of names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct GrpcConfig {
    /// Listen address, e.g. `0.0.0.0:6000`. Required when `transport` is `grpc`.
    pub listen: Option<String>,

    /// Records buffered from incoming RPCs before the sender is held.
    pub recv_buffer_size: usize,

    /// Receive timeout in milliseconds (0 = non-blocking).
    pub recv_timeout_ms: u64,

    /// Maximum message size in bytes.
    pub max_message_size: usize,

    /// Enable gzip compression for gRPC messages.
    pub compression: bool,

    /// Archive destination key for a record whose sender set no routing key.
    pub default_topic: String,

    /// The listener answers each push once its records are queued, with this
    /// on or off: a record is released only when its archive file completes, at
    /// the roll interval, long after any sender's deadline. So the archive copy
    /// on this transport is at-most-once, and `enabled: false` only changes the
    /// reason `pipeline_delivery_guarantee` reports, from `sink_cannot_confirm`
    /// to `acks_disabled`.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            listen: None,
            recv_buffer_size: 10_000,
            recv_timeout_ms: 100,
            max_message_size: 16 * 1024 * 1024,
            compression: false,
            default_topic: "default_land".to_string(),
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Kafka consumer configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct KafkaConfig {
    /// Broker addresses
    pub brokers: Vec<String>,

    /// Consumer group ID
    pub group_id: String,

    /// Topics to consume. Empty turns on scalo's broker-side auto-discovery,
    /// filtered by `topic_include`/`topic_exclude` and refreshed every
    /// `topic_refresh_secs`, so a source added after this pod started is
    /// archived without a restart.
    pub topics: Vec<String>,

    /// Regex patterns a discovered topic must match (OR). Empty means every
    /// topic the broker holds that `topic_exclude` does not drop.
    pub topic_include: Vec<String>,

    /// Regex patterns that drop a discovered topic (OR). Exclude wins over
    /// include. Defaults to scalo's list, which already drops the DLQ and
    /// Kafka's own internal topics.
    pub topic_exclude: Vec<String>,

    /// How often the discovered topic list is re-read from the broker.
    pub topic_refresh_secs: u64,

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

    /// With `enabled: true` (the default) a record's offset is committed only
    /// once the archive file holding it is complete in the store, so a kill
    /// re-reads what an open file held: duplicates are possible, loss is not.
    /// With `enabled: false` offsets are committed as records are received,
    /// and a kill loses what the open files and buffers held.
    pub acknowledgements: AcknowledgementsConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            group_id: "dfe-archiver".to_string(),
            topics: vec![],
            topic_include: vec![],
            // Taken from scalo rather than restated, so a new internal-topic
            // pattern reaches this app with the transport it belongs to.
            topic_exclude: scalo::transport::KafkaConfig::default().topic_exclude,
            topic_refresh_secs: scalo::transport::KafkaConfig::default().topic_refresh_secs,
            sasl_mechanism: None,
            security_protocol: "PLAINTEXT".to_string(),
            sasl_username: None,
            sasl_password: None,
            ssl_ca_location: None,
            allow_insecure_transport: false,
            batch_size: 10_000,
            max_poll_interval_ms: 300_000,
            session_timeout_ms: 30_000,
            acknowledgements: AcknowledgementsConfig::default(),
        }
    }
}

/// Archive output configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ArchiveConfig {
    /// Destination URL (file://, s3://, gs://, az://, minio://). Empty until an
    /// operator names one, which is an idle archiver rather than a default
    /// path: a container-local directory nobody asked for looks like it is
    /// archiving and loses every file when the pod is replaced.
    pub destination: String,

    /// Path template under the routed destination. The supported placeholders
    /// are exactly {year}, {month}, {day}, {hour}, {minute}, {timestamp} and
    /// {seq}; any other is refused at config validation, because the writer
    /// would pass it through into the object key as literal braces.
    pub path_template: String,

    /// File extension (without compression suffix)
    pub file_extension: String,

    /// Rolling trigger: final compressed file size in bytes (not inbound data)
    pub roll_size_bytes: u64,

    /// Rolling trigger: interval in seconds. Unset, it is 300 while the Kafka
    /// consumer holds offsets (`transport: kafka` with
    /// `kafka.acknowledgements.enabled`, the default) and 3600 otherwise,
    /// because every record in an open file keeps about 32 bytes of held
    /// offset in memory until the file completes. A configured value always
    /// wins, and one above 900 with offsets held logs a warning at startup.
    pub roll_interval_secs: Option<u64>,

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
            destination: String::new(),
            // The router's destination is already prepended as a directory, so
            // the topic needs no placeholder of its own here.
            path_template: "{year}/{month}/{day}/{hour}".to_string(),
            file_extension: "jsonl".to_string(),
            roll_size_bytes: 1024 * 1024 * 1024, // 1GB final compressed file size
            roll_interval_secs: None,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, schemars::JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct GcsConfig {
    pub project_id: Option<String>,
    pub service_account_key: Option<SensitiveString>,
    pub credentials_path: Option<String>,
    pub bucket: String,
}

/// Azure Blob configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct AzureConfig {
    pub account_name: String,
    pub account_key: Option<SensitiveString>,
    pub sas_token: Option<SensitiveString>,
    pub container: String,
    pub use_emulator: bool,
    pub endpoint: Option<String>,
}

/// `MinIO` configuration (S3-compatible)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct MinioConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: SensitiveString,
    pub bucket: String,
    pub use_ssl: bool,
}

/// Buffer management configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct BufferConfig {
    /// Flush a destination's in-memory buffer into its archive file once it
    /// holds this many bytes, plus one record. A ceiling per destination: up
    /// to 64 destinations buffer at once, and all of them together hold no
    /// more than a quarter of the memory limit, past which the largest
    /// buffers flush first.
    pub flush_bytes: usize,

    /// Flush when buffer exceeds this age (seconds)
    pub flush_age_secs: u64,

    /// Flush a destination's in-memory buffer into its archive file once it
    /// holds this many records, whatever its size.
    pub flush_records: usize,

    /// Object-store uploads of closed archive files running at once. Each
    /// holds four parts of `archive.multipart_chunk_size` in memory. A local
    /// destination has no uploads, so this does not apply to it.
    pub writer_parallelism: usize,

    /// How long to pause Kafka consumption after a backpressure trigger.
    /// Lower values cycle faster but burn more CPU when downstream is slow;
    /// higher values let buffers drain but increase tail latency.
    pub backpressure_pause_secs: u64,

    /// Directory holding `uploads/`, where object-store files are staged until
    /// the store takes them, created at startup. Absolute because a relative
    /// path resolves under the container WORKDIR, which appuser cannot write.
    pub spool_dir: String,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            flush_bytes: 1024 * 1024, // 1 MiB
            flush_age_secs: 60,
            flush_records: 100_000,
            writer_parallelism: 2,
            backpressure_pause_secs: 5,
            spool_dir: DEFAULT_SPOOL_DIR.to_string(),
        }
    }
}

/// Routing configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct RoutingConfig {
    /// Routing mode: "expression" (default) or "topic"
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
            mode: "expression".to_string(),
            expression_fields: vec!["org_id".to_string()],
            default_segment: "unknown".to_string(),
        }
    }
}

/// Compression configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct CompressionConfig {
    /// Compression codec: none, zstd (`.zst`), lz4 (LZ4 frame format, `.lz4`),
    /// snappy (snappy framing format, `.sz`), gzip (`.gz`). Each flush appends
    /// one frame or gzip member.
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
/// `KAFKA_*`, `ARCHIVER_*`, `DLQ_*`, `S3_*`. Single underscore throughout,
/// because these are the archiver's own sections. The double-underscore names
/// (`ARCHIVER_VERSION_CHECK__ENABLED` and the rest) belong to the scalo runtime
/// sections and reach them through the cascade, never through here.
///
/// CRITICAL: These env var names are the contract with dfe-engine.
/// Do NOT rename them.
///
/// `ARCHIVER_MEMORY_*` is absent by design: those names belong to scalo's
/// `MemoryGuard`, which reads them itself. Mapping them here would shadow the
/// guard with an unread second copy, and reject its `0` (auto-detect from the
/// cgroup) as invalid.
impl ApplyFlatEnv for Config {
    fn apply_flat_env(&mut self, _prefix: &str) {
        self.apply_transport_env();
        self.apply_kafka_env();
        self.apply_dlq_env();
        self.apply_archive_env();
    }
}

impl Config {
    /// Which transport carries records in, and where its listener binds. The
    /// chart sets these from the deployment-wide `kafka.mode` dial, so a
    /// brokerless profile authors no config blob to switch the archiver over.
    fn apply_transport_env(&mut self) {
        if let Some(v) = flat_env::flat_env_string("ARCHIVER", "TRANSPORT") {
            self.transport = v;
        }
        if let Some(v) = flat_env::flat_env_string("ARCHIVER", "GRPC_LISTEN") {
            self.grpc.listen = Some(v);
        }
    }

    /// The broker, its credentials, and which topics to read.
    fn apply_kafka_env(&mut self) {
        if let Some(v) = flat_env::flat_env_list("KAFKA", "BROKERS") {
            self.kafka.brokers = v;
        }
        if let Some(v) = flat_env::flat_env_string("KAFKA", "GROUP_ID") {
            self.kafka.group_id = v;
        }
        if let Some(v) = flat_env::flat_env_list("KAFKA", "TOPICS") {
            self.kafka.topics = v;
        }
        if let Some(v) = flat_env::flat_env_list("KAFKA", "TOPIC_INCLUDE") {
            self.kafka.topic_include = v;
        }
        if let Some(v) = flat_env::flat_env_list("KAFKA", "TOPIC_EXCLUDE") {
            self.kafka.topic_exclude = v;
        }
        if let Some(v) = flat_env::flat_env_parsed::<u64>("KAFKA", "TOPIC_REFRESH_SECS") {
            self.kafka.topic_refresh_secs = v;
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
    }

    /// Fleet-uniform DLQ names. TOPIC routes every entry to one fixed topic
    /// (the per-app standard) rather than per-destination suffix topics.
    fn apply_dlq_env(&mut self) {
        if let Some(v) = flat_env::flat_env_bool("DLQ", "ENABLED") {
            self.dlq.enabled = v;
        }
        if let Some(v) = flat_env::flat_env_string("DLQ", "TOPIC") {
            self.dlq.kafka.routing = scalo::dlq::DlqRouting::Common;
            self.dlq.kafka.common_topic = v;
        }
        if let Some(v) = flat_env::flat_env_string("DLQ", "MODE") {
            use scalo::dlq::DlqMode;
            self.dlq.mode = match v.as_str() {
                "fan_out" => DlqMode::FanOut,
                "file_only" => DlqMode::FileOnly,
                "kafka_only" => DlqMode::KafkaOnly,
                "cascade" => DlqMode::Cascade,
                other => {
                    // A typo'd mode must not silently pick a backend -- cascade
                    // includes the file backend, an EROFS no-op deployed, and
                    // the archiver aborts boot when that backend fails to init.
                    tracing::warn!(mode = %other, "unknown DLQ_MODE, using cascade");
                    DlqMode::Cascade
                }
            };
        }
    }

    /// Where archives are written, how they are rolled and compressed, where
    /// they are staged, plus the metrics address and the S3 credentials
    /// the same operator supplies.
    fn apply_archive_env(&mut self) {
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
        if let Some(v) = flat_env::flat_env_parsed::<usize>("ARCHIVER", "MULTIPART_CHUNK_SIZE") {
            self.archive.multipart_chunk_size = v;
        }
        if let Some(v) = flat_env::flat_env_string("ARCHIVER", "SPOOL_DIR") {
            self.buffer.spool_dir = v;
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
        // SASL credentials present -> ensure mechanism is set
        if self.kafka.sasl_username.is_some()
            && self.kafka.sasl_password.is_some()
            && self.kafka.sasl_mechanism.is_none()
        {
            self.kafka.sasl_mechanism = Some("PLAIN".to_string());
        }
    }
}

impl Config {
    /// Register all config sections in the scalo config registry, which drives
    /// redaction of sensitive fields such as `sasl_password`.
    ///
    /// The registry also backs a `/config` dump, but that route belongs to
    /// `scalo::http_server` and the archiver serves only the metrics listener,
    /// so this binary exposes no such endpoint.
    pub fn register_in_registry(&self) {
        use scalo::config::registry;
        registry::register("kafka", &self.kafka);
        registry::register("grpc", &self.grpc);
        registry::register("archive", &self.archive);
        registry::register("buffer", &self.buffer);
        registry::register("routing", &self.routing);
        registry::register("compression", &self.compression);
    }
}
