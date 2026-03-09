<!--
  Project:      dfe-archiver
  File:         docs/DESIGN.md
  Purpose:      Architecture and design documentation
  Language:     Markdown

  License:      FSL-1.1-ALv2
  Copyright:    (c) 2026 HyperI Pty Ltd
-->

# DFE Archiver Design Document

High-volume Kafka-to-storage archiver designed for PB/s scale data pipelines.

## Table of Contents

1. [Building & Artifacts](#building--artifacts)
2. [Overview](#overview)
3. [Architecture](#architecture)
4. [Data Flow](#data-flow)
5. [Tiered Buffer Design](#tiered-buffer-design)
6. [At-Least-Once Delivery](#at-least-once-delivery)
7. [Rolling Policy](#rolling-policy)
8. [Disk Protection](#disk-protection)
9. [Routing](#routing)
10. [Compression](#compression)
11. [Storage Backends](#storage-backends)
12. [Configuration](#configuration)
13. [Metrics](#metrics)
14. [Deployment](#deployment)

---

## Building & Artifacts

### Local Build

```bash
# Debug build
cargo build

# Release build (production, with jemalloc)
cargo build --features default,jemalloc --release
```

**Output location:**

| Build Type | Path |
|---|---|
| Debug (native) | `target/debug/dfe-archiver` |
| Release (native) | `target/release/dfe-archiver` |
| Cross-compile (x86_64) | `target/x86_64-unknown-linux-gnu/release/dfe-archiver` |
| Cross-compile (aarch64) | `target/aarch64-unknown-linux-gnu/release/dfe-archiver` |

### CI/CD Pipeline

CI is managed by the [HyperI CI](https://github.com/hyperi-io/ci) submodule (`ci/`).
Configuration is in `.hyperi-ci.yaml`.

**Pipeline stages:**

```text
Push to any branch
  → CI workflow: Detect → Quality (fmt, clippy, audit) → Test (with coverage)

GitHub Release created (via semantic-release)
  → Publish workflow: Detect → Build (x86_64 + aarch64) → Publish
```

### Build Targets

| Target | Architecture | Notes |
|---|---|---|
| `x86_64-unknown-linux-gnu` | AMD64 | Primary production target |
| `aarch64-unknown-linux-gnu` | ARM64 | AWS Graviton, Apple Silicon Linux |

Cross-compilation uses the CI cross-compilation toolchain (not vendored system
libraries). `openssl` is vendored for portability; `rdkafka` uses `cmake-build`
to compile `librdkafka` from source.

### Artifact Destinations

#### Crate (library)

| Destination | URL |
|---|---|
| HyperI Cargo Registry | `sparse+https://hypersec.jfrog.io/artifactory/api/cargo/hyperi-cargo-virtual/index/` |

Published via `cargo publish --registry hyperi`. Used as a dependency:

```toml
dfe-archiver = { version = ">=1.2", registry = "hyperi" }
```

#### Binaries

| Destination | Location |
|---|---|
| JFrog Artifactory | `https://hypersec.jfrog.io/artifactory/hyperi-binaries/dfe-archiver/{version}/` |
| JFrog Artifactory (latest) | `https://hypersec.jfrog.io/artifactory/hyperi-binaries/dfe-archiver/latest/` |
| GitHub Releases | `https://github.com/hyperi-io/dfe-archiver/releases/` |

**Binary naming convention:**

```text
dfe-archiver-{version}-linux-amd64     # x86_64
dfe-archiver-{version}-linux-arm64     # aarch64
```

**Checksums:** `SHA256SUMS` file published alongside binaries.

### Release Profile

```toml
[profile.release]
lto = "thin"          # Link-time optimization
codegen-units = 1     # Single codegen unit for better optimization
strip = true          # Remove debug symbols
panic = "abort"       # Smaller binary, no unwinding overhead
opt-level = 3         # Maximum optimization
```

---

## Overview

DFE Archiver consumes messages from Kafka topics and archives them to various storage backends (File, MinIO, S3, GCS, Azure Blob) with compression and smart routing. It is designed for:

- **PB/s scale throughput** - optimized hot path, SIMD JSON parsing
- **High destination cardinality** - supports 10,000+ concurrent destinations via tiered buffering
- **At-least-once delivery** - Kafka offsets committed only after successful archive write
- **Memory efficiency** - bounded memory usage with disk spillover
- **Pluggable compression** - zstd (default), lz4, snappy, gzip

### Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Rust 2024 (MSRV 1.94) |
| Async Runtime | Tokio |
| Shared Library | hyperi-rustlib (config, logging, metrics, transport) |
| JSON Parsing | sonic-rs (SIMD-accelerated) |
| Compression | zstd, lz4_flex, snap, flate2 |
| Cloud Storage | object_store (AWS, GCP, Azure) |
| Deployment | Kubernetes with KEDA autoscaling |

---

## Architecture

```text
                    ┌─────────────────────────────────────────────────────────┐
                    │                     DFE ARCHIVER                        │
                    └─────────────────────────────────────────────────────────┘
                                              │
┌──────────────┐    ┌─────────────────────────▼───────────────────────────────┐
│              │    │                                                          │
│    KAFKA     │───▶│  Kafka Transport (hyperi-rustlib)                           │
│   (Strimzi/  │    │  - Batch consumption (10K messages)                      │
│   AutoMQ)    │    │  - SASL/TLS authentication                              │
│              │    │  - Consumer group coordination                          │
└──────────────┘    │  - Offset tracking via KafkaToken                       │
                    │                                                          │
                    └─────────────────────────▼───────────────────────────────┘
                                              │
                    ┌─────────────────────────▼───────────────────────────────┐
                    │  Router                                                  │
                    │  - Topic-based routing (default)                         │
                    │  - Expression-based routing (JSON field extraction)     │
                    │    e.g., route by org_id, event_type                    │
                    └─────────────────────────▼───────────────────────────────┘
                                              │
                    ┌─────────────────────────▼───────────────────────────────┐
                    │  Tiered Buffer Manager                                   │
                    │                                                          │
                    │  ┌────────────────────────────────────────────────┐     │
                    │  │ Tier 1: Hot Buffers (memory, LRU bounded)      │     │
                    │  │ - 64 destinations × 1MB = 64MB max             │     │
                    │  │ - Fast path for active destinations            │     │
                    │  └──────────────────────┬─────────────────────────┘     │
                    │                         │ LRU eviction                   │
                    │  ┌──────────────────────▼─────────────────────────┐     │
                    │  │ Tier 2: Disk Spool (bounded, compressed)       │     │
                    │  │ - Unlimited destinations                       │     │
                    │  │ - Max 10GB default                             │     │
                    │  │ - Segment-based (yaque)                        │     │
                    │  └──────────────────────┬─────────────────────────┘     │
                    │                         │ batch flush                    │
                    └─────────────────────────▼───────────────────────────────┘
                                              │
                    ┌─────────────────────────▼───────────────────────────────┐
                    │  Archive Writers (semaphore-controlled pool)            │
                    │  - 8 concurrent writers (default)                        │
                    │  - Bounded file handle usage                             │
                    │  - One active file per destination                       │
                    │                                                          │
                    │  ┌────────────────────────────────────────────────┐     │
                    │  │ Compressor                                     │     │
                    │  │ - zstd (default, level 3)                      │     │
                    │  │ - lz4, snappy, gzip options                    │     │
                    │  └────────────────────────────────────────────────┘     │
                    │                                                          │
                    │  ┌────────────────────────────────────────────────┐     │
                    │  │ Rolling Policy                                 │     │
                    │  │ - By final compressed file size (1GB default)  │     │
                    │  │ - By time (1 hour default)                     │     │
                    │  └────────────────────────────────────────────────┘     │
                    │                                                          │
                    └─────────────────────────▼───────────────────────────────┘
                                              │
                    ┌─────────────────────────▼───────────────────────────────┐
                    │  Storage Backend (object_store)                         │
                    │  - File system                                           │
                    │  - MinIO / S3                                            │
                    │  - Google Cloud Storage                                  │
                    │  - Azure Blob Storage                                    │
                    └─────────────────────────────────────────────────────────┘
                                              │
                                              ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │  Kafka Offset Commit                                    │
                    │  - Only after CONFIRMED storage write                   │
                    │  - At-least-once delivery guarantee                     │
                    └─────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Message Processing Pipeline

1. **Kafka Consumption**: Batch receive from Kafka (10K messages default)
2. **Routing**: Determine destination based on topic or JSON expression
3. **Buffering**: Buffer in hot memory tier, spill to disk if LRU evicted
4. **Flush Triggers**: Size (64MB), records (100K), or age (60s)
5. **Compression**: Compress batch with configured codec
6. **Storage Write**: Write compressed data to storage backend
7. **Offset Commit**: Commit Kafka offsets only after confirmed write

### Critical Path Optimizations

- **SIMD JSON parsing** via sonic-rs for expression routing
- **Zero-copy message handling** where possible
- **Lock-free atomics** for counters and stats
- **DashMap** for concurrent buffer access
- **CompactString** for destination keys (24-byte inline storage)
- **Semaphore-controlled** writer pool to bound resources

---

## Tiered Buffer Design

### Problem Statement

With expression-based routing (e.g., by `org_id`), you could have 10,000+ unique destinations. Naive approach:

```
10,000 destinations × 64MB buffer = 640GB memory (not feasible)
```

### Solution: Two-Tier Architecture

```text
Tier 1: Hot Buffers (64 destinations × 1MB = 64MB memory)
    ↓ LRU eviction
Tier 2: Disk Spool (bounded by max_spool_bytes, default 10GB)
    ↓ batch flush
Archive Writers (8 concurrent, semaphore-controlled)
```

### Tier 1: Hot Buffers

- **Purpose**: Fast path for active destinations
- **Capacity**: 64 buffers × 1MB each = 64MB max memory
- **Eviction**: LRU when capacity exceeded
- **Flush Triggers**: Size (1MB), age (30s)

### Tier 2: Disk Spool

- **Purpose**: Handle overflow from hot buffers
- **Implementation**: Segment-based queue (yaque)
- **Capacity**: Bounded by `max_spool_bytes` (default 10GB)
- **Reclamation**: Segments deleted when fully consumed
- **Compression**: Optional zstd compression

### Buffer Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_hot_buffers` | 64 | Maximum Tier 1 buffers |
| `hot_buffer_size` | 1MB | Per-buffer size limit |
| `hot_buffer_age_secs` | 30 | Age-based flush trigger |
| `spool_dir` | `.tmp/archiver-spool` | Tier 2 spool directory |
| `max_writers` | 8 | Concurrent archive writers |
| `staging_batch_size` | 64MB | Batch size for archive write |
| `max_spool_bytes` | 10GB | Spool size limit (disk protection) |
| `min_free_disk_bytes` | 1GB | Reserved disk space |

---

## At-Least-Once Delivery

### Guarantee

Messages are **never lost**. In failure scenarios, duplicates may occur (at-least-once semantics).

### Implementation

```text
1. Consume batch from Kafka
2. Buffer messages (memory → disk spool if needed)
3. Compress and write to storage
4. CONFIRMED write successful
5. Commit Kafka offsets ← Only happens AFTER step 4
```

### KafkaToken Tracking

Each message carries a `KafkaToken` containing:

- Topic name
- Partition number
- Offset

Tokens are accumulated during buffering and committed in batch after successful archive write.

### Failure Scenarios

| Failure Point | Outcome | Data Status |
|---------------|---------|-------------|
| Before archive write | Restart re-consumes from Kafka | No data loss |
| After write, before commit | Duplicates on restart | At-least-once |
| After commit | Clean | Exactly processed |

### Crash Recovery

On restart:

1. Kafka consumer rejoins group with last committed offset
2. Re-processes any messages from uncommitted offset
3. Duplicates may exist in archive (idempotent consumers downstream must handle)

---

## Rolling Policy

### File Rolling Triggers

Archives are rolled (closed and new file opened) when either condition is met:

1. **Size-based**: Final compressed file size exceeds threshold (default 1GB)
2. **Time-based**: File age exceeds threshold (default 1 hour)

### Important: Compressed File Size

Rolling is based on **final compressed file size**, NOT:

- Inbound data size
- Uncompressed data size
- Buffered data size

This ensures predictable archive file sizes on storage (optimal for cloud storage parallelism).

### Configuration

```yaml
archive:
  roll_size_bytes: 1073741824  # 1GB final compressed size
  roll_interval_secs: 3600     # 1 hour
```

### Path Templates

Archive paths support template variables:

```yaml
archive:
  path_template: "{topic}/{year}/{month}/{day}/{hour}"
```

Available variables:

- `{topic}` - Kafka topic name
- `{year}`, `{month}`, `{day}`, `{hour}`, `{minute}` - Timestamp components
- `{timestamp}` - Unix timestamp

---

## Disk Protection

### Problem

Unbounded disk spool can exhaust disk space, causing system-wide failures.

### Solution: Backpressure

When disk limits are reached, `push()` returns an error, pausing Kafka consumption until space is freed.

### Protection Mechanisms

1. **Spool Size Limit** (`max_spool_bytes`, default 10GB)
   - Tracks current spool size
   - Returns error if write would exceed limit

2. **Free Disk Space** (`min_free_disk_bytes`, default 1GB)
   - Checks actual filesystem free space (via `fs2` crate)
   - Returns error if write would drop below threshold

### Backpressure Flow

```text
push() called
    ↓
Check spool size < max_spool_bytes?
    ↓ NO → Return Error → Kafka consumption pauses
    ↓ YES
Check free disk > min_free_disk_bytes?
    ↓ NO → Return Error → Kafka consumption pauses
    ↓ YES
Proceed with spool write
```

### Metrics

- `dfe_archiver_disk_pressure_events_total` - Count of backpressure events
- `dfe_archiver_spool_bytes` - Current spool size

---

## Routing

### Topic-Based (Default)

Messages routed by Kafka topic name:

```
Topic: events → Destination: events/
Topic: logs → Destination: logs/
```

### Expression-Based

Route by JSON field values for multi-tenant scenarios:

```yaml
routing:
  mode: expression
  expression_fields:
    - org_id
    - event_type
```

Example message:

```json
{"org_id": "acme", "event_type": "login", "data": {...}}
```

Destination: `events/acme/login/`

### Nested Field Support

Dot notation for nested fields:

```yaml
routing:
  expression_fields:
    - tags.category
```

Message: `{"tags": {"category": "security"}}` → Destination: `events/security/`

### Default Segment

Missing fields use configurable default:

```yaml
routing:
  default_segment: "unknown"
```

---

## Compression

### Supported Codecs

| Codec | Extension | Use Case |
|-------|-----------|----------|
| zstd | `.zst` | Default - best ratio with good speed |
| lz4 | `.lz4` | Speed-critical, lower ratio |
| snappy | `.snappy` | Hadoop ecosystem compatibility |
| gzip | `.gz` | Universal compatibility |

### Configuration

```yaml
compression:
  codec: zstd
  level: 3  # 1-22 for zstd, varies by codec
```

### Compression Ratios (Typical JSON)

| Codec | Ratio | Speed |
|-------|-------|-------|
| zstd-3 | 4-6x | Fast |
| zstd-9 | 5-8x | Moderate |
| lz4 | 2-3x | Very Fast |
| snappy | 2-3x | Very Fast |
| gzip-6 | 4-6x | Moderate |

---

## Storage Backends

### Local File System

```yaml
archive:
  destination: file:///var/data/archive
```

### AWS S3

All cloud backends use `ObjectStoreBackend` with streaming multipart uploads via
the `object_store` crate's `WriteMultipart`. Data is uploaded in configurable
chunk sizes (default 8MB), keeping memory usage bounded to ~chunk_size per active
file rather than buffering the entire file in memory.

Credentials are resolved via `AmazonS3Builder::from_env()`, then config-level
overrides are applied on top. This means standard AWS environment variables and
instance metadata are picked up automatically.

**Credential resolution order:**

1. Config-level `access_key_id` / `secret_access_key` (highest priority)
2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` env vars
3. Web identity token (`AWS_WEB_IDENTITY_TOKEN_FILE` — used by EKS IRSA)
4. EC2/ECS instance metadata (IMDS)

#### Deployment Scenarios

| Scenario | Config Required | Credentials Source |
|---|---|---|
| EKS with IRSA | `bucket`, `region` | IAM role injected via service account |
| EC2 with instance role | `bucket`, `region` | Instance metadata (IMDS) |
| External (outside AWS) | `bucket`, `region`, `access_key_id`, `secret_access_key` | Explicit static credentials |
| SSO / local dev | `bucket`, `region` + exported temp creds | `aws configure export-credentials --format env` |

#### EKS with IRSA (IAM Roles for Service Accounts)

No explicit credentials needed. EKS injects `AWS_ROLE_ARN` and
`AWS_WEB_IDENTITY_TOKEN_FILE` into the pod via the service account annotation.
The `object_store` crate resolves these automatically.

```yaml
archive:
  destination: s3://bucket-name/prefix
  s3:
    region: ap-southeast-2
```

#### External (Outside AWS)

Requires explicit access keys:

```yaml
archive:
  destination: s3://bucket-name/prefix
  s3:
    region: ap-southeast-2
    access_key_id: ${S3_ACCESS_KEY_ID}
    secret_access_key: ${S3_SECRET_ACCESS_KEY}
```

### MinIO (S3-Compatible)

MinIO uses the S3 protocol with a custom endpoint. Always requires explicit
credentials since there is no instance metadata to fall back to.

```yaml
archive:
  destination: minio://bucket-name/prefix
  minio:
    endpoint: http://minio:9000
    access_key: minioadmin
    secret_key: minioadmin
    bucket: archive
    use_ssl: false
```

### Google Cloud Storage

Credentials are resolved via `GoogleCloudStorageBuilder::from_env()`:

1. Config-level `service_account_key` (inline JSON)
2. Config-level `credentials_path` (path to service account JSON file)
3. `GOOGLE_APPLICATION_CREDENTIALS` env var (Application Default Credentials)
4. GCE instance metadata (when running on GCP)

```yaml
archive:
  destination: gs://bucket-name/prefix
  gcs:
    bucket: my-bucket
    credentials_path: /path/to/service-account.json
```

### Azure Blob Storage

Credentials are resolved via `MicrosoftAzureBuilder::from_env()`:

1. Config-level `account_key`
2. Config-level `sas_token`
3. `AZURE_STORAGE_ACCOUNT` / `AZURE_STORAGE_KEY` env vars
4. Managed identity (when running on Azure)

```yaml
archive:
  destination: az://container-name/prefix
  azure:
    account_name: myaccount
    account_key: ${AZURE_STORAGE_KEY}
```

### Multipart Upload Configuration

All cloud backends stream data via multipart uploads with configurable chunk size:

| Parameter | Default | Minimum | Description |
|---|---|---|---|
| `multipart_chunk_size` | 8MB | 5MB | Size of each upload part |

The 5MB minimum is enforced by S3's multipart upload API. Larger chunk sizes
reduce the number of HTTP requests but increase memory per active file.

---

## Configuration

### Cascade Priority

Configuration follows a cascade (highest to lowest priority):

1. CLI arguments (`--kafka-brokers`, etc.)
2. Environment variables (`KAFKA_BROKERS`, etc.)
3. `.env` file
4. Config file (`config.yaml`)
5. Built-in defaults

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID | `dfe-archiver` |
| `KAFKA_TOPICS` | Topics to consume (comma-separated) | (required) |
| `KAFKA_SASL_MECHANISM` | SASL mechanism | (none) |
| `KAFKA_SASL_USER` | SASL username | (none) |
| `KAFKA_SASL_PASSWORD` | SASL password | (none) |
| `ARCHIVER_DESTINATION` | Output destination URL | `file:///var/data/archive` |
| `ARCHIVER_COMPRESSION_CODEC` | Compression codec | `zstd` |
| `ARCHIVER_MULTIPART_CHUNK_SIZE` | Multipart upload chunk size | `8388608` (8MB) |
| `S3_BUCKET` | S3 bucket name | (from config) |
| `S3_REGION` | S3 region | (from config) |
| `S3_ACCESS_KEY_ID` | S3 access key | (from env chain) |
| `S3_SECRET_ACCESS_KEY` | S3 secret key | (from env chain) |
| `S3_ENDPOINT` | S3 endpoint (for MinIO) | (none) |
| `METRICS_ADDRESS` | Metrics server address | `0.0.0.0:9090` |
| `LOG_LEVEL` | Log level | `info` |

### Full Config Example

```yaml
kafka:
  brokers:
    - kafka:9092
  group_id: dfe-archiver
  topics:
    - events
    - logs
  sasl_mechanism: SCRAM-SHA-256
  sasl_username: archiver
  sasl_password: ${KAFKA_PASSWORD}

archive:
  destination: s3://my-bucket/archives
  path_template: "{topic}/{year}/{month}/{day}/{hour}"
  roll_size_bytes: 1073741824
  roll_interval_secs: 3600

buffer:
  flush_bytes: 67108864
  flush_age_secs: 60
  writer_parallelism: 4

compression:
  codec: zstd
  level: 3

routing:
  mode: expression
  expression_fields:
    - org_id
    - event_type

metrics:
  enabled: true
  address: 0.0.0.0:9090
```

---

## Metrics

### Prometheus Endpoint

Metrics exposed at `/metrics` (default `0.0.0.0:9090`).

### Counters

| Metric | Description |
|--------|-------------|
| `dfe_archiver_messages_received_total` | Messages received from Kafka |
| `dfe_archiver_messages_archived_total` | Messages successfully archived |
| `dfe_archiver_messages_dlq_total` | Messages sent to DLQ |
| `dfe_archiver_bytes_written_total` | Uncompressed bytes written |
| `dfe_archiver_bytes_compressed_total` | Compressed bytes written |
| `dfe_archiver_files_created_total` | Archive files created |
| `dfe_archiver_files_closed_total` | Archive files closed (rolled) |
| `dfe_archiver_flush_operations_total` | Flush operations |
| `dfe_archiver_archive_errors_total` | Archive errors |
| `dfe_archiver_disk_pressure_events_total` | Disk pressure backpressure events |

### Gauges

| Metric | Description |
|--------|-------------|
| `dfe_archiver_buffer_bytes` | Current buffer size |
| `dfe_archiver_buffer_records` | Current buffer record count |
| `dfe_archiver_kafka_lag` | Consumer lag (sum across partitions) |
| `dfe_archiver_hot_buffers_active` | Active hot buffers |
| `dfe_archiver_hot_buffers_bytes` | Total bytes in hot buffers |
| `dfe_archiver_spool_bytes` | Current spool size |

### Histograms

| Metric | Description |
|--------|-------------|
| `dfe_archiver_batch_size_bytes` | Archive batch size distribution |
| `dfe_archiver_flush_duration_seconds` | Flush duration distribution |

### Health Endpoints

- `/healthz` - Liveness probe
- `/readyz` - Readiness probe

---

## Deployment

### Kubernetes with KEDA

Production deployment uses KEDA for autoscaling based on Kafka consumer lag:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: dfe-archiver
spec:
  scaleTargetRef:
    name: dfe-archiver
  minReplicaCount: 1
  maxReplicaCount: 100
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka:9092
        consumerGroup: dfe-archiver
        topic: events
        lagThreshold: "10000"
```

### Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 500m | 2 cores |
| Memory | 256MB | 1GB |
| Disk (spool) | 10GB | 50GB |

### Docker

```bash
docker run -d \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPICS=events \
  -e ARCHIVER_DESTINATION=s3://bucket/archive \
  -v /data/spool:/tmp/archiver-spool \
  hyperi/dfe-archiver:latest
```

---

## Key Design Decisions

### 1. Use hyperi-rustlib for Core Infrastructure

**Decision**: Use hyperi-rustlib for config, logging, metrics, and Kafka transport.

**Rationale**: Consistency with other HyperI projects, proven patterns, reduced boilerplate.

**Alternatives**: Direct rdkafka usage, custom config system.

### 2. Compression Codec Selection

**Decision**: Default to zstd level 3, support lz4/snappy/gzip.

**Rationale**: zstd offers best compression ratio with acceptable speed; lz4 for speed-critical; gzip for compatibility.

**Alternatives**: brotli (too slow), lzma (too slow).

### 3. Rolling by Final Compressed Size

**Decision**: Roll based on actual compressed file size (1GB default), not inbound data.

**Rationale**: 1GB files optimal for cloud storage. Inbound data size is unpredictable for final file size.

**Alternatives**: Inbound data size, record count only.

### 4. Tiered Buffering

**Decision**: Two-tier (memory + disk) buffer architecture.

**Rationale**: Handles 10K+ destinations without exhausting memory or file handles.

**Alternatives**: Single large buffer (memory issues), per-destination files (handle exhaustion).

### 5. At-Least-Once via Token Tracking

**Decision**: Track KafkaToken per message, commit only after confirmed archive write.

**Rationale**: Guarantees no data loss. Duplicates acceptable for archive use case.

**Alternatives**: Exactly-once (complex, requires transactional writes).

---

## References

- [hyperi-rustlib](https://github.com/hyperi-io/hyperi-rustlib) - Shared Rust library
- [dfe-loader](https://github.com/hyperi-io/dfe-loader) - Pattern reference
- [object_store docs](https://docs.rs/object_store/)
- [KEDA ScaledObject](https://keda.sh/docs/concepts/scaling-deployments/)
- [sonic-rs](https://github.com/cloudwego/sonic-rs) - SIMD JSON parser
