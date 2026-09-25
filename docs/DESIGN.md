<!--
  Project:      dfe-archiver
  File:         docs/DESIGN.md
  Purpose:      Architecture and design documentation
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HyperI Pty Ltd
-->

# DFE Archiver Design Document

High-volume Kafka-to-storage archiver designed for PB/s scale data pipelines.

## Table of Contents

1. [Building & Artifacts](#building--artifacts)
2. [Overview](#overview)
3. [Architecture](#architecture)
4. [Inbound transport](#inbound-transport)
5. [Data Flow](#data-flow)
6. [Tiered Buffer Design](#tiered-buffer-design)
7. [At-Least-Once Delivery](#at-least-once-delivery)
8. [Rolling Policy](#rolling-policy)
9. [Disk Protection](#disk-protection)
10. [Routing](#routing)
11. [Compression](#compression)
12. [Storage Backends](#storage-backends)
13. [Configuration](#configuration)
14. [Metrics](#metrics)
15. [Deployment](#deployment)

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
| Shared Library | scalo (config, logging, metrics, transport) |
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
│    KAFKA     │───▶│  Kafka Transport (scalo)                                    │
│   (Strimzi/  │    │  - Batch consumption (10K messages)                      │
│   AutoMQ)    │    │  - SASL/TLS authentication                              │
│              │    │  - Consumer group coordination                          │
└──────────────┘    │  - Offset tracking via KafkaToken                       │
                    │                                                          │
                    └─────────────────────────▼───────────────────────────────┘
                                              │
                    ┌─────────────────────────▼────────────────────────────────────┐
                    │  Router                                                      │
                    │  - Topic-based routing                                       │
                    │  - Expression-based routing (default, JSON field extraction) │
                    │    e.g., route by org_id, event_type                         │
                    └─────────────────────────▼────────────────────────────────────┘
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
                    │  │ - By time (default 300 s or 1 hour)            │     │
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

## Inbound transport

`transport` selects how records reach the archiver, and it is the only thing
about the pipeline that differs between the two forms.

| `transport` | How records arrive | Release point |
|---|---|---|
| `kafka` | a consumer group over the landing topics | broker offset commit once the archive file holding the record is complete |
| `grpc` | the scalo Push listener, from the previous stage | the Push RPC response, at enqueue |

On `kafka` an empty `topics` list turns on broker-side discovery: the topic set
is re-read every `topic_refresh_secs` and filtered by `topic_include` and
`topic_exclude`, so a source added later is archived without a restart. Where a
source has both a landing and a transformed topic, discovery keeps the landing
one -- the archiver's job is the record as it arrived, which is the reverse of
the loader's preference.

`grpc` exists so a deployment with no broker can still archive: the previous
stage fans a matched record out to the loader and to the archiver over the same
Push RPC. A push stream keeps no backlog, so the KEDA composite drops its
`kafka_lag` term there rather than reading a false zero.

The listener answers each push once its records are queued. A record is released only when its archive file completes, at the roll interval, long after any sender's deadline, so holding the answer until then would expire every push and the sender would resend it. The archive copy on `grpc` is therefore at-most-once: a kill loses what the queue and the open files held, and `pipeline_delivery_guarantee` reports `best_effort` with reason `sink_cannot_confirm`.

### Why only two

scalo offers more transports and DLQ backends than the archiver uses. It compiles in
`transport-kafka`, `transport-grpc` and `dlq-kafka`, and that bound is a
decision rather than an oversight: a DFE deployment feeds the archiver from a
broker or from the previous stage and from nothing else, the DLQ has to survive
the charts' read-only rootfs (which rules the file backend out as an EROFS
no-op), and `validate_config` refuses any other `transport` value at boot. The
feature lists in the three member crates carry the same note, so adding
`transport-all` grows the image without widening anything an operator can
select.

### Idle until configured

An archiver with no destination, or on the bus with no topics and no discovery
pattern, has nothing to do. It starts anyway: it stays Ready, serves health and
metrics, opens no broker connection and binds no listener, reports the
`work_config` health component Degraded with the reason, and holds
`pipeline_idle` at 1. The first config change that gives it work starts the
pipeline with no restart. That behaviour is scalo's (`scalo::lifecycle`); the
predicate is this app's, in `Config::idle_reason`.

Structural faults are the other half of the split and still refuse loudly: an
unknown transport, a `grpc` form with no listen address, a bus form with no
brokers, a codec or routing mode that does not exist.

## Data Flow

### Message Processing Pipeline

1. **Ingest**: Batch receive from the configured transport (10K messages default)
2. **Routing**: Determine destination based on topic or JSON expression
3. **Buffering**: Buffer in hot memory tier, spill to disk if LRU evicted
4. **Flush Triggers**: Size (64MB), records (100K), or age (60s)
5. **Compression**: Compress batch with configured codec
6. **Storage Write**: Write compressed data to storage backend
7. **Release**: Commit offsets once the file holding their records is complete (a no-op on `grpc`)

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
- **Capacity**: 64 buffers x 1MB each = 64MB max memory
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
| `spool_dir` | `/var/spool/dfe/archiver` | Tier 2 spool directory (`buffer.spool_dir` / `ARCHIVER_SPOOL_DIR`) |
| `max_writers` | 8 | Concurrent archive writers |
| `staging_batch_size` | 64MB | Batch size for archive write |
| `max_spool_bytes` | 10GB | Spool size limit (disk protection) |
| `min_free_disk_bytes` | 1GB | Reserved disk space |

---

## At-Least-Once Delivery

### Guarantee

On `kafka` with `acknowledgements.enabled` (the default) no record is lost: a kill, a failed upload or a store outage of any length costs duplicates or consumer lag. Only a refusal the store gives for the object itself drops records, and they are counted. On `grpc` the archive copy is at-most-once (see [Inbound transport](#inbound-transport)).

### Implementation

```text
1. Receive a batch, and the armed consumer records every offset it hands out
2. Buffer per destination
3. Write each staged batch into the destination's open file, and hold its
   offsets on that file
4. The file closes (roll, age close, eviction or shutdown): a local file is
   synced, an object-store file is handed to an upload task
5. The upload task retries until the store takes the file
6. Release the file's offsets, and the consumer commits each partition up to
   its lowest offset not yet released
```

An object-store file is written to local staging, `<buffer.spool_dir>/uploads`, and uploaded whole once it closes. The loop never waits on the store: step 5 runs in a background task, which the loop collects each cycle. A failed upload keeps the staged file and its held offsets and retries with exponential backoff and jitter, from 0.5 s up to 60 s between attempts. Every attempt is a fresh multipart upload from the staged copy, so it signs its requests afresh however long the store has been down. On a local path, step 4 syncs the file, and every directory above it up to the destination, to disk.

Holding costs memory for as long as a file is not durable: about 32 bytes a record, scalo's offset tracking plus the writer's own eight. At 10k records/s that is 96 MB over a 300 s roll and 1.15 GB over an hour. So while offsets are held an unset `roll_interval_secs` defaults to 300 rather than 3600. A configured value always wins, and one above 900 logs a startup warning with the cost.

While uploads fail, held records and staged files grow, so both are pressure sources on the self-regulation latch that pauses the Kafka partitions. Held records read against a quarter of the memory limit at 32 bytes each, staged bytes against 8 GiB, under the spool volume's 10 GiB `emptyDir` limit. Both pause at the latch's `pause_above` (0.8 by default), so a store outage turns into consumer lag rather than memory or disk growth. With self-regulation off nothing pauses, and the archiver logs that at startup.

Only a refusal the store gives for the object itself is permanent: an invalid path, a key past the 1024-byte limit, which is checked before any record is staged, or an `EntityTooLarge`/`KeyTooLongError` answer. Its records are released `Dropped`, counted in `messages_dropped_total`, and the reason is logged. Anything else, credentials and a missing bucket included, is retried, because retrying costs lag while dropping costs records.

A batch no file takes goes to the DLQ through scalo's confirming write. Only a write the DLQ confirms releases the batch's offsets. When the DLQ refuses it or is off, a batch the store refused for good is released `Dropped` with its reason. Any other -- a local disk failure -- is released `Errored`, as are the offsets of a file whose local write or sync failed. Nothing reads an `Errored` span again short of a restart or rebalance, and scalo's consumer has no seek back to the commit floor, so the loop ends with `Error::Withheld`: the process drains, exits non-zero, and the restart reads again from the committed offset.

`kafka.acknowledgements.enabled: false` commits each batch at receipt instead.

### Failure Scenarios

| Failure Point | Outcome | Data Status |
|---------------|---------|-------------|
| Before the file is durable | Restart re-reads every record the file held, and clears the staged files | Duplicates, up to one roll interval plus pending uploads |
| Upload fails | File and offsets kept, upload retried, intake paused near the caps | Delayed, never lost |
| Store refuses the object for good | Offsets released `Dropped`, counted, reason logged | Dropped |
| Local write or sync fails | Offsets released `Errored`, the process exits and restarts | Duplicates |
| Write fails, DLQ confirms | Offsets released | Record is in the DLQ |
| After the commit | Clean | Archived once |

### Shutdown

The run loop stops first. The drain then closes the source -- the Push listener stops answering, the Kafka consumer stops fetching and can still commit -- and receives until the source reports it is empty, so every push already answered is written. Buffers flush into their files, evicted writers finish closing, every open file completes, and the uploads get 20 s. A file still uploading then keeps its offsets unreleased, to be read again after the restart.

### Object keys

Every file name ends `-<seq>-<writer id>`. A staged file is never checked against the store, so two writers on the same destination and window -- two replicas, or an evicted writer still closing beside its replacement -- would otherwise pick the same key, and the later upload would replace the earlier object.

---

## Rolling Policy

### File Rolling Triggers

Archives are rolled (closed and new file opened) when either condition is met:

1. **Size-based**: Final compressed file size exceeds threshold (default 1GB)
2. **Time-based**: File age exceeds threshold (default 300 s while offsets are held, otherwise 1 hour)

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
  roll_interval_secs: 300      # unset: 300 while offsets are held, else 3600
```

### Path Templates

The template sits under the routed destination, which is already the topic (or
the expression-routed segments), so it carries no topic placeholder of its own:

```yaml
archive:
  path_template: "{year}/{month}/{day}/{hour}"
```

The supported variables, and the whole set -- config validation refuses any
other `{...}` token rather than letting it reach the object key as literal
braces:

- `{year}`, `{month}`, `{day}`, `{hour}`, `{minute}` - Timestamp components
- `{timestamp}` - Unix timestamp
- `{seq}` - Rolled-file sequence number

The writer appends `-<seq>-<writer id>`, the extension and the codec suffix to the expanded template, whatever the template holds. See [Object keys](#object-keys).

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

Message: `{"tags": {"category": "security"}}` -> Destination: `events/security/`

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
  destination: file:///var/data/archive   # a mounted volume, never the rootfs
```

A file is synced to disk, with the directories above it, before its offsets are released, so a node crash after the commit cannot lose it.

### AWS S3

All cloud backends use `ObjectStoreBackend`, which writes each file to local staging and uploads it whole once it closes, as a multipart upload via the `object_store` crate's `WriteMultipart`. Parts are `multipart_chunk_size` (default 8MB), four in flight per upload and two uploads at once, so upload memory stays near 64 MB whatever the file size.

Credentials are resolved via `AmazonS3Builder::from_env()`, then config-level
overrides are applied on top. This means standard AWS environment variables and
instance metadata are picked up automatically.

**Credential resolution order:**

1. Config-level `access_key_id` / `secret_access_key` (highest priority)
2. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` env vars
3. Web identity token (`AWS_WEB_IDENTITY_TOKEN_FILE` -- used by EKS IRSA)
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
| `ARCHIVER_TRANSPORT` | `kafka` (a broker) or `grpc` (the Push listener) | `kafka` |
| `ARCHIVER_GRPC_LISTEN` | Push listener bind address, on `grpc` | (none) |
| `KAFKA_BROKERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_GROUP_ID` | Consumer group ID | `dfe-archiver` |
| `KAFKA_TOPICS` | Topics to consume (comma-separated); empty discovers | (discover) |
| `KAFKA_TOPIC_INCLUDE` | Regex patterns a discovered topic must match | (none) |
| `KAFKA_TOPIC_EXCLUDE` | Regex patterns that drop a discovered topic | DLQ + internal |
| `KAFKA_TOPIC_REFRESH_SECS` | How often discovery re-reads the broker | `60` |
| `KAFKA_SASL_MECHANISM` | SASL mechanism | (none) |
| `KAFKA_SASL_USER` | SASL username | (none) |
| `KAFKA_SASL_PASSWORD` | SASL password | (none) |
| `ARCHIVER_DESTINATION` | Output destination URL | (none -- idles) |
| `ARCHIVER_COMPRESSION_CODEC` | Compression codec | `zstd` |
| `ARCHIVER_MULTIPART_CHUNK_SIZE` | Multipart upload chunk size | `8388608` (8MB) |
| `S3_BUCKET` | S3 bucket name | (from config) |
| `S3_REGION` | S3 region | (from config) |
| `S3_ACCESS_KEY_ID` | S3 access key | (from env chain) |
| `S3_SECRET_ACCESS_KEY` | S3 secret key | (from env chain) |
| `S3_ENDPOINT` | S3 endpoint (for MinIO) | (none) |
| `ARCHIVER_MEMORY_LIMIT_BYTES` | Memory guard cap; `0` auto-detects from the cgroup | `0` |
| `ARCHIVER_MEMORY_PRESSURE_THRESHOLD` | Backpressure trigger, 0.0-1.0 | `0.8` |
| `ARCHIVER_MEMORY_CGROUP_HEADROOM` | Fraction of the cgroup limit to use | `0.85` |
| `ARCHIVER_SCALING__MEMORY_GATE_THRESHOLD` | Ratio that forces scaling pressure to 100 | `0.8` |
| `ARCHIVER_VERSION_CHECK__ENABLED` | `false` disables the startup version check | `true` |
| `METRICS_ADDR` | Metrics server address | `0.0.0.0:9090` |
| `LOG_LEVEL` | Log level | `info` |

The memory guard, the metrics listener and the scaling-pressure engine are built
by the scalo runtime before the config file is read, so they take the variables
above and never a `memory:`, `metrics:` or `scaling:` block in the config file.
The single-underscore names are read directly; the double-underscore ones nest
into the config cascade (`ARCHIVER_SCALING__ENABLED` sets `scaling.enabled`).

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
  path_template: "{year}/{month}/{day}/{hour}"
  roll_size_bytes: 1073741824
  roll_interval_secs: 300

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
```

---

## Metrics

### Prometheus Endpoint

Metrics exposed at `/metrics` (default `0.0.0.0:9090`).

### Counters

| Metric | Description |
|--------|-------------|
| `dfe_archiver_messages_received_total` | Messages received from Kafka |
| `dfe_archiver_messages_archived_total` | Records in archive files the store confirmed |
| `dfe_archiver_messages_written_total` | Records written into an open archive file, before the store confirms it |
| `dfe_archiver_messages_dropped_total` | Records dropped because the store refused their object for good |
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
| `dfe_archiver_kafka_lag` | Records past this pod's read position (sum across assigned partitions). The commit an open file holds does not inflate it |
| `pipeline_delivery_guarantee{guarantee,reason}` | 1 for the delivery guarantee in force |
| `dfe_archiver_hot_buffers_active` | Active hot buffers |
| `dfe_archiver_hot_buffers_bytes` | Total bytes in hot buffers |
| `dfe_archiver_spool_bytes` | Current spool size |
| `dfe_archiver_uploads_pending` | Archive files staged locally and not yet confirmed by the store |
| `dfe_archiver_staged_bytes` | Bytes of archive files staged locally and not yet uploaded |

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

KEDA scales on the `dfe_scaling_pressure` composite, not on a raw Kafka lag trigger: the deployment contract declares none. The composite's `kafka_lag` term is the consumer's position lag -- records past its read position -- because the committed-offset lag grows by up to a roll interval of intake while open files hold their commit, and would scale the archiver out on the hold rather than on backlog. `buffer_depth` and `memory` are the other terms, and an open sink circuit zeroes the composite.

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
  -v /data/spool:/var/spool/dfe/archiver \
  ghcr.io/hyperi-io/dfe-archiver:latest
```

---

## Key Design Decisions

### 1. Use scalo for Core Infrastructure

**Decision**: Use scalo for config, logging, metrics, and Kafka transport.

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

**Decision**: Track a Kafka offset per message, hold it on the archive file its record is written into, and release it when that file completes.

**Rationale**: A record is durable only in a completed file, so this guarantees no data loss. Duplicates, up to a roll interval of them after a kill, are acceptable for the archive use case.

**Alternatives**: Exactly-once (complex, requires transactional writes).

---

## References

- [scalo](https://github.com/hyperi-io/scalo-rs) - Shared Rust library
- [dfe-loader](https://github.com/hyperi-io/dfe-loader) - Pattern reference
- [object_store docs](https://docs.rs/object_store/)
- [KEDA ScaledObject](https://keda.sh/docs/concepts/scaling-deployments/)
- [sonic-rs](https://github.com/cloudwego/sonic-rs) - SIMD JSON parser
