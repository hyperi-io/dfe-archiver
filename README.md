<!--
  Project:      dfe-archiver
  File:         README.md
  Purpose:      Project overview and usage documentation
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HyperI Pty Ltd
-->

# DFE Archiver

High-volume Kafka-to-storage archiver designed for PB/s scale data pipelines.
Built on the [scalo](https://github.com/hyperi-io/scalo-rs) data-plane runtime
(config cascade, logging, metrics, Kafka transport, tiered sink, deployment
contract).

## Features

- **Multiple destinations**: File, MinIO, S3, GCS, Azure Blob
- **Compression**: Zstd (default), LZ4, Snappy, Gzip, or none
- **Smart routing**: By topic or JSON field expressions (e.g., `org_id`)
- **Rolling archives**: By final compressed file size (1GB default) or time (1 hour default)
- **At-least-once delivery**: Kafka offset commit only after successful archive write
- **Memory-capped**: Tiered buffering with configurable limits
- **Disk protection**: Backpressure when spool exceeds limits or disk space is low

## Architecture

```mermaid
flowchart LR
    K[("Kafka<br/>batch recv, 10K msgs")] --> BM["Buffer Manager<br/>per-destination buffering"]
    BM --> AW["Archive Writer<br/>compressed rolling files"]
    AW --> ST["Storage backend<br/>File / S3 / GCS / Azure / MinIO"]
    ST -. write ok .-> C["Kafka offset commit<br/>at-least-once"]
```

### Workspace Structure

The project is a Rust workspace with three crates:

- **`crates/core`** - Types, configuration, compression codecs, buffer manager, routing, archive writer
- **`crates/io`** - Kafka transport adapter, storage backends (File, S3, GCS, Azure, MinIO)
- **`crates/archiver`** - Binary entry point, pipeline orchestrator, metrics, CLI, deployment contract

### Tiered Buffer Design

Handles high destination cardinality (e.g., 10,000+ orgs) without exhausting memory:

```mermaid
flowchart TB
    R["incoming records"] --> T1["Tier 1: hot buffers<br/>64 destinations x 1MB = 64MB"]
    T1 -->|LRU eviction| T2["Tier 2: disk spool<br/>bounded by max_spool_bytes (10GB default)"]
    T2 -->|batch flush| AW["Archive writers<br/>8 concurrent, semaphore-controlled"]
    T2 -. spool over limit or low disk .-> BP["Backpressure<br/>pause Kafka consume"]
```

## Quick Start

```bash
# Build
cargo build --release

# Run with environment variables
KAFKA_BROKERS=localhost:9092 \
KAFKA_TOPICS=events \
ARCHIVER_DESTINATION=file:///var/data/archive \
./target/release/dfe-archiver

# Or with config file
./target/release/dfe-archiver --config config.yaml
```

## Configuration

Configuration follows a cascade (highest to lowest priority):

1. CLI arguments
2. Environment variables
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
| `ARCHIVER_DESTINATION` | Output URL | `file:///var/data/archive` |
| `ARCHIVER_COMPRESSION_CODEC` | Compression codec | `zstd` |
| `METRICS_ADDRESS` | Metrics server address | `0.0.0.0:9090` |
| `LOG_LEVEL` | Log level | `info` |

### Config File Example

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
  sasl_password: ${KAFKA_PASSWORD}  # env var substitution

archive:
  destination: s3://my-bucket/archives
  path_template: "{topic}/{year}/{month}/{day}/{hour}"
  roll_size_bytes: 1073741824  # 1GB (final compressed size)
  roll_interval_secs: 3600     # 1 hour

buffer:
  flush_bytes: 67108864        # 64MB
  flush_age_secs: 60
  writer_parallelism: 4

compression:
  codec: zstd
  level: 3

routing:
  mode: expression             # or "topic"
  expression_fields:
    - org_id
    - event_type

metrics:
  enabled: true
  address: 0.0.0.0:9090
```

### Hot-Reload Configuration

The archiver supports hot-reloading configuration without restart via SIGHUP
or file polling (5-second interval).

**Hot-reloaded (takes effect on next batch):**
- `kafka.batch_size`
- `buffer.flush_bytes`, `buffer.flush_age_secs`, `buffer.flush_records`
- `memory.limit_bytes`, `memory.pressure_threshold`
- `scaling.enabled`, `scaling.memory_gate_threshold`
- `archive.roll_size_bytes`, `archive.roll_interval_secs`

**Requires pod restart:**
- `kafka.*` (except `batch_size`) - transport connection established at startup
- `archive.destination`, `archive.path_template`, `archive.s3/gcs/azure/minio`
- `routing.*` - archive path structure must be atomic
- `compression.*` - file format consistency across rolling set
- `metrics.*` - HTTP server binds at startup

## Storage Backends

### Local File

```yaml
archive:
  destination: file:///var/data/archive
```

### MinIO / S3

```yaml
archive:
  destination: s3://bucket-name/prefix
  s3:
    endpoint: http://minio:9000
    region: us-east-1
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### Google Cloud Storage

```yaml
archive:
  destination: gs://bucket-name/prefix
  gcs:
    project_id: my-project
    service_account_key: /path/to/key.json
```

### Azure Blob Storage

```yaml
archive:
  destination: az://container-name/prefix
  azure:
    account_name: myaccount
    account_key: ${AZURE_STORAGE_KEY}
```

## At-Least-Once Delivery

The archiver guarantees at-least-once delivery:

1. Messages are consumed from Kafka in batches
2. Messages are buffered per-destination
3. Buffers are compressed and written to storage
4. **Only after successful storage write**, Kafka offsets are committed

If the archiver crashes:
- Before write: Messages are re-consumed from Kafka (no data loss)
- After write, before commit: Duplicates on restart (at-least-once semantics)

## Disk Protection

The archiver protects against disk exhaustion:

- `max_spool_bytes`: Hard limit on spool size (default 10GB)
- `min_free_disk_bytes`: Minimum free space to maintain (default 1GB)

When limits are exceeded, `push()` returns an error (backpressure), causing Kafka consumption to pause until space is freed.

## Metrics

Prometheus metrics at `http://0.0.0.0:9090/metrics` (configurable). Three layers:

**Platform** (`dfe_*`): `records_received_total`, `records_delivered_total`, `transport_sent_total`, `scaling_pressure` - auto-emitted by scalo.

**Metric groups** (`dfe_archiver_*`): `AppMetrics` (received/processed/error counts, memory, config reloads), `BufferMetrics` (bytes, records, flush duration), `ConsumerMetrics` (lag, partitions, rebalances, poll duration), `SinkMetrics` (write duration/errors by backend), `BackpressureMetrics`.

**Archiver-specific** (`dfe_archiver_*`): `files_created_total`, `files_closed_total`, `archive_roll_total{trigger}`, `compression_ratio`, `compression_duration_seconds`, `events_per_second`, `hot_buffers_active`, `unique_destinations`.

**rdkafka stats** (`rdkafka_*`): `broker_rtt_avg_seconds{broker}`, `topic_partition_consumer_lag{topic,partition}`, `consumer_rebalance_count` - collected via sidecar `StatsContext` consumer.

**Health endpoints:** `/healthz` (liveness), `/readyz` (readiness via `HealthRegistry`)

## Development

### Prerequisites

- Rust 1.94+ (edition 2024)
- Docker (for local Kafka via `dfe-docker`)
- `hyperi-ci` for CI validation

### Testing

```bash
# Unit + integration tests (no external services)
cargo nextest run

# E2E tests against Docker-local Kafka
TEST_MODE=docker cargo nextest run -- --ignored

# E2E tests against remote devex Kafka
TEST_MODE=remote cargo nextest run -- --ignored

# Pre-push validation
hyperi-ci check
```

### Building

```bash
cargo build --release

# With specific allocator
cargo build --release --features mimalloc
```

### CLI Commands

```bash
dfe-archiver                      # Run the archiver service
dfe-archiver --config config.yaml # With explicit config
dfe-archiver emit-contract        # Print deployment contract JSON
dfe-archiver emit-dockerfile      # Generate Dockerfile
dfe-archiver emit-helm chart/     # Generate Helm chart
dfe-archiver version              # Version info
```

## License

This software is licensed under the Business Source License 1.1 (BUSL-1.1). See [LICENSE](LICENSE) for details.

Copyright (c) 2026 HyperI Pty Ltd
