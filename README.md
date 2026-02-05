<!--
  Project:      dfe-archiver
  File:         README.md
  Purpose:      Project overview and usage documentation
  Language:     Markdown

  License:      FSL-1.1-ALv2
  Copyright:    (c) 2026 HyperI Pty Ltd
-->

# DFE Archiver

High-volume Kafka-to-storage archiver designed for PB/s scale data pipelines.

## Features

- **Multiple destinations**: File, MinIO, S3, GCS, Azure Blob
- **Compression**: Zstd (default), LZ4, Snappy, Gzip
- **Smart routing**: By topic or JSON field expressions (e.g., `org_id`)
- **Rolling archives**: By final compressed file size (1GB default) or time (1 hour default)
- **At-least-once delivery**: Kafka offset commit only after successful archive write
- **Memory-capped**: Tiered buffering with configurable limits
- **Disk protection**: Backpressure when spool exceeds limits or disk space is low

## Architecture

```
Kafka Consumer → Buffer Manager → Archive Writer → Storage Backend
      ↓                ↓               ↓               ↓
  Batch recv      Per-dest       Compressed       File/S3/GCS/
  (10K msgs)      buffering      rolling files    Azure/MinIO
```

### Tiered Buffer Design

Handles high destination cardinality (e.g., 10,000+ orgs) without exhausting memory:

```
Tier 1: Hot Buffers (64 destinations × 1MB = 64MB memory)
    ↓ LRU eviction
Tier 2: Disk Spool (bounded by max_spool_bytes, default 10GB)
    ↓ batch flush
Archive Writers (8 concurrent, semaphore-controlled)
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

Prometheus metrics are exposed at `/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `dfe_archiver_messages_received_total` | Counter | Messages received from Kafka |
| `dfe_archiver_messages_archived_total` | Counter | Messages successfully archived |
| `dfe_archiver_bytes_written_total` | Counter | Uncompressed bytes written |
| `dfe_archiver_bytes_compressed_total` | Counter | Compressed bytes written |
| `dfe_archiver_files_created_total` | Counter | Archive files created |
| `dfe_archiver_files_closed_total` | Counter | Archive files closed (rolled) |
| `dfe_archiver_disk_pressure_events_total` | Counter | Backpressure events |
| `dfe_archiver_buffer_bytes` | Gauge | Current buffer size |
| `dfe_archiver_kafka_lag` | Gauge | Consumer lag |
| `dfe_archiver_hot_buffers_active` | Gauge | Active hot buffers |
| `dfe_archiver_spool_bytes` | Gauge | Current spool size |

Health endpoints:
- `/healthz` - Liveness probe
- `/readyz` - Readiness probe

## Development

### Prerequisites

- Rust 1.75+
- Docker (for local testing)

### Local Testing

```bash
# Start local Kafka + MinIO
docker compose -f docker-compose.dev.yaml up -d

# Run tests
cargo test

# Run with local services
cargo run -- --config config.dev.yaml
```

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# With specific allocator
cargo build --release --features mimalloc
```

## License

This software is licensed under the Functional Source License, Version 1.1,
ALv2 Future License (FSL-1.1-ALv2). See [LICENSE](LICENSE) for details.

Copyright (c) 2026 HyperI Pty Ltd
