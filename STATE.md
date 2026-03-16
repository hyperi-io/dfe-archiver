<!--
  Project:      dfe-archiver
  File:         STATE.md
  Purpose:      Project state and implementation status
  Language:     Markdown

  License:      FSL-1.1-ALv2
  Copyright:    (c) 2026 HyperI Pty Ltd
-->

# Project Context

**Project:** DFE Archiver
**Purpose:** High-volume Kafka-to-storage archiver for PB/s scale data pipelines

> **Note:** The `ai/` submodule provides standards and configuration - not code
> to import. Your project never imports or links to it.

---

## DO NOT ADD TO THIS FILE

**The following belong elsewhere:**

| Data | Correct Location |
|------|------------------|
| Version numbers | `VERSION` file, `git describe --tags` |
| Tasks/Progress | `TODO.md` |
| Session history | Git log (`git log --oneline -10`) |
| Changelog | `CHANGELOG.md` (semantic-release) |
| Dates | Git commit timestamps |

**This file is for static project context only.**

---

## Project Overview

### Architecture

```text
Kafka Consumer → Buffer Manager → Archive Writer → Storage Backend
      ↓                ↓               ↓               ↓
  Batch recv      Per-dest       Compressed       File/S3/GCS/
  (10K msgs)      buffering      rolling files    Azure/MinIO
```

**Key Design Principles:**

- At-least-once delivery (commit after successful archive)
- Memory-capped buffering with backpressure
- Rolling archives by size or time
- Pluggable compression (zstd, lz4, snappy, gzip)
- Pluggable storage backends

### Key Components

1. **Kafka Transport** - Wraps hyperi-rustlib KafkaTransport for message consumption
2. **Buffer Manager** - Per-destination buffering with flush triggers (size/age/records)
3. **Router** - Routes messages by topic or JSON field expressions
4. **Compressor** - Pluggable compression codecs
5. **Archive Writer** - Handles file rolling and path generation
6. **Storage Backend** - Abstracts file, S3, GCS, Azure, MinIO

### Tech Stack

- **Language:** Rust (2021 edition, MSRV 1.94)
- **Async Runtime:** Tokio
- **Shared Library:** hyperi-rustlib (config, logging, metrics, transport)
- **JSON Parsing:** sonic-rs (SIMD-accelerated)
- **Compression:** zstd, lz4_flex, snap, flate2
- **Cloud Storage:** object_store (AWS, GCP, Azure)
- **Deployment:** Kubernetes with KEDA autoscaling

---

## Key Decisions

### Use hyperi-rustlib for Core Infrastructure

**Decision:** Use hyperi-rustlib for config, logging, metrics, and Kafka transport
**Rationale:** Consistency with other HyperI projects, proven patterns, reduces boilerplate
**Alternatives considered:** Direct rdkafka usage, custom config system

### Compression Codec Selection

**Decision:** Default to zstd level 3, support lz4/snappy/gzip
**Rationale:** zstd offers best compression ratio with acceptable speed; lz4 for speed-critical; gzip for compatibility
**Alternatives considered:** brotli (too slow), lzma (too slow)

### Rolling Policy

**Decision:** Roll by final compressed file size (default 1GB) OR time (default 1 hour)
**Rationale:** 1GB files are optimal for cloud storage (S3/GCS/Azure) - balances parallelism with overhead. Rolling is based on actual file size on disk, NOT inbound/uncompressed data size.
**Alternatives considered:** Inbound data size (unpredictable final size), record count only (unpredictable sizes)

### Buffer Flush Triggers

**Decision:** Three-way trigger: size (64MB), records (100K), age (60s)
**Rationale:** Prevents memory bloat while ensuring timely delivery
**Alternatives considered:** Single trigger (too limiting)

### Routing Modes

**Decision:** Support topic-based (default) and expression-based routing
**Rationale:** Simple topic routing covers most cases; expression-based for multi-tenant
**Alternatives considered:** Regex routing (too slow for hot path)

---

## External Dependencies

- **hyperi-rustlib** - Shared HyperI library (Artifactory registry)
- **Kafka** - AutoMQ or Strimzi deployment
- **Object Storage** - S3/MinIO/GCS/Azure for production archives

---

## Resources

**Documentation:**

- [docs/DESIGN.md](docs/DESIGN.md) - Architecture and design documentation
- [README.md](README.md) - Usage documentation

**Reference Projects:**

- [dfe-loader](https://github.com/hyperi-io/dfe-loader) - Pattern reference for Kafka consumption
- [hyperi-rustlib](https://github.com/hyperi-io/hyperi-rustlib) - Shared Rust library

**External Resources:**

- [rdkafka docs](https://docs.rs/rdkafka/)
- [object_store docs](https://docs.rs/object_store/)
- [KEDA ScaledObject](https://keda.sh/docs/concepts/scaling-deployments/)

---

## Configuration Cascade

Priority (highest to lowest):

1. CLI arguments (`--kafka-brokers`, etc.)
2. Environment variables (`KAFKA_BROKERS`, etc.)
3. `.env` file
4. Config file (`config.yaml`)
5. Hard-coded defaults

**Environment Variables:**

| Variable                     | Description              | Default                  |
| ---------------------------- | ------------------------ | ------------------------ |
| `KAFKA_BROKERS`              | Kafka broker addresses   | localhost:9092           |
| `KAFKA_GROUP_ID` | Consumer group ID | dfe-archiver |
| `KAFKA_TOPICS` | Topics to consume | (required) |
| `KAFKA_SASL_MECHANISM` | SASL mechanism | (none) |
| `KAFKA_SASL_USER` | SASL username | (none) |
| `KAFKA_SASL_PASSWORD` | SASL password | (none) |
| `ARCHIVER_DESTINATION` | Output destination URL | file:///var/data/archive |
| `ARCHIVER_COMPRESSION_CODEC` | Compression codec | zstd |
| `METRICS_ADDRESS` | Metrics server address | 0.0.0.0:9090 |

---

## Notes for AI Assistants

This file contains **static project context only**.

**Build host rules:**

- **NEVER kill cargo processes** to free the build directory lock. Multiple projects share this host and run cargo concurrently. Wait for the lock to clear.

**DO NOT add:**

- Version numbers (use `git describe --tags`)
- Progress/tasks (use `TODO.md`)
- Dates or session history (use `git log`)
- "Current Session" or "Last Session" sections

**DO add:**

- Architecture decisions and rationale
- Key component descriptions
- External dependencies
- How things work (not what's happening)

When in doubt, ask: "Will this be true next week?" If no, it doesn't belong here.
