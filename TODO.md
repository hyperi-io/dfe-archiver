<!--
  Project:      dfe-archiver
  File:         TODO.md
  Purpose:      Task tracking and project progress
  Language:     Markdown

  License:      FSL-1.1-ALv2
  Copyright:    (c) 2026 HyperI Pty Ltd
-->

# TODO - DFE Archiver

This is the **single source of truth** for all tasks and progress.

---

## Active Tasks

Tasks currently being worked on. Only one task should be `[IN PROGRESS]` at a time.

- [ ] Implement S3/MinIO storage backend with object_store `[PENDING]`
- [ ] Add KEDA scaling metrics endpoint `[PENDING]`

---

## Work Breakdown Structure (WBS)

### Phase 1: Core Infrastructure

**Goal:** Establish working Kafka-to-file archive pipeline

1. [x] Project structure and Cargo.toml with hyperi-rustlib
2. [x] Configuration cascade (CLI → ENV → .env → file → defaults)
3. [x] Compression codecs (zstd, lz4, snappy, gzip)
4. [x] Buffer manager with flush triggers
5. [x] Routing by topic or JSON expression
6. [x] File storage backend
7. [x] Archive writer with rolling
8. [x] Integration test infrastructure
9. [x] Complete hyperi-rustlib KafkaTransport adapter
10. [x] Main pipeline orchestrator

### Phase 2: Cloud Storage

**Goal:** Support S3, GCS, Azure Blob, MinIO

1. [ ] S3 backend with multipart upload
2. [ ] MinIO backend (S3-compatible)
3. [ ] GCS backend
4. [ ] Azure Blob backend
5. [ ] Integration tests for each backend

### Phase 3: Production Hardening

**Goal:** Production-ready with observability

1. [ ] KEDA-compatible scaling metrics
2. [ ] Prometheus metrics via hyperi-rustlib
3. [ ] Health endpoints (/healthz, /readyz)
4. [ ] Graceful shutdown with buffer drain
5. [ ] Memory pressure handling
6. [ ] DLQ support for failed messages

### Phase 4: Performance Optimization

**Goal:** PB/s scale throughput

1. [ ] Hot path profiling and optimization
2. [ ] SIMD JSON field extraction (mison pattern)
3. [ ] Memory pool for buffer reuse
4. [ ] Parallel archive writers
5. [ ] Benchmark suite

---

## Completed (This Session)

- [x] Created project structure
- [x] Set up Cargo.toml with hyperi-rustlib dependency
- [x] Created .cargo/config.toml for Artifactory registry
- [x] Implemented config module with cascade loading
- [x] Implemented compression module (zstd, lz4, snappy, gzip)
- [x] Implemented buffer manager
- [x] Implemented routing module
- [x] Implemented metrics module
- [x] Implemented storage backend (file)
- [x] Implemented archive writer with rolling
- [x] Created integration test infrastructure
- [x] Created benchmark scaffolding
- [x] Completed KafkaTransport adapter (hyperi-rustlib integration)
- [x] Completed main pipeline orchestrator (src/archiver.rs)
- [x] Fixed at-least-once delivery (offsets committed only after successful archive write)
- [x] Restructured integration tests (moved from tests/integration/ to tests/)

---

## Backlog

### High Priority

- [x] Complete KafkaTransport integration with hyperi-rustlib
- [ ] S3 multipart upload support
- [x] At-least-once delivery with offset commit

### Medium Priority

- [ ] Hot-reload config watcher
- [ ] DLQ producer for failed records
- [ ] Rate limiting/backpressure

### Low Priority

- [ ] Parquet output format
- [ ] Avro output format
- [ ] Schema registry integration

---

## Blocked

(None currently)

---

## Notes for AI Assistants

This file is the **single source of truth** for tasks and progress.

**Rules:**

- All tasks go here, nowhere else
- Planning mode outputs go here (WBS section)
- Mark tasks `[IN PROGRESS]` when starting
- Mark tasks `[x]` when complete, move to Completed section
- Never add tasks to STATE.md or CLAUDE.md

**Status tags:**

- `[PENDING]` - Not started
- `[IN PROGRESS]` - Currently working on
- `[BLOCKED]` - Waiting on something
- `[x]` - Completed (checkbox checked)
