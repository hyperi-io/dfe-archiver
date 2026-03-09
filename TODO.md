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

- [ ] Verify local release build passes `[PENDING]`
  - Needs libsasl2-dev installed for sasl2-sys
  - Cross-compilation deps provided by CI sysroot, not vendored locally
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

1. [x] S3 backend with multipart upload (ObjectStoreBackend with WriteMultipart streaming)
2. [x] MinIO backend (S3-compatible, via ObjectStoreBackend)
3. [x] GCS backend (ObjectStoreBackend constructor, tested against real GCS)
4. [x] Azure Blob backend (ObjectStoreBackend constructor, tested against real Azure)
5. [x] Integration tests for each backend (S3, GCS, Azure tested with real credentials; MinIO needs Docker)
6. [x] Rolling/rollover tests for all backends (size-based rolling verified on file, S3, GCS, Azure)
7. [x] File sequence counter for unique rolling paths (prevents same-second collisions)
8. [x] `list_prefix()` method on StorageBackend trait (for both FileBackend and ObjectStoreBackend)

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
- [x] Replaced S3Backend with ObjectStoreBackend (streaming multipart uploads, ~8MB/writer vs ~1GB)
- [x] Added S3, MinIO, GCS, Azure factory constructors on ObjectStoreBackend
- [x] Added multipart_chunk_size config, S3 env var overrides, chunk size validation
- [x] Created S3 integration tests (tests/s3_test.rs)
- [x] Updated MinIO integration tests for ObjectStoreBackend
- [x] Created Azure integration tests (tests/azure_test.rs) — 5 tests including rolling
- [x] Created GCS integration tests (tests/gcs_test.rs) — 5 tests including rolling
- [x] Added rolling/rollover tests for all backends (file, S3, GCS, Azure, MinIO)
- [x] Added file_seq counter to ArchiveWriter for unique file paths during rolling
- [x] Added list_prefix() to StorageBackend trait (FileBackend + ObjectStoreBackend)
- [x] Fixed test_rolling_by_size to properly trigger rolling (flush between batches)
- [x] Added test_rolling_by_time (1-second threshold with sleep)
- [x] Added binary app build CI config (.hyperi-ci.yaml, ci.yml, publish.yml)
- [x] Replaced release.yml with publish.yml (matching dfe-loader CI pattern)
- [x] Added rdkafka and openssl vendored deps to Cargo.toml
- [x] Added Building & Artifacts documentation to DESIGN.md
- [x] CI submodule updated to v1.58.18 (mac-friendly changes verified compatible)
- [x] AI submodule updated
- [x] Migrated to 3-crate Cargo workspace (core, io, archiver)
- [x] Audited and updated all dependencies to latest versions
- [x] Replaced deprecated serde_yaml with serde_yaml_ng
- [x] Upgraded thiserror 1.x → 2.x
- [x] Edition 2024, MSRV 1.94 (no pin until OSS)

---

## Backlog

### High Priority

- [x] Complete KafkaTransport integration with hyperi-rustlib
- [x] S3 multipart upload support
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

- Changes staged, awaiting commit — verify local release build before pushing

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
