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

### Migration & Release (Current Sprint)

- [ ] [IN PROGRESS] A: Migrate to single versioning on main
  - [ ] A1: Convert `.releaserc.json` -> `.releaserc.yaml` (branches: [main], remove @semantic-release/github, add missing releaseRules)
  - [ ] A2: Update CI workflow (add tag input, pass to reusable workflow)
  - [ ] A3: Fix VERSION + Cargo.toml versions -> 1.5.2
  - [ ] A4: Create `.githooks/commit-msg`
  - [ ] A5: Delete old `.releaserc.json`
- [ ] B: Migrate to hyperi-rustlib >= 1.20.0
  - [ ] B1: Bump version + cargo update
  - [ ] B2: Fix compilation
  - [ ] B3: Wire `shutdown::install_signal_handler()` (replace manual SIGTERM/SIGINT)
  - [ ] B4: Register health checks via `HealthRegistry`
  - [ ] B5: Add health + shutdown features to Cargo.toml
  - [ ] B6: Verify clippy + tests
- [ ] C: Code review (/review)
- [ ] D: Commit, push, release
  - [ ] D1: Single atomic commit
  - [ ] D2: Push to main
  - [ ] D3: Force-tag v1.5.2 to HEAD
  - [ ] D4: Wait for CI green
  - [ ] D5: Delete stale remote branches (release, merge-to-release)
  - [ ] D6: Release v1.5.3 via hyperi-ci

### Metrics Wiring (Deferred)

- [ ] Wire compression metrics into ArchiveWriter (record_bytes_compressed, record_compression_duration at flush site)
- [ ] Wire archive roll trigger metrics (record_archive_roll with "size"/"age" at roll decision point)
- [ ] Wire record_file_closed with compressed_bytes at writer close
- [ ] Wire record_sink_duration with backend label at storage write site

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

1. [x] KEDA-compatible scaling metrics
2. [x] Prometheus metrics via hyperi-rustlib
3. [x] Health endpoints (/healthz, /readyz)
4. [x] Graceful shutdown with buffer drain
5. [x] Memory pressure handling (MemoryGuard with cgroup-aware backpressure)
6. [x] DFE metrics standard adoption (metric groups, archiver-specific metrics)
7. [ ] DLQ support for failed messages

### Phase 4: Performance Optimization

**Goal:** PB/s scale throughput

1. [ ] Hot path profiling and optimization
2. [ ] SIMD JSON field extraction (mison pattern)
3. [ ] Memory pool for buffer reuse
4. [ ] Parallel archive writers
5. [ ] Benchmark suite

---

## Completed

- [x] Phase 1 complete: core infrastructure (config, compression, buffer, routing, storage, writer, pipeline)
- [x] Phase 2 complete: cloud storage (S3, MinIO, GCS, Azure with multipart uploads)
- [x] Migrated to 3-crate workspace (core, io, archiver)
- [x] Migrated to hyperi-ci from legacy ci submodule
- [x] Switched rdkafka to dynamic linking via hyperi-rustlib >=1.14
- [x] Created Dockerfile following rustlib container contract
- [x] Added Rust tooling config (rustfmt.toml, clippy.toml, deny.toml, rust-toolchain.toml)
- [x] Edition 2024, MSRV 1.94
- [x] KEDA scaling metrics endpoint (ScalingPressure via hyperi-rustlib)
- [x] DeploymentContract integration (emit-dockerfile, emit-helm, emit-contract CLI)
- [x] Migrated CLI to DfeApp pattern (hyperi-rustlib cli feature)
- [x] Hot-reload config via rustlib SharedConfig + ConfigReloader (SIGHUP + file polling)
- [x] CancellationToken replacing AtomicBool for shutdown
- [x] SIGTERM + SIGINT handling in main.rs
- [x] MemoryGuard with cgroup-aware backpressure (Pattern B: pause consumption)
- [x] DFE metrics standard: rustlib metric groups (AppMetrics, BufferMetrics, ConsumerMetrics, SinkMetrics, BackpressureMetrics)
- [x] Archiver-specific metrics: compression ratio/duration, routing errors, staleness gauge, evictions, archive roll triggers, storage backend labels
- [x] Test infrastructure with docker/remote dual-mode (TEST_MODE=docker|remote)
- [x] Released v1.5.0 GA (full CI: quality, test, build amd64+arm64, release, publish)
- [x] Restructured tests/ to HyperI testing standard (smoke.rs, integration/, e2e/, fixtures/)
- [x] Single-binary test pattern (integration.rs + e2e.rs with #[path] submodules)
- [x] Added 49 new unit tests (79 total, up from 30): writer, metrics, error, types, buffer, compression, routing, config, storage
- [x] Mandatory smoke test: startup components + deployment contract
- [x] rdkafka StatsContext sidecar consumer for broker/partition metrics (KafkaStatsEmitter)
- [x] EPS (events per second) gauge for KEDA autoscaling
- [x] gRPC transport metrics (auto-emitted via rustlib transport-grpc feature)
- [x] Bumped hyperi-rustlib to >=1.19.7 (SensitiveString, config registry)

---

## Backlog

### High Priority

- [x] Complete KafkaTransport integration with hyperi-rustlib
- [x] S3 multipart upload support
- [x] At-least-once delivery with offset commit

### Medium Priority

- [x] Hot-reload config watcher
- [x] Update hyperi-ai submodule to latest
- [ ] Documentation review using /doco skill (audit docs against code reality)
- [ ] Rebuild and retest with updated hyperi-ci (prod/test change separation)
- [ ] DLQ producer for failed records
- [ ] Rate limiting/backpressure

### Low Priority

- [ ] Parquet output format
- [ ] Avro output format
- [ ] Schema registry integration

---

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
