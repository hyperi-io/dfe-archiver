// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e.rs
// Purpose:   End-to-end tests requiring real infrastructure (Kafka, S3, GCS, Azure, MinIO)
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! E2E tests that require real infrastructure.
//! Run with: `cargo nextest run --test e2e` or `cargo nextest run -- --ignored`

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::manual_let_else,
    clippy::unused_async,
    clippy::doc_markdown
)]

#[path = "common/mod.rs"]
mod common;

#[path = "e2e/azure.rs"]
mod azure;
#[path = "e2e/container_hygiene.rs"]
mod container_hygiene;
#[path = "e2e/gcs.rs"]
mod gcs;
#[path = "e2e/kafka.rs"]
mod kafka;
#[path = "e2e/minio.rs"]
mod minio;
#[path = "e2e/pipeline.rs"]
mod pipeline;
#[path = "e2e/s3.rs"]
mod s3;
