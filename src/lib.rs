// Project:   dfe-archiver
// File:      src/lib.rs
// Purpose:   Library root - high-volume Kafka-to-storage archiver
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
#![forbid(unsafe_code)]

//! # DFE Archiver
//!
//! High-volume Kafka-to-storage archiver designed for PB/s scale data pipelines.
//!
//! ## Features
//!
//! - **Multiple destinations**: File, MinIO, S3, GCS, Azure Blob
//! - **Compression**: Zstd, LZ4, Snappy, Gzip (configurable)
//! - **Smart routing**: By topic or JSON field expressions
//! - **Rolling archives**: By size or time interval
//! - **At-least-once delivery**: Kafka offset commit after successful archive
//! - **Memory-capped**: Configurable memory limits with backpressure
//!
//! ## Architecture
//!
//! ```text
//! Kafka Consumer → Buffer Manager → Archive Writer → Storage Backend
//!       ↓                ↓               ↓
//!   Batch recv      Per-dest       Compressed
//!   (10K msgs)      buffering      rolling files
//! ```

pub mod archive;
pub mod archiver;
pub mod buffer;
pub mod compression;
pub mod config;
pub mod error;
pub mod kafka;
pub mod metrics;
pub mod routing;
pub mod storage;

pub use archiver::Archiver;

// Re-export key types for convenience
pub use config::Config;
pub use error::{Error, Result};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default batch size for Kafka consumption
pub const DEFAULT_BATCH_SIZE: usize = 10_000;

/// Default buffer flush threshold (bytes)
pub const DEFAULT_FLUSH_BYTES: usize = 64 * 1024 * 1024; // 64MB

/// Default buffer flush interval (seconds)
pub const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 60;

/// Default rolling file size (final compressed size, not inbound data)
pub const DEFAULT_ROLL_SIZE_BYTES: u64 = 1024 * 1024 * 1024; // 1GB
