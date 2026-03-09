// Project:   dfe-archiver
// File:      crates/core/src/lib.rs
// Purpose:   Core library - types, compression, buffering, routing, archive logic
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

pub mod archive;
pub mod buffer;
pub mod compression;
pub mod config;
pub mod error;
pub mod routing;
pub mod storage;
pub mod types;

pub use error::{Error, ErrorCategory, Result};
pub use types::{KafkaMessage, KafkaOffset};

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
