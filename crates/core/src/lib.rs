// Project:   dfe-archiver
// File:      crates/core/src/lib.rs
// Purpose:   Core library - types, compression, buffering, routing, archive logic
// Language:  Rust
//
// License:      BUSL-1.1
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
pub use types::{KafkaMessage, KafkaOffset, OffsetSet};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
