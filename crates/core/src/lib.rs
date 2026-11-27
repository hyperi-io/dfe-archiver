// Project:   dfe-archiver
// File:      crates/core/src/lib.rs
// Purpose:   Core library - types, compression, buffering, routing, archive logic
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

pub mod archive;
pub mod buffer;
pub mod compression;
pub mod config;
pub mod error;
pub mod routing;
pub mod storage;
pub mod types;

pub use error::{Error, Result};
pub use types::{KafkaMessage, KafkaOffset, OffsetSet};
