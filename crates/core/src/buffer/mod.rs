// Project:   dfe-archiver
// File:      crates/core/src/buffer/mod.rs
// Purpose:   Per-destination buffer management
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

mod manager;
mod tiered;

pub use manager::BufferManager;
pub use tiered::{BufferStatsSnapshot, StagedBatch, TieredBufferConfig, TieredBufferManager};
