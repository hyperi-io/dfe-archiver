// Project:   dfe-archiver
// File:      crates/core/src/buffer/mod.rs
// Purpose:   Per-destination buffer management
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

mod tiered;

pub use tiered::{
    BufferStatsSnapshot, DEFAULT_SPOOL_DIR, StagedBatch, TieredBufferConfig, TieredBufferManager,
};
