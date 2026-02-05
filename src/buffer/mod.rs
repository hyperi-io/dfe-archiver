// Project:   dfe-archiver
// File:      src/buffer/mod.rs
// Purpose:   Per-destination buffer management
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

mod manager;
mod tiered;

pub use manager::BufferManager;
pub use tiered::{BufferStatsSnapshot, StagedBatch, TieredBufferConfig, TieredBufferManager};
