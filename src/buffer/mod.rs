// Project:   dfe-archiver
// File:      src/buffer/mod.rs
// Purpose:   Per-destination buffer management
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

mod manager;
mod tiered;

pub use manager::BufferManager;
pub use tiered::{BufferStatsSnapshot, StagedBatch, TieredBufferConfig, TieredBufferManager};
