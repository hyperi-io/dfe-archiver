// Project:   dfe-archiver
// File:      crates/core/src/archive/mod.rs
// Purpose:   Archive file management and rolling
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

mod writer;

pub use writer::{ArchiveWriter, CloseStats, FlushStats, RollingPolicy};
