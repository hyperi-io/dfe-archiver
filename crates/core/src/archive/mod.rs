// Project:   dfe-archiver
// File:      crates/core/src/archive/mod.rs
// Purpose:   Archive file management and rolling
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

mod writer;

pub use writer::{
    ArchiveWriter, CloseStats, FlushStats, PATH_TEMPLATE_PLACEHOLDERS, RollingPolicy, Settled,
    unknown_placeholders,
};
