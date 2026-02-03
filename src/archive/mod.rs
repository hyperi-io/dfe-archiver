// Project:   dfe-archiver
// File:      src/archive/mod.rs
// Purpose:   Archive file management and rolling
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

mod writer;

pub use writer::{ArchiveWriter, RollingPolicy};
