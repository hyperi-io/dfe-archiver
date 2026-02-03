// Project:   dfe-archiver
// File:      src/storage/mod.rs
// Purpose:   Storage backend abstraction
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

mod backend;

pub use backend::{create_backend, FileBackend, S3Backend, StorageBackend};
