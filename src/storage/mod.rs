// Project:   dfe-archiver
// File:      src/storage/mod.rs
// Purpose:   Storage backend abstraction
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

mod backend;

pub use backend::{create_backend, FileBackend, ObjectStoreBackend, StorageBackend};
