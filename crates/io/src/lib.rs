// Project:   dfe-archiver
// File:      crates/io/src/lib.rs
// Purpose:   I/O layer - Kafka transport and storage backend implementations
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

pub mod kafka;
pub mod storage;

pub use kafka::TransportAdapter;
pub use storage::{FileBackend, ObjectStoreBackend, create_backend};

#[cfg(any(test, feature = "transport-memory"))]
pub use kafka::MemoryTransportAdapter;
