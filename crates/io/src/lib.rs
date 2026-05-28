// Project:   dfe-archiver
// File:      crates/io/src/lib.rs
// Purpose:   I/O layer - Kafka transport and storage backend implementations
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

pub mod kafka;
pub mod storage;

pub use kafka::{KafkaStatsEmitter, TransportAdapter};
pub use storage::{FileBackend, ObjectStoreBackend, create_backend};

#[cfg(any(test, feature = "transport-memory"))]
pub use kafka::MemoryTransportAdapter;
