// Project:   dfe-archiver
// File:      crates/io/src/lib.rs
// Purpose:   I/O layer - Kafka transport and storage backend implementations
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

pub mod grpc;
pub mod kafka;
pub mod storage;
pub mod transport;

pub use grpc::PushTransportAdapter;
pub use kafka::TransportAdapter;
pub use storage::{
    FileBackend, ObjectStoreBackend, Quarantined, Recovery, Staging, create_backend,
};
pub use transport::{ReceivedBatch, SourceTransport};

#[cfg(any(test, feature = "transport-memory"))]
pub use kafka::MemoryTransportAdapter;
