// Project:   dfe-archiver
// File:      src/kafka/mod.rs
// Purpose:   Kafka transport abstraction using hyperi-rustlib
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

mod transport;

pub use transport::{KafkaMessage, KafkaOffset, TransportAdapter};

#[cfg(any(test, feature = "transport-memory"))]
pub use transport::MemoryTransportAdapter;
