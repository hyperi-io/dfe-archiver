// Project:   dfe-archiver
// File:      src/kafka/mod.rs
// Purpose:   Kafka transport abstraction using hs-rustlib
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

mod transport;

pub use transport::{KafkaMessage, KafkaOffset, TransportAdapter};

#[cfg(any(test, feature = "transport-memory"))]
pub use transport::MemoryTransportAdapter;
