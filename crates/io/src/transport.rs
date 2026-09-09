// Project:   dfe-archiver
// File:      crates/io/src/transport.rs
// Purpose:   One inbound transport surface over the bus and the direct forms
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! The archiver's inbound transport, whichever form a deployment runs.
//!
//! `bus` and `direct` are the product's two words for it; the config spells
//! them `kafka` and `grpc`, matching dfe-loader so both sinks read the same.
//! The pipeline holds this enum rather than a concrete adapter, so nothing
//! downstream of `recv` knows which form delivered the records.

use dfe_archiver_core::Result;
use dfe_archiver_core::config::Config;
use dfe_archiver_core::types::{KafkaMessage, KafkaOffset};
use scalo::SelfRegulationGovernor;
use scalo::transport::filter::FilteredDlqEntry;

use crate::grpc::PushTransportAdapter;
use crate::kafka::TransportAdapter;

/// A received block: the passing messages plus any inbound-filter DLQ entries.
///
/// The DLQ entries are surfaced (never silently dropped) so the orchestrator
/// can route them onward. The archiver configures no inbound scalo filters, so
/// `dlq_entries` is empty in practice -- but the no-silent-drop contract is
/// honoured regardless.
pub struct ReceivedBatch {
    /// Passing messages, each carrying its own commit token.
    pub messages: Vec<KafkaMessage>,
    /// Inbound-filter DLQ entries carried forward from the transport.
    pub dlq_entries: Vec<FilteredDlqEntry>,
}

/// The transport records arrive on.
pub enum SourceTransport {
    /// A broker holds records between the previous stage and this one.
    Bus(TransportAdapter),
    /// The scalo Push listener: the previous stage sends point to point.
    Direct(PushTransportAdapter),
}

impl SourceTransport {
    /// Build the transport the config names.
    ///
    /// `governor` attaches the self-regulation pause-partitions brake to the
    /// Kafka consumer. A push source has no broker-side brake -- it sheds
    /// through the sender's own backpressure -- so the direct form ignores it.
    ///
    /// # Errors
    /// Returns an error when the consumer cannot connect or the listener
    /// cannot bind.
    pub async fn from_config(
        config: &Config,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        if config.is_direct() {
            Ok(Self::Direct(PushTransportAdapter::new(&config.grpc).await?))
        } else {
            Ok(Self::Bus(
                TransportAdapter::new(&config.kafka, governor).await?,
            ))
        }
    }

    /// Receive a batch.
    ///
    /// # Errors
    /// Returns an error when the underlying receive fails.
    pub async fn recv(&self, max_messages: usize) -> Result<ReceivedBatch> {
        match self {
            Self::Bus(a) => a.recv(max_messages).await,
            Self::Direct(a) => a.recv(max_messages).await,
        }
    }

    /// Release processed records. A no-op on the direct form, where the Push
    /// RPC response is the acknowledgement.
    ///
    /// # Errors
    /// Returns an error when the broker commit fails.
    pub async fn commit(&self, offsets: Vec<KafkaOffset>) -> Result<()> {
        match self {
            Self::Bus(a) => a.commit(offsets).await,
            Self::Direct(a) => a.commit(offsets).await,
        }
    }

    /// Whether the transport is serving.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        match self {
            Self::Bus(a) => a.is_healthy(),
            Self::Direct(a) => a.is_healthy(),
        }
    }

    /// Per-pod assigned-partition consumer lag, or `None` on the direct form:
    /// a push source keeps no backlog this pod can read, so the KEDA composite
    /// falls back to its other components rather than reading a false zero.
    #[must_use]
    pub fn assigned_lag(&self) -> Option<i64> {
        match self {
            Self::Bus(a) => Some(a.assigned_lag()),
            Self::Direct(_) => None,
        }
    }

    /// Stop the transport.
    ///
    /// # Errors
    /// Returns an error when the underlying close fails.
    pub async fn close(&self) -> Result<()> {
        match self {
            Self::Bus(a) => a.close().await,
            Self::Direct(a) => a.close().await,
        }
    }
}
