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
use scalo::transport::ack::{DeliveryGuarantee, GuaranteeReason};
use scalo::transport::filter::FilteredDlqEntry;
use scalo::transport::{DeliveryStatus, KafkaToken, SinkConfirmation};

use crate::grpc::PushTransportAdapter;
use crate::kafka::TransportAdapter;

/// A received block: the passing messages plus any inbound-filter DLQ entries.
///
/// The DLQ entries are surfaced (never silently dropped) so the orchestrator
/// can route them onward. The archiver configures no inbound scalo filters, so
/// `dlq_entries` and `filtered` are empty in practice -- but the no-silent-drop
/// contract is honoured regardless.
pub struct ReceivedBatch {
    /// Passing messages, each carrying its own commit token.
    pub messages: Vec<KafkaMessage>,
    /// Offsets of records an inbound filter removed, still to be released.
    pub filtered: Vec<KafkaOffset>,
    /// Inbound-filter DLQ entries carried forward from the transport.
    pub dlq_entries: Vec<FilteredDlqEntry>,
}

impl ReceivedBatch {
    /// Whether the block carries nothing to process, dead-letter or release.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.filtered.is_empty() && self.dlq_entries.is_empty()
    }
}

/// The transport records arrive on.
// One transport lives for the whole process, so the larger variant's size is paid once.
#[allow(clippy::large_enum_variant)]
pub enum SourceTransport {
    /// A broker holds records between the previous stage and this one.
    Bus(TransportAdapter),
    /// The scalo Push listener: the previous stage sends point to point.
    Direct(PushTransportAdapter),
}

impl SourceTransport {
    /// Build the transport the config names.
    ///
    /// `governor` is the self-regulation brake: it pauses the Kafka consumer's
    /// partitions, and makes the Push listener shed pushes with `Unavailable`.
    ///
    /// # Errors
    /// Returns an error when the consumer cannot connect or the listener
    /// cannot bind.
    pub async fn from_config(
        config: &Config,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        if config.is_direct() {
            Ok(Self::Direct(
                PushTransportAdapter::new(&config.grpc, governor).await?,
            ))
        } else {
            Ok(Self::Bus(
                TransportAdapter::new(&config.kafka, governor).await?,
            ))
        }
    }

    /// Receive a batch.
    ///
    /// # Errors
    /// [`dfe_archiver_core::Error::Shutdown`] once the transport is closed and
    /// has nothing left to return, or an error when the receive fails otherwise.
    pub async fn recv(&self, max_messages: usize) -> Result<ReceivedBatch> {
        match self {
            Self::Bus(a) => a.recv(max_messages).await,
            Self::Direct(a) => a.recv(max_messages).await,
        }
    }

    /// Release the records `tokens` names with `status`.
    ///
    /// On the bus this is the offset commit, up to each partition's lowest
    /// offset not yet released. A no-op on the direct form, whose listener
    /// answered each push at enqueue.
    ///
    /// # Errors
    /// Returns an error when the broker commit fails.
    pub async fn release(&self, tokens: &[KafkaToken], status: DeliveryStatus) -> Result<()> {
        match self {
            Self::Bus(a) => a.release(tokens, status).await,
            Self::Direct(_) => Ok(()),
        }
    }

    /// Whether a release reaches the source, so the caller has to hold each
    /// record's offset until the record is written. False on the direct form
    /// and on a Kafka consumer that commits at receipt.
    #[must_use]
    pub fn holds_offsets(&self) -> bool {
        match self {
            Self::Bus(a) => a.holds_offsets(),
            Self::Direct(_) => false,
        }
    }

    /// Records handed out and not yet released: zero unless offsets are held.
    #[must_use]
    pub fn held_records(&self) -> u64 {
        match self {
            Self::Bus(a) => a.held_records(),
            Self::Direct(_) => 0,
        }
    }

    /// The delivery guarantee of the pipeline from this source into a sink
    /// that confirms as `sink` does.
    #[must_use]
    pub fn guarantee(&self, sink: SinkConfirmation) -> (DeliveryGuarantee, GuaranteeReason) {
        match self {
            Self::Bus(a) => a.guarantee(sink),
            Self::Direct(a) => a.guarantee(),
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

    /// Records past this pod's read position, or `None` on the direct form: a
    /// push source keeps no backlog this pod can read, so the KEDA composite
    /// falls back to its other components rather than reading a false zero.
    #[must_use]
    pub fn position_lag(&self) -> Option<i64> {
        match self {
            Self::Bus(a) => Some(a.position_lag()),
            Self::Direct(_) => None,
        }
    }

    /// Stop the transport. The Push listener stops accepting and keeps what it
    /// already answered for [`recv`](Self::recv). The Kafka consumer stops
    /// fetching and can still commit.
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
