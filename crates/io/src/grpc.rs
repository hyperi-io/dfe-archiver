// Project:   dfe-archiver
// File:      crates/io/src/grpc.rs
// Purpose:   Push-listener transport adapter for the direct transport
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! The receive side of the direct transport.
//!
//! On a deployment with no broker the sender (dfe-receiver or dfe-fetcher) fans
//! a matched record out to the loader AND here, over the same scalo Push RPC.
//! The archiver is the server: it binds the listener and converts each pushed
//! record into the `KafkaMessage` the rest of the pipeline already speaks.

use compact_str::CompactString;
use dfe_archiver_core::config::GrpcConfig;
use dfe_archiver_core::types::KafkaMessage;
use dfe_archiver_core::{Error, Result};
use scalo::SelfRegulationGovernor;
use scalo::transport::ack::{DeliveryGuarantee, GuaranteeReason};
use scalo::transport::{
    AcknowledgementsConfig, GrpcConfig as TransportGrpcConfig, GrpcTransport, KafkaToken,
    TransportBase, TransportError, TransportReceiver,
};
use std::sync::Arc;
use tracing::{debug, info};

use crate::transport::ReceivedBatch;

/// Adapter wrapping the scalo `GrpcTransport` in receive mode.
pub struct PushTransportAdapter {
    transport: GrpcTransport,
    /// Routing key applied when the sender set none. The archiver routes on the
    /// message topic, so a keyless record still lands somewhere named.
    default_topic: Arc<str>,
    /// `grpc.acknowledgements`, reported through the delivery guarantee.
    acknowledgements: AcknowledgementsConfig,
}

impl PushTransportAdapter {
    /// Bind the Push listener.
    ///
    /// `governor` sheds pushes with `Unavailable` while its pressure holds, as
    /// the Kafka consumer pauses its partitions. The listener is never armed:
    /// a record is released only when its archive file completes, at the roll
    /// interval, so a held answer would outlive every sender's deadline and the
    /// sender would resend the same records each time. It answers each push
    /// once its records are queued.
    ///
    /// # Errors
    /// Returns an error when `config.listen` is unset or the bind fails.
    pub async fn new(
        config: &GrpcConfig,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        let listen = config
            .listen
            .as_ref()
            .ok_or_else(|| Error::Config("grpc.listen is required on transport: grpc".into()))?;
        info!(
            listen = %listen,
            governed = governor.is_some(),
            "Binding the scalo Push listener"
        );

        let transport_config = TransportGrpcConfig {
            listen: Some(listen.clone()),
            // Receive only: the archiver is a terminal stage, it sends nothing on.
            endpoint: None,
            recv_buffer_size: config.recv_buffer_size,
            recv_timeout_ms: config.recv_timeout_ms,
            max_message_size: config.max_message_size,
            compression: config.compression,
            ..Default::default()
        };

        let builder =
            GrpcTransport::builder(&transport_config).acknowledgements(config.acknowledgements);
        let builder = match governor {
            Some(governor) => builder.pressure(governor.pressure()),
            None => builder,
        };
        let transport = builder
            .start()
            .await
            .map_err(|e| Error::transport_with("Push listener bind failed", e))?;

        Ok(Self {
            transport,
            default_topic: Arc::from(config.default_topic.as_str()),
            acknowledgements: config.acknowledgements,
        })
    }

    /// Receive a batch of pushed records.
    ///
    /// The sender names the destination through the record's routing key; the
    /// commit-token sequence stands in for the Kafka offset, so the buffer's
    /// per-message accounting is unchanged.
    ///
    /// After [`close`](Self::close) this returns the records already answered,
    /// then [`Error::Shutdown`].
    ///
    /// # Errors
    /// [`Error::Shutdown`] once the listener is closed and drained, or a
    /// transport error when the receive fails otherwise.
    pub async fn recv(&self, max_messages: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max_messages)
            .await
            .map_err(|e| match e {
                TransportError::Closed => Error::Shutdown,
                other => Error::transport_with("Push recv failed", other),
            })?;

        debug!(
            count = batch.records.len(),
            dlq = batch.dlq_entries.len(),
            "Received batch from the Push listener"
        );

        let messages = batch
            .records
            .into_iter()
            .zip(batch.commit_tokens)
            .map(|(record, token)| {
                let topic = record
                    .key
                    .clone()
                    .unwrap_or_else(|| self.default_topic.clone());
                // A push stream's sequence stands in for the broker offset; the
                // buffer only needs it to be monotonic per message.
                let offset = token.seq.cast_signed();
                KafkaMessage::new(
                    record.key,
                    record.payload.to_vec(),
                    CompactString::from(topic.as_ref()),
                    // A push stream has no partitions and no broker offsets.
                    0,
                    offset,
                    record.metadata.timestamp_ms,
                    KafkaToken::new(topic, 0, offset),
                )
            })
            .collect();

        Ok(ReceivedBatch {
            messages,
            // The listener answered at enqueue, so no token waits for a release.
            filtered: Vec::new(),
            dlq_entries: batch.dlq_entries,
        })
    }

    /// The delivery guarantee of the archive copy on this transport.
    ///
    /// Best effort either way: the listener answers at enqueue because the
    /// archive sink cannot confirm a record within a sender's deadline.
    #[must_use]
    pub fn guarantee(&self) -> (DeliveryGuarantee, GuaranteeReason) {
        direct_guarantee(self.acknowledgements)
    }

    /// Whether the listener is serving.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.transport.is_healthy()
    }

    /// Stop accepting pushes. The records already answered stay queued for
    /// [`recv`](Self::recv).
    ///
    /// # Errors
    /// Returns an error when the transport close fails.
    pub async fn close(&self) -> Result<()> {
        info!("Closing the Push listener");
        self.transport
            .close()
            .await
            .map_err(|e| Error::transport_with("Push listener close failed", e))
    }
}

/// The guarantee of the unarmed Push listener: best effort, because the
/// archive sink cannot confirm in time, unless acknowledgements are off.
fn direct_guarantee(
    acknowledgements: AcknowledgementsConfig,
) -> (DeliveryGuarantee, GuaranteeReason) {
    let reason = if acknowledgements.enabled {
        GuaranteeReason::SinkCannotConfirm
    } else {
        GuaranteeReason::AcksDisabled
    };
    (DeliveryGuarantee::BestEffort, reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The direct path answers at enqueue whatever the key says, so it never
    /// reports at-least-once. The key only picks the reason.
    #[test]
    fn the_direct_path_reports_best_effort_with_the_reason_the_key_gives() {
        assert_eq!(
            direct_guarantee(AcknowledgementsConfig::default()),
            (
                DeliveryGuarantee::BestEffort,
                GuaranteeReason::SinkCannotConfirm
            )
        );
        assert_eq!(
            direct_guarantee(AcknowledgementsConfig::new(false)),
            (DeliveryGuarantee::BestEffort, GuaranteeReason::AcksDisabled)
        );
    }
}
