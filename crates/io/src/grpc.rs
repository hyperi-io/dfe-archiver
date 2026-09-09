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
use dfe_archiver_core::types::{KafkaMessage, KafkaOffset};
use dfe_archiver_core::{Error, Result};
use scalo::transport::{
    GrpcConfig as TransportGrpcConfig, GrpcTransport, KafkaToken, TransportBase, TransportReceiver,
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
}

impl PushTransportAdapter {
    /// Bind the Push listener.
    ///
    /// # Errors
    /// Returns an error when `config.listen` is unset or the bind fails.
    pub async fn new(config: &GrpcConfig) -> Result<Self> {
        let listen = config
            .listen
            .as_ref()
            .ok_or_else(|| Error::Config("grpc.listen is required on transport: grpc".into()))?;
        info!(listen = %listen, "Binding the scalo Push listener");

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

        let transport = GrpcTransport::new(&transport_config)
            .await
            .map_err(|e| Error::transport_with("Push listener bind failed", e))?;

        Ok(Self {
            transport,
            default_topic: Arc::from(config.default_topic.as_str()),
        })
    }

    /// Receive a batch of pushed records.
    ///
    /// The sender names the destination through the record's routing key; the
    /// commit-token sequence stands in for the Kafka offset, so the buffer's
    /// per-message accounting is unchanged.
    ///
    /// # Errors
    /// Returns an error when the transport receive fails.
    pub async fn recv(&self, max_messages: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max_messages)
            .await
            .map_err(|e| Error::transport_with("Push recv failed", e))?;

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
            dlq_entries: batch.dlq_entries,
        })
    }

    /// Commit is the Push RPC response itself, so there is nothing to do here.
    // async is deliberate: the enum awaits every arm's commit uniformly, and the
    // Kafka arm genuinely awaits.
    #[allow(clippy::unused_async, clippy::unused_async_trait_impl)]
    pub async fn commit(&self, _offsets: Vec<KafkaOffset>) -> Result<()> {
        Ok(())
    }

    /// Whether the listener is serving.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.transport.is_healthy()
    }

    /// Stop serving.
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
