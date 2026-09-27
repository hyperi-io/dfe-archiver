// Project:   dfe-archiver
// File:      crates/io/src/kafka.rs
// Purpose:   Kafka transport adapter wrapping scalo
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use compact_str::CompactString;
use dfe_archiver_core::config::KafkaConfig;
use dfe_archiver_core::types::{KafkaMessage, KafkaOffset};
use dfe_archiver_core::{Error, Result};
use scalo::SelfRegulationGovernor;
use scalo::transport::ack::{DeliveryGuarantee, EffectiveGuarantee, GuaranteeReason};
use scalo::transport::{
    DeliveryStatus, KafkaToken, KafkaTransport, SinkConfirmation, TransportBase, TransportError,
    TransportReceiver,
};
use tracing::{debug, info, trace, warn};

use crate::transport::ReceivedBatch;

/// Transport adapter wrapping the scalo Kafka transport
pub struct TransportAdapter {
    transport: KafkaTransport,
    /// `kafka.acknowledgements.enabled`: every offset handed out waits for its
    /// release, rather than being committed at receipt.
    held: bool,
}

impl TransportAdapter {
    /// Create a new transport adapter.
    ///
    /// When `governor` is `Some`, the self-regulation inbound brake is attached
    /// to the Kafka receiver: under memory pressure the consumer's ASSIGNED
    /// partitions are paused (the member stays in the group -- no rebalance) and
    /// resumed once pressure clears. This is the pause-partitions gate for a
    /// Kafka-source stage; nothing on the outbound archive drain is gated. When
    /// `governor` is `None` (self-regulation disabled), construction is
    /// byte-identical to before.
    ///
    /// With `acknowledgements.enabled` the transport is armed before its first
    /// receive: it then tracks every offset it hands out and commits each
    /// partition only up to its lowest offset not yet released.
    ///
    /// # Errors
    /// Returns error if connection fails
    pub async fn new(
        config: &KafkaConfig,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        let held = config.acknowledgements.enabled;
        info!(
            brokers = %config.brokers.join(","),
            group_id = %config.group_id,
            topics = ?config.topics,
            governed = governor.is_some(),
            acknowledgements = held,
            "Creating Kafka transport via scalo"
        );

        let hs_config = convert_config(config);
        let transport = KafkaTransport::new(&hs_config)
            .await
            .map_err(|e| Error::transport_with("transport creation failed", e))?
            .with_acknowledgements(config.acknowledgements);

        // Attach the self-regulation pause-partitions gate over the runtime's
        // shared pressure (the gate is evaluated automatically inside `recv`).
        let transport = match governor {
            Some(gov) => gov.attach_kafka_gate(transport),
            None => transport,
        };

        if held && let Some(control) = transport.ack_control() {
            control.arm();
        }

        Ok(Self { transport, held })
    }

    /// Receive a batch from Kafka as a `WorkBatch`, reshaped into the archiver's
    /// per-message `KafkaMessage` model.
    ///
    /// The transport yields a `WorkBatch<KafkaToken>` whose first
    /// `records.len()` commit tokens pair 1:1 with the records, in order, and
    /// whose remaining tokens belong to records an inbound filter removed.
    /// The pairs are zipped back into individual `KafkaMessage`s -- preserving
    /// the per-message offset tracking the tiered buffer relies on -- and the
    /// filtered tokens come back in `filtered`, so a held transport can release
    /// them. Inbound-filter DLQ entries are surfaced for the caller to route
    /// onward.
    ///
    /// With acknowledgements off, the whole batch is committed here, before the
    /// caller sees it.
    ///
    /// # Errors
    /// [`Error::Shutdown`] once the transport is closed, or a transport error
    /// when the receive fails otherwise.
    pub async fn recv(&self, max_messages: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max_messages)
            .await
            .map_err(|e| match e {
                TransportError::Closed => Error::Shutdown,
                other => Error::transport_with("recv failed", other),
            })?;

        debug!(
            count = batch.records.len(),
            dlq = batch.dlq_entries.len(),
            "Received batch from Kafka"
        );

        if !self.held
            && !batch.commit_tokens.is_empty()
            && let Err(e) = self
                .transport
                .release(&batch.commit_tokens, DeliveryStatus::Delivered)
                .await
        {
            // The records are in hand, and a missed commit only re-reads them after a restart.
            warn!(error = %e, "Commit at receipt failed");
        }

        let mut tokens = batch.commit_tokens.into_iter();
        let messages: Vec<KafkaMessage> = batch
            .records
            .into_iter()
            .zip(tokens.by_ref())
            .map(|(record, token)| {
                let topic = CompactString::from(token.topic.as_ref());
                let partition = token.partition;
                let offset = token.offset;
                trace!(
                    topic = %topic,
                    partition,
                    offset,
                    payload_bytes = record.payload.len(),
                    "Received Kafka message"
                );
                KafkaMessage::new(
                    record.key,
                    record.payload.to_vec(),
                    topic,
                    partition,
                    offset,
                    record.metadata.timestamp_ms,
                    token,
                )
            })
            .collect();
        let filtered = if self.held {
            tokens.map(KafkaOffset::new).collect()
        } else {
            Vec::new()
        };

        Ok(ReceivedBatch {
            messages,
            filtered,
            dlq_entries: batch.dlq_entries,
        })
    }

    /// Release the records `tokens` names with `status`.
    ///
    /// Held, a release commits each partition up to its lowest offset not yet
    /// released, and an `Errored` release keeps its offsets below every later
    /// commit until the process restarts. With acknowledgements off the batch
    /// was committed at receipt, so this does nothing.
    ///
    /// # Errors
    /// Returns error if the commit the release allows fails.
    pub async fn release(&self, tokens: &[KafkaToken], status: DeliveryStatus) -> Result<()> {
        if !self.held || tokens.is_empty() {
            return Ok(());
        }
        self.transport
            .release(tokens, status)
            .await
            .map_err(|e| Error::transport_with("release failed", e))?;
        debug!(count = tokens.len(), ?status, "Released Kafka offsets");
        Ok(())
    }

    /// Whether released offsets reach the commit, so the caller has to hold
    /// each record's offset until the record is written.
    #[must_use]
    pub fn holds_offsets(&self) -> bool {
        self.held
    }

    /// Records handed out and not yet released.
    #[must_use]
    pub fn held_records(&self) -> u64 {
        if !self.held {
            return 0;
        }
        self.transport
            .ack_control()
            .map_or(0, |control| control.held().count)
    }

    /// Records past this pod's read position, summed over its partitions.
    ///
    /// The committed-offset lag grows by up to a roll interval of intake while
    /// open files hold their commit, so the `kafka_lag` scaling term reads
    /// this instead: it counts unread backlog whatever the commit policy.
    /// Requires librdkafka statistics on the consumer, which `convert_config`
    /// forces on.
    #[must_use]
    pub fn position_lag(&self) -> i64 {
        self.transport.total_position_lag().max(0)
    }

    /// The delivery guarantee this source gives into a sink that confirms as
    /// `sink` does.
    #[must_use]
    pub fn guarantee(&self, sink: SinkConfirmation) -> (DeliveryGuarantee, GuaranteeReason) {
        let effective = EffectiveGuarantee::of(self.transport.ack_control(), sink);
        (effective.guarantee, effective.reason)
    }

    /// Check if transport is healthy
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.transport.is_healthy()
    }

    /// Close transport connection
    pub async fn close(&self) -> Result<()> {
        info!("Closing Kafka transport");
        self.transport
            .close()
            .await
            .map_err(|e| Error::transport_with("close failed", e))?;
        Ok(())
    }
}

/// Convert local config to the scalo transport config.
///
/// Public because the DLQ producer rides the SAME conversion as the consumer
/// transport -- dead-letters must land on the broker the data came from.
pub fn convert_config(config: &KafkaConfig) -> scalo::transport::KafkaConfig {
    // Force librdkafka statistics on so `position_lag()` populates -- the
    // unified ScalingPressure's Kafka inbound term and the
    // `dfe_archiver_kafka_lag` gauge both read it, and the same statistics
    // callback publishes the `rdkafka_*` gauges. scalo
    // already defaults this to 5000ms when unset, but we set it EXPLICITLY (as
    // a highest-priority override) so a future profile/default change can never
    // silently flip it to 0 and zero the lag signal.
    let mut librdkafka_overrides = std::collections::HashMap::new();
    librdkafka_overrides.insert("statistics.interval.ms".to_string(), "5000".to_string());

    scalo::transport::KafkaConfig {
        // An empty topic list discovers instead, so a source added after this
        // pod started is archived without a restart; scalo's refresh loop
        // subscribes when the first matching topic appears.
        auto_discover: config.topics.is_empty(),
        topic_include: config.topic_include.clone(),
        topic_exclude: config.topic_exclude.clone(),
        topic_refresh_secs: config.topic_refresh_secs,
        // The archiver keeps the record as it arrived, so a source with both
        // topics is archived from the landing one -- the reverse of the
        // loader's rule.
        topic_suppression_rules: vec![scalo::transport::kafka::SuppressionRule {
            preferred_suffix: "_land".to_string(),
            suppressed_suffix: "_load".to_string(),
        }],
        // Archiver is consume-only (Kafka -> storage). scalo 2.9 KafkaConfig is
        // profile-based (no `role` field): a non-empty `group` + subscribed
        // `topics` make this a consumer; no produce calls means no idle producer
        // (#44).
        brokers: config.brokers.clone(),
        group: config.group_id.clone(),
        topics: config.topics.clone(),
        sasl_mechanism: config.sasl_mechanism.clone(),
        sasl_username: config.sasl_username.clone(),
        sasl_password: config.sasl_password.clone(),
        security_protocol: config.security_protocol.clone(),
        ssl_ca_location: config.ssl_ca_location.clone(),
        allow_insecure_transport: config.allow_insecure_transport,
        session_timeout_ms: config.session_timeout_ms,
        max_poll_interval_ms: config.max_poll_interval_ms,
        // Use manual commit for at-least-once delivery
        enable_auto_commit: false,
        librdkafka_overrides,
        ..Default::default()
    }
}

#[cfg(any(test, feature = "transport-memory"))]
use std::sync::Arc;

/// Memory transport adapter for testing (no external services required)
#[cfg(any(test, feature = "transport-memory"))]
pub struct MemoryTransportAdapter {
    messages: std::sync::Mutex<Vec<KafkaMessage>>,
    next_offset: std::sync::atomic::AtomicI64,
}

#[cfg(any(test, feature = "transport-memory"))]
#[allow(clippy::expect_used)]
impl MemoryTransportAdapter {
    /// Create new memory transport
    #[must_use]
    pub const fn new() -> Self {
        Self {
            messages: std::sync::Mutex::new(Vec::new()),
            next_offset: std::sync::atomic::AtomicI64::new(0),
        }
    }

    /// Inject messages for testing
    pub fn inject(&self, payloads: Vec<Vec<u8>>, topic: &str) {
        let mut guard = self.messages.lock().expect("lock poisoned");
        for payload in payloads {
            let offset = self
                .next_offset
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            guard.push(KafkaMessage::new(
                None,
                payload,
                CompactString::from(topic),
                0,
                offset,
                None,
                KafkaToken::new(Arc::from(topic), 0, offset),
            ));
        }
    }

    /// Receive messages (drains injected messages)
    pub fn recv(&self, max_messages: usize) -> Vec<KafkaMessage> {
        let mut guard = self.messages.lock().expect("lock poisoned");
        let count = max_messages.min(guard.len());
        guard.drain(..count).collect()
    }

    /// Commit is a no-op for memory transport
    pub fn commit(&self, _offsets: Vec<KafkaOffset>) {
        // No-op
    }
}

#[cfg(any(test, feature = "transport-memory"))]
impl Default for MemoryTransportAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::ClientContext;
    use rdkafka::statistics::{Partition, Statistics, Topic};
    use scalo::transport::kafka::{StatsContext, total_consumer_lag};

    /// The lag behind `position_lag` counts records past the read position, so
    /// the records an open file holds uncommitted are never read as backlog.
    #[test]
    fn position_lag_counts_unread_records_never_held_ones() {
        // Committed at 10, read to 60, 100 on the broker: 50 held, 40 unread.
        let partition = Partition {
            partition: 0,
            committed_offset: 10,
            app_offset: 60,
            hi_offset: 100,
            ls_offset: 100,
            consumer_lag: 90,
            ..Partition::default()
        };
        let mut topic = Topic {
            topic: "events".to_string(),
            ..Topic::default()
        };
        topic.partitions.insert(0, partition);
        let mut stats = Statistics::default();
        stats.topics.insert("events".to_string(), topic);

        let context = StatsContext::new();
        context.stats(stats);

        assert_eq!(context.total_position_lag(), 40);
        assert_eq!(
            total_consumer_lag(&context.get_metrics()),
            90,
            "the committed lag counts the held records as backlog"
        );
    }
}
