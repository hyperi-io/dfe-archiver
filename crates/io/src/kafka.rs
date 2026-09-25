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
use rdkafka::consumer::Consumer;
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

/// Sidecar consumer that collects rdkafka statistics via `StatsContext`
/// and emits them as Prometheus metrics.
///
/// Creates a lightweight `BaseConsumer` with `statistics.interval.ms=5000`
/// in a separate consumer group (`{group_id}-stats`). A background tokio
/// task polls the consumer every 5s (triggering stats callbacks) and calls
/// `emit_prometheus_metrics()` to push rdkafka internal stats to the global
/// Prometheus recorder.
///
/// Emitted metrics (per DFE metrics standard, `rdkafka_` prefix):
/// - `rdkafka_global_msg_cnt` / `rdkafka_global_msg_size_bytes`
/// - `rdkafka_broker_rtt_avg_seconds{broker}` / `rdkafka_broker_outbuf_cnt{broker}`
/// - `rdkafka_topic_partition_consumer_lag{topic,partition}`
/// - `rdkafka_topic_partition_committed_offset{topic,partition}`
/// - `rdkafka_consumer_rebalance_count`
pub struct KafkaStatsEmitter {
    consumer:
        std::sync::Arc<rdkafka::consumer::BaseConsumer<scalo::transport::kafka::StatsContext>>,
    _task: tokio::task::JoinHandle<()>,
}

impl KafkaStatsEmitter {
    /// Create a stats emitter for the given Kafka config.
    ///
    /// # Errors
    /// Returns error if the sidecar consumer cannot be created.
    pub fn new(config: &KafkaConfig) -> Result<Self> {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::Consumer;

        let stats_ctx = scalo::transport::kafka::StatsContext::new();

        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", config.brokers.join(","));
        // Separate group so this consumer doesn't steal partitions
        client_config.set("group.id", format!("{}-stats", config.group_id));
        client_config.set("security.protocol", &config.security_protocol);
        client_config.set("statistics.interval.ms", "5000");
        client_config.set("enable.auto.commit", "false");

        if let Some(ref mechanism) = config.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }
        if let Some(ref user) = config.sasl_username {
            client_config.set("sasl.username", user);
        }
        if let Some(ref pass) = config.sasl_password {
            client_config.set("sasl.password", pass.expose());
        }
        // Same private-CA trust as the consumer transport. Without it a broker
        // on a private CA fails this sidecar's TLS handshake and every
        // `rdkafka_*` metric disappears behind one non-fatal warn line.
        if let Some(ref ca) = config.ssl_ca_location {
            client_config.set("ssl.ca.location", ca);
        }

        let consumer: rdkafka::consumer::BaseConsumer<scalo::transport::kafka::StatsContext> =
            client_config
                .create_with_context(stats_ctx)
                .map_err(|e| Error::transport_with("stats consumer creation failed", e))?;

        // Subscribe to same topics so we get partition-level lag stats. An
        // empty list is auto-discovery, which this sidecar does not run: it
        // still reports the global and per-broker stats, without per-partition
        // lag. `position_lag()` reads the main consumer, so the KEDA signal is
        // unaffected either way.
        if config.topics.is_empty() {
            info!("Kafka topics are discovered, so the stats sidecar reports no per-partition lag");
        } else {
            let topic_refs: Vec<&str> = config.topics.iter().map(String::as_str).collect();
            consumer
                .subscribe(&topic_refs)
                .map_err(|e| Error::transport_with("stats subscribe failed", e))?;
        }

        let consumer = std::sync::Arc::new(consumer);

        // Background task: poll consumer (triggers stats callbacks) then emit metrics
        let consumer_bg = std::sync::Arc::clone(&consumer);
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                // poll triggers internal librdkafka callbacks including stats
                let _ = consumer_bg.poll(std::time::Duration::from_millis(0));
                consumer_bg.context().emit_prometheus_metrics();
            }
        });

        info!("Kafka stats emitter started (statistics.interval.ms=5000)");

        Ok(Self {
            consumer,
            _task: task,
        })
    }

    /// Get the current metrics snapshot.
    #[must_use]
    pub fn get_metrics(&self) -> scalo::transport::kafka::KafkaMetrics {
        self.consumer.context().get_metrics()
    }

    /// Get total consumer lag across all partitions.
    #[must_use]
    pub fn total_lag(&self) -> i64 {
        scalo::transport::kafka::total_consumer_lag(&self.consumer.context().get_metrics())
    }
}

/// Convert local config to the scalo transport config.
///
/// Public because the DLQ producer rides the SAME conversion as the consumer
/// transport -- dead-letters must land on the broker the data came from.
pub fn convert_config(config: &KafkaConfig) -> scalo::transport::KafkaConfig {
    // Force librdkafka statistics on so `position_lag()` populates -- the
    // unified ScalingPressure's Kafka inbound term and the
    // `dfe_archiver_kafka_lag` gauge both read it. scalo
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
