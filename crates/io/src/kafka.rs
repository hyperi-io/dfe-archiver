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
use scalo::transport::{KafkaToken, KafkaTransport, TransportBase, TransportReceiver};
use tracing::{debug, info, trace};

use crate::transport::ReceivedBatch;

/// Transport adapter wrapping the scalo Kafka transport
pub struct TransportAdapter {
    transport: KafkaTransport,
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
    /// # Errors
    /// Returns error if connection fails
    pub async fn new(
        config: &KafkaConfig,
        governor: Option<&SelfRegulationGovernor>,
    ) -> Result<Self> {
        info!(
            brokers = %config.brokers.join(","),
            group_id = %config.group_id,
            topics = ?config.topics,
            governed = governor.is_some(),
            "Creating Kafka transport via scalo"
        );

        let hs_config = convert_config(config);
        let transport = KafkaTransport::new(&hs_config)
            .await
            .map_err(|e| Error::transport_with("transport creation failed", e))?;

        // Attach the self-regulation pause-partitions gate over the runtime's
        // shared pressure (the gate is evaluated automatically inside `recv`).
        let transport = match governor {
            Some(gov) => gov.attach_kafka_gate(transport),
            None => transport,
        };

        Ok(Self { transport })
    }

    /// Receive a batch from Kafka as a `WorkBatch`, reshaped into the archiver's
    /// per-message `KafkaMessage` model.
    ///
    /// The transport yields a `WorkBatch<KafkaToken>` whose `records` and
    /// `commit_tokens` are 1:1 and in the same order (one Kafka record produces
    /// one record + one commit token), so they are zipped back into individual
    /// `KafkaMessage`s -- preserving the per-message offset tracking the tiered
    /// buffer relies on. Inbound-filter DLQ entries are surfaced for the caller
    /// to route onward.
    ///
    /// # Errors
    /// Returns error if receive fails
    pub async fn recv(&self, max_messages: usize) -> Result<ReceivedBatch> {
        let batch = self
            .transport
            .recv(max_messages)
            .await
            .map_err(|e| Error::transport_with("recv failed", e))?;

        debug!(
            count = batch.records.len(),
            dlq = batch.dlq_entries.len(),
            "Received batch from Kafka"
        );

        // `records[i]` corresponds to `commit_tokens[i]` (the transport builds
        // both in the same order from each Kafka record). Zip them back into the
        // per-message model the buffer uses.
        let messages: Vec<KafkaMessage> = batch
            .records
            .into_iter()
            .zip(batch.commit_tokens)
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

        Ok(ReceivedBatch {
            messages,
            dlq_entries: batch.dlq_entries,
        })
    }

    /// Commit offsets for processed messages
    ///
    /// IMPORTANT: Only call this AFTER data is confirmed written to storage.
    /// This is the "release" point for at-least-once delivery.
    ///
    /// # Errors
    /// Returns error if commit fails
    pub async fn commit(&self, offsets: Vec<KafkaOffset>) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        let count = offsets.len();
        let tokens: Vec<KafkaToken> = offsets.into_iter().map(KafkaOffset::into_token).collect();

        self.transport
            .commit(&tokens)
            .await
            .map_err(|e| Error::transport_with("commit failed", e))?;

        debug!(count, "Committed offsets to Kafka");
        Ok(())
    }

    /// Total consumer lag summed over THIS pod's ASSIGNED partitions.
    ///
    /// rdkafka reports `consumer_lag` only for assigned partitions, so the sum
    /// is inherently PER-POD and scale-invariant: as the consumer group grows,
    /// each pod's assigned lag falls. The archiver feeds it as the `kafka_lag`
    /// component of the unified `ScalingPressure` engine (the Kafka inbound
    /// pressure term KEDA scales on).
    ///
    /// Requires librdkafka statistics on the consumer (a non-zero
    /// `statistics.interval.ms`); scalo's `KafkaTransport` defaults it to
    /// 5000ms, so the stats snapshot populates without extra config. With stats
    /// disabled the snapshot is empty and this returns 0 (the Kafka term then
    /// contributes 0).
    #[must_use]
    pub fn assigned_lag(&self) -> i64 {
        scalo::transport::kafka::total_consumer_lag(&self.transport.stats()).max(0)
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
        // lag. `assigned_lag()` reads the main consumer, so the KEDA signal is
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
    // Force librdkafka statistics on so `transport.stats()` (and hence
    // `assigned_lag()`) populates -- the unified ScalingPressure's Kafka
    // inbound term and the `dfe_archiver_kafka_lag` gauge both read it. scalo
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
