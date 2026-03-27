// Project:   dfe-archiver
// File:      crates/io/src/kafka.rs
// Purpose:   Kafka transport adapter wrapping hyperi-rustlib
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use compact_str::CompactString;
use dfe_archiver_core::config::KafkaConfig;
use dfe_archiver_core::types::{KafkaMessage, KafkaOffset};
use dfe_archiver_core::{Error, Result};
use hyperi_rustlib::transport::{KafkaToken, KafkaTransport, TransportBase, TransportReceiver};
use rdkafka::consumer::Consumer;
use tracing::{debug, info};

/// Transport adapter wrapping hyperi-rustlib Kafka transport
pub struct TransportAdapter {
    transport: KafkaTransport,
}

impl TransportAdapter {
    /// Create new transport adapter
    ///
    /// # Errors
    /// Returns error if connection fails
    pub async fn new(config: &KafkaConfig) -> Result<Self> {
        info!(
            brokers = %config.brokers.join(","),
            group_id = %config.group_id,
            topics = ?config.topics,
            "Creating Kafka transport via hyperi-rustlib"
        );

        let hs_config = convert_config(config);
        let transport = KafkaTransport::new(&hs_config)
            .await
            .map_err(|e| Error::Kafka(format!("transport creation failed: {e}")))?;

        Ok(Self { transport })
    }

    /// Receive batch of messages
    ///
    /// # Errors
    /// Returns error if receive fails
    pub async fn recv(&self, max_messages: usize) -> Result<Vec<KafkaMessage>> {
        let messages = self
            .transport
            .recv(max_messages)
            .await
            .map_err(|e| Error::Kafka(format!("recv failed: {e}")))?;

        debug!(count = messages.len(), "Received messages from Kafka");

        Ok(messages
            .into_iter()
            .map(|msg| {
                let topic = CompactString::from(msg.token.topic.as_ref());
                let partition = msg.token.partition;
                let offset = msg.token.offset;
                KafkaMessage::new(
                    msg.key.clone(),
                    msg.payload.clone(),
                    topic,
                    partition,
                    offset,
                    msg.timestamp_ms,
                    msg.token,
                )
            })
            .collect())
    }

    /// Commit offsets for processed messages
    ///
    /// IMPORTANT: Only call this AFTER data is confirmed written to storage.
    /// This is the "release" point for at-least-once delivery.
    ///
    /// # Errors
    /// Returns error if commit fails
    pub async fn commit(&self, offsets: &[KafkaOffset]) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        let tokens: Vec<KafkaToken> = offsets.iter().map(|o| o.token().clone()).collect();

        self.transport
            .commit(&tokens)
            .await
            .map_err(|e| Error::Kafka(format!("commit failed: {e}")))?;

        debug!(count = offsets.len(), "Committed offsets to Kafka");
        Ok(())
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
            .map_err(|e| Error::Kafka(format!("close failed: {e}")))?;
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
    consumer: std::sync::Arc<
        rdkafka::consumer::BaseConsumer<hyperi_rustlib::transport::kafka::StatsContext>,
    >,
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

        let stats_ctx = hyperi_rustlib::transport::kafka::StatsContext::new();

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

        let consumer: rdkafka::consumer::BaseConsumer<
            hyperi_rustlib::transport::kafka::StatsContext,
        > = client_config
            .create_with_context(stats_ctx)
            .map_err(|e| Error::Kafka(format!("stats consumer creation failed: {e}")))?;

        // Subscribe to same topics so we get partition-level lag stats
        let topic_refs: Vec<&str> = config.topics.iter().map(String::as_str).collect();
        consumer
            .subscribe(&topic_refs)
            .map_err(|e| Error::Kafka(format!("stats subscribe failed: {e}")))?;

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
    pub fn get_metrics(&self) -> hyperi_rustlib::transport::kafka::KafkaMetrics {
        self.consumer.context().get_metrics()
    }

    /// Get total consumer lag across all partitions.
    #[must_use]
    pub fn total_lag(&self) -> i64 {
        hyperi_rustlib::transport::kafka::total_consumer_lag(&self.consumer.context().get_metrics())
    }
}

/// Convert local config to hyperi-rustlib transport config
fn convert_config(config: &KafkaConfig) -> hyperi_rustlib::transport::KafkaConfig {
    hyperi_rustlib::transport::KafkaConfig {
        brokers: config.brokers.clone(),
        group: config.group_id.clone(),
        topics: config.topics.clone(),
        sasl_mechanism: config.sasl_mechanism.clone(),
        sasl_username: config.sasl_username.clone(),
        sasl_password: config
            .sasl_password
            .as_ref()
            .map(|s| s.expose().to_string()),
        security_protocol: config.security_protocol.clone(),
        session_timeout_ms: config.session_timeout_ms,
        max_poll_interval_ms: config.max_poll_interval_ms,
        // Use manual commit for at-least-once delivery
        enable_auto_commit: false,
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
    pub const fn commit(&self, _offsets: &[KafkaOffset]) {
        // No-op
    }
}

#[cfg(any(test, feature = "transport-memory"))]
impl Default for MemoryTransportAdapter {
    fn default() -> Self {
        Self::new()
    }
}
