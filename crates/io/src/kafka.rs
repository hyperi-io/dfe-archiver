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
use hyperi_rustlib::transport::{KafkaToken, KafkaTransport, Transport};
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
