// Project:   dfe-archiver
// File:      src/kafka/transport.rs
// Purpose:   Kafka transport adapter wrapping hyperi-rustlib
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::config::KafkaConfig;
use crate::{Error, Result};
use compact_str::CompactString;
use hyperi_rustlib::transport::{KafkaToken, KafkaTransport, Transport};
use std::sync::Arc;
use tracing::{debug, info};

/// Message received from Kafka
#[derive(Debug, Clone)]
pub struct KafkaMessage {
    /// Message key (topic for routing)
    pub key: Option<Arc<str>>,

    /// Message payload (raw bytes)
    pub payload: Vec<u8>,

    /// Topic name
    pub topic: CompactString,

    /// Partition
    pub partition: i32,

    /// Offset
    pub offset: i64,

    /// Timestamp (milliseconds)
    pub timestamp_ms: Option<i64>,

    /// Commit token for at-least-once delivery
    pub(crate) token: KafkaToken,
}

impl KafkaMessage {
    /// Create a test message (for benchmarks and tests)
    #[must_use]
    pub fn for_test(payload: Vec<u8>, topic: &str, partition: i32, offset: i64) -> Self {
        Self {
            key: None,
            payload,
            topic: CompactString::from(topic),
            partition,
            offset,
            timestamp_ms: None,
            token: KafkaToken::new(Arc::from(topic), partition, offset),
        }
    }
}

/// Kafka offset for commit tracking (wraps hyperi-rustlib token)
#[derive(Debug, Clone)]
pub struct KafkaOffset {
    pub topic: CompactString,
    pub partition: i32,
    pub offset: i64,
    pub(crate) token: KafkaToken,
}

impl From<&KafkaMessage> for KafkaOffset {
    fn from(msg: &KafkaMessage) -> Self {
        Self {
            topic: msg.topic.clone(),
            partition: msg.partition,
            offset: msg.offset,
            token: msg.token.clone(),
        }
    }
}

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

        // Convert hyperi-rustlib messages to local type
        Ok(messages
            .into_iter()
            .map(|msg| KafkaMessage {
                key: msg.key.clone(),
                payload: msg.payload.clone(),
                topic: CompactString::from(msg.token.topic.as_ref()),
                partition: msg.token.partition,
                offset: msg.token.offset,
                timestamp_ms: msg.timestamp_ms,
                token: msg.token,
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

        let tokens: Vec<KafkaToken> = offsets.iter().map(|o| o.token.clone()).collect();

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
        sasl_password: config.sasl_password.clone(),
        security_protocol: config.security_protocol.clone(),
        session_timeout_ms: config.session_timeout_ms,
        max_poll_interval_ms: config.max_poll_interval_ms,
        // Use manual commit for at-least-once delivery
        enable_auto_commit: false,
        ..Default::default()
    }
}

/// Memory transport adapter for testing (no external services required)
#[cfg(any(test, feature = "transport-memory"))]
pub struct MemoryTransportAdapter {
    messages: std::sync::Mutex<Vec<KafkaMessage>>,
    next_offset: std::sync::atomic::AtomicI64,
}

#[cfg(any(test, feature = "transport-memory"))]
impl MemoryTransportAdapter {
    /// Create new memory transport
    #[must_use]
    pub fn new() -> Self {
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
            guard.push(KafkaMessage {
                key: None,
                payload,
                topic: CompactString::from(topic),
                partition: 0,
                offset,
                timestamp_ms: None,
                token: KafkaToken::new(Arc::from(topic), 0, offset),
            });
        }
    }

    /// Receive messages (drains injected messages)
    pub fn recv(&self, max_messages: usize) -> Vec<KafkaMessage> {
        let mut guard = self.messages.lock().expect("lock poisoned");
        let count = max_messages.min(guard.len());
        guard.drain(..count).collect()
    }

    /// Commit is a no-op for memory transport
    pub fn commit(&self, _offsets: &[KafkaOffset]) {
        // No-op
    }
}

#[cfg(any(test, feature = "transport-memory"))]
impl Default for MemoryTransportAdapter {
    fn default() -> Self {
        Self::new()
    }
}
