// Project:   dfe-archiver
// File:      crates/core/src/types.rs
// Purpose:   Shared message types for Kafka messages and offsets
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use compact_str::CompactString;
use hyperi_rustlib::transport::KafkaToken;
use std::sync::Arc;

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
    /// Create a new message (used by transport adapters)
    pub fn new(
        key: Option<Arc<str>>,
        payload: Vec<u8>,
        topic: CompactString,
        partition: i32,
        offset: i64,
        timestamp_ms: Option<i64>,
        token: KafkaToken,
    ) -> Self {
        Self {
            key,
            payload,
            topic,
            partition,
            offset,
            timestamp_ms,
            token,
        }
    }

    /// Get the commit token (for transport layer)
    pub fn token(&self) -> &KafkaToken {
        &self.token
    }

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

impl KafkaOffset {
    /// Get the commit token (for transport layer)
    pub fn token(&self) -> &KafkaToken {
        &self.token
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_message_for_test() {
        let msg = KafkaMessage::for_test(b"hello".to_vec(), "test-topic", 0, 42);
        assert_eq!(msg.topic.as_str(), "test-topic");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.offset, 42);
        assert_eq!(msg.payload, b"hello");
        assert!(msg.key.is_none());
        assert!(msg.timestamp_ms.is_none());
    }

    #[test]
    fn test_kafka_offset_from_message() {
        let msg = KafkaMessage::for_test(b"data".to_vec(), "events", 3, 100);
        let offset = KafkaOffset::from(&msg);

        assert_eq!(offset.topic.as_str(), "events");
        assert_eq!(offset.partition, 3);
        assert_eq!(offset.offset, 100);
    }

    #[test]
    fn test_kafka_message_clone() {
        let msg = KafkaMessage::for_test(b"payload".to_vec(), "t", 1, 5);
        let cloned = msg.clone();
        assert_eq!(cloned.topic, msg.topic);
        assert_eq!(cloned.offset, msg.offset);
        assert_eq!(cloned.payload, msg.payload);
    }
}
