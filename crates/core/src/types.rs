// Project:   dfe-archiver
// File:      crates/core/src/types.rs
// Purpose:   Shared message types for Kafka messages and offsets
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use compact_str::CompactString;
use scalo::transport::KafkaToken;
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

    /// Destructure into payload and offset, moving the payload without copying
    #[must_use]
    pub fn into_parts(self) -> (Vec<u8>, KafkaOffset) {
        (self.payload, KafkaOffset::new(self.token))
    }
}

/// Kafka offset for commit tracking (newtype around the scalo token)
#[derive(Debug, Clone)]
pub struct KafkaOffset(KafkaToken);

impl KafkaOffset {
    /// Create a new offset from a commit token
    #[must_use]
    pub fn new(token: KafkaToken) -> Self {
        Self(token)
    }

    /// Topic name
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.0.topic
    }

    /// Partition number
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.0.partition
    }

    /// Offset value
    #[must_use]
    pub fn offset(&self) -> i64 {
        self.0.offset
    }

    /// Consume the offset and return the inner commit token
    #[must_use]
    pub fn into_token(self) -> KafkaToken {
        self.0
    }

    /// Get a reference to the commit token (for transport layer)
    pub fn token(&self) -> &KafkaToken {
        &self.0
    }
}

impl From<&KafkaMessage> for KafkaOffset {
    fn from(msg: &KafkaMessage) -> Self {
        Self(msg.token.clone())
    }
}

/// The offsets of one partition inside an [`OffsetSet`].
#[derive(Debug)]
struct PartitionOffsets {
    topic: Arc<str>,
    partition: i32,
    offsets: Vec<i64>,
}

impl PartitionOffsets {
    fn holds(&self, token: &KafkaToken) -> bool {
        self.partition == token.partition
            && (Arc::ptr_eq(&self.topic, &token.topic) || *self.topic == *token.topic)
    }
}

/// Offsets grouped by partition, eight bytes each.
///
/// An open archive file holds the offset of every record in it until the file
/// completes, which at the default roll interval is an hour of intake, so the
/// topic and partition are stored once per partition rather than per record.
#[derive(Debug, Default)]
pub struct OffsetSet {
    partitions: Vec<PartitionOffsets>,
}

impl OffsetSet {
    /// Add `offsets`, in any order.
    pub fn extend(&mut self, offsets: impl IntoIterator<Item = KafkaOffset>) {
        // Records arrive in runs from one partition, so the last one matched
        // is tried before the others are searched.
        let mut at = 0;
        for offset in offsets {
            let token = offset.into_token();
            if !self.partitions.get(at).is_some_and(|p| p.holds(&token)) {
                at = self
                    .partitions
                    .iter()
                    .position(|p| p.holds(&token))
                    .unwrap_or_else(|| {
                        self.partitions.push(PartitionOffsets {
                            topic: Arc::clone(&token.topic),
                            partition: token.partition,
                            offsets: Vec::new(),
                        });
                        self.partitions.len() - 1
                    });
            }
            if let Some(held) = self.partitions.get_mut(at) {
                held.offsets.push(token.offset);
            }
        }
    }

    /// Move every offset of `other` into this set.
    pub fn append(&mut self, other: &mut Self) {
        self.partitions.append(&mut other.partitions);
    }

    /// How many offsets the set holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.partitions.iter().map(|p| p.offsets.len()).sum()
    }

    /// Whether the set holds no offset.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.partitions.iter().all(|p| p.offsets.is_empty())
    }

    /// The commit token of every offset in the set.
    #[must_use]
    pub fn tokens(&self) -> Vec<KafkaToken> {
        let mut tokens = Vec::with_capacity(self.len());
        for held in &self.partitions {
            tokens.extend(
                held.offsets.iter().map(|&offset| {
                    KafkaToken::new(Arc::clone(&held.topic), held.partition, offset)
                }),
            );
        }
        tokens
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

        assert_eq!(offset.topic(), "events");
        assert_eq!(offset.partition(), 3);
        assert_eq!(offset.offset(), 100);
    }

    #[test]
    fn test_kafka_message_clone() {
        let msg = KafkaMessage::for_test(b"payload".to_vec(), "t", 1, 5);
        let cloned = msg.clone();
        assert_eq!(cloned.topic, msg.topic);
        assert_eq!(cloned.offset, msg.offset);
        assert_eq!(cloned.payload, msg.payload);
    }

    #[test]
    fn test_into_parts_moves_payload() {
        let payload = vec![1, 2, 3, 4, 5];
        let msg = KafkaMessage::for_test(payload.clone(), "t", 0, 42);
        let (extracted_payload, offset) = msg.into_parts();
        assert_eq!(extracted_payload, payload);
        assert_eq!(offset.topic(), "t");
        assert_eq!(offset.partition(), 0);
        assert_eq!(offset.offset(), 42);
    }

    fn offset(topic: &str, partition: i32, value: i64) -> KafkaOffset {
        KafkaOffset::from(&KafkaMessage::for_test(Vec::new(), topic, partition, value))
    }

    fn triples(set: &OffsetSet) -> Vec<(String, i32, i64)> {
        let mut out: Vec<_> = set
            .tokens()
            .into_iter()
            .map(|t| (t.topic.to_string(), t.partition, t.offset))
            .collect();
        out.sort_unstable();
        out
    }

    /// Interleaved partitions come back as the tokens that went in, each once.
    #[test]
    fn an_offset_set_returns_every_token_it_was_given() {
        let mut set = OffsetSet::default();
        assert!(set.is_empty());
        set.extend([
            offset("events", 0, 10),
            offset("events", 1, 20),
            offset("events", 0, 11),
            offset("other", 0, 5),
            offset("events", 1, 21),
        ]);

        assert_eq!(set.len(), 5);
        assert!(!set.is_empty());
        assert_eq!(
            triples(&set),
            vec![
                ("events".to_string(), 0, 10),
                ("events".to_string(), 0, 11),
                ("events".to_string(), 1, 20),
                ("events".to_string(), 1, 21),
                ("other".to_string(), 0, 5),
            ]
        );
    }

    /// Appending moves the offsets, so a settled file cannot be released twice.
    #[test]
    fn appending_an_offset_set_empties_the_source() {
        let mut held = OffsetSet::default();
        held.extend([offset("events", 0, 1), offset("events", 0, 2)]);
        let mut settled = OffsetSet::default();
        settled.extend([offset("events", 1, 9)]);

        settled.append(&mut held);

        assert!(held.is_empty());
        assert_eq!(settled.len(), 3);
    }

    #[test]
    fn test_into_token_roundtrip() {
        let msg = KafkaMessage::for_test(b"x".to_vec(), "events", 5, 99);
        let offset = KafkaOffset::from(&msg);
        let token = offset.into_token();
        assert_eq!(token.topic.as_ref(), "events");
        assert_eq!(token.partition, 5);
        assert_eq!(token.offset, 99);
    }
}
