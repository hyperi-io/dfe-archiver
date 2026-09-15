// Project:   dfe-archiver
// File:      crates/core/src/buffer/manager.rs
// Purpose:   Per-destination buffer management with flush triggers
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::config::BufferConfig;
use crate::types::KafkaMessage;
use compact_str::CompactString;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Buffer for a single destination
pub struct DestinationBuffer {
    /// Destination key (topic or routed path)
    pub key: CompactString,

    /// Buffered messages
    messages: Vec<KafkaMessage>,

    /// Total bytes in buffer
    bytes: AtomicUsize,

    /// Buffer creation time
    created_at: Instant,
}

impl DestinationBuffer {
    /// Create new destination buffer
    fn new(key: CompactString) -> Self {
        Self {
            key,
            messages: Vec::with_capacity(1024),
            bytes: AtomicUsize::new(0),
            created_at: Instant::now(),
        }
    }

    /// Add message to buffer
    fn push(&mut self, message: KafkaMessage) {
        let size = message.payload.len();
        self.messages.push(message);
        self.bytes.fetch_add(size, Ordering::Relaxed);
    }

    /// Get buffer size in bytes
    fn size(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Get message count
    fn len(&self) -> usize {
        self.messages.len()
    }

    /// Get buffer age in seconds
    fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    /// Drain messages from buffer
    fn drain(&mut self) -> Vec<KafkaMessage> {
        self.bytes.store(0, Ordering::Relaxed);
        self.created_at = Instant::now();
        std::mem::take(&mut self.messages)
    }
}

/// Flush decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// Buffer size exceeded threshold
    Size,
    /// Buffer age exceeded threshold
    Age,
    /// Record count exceeded threshold
    Records,
    /// Memory pressure
    Pressure,
    /// Explicit flush request
    Explicit,
}

/// Per-destination buffer manager
pub struct BufferManager {
    config: BufferConfig,
    buffers: DashMap<CompactString, DestinationBuffer>,
    total_bytes: AtomicUsize,
    total_records: AtomicU64,
}

impl BufferManager {
    /// Create new buffer manager
    #[must_use]
    pub fn new(config: BufferConfig) -> Self {
        Self {
            config,
            buffers: DashMap::new(),
            total_bytes: AtomicUsize::new(0),
            total_records: AtomicU64::new(0),
        }
    }

    /// Add message to appropriate buffer
    pub fn push(&self, destination: &str, message: KafkaMessage) {
        let size = message.payload.len();
        let key = CompactString::from(destination);

        self.buffers
            .entry(key.clone())
            .or_insert_with(|| DestinationBuffer::new(key))
            .push(message);

        self.total_bytes.fetch_add(size, Ordering::Relaxed);
        self.total_records.fetch_add(1, Ordering::Relaxed);
    }

    /// Check which buffers should be flushed
    pub fn check_flush(&self) -> Vec<(CompactString, FlushReason)> {
        let mut to_flush = Vec::new();

        for entry in &self.buffers {
            let buffer = entry.value();

            if buffer.size() >= self.config.flush_bytes {
                to_flush.push((buffer.key.clone(), FlushReason::Size));
                continue;
            }

            if buffer.len() >= self.config.flush_records {
                to_flush.push((buffer.key.clone(), FlushReason::Records));
                continue;
            }

            if buffer.age_secs() >= self.config.flush_age_secs {
                to_flush.push((buffer.key.clone(), FlushReason::Age));
            }
        }

        to_flush
    }

    /// Drain messages from a specific buffer
    pub fn drain(&self, destination: &str) -> Option<Vec<KafkaMessage>> {
        let key = CompactString::from(destination);

        self.buffers.get_mut(&key).map(|mut entry| {
            let messages = entry.drain();
            let bytes: usize = messages.iter().map(|m| m.payload.len()).sum();
            self.total_bytes.fetch_sub(bytes, Ordering::Relaxed);
            self.total_records
                .fetch_sub(messages.len() as u64, Ordering::Relaxed);
            messages
        })
    }

    /// Drain all buffers (for shutdown)
    pub fn drain_all(&self) -> Vec<(CompactString, Vec<KafkaMessage>)> {
        let mut all = Vec::new();

        for mut entry in self.buffers.iter_mut() {
            let key = entry.key().clone();
            let messages = entry.drain();
            if !messages.is_empty() {
                all.push((key, messages));
            }
        }

        self.total_bytes.store(0, Ordering::Relaxed);
        self.total_records.store(0, Ordering::Relaxed);

        all
    }

    /// Get total bytes buffered
    #[must_use]
    pub fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get total records buffered
    #[must_use]
    pub fn total_records(&self) -> u64 {
        self.total_records.load(Ordering::Relaxed)
    }

    /// Get number of active destinations
    #[must_use]
    pub fn destination_count(&self) -> usize {
        self.buffers.len()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::types::KafkaMessage;

    fn make_message(payload: &[u8]) -> KafkaMessage {
        KafkaMessage::for_test(payload.to_vec(), "test-topic", 0, 0)
    }

    #[test]
    fn test_buffer_push_and_drain() {
        let config = BufferConfig {
            flush_bytes: 1000,
            flush_age_secs: 60,
            flush_records: 100,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);

        manager.push("dest1", make_message(b"hello"));
        manager.push("dest1", make_message(b"world"));
        manager.push("dest2", make_message(b"other"));

        assert_eq!(manager.total_records(), 3);
        assert_eq!(manager.destination_count(), 2);

        let messages = manager.drain("dest1").expect("should have messages");
        assert_eq!(messages.len(), 2);
        assert_eq!(manager.total_records(), 1);
    }

    #[test]
    fn test_flush_by_size() {
        let config = BufferConfig {
            flush_bytes: 10,
            flush_age_secs: 3600,
            flush_records: 1000,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);
        manager.push("dest1", make_message(b"this is a longer message"));

        let to_flush = manager.check_flush();
        assert!(!to_flush.is_empty());
        assert_eq!(to_flush[0].1, FlushReason::Size);
    }

    #[test]
    fn test_flush_by_records() {
        let config = BufferConfig {
            flush_bytes: 1_000_000,
            flush_age_secs: 3600,
            flush_records: 3,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);
        manager.push("dest1", make_message(b"a"));
        manager.push("dest1", make_message(b"b"));
        manager.push("dest1", make_message(b"c"));

        let to_flush = manager.check_flush();
        assert!(!to_flush.is_empty());
        assert_eq!(to_flush[0].1, FlushReason::Records);
    }

    #[test]
    fn test_flush_by_age() {
        let config = BufferConfig {
            flush_bytes: 1_000_000,
            flush_age_secs: 0, // immediate age trigger
            flush_records: 1_000_000,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);
        manager.push("dest1", make_message(b"hello"));

        let to_flush = manager.check_flush();
        assert!(!to_flush.is_empty());
        assert_eq!(to_flush[0].1, FlushReason::Age);
    }

    #[test]
    fn test_drain_all() {
        let config = BufferConfig {
            flush_bytes: 1_000_000,
            flush_age_secs: 3600,
            flush_records: 1_000_000,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);
        manager.push("dest1", make_message(b"a"));
        manager.push("dest2", make_message(b"b"));
        manager.push("dest3", make_message(b"c"));

        assert_eq!(manager.destination_count(), 3);
        assert_eq!(manager.total_records(), 3);

        let all = manager.drain_all();
        assert_eq!(all.len(), 3);
        assert_eq!(manager.total_records(), 0);
        assert_eq!(manager.total_bytes(), 0);
    }

    #[test]
    fn test_drain_nonexistent_destination() {
        let config = BufferConfig {
            flush_bytes: 1_000_000,
            flush_age_secs: 3600,
            flush_records: 1_000_000,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);
        let result = manager.drain("nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_check_flush_no_triggers() {
        let config = BufferConfig {
            flush_bytes: 1_000_000,
            flush_age_secs: 3600,
            flush_records: 1_000_000,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);
        manager.push("dest1", make_message(b"small"));

        let to_flush = manager.check_flush();
        assert!(to_flush.is_empty(), "nothing should trigger flush");
    }

    #[test]
    fn test_total_bytes_tracking() {
        let config = BufferConfig {
            flush_bytes: 1_000_000,
            flush_age_secs: 3600,
            flush_records: 1_000_000,
            writer_parallelism: 4,
            backpressure_pause_secs: 5,
            ..Default::default()
        };

        let manager = BufferManager::new(config);
        manager.push("dest1", make_message(b"hello")); // 5 bytes
        manager.push("dest1", make_message(b"world")); // 5 bytes
        assert_eq!(manager.total_bytes(), 10);

        manager.drain("dest1");
        assert_eq!(manager.total_bytes(), 0);
    }
}
