// Project:   dfe-archiver
// File:      crates/core/src/buffer/tiered.rs
// Purpose:   Tiered buffer manager for high destination cardinality
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::types::{KafkaMessage, KafkaOffset};
use crate::{Error, Result};
use compact_str::CompactString;
use dashmap::DashMap;
use hyperi_rustlib::logger::helpers::log_state_change;
use indexmap::IndexMap;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{debug, info, trace, warn};

// Log spam guards for disk pressure conditions
static SPOOL_FULL: AtomicBool = AtomicBool::new(false);
static DISK_LOW: AtomicBool = AtomicBool::new(false);

/// Configuration for tiered buffer manager
#[derive(Debug, Clone)]
pub struct TieredBufferConfig {
    /// Maximum hot buffers in Tier 1 (default: 64)
    pub max_hot_buffers: usize,

    /// Per-buffer size limit in Tier 1 (default: 1MB)
    pub hot_buffer_size: usize,

    /// Flush age for hot buffers (seconds)
    pub hot_buffer_age_secs: u64,

    /// Staging spool directory
    pub spool_dir: PathBuf,

    /// Maximum concurrent archive writers (default: 8)
    pub max_writers: usize,

    /// Staging batch size before archive write (bytes)
    pub staging_batch_size: usize,

    /// Maximum spool size in bytes (disk protection)
    pub max_spool_bytes: u64,

    /// Minimum free disk space to maintain (bytes)
    pub min_free_disk_bytes: u64,

    /// Enable compression for spooled data
    pub spool_compression: bool,
}

impl Default for TieredBufferConfig {
    fn default() -> Self {
        Self {
            max_hot_buffers: 64,
            hot_buffer_size: 1024 * 1024,
            hot_buffer_age_secs: 30,
            spool_dir: PathBuf::from(".tmp/archiver-spool"),
            max_writers: 8,
            staging_batch_size: 64 * 1024 * 1024,
            max_spool_bytes: 10 * 1024 * 1024 * 1024,
            min_free_disk_bytes: 1024 * 1024 * 1024,
            spool_compression: true,
        }
    }
}

/// Hot buffer for a single destination (Tier 1).
/// Appends payload+newline directly during push to avoid a separate
/// serialisation phase at flush time. Flush is a zero-cost `std::mem::take`.
struct HotBuffer {
    data: Vec<u8>,
    offsets: Vec<KafkaOffset>,
    record_count: usize,
    last_access: Instant,
    created_at: Instant,
}

impl HotBuffer {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(1024 * 1024),
            offsets: Vec::with_capacity(1024),
            record_count: 0,
            last_access: Instant::now(),
            created_at: Instant::now(),
        }
    }

    fn push(&mut self, payload: &[u8], offset: KafkaOffset) {
        self.data.extend_from_slice(payload);
        self.data.push(b'\n');
        self.offsets.push(offset);
        self.record_count += 1;
        self.last_access = Instant::now();
    }

    fn drain(&mut self) -> (Vec<u8>, Vec<KafkaOffset>, usize) {
        self.created_at = Instant::now();
        let count = self.record_count;
        self.record_count = 0;
        (
            std::mem::take(&mut self.data),
            std::mem::take(&mut self.offsets),
            count,
        )
    }

    fn size(&self) -> usize {
        self.data.len()
    }

    fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// LRU tracking for hot buffers.
/// Lookup is O(1) via hash. Removal uses `shift_remove` (O(n) index shift)
/// but this is a fast memmove on a small array (`max_hot_buffers`, typically 64).
struct LruTracker {
    order: IndexMap<CompactString, ()>,
    capacity: usize,
}

impl LruTracker {
    fn new(capacity: usize) -> Self {
        Self {
            order: IndexMap::with_capacity(capacity),
            capacity,
        }
    }

    fn access(&mut self, key: &CompactString) -> Option<CompactString> {
        self.order.shift_remove(key);
        self.order.insert(key.clone(), ());
        if self.order.len() > self.capacity {
            self.order.shift_remove_index(0).map(|(k, ())| k)
        } else {
            None
        }
    }
}

/// Staged batch ready for archive writing
#[derive(Debug)]
pub struct StagedBatch {
    /// Destination key
    pub destination: CompactString,

    /// Serialized records (newline-delimited)
    pub data: Vec<u8>,

    /// Kafka offsets to commit after successful write
    pub offsets: Vec<KafkaOffset>,

    /// Record count
    pub record_count: usize,
}

/// Tiered buffer manager
pub struct TieredBufferManager {
    config: TieredBufferConfig,
    hot_buffers: DashMap<CompactString, HotBuffer>,
    lru: Mutex<LruTracker>,
    writer_semaphore: Arc<Semaphore>,
    stats: BufferStats,
}

/// Buffer statistics
#[derive(Default)]
pub struct BufferStats {
    pub messages_buffered: AtomicU64,
    pub hot_buffer_hits: AtomicU64,
    pub hot_buffer_evictions: AtomicU64,
    pub staging_writes: AtomicU64,
    pub archive_writes: AtomicU64,
    pub current_hot_buffers: AtomicUsize,
    pub current_hot_bytes: AtomicUsize,
    pub current_spool_bytes: AtomicU64,
    pub disk_pressure_events: AtomicU64,
}

/// Check available disk space on the volume containing the given path
fn get_available_disk_space(path: &Path) -> std::io::Result<u64> {
    use fs2::available_space;
    available_space(path)
}

impl TieredBufferManager {
    /// Create new tiered buffer manager
    pub fn new(config: TieredBufferConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.spool_dir)?;

        let available = get_available_disk_space(&config.spool_dir).unwrap_or(0);
        if available < config.min_free_disk_bytes {
            return Err(Error::Storage(format!(
                "insufficient disk space: {} bytes available, {} required",
                available, config.min_free_disk_bytes
            )));
        }

        info!(
            spool_dir = %config.spool_dir.display(),
            max_spool_bytes = config.max_spool_bytes,
            min_free_disk = config.min_free_disk_bytes,
            available_disk = available,
            "Tiered buffer manager initialized"
        );

        let writer_semaphore = Arc::new(Semaphore::new(config.max_writers));

        Ok(Self {
            lru: Mutex::new(LruTracker::new(config.max_hot_buffers)),
            hot_buffers: DashMap::with_capacity(config.max_hot_buffers),
            writer_semaphore,
            config,
            stats: BufferStats::default(),
        })
    }

    /// Check if we have enough disk space for a spool write
    pub fn check_disk_space(&self, write_size: u64) -> Result<()> {
        let current_spool = self.stats.current_spool_bytes.load(Ordering::Relaxed);
        if current_spool + write_size > self.config.max_spool_bytes {
            self.stats
                .disk_pressure_events
                .fetch_add(1, Ordering::Relaxed);
            if log_state_change(&SPOOL_FULL, true) {
                warn!(
                    current_spool,
                    write_size,
                    max = self.config.max_spool_bytes,
                    "Spool size limit reached — backpressure"
                );
            }
            return Err(Error::Storage(format!(
                "spool full: {} + {} > {} bytes",
                current_spool, write_size, self.config.max_spool_bytes
            )));
        }

        let available = get_available_disk_space(&self.config.spool_dir).unwrap_or(0);
        if available < self.config.min_free_disk_bytes + write_size {
            self.stats
                .disk_pressure_events
                .fetch_add(1, Ordering::Relaxed);
            if log_state_change(&DISK_LOW, true) {
                warn!(
                    available,
                    write_size,
                    min_free = self.config.min_free_disk_bytes,
                    "Disk space low — backpressure"
                );
            }
            return Err(Error::Storage(format!(
                "disk full: {} available, need {} + {} reserved",
                available, write_size, self.config.min_free_disk_bytes
            )));
        }

        Ok(())
    }

    /// Record bytes written to spool (for tracking)
    pub fn record_spool_write(&self, bytes: u64) {
        self.stats
            .current_spool_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes consumed from spool (for tracking)
    pub fn record_spool_drain(&self, bytes: u64) {
        self.stats
            .current_spool_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Buffer a message for a destination
    pub fn push(&self, destination: &str, message: KafkaMessage) -> Result<Vec<StagedBatch>> {
        let key = CompactString::from(destination);
        let (payload, offset) = message.into_parts();

        self.stats.messages_buffered.fetch_add(1, Ordering::Relaxed);

        let mut batches_to_write = Vec::new();

        let evict_key = {
            let mut lru = self.lru.lock();
            lru.access(&key)
        };

        if let Some(evict_key) = evict_key
            && let Some((_, mut buffer)) = self.hot_buffers.remove(&evict_key)
            && !buffer.is_empty()
        {
            debug!(
                evicted_dest = %evict_key,
                records = buffer.record_count,
                bytes = buffer.size(),
                "LRU eviction triggered"
            );
            self.stats
                .current_hot_bytes
                .fetch_sub(buffer.size(), Ordering::Relaxed);
            let (data, offsets, record_count) = buffer.drain();
            self.stats.staging_writes.fetch_add(1, Ordering::Relaxed);
            batches_to_write.push(StagedBatch {
                destination: evict_key,
                data,
                offsets,
                record_count,
            });
            self.stats
                .hot_buffer_evictions
                .fetch_add(1, Ordering::Relaxed);
        }

        let mut entry = self.hot_buffers.entry(key.clone()).or_insert_with(|| {
            self.stats
                .current_hot_buffers
                .fetch_add(1, Ordering::Relaxed);
            HotBuffer::new()
        });

        let payload_size = payload.len() + 1; // payload + newline
        entry.push(&payload, offset);
        drop(payload); // free Kafka allocation immediately
        self.stats
            .current_hot_bytes
            .fetch_add(payload_size, Ordering::Relaxed);
        self.stats.hot_buffer_hits.fetch_add(1, Ordering::Relaxed);

        trace!(
            destination = %key,
            buffer_size = entry.size(),
            buffer_records = entry.record_count,
            buffer_age_secs = entry.age_secs(),
            "Buffer state after push"
        );

        if entry.size() >= self.config.hot_buffer_size
            || entry.age_secs() >= self.config.hot_buffer_age_secs
        {
            let trigger = if entry.size() >= self.config.hot_buffer_size {
                "size"
            } else {
                "age"
            };
            debug!(
                destination = %key,
                trigger,
                records = entry.record_count,
                bytes = entry.size(),
                "Hot buffer flush"
            );
            let flush_bytes = entry.size();
            let (data, offsets, record_count) = entry.drain();
            self.stats
                .current_hot_bytes
                .fetch_sub(flush_bytes, Ordering::Relaxed);
            self.stats.staging_writes.fetch_add(1, Ordering::Relaxed);
            batches_to_write.push(StagedBatch {
                destination: key,
                data,
                offsets,
                record_count,
            });
        }

        Ok(batches_to_write)
    }

    /// Flush all hot buffers (for shutdown or time-based flush)
    pub fn flush_all(&self) -> Vec<StagedBatch> {
        let mut batches = Vec::new();
        let keys: Vec<_> = self.hot_buffers.iter().map(|e| e.key().clone()).collect();
        debug!(buffer_count = keys.len(), "Flushing all hot buffers");

        for key in keys {
            if let Some(mut entry) = self.hot_buffers.get_mut(&key)
                && !entry.is_empty()
            {
                trace!(
                    destination = %key,
                    records = entry.record_count,
                    bytes = entry.size(),
                    "Flushing hot buffer"
                );
                let (data, offsets, record_count) = entry.drain();
                self.stats.staging_writes.fetch_add(1, Ordering::Relaxed);
                batches.push(StagedBatch {
                    destination: key.clone(),
                    data,
                    offsets,
                    record_count,
                });
            }
        }

        batches
    }

    /// Flush buffers older than max age
    pub fn flush_aged(&self) -> Vec<StagedBatch> {
        let mut batches = Vec::new();
        let max_age = self.config.hot_buffer_age_secs;

        for mut entry in self.hot_buffers.iter_mut() {
            if entry.age_secs() >= max_age && !entry.is_empty() {
                let key = entry.key().clone();
                debug!(
                    destination = %key,
                    age_secs = entry.age_secs(),
                    records = entry.record_count,
                    bytes = entry.size(),
                    "Flushing aged hot buffer"
                );
                let (data, offsets, record_count) = entry.drain();
                self.stats.staging_writes.fetch_add(1, Ordering::Relaxed);
                batches.push(StagedBatch {
                    destination: key,
                    data,
                    offsets,
                    record_count,
                });
            }
        }

        batches
    }

    /// Get writer permit (blocks if max concurrent writers reached)
    #[allow(clippy::expect_used)]
    pub async fn acquire_writer_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.writer_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed")
    }

    /// Get current stats snapshot
    pub fn stats(&self) -> BufferStatsSnapshot {
        BufferStatsSnapshot {
            messages_buffered: self.stats.messages_buffered.load(Ordering::Relaxed),
            hot_buffer_hits: self.stats.hot_buffer_hits.load(Ordering::Relaxed),
            hot_buffer_evictions: self.stats.hot_buffer_evictions.load(Ordering::Relaxed),
            staging_writes: self.stats.staging_writes.load(Ordering::Relaxed),
            archive_writes: self.stats.archive_writes.load(Ordering::Relaxed),
            current_hot_buffers: self.stats.current_hot_buffers.load(Ordering::Relaxed),
            current_hot_bytes: self.stats.current_hot_bytes.load(Ordering::Relaxed),
            current_spool_bytes: self.stats.current_spool_bytes.load(Ordering::Relaxed),
            disk_pressure_events: self.stats.disk_pressure_events.load(Ordering::Relaxed),
        }
    }

    /// Get disk protection config for visibility
    pub fn disk_protection_config(&self) -> (u64, u64) {
        (self.config.max_spool_bytes, self.config.min_free_disk_bytes)
    }
}

/// Snapshot of buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStatsSnapshot {
    pub messages_buffered: u64,
    pub hot_buffer_hits: u64,
    pub hot_buffer_evictions: u64,
    pub staging_writes: u64,
    pub archive_writes: u64,
    pub current_hot_buffers: usize,
    pub current_hot_bytes: usize,
    pub current_spool_bytes: u64,
    pub disk_pressure_events: u64,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn make_message(payload: &[u8], topic: &str, offset: i64) -> KafkaMessage {
        KafkaMessage::for_test(payload.to_vec(), topic, 0, offset)
    }

    #[test]
    fn test_lru_eviction() {
        let config = TieredBufferConfig {
            max_hot_buffers: 2,
            hot_buffer_size: 10000,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };

        let manager = TieredBufferManager::new(config).expect("create manager");

        let batches = manager
            .push("dest1", make_message(b"msg1", "topic", 0))
            .expect("push");
        assert!(batches.is_empty(), "no eviction yet");

        let batches = manager
            .push("dest2", make_message(b"msg2", "topic", 1))
            .expect("push");
        assert!(batches.is_empty(), "no eviction yet");

        let batches = manager
            .push("dest3", make_message(b"msg3", "topic", 2))
            .expect("push");
        assert_eq!(batches.len(), 1, "should evict one");
        assert_eq!(batches[0].destination.as_str(), "dest1");
    }

    #[test]
    fn test_size_based_flush() {
        let config = TieredBufferConfig {
            max_hot_buffers: 10,
            hot_buffer_size: 10,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };

        let manager = TieredBufferManager::new(config).expect("create manager");

        let batches = manager
            .push(
                "dest1",
                make_message(b"this is a longer message", "topic", 0),
            )
            .expect("push");

        assert_eq!(batches.len(), 1, "should flush");
        assert_eq!(batches[0].destination.as_str(), "dest1");
    }

    #[test]
    fn test_flush_all() {
        let config = TieredBufferConfig {
            max_hot_buffers: 10,
            hot_buffer_size: 10000,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };

        let manager = TieredBufferManager::new(config).expect("create manager");

        manager
            .push("dest1", make_message(b"msg1", "topic", 0))
            .expect("push");
        manager
            .push("dest2", make_message(b"msg2", "topic", 1))
            .expect("push");
        manager
            .push("dest3", make_message(b"msg3", "topic", 2))
            .expect("push");

        let batches = manager.flush_all();
        assert_eq!(batches.len(), 3, "should flush all");
    }

    #[test]
    fn test_lru_access_order() {
        let config = TieredBufferConfig {
            max_hot_buffers: 3,
            hot_buffer_size: 100_000,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };
        let manager = TieredBufferManager::new(config).expect("create");
        manager.push("a", make_message(b"1", "t", 0)).expect("push");
        manager.push("b", make_message(b"2", "t", 1)).expect("push");
        manager.push("c", make_message(b"3", "t", 2)).expect("push");
        manager.push("a", make_message(b"4", "t", 3)).expect("push"); // touch "a"
        let batches = manager.push("d", make_message(b"5", "t", 4)).expect("push");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].destination.as_str(), "b"); // "b" evicted, not "a"
    }

    #[test]
    fn test_lru_repeated_access_no_eviction() {
        let config = TieredBufferConfig {
            max_hot_buffers: 2,
            hot_buffer_size: 100_000,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };
        let manager = TieredBufferManager::new(config).expect("create");
        manager.push("a", make_message(b"1", "t", 0)).expect("push");
        manager.push("b", make_message(b"2", "t", 1)).expect("push");
        for i in 2..100 {
            let batches = manager
                .push(
                    if i % 2 == 0 { "a" } else { "b" },
                    make_message(b"x", "t", i),
                )
                .expect("push");
            assert!(batches.is_empty(), "no eviction for existing keys at i={i}");
        }
    }

    #[test]
    fn test_direct_append_ndjson_format() {
        let config = TieredBufferConfig {
            max_hot_buffers: 10,
            hot_buffer_size: 100_000,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };
        let manager = TieredBufferManager::new(config).expect("create");
        manager
            .push("dest", make_message(b"line1", "t", 0))
            .expect("push");
        manager
            .push("dest", make_message(b"line2", "t", 1))
            .expect("push");
        manager
            .push("dest", make_message(b"line3", "t", 2))
            .expect("push");
        let batches = manager.flush_all();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].record_count, 3);
        assert_eq!(batches[0].data, b"line1\nline2\nline3\n");
    }

    #[test]
    fn test_direct_append_preserves_offsets() {
        let config = TieredBufferConfig {
            max_hot_buffers: 10,
            hot_buffer_size: 100_000,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };
        let manager = TieredBufferManager::new(config).expect("create");
        for i in 0..5 {
            manager
                .push("dest", make_message(b"msg", "topic", i))
                .expect("push");
        }
        let batches = manager.flush_all();
        assert_eq!(batches[0].offsets.len(), 5);
        for (i, offset) in batches[0].offsets.iter().enumerate() {
            assert_eq!(offset.offset(), i as i64);
            assert_eq!(offset.topic(), "topic");
        }
    }

    #[test]
    fn test_size_flush_triggers_at_buffer_limit() {
        let config = TieredBufferConfig {
            max_hot_buffers: 10,
            hot_buffer_size: 20,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };
        let manager = TieredBufferManager::new(config).expect("create");
        let batches = manager
            .push("dest", make_message(b"fifteen_bytes!!", "t", 0))
            .expect("push");
        assert!(batches.is_empty(), "should not flush yet");
        let batches = manager
            .push("dest", make_message(b"more!", "t", 1))
            .expect("push");
        assert_eq!(batches.len(), 1, "should flush on size trigger");
        assert_eq!(batches[0].record_count, 2);
    }

    #[test]
    fn test_empty_payload_handling() {
        let config = TieredBufferConfig {
            max_hot_buffers: 10,
            hot_buffer_size: 100_000,
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };
        let manager = TieredBufferManager::new(config).expect("create");
        manager
            .push("dest", make_message(b"", "t", 0))
            .expect("push");
        manager
            .push("dest", make_message(b"data", "t", 1))
            .expect("push");
        let batches = manager.flush_all();
        assert_eq!(batches[0].data, b"\ndata\n");
        assert_eq!(batches[0].record_count, 2);
    }
}
