// Project:   dfe-archiver
// File:      src/buffer/tiered.rs
// Purpose:   Tiered buffer manager for high destination cardinality
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

//! # Tiered Buffer Manager
//!
//! Handles potentially thousands of concurrent destinations (e.g., per-org routing)
//! without exhausting memory or file handles.
//!
//! ## Architecture
//!
//! ```text
//! Kafka Messages
//!       ↓
//! ┌─────────────────────────────────────────────────────────┐
//! │  Tier 1: Hot Buffers (memory, LRU bounded)              │
//! │  - Max N destinations (default 64)                      │
//! │  - Fast path for active destinations                    │
//! │  - LRU eviction to Tier 2 when full                     │
//! └─────────────────────────────────────────────────────────┘
//!       ↓ (eviction or flush)
//! ┌─────────────────────────────────────────────────────────┐
//! │  Tier 2: Staging Spool (disk-backed)                    │
//! │  - Unbounded destinations                               │
//! │  - Compressed on disk                                   │
//! │  - Batched for final archive write                      │
//! └─────────────────────────────────────────────────────────┘
//!       ↓ (archive flush)
//! ┌─────────────────────────────────────────────────────────┐
//! │  Archive Writers (bounded pool)                         │
//! │  - Max M concurrent writers (default 8)                 │
//! │  - Semaphore-controlled                                 │
//! │  - One active file per destination at a time            │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Why Two Tiers?
//!
//! With expression-based routing (e.g., by org_id), you could have:
//! - 10,000+ unique destinations
//! - Each needing buffer + compression state + file handle
//! - 64MB buffer × 10K = 640GB memory (not feasible)
//!
//! The tiered approach:
//! - Tier 1: 64 hot buffers × 1MB = 64MB memory (fast path)
//! - Tier 2: Disk spool handles overflow (bounded disk, not memory)
//! - Writers: 8 concurrent files max (bounded file handles)

use crate::kafka::{KafkaMessage, KafkaOffset};
use crate::{Error, Result};
use compact_str::CompactString;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{info, warn};

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
    /// When exceeded, backpressure is applied by returning error.
    /// Default: 10GB
    pub max_spool_bytes: u64,

    /// Minimum free disk space to maintain (bytes)
    /// Spool operations fail if free space would drop below this.
    /// Default: 1GB
    pub min_free_disk_bytes: u64,

    /// Enable compression for spooled data (uses zstd via hyperi-rustlib)
    pub spool_compression: bool,
}

impl Default for TieredBufferConfig {
    fn default() -> Self {
        Self {
            max_hot_buffers: 64,
            hot_buffer_size: 1024 * 1024, // 1MB per hot buffer
            hot_buffer_age_secs: 30,
            spool_dir: PathBuf::from(".tmp/archiver-spool"),
            max_writers: 8,
            staging_batch_size: 64 * 1024 * 1024, // 64MB staging batch
            max_spool_bytes: 10 * 1024 * 1024 * 1024, // 10GB
            min_free_disk_bytes: 1024 * 1024 * 1024, // 1GB
            spool_compression: true,
        }
    }
}

/// Hot buffer for a single destination (Tier 1)
struct HotBuffer {
    /// Destination key
    key: CompactString,

    /// Buffered messages (raw payloads)
    messages: Vec<Vec<u8>>,

    /// Kafka offsets for commit tracking
    offsets: Vec<KafkaOffset>,

    /// Current buffer size in bytes
    size: usize,

    /// Last access time (for LRU)
    last_access: Instant,

    /// Creation time (for age-based flush)
    created_at: Instant,
}

impl HotBuffer {
    fn new(key: CompactString) -> Self {
        Self {
            key,
            messages: Vec::with_capacity(1024),
            offsets: Vec::with_capacity(1024),
            size: 0,
            last_access: Instant::now(),
            created_at: Instant::now(),
        }
    }

    fn push(&mut self, payload: Vec<u8>, offset: KafkaOffset) {
        self.size += payload.len();
        self.messages.push(payload);
        self.offsets.push(offset);
        self.last_access = Instant::now();
    }

    fn drain(&mut self) -> (Vec<Vec<u8>>, Vec<KafkaOffset>) {
        self.size = 0;
        self.created_at = Instant::now();
        (
            std::mem::take(&mut self.messages),
            std::mem::take(&mut self.offsets),
        )
    }

    fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

/// LRU tracking for hot buffers
struct LruTracker {
    /// Order of access (most recent at back)
    order: VecDeque<CompactString>,

    /// Max capacity
    capacity: usize,
}

impl LruTracker {
    fn new(capacity: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Record access, returns key to evict if over capacity
    fn access(&mut self, key: &CompactString) -> Option<CompactString> {
        // Remove from current position if exists
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }

        // Add to back (most recent)
        self.order.push_back(key.clone());

        // Evict oldest if over capacity
        if self.order.len() > self.capacity {
            self.order.pop_front()
        } else {
            None
        }
    }

    /// Remove key from tracking
    fn remove(&mut self, key: &CompactString) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
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

    /// Tier 1: Hot buffers (bounded, in-memory)
    hot_buffers: DashMap<CompactString, HotBuffer>,

    /// LRU tracker for hot buffers
    lru: Mutex<LruTracker>,

    /// Writer semaphore (bounds concurrent archive writes)
    writer_semaphore: Arc<Semaphore>,

    /// Stats
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
    /// Current spool size in bytes (tracked for disk protection)
    pub current_spool_bytes: AtomicU64,
    /// Number of times disk space protection was triggered
    pub disk_pressure_events: AtomicU64,
}

/// Check available disk space on the volume containing the given path
fn get_available_disk_space(path: &Path) -> std::io::Result<u64> {
    use fs2::available_space;
    // fs2::available_space works on both files and directories
    // It returns the available space on the volume containing the path
    available_space(path)
}

/// Disk space protection error
#[derive(Debug, Clone)]
pub struct DiskSpaceError {
    pub available_bytes: u64,
    pub required_bytes: u64,
    pub min_free_bytes: u64,
}

impl TieredBufferManager {
    /// Create new tiered buffer manager
    pub fn new(config: TieredBufferConfig) -> Result<Self> {
        // Create spool directory if needed
        std::fs::create_dir_all(&config.spool_dir)?;

        // Check initial disk space
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

    /// Check if we have enough disk space for a spool write.
    /// Returns Ok(()) if safe, Err with DiskSpaceError if disk pressure detected.
    pub fn check_disk_space(&self, write_size: u64) -> Result<()> {
        // Check against max spool size limit
        let current_spool = self.stats.current_spool_bytes.load(Ordering::Relaxed);
        if current_spool + write_size > self.config.max_spool_bytes {
            self.stats
                .disk_pressure_events
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                current_spool,
                write_size,
                max = self.config.max_spool_bytes,
                "Spool size limit reached - backpressure"
            );
            return Err(Error::Storage(format!(
                "spool full: {} + {} > {} bytes",
                current_spool, write_size, self.config.max_spool_bytes
            )));
        }

        // Check actual filesystem free space
        let available = get_available_disk_space(&self.config.spool_dir).unwrap_or(0);
        if available < self.config.min_free_disk_bytes + write_size {
            self.stats
                .disk_pressure_events
                .fetch_add(1, Ordering::Relaxed);
            warn!(
                available,
                write_size,
                min_free = self.config.min_free_disk_bytes,
                "Disk space low - backpressure"
            );
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
    ///
    /// Returns batches that should be written (evicted or ready to flush)
    pub fn push(&self, destination: &str, message: KafkaMessage) -> Result<Vec<StagedBatch>> {
        let key = CompactString::from(destination);
        let offset = KafkaOffset::from(&message);
        let payload = message.payload;

        self.stats.messages_buffered.fetch_add(1, Ordering::Relaxed);

        let mut batches_to_write = Vec::new();

        // Check LRU and get eviction candidate
        let evict_key = {
            let mut lru = self.lru.lock();
            lru.access(&key)
        };

        // Evict if needed
        if let Some(evict_key) = evict_key {
            if let Some((_, mut buffer)) = self.hot_buffers.remove(&evict_key) {
                if !buffer.is_empty() {
                    let (messages, offsets) = buffer.drain();
                    batches_to_write.push(self.create_staged_batch(evict_key, messages, offsets));
                    self.stats
                        .hot_buffer_evictions
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Get or create hot buffer
        let mut entry = self.hot_buffers.entry(key.clone()).or_insert_with(|| {
            self.stats
                .current_hot_buffers
                .fetch_add(1, Ordering::Relaxed);
            HotBuffer::new(key.clone())
        });

        let payload_size = payload.len();
        entry.push(payload, offset);
        self.stats
            .current_hot_bytes
            .fetch_add(payload_size, Ordering::Relaxed);
        self.stats.hot_buffer_hits.fetch_add(1, Ordering::Relaxed);

        // Check if buffer should flush (size or age)
        if entry.size >= self.config.hot_buffer_size
            || entry.age_secs() >= self.config.hot_buffer_age_secs
        {
            let (messages, offsets) = entry.drain();
            self.stats.current_hot_bytes.fetch_sub(
                messages.iter().map(|m| m.len()).sum::<usize>(),
                Ordering::Relaxed,
            );
            batches_to_write.push(self.create_staged_batch(key, messages, offsets));
        }

        Ok(batches_to_write)
    }

    /// Flush all hot buffers (for shutdown or time-based flush)
    pub fn flush_all(&self) -> Vec<StagedBatch> {
        let mut batches = Vec::new();

        // Collect all keys first to avoid holding locks
        let keys: Vec<_> = self.hot_buffers.iter().map(|e| e.key().clone()).collect();

        for key in keys {
            if let Some(mut entry) = self.hot_buffers.get_mut(&key) {
                if !entry.is_empty() {
                    let (messages, offsets) = entry.drain();
                    batches.push(self.create_staged_batch(key.clone(), messages, offsets));
                }
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
                let (messages, offsets) = entry.drain();
                batches.push(self.create_staged_batch(key, messages, offsets));
            }
        }

        batches
    }

    /// Get writer permit (blocks if max concurrent writers reached)
    pub async fn acquire_writer_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.writer_semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed")
    }

    /// Create staged batch from buffered messages
    fn create_staged_batch(
        &self,
        destination: CompactString,
        messages: Vec<Vec<u8>>,
        offsets: Vec<KafkaOffset>,
    ) -> StagedBatch {
        let record_count = messages.len();

        // Concatenate with newlines
        let total_size: usize = messages.iter().map(|m| m.len() + 1).sum();
        let mut data = Vec::with_capacity(total_size);

        for msg in messages {
            data.extend_from_slice(&msg);
            data.push(b'\n');
        }

        self.stats.staging_writes.fetch_add(1, Ordering::Relaxed);

        StagedBatch {
            destination,
            data,
            offsets,
            record_count,
        }
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
mod tests {
    use super::*;
    use hyperi_rustlib::transport::KafkaToken;
    use std::sync::Arc;

    fn make_message(payload: &[u8], topic: &str, offset: i64) -> KafkaMessage {
        KafkaMessage {
            key: None,
            payload: payload.to_vec(),
            topic: CompactString::from(topic),
            partition: 0,
            offset,
            timestamp_ms: None,
            token: KafkaToken::new(Arc::from(topic), 0, offset),
        }
    }

    #[test]
    fn test_lru_eviction() {
        let config = TieredBufferConfig {
            max_hot_buffers: 2,
            hot_buffer_size: 10000, // Large to prevent size-based flush
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };

        let manager = TieredBufferManager::new(config).expect("create manager");

        // Add to dest1
        let batches = manager
            .push("dest1", make_message(b"msg1", "topic", 0))
            .expect("push");
        assert!(batches.is_empty(), "no eviction yet");

        // Add to dest2
        let batches = manager
            .push("dest2", make_message(b"msg2", "topic", 1))
            .expect("push");
        assert!(batches.is_empty(), "no eviction yet");

        // Add to dest3 - should evict dest1 (LRU)
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
            hot_buffer_size: 10, // Very small to trigger flush
            hot_buffer_age_secs: 3600,
            ..Default::default()
        };

        let manager = TieredBufferManager::new(config).expect("create manager");

        // Add message that exceeds buffer size
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

        // Add to multiple destinations
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
}
