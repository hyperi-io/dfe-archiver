// Project:   dfe-archiver
// File:      crates/core/src/buffer/tiered.rs
// Purpose:   Tiered buffer manager for high destination cardinality
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::types::{KafkaMessage, KafkaOffset};
use crate::{Error, Result};
use compact_str::CompactString;
use dashmap::DashMap;
use indexmap::IndexMap;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;
use tracing::{debug, info, trace};

/// Default spool directory, and the path the image pre-creates for
/// `appuser`. A relative default resolves under the root-owned WORKDIR.
pub const DEFAULT_SPOOL_DIR: &str = "/var/spool/dfe/archiver";

/// Configuration for tiered buffer manager
#[derive(Debug, Clone)]
pub struct TieredBufferConfig {
    /// Maximum hot buffers in Tier 1 (default: 64)
    pub max_hot_buffers: usize,

    /// Bytes at which a destination's hot buffer flushes (default 1 MiB).
    pub hot_buffer_size: usize,

    /// Records at which a destination's hot buffer flushes, whatever its
    /// size (default 100,000).
    pub hot_buffer_records: usize,

    /// Bytes every destination's hot buffer may hold together. Past it the
    /// largest buffers flush first, so `hot_buffer_size` is a ceiling per
    /// destination rather than a figure multiplied by the destination count.
    pub max_hot_bytes: usize,

    /// Flush age for hot buffers (seconds)
    pub hot_buffer_age_secs: u64,

    /// Spool directory, created at construction.
    pub spool_dir: PathBuf,

    /// Free space the spool volume must have at construction (bytes).
    pub min_free_disk_bytes: u64,
}

impl Default for TieredBufferConfig {
    fn default() -> Self {
        Self {
            max_hot_buffers: 64,
            hot_buffer_size: 1024 * 1024,
            hot_buffer_records: 100_000,
            max_hot_bytes: usize::MAX,
            hot_buffer_age_secs: 30,
            spool_dir: PathBuf::from(DEFAULT_SPOOL_DIR),
            min_free_disk_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Hot buffer for a single destination (Tier 1).
/// Appends payload+newline directly during push to avoid a separate
/// serialisation phase at flush time. Flush is a zero-cost `std::mem::take`.
struct HotBuffer {
    data: Vec<u8>,
    offsets: Vec<KafkaOffset>,
    record_ends: Vec<usize>,
    record_count: usize,
    last_access: Instant,
    created_at: Instant,
}

impl HotBuffer {
    /// Allocates nothing until a record arrives, as a drained buffer does.
    fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            record_ends: Vec::new(),
            record_count: 0,
            last_access: Instant::now(),
            created_at: Instant::now(),
        }
    }

    fn push(&mut self, payload: &[u8], offset: KafkaOffset) {
        self.data.extend_from_slice(payload);
        self.data.push(b'\n');
        self.offsets.push(offset);
        self.record_ends.push(self.data.len());
        self.record_count += 1;
        self.last_access = Instant::now();
    }

    fn drain(&mut self, destination: CompactString) -> StagedBatch {
        self.created_at = Instant::now();
        let record_count = self.record_count;
        self.record_count = 0;
        StagedBatch {
            destination,
            data: std::mem::take(&mut self.data),
            offsets: std::mem::take(&mut self.offsets),
            record_ends: std::mem::take(&mut self.record_ends),
            record_count,
        }
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

    /// Offsets of the batch's records, released once the file they are
    /// written into completes
    pub offsets: Vec<KafkaOffset>,

    /// Where each record ends in `data`, just past its newline, in the order
    /// of `offsets`. A payload may itself hold a newline, so `data` alone
    /// does not say where one record ends.
    pub record_ends: Vec<usize>,

    /// Record count
    pub record_count: usize,
}

impl StagedBatch {
    /// Each record as it arrived, without the newline the buffer appended,
    /// in the order of `offsets`.
    pub fn records(&self) -> impl Iterator<Item = &[u8]> {
        let mut start = 0;
        self.record_ends.iter().map(move |&end| {
            let record = self
                .data
                .get(start..end.saturating_sub(1))
                .unwrap_or_default();
            start = end;
            record
        })
    }
}

/// Tiered buffer manager
pub struct TieredBufferManager {
    config: TieredBufferConfig,
    hot_buffers: DashMap<CompactString, HotBuffer>,
    lru: Mutex<LruTracker>,
    stats: BufferStats,
}

/// Buffer statistics
#[derive(Default)]
pub struct BufferStats {
    /// Buffers holding records that an LRU eviction flushed to make room.
    pub hot_buffer_evictions: AtomicU64,
    pub current_hot_buffers: AtomicUsize,
    pub current_hot_bytes: AtomicUsize,
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
            return Err(Error::storage(format!(
                "insufficient disk space: {} bytes available, {} required",
                available, config.min_free_disk_bytes
            )));
        }

        info!(
            spool_dir = %config.spool_dir.display(),
            min_free_disk = config.min_free_disk_bytes,
            available_disk = available,
            "Tiered buffer manager initialized"
        );

        Ok(Self {
            lru: Mutex::new(LruTracker::new(config.max_hot_buffers)),
            hot_buffers: DashMap::with_capacity(config.max_hot_buffers),
            config,
            stats: BufferStats::default(),
        })
    }

    /// Buffers holding records that an LRU eviction has flushed, since
    /// construction.
    #[must_use]
    pub fn evictions(&self) -> u64 {
        self.stats.hot_buffer_evictions.load(Ordering::Relaxed)
    }

    /// Buffer a message for a destination, and return the batches it closed:
    /// its own buffer at a flush threshold, one an LRU eviction flushed, and
    /// the largest ones past the shared cap.
    #[must_use]
    pub fn push(&self, destination: &str, message: KafkaMessage) -> Vec<StagedBatch> {
        let key = CompactString::from(destination);
        let (payload, offset) = message.into_parts();

        let mut batches_to_write = Vec::new();

        let evict_key = {
            let mut lru = self.lru.lock();
            lru.access(&key)
        };

        if let Some(evict_key) = evict_key
            && let Some((_, mut buffer)) = self.hot_buffers.remove(&evict_key)
        {
            self.stats
                .current_hot_buffers
                .fetch_sub(1, Ordering::Relaxed);
            if !buffer.is_empty() {
                debug!(
                    evicted_dest = %evict_key,
                    records = buffer.record_count,
                    bytes = buffer.size(),
                    "LRU eviction triggered"
                );
                self.stats
                    .current_hot_bytes
                    .fetch_sub(buffer.size(), Ordering::Relaxed);
                batches_to_write.push(buffer.drain(evict_key));
                self.stats
                    .hot_buffer_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
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

        trace!(
            destination = %key,
            buffer_size = entry.size(),
            buffer_records = entry.record_count,
            buffer_age_secs = entry.age_secs(),
            "Buffer state after push"
        );

        let trigger = if entry.size() >= self.config.hot_buffer_size {
            Some("size")
        } else if entry.record_count >= self.config.hot_buffer_records {
            Some("records")
        } else if entry.age_secs() >= self.config.hot_buffer_age_secs {
            Some("age")
        } else {
            None
        };
        if let Some(trigger) = trigger {
            debug!(
                destination = %key,
                trigger,
                records = entry.record_count,
                bytes = entry.size(),
                "Hot buffer flush"
            );
            let flush_bytes = entry.size();
            let batch = entry.drain(key);
            self.stats
                .current_hot_bytes
                .fetch_sub(flush_bytes, Ordering::Relaxed);
            batches_to_write.push(batch);
        }
        // The cap walks every buffer, so this destination's entry is released first.
        drop(entry);
        self.flush_largest_past_cap(&mut batches_to_write);

        batches_to_write
    }

    /// Flush the largest hot buffers until those left hold no more than
    /// `max_hot_bytes` together.
    fn flush_largest_past_cap(&self, batches: &mut Vec<StagedBatch>) {
        while self.stats.current_hot_bytes.load(Ordering::Relaxed) > self.config.max_hot_bytes {
            let largest = self
                .hot_buffers
                .iter()
                .filter(|entry| !entry.is_empty())
                .max_by_key(|entry| entry.size())
                .map(|entry| entry.key().clone());
            let Some(key) = largest else {
                break;
            };
            let Some(mut entry) = self.hot_buffers.get_mut(&key) else {
                break;
            };
            let bytes = entry.size();
            debug!(
                destination = %key,
                bytes,
                cap = self.config.max_hot_bytes,
                "Hot buffers reached their memory cap; flushing the largest"
            );
            batches.push(entry.drain(key));
            self.stats
                .current_hot_bytes
                .fetch_sub(bytes, Ordering::Relaxed);
        }
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
                self.stats
                    .current_hot_bytes
                    .fetch_sub(entry.size(), Ordering::Relaxed);
                batches.push(entry.drain(key.clone()));
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
                self.stats
                    .current_hot_bytes
                    .fetch_sub(entry.size(), Ordering::Relaxed);
                batches.push(entry.drain(key));
            }
        }

        batches
    }

    /// Get current stats snapshot
    pub fn stats(&self) -> BufferStatsSnapshot {
        BufferStatsSnapshot {
            current_hot_buffers: self.stats.current_hot_buffers.load(Ordering::Relaxed),
            current_hot_bytes: self.stats.current_hot_bytes.load(Ordering::Relaxed),
        }
    }

    /// Bytes the hot buffers hold, summed buffer by buffer rather than read
    /// from the running total.
    #[cfg(test)]
    fn held_bytes(&self) -> usize {
        self.hot_buffers.iter().map(|entry| entry.size()).sum()
    }
}

/// Snapshot of buffer statistics
#[derive(Debug, Clone)]
pub struct BufferStatsSnapshot {
    pub current_hot_buffers: usize,
    pub current_hot_bytes: usize,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn make_message(payload: &[u8], topic: &str, offset: i64) -> KafkaMessage {
        KafkaMessage::for_test(payload.to_vec(), topic, 0, offset)
    }

    /// The manager creates its spool on construction, so every test gets its
    /// own directory rather than the absolute default the image provides.
    /// Hold the returned `TempDir` for the test body -- dropping it removes
    /// the spool.
    fn spooled(
        max_hot_buffers: usize,
        hot_buffer_size: usize,
    ) -> (tempfile::TempDir, TieredBufferConfig) {
        let spool = tempfile::tempdir().expect("tempdir");
        let config = TieredBufferConfig {
            max_hot_buffers,
            hot_buffer_size,
            hot_buffer_age_secs: 3600,
            spool_dir: spool.path().to_path_buf(),
            ..Default::default()
        };
        (spool, config)
    }

    #[test]
    fn test_lru_eviction() {
        let (_spool, config) = spooled(2, 10000);

        let manager = TieredBufferManager::new(config).expect("create manager");

        let batches = manager.push("dest1", make_message(b"msg1", "topic", 0));
        assert!(batches.is_empty(), "no eviction yet");

        let batches = manager.push("dest2", make_message(b"msg2", "topic", 1));
        assert!(batches.is_empty(), "no eviction yet");

        assert_eq!(manager.evictions(), 0);

        let batches = manager.push("dest3", make_message(b"msg3", "topic", 2));
        assert_eq!(batches.len(), 1, "should evict one");
        assert_eq!(batches[0].destination.as_str(), "dest1");
        assert_eq!(manager.evictions(), 1, "the eviction is counted");
    }

    /// An evicted buffer that already flushed holds nothing, so its eviction
    /// flushes nothing and is not counted.
    #[test]
    fn an_eviction_of_an_empty_buffer_is_not_counted() {
        let (_spool, config) = spooled(1, 1);
        let manager = TieredBufferManager::new(config).expect("create");
        assert_eq!(manager.push("a", make_message(b"x", "t", 0)).len(), 1);
        assert_eq!(manager.push("b", make_message(b"y", "t", 1)).len(), 1);
        assert_eq!(manager.evictions(), 0);
    }

    #[test]
    fn test_size_based_flush() {
        let (_spool, config) = spooled(10, 10);

        let manager = TieredBufferManager::new(config).expect("create manager");

        let batches = manager.push(
            "dest1",
            make_message(b"this is a longer message", "topic", 0),
        );

        assert_eq!(batches.len(), 1, "should flush");
        assert_eq!(batches[0].destination.as_str(), "dest1");
    }

    #[test]
    fn test_flush_all() {
        let (_spool, config) = spooled(10, 10000);

        let manager = TieredBufferManager::new(config).expect("create manager");

        for (offset, destination) in (0..).zip(["dest1", "dest2", "dest3"]) {
            assert!(
                manager
                    .push(destination, make_message(b"msg", "topic", offset))
                    .is_empty()
            );
        }

        let batches = manager.flush_all();
        assert_eq!(batches.len(), 3, "should flush all");
    }

    #[test]
    fn test_lru_access_order() {
        let (_spool, config) = spooled(3, 100_000);
        let manager = TieredBufferManager::new(config).expect("create");
        // The second "a" touches it, so "b" is the least recently used.
        for (offset, destination) in (0..).zip(["a", "b", "c", "a"]) {
            assert!(
                manager
                    .push(destination, make_message(b"1", "t", offset))
                    .is_empty()
            );
        }
        let batches = manager.push("d", make_message(b"5", "t", 4));
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].destination.as_str(), "b"); // "b" evicted, not "a"
    }

    #[test]
    fn test_lru_repeated_access_no_eviction() {
        let (_spool, config) = spooled(2, 100_000);
        let manager = TieredBufferManager::new(config).expect("create");
        assert!(manager.push("a", make_message(b"1", "t", 0)).is_empty());
        assert!(manager.push("b", make_message(b"2", "t", 1)).is_empty());
        for i in 2..100 {
            let batches = manager.push(
                if i % 2 == 0 { "a" } else { "b" },
                make_message(b"x", "t", i),
            );
            assert!(batches.is_empty(), "no eviction for existing keys at i={i}");
        }
    }

    #[test]
    fn test_direct_append_ndjson_format() {
        let (_spool, config) = spooled(10, 100_000);
        let manager = TieredBufferManager::new(config).expect("create");
        for (offset, line) in (0..).zip([&b"line1"[..], b"line2", b"line3"]) {
            assert!(
                manager
                    .push("dest", make_message(line, "t", offset))
                    .is_empty()
            );
        }
        let batches = manager.flush_all();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].record_count, 3);
        assert_eq!(batches[0].data, b"line1\nline2\nline3\n");
    }

    #[test]
    fn test_direct_append_preserves_offsets() {
        let (_spool, config) = spooled(10, 100_000);
        let manager = TieredBufferManager::new(config).expect("create");
        for i in 0..5 {
            assert!(
                manager
                    .push("dest", make_message(b"msg", "topic", i))
                    .is_empty()
            );
        }
        let batches = manager.flush_all();
        assert_eq!(batches[0].offsets.len(), 5);
        for (i, offset) in batches[0].offsets.iter().enumerate() {
            #[allow(clippy::cast_possible_wrap)]
            {
                assert_eq!(offset.offset(), i as i64);
            }
            assert_eq!(offset.topic(), "topic");
        }
    }

    #[test]
    fn test_size_flush_triggers_at_buffer_limit() {
        let (_spool, config) = spooled(10, 20);
        let manager = TieredBufferManager::new(config).expect("create");
        let batches = manager.push("dest", make_message(b"fifteen_bytes!!", "t", 0));
        assert!(batches.is_empty(), "should not flush yet");
        let batches = manager.push("dest", make_message(b"more!", "t", 1));
        assert_eq!(batches.len(), 1, "should flush on size trigger");
        assert_eq!(batches[0].record_count, 2);
    }

    #[test]
    fn test_empty_payload_handling() {
        let (_spool, config) = spooled(10, 100_000);
        let manager = TieredBufferManager::new(config).expect("create");
        assert!(manager.push("dest", make_message(b"", "t", 0)).is_empty());
        assert!(
            manager
                .push("dest", make_message(b"data", "t", 1))
                .is_empty()
        );
        let batches = manager.flush_all();
        assert_eq!(batches[0].data, b"\ndata\n");
        assert_eq!(batches[0].record_count, 2);
    }

    /// Many destinations at a large per-destination flush size hold no more
    /// than the cap together, and every record pushed comes back once.
    #[test]
    fn hot_buffers_together_stay_under_the_cap_and_lose_nothing() {
        const RECORD: usize = 1024;
        const CAP: usize = 256 * 1024;
        const RECORDS: i64 = 20_000;
        let (_spool, mut config) = spooled(64, 1024 * 1024 * 1024);
        config.max_hot_bytes = CAP;
        let manager = TieredBufferManager::new(config).expect("create");

        let mut batches = Vec::new();
        let mut most_held = 0;
        for offset in 0..RECORDS {
            let destination = format!("dest-{}", offset % 64);
            let payload = vec![b'x'; RECORD - 1];
            batches.extend(manager.push(&destination, make_message(&payload, "t", offset)));
            let held = manager.held_bytes();
            most_held = most_held.max(held);
            assert!(held <= CAP, "{held} bytes held after offset {offset}");
        }
        assert!(most_held > CAP / 2, "the cap was reached: {most_held}");
        batches.extend(manager.flush_all());

        let mut offsets: Vec<i64> = batches
            .iter()
            .flat_map(|batch| batch.offsets.iter().map(KafkaOffset::offset))
            .collect();
        offsets.sort_unstable();
        assert_eq!(
            offsets,
            (0..RECORDS).collect::<Vec<_>>(),
            "each record once"
        );
        let bytes: usize = batches.iter().map(|batch| batch.data.len()).sum();
        assert_eq!(bytes, 20_000 * RECORD);
        assert_eq!(manager.held_bytes(), 0);
        assert_eq!(
            manager.stats().current_hot_bytes,
            0,
            "the total follows every flush"
        );
    }

    /// At the cap the largest buffer flushes, and a smaller one keeps filling.
    #[test]
    fn the_largest_buffer_flushes_first_at_the_cap() {
        let (_spool, mut config) = spooled(10, 1024 * 1024);
        // Four records of ten bytes pass it.
        config.max_hot_bytes = 39;
        let manager = TieredBufferManager::new(config).expect("create");

        for offset in 0..3 {
            let flushed = manager.push("large", make_message(b"123456789", "t", offset));
            assert!(flushed.is_empty());
        }
        let flushed = manager.push("small", make_message(b"123456789", "t", 3));
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].destination.as_str(), "large");
        assert_eq!(flushed[0].record_count, 3);
        assert_eq!(
            manager.held_bytes(),
            10,
            "the small buffer keeps its record"
        );
    }

    /// Flushing by age counts the flushed bytes off the running total, so the
    /// cap and the buffer gauges read what the buffers hold.
    #[test]
    fn an_aged_flush_counts_its_bytes_off_the_total() {
        let (_spool, mut config) = spooled(10, 1024 * 1024);
        config.hot_buffer_age_secs = 0;
        let manager = TieredBufferManager::new(config).expect("create");
        let pushed = manager.push("dest", make_message(b"aged", "t", 0));

        let aged = manager.flush_aged();
        assert_eq!(
            pushed.len() + aged.len(),
            1,
            "the record flushes once, at the push or by age"
        );
        assert_eq!(manager.stats().current_hot_bytes, manager.held_bytes());
    }

    /// No more than `max_hot_buffers` destinations buffer at once, and the
    /// buffer count and the eviction count follow the evictions.
    #[test]
    fn no_more_than_max_hot_buffers_destinations_buffer_at_once() {
        let (_spool, config) = spooled(64, 1024 * 1024);
        let manager = TieredBufferManager::new(config).expect("create");
        let evicted: usize = (0..100)
            .map(|offset| {
                manager
                    .push(&format!("dest-{offset}"), make_message(b"x", "t", offset))
                    .len()
            })
            .sum();
        assert_eq!(evicted, 36);
        assert_eq!(manager.evictions(), 36);
        assert_eq!(manager.hot_buffers.len(), 64);
        assert_eq!(manager.stats().current_hot_buffers, 64);
    }

    /// Each record comes back whole, one per offset, even when its payload
    /// holds a newline of its own.
    #[test]
    fn records_split_on_record_boundaries_not_newlines() {
        let (_spool, config) = spooled(10, 100_000);
        let manager = TieredBufferManager::new(config).expect("create");
        for (offset, payload) in (0i64..).zip([&b"one"[..], b"two\nlines", b"", b"four"]) {
            assert!(
                manager
                    .push("dest", make_message(payload, "t", offset))
                    .is_empty()
            );
        }
        let batches = manager.flush_all();
        let records: Vec<&[u8]> = batches[0].records().collect();
        assert_eq!(
            records,
            vec![&b"one"[..], b"two\nlines", b"", b"four"],
            "one record per payload, embedded newline kept"
        );
        assert_eq!(records.len(), batches[0].offsets.len());
    }
}
