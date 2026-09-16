// Project:   dfe-archiver
// File:      crates/core/src/archive/writer.rs
// Purpose:   Archive writer with rolling support
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::Result;
use crate::compression::Compressor;
use crate::config::ArchiveConfig;
use crate::storage::StorageBackend;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, trace};

const MAX_OPEN_RETRIES: u32 = 10_000;

/// The placeholders `path_stem` and `generate_path` substitute, and the whole
/// set an operator may write in `archive.path_template`.
///
/// Keep this beside the substitutions below: an unlisted placeholder is not
/// rejected by the writer, it survives into the object key as literal braces,
/// and it is then baked into every object already written.
pub const PATH_TEMPLATE_PLACEHOLDERS: [&str; 7] = [
    "{year}",
    "{month}",
    "{day}",
    "{hour}",
    "{minute}",
    "{timestamp}",
    "{seq}",
];

/// The `{...}` tokens in `template` that no substitution handles.
///
/// A token runs from the last `{` before a `}`, so an unclosed brace shadows
/// nothing: `{stray/{year}` reports no unknown placeholder.
#[must_use]
pub fn unknown_placeholders(template: &str) -> Vec<String> {
    let mut unknown: Vec<String> = Vec::new();
    let mut open: Option<usize> = None;

    for (index, c) in template.char_indices() {
        match c {
            '{' => open = Some(index),
            '}' => {
                if let Some(start) = open.take() {
                    let token = &template[start..=index];
                    if !PATH_TEMPLATE_PLACEHOLDERS.contains(&token)
                        && !unknown.iter().any(|seen| seen.as_str() == token)
                    {
                        unknown.push(token.to_string());
                    }
                }
            }
            _ => {}
        }
    }

    unknown
}

/// Stats returned from a flush operation (for metrics wiring)
#[derive(Debug, Clone)]
pub struct FlushStats {
    /// Bytes after compression
    pub compressed_bytes: u64,
    /// Bytes before compression
    pub uncompressed_bytes: u64,
    /// Time spent in the compressor
    pub compression_duration_secs: f64,
}

/// Stats returned from a roll/close operation (for metrics wiring)
#[derive(Debug, Clone)]
pub struct CloseStats {
    /// Total compressed bytes in the closed file
    pub compressed_bytes: u64,
    /// Roll trigger reason (None for explicit close, Some for automatic roll)
    pub trigger: Option<&'static str>,
}

/// Rolling policy for archive files
#[derive(Debug, Clone)]
pub struct RollingPolicy {
    /// Roll when final compressed file size exceeds this (bytes)
    pub max_size_bytes: u64,

    /// Roll when file age exceeds this (seconds)
    pub max_age_secs: u64,
}

impl Default for RollingPolicy {
    fn default() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024,
            max_age_secs: 3600,
        }
    }
}

/// Archive writer state
#[derive(Debug)]
pub struct ArchiveState {
    pub path: String,
    pub compressed_bytes: AtomicU64,
    pub uncompressed_bytes: AtomicU64,
    pub records_written: AtomicU64,
    pub created_at: DateTime<Utc>,
}

/// Archive writer with compression and rolling
pub struct ArchiveWriter {
    config: ArchiveConfig,
    policy: RollingPolicy,
    compressor: Arc<dyn Compressor + Send + Sync>,
    storage: Box<dyn StorageBackend + Send + Sync>,
    state: Option<ArchiveState>,
    buffer: Vec<u8>,
    file_seq: u64,
    last_stem: Option<String>,
    /// Files opened since the caller last drained the count. This crate owns no
    /// metrics, so the writer counts and the archiver crate records.
    files_opened: u64,
    /// Rolls completed since the caller last drained them, same reason as
    /// `files_opened`. A single `write_record` can roll on either half, so one
    /// returned `CloseStats` would drop the other and undercount the closes.
    rolls: Vec<CloseStats>,
}

impl ArchiveWriter {
    /// Create new archive writer
    pub fn new(
        config: ArchiveConfig,
        policy: RollingPolicy,
        compressor: Box<dyn Compressor + Send + Sync>,
        storage: Box<dyn StorageBackend + Send + Sync>,
    ) -> Self {
        Self {
            config,
            policy,
            compressor: Arc::from(compressor),
            storage,
            state: None,
            buffer: Vec::with_capacity(1024 * 1024),
            file_seq: 0,
            last_stem: None,
            files_opened: 0,
            rolls: Vec::new(),
        }
    }

    /// Take the number of files opened since the last call, resetting the count.
    ///
    /// The archiver crate drains this into `files_created_total`, the success
    /// denominator for the sink-failure alert.
    pub fn take_files_opened(&mut self) -> u64 {
        std::mem::take(&mut self.files_opened)
    }

    /// Take the rolls completed since the last call, resetting the list.
    ///
    /// The archiver crate drains this into `archive_roll_total` and
    /// `files_closed_total`.
    pub fn take_rolls(&mut self) -> Vec<CloseStats> {
        std::mem::take(&mut self.rolls)
    }

    /// Write data to archive, rolling first if the policy says so.
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        if let Some(trigger) = self.should_roll() {
            self.roll(trigger).await?;
        }

        if self.state.is_none() {
            self.open_new_file().await?;
        }

        self.buffer.extend_from_slice(data);

        if let Some(ref state) = self.state {
            state
                .uncompressed_bytes
                .fetch_add(data.len() as u64, Ordering::Relaxed);
            trace!(
                path = %state.path,
                write_bytes = data.len(),
                buffer_bytes = self.buffer.len(),
                total_uncompressed = state.uncompressed_bytes.load(Ordering::Relaxed),
                total_compressed = state.compressed_bytes.load(Ordering::Relaxed),
                "Buffered write to archive"
            );
        }

        Ok(())
    }

    /// Write a single record (adds newline).
    pub async fn write_record(&mut self, record: &[u8]) -> Result<()> {
        self.write(record).await?;
        self.write(b"\n").await?;

        if let Some(ref state) = self.state {
            state.records_written.fetch_add(1, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Flush buffer to storage. Returns compression stats if data was written.
    pub async fn flush(&mut self) -> Result<Option<FlushStats>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let uncompressed_len = self.buffer.len() as u64;

        // Swap buffer with a pre-allocated replacement (avoids re-alloc on next write cycle)
        let buffer = std::mem::replace(&mut self.buffer, Vec::with_capacity(1024 * 1024));
        let compressor = Arc::clone(&self.compressor);

        let compress_start = std::time::Instant::now();
        let compressed = tokio::task::spawn_blocking(move || compressor.compress(&buffer))
            .await
            .map_err(|e| {
                crate::Error::Compression(format!("compression task join failed: {e}"))
            })??;
        let compression_duration = compress_start.elapsed().as_secs_f64();
        let compressed_len = compressed.len() as u64;

        if let Some(ref state) = self.state {
            self.storage.append(&state.path, &compressed).await?;
            state
                .compressed_bytes
                .fetch_add(compressed_len, Ordering::Relaxed);
            debug!(
                path = %state.path,
                uncompressed = uncompressed_len,
                compressed = compressed_len,
                total_file_size = state.compressed_bytes.load(Ordering::Relaxed),
                "Flushed buffer to storage"
            );
        }

        Ok(Some(FlushStats {
            compressed_bytes: compressed_len,
            uncompressed_bytes: uncompressed_len,
            compression_duration_secs: compression_duration,
        }))
    }

    /// Check if file should be rolled. Returns the trigger reason if so.
    fn should_roll(&self) -> Option<&'static str> {
        let state = self.state.as_ref()?;

        let file_size = state.compressed_bytes.load(Ordering::Relaxed);
        if file_size >= self.policy.max_size_bytes {
            debug!(
                file_size,
                max = self.policy.max_size_bytes,
                "Rolling: final file size exceeded"
            );
            return Some("size");
        }

        let age = Utc::now().signed_duration_since(state.created_at);
        #[allow(clippy::cast_possible_wrap)]
        if age.num_seconds() >= self.policy.max_age_secs as i64 {
            debug!(
                age_secs = age.num_seconds(),
                max = self.policy.max_age_secs,
                "Rolling: age exceeded"
            );
            return Some("age");
        }

        None
    }

    /// Roll to a new file, recording the closed file in `rolls`.
    async fn roll(&mut self, trigger: &'static str) -> Result<()> {
        self.flush().await?;

        if let Some(state) = self.state.take() {
            let compressed_bytes = state.compressed_bytes.load(Ordering::Relaxed);
            self.storage.close(&state.path).await?;
            info!(
                path = %state.path,
                file_size = compressed_bytes,
                uncompressed = state.uncompressed_bytes.load(Ordering::Relaxed),
                records = state.records_written.load(Ordering::Relaxed),
                trigger,
                "Closed archive file"
            );
            self.rolls.push(CloseStats {
                compressed_bytes,
                trigger: Some(trigger),
            });
        }

        self.open_new_file().await?;
        Ok(())
    }

    /// Open a new archive file - iterates through existing sequence numbers
    async fn open_new_file(&mut self) -> Result<()> {
        let now = Utc::now();
        let stem = self.path_stem(&now);

        if self.last_stem.as_deref() != Some(stem.as_str()) {
            self.file_seq = 0;
            self.last_stem = Some(stem);
        }

        let mut retries = 0;
        let path = loop {
            self.file_seq += 1;
            let path = self.generate_path(&now);

            match self.storage.create(&path).await {
                Ok(()) => break path,
                Err(crate::Error::AlreadyExists { .. }) if retries < MAX_OPEN_RETRIES => {
                    retries += 1;
                    info!(path = %path, "Archive file exists - advancing sequence");
                }
                Err(e) => return Err(e),
            }
        };

        self.state = Some(ArchiveState {
            path: path.clone(),
            compressed_bytes: AtomicU64::new(0),
            uncompressed_bytes: AtomicU64::new(0),
            records_written: AtomicU64::new(0),
            created_at: now,
        });
        self.files_opened += 1;

        info!(path = %path, "Opened new archive file");
        Ok(())
    }

    fn path_stem(&self, timestamp: &DateTime<Utc>) -> String {
        let mut path = self.config.path_template.clone();

        path = path.replace("{year}", &timestamp.format("%Y").to_string());
        path = path.replace("{month}", &timestamp.format("%m").to_string());
        path = path.replace("{day}", &timestamp.format("%d").to_string());
        path = path.replace("{hour}", &timestamp.format("%H").to_string());
        path = path.replace("{minute}", &timestamp.format("%M").to_string());
        path = path.replace("{timestamp}", &timestamp.timestamp().to_string());

        path
    }

    fn generate_path(&self, timestamp: &DateTime<Utc>) -> String {
        let path = self
            .path_stem(timestamp)
            .replace("{seq}", &format!("{:04}", self.file_seq));

        let ext = &self.config.file_extension;
        let compression_ext = self.compressor.extension();

        if compression_ext.is_empty() {
            format!("{path}-{:04}.{ext}", self.file_seq)
        } else {
            format!("{path}-{:04}.{ext}.{compression_ext}", self.file_seq)
        }
    }

    /// Close the writer. Returns stats for the closed file (if any was open).
    pub async fn close(&mut self) -> Result<Option<CloseStats>> {
        self.flush().await?;

        let close_stats = if let Some(state) = self.state.take() {
            let compressed_bytes = state.compressed_bytes.load(Ordering::Relaxed);
            self.storage.close(&state.path).await?;
            info!(
                path = %state.path,
                file_size = compressed_bytes,
                uncompressed = state.uncompressed_bytes.load(Ordering::Relaxed),
                records = state.records_written.load(Ordering::Relaxed),
                "Closed archive writer"
            );
            Some(CloseStats {
                compressed_bytes,
                trigger: None,
            })
        } else {
            None
        };

        Ok(close_stats)
    }

    /// Close the open file if the rolling policy has expired, leaving no new
    /// file behind. Returns stats for the closed file.
    ///
    /// The caller drives this from its own timer, because `write` is otherwise
    /// the only thing that consults the policy: a destination that stops
    /// receiving data holds its file open until shutdown, and an active one
    /// overruns the interval by however long it waits for the next batch.
    ///
    /// Closing rather than rolling is the difference that matters for an idle
    /// destination -- `roll` would open a replacement, manufacturing an empty
    /// file every interval, while `write` opens on demand when data returns.
    pub async fn close_if_aged(&mut self) -> Result<Option<CloseStats>> {
        let Some(trigger) = self.should_roll() else {
            return Ok(None);
        };

        Ok(self.close().await?.map(|stats| CloseStats {
            trigger: Some(trigger),
            ..stats
        }))
    }

    /// Expose `generate_path` for testing
    #[cfg(test)]
    pub fn test_generate_path(&self, timestamp: &DateTime<Utc>) -> String {
        self.generate_path(timestamp)
    }

    /// Expose `should_roll` for testing
    #[cfg(test)]
    pub fn test_should_roll(&self) -> Option<&'static str> {
        self.should_roll()
    }

    /// Path of the currently open file for testing
    #[cfg(test)]
    pub fn test_current_path(&self) -> Option<&str> {
        self.state.as_ref().map(|s| s.path.as_str())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::compression::create_compressor;
    use crate::storage::StorageBackend;
    use async_trait::async_trait;
    use chrono::TimeZone;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    /// In-memory storage backend for unit tests (no disk, no network)
    struct MemoryBackend {
        files: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MemoryBackend {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
            }
        }

        fn file_count(&self) -> usize {
            self.files.lock().expect("lock").len()
        }

        fn total_bytes(&self) -> usize {
            self.files
                .lock()
                .expect("lock")
                .values()
                .map(Vec::len)
                .sum()
        }
    }

    #[async_trait]
    impl StorageBackend for MemoryBackend {
        async fn create(&self, path: &str) -> Result<()> {
            let mut files = self.files.lock().expect("lock");
            if files.contains_key(path) {
                return Err(crate::Error::AlreadyExists {
                    path: path.to_string(),
                });
            }
            files.insert(path.to_string(), Vec::new());
            Ok(())
        }

        async fn append(&self, path: &str, data: &[u8]) -> Result<()> {
            self.files
                .lock()
                .expect("lock")
                .get_mut(path)
                .expect("file exists")
                .extend_from_slice(data);
            Ok(())
        }

        async fn close(&self, _path: &str) -> Result<()> {
            Ok(())
        }

        async fn exists(&self, path: &str) -> Result<bool> {
            Ok(self.files.lock().expect("lock").contains_key(path))
        }

        async fn delete(&self, path: &str) -> Result<()> {
            self.files.lock().expect("lock").remove(path);
            Ok(())
        }

        async fn list_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<String>> {
            let mut keys: Vec<String> = self
                .files
                .lock()
                .expect("lock")
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            if let Some(cap) = limit
                && keys.len() > cap
            {
                keys.truncate(cap);
            }
            Ok(keys)
        }

        fn name(&self) -> &'static str {
            "memory"
        }
    }

    fn test_writer(policy: RollingPolicy, codec: &str) -> (ArchiveWriter, Arc<MemoryBackend>) {
        let backend = Arc::new(MemoryBackend::new());
        let writer = test_writer_on(&backend, policy, codec);
        (writer, backend)
    }

    fn test_writer_on(
        backend: &Arc<MemoryBackend>,
        policy: RollingPolicy,
        codec: &str,
    ) -> ArchiveWriter {
        test_writer_with_template(
            backend,
            policy,
            codec,
            "{year}/{month}/{day}/{hour}/archive",
        )
    }

    fn test_writer_with_template(
        backend: &Arc<MemoryBackend>,
        policy: RollingPolicy,
        codec: &str,
        template: &str,
    ) -> ArchiveWriter {
        let config = ArchiveConfig {
            destination: "memory://test".to_string(),
            path_template: template.to_string(),
            file_extension: "jsonl".to_string(),
            ..Default::default()
        };
        let compressor = create_compressor(codec, 0).expect("compressor");
        ArchiveWriter::new(
            config,
            policy,
            compressor,
            Box::new(MemoryBackendRef(Arc::clone(backend))),
        )
    }

    /// Wrapper to use Arc<MemoryBackend> as Box<dyn StorageBackend>
    struct MemoryBackendRef(Arc<MemoryBackend>);

    #[async_trait]
    impl StorageBackend for MemoryBackendRef {
        async fn create(&self, path: &str) -> Result<()> {
            self.0.create(path).await
        }
        async fn append(&self, path: &str, data: &[u8]) -> Result<()> {
            self.0.append(path, data).await
        }
        async fn close(&self, path: &str) -> Result<()> {
            self.0.close(path).await
        }
        async fn exists(&self, path: &str) -> Result<bool> {
            self.0.exists(path).await
        }
        async fn delete(&self, path: &str) -> Result<()> {
            self.0.delete(path).await
        }
        async fn list_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<String>> {
            self.0.list_prefix(prefix, limit).await
        }
        fn name(&self) -> &'static str {
            self.0.name()
        }
    }

    #[test]
    fn test_path_template_expansion() {
        let config = ArchiveConfig {
            path_template: "{year}/{month}/{day}/{hour}/archive".to_string(),
            file_extension: "jsonl".to_string(),
            ..Default::default()
        };
        let compressor = create_compressor("none", 0).expect("compressor");
        let backend = Box::new(MemoryBackend::new());
        let writer = ArchiveWriter::new(config, RollingPolicy::default(), compressor, backend);

        let ts = Utc.with_ymd_and_hms(2026, 3, 15, 14, 30, 0).unwrap();
        let path = writer.test_generate_path(&ts);

        assert!(path.contains("2026"), "year missing: {path}");
        assert!(path.contains("03"), "month missing: {path}");
        assert!(path.contains("15"), "day missing: {path}");
        assert!(path.contains("14"), "hour missing: {path}");
        assert!(path.contains(".jsonl"), "extension missing: {path}");
    }

    #[test]
    fn test_path_template_with_compression_extension() {
        let config = ArchiveConfig {
            path_template: "data/{timestamp}".to_string(),
            file_extension: "jsonl".to_string(),
            ..Default::default()
        };
        let compressor = create_compressor("zstd", 3).expect("compressor");
        let backend = Box::new(MemoryBackend::new());
        let writer = ArchiveWriter::new(config, RollingPolicy::default(), compressor, backend);

        let ts = Utc::now();
        let path = writer.test_generate_path(&ts);

        assert!(path.ends_with(".jsonl.zst"), "should have .zst ext: {path}");
    }

    #[tokio::test]
    async fn test_new_writer_does_not_truncate_existing_file() {
        let policy = RollingPolicy {
            max_size_bytes: 1024 * 1024,
            max_age_secs: 3600,
        };
        let backend = Arc::new(MemoryBackend::new());

        let mut first = test_writer_on(&backend, policy.clone(), "none");
        first.write_record(b"before_restart").await.expect("write");
        first.close().await.expect("close");

        let after_first = backend.total_bytes();
        assert_eq!(backend.file_count(), 1);
        assert!(after_first > 0);

        // Second writer on the same hour (ensure sequence is advanced)
        let mut second = test_writer_on(&backend, policy, "none");
        second.write_record(b"after_restart").await.expect("write");
        second.close().await.expect("close");

        assert_eq!(
            backend.file_count(),
            2,
            "second writer must open a new file, not reopen the first"
        );
        assert!(
            backend.total_bytes() > after_first,
            "first file was truncated: {after_first} bytes before, {} total after",
            backend.total_bytes()
        );
    }

    #[tokio::test]
    async fn test_seq_resets_when_path_stem_changes() {
        let backend = Arc::new(MemoryBackend::new());
        // `{timestamp}` gives a different stem each second
        let mut writer = test_writer_with_template(
            &backend,
            RollingPolicy::default(),
            "none",
            "data/{timestamp}",
        );

        writer.write_record(b"first").await.expect("write");
        let first = writer.test_current_path().expect("open file").to_string();
        assert!(first.ends_with("-0001.jsonl"), "expected -0001: {first}");

        writer.close().await.expect("close");
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        writer.write_record(b"second").await.expect("write");

        let second = writer.test_current_path().expect("open file");
        assert_ne!(first, second, "stem should have changed");
        assert!(
            second.ends_with("-0001.jsonl"),
            "sequence should restart in a new stem, got {second}"
        );
    }

    #[test]
    fn test_should_roll_none_when_no_state() {
        let (writer, _) = test_writer(RollingPolicy::default(), "none");
        assert!(writer.test_should_roll().is_none());
    }

    #[tokio::test]
    async fn test_write_record_creates_file() {
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 1024 * 1024,
                max_age_secs: 3600,
            },
            "none",
        );

        writer.write_record(b"hello world").await.expect("write");
        writer.close().await.expect("close");

        assert_eq!(backend.file_count(), 1);
        assert!(backend.total_bytes() > 0);
    }

    #[tokio::test]
    async fn test_close_without_write() {
        let (mut writer, backend) = test_writer(RollingPolicy::default(), "none");
        writer.close().await.expect("close on empty writer");
        assert_eq!(backend.file_count(), 0);
    }

    #[tokio::test]
    async fn test_flush_on_empty_writer() {
        let (mut writer, _) = test_writer(RollingPolicy::default(), "none");
        writer.flush().await.expect("flush on empty writer");
    }

    #[tokio::test]
    async fn test_write_empty_record() {
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 1024 * 1024,
                max_age_secs: 3600,
            },
            "none",
        );

        writer.write_record(b"").await.expect("write empty");
        writer.close().await.expect("close");

        assert_eq!(backend.file_count(), 1);
    }

    #[tokio::test]
    async fn test_rolling_by_size_in_memory() {
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 100,
                max_age_secs: 3600,
            },
            "none",
        );

        for batch in 0..5 {
            for i in 0..10 {
                let record = format!("record-{batch}-{i}");
                writer.write_record(record.as_bytes()).await.expect("write");
            }
            writer.flush().await.expect("flush");
        }
        writer.close().await.expect("close");

        assert!(
            backend.file_count() >= 3,
            "expected multiple rolled files, got {}",
            backend.file_count()
        );
    }

    #[tokio::test]
    async fn test_flush_with_spawn_blocking_compression() {
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 1024 * 1024,
                max_age_secs: 3600,
            },
            "zstd",
        );
        let data = "x".repeat(10_000);
        writer.write(data.as_bytes()).await.expect("write");
        let stats = writer
            .flush()
            .await
            .expect("flush")
            .expect("should have stats");
        assert!(
            stats.compressed_bytes < stats.uncompressed_bytes,
            "zstd should compress"
        );
        assert!(stats.compression_duration_secs >= 0.0);
        writer.close().await.expect("close");
        assert_eq!(backend.file_count(), 1);
        assert!(backend.total_bytes() > 0);
    }

    #[tokio::test]
    async fn test_multiple_flush_cycles() {
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 1024 * 1024,
                max_age_secs: 3600,
            },
            "lz4",
        );
        for _ in 0..5 {
            writer
                .write(b"repeated data for compression test\n")
                .await
                .expect("write");
            let stats = writer.flush().await.expect("flush");
            assert!(stats.is_some(), "each flush should produce stats");
        }
        writer.close().await.expect("close");
        assert_eq!(backend.file_count(), 1);
    }

    #[tokio::test]
    async fn test_flush_empty_is_noop() {
        let (mut writer, _) = test_writer(RollingPolicy::default(), "zstd");
        let result = writer.flush().await.expect("flush empty");
        assert!(result.is_none(), "flushing empty buffer should return None");
    }

    /// `write_record` writes the record and the newline separately, so both
    /// halves can roll. Reporting one `CloseStats` per call dropped the second
    /// close, and `files_closed_total` undercounted by that much.
    #[tokio::test]
    async fn test_every_roll_in_a_write_record_is_reported() {
        // A zero-second interval makes each half roll, which is otherwise only
        // reachable when a batch straddles the boundary.
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 1024 * 1024,
                max_age_secs: 0,
            },
            "none",
        );

        writer.write_record(b"first").await.expect("write");
        writer.write_record(b"second").await.expect("write");

        let rolls = writer.take_rolls();
        assert_eq!(
            rolls.len(),
            3,
            "one roll on the first newline, two more across the second record"
        );
        assert!(rolls.iter().all(|r| r.trigger == Some("age")));
        assert!(
            writer.take_rolls().is_empty(),
            "draining twice must not double-count"
        );
        assert_eq!(backend.file_count(), rolls.len() + 1);
    }

    /// The rolling policy has to be enforceable from the caller's timer: an
    /// idle destination otherwise holds its file open until shutdown.
    #[tokio::test]
    async fn test_close_if_aged_closes_without_opening_a_replacement() {
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 1024 * 1024,
                max_age_secs: 0,
            },
            "none",
        );

        writer.write_record(b"idle").await.expect("write");
        let before = backend.file_count();

        let stats = writer
            .close_if_aged()
            .await
            .expect("close if aged")
            .expect("a file was open");
        assert_eq!(stats.trigger, Some("age"));
        assert_eq!(
            backend.file_count(),
            before,
            "an idle destination must not be handed an empty replacement file"
        );
        assert!(
            writer.close_if_aged().await.expect("second call").is_none(),
            "nothing is left open to close"
        );
    }

    /// A file still inside its interval is left alone, so the timer does not
    /// shred an active destination into one file per tick.
    #[tokio::test]
    async fn test_close_if_aged_leaves_a_live_file_open() {
        let (mut writer, backend) = test_writer(
            RollingPolicy {
                max_size_bytes: 1024 * 1024,
                max_age_secs: 3600,
            },
            "none",
        );

        writer.write_record(b"live").await.expect("write");

        assert!(
            writer
                .close_if_aged()
                .await
                .expect("close if aged")
                .is_none()
        );
        assert!(writer.test_current_path().is_some());
        assert_eq!(backend.file_count(), 1);
    }

    #[test]
    fn test_unknown_placeholders_names_every_unsupported_token() {
        assert!(unknown_placeholders("{year}/{month}/{day}/{hour}/archive").is_empty());
        assert!(unknown_placeholders("data/{timestamp}-{seq}").is_empty());
        assert!(unknown_placeholders("archive/plain/path").is_empty());

        assert_eq!(
            unknown_placeholders("{topic}/{year}/{date}"),
            vec!["{topic}".to_string(), "{date}".to_string()],
        );
        assert_eq!(
            unknown_placeholders("{topic}/{year}/{topic}"),
            vec!["{topic}".to_string()],
            "a repeated token is reported once"
        );
        assert!(
            unknown_placeholders("{stray/{year}").is_empty(),
            "an unclosed brace shadows nothing"
        );
    }

    /// The list and the substitutions live beside each other precisely so they
    /// cannot drift: every advertised placeholder must actually be replaced.
    #[test]
    fn test_every_advertised_placeholder_is_substituted() {
        let config = ArchiveConfig {
            path_template: PATH_TEMPLATE_PLACEHOLDERS.join("/"),
            file_extension: "jsonl".to_string(),
            ..Default::default()
        };
        let compressor = create_compressor("none", 0).expect("compressor");
        let backend = Box::new(MemoryBackend::new());
        let writer = ArchiveWriter::new(config, RollingPolicy::default(), compressor, backend);

        let path = writer.test_generate_path(&Utc::now());
        assert!(
            !path.contains('{') && !path.contains('}'),
            "an advertised placeholder survived into the key: {path}"
        );
    }
}
