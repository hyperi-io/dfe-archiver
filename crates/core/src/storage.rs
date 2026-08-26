// Project:   dfe-archiver
// File:      crates/core/src/storage.rs
// Purpose:   Storage backend trait definition
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::Result;
use async_trait::async_trait;

/// Storage backend trait
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Create a new file/object (does not overwrite existing)
    async fn create(&self, path: &str) -> Result<()>;

    /// Append data to existing file/object
    async fn append(&self, path: &str, data: &[u8]) -> Result<()>;

    /// Close file/object (finalise upload)
    async fn close(&self, path: &str) -> Result<()>;

    /// Check if path exists
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Delete file/object
    async fn delete(&self, path: &str) -> Result<()>;

    /// List objects under a prefix.
    ///
    /// `limit`: when `Some(n)`, return at most `n` entries; the
    /// implementation should stop walking / paginating as soon as the
    /// limit is met to avoid materialising large result sets in memory.
    /// `None` returns every match (use only for small prefixes — high-
    /// cardinality prefixes can OOM the process).
    async fn list_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<String>>;

    /// Get backend name
    fn name(&self) -> &'static str;
}
