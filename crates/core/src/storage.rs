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

/// Prove the archive sink answers, with one listing capped at a single entry.
///
/// A bucket that does not exist, an endpoint nothing is listening on, or a
/// credential that cannot read the prefix all fail here rather than at the
/// first roll, hours after startup.
///
/// # Errors
/// Returns the backend's own error when the listing cannot be served.
pub async fn probe_sink(backend: &dyn StorageBackend) -> Result<()> {
    backend.list_prefix("", Some(1)).await.map(|_| ())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::Error;

    /// Answers every call from memory, so the probe sees a reachable sink.
    struct ReachableBackend;

    #[async_trait]
    impl StorageBackend for ReachableBackend {
        async fn create(&self, _path: &str) -> Result<()> {
            Ok(())
        }
        async fn append(&self, _path: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&self, _path: &str) -> Result<()> {
            Ok(())
        }
        async fn exists(&self, _path: &str) -> Result<bool> {
            Ok(false)
        }
        async fn delete(&self, _path: &str) -> Result<()> {
            Ok(())
        }
        async fn list_prefix(&self, _prefix: &str, _limit: Option<usize>) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn name(&self) -> &'static str {
            "reachable"
        }
    }

    /// Fails the listing the way a missing bucket or a refused endpoint does.
    struct UnreachableBackend;

    #[async_trait]
    impl StorageBackend for UnreachableBackend {
        async fn create(&self, _path: &str) -> Result<()> {
            Ok(())
        }
        async fn append(&self, _path: &str, _data: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn close(&self, _path: &str) -> Result<()> {
            Ok(())
        }
        async fn exists(&self, _path: &str) -> Result<bool> {
            Ok(false)
        }
        async fn delete(&self, _path: &str) -> Result<()> {
            Ok(())
        }
        async fn list_prefix(&self, _prefix: &str, _limit: Option<usize>) -> Result<Vec<String>> {
            Err(Error::storage("s3: list failed for prefix archive/"))
        }
        fn name(&self) -> &'static str {
            "unreachable"
        }
    }

    #[tokio::test]
    async fn probe_passes_when_the_sink_answers() {
        probe_sink(&ReachableBackend)
            .await
            .expect("a backend that lists must probe clean");
    }

    #[tokio::test]
    async fn probe_fails_when_the_sink_does_not_answer() {
        let err = probe_sink(&UnreachableBackend)
            .await
            .expect_err("a backend that cannot list must fail the probe");
        assert!(
            matches!(err, Error::Storage { .. }),
            "expected the backend's own storage error, got {err:?}"
        );
    }
}
