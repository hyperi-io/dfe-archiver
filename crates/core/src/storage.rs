// Project:   dfe-archiver
// File:      crates/core/src/storage.rs
// Purpose:   Storage backend trait definition
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use crate::Result;
use async_trait::async_trait;

/// A completed file on local disk, waiting to reach the store.
#[async_trait]
pub trait PendingUpload: Send + Sync {
    /// The file's path under the destination.
    fn path(&self) -> &str;

    /// The file's size in bytes.
    fn size(&self) -> u64;

    /// Upload the whole file once, removing the local copy once the store
    /// holds it. Safe to call again after an error.
    async fn attempt(&self) -> Result<()>;

    /// The `index`th block appended to the file, exactly as it was appended,
    /// or `None` past the last one. The writer appends one compressed flush
    /// per block, so each block decompresses on its own.
    async fn block(&self, index: usize) -> Result<Option<Vec<u8>>>;

    /// Give the file up and remove the local copy.
    async fn discard(&self);

    /// Give the file up and move the local copy aside for an operator,
    /// instead of removing it. `true` when there was a copy to move.
    async fn quarantine(&self) -> bool {
        self.discard().await;
        false
    }
}

/// A complete staged file a previous process left, found at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredFile {
    /// Where the local copy is.
    pub local: std::path::PathBuf,
    /// The file's path under the destination.
    pub path: String,
    /// The local copy's size in bytes.
    pub size: u64,
    /// The length of each block appended, in order.
    pub blocks: Vec<u64>,
}

/// Where a closed file stands.
pub enum Closed {
    /// Durable where it was written: its records are archived.
    Durable,
    /// Complete on local disk, and archived once an upload attempt succeeds.
    Pending(Box<dyn PendingUpload>),
}

impl std::fmt::Debug for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Durable => f.write_str("Durable"),
            Self::Pending(upload) => f
                .debug_struct("Pending")
                .field("path", &upload.path())
                .field("size", &upload.size())
                .finish(),
        }
    }
}

/// Storage backend trait
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Create a new file/object.
    ///
    /// A local file refuses a path that already holds one. An object is
    /// staged on local disk and never checked against the store, so keeping
    /// two writers' paths apart is the caller's job.
    async fn create(&self, path: &str) -> Result<()>;

    /// Append data to existing file/object
    async fn append(&self, path: &str, data: &[u8]) -> Result<()>;

    /// Cut an open file back to its first `len` bytes, undoing an append that
    /// failed part way, so the file ends on its last whole block again.
    ///
    /// # Errors
    /// The default cannot cut a file back, and the writer then gives it up.
    async fn truncate(&self, path: &str, _len: u64) -> Result<()> {
        Err(crate::Error::storage(format!(
            "{}: {path} cannot be cut back",
            self.name()
        )))
    }

    /// Complete the file: [`Closed::Durable`] once it survives a crash of the
    /// process or the node, [`Closed::Pending`] when it still has to reach
    /// the store. A record's offset is released only once its file is durable
    /// in the store.
    async fn close(&self, path: &str) -> Result<Closed>;

    /// Give up a file whose write failed, removing what it left behind.
    async fn abort(&self, _path: &str) {}

    /// Take over a complete staged file a previous process left, to upload it.
    ///
    /// # Errors
    /// Hands the file back when this backend stages nothing, or the file's
    /// path is not one it would write.
    fn adopt(
        &self,
        file: RecoveredFile,
    ) -> std::result::Result<Box<dyn PendingUpload>, RecoveredFile> {
        Err(file)
    }

    /// Check if path exists
    async fn exists(&self, path: &str) -> Result<bool>;

    /// Delete file/object
    async fn delete(&self, path: &str) -> Result<()>;

    /// List objects under a prefix.
    ///
    /// `limit`: when `Some(n)`, return at most `n` entries; the
    /// implementation should stop walking / paginating as soon as the
    /// limit is met to avoid materialising large result sets in memory.
    /// `None` returns every match (use only for small prefixes -- high-
    /// cardinality prefixes can OOM the process).
    async fn list_prefix(&self, prefix: &str, limit: Option<usize>) -> Result<Vec<String>>;

    /// Get backend name
    fn name(&self) -> &'static str;
}

/// Refuse an archive path that could name something outside its destination:
/// an absolute path, or one with a `.` or `..` segment.
///
/// Every backend checks the same rule, so a path one destination refuses is
/// refused by all of them, and a path one accepts is named alike by all of them.
///
/// # Errors
/// [`crate::Error::Refused`], because the same path is refused on every retry.
pub fn confine(path: &str) -> Result<()> {
    let relative_step = path
        .split('/')
        .any(|segment| segment == "." || segment == "..");
    if path.starts_with('/') || relative_step {
        return Err(crate::Error::refused(format!(
            "{path} is not a path under the archive destination"
        )));
    }
    Ok(())
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
#[allow(clippy::expect_used, clippy::panic)]
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
        async fn close(&self, _path: &str) -> Result<Closed> {
            Ok(Closed::Durable)
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
        async fn close(&self, _path: &str) -> Result<Closed> {
            Ok(Closed::Durable)
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

    #[test]
    fn a_path_that_steps_out_of_the_destination_is_refused() {
        for path in [
            "../x",
            "a/../../x",
            "a/..",
            "./a",
            "a/./b",
            "/etc/passwd",
            "..",
        ] {
            let err = confine(path).expect_err(path);
            assert!(err.is_refused(), "{path}: {err:?}");
        }
        for path in [
            "",
            "events/2026/09",
            "events/=2E=2E/a",
            "a/...",
            "a/.hidden",
        ] {
            confine(path).unwrap_or_else(|e| panic!("{path}: {e}"));
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
