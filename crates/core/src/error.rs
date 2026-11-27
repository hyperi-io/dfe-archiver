// Project:   dfe-archiver
// File:      crates/core/src/error.rs
// Purpose:   Error types for the archiver
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

use thiserror::Error;

/// Result type alias for archiver operations
pub type Result<T> = std::result::Result<T, Error>;

/// Boxed dynamic error source, kept so `e.source()` walks reach the underlying
/// scalo / `object_store` / rdkafka diagnostic.
pub type BoxSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// `: <source>` when there is one. Every log line and dead letter formats the
/// error with `%e`, so a cause left out of `Display` reaches no operator.
fn cause(source: Option<&BoxSource>) -> String {
    source.map_or_default(|e| format!(": {e}"))
}

/// Main error type for the archiver
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// Inbound transport error, on the bus or the Push listener. `source`
    /// carries the underlying `scalo::transport::TransportError` (or rdkafka
    /// error) when the failure originated outside this crate.
    #[error("transport error: {message}{}", cause(.source.as_ref()))]
    Transport {
        message: String,
        #[source]
        source: Option<BoxSource>,
    },

    /// Storage backend error. `source` carries the underlying
    /// `object_store::Error` or filesystem error when available.
    #[error("storage error: {message}{}", cause(.source.as_ref()))]
    Storage {
        message: String,
        #[source]
        source: Option<BoxSource>,
    },

    /// The store refused this object for good: retrying the same object gets
    /// the same answer, so its records are dropped rather than retried.
    #[error("refused by the store: {message}{}", cause(.source.as_ref()))]
    Refused {
        message: String,
        #[source]
        source: Option<BoxSource>,
    },

    /// A staged file's local copy cannot be read back -- gone, not readable,
    /// or shorter than what was written -- which no retry of its upload changes.
    #[error("staged copy unreadable: {message}{}", cause(.source.as_ref()))]
    Unreadable {
        message: String,
        #[source]
        source: Option<BoxSource>,
    },

    /// Already exists (file create was overwriting)
    #[error("already exists: {path}")]
    AlreadyExists { path: String },

    /// Compression error
    #[error("compression error: {0}")]
    Compression(String),

    /// IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Routing error (cannot determine destination)
    #[error("routing error: {0}")]
    Routing(String),

    /// The record nests deeper than routing will parse, so the same bytes are
    /// refused every time. The message is the reason its dead letter carries.
    #[error("payload nesting exceeds the maximum parse depth of {max}")]
    TooDeep { max: usize },

    /// Records neither archived nor dead-lettered hold the commit below them,
    /// and only a restart or rebalance reads them again.
    #[error(
        "{records} records were neither archived nor dead-lettered and hold the commit below them"
    )]
    Withheld { records: u64 },

    /// Shutdown requested
    #[error("shutdown requested")]
    Shutdown,
}

impl Error {
    /// Construct a transport error without an underlying source.
    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport {
            message: message.into(),
            source: None,
        }
    }

    /// Construct a transport error wrapping the underlying error chain.
    pub fn transport_with(message: impl Into<String>, source: impl Into<BoxSource>) -> Self {
        Self::Transport {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Construct a Storage error without an underlying source.
    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
            source: None,
        }
    }

    /// Construct a Storage error wrapping the underlying error chain.
    pub fn storage_with(message: impl Into<String>, source: impl Into<BoxSource>) -> Self {
        Self::Storage {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Construct a refusal without an underlying source.
    pub fn refused(message: impl Into<String>) -> Self {
        Self::Refused {
            message: message.into(),
            source: None,
        }
    }

    /// Construct a refusal wrapping the underlying error chain.
    pub fn refused_with(message: impl Into<String>, source: impl Into<BoxSource>) -> Self {
        Self::Refused {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Construct an unreadable-copy error wrapping the underlying error chain.
    pub fn unreadable_with(message: impl Into<String>, source: impl Into<BoxSource>) -> Self {
        Self::Unreadable {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Whether the store refused the object for good.
    #[must_use]
    pub fn is_refused(&self) -> bool {
        matches!(self, Self::Refused { .. })
    }

    /// Whether a staged file's local copy cannot be read back.
    #[must_use]
    pub fn is_unreadable(&self) -> bool {
        matches!(self, Self::Unreadable { .. })
    }

    /// Whether a write failed for a reason no retry of the same records
    /// changes: the store refused their object, or the writer could not encode
    /// them. Every other failure, a full or failing disk included, can clear.
    #[must_use]
    pub fn is_permanent(&self) -> bool {
        matches!(self, Self::Refused { .. } | Self::Compression(_))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_formats() {
        let e = Error::Config("bad value".to_string());
        assert_eq!(format!("{e}"), "configuration error: bad value");

        let e = Error::transport("connection refused");
        assert_eq!(format!("{e}"), "transport error: connection refused");

        let e = Error::storage("permission denied");
        assert_eq!(format!("{e}"), "storage error: permission denied");

        let e = Error::Shutdown;
        assert_eq!(format!("{e}"), "shutdown requested");

        let e = Error::Withheld { records: 20 };
        assert_eq!(
            format!("{e}"),
            "20 records were neither archived nor dead-lettered and hold the commit below them"
        );

        let e = Error::TooDeep { max: 64 };
        assert_eq!(
            format!("{e}"),
            "payload nesting exceeds the maximum parse depth of 64"
        );
    }

    /// Only a refusal or an encoding failure is the same on every retry, so
    /// only those stop a write retrying.
    #[test]
    fn only_a_refusal_or_an_encoding_failure_is_permanent() {
        assert!(Error::refused("key too long").is_refused());
        assert!(!Error::storage("503").is_refused());

        assert!(Error::refused("key too long").is_permanent());
        assert!(Error::Compression("cannot encode".into()).is_permanent());
        for can_clear in [
            Error::Io(std::io::Error::from(std::io::ErrorKind::StorageFull)),
            Error::Io(std::io::Error::from_raw_os_error(5)),
            Error::Io(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            Error::Io(std::io::Error::from(std::io::ErrorKind::TimedOut)),
            Error::Io(std::io::Error::from(std::io::ErrorKind::NotADirectory)),
            Error::storage("staging failed"),
            Error::transport("timeout"),
            Error::Withheld { records: 1 },
        ] {
            assert!(!can_clear.is_permanent(), "{can_clear}");
        }
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::other("disk full");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    /// Every log line formats the error with `%e`, so the cause has to be in
    /// `Display`: "s3: list failed" alone hides a refused connection, a 403 or
    /// a clock-skew rejection behind the same words.
    #[test]
    fn a_wrapped_error_names_its_cause() {
        use std::error::Error as _;

        let cause = || std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "broker down");
        let cases = [
            (
                Error::transport_with("recv failed", cause()),
                "transport error: recv failed: broker down",
            ),
            (
                Error::storage_with("s3: list failed", cause()),
                "storage error: s3: list failed: broker down",
            ),
            (
                Error::refused_with("s3: put failed", cause()),
                "refused by the store: s3: put failed: broker down",
            ),
            (
                Error::unreadable_with("reading the staged copy failed", cause()),
                "staged copy unreadable: reading the staged copy failed: broker down",
            ),
            (Error::storage("no cause"), "storage error: no cause"),
        ];
        for (err, display) in cases {
            assert_eq!(err.to_string(), display);
        }

        let err = Error::transport_with("recv failed", cause());
        let src = err.source().expect("source set");
        assert_eq!(src.to_string(), "broker down");
    }

    #[test]
    fn test_storage_constructor_no_source() {
        use std::error::Error as _;

        let err = Error::storage("disk full");
        assert_eq!(format!("{err}"), "storage error: disk full");
        assert!(err.source().is_none());
    }
}
