// Project:   dfe-archiver
// File:      crates/core/src/error.rs
// Purpose:   Error types for the archiver
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use thiserror::Error;

/// Result type alias for archiver operations
pub type Result<T> = std::result::Result<T, Error>;

/// Boxed dynamic error source — preserves the original error chain so
/// `tracing::error!(error = %e, ...)` plus `e.source()` walks reach the
/// underlying rustlib / `object_store` / rdkafka diagnostic.
pub type BoxSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Main error type for the archiver
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// Kafka transport error. `source` carries the underlying
    /// `hyperi_rustlib::transport::TransportError` (or rdkafka error) when
    /// the failure originated outside this crate.
    #[error("kafka error: {message}")]
    Kafka {
        message: String,
        #[source]
        source: Option<BoxSource>,
    },

    /// Storage backend error. `source` carries the underlying
    /// `object_store::Error`, `aws_sdk_s3` error, or filesystem error when
    /// available.
    #[error("storage error: {message}")]
    Storage {
        message: String,
        #[source]
        source: Option<BoxSource>,
    },

    /// Compression error
    #[error("compression error: {0}")]
    Compression(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Buffer overflow (memory pressure)
    #[error("buffer overflow: {message}")]
    BufferOverflow { message: String },

    /// Routing error (cannot determine destination)
    #[error("routing error: {0}")]
    Routing(String),

    /// Runtime error
    #[error("runtime error: {0}")]
    Runtime(String),

    /// Shutdown requested
    #[error("shutdown requested")]
    Shutdown,
}

impl Error {
    /// Construct a Kafka error without an underlying source.
    pub fn kafka(message: impl Into<String>) -> Self {
        Self::Kafka {
            message: message.into(),
            source: None,
        }
    }

    /// Construct a Kafka error wrapping the underlying error chain.
    pub fn kafka_with(message: impl Into<String>, source: impl Into<BoxSource>) -> Self {
        Self::Kafka {
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
}

/// Error category for retry/DLQ decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Transient error - retry with backoff (network, timeout)
    Transient,
    /// Data error - send to DLQ (parse failure, invalid format)
    Data,
    /// Fatal error - fail immediately (auth, config)
    Fatal,
}

impl Error {
    /// Categorise error for retry/DLQ decisions
    #[must_use]
    pub fn category(&self) -> ErrorCategory {
        match self {
            // Transient - retry
            Self::Kafka { .. }
            | Self::Storage { .. }
            | Self::Runtime(_)
            | Self::BufferOverflow { .. } => ErrorCategory::Transient,

            // Data - DLQ
            Self::Serialization(_) | Self::Routing(_) | Self::Compression(_) => ErrorCategory::Data,

            // Fatal - fail
            Self::Config(_) | Self::Shutdown => ErrorCategory::Fatal,

            // Context-dependent
            Self::Io(e) => {
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::PermissionDenied
                {
                    ErrorCategory::Fatal
                } else {
                    ErrorCategory::Transient
                }
            }
        }
    }

    /// Check if error is retryable
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.category() == ErrorCategory::Transient
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_formats() {
        let e = Error::Config("bad value".to_string());
        assert_eq!(format!("{e}"), "configuration error: bad value");

        let e = Error::kafka("connection refused");
        assert_eq!(format!("{e}"), "kafka error: connection refused");

        let e = Error::storage("permission denied");
        assert_eq!(format!("{e}"), "storage error: permission denied");

        let e = Error::Shutdown;
        assert_eq!(format!("{e}"), "shutdown requested");

        let e = Error::BufferOverflow {
            message: "exceeded 64MB".to_string(),
        };
        assert_eq!(format!("{e}"), "buffer overflow: exceeded 64MB");
    }

    #[test]
    fn test_error_categories() {
        assert_eq!(Error::kafka("timeout").category(), ErrorCategory::Transient);
        assert_eq!(
            Error::storage("network").category(),
            ErrorCategory::Transient
        );
        assert_eq!(
            Error::Runtime("panic".into()).category(),
            ErrorCategory::Transient
        );
        assert_eq!(
            Error::BufferOverflow {
                message: "oom".into()
            }
            .category(),
            ErrorCategory::Transient
        );

        assert_eq!(
            Error::Routing("no field".into()).category(),
            ErrorCategory::Data
        );
        assert_eq!(
            Error::Compression("corrupt".into()).category(),
            ErrorCategory::Data
        );

        assert_eq!(
            Error::Config("missing".into()).category(),
            ErrorCategory::Fatal
        );
        assert_eq!(Error::Shutdown.category(), ErrorCategory::Fatal);
    }

    #[test]
    fn test_io_error_categories() {
        let e = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"));
        assert_eq!(e.category(), ErrorCategory::Fatal);

        let e = Error::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "denied",
        ));
        assert_eq!(e.category(), ErrorCategory::Fatal);

        let e = Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert_eq!(e.category(), ErrorCategory::Transient);
    }

    #[test]
    fn test_is_retryable() {
        assert!(Error::kafka("timeout").is_retryable());
        assert!(!Error::Config("bad".into()).is_retryable());
        assert!(!Error::Routing("no dest".into()).is_retryable());
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::other("disk full");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::Serialization(_)));
    }

    #[test]
    fn test_kafka_with_preserves_source_chain() {
        use std::error::Error as _;

        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "broker down");
        let err = Error::kafka_with("recv failed", inner);

        // Top-level message is the wrapper text only.
        assert_eq!(format!("{err}"), "kafka error: recv failed");

        // source() walks reach the underlying io::Error.
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
