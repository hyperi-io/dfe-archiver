// Project:   dfe-archiver
// File:      crates/core/src/error.rs
// Purpose:   Error types for the archiver
// Language:  Rust
//
// License:      FSL-1.1-ALv2
// Copyright:    (c) 2026 HyperI Pty Ltd

use thiserror::Error;

/// Result type alias for archiver operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for the archiver
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration error
    #[error("configuration error: {0}")]
    Config(String),

    /// Kafka transport error
    #[error("kafka error: {0}")]
    Kafka(String),

    /// Storage backend error
    #[error("storage error: {0}")]
    Storage(String),

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
            Self::Kafka(_) | Self::Storage(_) | Self::Runtime(_) | Self::BufferOverflow { .. } => {
                ErrorCategory::Transient
            }

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

        let e = Error::Kafka("connection refused".to_string());
        assert_eq!(format!("{e}"), "kafka error: connection refused");

        let e = Error::Storage("permission denied".to_string());
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
        assert_eq!(
            Error::Kafka("timeout".into()).category(),
            ErrorCategory::Transient
        );
        assert_eq!(
            Error::Storage("network".into()).category(),
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
        assert!(Error::Kafka("timeout".into()).is_retryable());
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
}
