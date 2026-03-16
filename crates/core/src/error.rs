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
