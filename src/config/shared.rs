// Project:   dfe-archiver
// File:      src/config/shared.rs
// Purpose:   Thread-safe shared configuration with hot-reload support
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

use super::Config;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

/// Thread-safe shared configuration with version tracking
///
/// Supports hot-reload: components can subscribe to config changes
/// via the watch channel and react accordingly.
#[derive(Clone)]
pub struct SharedConfig {
    inner: Arc<RwLock<Config>>,
    version: Arc<AtomicU64>,
    watch_tx: Arc<watch::Sender<u64>>,
    watch_rx: watch::Receiver<u64>,
}

impl SharedConfig {
    /// Create new shared configuration
    #[must_use]
    pub fn new(config: Config) -> Self {
        let (watch_tx, watch_rx) = watch::channel(0);

        Self {
            inner: Arc::new(RwLock::new(config)),
            version: Arc::new(AtomicU64::new(0)),
            watch_tx: Arc::new(watch_tx),
            watch_rx,
        }
    }

    /// Get current configuration (read lock)
    #[must_use]
    pub fn get(&self) -> Config {
        self.inner.read().clone()
    }

    /// Get current version number
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Update configuration (triggers version bump and notification)
    pub fn update(&self, new_config: Config) {
        {
            let mut guard = self.inner.write();
            *guard = new_config;
        }

        let new_version = self.version.fetch_add(1, Ordering::AcqRel) + 1;

        // Notify subscribers (ignore if no receivers)
        let _ = self.watch_tx.send(new_version);
    }

    /// Subscribe to configuration changes
    ///
    /// Returns a receiver that yields the new version number on each change.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.watch_rx.clone()
    }

    /// Access configuration with a closure (avoids cloning for read-only access)
    pub fn with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Config) -> R,
    {
        let guard = self.inner.read();
        f(&guard)
    }
}

impl std::fmt::Debug for SharedConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedConfig")
            .field("version", &self.version())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_config_version_increments() {
        let config = Config::default();
        let shared = SharedConfig::new(config.clone());

        assert_eq!(shared.version(), 0);

        shared.update(config.clone());
        assert_eq!(shared.version(), 1);

        shared.update(config);
        assert_eq!(shared.version(), 2);
    }

    #[tokio::test]
    async fn test_shared_config_subscription() {
        let config = Config::default();
        let shared = SharedConfig::new(config.clone());

        let mut rx = shared.subscribe();

        // Update config
        shared.update(config);

        // Should receive notification
        rx.changed().await.expect("should receive change");
        assert_eq!(*rx.borrow(), 1);
    }
}
