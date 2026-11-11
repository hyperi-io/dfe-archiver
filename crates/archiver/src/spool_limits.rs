// Project:   dfe-archiver
// File:      crates/archiver/src/spool_limits.rs
// Purpose:   Byte limits on the spool volume, from config or the volume's size limit
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HYPERI PTY LIMITED

//! The three byte limits the spool volume carries: the staged bytes past which
//! intake pauses, the free space needed at startup, and the quarantine cap.
//!
//! Each is a `buffer.*` config key. Unset, it is a share of the spool budget
//! when the deployment declares the volume's size limit, so a chart that
//! resizes the volume moves all three. With no declared limit it is the
//! absolute default below.

use dfe_archiver_core::config::BufferConfig;
use scalo::tiered_sink::{TieredSinkConfig, WRITABLE_PATH_LIMITS_ENV};
use std::path::Path;

const GIB: u64 = 1024 * 1024 * 1024;

/// Staged-bytes cap where no volume limit is declared. The last resort of the
/// cascade: the budget share is 8 GiB on the contract's 10Gi volume too.
const DEFAULT_STAGED_BYTES_CAP: u64 = 8 * GIB;

/// Startup free-space floor where no volume limit is declared.
const DEFAULT_MIN_FREE_SPOOL_BYTES: u64 = GIB;

/// Quarantine cap where no volume limit is declared.
const DEFAULT_QUARANTINE_BYTES_CAP: u64 = GIB;

/// Shares of the spool budget, in ninths. scalo's budget is 90% of the
/// volume's size limit, so staged bytes take 80% of the volume and each of the
/// others 10%, and together they stay under the limit past which kubelet
/// evicts the pod.
const STAGED_NINTHS: u64 = 8;
const MIN_FREE_NINTHS: u64 = 1;
const QUARANTINE_NINTHS: u64 = 1;

/// What the spool volume may hold, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolLimits {
    /// Staged bytes past which intake pauses.
    pub staged_bytes_cap: u64,
    /// Free bytes needed at startup when staging holds nothing to upload.
    pub min_free_spool_bytes: u64,
    /// Bytes the quarantine directory may hold.
    pub quarantine_bytes_cap: u64,
}

impl SpoolLimits {
    /// The limits for the spool under `spool_dir`, reading the volume limits
    /// the chart declares from the environment.
    #[must_use]
    pub fn from_env(buffer: &BufferConfig, spool_dir: &Path) -> Self {
        let declared = std::env::var(WRITABLE_PATH_LIMITS_ENV).ok();
        Self::resolve(buffer, spool_dir, declared.as_deref())
    }

    /// Each key that is set, else its share of the spool budget `declared`
    /// gives for `spool_dir`, else its absolute default.
    #[must_use]
    pub fn resolve(buffer: &BufferConfig, spool_dir: &Path, declared: Option<&str>) -> Self {
        let budget =
            declared.and_then(|limits| TieredSinkConfig::new(spool_dir).volume_spool_cap(limits));
        let pick = |set: Option<u64>, ninths: u64, default: u64| {
            set.or_else(|| budget.map(|budget| budget / 9 * ninths))
                .unwrap_or(default)
        };
        Self {
            staged_bytes_cap: pick(
                buffer.staged_bytes_cap,
                STAGED_NINTHS,
                DEFAULT_STAGED_BYTES_CAP,
            ),
            min_free_spool_bytes: pick(
                buffer.min_free_spool_bytes,
                MIN_FREE_NINTHS,
                DEFAULT_MIN_FREE_SPOOL_BYTES,
            ),
            quarantine_bytes_cap: pick(
                buffer.quarantine_bytes_cap,
                QUARANTINE_NINTHS,
                DEFAULT_QUARANTINE_BYTES_CAP,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPOOL: &str = "/var/spool/dfe/archiver";

    fn limits(buffer: &BufferConfig, declared: Option<&str>) -> SpoolLimits {
        SpoolLimits::resolve(buffer, Path::new(SPOOL), declared)
    }

    /// The contract's 10Gi volume gives the same 8 / 1 / 1 GiB the literals
    /// held, and a resized volume moves all three.
    #[test]
    fn a_declared_volume_limit_sets_all_three_as_shares_of_its_budget() {
        let cases = [
            ("10Gi", 8 * GIB, GIB, GIB),
            ("20Gi", 16 * GIB, 2 * GIB, 2 * GIB),
            ("90Gi", 72 * GIB, 9 * GIB, 9 * GIB),
        ];
        for (limit, staged, min_free, quarantine) in cases {
            let declared = format!("{SPOOL}={limit}");
            let got = limits(&BufferConfig::default(), Some(&declared));
            assert_eq!(got.staged_bytes_cap, staged, "{limit}");
            assert_eq!(got.min_free_spool_bytes, min_free, "{limit}");
            assert_eq!(got.quarantine_bytes_cap, quarantine, "{limit}");
        }
    }

    /// The deepest declared path holding the spool is the volume, and a limit
    /// declared for another path says nothing about it.
    #[test]
    fn only_the_volume_holding_the_spool_counts() {
        let other = limits(&BufferConfig::default(), Some("/var/spool/dfe/dlq=40Gi"));
        assert_eq!(other.staged_bytes_cap, DEFAULT_STAGED_BYTES_CAP);
        let both = limits(
            &BufferConfig::default(),
            Some(&format!("/var/spool/dfe/dlq=40Gi,{SPOOL}=20Gi")),
        );
        assert_eq!(both.staged_bytes_cap, 16 * GIB);
    }

    /// Off Kubernetes nothing declares a limit, so the absolute defaults hold,
    /// and so they do for a value that is not a size.
    #[test]
    fn with_no_declared_limit_the_absolute_defaults_apply() {
        for declared in [
            None,
            Some(""),
            Some("no-equals-sign"),
            Some("/var/spool/dfe/archiver=lots"),
        ] {
            let got = limits(&BufferConfig::default(), declared);
            assert_eq!(
                got,
                SpoolLimits {
                    staged_bytes_cap: 8 * GIB,
                    min_free_spool_bytes: GIB,
                    quarantine_bytes_cap: GIB,
                },
                "{declared:?}"
            );
        }
    }

    /// A key that is set wins over the declared volume and the default, one
    /// key at a time.
    #[test]
    fn a_set_key_overrides_its_own_limit_only() {
        let declared = format!("{SPOOL}=10Gi");
        let buffer = BufferConfig {
            staged_bytes_cap: Some(3 * GIB),
            ..BufferConfig::default()
        };
        let got = limits(&buffer, Some(&declared));
        assert_eq!(got.staged_bytes_cap, 3 * GIB);
        assert_eq!(got.min_free_spool_bytes, GIB);
        assert_eq!(got.quarantine_bytes_cap, GIB);

        let buffer = BufferConfig {
            min_free_spool_bytes: Some(0),
            quarantine_bytes_cap: Some(7),
            ..BufferConfig::default()
        };
        let got = limits(&buffer, None);
        assert_eq!(got.staged_bytes_cap, 8 * GIB);
        assert_eq!(got.min_free_spool_bytes, 0);
        assert_eq!(got.quarantine_bytes_cap, 7);
    }
}
