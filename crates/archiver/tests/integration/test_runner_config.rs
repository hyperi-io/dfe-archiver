// Project:   dfe-archiver
// File:      crates/archiver/tests/integration/test_runner_config.rs
// Purpose:   Pin the nextest profile CI actually uses to zero retries
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! The test runner must not retry.
//!
//! `hyperi-ci`'s Rust handler runs a bare `cargo nextest run` -- it never passes
//! `--profile ci` and never sets `NEXTEST_PROFILE`. CI therefore runs under
//! `[profile.default]`, so a `retries` value there is live in CI, not just
//! locally, and `[profile.ci]`'s `retries = 0` never applies.
//!
//! A retry turns a real intermittent defect into a green run: the failure is
//! reported nowhere, the second attempt passes, and the suite claims the code
//! works. Flaky tests get fixed or deleted, never retried.

/// Read the workspace-root `.config/nextest.toml` (`CARGO_MANIFEST_DIR` is
/// `crates/archiver`, so the workspace root is two levels up).
fn nextest_config() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.config/nextest.toml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Extract a key from one `[profile.<name>]` section, ignoring comments.
///
/// Hand-scanned rather than parsed: the file is 20 lines of flat key/value
/// pairs, so a TOML dev-dependency for this one assertion would be a bigger
/// maintenance surface than the scan.
fn profile_key(toml: &str, profile: &str, key: &str) -> Option<String> {
    let header = format!("[profile.{profile}]");
    let mut in_section = false;
    for line in toml.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_section = line == header;
            continue;
        }
        if !in_section || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            return Some(v.split('#').next().unwrap_or(v).trim().to_string());
        }
    }
    None
}

/// `[profile.default]` is the profile CI runs under, so its retry count is the
/// one that matters. Absent means nextest's own default of 0, which is fine.
#[test]
fn test_default_nextest_profile_does_not_retry() {
    let toml = nextest_config();
    let retries = profile_key(&toml, "default", "retries");

    assert!(
        matches!(retries.as_deref(), None | Some("0")),
        "[profile.default] retries must be 0: hyperi-ci runs a bare \
         `cargo nextest run`, so this is the profile CI uses and a retry here \
         hides a real failure behind a green run. Found: {retries:?}"
    );
}

/// If the `ci` profile exists it must not be more permissive than `default`,
/// so that selecting it explicitly can never re-introduce retries.
#[test]
fn test_ci_nextest_profile_does_not_retry() {
    let toml = nextest_config();
    let retries = profile_key(&toml, "ci", "retries");

    assert!(
        matches!(retries.as_deref(), None | Some("0")),
        "[profile.ci] retries must be 0, found: {retries:?}"
    );
}
