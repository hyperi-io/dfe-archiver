// Project:   dfe-archiver
// File:      crates/archiver/tests/integration/workspace_manifests.rs
// Purpose:   Pin the manifest keys the release stamp and the publish guard need
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! The workspace manifests carry two release-critical keys.
//!
//! hyperi-ci stamps the release version into the ROOT `Cargo.toml`, under
//! `[package]` and `[workspace.package]`, and its replacement never inserts a
//! key that is absent. This is a virtual workspace with the binary in
//! `crates/archiver`, so without a `version` under `[workspace.package]` for the
//! members to inherit the stamp matches nothing and the binary reports whatever
//! literal the crate carries.
//!
//! The second key is `publish = false`: these are BUSL-1.1 crates with
//! registry-shaped names, and nothing here goes to a public registry.

/// Read one workspace manifest. `CARGO_MANIFEST_DIR` is `crates/archiver`, so
/// the workspace root is two levels up.
fn manifest(relative: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Extract a key from one `[section]`, ignoring comments.
///
/// Hand-scanned rather than parsed, matching `test_runner_config.rs`: a TOML
/// dev-dependency for these assertions would be a bigger maintenance surface
/// than the scan.
fn section_key(toml: &str, section: &str, key: &str) -> Option<String> {
    let header = format!("[{section}]");
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

const MEMBER_MANIFESTS: [&str; 3] = [
    "crates/core/Cargo.toml",
    "crates/io/Cargo.toml",
    "crates/archiver/Cargo.toml",
];

/// Without this key the release stamp is a no-op on this repo.
#[test]
fn test_workspace_package_carries_a_version_for_the_stamp() {
    let root = manifest("Cargo.toml");
    let version = section_key(&root, "workspace.package", "version");

    assert!(
        version.is_some_and(|v| v.starts_with('"')),
        "[workspace.package] needs a literal `version` for hyperi-ci to stamp; \
         without one the members keep their own and the binary misreports its \
         release"
    );
}

/// Every member inherits that version, so no crate can drift from the stamp.
#[test]
fn test_member_crates_inherit_the_workspace_version() {
    for member in MEMBER_MANIFESTS {
        let toml = manifest(member);
        assert_eq!(
            section_key(&toml, "package", "version.workspace").as_deref(),
            Some("true"),
            "{member} must set `version.workspace = true`"
        );
        assert_eq!(
            section_key(&toml, "package", "version"),
            None,
            "{member} carries its own version literal, which the root stamp \
             never reaches"
        );
    }
}

/// The version the binary reports is the one the root manifest holds.
#[test]
fn test_the_binary_reports_the_workspace_version() {
    let root = manifest("Cargo.toml");
    let declared = section_key(&root, "workspace.package", "version")
        .expect("[workspace.package] version")
        .trim_matches('"')
        .to_string();

    assert_eq!(
        env!("CARGO_PKG_VERSION"),
        declared,
        "the binary's CARGO_PKG_VERSION must be the root manifest's version, \
         which is the only value hyperi-ci stamps"
    );
}

/// `publish = false` is the one-line guard against a crates.io push.
#[test]
fn test_member_crates_are_not_publishable() {
    for member in MEMBER_MANIFESTS {
        let toml = manifest(member);
        assert_eq!(
            section_key(&toml, "package", "publish").as_deref(),
            Some("false"),
            "{member} must set `publish = false`: these are BUSL-1.1 crates \
             with registry-shaped names"
        );
    }
}
