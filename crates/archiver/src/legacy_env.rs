// Project:   dfe-archiver
// File:      crates/archiver/src/legacy_env.rs
// Purpose:   Copy the pre-DFE_ARCHIVER_ env names to their new names at startup
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver_core::config::{ENV_PREFIX, LEGACY_FLAT_ENV};
use std::collections::HashSet;
use std::sync::Once;

/// The old prefix of the scalo cascade and the flat readers, copied whole
/// because the cascade reads any `<PREFIX>_<SECTION>__<KEY>` a deployment sets.
const LEGACY_PREFIX: &str = "ARCHIVER_";

/// The legacy variables found in an environment.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct LegacyEnv {
    /// `(new name, value)` for each legacy variable whose new name is not set.
    pub copies: Vec<(String, String)>,
    /// Every legacy name present, sorted.
    pub found: Vec<String>,
}

fn dotenv_text(copies: &[(String, String)]) -> String {
    let mut text = String::new();
    for (name, value) in copies {
        text.push_str(name);
        text.push_str("=\"");
        for c in value.chars() {
            match c {
                '\\' | '"' | '$' => {
                    text.push('\\');
                    text.push(c);
                }
                '\n' => text.push_str("\\n"),
                _ => text.push(c),
            }
        }
        text.push_str("\"\n");
    }
    text
}

fn process_env() -> Vec<(String, String)> {
    std::env::vars_os()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .collect()
}

fn replacement(old: &str) -> Option<String> {
    if !old.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return None;
    }
    match old.strip_prefix(LEGACY_PREFIX) {
        Some(rest) if !rest.is_empty() => Some(format!("{ENV_PREFIX}_{rest}")),
        _ if LEGACY_FLAT_ENV.contains(&old) => Some(format!("{ENV_PREFIX}_{old}")),
        _ => None,
    }
}

/// Sort the legacy variables in `vars` into those to copy and those present.
#[must_use]
pub fn plan(vars: &[(String, String)]) -> LegacyEnv {
    let present: HashSet<&str> = vars.iter().map(|(name, _)| name.as_str()).collect();
    let mut legacy = LegacyEnv::default();
    for (old, value) in vars {
        let Some(new) = replacement(old) else {
            continue;
        };
        legacy.found.push(old.clone());
        if !present.contains(new.as_str()) {
            legacy.copies.push((new, value.clone()));
        }
    }
    legacy.found.sort();
    legacy.copies.sort();
    legacy
}

/// Copy each legacy variable to its `DFE_ARCHIVER_` name when that name is unset.
///
/// Runs before any thread starts, since the scalo cascade and memory guard read
/// only the new names. The workspace forbids `unsafe`, so the copy goes through
/// `dotenvy`, which sets only variables that are not already set.
///
/// # Errors
///
/// Returns the `dotenvy` error when a copy cannot be applied, leaving some
/// legacy names unread.
pub fn adopt_legacy_names() -> Result<(), dotenvy::Error> {
    let copies = plan(&process_env()).copies;
    dotenvy::from_read(dotenv_text(&copies).as_bytes())
}

/// Log one warning naming the legacy variables set, never their values.
///
/// Only the first call warns, because the config reloader loads again.
pub fn warn_on_legacy_names() {
    static WARNED: Once = Once::new();
    let found = plan(&process_env()).found;
    if !found.is_empty() {
        WARNED.call_once(|| {
            tracing::warn!(
                variables = %found.join(","),
                "deprecated environment variable names in use: rename ARCHIVER_X to DFE_ARCHIVER_X, and KAFKA_X, DLQ_X, S3_X to DFE_ARCHIVER_KAFKA_X, DFE_ARCHIVER_DLQ_X, DFE_ARCHIVER_S3_X"
            );
        });
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn the_new_name_is_derived_from_the_old_one() {
        let cases: [(&str, Option<&str>); 17] = [
            ("ARCHIVER_DESTINATION", Some("DFE_ARCHIVER_DESTINATION")),
            ("ARCHIVER_CONFIG", Some("DFE_ARCHIVER_CONFIG")),
            (
                "ARCHIVER_MEMORY_LIMIT_BYTES",
                Some("DFE_ARCHIVER_MEMORY_LIMIT_BYTES"),
            ),
            (
                "ARCHIVER_VERSION_CHECK__ENABLED",
                Some("DFE_ARCHIVER_VERSION_CHECK__ENABLED"),
            ),
            (
                "ARCHIVER_SCALING__ENABLED",
                Some("DFE_ARCHIVER_SCALING__ENABLED"),
            ),
            ("KAFKA_BROKERS", Some("DFE_ARCHIVER_KAFKA_BROKERS")),
            (
                "KAFKA_SASL_PASSWORD",
                Some("DFE_ARCHIVER_KAFKA_SASL_PASSWORD"),
            ),
            ("DLQ_MODE", Some("DFE_ARCHIVER_DLQ_MODE")),
            (
                "S3_SECRET_ACCESS_KEY",
                Some("DFE_ARCHIVER_S3_SECRET_ACCESS_KEY"),
            ),
            ("ARCHIVER_", None),
            ("DFE_ARCHIVER_DESTINATION", None),
            ("DFE_ARCHIVER_KAFKA_BROKERS", None),
            ("KAFKA_SCHEMAREGISTRY_URLS", None),
            ("KAFKA_NODE_ID", None),
            ("S3_BUCKET_EXTRA", None),
            ("TOKIO_WORKER_THREADS", None),
            ("ARCHIVER_BAD-NAME", None),
        ];
        for (old, expected) in cases {
            assert_eq!(replacement(old).as_deref(), expected, "{old}");
        }
    }

    #[test]
    fn a_legacy_variable_is_copied_unless_its_new_name_is_set() {
        let legacy = plan(&env(&[
            ("ARCHIVER_DESTINATION", "file:///old"),
            ("DFE_ARCHIVER_DESTINATION", "file:///new"),
            ("KAFKA_BROKERS", "old:9092"),
            ("DLQ_MODE", "cascade"),
            ("PATH", "/usr/bin"),
        ]));

        assert_eq!(
            legacy.copies,
            env(&[
                ("DFE_ARCHIVER_DLQ_MODE", "cascade"),
                ("DFE_ARCHIVER_KAFKA_BROKERS", "old:9092"),
            ])
        );
        assert_eq!(
            legacy.found,
            vec!["ARCHIVER_DESTINATION", "DLQ_MODE", "KAFKA_BROKERS"]
        );
        assert_eq!(plan(&env(&[("HOME", "/root")])), LegacyEnv::default());
    }

    /// The values include credentials, so the dotenv text must hand back every byte.
    #[test]
    fn values_survive_the_dotenv_round_trip() {
        let values = [
            "",
            "plain",
            "with space ",
            "$HOME and ${HOME}",
            "say \"hi\"",
            "it's",
            "back\\slash",
            "a#b # c",
            "line one\nline two",
            "tab\there",
            "caf\u{e9} \u{1f600}",
            "k=v;x=y",
        ];
        for value in values {
            let copies = env(&[("DFE_ARCHIVER_KAFKA_SASL_PASSWORD", value)]);
            let parsed: Vec<(String, String)> =
                dotenvy::from_read_iter(dotenv_text(&copies).as_bytes())
                    .collect::<Result<_, _>>()
                    .expect("dotenv text parses");
            assert_eq!(parsed, copies, "{value:?}");
        }
    }
}
