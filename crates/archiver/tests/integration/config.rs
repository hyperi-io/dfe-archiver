// Project:   dfe-archiver
// File:      crates/archiver/tests/integration/config.rs
// Purpose:   Config loading and validation integration tests
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver::config::{Config, load_config, validate_config};
use dfe_archiver::contract::deployment_contract;

/// Path to the committed fixture config.
fn fixture_path() -> String {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/valid_config.yaml"
    )
    .to_string()
}

/// Parse the `KEY="value"` assignments out of `.env.example`.
fn env_example_assignments() -> Vec<(String, String)> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env.example");
    let text = std::fs::read_to_string(path).expect("read .env.example");

    text.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| l.split_once('='))
        .map(|(k, v)| (k.to_string(), v.trim_matches('"').to_string()))
        .collect()
}

/// The deployment's env-var surface must reach the settings it names.
///
/// One test rather than three: scalo's cascade is a process-global `OnceLock`
/// and `temp_env` mutates the process environment, so separate tests would
/// race under `cargo test`, which runs a test binary's cases on threads.
///
/// Covers, in order:
///
/// 1. `load_config` leaves the cascade initialised. Every deployment knob
///    `ServiceRuntime` resolves -- `version_check`, `metrics`, `logger`,
///    `self_regulation`, `scaling`, `worker_pool` -- comes from a
///    `from_cascade()` helper that silently returns its own default when the
///    cascade was never set up. The single assertion covers a section added
///    later too, so it needs no editing when the runtime grows one.
/// 2. The chart's version-check opt-out actually opts out.
///    `dfe-common.versionCheckEnv` (dfe-infra) renders
///    `<PREFIX>_VERSION_CHECK__*` and documents `enabled: false` as a total
///    opt-out for an air-gapped install. The name is built from the contract's
///    own `env_prefix`, so a prefix rename fails here and not in a cluster.
///    Asserted against the resolved setting, never a config dump: `enabled`
///    defaults to true, so `false` can only have come from the environment.
/// 3. `.env.example` starts the binary. It says `cp .env.example .env`, and
///    dotenvy loads that file before config resolves, so every name in it is a
///    live override.
/// 4. `ARCHIVER_SPOOL_DIR` moves the spool. It is the deployment's only
///    way off the default path, and the image is the only place that path
///    exists, so a name that reaches nothing is a boot failure in a container.
#[test]
fn deployment_env_surface_reaches_the_settings_it_names() {
    let version_check_var = format!(
        "{}_VERSION_CHECK__ENABLED",
        deployment_contract().env_prefix
    );

    let assignments = env_example_assignments();
    assert!(
        !assignments.is_empty(),
        "no assignments parsed out of .env.example"
    );

    let mut vars: Vec<(&str, Option<&str>)> = assignments
        .iter()
        .map(|(k, v)| (k.as_str(), Some(v.as_str())))
        .collect();
    vars.push((version_check_var.as_str(), Some("false")));

    temp_env::with_vars(vars, || {
        let config = load_config(Some(&fixture_path()))
            .expect(".env.example must not stop the archiver from starting");

        assert!(
            scalo::config::try_get().is_some(),
            "load_config must call scalo::config::setup() -- without it every \
             from_cascade() reader in ServiceRuntime silently takes its default \
             and no deployment override reaches anything"
        );

        let version_check = scalo::version_check::VersionCheckConfig::from_cascade_or(
            "dfe-archiver",
            "0.0.0-test",
            dfe_archiver::version_check_defaults(),
        );
        assert!(
            !version_check.enabled,
            "{version_check_var}=false must disable the startup version check; \
             the deployment sets it to keep an air-gapped install from calling \
             out to {}",
            version_check.api_url
        );

        // Beyond "it validated": .env.example's own ARCHIVER_DESTINATION has to
        // be the destination, or none of the assignments reached the config.
        assert_eq!(config.archive.destination, "file:///tmp/dfe-archiver-test");
        assert_eq!(config.buffer.spool_dir, "/tmp/dfe-archiver-spool");
    });
}

/// `.env.example` must not name a memory-guard variable the guard cannot read.
///
/// `MemoryGuardConfig::from_env` is handed the app's env prefix, so the guard's
/// three names are `<PREFIX>_MEMORY_*`. Any other prefix parses as a comment
/// and reaches nothing. Built from the contract's prefix so a rename fails here.
#[test]
fn env_example_memory_guard_names_carry_the_contract_env_prefix() {
    let prefix = deployment_contract().env_prefix;

    let offenders: Vec<String> = env_example_assignments()
        .into_iter()
        .map(|(k, _)| k)
        .filter(|k| k.contains("MEMORY_") && !k.starts_with(&format!("{prefix}_")))
        .collect();

    assert!(
        offenders.is_empty(),
        "memory-guard variables must start with {prefix}_ or scalo's MemoryGuard \
         never reads them: {offenders:?}"
    );
}

/// Load the fixture YAML and validate it parses + validates
#[test]
fn test_fixture_config_loads_and_validates() {
    let yaml = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/valid_config.yaml"
    ))
    .expect("read fixture");

    let config: Config = serde_yaml_ng::from_str(&yaml).expect("parse YAML");
    validate_config(&config).expect("fixture config should validate");

    assert_eq!(config.kafka.brokers, vec!["localhost:9092"]);
    assert_eq!(config.kafka.group_id, "dfe-archiver");
    assert_eq!(config.compression.codec, "zstd");
    assert_eq!(config.archive.destination, "file:///var/data/archive");
}

/// Default config (with brokers populated) validates
#[test]
fn test_default_config_validates() {
    let mut config = Config::default();
    config.kafka.brokers = vec!["localhost:9092".to_string()];
    validate_config(&config).expect("default config should validate");
}

/// `config-check` run by the binary from `dir` on the fixture config, returning
/// what it printed.
fn config_check_in(dir: &std::path::Path) -> String {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-archiver"))
        .args(["--config", &fixture_path(), "config-check"])
        .current_dir(dir)
        .env_remove("ARCHIVER_CONFIG")
        .env_remove("ARCHIVER_DESTINATION")
        .output()
        .expect("the binary runs");
    let printed = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "config-check failed:\n{printed}");
    printed
}

/// The binary reads the `.env` in its working directory and no other.
///
/// A `.env` in a parent directory belongs to whatever project sits above, so a
/// search up the tree loads another project's settings and credentials.
#[test]
fn a_dotenv_in_a_parent_directory_is_not_loaded() {
    let root = tempfile::TempDir::new().expect("tempdir");
    let project = root.path().join("project");
    std::fs::create_dir(&project).expect("project dir");
    std::fs::write(
        root.path().join(".env"),
        "ARCHIVER_DESTINATION=file:///tmp/from_parent_dotenv\n",
    )
    .expect("parent .env");

    let printed = config_check_in(&project);
    assert!(
        !printed.contains("from_parent_dotenv"),
        "a .env in the parent directory reached the config"
    );

    // The project's own .env still loads.
    std::fs::write(
        project.join(".env"),
        "ARCHIVER_DESTINATION=file:///tmp/from_project_dotenv\n",
    )
    .expect("project .env");
    let printed = config_check_in(&project);
    assert!(
        printed.contains("from_project_dotenv"),
        "the project's own .env did not reach the config"
    );
}
