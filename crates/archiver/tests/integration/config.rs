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
///    `self_regulation`, `scaling` -- comes from a
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
/// 4. `DFE_ARCHIVER_SPOOL_DIR` moves the spool. It is the deployment's only
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

        // Beyond "it validated": .env.example's own DFE_ARCHIVER_DESTINATION has to
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

    let config: Config = scalo::yaml::from_str(&yaml, "config.yaml", scalo::yaml::Profile::Config)
        .expect("parse YAML");
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

/// `config-check` run by the binary from `dir` on the fixture config with
/// `envs` set, returning what it printed.
fn config_check_with(dir: &std::path::Path, envs: &[(&str, &str)]) -> String {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_dfe-archiver"));
    command
        .args(["--config", &fixture_path(), "config-check"])
        .current_dir(dir);
    for name in [
        "ARCHIVER_CONFIG",
        "ARCHIVER_DESTINATION",
        "DFE_ARCHIVER_CONFIG",
        "DFE_ARCHIVER_DESTINATION",
    ] {
        command.env_remove(name);
    }
    let out = command
        .envs(envs.iter().copied())
        .output()
        .expect("the binary runs");
    let printed = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "config-check failed:\n{printed}");
    printed
}

fn config_check_in(dir: &std::path::Path) -> String {
    config_check_with(dir, &[])
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
        "DFE_ARCHIVER_DESTINATION=file:///tmp/from_parent_dotenv\n",
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
        "DFE_ARCHIVER_DESTINATION=file:///tmp/from_project_dotenv\n",
    )
    .expect("project .env");
    let printed = config_check_in(&project);
    assert!(
        printed.contains("from_project_dotenv"),
        "the project's own .env did not reach the config"
    );
}

/// A deployment still on the old names keeps working, and a new name outranks
/// its old spelling wherever both are set, in the environment or in `.env`.
#[test]
fn a_legacy_env_name_reaches_the_config_and_the_new_name_wins() {
    type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a str);
    let cases: [Case; 3] = [
        (
            "",
            &[("ARCHIVER_DESTINATION", "file:///tmp/legacy_env")],
            "legacy_env",
        ),
        (
            "ARCHIVER_DESTINATION=file:///tmp/legacy_dotenv\n",
            &[],
            "legacy_dotenv",
        ),
        (
            "ARCHIVER_DESTINATION=file:///tmp/legacy_dotenv\n",
            &[("DFE_ARCHIVER_DESTINATION", "file:///tmp/new_env")],
            "new_env",
        ),
    ];
    for (dotenv, envs, expected) in cases {
        let project = tempfile::TempDir::new().expect("tempdir");
        std::fs::write(project.path().join(".env"), dotenv).expect("project .env");

        let printed = config_check_with(project.path(), envs);

        assert!(printed.contains(expected), "{envs:?}: {printed}");
        for other in ["legacy_env", "legacy_dotenv", "new_env"] {
            assert!(
                other == expected || !printed.contains(other),
                "{other} outranked {expected}: {printed}"
            );
        }
    }
}

/// Every flat suffix `Config::apply_flat_env` reads, with a value that moves
/// the setting off its default.
const FLAT_ENV_CASES: [(&str, &str); 30] = [
    ("TRANSPORT", "grpc"),
    ("GRPC_LISTEN", "0.0.0.0:7000"),
    ("KAFKA_BROKERS", "broker-a:9092,broker-b:9092"),
    ("KAFKA_GROUP_ID", "group-x"),
    ("KAFKA_TOPICS", "topic-a,topic-b"),
    ("KAFKA_TOPIC_INCLUDE", "_land$"),
    ("KAFKA_TOPIC_EXCLUDE", "_dlq$"),
    ("KAFKA_TOPIC_REFRESH_SECS", "7"),
    ("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
    ("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    ("KAFKA_SASL_USER", "user-x"),
    ("KAFKA_SASL_PASSWORD", "password-x"),
    ("KAFKA_SSL_CA_LOCATION", "/etc/ca.pem"),
    ("KAFKA_ALLOW_INSECURE_TRANSPORT", "true"),
    ("DLQ_ENABLED", "false"),
    ("DLQ_TOPIC", "dlq-x"),
    ("DLQ_MODE", "file_only"),
    ("DESTINATION", "file:///tmp/x"),
    ("PATH_TEMPLATE", "{year}/{month}"),
    ("COMPRESSION_CODEC", "gzip"),
    ("FLUSH_BYTES", "2048"),
    ("FLUSH_INTERVAL_SECS", "9"),
    ("MULTIPART_CHUNK_SIZE", "6291456"),
    ("SPOOL_DIR", "/tmp/spool-x"),
    ("S3_BUCKET", "bucket-x"),
    ("S3_REGION", "region-x"),
    ("S3_ACCESS_KEY_ID", "key-x"),
    ("S3_SECRET_ACCESS_KEY", "secret-x"),
    ("S3_ENDPOINT", "http://localhost:9000"),
    ("S3_ALLOW_HTTP", "true"),
];

/// Each flat name moves its setting under the contract's prefix, and
/// `LEGACY_FLAT_ENV` lists exactly the old spellings with no `ARCHIVER_` prefix,
/// so a reader added without its old name or a stale list entry fails here.
#[test]
fn every_flat_env_name_is_read_under_the_contract_prefix() {
    use dfe_archiver_core::config::{ENV_PREFIX, LEGACY_FLAT_ENV};
    use scalo::config::flat_env::ApplyFlatEnv;

    let effective = |config: &Config| {
        scalo::expose_during(|| serde_json::to_value(config)).expect("config serialises")
    };
    let baseline = effective(&Config::default());
    assert_eq!(deployment_contract().env_prefix, ENV_PREFIX);

    for (suffix, value) in FLAT_ENV_CASES {
        let name = format!("{ENV_PREFIX}_{suffix}");
        let mut config = Config::default();
        temp_env::with_var(&name, Some(value), || config.apply_flat_env(ENV_PREFIX));
        assert_ne!(effective(&config), baseline, "{name} reaches no setting");
    }

    let mut prefixed: Vec<&str> = FLAT_ENV_CASES
        .iter()
        .map(|(suffix, _)| *suffix)
        .filter(|suffix| {
            ["KAFKA_", "DLQ_", "S3_"]
                .iter()
                .any(|p| suffix.starts_with(p))
        })
        .collect();
    let mut listed: Vec<&str> = LEGACY_FLAT_ENV.to_vec();
    prefixed.sort_unstable();
    listed.sort_unstable();
    assert_eq!(listed, prefixed);
}
