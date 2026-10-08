// Project:   dfe-archiver
// File:      crates/archiver/src/contract.rs
// Purpose:   DeploymentContract definition for dfe-archiver
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use dfe_archiver_core::buffer::DEFAULT_SPOOL_DIR;
use dfe_archiver_core::config::TRANSPORT_GRPC;
use scalo::deployment::{
    CONTRACT_SCHEMA_VERSION, DeploymentContract, HealthContract, ImageProfile, KafkaLagTrigger,
    KedaContract, NativeDepsContract, OciLabels, PortContract, ResourceList, ResourcesContract,
    SecretEnvContract, SecretGroupContract, SecurityContract, WritablePath,
    base_image_from_cascade,
};

/// Build the deployment contract for dfe-archiver.
///
/// This defines the deployment-facing contract: ports, health probes,
/// native dependencies, KEDA config, and entrypoint. Used by
/// `emit-dockerfile`, `emit-helm`, and `emit-contract` subcommands.
#[must_use]
pub fn deployment_contract() -> DeploymentContract {
    // Resolve the base image via the scalo cascade helper so the org-wide
    // `deployment.base_image` override (config or env) wins before falling
    // back to scalo's DEFAULT_BASE_IMAGE (debian:trixie-slim). NEVER hardcode
    // a distro -- the old "ubuntu:24.04" pin predated the trixie cutover.
    let base_image = base_image_from_cascade();
    DeploymentContract {
        schema_version: CONTRACT_SCHEMA_VERSION,
        app_name: "dfe-archiver".into(),
        binary_name: "dfe-archiver".into(),
        description: "High-volume Kafka-to-storage archiver for PB/s scale data pipelines".into(),
        metrics_port: 9090,
        health: HealthContract {
            startup_budget_seconds: 120,
            ..HealthContract::default()
        },
        env_prefix: "ARCHIVER".into(),
        metric_prefix: "archiver".into(),
        config_mount_path: "/etc/dfe/archiver.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        // Gated so the push port is published only where the Push listener binds it.
        extra_ports: vec![
            PortContract::tcp("push", 6000)
                .when_equals("config.transport", TRANSPORT_GRPC)
                .bound_from("grpc.listen")
                .app_protocol("kubernetes.io/h2c"),
        ],
        unbound_listen_paths: vec![],
        entrypoint_args: vec!["--config".into(), "/etc/dfe/archiver.yaml".into()],
        secrets: secrets(),
        default_config: default_config(),
        depends_on: vec!["kafka".into()],
        // No raw-lag trigger: DFE scales on the pressure composite, which this generator cannot express.
        keda: Some(KedaContract::default().with_kafka_trigger(KafkaLagTrigger::disabled())),
        native_deps: NativeDepsContract::for_scalo_features(
            &["transport-kafka", "spool", "tiered-sink"],
            &base_image,
        ),
        base_image,
        image_profile: ImageProfile::Production,
        // scalo writes no vendor, licence or copyright of its own, so the
        // labels and the generated Dockerfile header carry exactly these.
        oci_labels: OciLabels {
            description: "High-volume Kafka-to-storage archiver for PB/s scale data pipelines"
                .into(),
            vendor: "HYPERI PTY LIMITED".into(),
            label_namespace: "io.hyperi".into(),
            licenses: "BUSL-1.1".into(),
            copyright: "(c) 2026 HYPERI PTY LIMITED".into(),
            ..OciLabels::default()
        },
        // Reflectable config (scalo-rs#6): derived JSON Schema of the full
        // Config + a catalog of the storage backends the archiver writes to.
        config_schema: Some(scalo::deployment::config_schema_json::<
            dfe_archiver_core::config::Config,
        >()),
        capabilities: capabilities(),
        // The spool stages object-store uploads, so the root filesystem stays read-only.
        writable_paths: vec![WritablePath::new("spool", DEFAULT_SPOOL_DIR).size_limit("10Gi")],
        termination_grace_seconds: 45,
        resources: ResourcesContract {
            requests: ResourceList {
                cpu: "100m".into(),
                memory: "128Mi".into(),
            },
            limits: ResourceList {
                cpu: "500m".into(),
                memory: "512Mi".into(),
            },
        },
        security: SecurityContract::default(),
        singleton: false,
    }
}

/// The Secrets the chart mounts as env vars.
///
/// `Config::apply_flat_env` reads these under the `KAFKA_` and `S3_` prefixes,
/// never `ARCHIVER_`. The S3 group is optional because only an `s3://`
/// destination reads it.
fn secrets() -> Vec<SecretGroupContract> {
    let env = |env_var: &str, key_name: &str, secret_key: &str| SecretEnvContract {
        env_var: env_var.into(),
        key_name: key_name.into(),
        secret_key: secret_key.into(),
    };
    vec![
        SecretGroupContract::new(
            "kafka",
            vec![
                env("KAFKA_SASL_USER", "username", "username"),
                env("KAFKA_SASL_PASSWORD", "password", "password"),
                env("KAFKA_SASL_MECHANISM", "mechanism", "sasl.mechanism"),
            ],
        ),
        SecretGroupContract::new(
            "s3",
            vec![
                env("S3_ACCESS_KEY_ID", "access_key_id", "access_key_id"),
                env(
                    "S3_SECRET_ACCESS_KEY",
                    "secret_access_key",
                    "secret_access_key",
                ),
            ],
        )
        .optional(),
    ]
}

/// The default configuration the contract publishes, serialised from
/// `Config::default()`.
///
/// Derived rather than hand-authored so the published default cannot drift
/// from the one the binary boots with, which is what makes
/// `generate-artefacts` output pass this app's own `config-check`.
fn default_config() -> Option<serde_json::Value> {
    match serde_json::to_value(dfe_archiver_core::config::Config::default()) {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::error!(
                error = %e,
                "Config::default() did not serialise -- the contract publishes no default config"
            );
            None
        }
    }
}

/// Capability catalog for dfe-archiver: the object-store backends it writes
/// archives to, grounded in `dfe_archiver_core::config::ArchiveConfig`. The
/// typed per-backend knobs live in the derived schema; this lists the backends
/// + the destination URL scheme that selects each.
fn capabilities() -> Vec<scalo::deployment::Capability> {
    use scalo::deployment::{Capability, FieldSpec};
    vec![
        Capability::sink("archive")
            .description("Rolling object-store archiver: consumes Kafka, writes compressed rolled files to a storage backend selected by the destination URL scheme.")
            .maturity("stable")
            .field(FieldSpec::string("destination").required().description("Destination URL; the scheme selects the backend (file:// | s3:// | gs:// | az:// | minio://)."))
            .field(FieldSpec::string("path_template").description("Path template under the routed destination; supported placeholders are {year}, {month}, {day}, {hour}, {minute}, {timestamp} and {seq}."))
            .field(FieldSpec::enumeration("compression", ["none", "zstd", "lz4", "snappy", "gzip"]).description("Rolled-file compression codec."))
            .children(vec![
                Capability::service("file").description("Local filesystem (file://)."),
                Capability::service("s3").description("Amazon S3 (s3://)."),
                Capability::service("gcs").description("Google Cloud Storage (gs:// / gcs://)."),
                Capability::service("azure").description("Azure Blob Storage (az:// / azure://)."),
                Capability::service("minio").description("MinIO / S3-compatible (minio://)."),
            ]),
    ]
}

/// Generate the Dockerfile from the deployment contract.
///
/// Thin wrapper over `scalo::deployment::generate_dockerfile`. Both the
/// `emit-dockerfile` CLI path and the
/// `checked_in_dockerfile_matches_emit_dockerfile` drift guard call THIS
/// function so there is one source of truth for the checked-in `Dockerfile`.
/// `None` = no contract-identity labels (those are stamped by the
/// CI-orchestrated invocation, not this one-off).
#[must_use]
pub fn emit_dockerfile() -> String {
    scalo::deployment::generate_dockerfile(&deployment_contract(), None)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use scalo::config::flat_env::ApplyFlatEnv;

    /// The chart mounts a Secret under every declared name, so a name the
    /// config never reads leaves the credential silently unused.
    #[test]
    fn every_declared_secret_env_var_reaches_the_config() {
        let contract = deployment_contract();
        assert!(
            !contract.secrets.is_empty(),
            "the contract declares no secrets"
        );
        for group in &contract.secrets {
            for env in &group.env_vars {
                let sentinel = format!("sentinel-{}", env.key_name);
                let mut config = dfe_archiver_core::config::Config::default();
                temp_env::with_var(&env.env_var, Some(sentinel.as_str()), || {
                    config.apply_flat_env(&contract.env_prefix);
                });

                // Secrets serialise redacted outside `expose_during`.
                let applied = scalo::expose_during(|| serde_json::to_string(&config))
                    .expect("config serialises");
                // The message carries the env var and group names only, never a value.
                assert!(
                    applied.contains(&sentinel),
                    "{} ({}) was set and no config field read it",
                    env.env_var,
                    group.group_name
                );
            }
        }
    }

    #[test]
    fn test_contract_is_valid() {
        let contract = deployment_contract();
        assert_eq!(contract.app_name, "dfe-archiver");
        assert_eq!(contract.binary(), "dfe-archiver");
        assert_eq!(contract.metrics_port, 9090);
        assert_eq!(contract.health.liveness_path, "/livez");
        assert_eq!(contract.health.readiness_path, "/readyz");
        assert!(contract.keda.is_some());
    }

    /// `generate-artefacts` and `generate_chart` refuse a contract that fails
    /// these checks, and write nothing.
    #[test]
    fn test_contract_passes_the_generator_checks() {
        deployment_contract()
            .validate()
            .expect("contract must pass DeploymentContract::validate");
    }

    #[test]
    fn test_every_listener_has_a_port() {
        scalo::deployment::assert_listeners_declared(&deployment_contract());
    }

    /// The push port follows `transport`: off on the published kafka default,
    /// on for grpc, and the address the charts bind agrees with its number.
    #[test]
    fn test_push_port_exists_on_the_grpc_transport_only() {
        let mut contract = deployment_contract();
        let push = contract
            .extra_ports
            .iter()
            .find(|p| p.name == "push")
            .expect("push port")
            .clone();
        assert_eq!(push.port, 6000);
        let gate = push.when.as_ref().expect("push port must be gated");

        let mut config = contract.default_config.clone().expect("default config");
        assert_eq!(gate.holds_in(&config), Some(false));

        config["transport"] = serde_json::json!(TRANSPORT_GRPC);
        config["grpc"]["listen"] = serde_json::json!("0.0.0.0:6000");
        assert_eq!(gate.holds_in(&config), Some(true));

        contract.default_config = Some(config);
        assert!(
            contract.undeclared_listeners().is_empty(),
            "{:?}",
            contract.undeclared_listeners()
        );
    }

    /// KEDA stays on with CPU as its only trigger: raw consumer lag rises when
    /// a downstream stage breaks, and scaling out then does nothing.
    #[test]
    fn test_keda_has_no_kafka_lag_trigger() {
        let contract = deployment_contract();
        let keda = contract.keda.as_ref().expect("keda contract");
        assert!(keda.enabled);
        assert!(keda.cpu_enabled);
        assert!(!keda.kafka_trigger.enabled, "a raw-lag trigger is declared");
        assert!(keda.min_replicas >= 1, "CPU alone cannot scale from zero");
        let unresolved = contract.unresolved_values_paths();
        assert!(unresolved.is_empty(), "{unresolved:?}");
    }

    #[test]
    fn test_contract_carries_reflectable_config() {
        let contract = deployment_contract();
        assert_eq!(contract.schema_version, CONTRACT_SCHEMA_VERSION);
        assert!(contract.config_schema.is_some());
        let archive = contract
            .capabilities
            .iter()
            .find(|c| c.name == "archive")
            .expect("archive sink capability");
        let backends: Vec<&str> = archive.children.iter().map(|s| s.name.as_str()).collect();
        assert!(
            backends.contains(&"s3") && backends.contains(&"gcs") && backends.contains(&"azure")
        );
    }

    /// Committed reflectable artefacts under the workspace-root docs/ must not
    /// drift. Regenerate with `dfe-archiver config-schema --dir docs`.
    #[test]
    fn test_config_artifacts_do_not_drift() {
        // CARGO_MANIFEST_DIR is crates/archiver; the repo docs/ is two up.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs");
        scalo::deployment::assert_no_config_artifact_drift(&deployment_contract(), dir);
    }

    /// dfe-docker generates `defaults_v<version>.yaml` from this field, so it
    /// has to be the real `Config::default()` and it has to pass the same
    /// validator the service boots through.
    #[test]
    fn test_published_default_config_round_trips_through_the_validator() {
        let published = deployment_contract()
            .default_config
            .expect("the contract must publish a default config");

        let config: dfe_archiver_core::config::Config =
            serde_json::from_value(published.clone()).expect("published default must deserialise");
        crate::config::validate_config(&config).expect("published default must validate");

        assert_eq!(
            serde_json::to_value(&config).expect("re-serialise"),
            published,
            "the published default is not Config::default()"
        );
    }

    #[test]
    fn test_contract_json_roundtrip() {
        let contract = deployment_contract();
        let json = contract.to_json();
        let parsed: DeploymentContract =
            serde_json::from_str(&json).expect("contract should roundtrip via JSON");
        assert_eq!(parsed.app_name, "dfe-archiver");
        assert_eq!(parsed.metrics_port, 9090);
    }

    #[test]
    fn test_contract_native_deps() {
        let contract = deployment_contract();
        // librdkafka1 is required for the kafka transport. On debian trixie it
        // comes from debian's own repos (a plain apt package); on ubuntu it came
        // via the confluent apt repo (no trixie suite). Assert it is required
        // either way -- distro-agnostic, since the base is cascade-resolved.
        let in_packages = contract
            .native_deps
            .apt_packages
            .contains(&"librdkafka1".into());
        let in_repos = contract
            .native_deps
            .apt_repos
            .iter()
            .any(|r| r.packages.contains(&"librdkafka1".into()));
        assert!(in_packages || in_repos, "should require librdkafka1");
    }

    #[test]
    fn test_contract_base_image() {
        // The cascade helper resolves the org-wide `deployment.base_image`
        // override (config or env) when set, else falls back to scalo's
        // DEFAULT_BASE_IMAGE (debian:trixie-slim). In CI / local-dev with no
        // overrides the default applies. Assert it is non-empty and carries an
        // explicit tag -- never pin a distro here.
        let contract = deployment_contract();
        assert_ne!(contract.base_image, "");
        assert!(
            contract.base_image.contains(':'),
            "base_image must include an explicit tag: {}",
            contract.base_image
        );
    }

    #[test]
    fn test_contract_generates_dockerfile() {
        let contract = deployment_contract();
        let dockerfile = scalo::deployment::generate_dockerfile(&contract, None);
        // Base image is cascade-resolved (debian:trixie-slim by default), so
        // assert the FROM line matches the contract's resolved base_image
        // rather than pinning a distro.
        assert!(
            dockerfile.contains(&format!("FROM {}", contract.base_image)),
            "missing/incorrect base image FROM line in Dockerfile",
        );
        assert!(dockerfile.contains("dfe-archiver"));
        assert!(dockerfile.contains("9090"));
        assert!(dockerfile.contains("librdkafka1"));
        // BUSL-1.1 from oci_labels.licenses drives the Dockerfile header.
        assert!(
            dockerfile.contains("# License:   BUSL-1.1"),
            "Dockerfile missing BUSL-1.1 license header",
        );
    }

    #[test]
    fn checked_in_dockerfile_matches_emit_dockerfile() {
        // The checked-in Dockerfile is autogenerated from emit_dockerfile()
        // (the same function the `emit-dockerfile` CLI path uses). If they
        // drift, CI publishes from a stale Dockerfile -- e.g. a base-image or
        // licence change in the contract that never got regenerated.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("Dockerfile");
        let on_disk = std::fs::read_to_string(&path).expect("read Dockerfile");
        let emitted = emit_dockerfile();
        assert_eq!(
            on_disk.trim(),
            emitted.trim(),
            "Dockerfile on disk does not match emit_dockerfile() output -- \
             regenerate with: `dfe-archiver emit-dockerfile Dockerfile`",
        );
    }
}
