// Project:   dfe-archiver
// File:      crates/archiver/src/contract.rs
// Purpose:   DeploymentContract definition for dfe-archiver
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use scalo::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaContract, NativeDepsContract, OciLabels,
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
        schema_version: 3,
        app_name: "dfe-archiver".into(),
        binary_name: "dfe-archiver".into(),
        description: "High-volume Kafka-to-storage archiver for PB/s scale data pipelines".into(),
        metrics_port: 9090,
        health: HealthContract::default(),
        env_prefix: "ARCHIVER".into(),
        metric_prefix: "archiver".into(),
        config_mount_path: "/etc/dfe/archiver.yaml".into(),
        image_registry: "ghcr.io/hyperi-io".into(),
        extra_ports: vec![],
        entrypoint_args: vec!["--config".into(), "/etc/dfe/archiver.yaml".into()],
        secrets: vec![],
        default_config: None,
        depends_on: vec!["kafka".into()],
        keda: Some(KedaContract::default()),
        native_deps: NativeDepsContract::for_rustlib_features(
            &["transport-kafka", "spool", "tiered-sink"],
            &base_image,
        ),
        base_image,
        image_profile: ImageProfile::Production,
        oci_labels: OciLabels {
            description: "High-volume Kafka-to-storage archiver for PB/s scale data pipelines"
                .into(),
            // BUSL-1.1 drives the OCI `org.opencontainers.image.licenses`
            // label and the Dockerfile `# License` header. Copyright stays
            // the scalo default (the right HYPERI line).
            licenses: "BUSL-1.1".into(),
            ..OciLabels::default()
        },
        // Reflectable config (scalo-rs#6): derived JSON Schema of the full
        // Config + a catalog of the storage backends the archiver writes to.
        config_schema: Some(scalo::deployment::config_schema_json::<
            dfe_archiver_core::config::Config,
        >()),
        capabilities: capabilities(),
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
            .field(FieldSpec::string("path_template").description("Path template with {year}/{month}/{day}/{hour} placeholders."))
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

    #[test]
    fn test_contract_is_valid() {
        let contract = deployment_contract();
        assert_eq!(contract.app_name, "dfe-archiver");
        assert_eq!(contract.binary(), "dfe-archiver");
        assert_eq!(contract.metrics_port, 9090);
        assert_eq!(contract.health.liveness_path, "/healthz");
        assert_eq!(contract.health.readiness_path, "/readyz");
        assert!(contract.keda.is_some());
    }

    #[test]
    fn test_contract_carries_reflectable_config() {
        let contract = deployment_contract();
        assert_eq!(contract.schema_version, 3);
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
        assert!(!contract.base_image.is_empty());
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
