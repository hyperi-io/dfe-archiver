// Project:   dfe-archiver
// File:      crates/archiver/src/contract.rs
// Purpose:   DeploymentContract definition for dfe-archiver
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

use hyperi_rustlib::deployment::{
    DeploymentContract, HealthContract, ImageProfile, KedaContract, NativeDepsContract, OciLabels,
};

/// Build the deployment contract for dfe-archiver.
///
/// This defines the deployment-facing contract: ports, health probes,
/// native dependencies, KEDA config, and entrypoint. Used by
/// `emit-dockerfile`, `emit-helm`, and `emit-contract` subcommands.
#[must_use]
pub fn deployment_contract() -> DeploymentContract {
    DeploymentContract {
        schema_version: 2,
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
        base_image: "ubuntu:24.04".into(),
        native_deps: NativeDepsContract::for_rustlib_features(
            &["transport-kafka", "spool", "tiered-sink"],
            "ubuntu:24.04",
        ),
        image_profile: ImageProfile::Production,
        oci_labels: OciLabels {
            description: "High-volume Kafka-to-storage archiver for PB/s scale data pipelines"
                .into(),
            ..OciLabels::default()
        },
    }
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
        assert!(
            !contract.native_deps.apt_repos.is_empty(),
            "should have confluent repo"
        );
        assert!(
            contract.native_deps.apt_repos[0]
                .packages
                .contains(&"librdkafka1".into()),
            "should require librdkafka1"
        );
    }

    #[test]
    fn test_contract_generates_dockerfile() {
        let contract = deployment_contract();
        let dockerfile = hyperi_rustlib::deployment::generate_dockerfile(&contract, None);
        assert!(dockerfile.contains("ubuntu:24.04"));
        assert!(dockerfile.contains("dfe-archiver"));
        assert!(dockerfile.contains("9090"));
        assert!(dockerfile.contains("librdkafka1"));
    }
}
