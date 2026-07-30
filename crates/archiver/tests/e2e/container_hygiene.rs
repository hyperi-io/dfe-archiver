// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/container_hygiene.rs
// Purpose:   Pin the test-container naming and cleanup convention
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! The naming and cleanup convention for containers this suite starts.
//!
//! Several runs share a developer machine, so a container has to say what it is,
//! which suite started it, and whose run owns it -- otherwise `docker ps` shows a
//! wall of random hex and nobody can tell what is safe to remove. These tests
//! pin the scheme so it cannot drift back to testcontainers' defaults.
//!
//! Note what is deliberately NOT in scope: the dev-stack containers in
//! `docker-compose.dev.yaml` (`archiver-minio`, `archiver-minio-init`). Those
//! belong to the developer who ran `docker compose up` and are correctly named
//! for that. The convention covers containers a TEST starts.

use crate::common;
use crate::common::{TEST_SUITE_LABEL, container_name};

/// A per-test instance carries the test name between the suite and the service,
/// so two tests owning their own container do not collide.
#[test]
fn per_test_container_name_includes_the_test() {
    let name = container_name(Some("test_azure_basic_operations"), "azurite");
    assert_eq!(
        name,
        "dfe-archiver-test-integration-test-azure-basic-operations-azurite"
    );
}

/// The binary-scoped form, for a container started once for a whole test binary.
#[test]
fn binary_scoped_container_name_omits_the_test() {
    let name = container_name(None, "azurite");
    assert_eq!(name, "dfe-archiver-test-integration-azurite");
}

/// Two tests asking for the same service must get DIFFERENT names.
///
/// nextest runs each test in its own process, so two tests calling the same
/// `acquire_*` start two containers -- they do not share one. On a single name
/// the first create wins and the rest fail with "name is already in use", and
/// those tests then take their `else` branch and SKIP. Green, testing nothing.
#[test]
fn two_tests_wanting_one_service_do_not_collide() {
    assert_ne!(
        container_name(Some("first_test"), "azurite"),
        container_name(Some("second_test"), "azurite"),
    );
}

/// Docker only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*`. A Rust test path carries
/// colons (`azure::test_azure_basic_operations`), which would be rejected at
/// create time -- a failure that reads as a Docker problem rather than a naming
/// one, so it is normalised here instead.
#[test]
fn container_name_is_a_legal_docker_name() {
    let name = container_name(Some("azure::test_azure_basic_operations"), "Azurite");
    assert_eq!(
        name,
        "dfe-archiver-test-integration-azure--test-azure-basic-operations-azurite"
    );

    let legal = |s: &str| {
        let mut chars = s.chars();
        chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
            && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    };
    assert!(legal(&name), "{name} is not a legal Docker container name");
    assert!(legal(&container_name(None, "azurite")));
}

/// Every name is prefixed so one `docker ps` filter finds everything this repo's
/// suite started, and nothing belonging to another repo.
#[test]
fn names_share_one_greppable_prefix() {
    for name in [
        container_name(None, "azurite"),
        container_name(Some("some_test"), "azurite"),
    ] {
        assert!(
            name.starts_with("dfe-archiver-test-integration-"),
            "{name} does not carry the suite prefix"
        );
    }
}

/// The label is what makes a bulk sweep possible when a run was killed and the
/// names are not known: `docker rm -f $(docker ps -aq --filter label=...)`.
#[test]
fn suite_label_identifies_this_repo() {
    assert_eq!(TEST_SUITE_LABEL.0, "io.hyperi.test.suite");
    assert_eq!(TEST_SUITE_LABEL.1, "dfe-archiver-integration");
}

/// A REAL container carries the name and labels, and is gone afterwards.
///
/// The tests above only exercise `container_name`, which proves the helper
/// returns the right string -- not that anything calls it. A convention that is
/// never wired to a container is decoration, so this asks Docker what actually
/// got created, and then that it was removed.
#[tokio::test]
async fn a_started_container_carries_the_name_and_label_then_goes_away() {
    const TEST: &str = "container-hygiene-inspects-a-real-one";
    let expected = container_name(Some(TEST), "azurite");

    let inspect = |field: &str| {
        std::process::Command::new("docker")
            .args(["inspect", &expected, "--format", field])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };

    {
        let Some(azure) = common::acquire_azure(TEST, "hygiene-test").await else {
            return; // no Docker; acquire_azure said so and failed the run in CI
        };
        if !azure.manages_container() {
            eprintln!("skipping: a live Azure account is configured, so no container was started");
            return;
        }

        let name =
            inspect("{{.Name}}").expect("docker must know the container by its expected name");
        assert_eq!(
            name.trim_start_matches('/'),
            expected,
            "the container is not named per the convention"
        );

        let suite = inspect(&format!(
            "{{{{index .Config.Labels \"{}\"}}}}",
            TEST_SUITE_LABEL.0
        ))
        .expect("docker inspect must report labels");
        assert_eq!(suite, TEST_SUITE_LABEL.1, "suite label missing or wrong");

        let service = inspect(r#"{{index .Config.Labels "io.hyperi.test.service"}}"#)
            .expect("docker inspect must report labels");
        assert_eq!(service, "azurite", "service label missing or wrong");

        let owner = inspect(r#"{{index .Config.Labels "io.hyperi.test.owner-pid"}}"#)
            .expect("docker inspect must report labels");
        assert_eq!(
            owner,
            std::process::id().to_string(),
            "owner-pid label must name THIS test process, or it cannot answer whose run left it"
        );
    }

    // Dropped above. Removal is asynchronous, so give it a moment before
    // asserting -- a bare check here would be racy and fail for the wrong reason.
    for _ in 0..50 {
        if inspect("{{.Name}}").is_none() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{expected} still exists after the holder was dropped -- the suite leaks containers");
}
