// Project:   dfe-archiver
// File:      crates/archiver/tests/common/mod.rs
// Purpose:   Shared test utilities with dual-mode infrastructure
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

#![allow(dead_code, clippy::expect_used)]

use std::env;
use std::net::ToSocketAddrs;
use std::time::Duration;

// ── Reachability ─────────────────────────────────────────────────────

/// Is `host_port` accepting TCP connections?
///
/// Tries EVERY address the name resolves to. Taking only the first one is a
/// silent false negative on any dual-stack host: `localhost` resolves to `::1`
/// ahead of `127.0.0.1` here, and Docker's `-p 9000:9000` publishes the IPv4
/// mapping only, so a first-address-only probe reports a healthy service down.
/// Measured against a running `MinIO` -- `[::1]:9000` refused, `127.0.0.1:9000`
/// connected -- which is what made six `MinIO` tests skip and report green.
///
/// `timeout` is per address, so the worst case is `timeout` times the number of
/// resolved addresses. Keep it short: these are local services.
fn tcp_reachable(host_port: &str, timeout: Duration) -> bool {
    match host_port.to_socket_addrs() {
        Ok(mut addrs) => addrs.any(|a| std::net::TcpStream::connect_timeout(&a, timeout).is_ok()),
        Err(_) => false,
    }
}

/// Fail in CI where a test would otherwise skip for want of a service.
///
/// A skip in CI is the worst outcome available: the job is green and nothing was
/// tested. Outside CI a skip is legitimate -- a developer may not have the stack
/// up -- but the reason still has to be printed, because it is the only signal
/// that the test did not run.
pub fn require_service_in_ci(service: &str, reason: &str) {
    assert!(
        env::var_os("CI").is_none(),
        "no {service} available in CI ({reason}). Integration tests must RUN \
         here, not skip; skipping would report green while testing nothing."
    );
    eprintln!("{service} unavailable, test will skip: {reason}");
}

/// Test backend mode.
///
/// Controlled by `TEST_MODE` in `.env`:
/// - `"remote"` (default) — use devex cluster endpoints from env vars
/// - `"docker"` — use dfe-docker infra profile (localhost, no auth, no TLS)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Remote,
    Docker,
}

impl TestMode {
    pub fn detect() -> Self {
        load_dotenv();
        match env::var("TEST_MODE").unwrap_or_default().as_str() {
            "docker" => Self::Docker,
            _ => Self::Remote,
        }
    }
}

pub fn load_dotenv() {
    let _ = dotenvy::dotenv();
}

// ── Kafka ────────────────────────────────────────────────────────────

/// Kafka connection config for the active test mode.
///
/// Docker mode: `localhost:19092`, PLAINTEXT, no SASL.
/// Remote mode: from env vars (`KAFKA_BROKERS`, `KAFKA_SASL_MECHANISM`, etc.)
pub struct KafkaTestConfig {
    pub brokers: String,
    pub security_protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_user: Option<String>,
    pub sasl_password: Option<String>,
}

impl KafkaTestConfig {
    pub fn has_sasl(&self) -> bool {
        self.sasl_mechanism.is_some() && self.sasl_user.is_some()
    }

    /// Check if broker is reachable via TCP
    pub fn is_reachable(&self) -> bool {
        let first = self.brokers.split(',').next().unwrap_or(&self.brokers);
        tcp_reachable(first, Duration::from_secs(3))
    }
}

pub fn kafka_test_config() -> KafkaTestConfig {
    load_dotenv();
    match TestMode::detect() {
        TestMode::Docker => KafkaTestConfig {
            brokers: "localhost:19092".into(),
            security_protocol: "PLAINTEXT".into(),
            sasl_mechanism: None,
            sasl_user: None,
            sasl_password: None,
        },
        TestMode::Remote => KafkaTestConfig {
            brokers: env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
            security_protocol: env::var("KAFKA_SECURITY_PROTOCOL")
                .unwrap_or_else(|_| "SASL_PLAINTEXT".into()),
            sasl_mechanism: env::var("KAFKA_SASL_MECHANISM").ok(),
            sasl_user: env::var("KAFKA_SASL_USER").ok(),
            sasl_password: env::var("KAFKA_SASL_PASSWORD").ok(),
        },
    }
}

/// Skip test if Kafka is not available in the current test mode
#[macro_export]
macro_rules! skip_if_no_kafka {
    () => {
        let kf = $crate::common::kafka_test_config();
        if !kf.is_reachable() {
            eprintln!(
                "Skipping: Kafka not reachable at {} (TEST_MODE={:?})",
                kf.brokers,
                $crate::common::TestMode::detect()
            );
            return;
        }
    };
}

// ── Docker lifecycle ─────────────────────────────────────────────────

/// Start dfe-docker infra profile if `TEST_MODE=docker` and containers aren't running.
///
/// Looks for dfe-docker at:
///   1. `DFE_DOCKER_PATH` env var
///   2. `../dfe-docker` (sibling directory convention)
///
/// Returns `Ok(true)` if containers were started, `Ok(false)` if already running or not docker mode.
pub fn ensure_docker_infra() -> Result<bool, String> {
    if TestMode::detect() != TestMode::Docker {
        return Ok(false);
    }

    // Check if already running (look for redpanda container from dfe-docker)
    let output = std::process::Command::new("docker")
        .args(["ps", "--filter", "name=dfe-kafka", "--format", "{{.Names}}"])
        .output()
        .map_err(|e| format!("docker not found: {e}"))?;

    if String::from_utf8_lossy(&output.stdout).contains("dfe-kafka") {
        return Ok(false);
    }

    let docker_path = env::var("DFE_DOCKER_PATH").unwrap_or_else(|_| "../dfe-docker".into());

    if !std::path::Path::new(&docker_path)
        .join("docker-compose.yml")
        .exists()
    {
        return Err(format!(
            "dfe-docker not found at {docker_path}. Set DFE_DOCKER_PATH or clone dfe-docker as a sibling."
        ));
    }

    let status = std::process::Command::new("docker")
        .args(["compose", "--profile", "infra", "up", "-d"])
        .current_dir(&docker_path)
        .status()
        .map_err(|e| format!("docker compose failed: {e}"))?;

    if !status.success() {
        return Err("docker compose --profile infra up -d failed".into());
    }

    // Poll TCP reachability at 250ms cadence (4x faster feedback than 1s)
    // up to a 30s ceiling. Avoids blind 1s sleeps between probes — fast
    // services no longer pay the worst-case wait.
    let kf = kafka_test_config();
    for _ in 0..120 {
        if kf.is_reachable() {
            return Ok(true);
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    Err("Kafka did not become healthy within 30s".into())
}

// ── MinIO: the shared dev stack, a precondition rather than a fixture ─

/// Host:port the `MinIO` tests talk to, from `MINIO_ENDPOINT` or the dev default.
fn minio_host_port() -> String {
    load_dotenv();
    let endpoint =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let addr = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("localhost:9000")
        .to_string();
    if addr.contains(':') {
        addr
    } else {
        format!("{addr}:9000")
    }
}

/// Is the shared dev-stack `MinIO` up? Returns false having said why, and having
/// failed the run outright in CI.
///
/// `archiver-minio` in `docker-compose.dev.yaml` is the DEVELOPER's stack, and
/// it is genuinely shared -- one `MinIO` serves the whole suite, and the bucket
/// prefixes keep the tests out of each other's way. That makes it a
/// PRECONDITION, not a fixture: bring it up with
///
///     docker compose -f docker-compose.dev.yaml up -d minio minio-init
///
/// This used to start `MinIO` itself and hand back an RAII guard that ran
/// `compose down` on drop. Two things were wrong with that and neither survives
/// process-per-test. nextest runs every test in its own PROCESS, so all six
/// `MinIO` tests raced to `compose up` and several came away believing they owned
/// the container -- then the first to finish tore it down under the other five.
/// And there is no end-of-suite hook to tear a shared container down from, so
/// the honest options were "leak it" or "do not start it". Starting a container
/// nothing can clean up is what left `archiver-minio` running after the suite.
///
/// Where a test genuinely needs a container of its own it starts one via
/// testcontainers under [`container_name`], which is per-test, labelled, and
/// removed on drop.
pub fn ensure_minio() -> bool {
    let host_port = minio_host_port();
    // 2s, and every resolved address: see `tcp_reachable`.
    if tcp_reachable(&host_port, Duration::from_secs(2)) {
        return true;
    }
    require_service_in_ci(
        "MinIO",
        &format!(
            "nothing accepting TCP on {host_port} -- start the dev stack with \
             `docker compose -f docker-compose.dev.yaml up -d minio minio-init`"
        ),
    );
    false
}

// ── Test data helpers ────────────────────────────────────────────────

/// Generate unique topic name for tests
pub fn test_topic_name(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_millis();
    format!("{prefix}-test-{ts}")
}

/// Generate test JSON message
pub fn test_json_message(id: u64, org_id: &str, event_type: &str) -> Vec<u8> {
    serde_json::json!({
        "id": id,
        "org_id": org_id,
        "event_type": event_type,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "data": {
            "field1": "value1",
            "field2": 123,
            "nested": {
                "key": "value"
            }
        }
    })
    .to_string()
    .into_bytes()
}
