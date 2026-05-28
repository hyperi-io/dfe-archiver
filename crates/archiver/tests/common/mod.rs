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
        first
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .is_some_and(|a| {
                std::net::TcpStream::connect_timeout(&a, Duration::from_secs(3)).is_ok()
            })
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

// ── MinIO lifecycle ──────────────────────────────────────────────────

/// RAII guard that stops docker-compose services on drop — but only if the
/// test started them. If services were already running, the guard leaves them
/// alone so concurrent tests and dev workflows aren't disrupted.
pub struct DockerGuard {
    compose_file: String,
    services: Vec<String>,
    started_by_test: bool,
}

impl DockerGuard {
    /// Mark that the guard doesn't own the containers (pre-existing).
    pub fn noop(compose_file: impl Into<String>) -> Self {
        Self {
            compose_file: compose_file.into(),
            services: Vec::new(),
            started_by_test: false,
        }
    }
}

impl Drop for DockerGuard {
    fn drop(&mut self) {
        if !self.started_by_test || self.services.is_empty() {
            return;
        }
        // Best-effort cleanup; don't panic in Drop.
        let mut args = vec![
            "compose".to_string(),
            "-f".to_string(),
            self.compose_file.clone(),
            "down".to_string(),
        ];
        args.extend(self.services.iter().cloned());
        let _ = std::process::Command::new("docker").args(&args).status();
    }
}

/// Ensure `MinIO` is available for a test.
///
/// Order of preference:
///   1. If `MINIO_ENDPOINT` is reachable — use it (no lifecycle management)
///   2. Otherwise, try to start `MinIO` via `docker-compose.dev.yaml`
///   3. If neither works, return `None` so the caller can skip the test
///
/// Returns a `DockerGuard` that stops the container on drop (only if this
/// call actually started it).
pub fn ensure_minio() -> Option<DockerGuard> {
    load_dotenv();
    let endpoint =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string());

    // Quick reachability check (blocking reqwest would need runtime; use raw TCP)
    let addr = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or("localhost:9000")
        .to_string();
    let host_port = if addr.contains(':') {
        addr
    } else {
        format!("{addr}:9000")
    };

    let already_running = host_port
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|a| std::net::TcpStream::connect_timeout(&a, Duration::from_secs(2)).is_ok());

    if already_running {
        return Some(DockerGuard::noop("docker-compose.dev.yaml"));
    }

    // Try to start via docker compose. The compose file lives at the workspace
    // root, so resolve it relative to CARGO_MANIFEST_DIR (the crate root is
    // crates/archiver — go up two levels).
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let compose_path = manifest
        .parent()
        .and_then(std::path::Path::parent)
        .map(|p| p.join("docker-compose.dev.yaml"))
        .filter(|p| p.exists())?;
    let compose_file = compose_path.to_string_lossy().into_owned();

    let status = std::process::Command::new("docker")
        .args([
            "compose",
            "-f",
            &compose_file,
            "up",
            "-d",
            "minio",
            "minio-init",
        ])
        .status()
        .ok()?;

    if !status.success() {
        return None;
    }

    // Poll TCP reachability at 250ms cadence (4x faster feedback than 1s)
    // up to a 30s ceiling. Connect timeout reduced to 500ms — MinIO is
    // local, anything slower is the container failing to bind.
    for _ in 0..120 {
        if host_port
            .to_socket_addrs()
            .ok()
            .and_then(|mut a| a.next())
            .is_some_and(|a| {
                std::net::TcpStream::connect_timeout(&a, Duration::from_millis(500)).is_ok()
            })
        {
            return Some(DockerGuard {
                compose_file: compose_file.clone(),
                services: vec!["minio".into(), "minio-init".into()],
                started_by_test: true,
            });
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    None
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
