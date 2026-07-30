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

/// Is a Docker daemon reachable?
fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["version", "--format", "{{.Server.Version}}"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Fail when a container this suite starts for itself would not come up.
///
/// Stricter than [`require_service_in_ci`], and deliberately so. That one covers
/// a service the suite does NOT own -- a developer without the dev stack up is a
/// legitimate skip. A container the suite starts is different: if Docker is
/// there, a failure to start it is OUR fault, so skipping hides a broken fixture
/// behind a green run.
///
/// This is not hypothetical. A wait strategy pointed at stdout when
/// fake-gcs-server logs to stderr made all five GCS tests time out at 60s and
/// then report PASS. Sixty seconds of nothing, five times, green.
///
/// Only a genuinely absent Docker still skips.
pub fn require_container_in_ci(service: &str, reason: &str) {
    assert!(
        !docker_available(),
        "{service} would not start although Docker is running ({reason}). \
         This suite owns that container, so a failure to start it is a fault \
         here, not a missing dependency -- skipping would report green while \
         testing nothing."
    );
    require_service_in_ci(service, reason);
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

// ── Container naming and cleanup ──────────────────────────────────────
//
// Every container this suite starts carries a name saying which repo, which
// suite and which service it is, so an operator reading `docker ps` can tell
// what left it behind. testcontainers' default is random hex, untraceable the
// moment one survives.
//
// Naming is PER-TEST -- `dfe-archiver-test-integration-<test>-<service>` --
// because nextest runs each test in its own PROCESS, so two tests calling the
// same `acquire_*` start two containers whatever the name suggests. On one
// shared name the first create wins and the rest fail with "name is already in
// use", fall into their skip branch, and report green while testing nothing.
// Random names already meant one container per test, so per-test naming costs
// nothing; a shared name would be the regression.
//
// The dev-stack containers (`archiver-minio` and friends in
// docker-compose.dev.yaml) are deliberately NOT renamed to this scheme. They
// belong to the developer who ran `docker compose up` and are correctly named
// for that -- see `ensure_minio`.
//
// Cleanup is belt AND braces, because Drop is not enough: normal completion and
// a panic both unwind so Drop stops the container, but a SIGKILL, an abort or
// Ctrl-C does not, and testcontainers 0.27 has no resource reaper. A
// deterministic name makes that leak WORSE than a random one, because the leaked
// container holds the name and every later run fails on it. `reap_stale` closes
// that, so a leak costs the next run nothing.
//
// The label goes on as well, so a sweep can find these when the names are not
// known:
//   docker rm -f $(docker ps -aq --filter label=io.hyperi.test.suite=dfe-archiver-integration)

/// Label marking every container this suite starts, for bulk cleanup.
pub const TEST_SUITE_LABEL: (&str, &str) = ("io.hyperi.test.suite", "dfe-archiver-integration");

/// Labels for a container this suite starts: what it is, and whose run owns it.
///
/// The name says what and why; these say WHO, which is the question someone
/// actually has on finding a leftover. `ps -p <owner-pid>` answers "is that run
/// still going, or is this rubbish I can remove?".
fn test_labels(service: &str) -> Vec<(String, String)> {
    vec![
        (
            TEST_SUITE_LABEL.0.to_string(),
            TEST_SUITE_LABEL.1.to_string(),
        ),
        (
            "io.hyperi.test.repo".to_string(),
            "dfe-archiver".to_string(),
        ),
        ("io.hyperi.test.service".to_string(), service.to_string()),
        (
            "io.hyperi.test.owner-pid".to_string(),
            std::process::id().to_string(),
        ),
    ]
}

/// Container name for a backing service in this suite.
///
/// Pass `Some(test)` -- the owning test -- for anything a test starts for
/// itself, which is everything here. `None` is the form for a container started
/// once for a whole test binary; nothing does that, and using it from several
/// tests would make them collide on the name rather than share the container.
///
/// Names are lowercased with non-alphanumerics collapsed to `-`, because Docker
/// only accepts `[a-zA-Z0-9][a-zA-Z0-9_.-]*` and a Rust test path
/// (`azure::test_azure_basic_operations`) has colons in it -- unnormalised it is
/// rejected at create time as what reads like a Docker fault.
#[must_use]
pub fn container_name(test: Option<&str>, service: &str) -> String {
    let slug = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
    };
    match test {
        Some(t) => format!(
            "dfe-archiver-test-integration-{}-{}",
            slug(t),
            slug(service)
        ),
        None => format!("dfe-archiver-test-integration-{}", slug(service)),
    }
}

/// Remove a DEAD container holding `name`, so a leak from a killed run cannot
/// block this one.
///
/// Only ever call this for a name that belongs to ONE test. On a shared name a
/// peer may have created the container a moment ago and not started it yet, and
/// removing that deletes the work of the run doing the right thing.
///
/// Never touches a RUNNING container even so. Two concurrent runs of this suite
/// on one machine share these names, and force-removing a live one sabotages a
/// process that did nothing wrong -- it surfaces over there as a baffling
/// mid-test failure. Leaving it means the start here fails with "name is already
/// in use", which says what actually happened.
///
/// Best-effort otherwise: no Docker, nothing to remove, or an already-gone
/// container are all fine. A failure here must not fail the test; the start that
/// follows reports the real problem.
pub fn reap_stale(name: &str) {
    let running = std::process::Command::new("docker")
        .args(["ps", "--quiet", "--filter", &format!("name=^{name}$")])
        .output();
    // Non-empty stdout means a container by this name is up. Leave it alone.
    if let Ok(out) = &running
        && !out.stdout.is_empty()
    {
        return;
    }
    let _ = std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

// ── Azurite (Azure Blob emulator) ─────────────────────────────────────

/// Azurite, the Microsoft-published Azure Blob emulator. Pinned rather than
/// `latest`: a floating tag retargets the suite on every image refresh, so a
/// break lands with nothing in the diff to explain it.
///
/// renovate: datasource=docker depName=mcr.microsoft.com/azure-storage/azurite
const AZURITE_TAG: &str = "3.36.0";

/// Azurite's published dev account. Fixed and documented by Microsoft, not a
/// secret -- it only ever unlocks an emulator on localhost.
const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// An Azure Blob target for a test: a live account if one is configured, else an
/// Azurite this fixture owns. Dropping it stops and removes any container.
pub struct AzureFixture {
    /// Config pointed at the target, ready for `ObjectStoreBackend::new_azure`.
    pub config: dfe_archiver::config::AzureConfig,
    container: Option<testcontainers::ContainerAsync<testcontainers::GenericImage>>,
}

impl AzureFixture {
    /// Did this fixture start its own container? False when a live Azure account
    /// is configured, in which case there is nothing for a cleanup test to check.
    #[must_use]
    pub fn manages_container(&self) -> bool {
        self.container.is_some()
    }
}

/// An Azure Blob target for `test`, writing into blob container `container`.
///
/// A live account wins when `AZURE_STORAGE_ACCOUNT` names one that ANSWERS;
/// otherwise an Azurite started for this test alone. Azurite is what makes the
/// Azure backend testable with no Azure subscription: `new_azure` talks to it
/// over the same `object_store` client it uses against Azure Blob.
///
/// The account is probed rather than taken on trust because a stale
/// `AZURE_STORAGE_ACCOUNT` in a developer's `.env` would otherwise silently
/// steer every Azure test away from the emulator and into a DNS failure -- which
/// is exactly what the checked-in `.env` does today: it names an account that
/// does not resolve. Presence of a variable is not evidence of a service.
///
/// Returns `None` when neither is available -- having failed the run in CI,
/// where a skip would be a green job that tested nothing.
pub async fn acquire_azure(test: &str, container: &str) -> Option<AzureFixture> {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    load_dotenv();
    if let Ok(account_name) = env::var("AZURE_STORAGE_ACCOUNT") {
        let host_port = format!("{account_name}.blob.core.windows.net:443");
        if tcp_reachable(&host_port, Duration::from_secs(3)) {
            return Some(AzureFixture {
                config: dfe_archiver::config::AzureConfig {
                    account_name,
                    account_key: env::var("AZURE_STORAGE_KEY")
                        .ok()
                        .map(dfe_archiver::config::sensitive::SensitiveString::from),
                    sas_token: None,
                    container: env::var("AZURE_CONTAINER")
                        .unwrap_or_else(|_| container.to_string()),
                    use_emulator: false,
                    endpoint: None,
                },
                container: None,
            });
        }
        eprintln!(
            "AZURE_STORAGE_ACCOUNT names {host_port}, which does not answer -- using Azurite"
        );
    }

    let name = container_name(Some(test), "azurite");
    // This name belongs to this test alone, so anything holding it is a leak.
    reap_stale(&name);

    let image = GenericImage::new("mcr.microsoft.com/azure-storage/azurite", AZURITE_TAG)
        .with_exposed_port(10000u16.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "Azurite Blob service successfully listens",
        ))
        // Blob only: the queue and table services are not used and each one
        // costs another port and another readiness line to wait on.
        // --blobHost 0.0.0.0 is required or Azurite binds 127.0.0.1 INSIDE the
        // container, where the published port cannot reach it.
        .with_cmd(["azurite-blob", "--blobHost", "0.0.0.0"])
        .with_container_name(&name)
        .with_labels(test_labels("azurite"));

    let started = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            require_container_in_ci("Azurite", &format!("container start failed: {e}"));
            return None;
        }
    };
    let host = match started.get_host().await {
        Ok(h) => h,
        Err(e) => {
            require_container_in_ci("Azurite", &format!("get_host: {e}"));
            return None;
        }
    };
    let port = match started.get_host_port_ipv4(10000u16).await {
        Ok(p) => p,
        Err(e) => {
            require_container_in_ci("Azurite", &format!("get_host_port: {e}"));
            return None;
        }
    };

    // Azurite serves the legacy emulator URL layout, account name in the path:
    // http://host:port/devstoreaccount1/<container>/<blob>. Baking the account
    // into the endpoint is what makes object_store's non-emulator code path
    // produce those URLs -- and the Shared Key signature it computes then
    // matches, because the canonicalised resource is /<account> plus the URL
    // path either way.
    //
    // `use_emulator` is deliberately NOT set. object_store's emulator branch
    // takes its URL from the AZURITE_BLOB_STORAGE_URL env var and ignores
    // `with_endpoint`, so it cannot address a random published port without
    // mutating process env -- unsound under `cargo test`, where tests share a
    // process.
    let endpoint = format!("http://{host}:{port}/{AZURITE_ACCOUNT}");

    if let Err(e) = create_blob_container(&endpoint, container).await {
        require_container_in_ci("Azurite", &format!("could not create blob container: {e}"));
        return None;
    }

    Some(AzureFixture {
        config: dfe_archiver::config::AzureConfig {
            account_name: AZURITE_ACCOUNT.to_string(),
            account_key: Some(dfe_archiver::config::sensitive::SensitiveString::from(
                AZURITE_KEY.to_string(),
            )),
            sas_token: None,
            container: container.to_string(),
            use_emulator: false,
            endpoint: Some(endpoint),
        },
        container: Some(started),
    })
}

/// The API version every signed request below declares.
const AZURITE_API_VERSION: &str = "2025-05-05";

/// Send a Shared Key signed request to Azurite.
///
/// Needed because Azurite rejects unsigned requests (403 `AuthorizationFailure`)
/// and `object_store` exposes neither container creation nor a raw signed
/// request. Recipe per
/// <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>,
/// checked field-by-field against Azurite's own `[STRING TO SIGN]` debug line.
///
/// `resource` is the path under the account (`<container>[/<blob>]`) and `query`
/// is appended to both the URL and the canonicalised resource -- it must already
/// be lowercase and sorted, which for the single-parameter calls here is free.
async fn azurite_signed(
    method: reqwest::Method,
    endpoint: &str,
    resource: &str,
    query: Option<(&str, &str)>,
) -> Result<reqwest::Response, String> {
    use base64::Engine as _;
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    // CanonicalizedResource is /<account> followed by the URL path -- which on
    // the emulator layout already starts with the account, so the account
    // appears twice. That is what Azurite expects.
    let account_path = endpoint
        .split_once("://")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map(|(_, p)| p)
        .ok_or_else(|| format!("endpoint has no account path: {endpoint}"))?;

    let (url, canonical) = match query {
        Some((key, value)) => (
            format!("{endpoint}/{resource}?{key}={value}"),
            format!("/{AZURITE_ACCOUNT}/{account_path}/{resource}\n{key}:{value}"),
        ),
        None => (
            format!("{endpoint}/{resource}"),
            format!("/{AZURITE_ACCOUNT}/{account_path}/{resource}"),
        ),
    };

    let ms_date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();

    // Field order is fixed by the spec: verb, then Content-Encoding,
    // Content-Language, Content-Length, Content-MD5, Content-Type, Date,
    // If-Modified-Since, If-Match, If-None-Match, If-Unmodified-Since, Range.
    // All empty here -- Content-Length is the empty string when zero and Date is
    // empty because x-ms-date carries it -- then the x-ms-* headers, then the
    // canonicalised resource.
    let string_to_sign = format!(
        "{method}\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{ms_date}\nx-ms-version:{AZURITE_API_VERSION}\n{canonical}"
    );

    let key = base64::engine::general_purpose::STANDARD
        .decode(AZURITE_KEY)
        .map_err(|e| format!("emulator key is not base64: {e}"))?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|e| format!("hmac key: {e}"))?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    reqwest::Client::new()
        .request(method.clone(), &url)
        .header("x-ms-date", &ms_date)
        .header("x-ms-version", AZURITE_API_VERSION)
        .header(
            "Authorization",
            format!("SharedKey {AZURITE_ACCOUNT}:{signature}"),
        )
        .send()
        .await
        .map_err(|e| format!("{method} {url}: {e}"))
}

/// Create a blob container, which `object_store` has no API for.
async fn create_blob_container(endpoint: &str, container: &str) -> Result<(), String> {
    let response = azurite_signed(
        reqwest::Method::PUT,
        endpoint,
        container,
        Some(("restype", "container")),
    )
    .await?;

    let status = response.status();
    // 409 ContainerAlreadyExists is success as far as a test is concerned.
    if status.is_success() || status == reqwest::StatusCode::CONFLICT {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    Err(format!("create container {container} -> {status}: {body}"))
}

/// Byte length of a blob, from Azurite, via a signed HEAD.
///
/// `StorageBackend::exists` only reports existence, and existence is true of a
/// zero-byte blob -- so a multipart upload that committed an empty block list
/// would satisfy it. This asks the server how many bytes actually landed.
///
/// Only usable against an Azurite fixture (it signs with the emulator key).
pub async fn azurite_blob_len(
    config: &dfe_archiver::config::AzureConfig,
    prefix: &str,
    path: &str,
) -> Result<u64, String> {
    let endpoint = config
        .endpoint
        .as_deref()
        .ok_or_else(|| "not an Azurite fixture: no endpoint".to_string())?;
    let resource = if prefix.is_empty() {
        format!("{}/{path}", config.container)
    } else {
        format!("{}/{prefix}/{path}", config.container)
    };

    let response = azurite_signed(reqwest::Method::HEAD, endpoint, &resource, None).await?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("HEAD {resource} -> {status}"));
    }
    response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| format!("HEAD {resource} returned no usable Content-Length"))
}

// ── fake-gcs-server (Google Cloud Storage emulator) ───────────────────

/// `fsouza/fake-gcs-server`, the maintained GCS emulator (1.55.1 published
/// 2026-07-19). Pinned, not `latest`, for the same reason as Azurite.
///
/// renovate: datasource=docker depName=fsouza/fake-gcs-server
const FAKE_GCS_TAG: &str = "1.55.1";

/// A GCS target for a test: a live bucket if credentials are configured, else a
/// fake-gcs-server this fixture owns.
pub struct GcsFixture {
    /// Config pointed at the target, ready for `ObjectStoreBackend::new_gcs`.
    pub config: dfe_archiver::config::GcsConfig,
    container: Option<testcontainers::ContainerAsync<testcontainers::GenericImage>>,
}

impl GcsFixture {
    /// Did this fixture start its own container?
    #[must_use]
    pub fn manages_container(&self) -> bool {
        self.container.is_some()
    }
}

/// A GCS target for `test`, writing into `bucket`.
///
/// Live first, but only when `GCS_BUCKET` comes with a credential that actually
/// exists -- `storage.googleapis.com` always resolves, so unlike Azure there is
/// nothing to probe, and a bucket name on its own is not evidence of access. The
/// repo's `.env` sets `GCS_BUCKET` and no credential, which is exactly the case
/// that has to fall through to the emulator rather than fail on ADC lookup.
///
/// Returns `None` when neither is available -- having failed the run in CI.
pub async fn acquire_gcs(test: &str, bucket: &str) -> Option<GcsFixture> {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    load_dotenv();
    if let Ok(live_bucket) = env::var("GCS_BUCKET") {
        let inline_key = env::var("GCS_SERVICE_ACCOUNT_KEY").ok();
        // A path that does not exist is the same defect as no path at all.
        let key_path = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .ok()
            .filter(|p| std::path::Path::new(p).is_file());
        if inline_key.is_some() || key_path.is_some() {
            return Some(GcsFixture {
                config: dfe_archiver::config::GcsConfig {
                    bucket: live_bucket,
                    project_id: None,
                    service_account_key: inline_key
                        .map(dfe_archiver::config::sensitive::SensitiveString::from),
                    credentials_path: key_path,
                },
                container: None,
            });
        }
        eprintln!("GCS_BUCKET is set but no usable credential is -- using fake-gcs-server instead");
    }

    let name = container_name(Some(test), "fake-gcs");
    // This name belongs to this test alone, so anything holding it is a leak.
    reap_stale(&name);

    let image = GenericImage::new("fsouza/fake-gcs-server", FAKE_GCS_TAG)
        .with_exposed_port(4443u16.tcp())
        // STDERR: fake-gcs-server's slog default writes there and stdout stays
        // empty, so waiting on stdout never matches and every test times out.
        .with_wait_for(WaitFor::message_on_stderr("server started at"))
        // -backend memory keeps it disposable, and plain http avoids having to
        // trust the emulator's self-signed certificate from a Rust client.
        .with_cmd(["-scheme", "http", "-backend", "memory"])
        .with_container_name(&name)
        .with_labels(test_labels("fake-gcs"));

    let started = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            require_container_in_ci("fake-gcs-server", &format!("container start failed: {e}"));
            return None;
        }
    };
    let host = match started.get_host().await {
        Ok(h) => h,
        Err(e) => {
            require_container_in_ci("fake-gcs-server", &format!("get_host: {e}"));
            return None;
        }
    };
    let port = match started.get_host_port_ipv4(4443u16).await {
        Ok(p) => p,
        Err(e) => {
            require_container_in_ci("fake-gcs-server", &format!("get_host_port: {e}"));
            return None;
        }
    };
    let base_url = format!("http://{host}:{port}");

    if let Err(e) = fake_gcs_setup(&base_url, bucket).await {
        require_container_in_ci("fake-gcs-server", &e);
        return None;
    }

    Some(GcsFixture {
        config: dfe_archiver::config::GcsConfig {
            bucket: bucket.to_string(),
            project_id: None,
            // object_store reads `gcs_base_url` and `disable_oauth` out of the
            // service-account JSON, and that is the ONLY way to redirect its GCS
            // client -- `GcsConfig` has no endpoint field and the builder's
            // `with_base_url` is not reachable through `new_gcs`. Documented by
            // object_store itself as the emulator recipe, so the archiver
            // already supports this without any production-code change.
            service_account_key: Some(dfe_archiver::config::sensitive::SensitiveString::from(
                format!(
                    r#"{{"gcs_base_url":"{base_url}","disable_oauth":true,"client_email":"","private_key_id":"","private_key":""}}"#
                ),
            )),
            credentials_path: None,
        },
        container: Some(started),
    })
}

/// Point fake-gcs-server at its reachable address and create the bucket.
///
/// `publicHost` is what makes this work at all. `object_store`'s GCS client
/// addresses objects the XML way -- `<base_url>/<bucket>/<object>` -- and
/// fake-gcs-server only routes that path when the request's Host matches its
/// public host, which defaults to `storage.googleapis.com`. Left alone it 404s
/// every head, get and delete, which reads as "the object is not there" rather
/// than "the emulator is not listening on that route". Verified by hand: 404
/// before the call, 200 after it.
///
/// It cannot be passed as a flag at start time because testcontainers only
/// assigns the published port afterwards, hence the runtime config endpoint.
/// `externalUrl` goes with it so any URL the emulator hands back is reachable.
///
/// No call here needs credentials: the emulator has no auth.
async fn fake_gcs_setup(base_url: &str, bucket: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let public_host = base_url.trim_start_matches("http://");

    // Bodies are built as strings rather than with `.json()`: reqwest is carried
    // here without its `json` feature and one emulator call is not a reason to
    // pull serde into the HTTP client.
    let config = client
        .put(format!("{base_url}/_internal/config"))
        .header("Content-Type", "application/json")
        .body(serde_json::json!({ "externalUrl": base_url, "publicHost": public_host }).to_string())
        .send()
        .await
        .map_err(|e| format!("PUT /_internal/config: {e}"))?;
    if !config.status().is_success() {
        return Err(format!(
            "PUT /_internal/config -> {} (without it the emulator 404s every \
             object request)",
            config.status()
        ));
    }

    let created = client
        .post(format!("{base_url}/storage/v1/b?project=dfe-archiver-test"))
        .header("Content-Type", "application/json")
        .body(serde_json::json!({ "name": bucket }).to_string())
        .send()
        .await
        .map_err(|e| format!("create bucket {bucket}: {e}"))?;
    let status = created.status();
    if status.is_success() || status == reqwest::StatusCode::CONFLICT {
        return Ok(());
    }
    let body = created.text().await.unwrap_or_default();
    Err(format!("create bucket {bucket} -> {status}: {body}"))
}

/// Put an object into fake-gcs-server directly, over its JSON media-upload API.
///
/// Needed because the archiver cannot write to this emulator: `object_store`'s
/// GCS `put_multipart` is XML-multipart-only (`POST ?uploads=`) and
/// fake-gcs-server does not implement that API -- it 404s. Seeding from the
/// fixture side is what lets the read, list and delete paths still be tested.
pub async fn fake_gcs_put_object(
    config: &dfe_archiver::config::GcsConfig,
    name: &str,
    body: &[u8],
) -> Result<(), String> {
    let base_url = fake_gcs_base_url(config)?;
    let url = format!(
        "{base_url}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        config.bucket,
        name.replace('/', "%2F")
    );
    let response = reqwest::Client::new()
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .body(body.to_vec())
        .send()
        .await
        .map_err(|e| format!("POST {url}: {e}"))?;
    if response.status().is_success() {
        return Ok(());
    }
    Err(format!("POST {url} -> {}", response.status()))
}

/// The emulator base URL, read back out of the fixture's service-account JSON.
fn fake_gcs_base_url(config: &dfe_archiver::config::GcsConfig) -> Result<String, String> {
    let key = config
        .service_account_key
        .as_ref()
        .ok_or_else(|| "not a fake-gcs-server fixture: no inline key".to_string())?;
    let parsed: serde_json::Value = serde_json::from_str(key.expose())
        .map_err(|e| format!("service account key is not JSON: {e}"))?;
    parsed
        .get("gcs_base_url")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| "not a fake-gcs-server fixture: no gcs_base_url".to_string())
}

/// Byte length of an object, from fake-gcs-server's JSON metadata API.
///
/// `StorageBackend::exists` is true of a zero-byte object, so a resumable upload
/// that committed nothing would satisfy it. This asks the server how many bytes
/// actually landed.
///
/// Only usable against a fake-gcs-server fixture: it reads the emulator base URL
/// back out of the service-account JSON, and sends no credentials.
pub async fn fake_gcs_object_len(
    config: &dfe_archiver::config::GcsConfig,
    prefix: &str,
    path: &str,
) -> Result<u64, String> {
    let base_url = fake_gcs_base_url(config)?;

    let object = if prefix.is_empty() {
        path.to_string()
    } else {
        format!("{prefix}/{path}")
    };
    // The object name is a single path segment in the URL, so its slashes have to
    // be escaped or the API reads them as more segments and 404s.
    let escaped = object.replace('/', "%2F");
    let url = format!("{base_url}/storage/v1/b/{}/o/{escaped}", config.bucket);

    let response = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GET {url} -> {status}"));
    }
    let body = response
        .text()
        .await
        .map_err(|e| format!("reading object metadata: {e}"))?;
    let meta: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("object metadata is not JSON: {e}"))?;
    // GCS reports size as a STRING in JSON, per the API's int64 encoding.
    meta.get("size")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| format!("no usable size in {meta}"))
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
