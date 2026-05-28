// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! PGO workload driver for dfe-archiver.
//!
//! Produces realistic Kafka messages so a running, PGO-instrumented
//! `dfe-archiver` accumulates representative profile data across its hot
//! path: Kafka consume → SIMD JSON route lookup → buffer accumulation →
//! compression (zstd) → storage write (file/S3).
//!
//! Invoked by `scripts/pgo-workload.sh` which owns the Kafka testcontainer
//! lifecycle, the destination filesystem, and the archiver process.
//!
//! Built only with `--features pgo-driver`. Main archiver binary unaffected.
//!
//! Configuration via environment variables:
//! - `PGO_DRIVER_DURATION_SECS` (default 300) — total runtime
//! - `PGO_DRIVER_BROKERS` (default `127.0.0.1:19092`)
//! - `PGO_DRIVER_TOPIC` (default `events`) — target topic
//! - `PGO_DRIVER_RPS` (default 5000) — messages per second
//! - `PGO_DRIVER_BATCH_LINGER_MS` (default 10) — librdkafka batching
//! - `PGO_DRIVER_ORG_CARDINALITY` (default 32) — distinct routing keys
//!
//! Exit codes:
//! - 0: workload completed for full duration
//! - 1: fatal setup error (broker unreachable, producer init)

#![allow(clippy::expect_used)] // workload driver, not library code

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cfg = Config::from_env();
    println!("pgo-driver starting: {cfg:#?}");

    let producer: FutureProducer = match ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("client.id", "dfe-archiver-pgo-driver")
        .set("linger.ms", cfg.batch_linger_ms.to_string())
        .set("compression.type", "lz4")
        .set("acks", "1")
        .set("queue.buffering.max.messages", "1000000")
        .set("queue.buffering.max.kbytes", "262144")
        .set("message.timeout.ms", "30000")
        .create()
    {
        Ok(p) => p,
        Err(e) => {
            eprintln!("pgo-driver: producer init failed: {e}");
            std::process::exit(1);
        }
    };
    let producer = Arc::new(producer);

    let stats = Arc::new(Stats::new());
    let deadline = Instant::now() + Duration::from_secs(cfg.duration_secs);

    let mut tasks = JoinSet::new();

    // Three producer tasks split RPS across small/medium/large payloads to
    // exercise sonic-rs routing paths (when expression mode is configured)
    // and the zstd compressor over different compressibility profiles.
    let small_rps = cfg.rps * 60 / 100; // 60% small
    let medium_rps = cfg.rps * 30 / 100; // 30% medium
    let large_rps = cfg.rps - small_rps - medium_rps; // 10% large

    tasks.spawn(produce_loop(
        producer.clone(),
        cfg.clone(),
        stats.clone(),
        deadline,
        small_rps,
        PayloadShape::Small,
    ));
    tasks.spawn(produce_loop(
        producer.clone(),
        cfg.clone(),
        stats.clone(),
        deadline,
        medium_rps,
        PayloadShape::Medium,
    ));
    tasks.spawn(produce_loop(
        producer.clone(),
        cfg.clone(),
        stats.clone(),
        deadline,
        large_rps,
        PayloadShape::Large,
    ));

    // Progress reporter
    let reporter_stats = stats.clone();
    tasks.spawn(async move {
        let mut tick = interval(Duration::from_secs(15));
        tick.tick().await; // skip immediate
        while Instant::now() < deadline {
            tick.tick().await;
            reporter_stats.report();
        }
    });

    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            eprintln!("pgo-driver task error: {e}");
        }
    }

    // Flush outstanding records
    if let Err(e) = producer.flush(Duration::from_secs(10)) {
        eprintln!("pgo-driver: flush error: {e}");
    }

    stats.report();
    println!("pgo-driver: complete");
}

// ===========================================================================
// Config
// ===========================================================================

#[derive(Clone, Debug)]
struct Config {
    duration_secs: u64,
    brokers: String,
    topic: String,
    rps: u32,
    batch_linger_ms: u32,
    org_cardinality: u32,
}

impl Config {
    fn from_env() -> Self {
        Self {
            duration_secs: env_u64("PGO_DRIVER_DURATION_SECS", 300),
            brokers: env_str("PGO_DRIVER_BROKERS", "127.0.0.1:19092"),
            topic: env_str("PGO_DRIVER_TOPIC", "events"),
            rps: env_u32("PGO_DRIVER_RPS", 5000),
            batch_linger_ms: env_u32("PGO_DRIVER_BATCH_LINGER_MS", 10),
            org_cardinality: env_u32("PGO_DRIVER_ORG_CARDINALITY", 32),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ===========================================================================
// Stats
// ===========================================================================

struct Stats {
    sent: AtomicU64,
    enqueue_errors: AtomicU64,
    deliver_errors: AtomicU64,
    start: Instant,
}

impl Stats {
    fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            enqueue_errors: AtomicU64::new(0),
            deliver_errors: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    fn report(&self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        let sent = self.sent.load(Ordering::Relaxed);
        let enq = self.enqueue_errors.load(Ordering::Relaxed);
        let del = self.deliver_errors.load(Ordering::Relaxed);
        println!(
            "pgo-driver [{elapsed:>6.1}s] sent={sent:>9} enq_err={enq} deliver_err={del} rate={:.0}/s",
            (sent as f64) / elapsed.max(1.0)
        );
    }
}

// ===========================================================================
// Producer
// ===========================================================================

#[derive(Clone, Copy)]
enum PayloadShape {
    Small,
    Medium,
    Large,
}

fn rate_limiter(rps: u32) -> tokio::time::Interval {
    let period = Duration::from_nanos(1_000_000_000 / u64::from(rps.max(1)));
    let mut iv = interval(period);
    iv.set_missed_tick_behavior(MissedTickBehavior::Delay);
    iv
}

async fn produce_loop(
    producer: Arc<FutureProducer>,
    cfg: Config,
    stats: Arc<Stats>,
    deadline: Instant,
    rps: u32,
    shape: PayloadShape,
) {
    if rps == 0 {
        return;
    }
    let mut tick = rate_limiter(rps);
    let payloads = build_payloads(shape, cfg.org_cardinality);
    let mut idx = 0u64;

    while Instant::now() < deadline {
        tick.tick().await;
        let payload = &payloads[(idx as usize) % payloads.len()];
        idx = idx.wrapping_add(1);
        // Spread across partitions; archiver routes by topic by default but
        // expression-routing setups key off org_id which lives in the payload.
        let key = format!("k{}", idx % 1024);
        let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(&cfg.topic)
            .payload(payload.as_slice())
            .key(&key);
        // Fire-and-forget: send_result returns immediately. Delivery futures
        // are spawned so we keep producing at the configured rate.
        match producer.send_result(record) {
            Ok(fut) => {
                stats.sent.fetch_add(1, Ordering::Relaxed);
                let stats_inner = stats.clone();
                tokio::spawn(async move {
                    if let Ok(Err((_e, _msg))) = fut.await {
                        stats_inner.deliver_errors.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            Err(_) => {
                stats.enqueue_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

// ===========================================================================
// Payload corpora — exercise route lookup, header extraction, compression
// ===========================================================================

fn build_payloads(shape: PayloadShape, org_cardinality: u32) -> Vec<Vec<u8>> {
    match shape {
        PayloadShape::Small => small_payloads(org_cardinality),
        PayloadShape::Medium => medium_payloads(org_cardinality),
        PayloadShape::Large => large_payloads(org_cardinality),
    }
}

fn small_payloads(org_cardinality: u32) -> Vec<Vec<u8>> {
    // ~150-300 byte events, varied org_id to drive the writer-LRU eviction
    // path and exercise expression-routing lookups. The compressed size is
    // small relative to zstd block overhead so this stresses the
    // accumulator-merge path more than the compressor itself.
    let mut out = Vec::new();
    let sources = ["auth", "api", "admin", "errors"];
    for org in 0..org_cardinality {
        for src in sources {
            let s = format!(
                r#"{{"_timestamp":"2026-04-29T12:00:00.123Z","_source":"{src}","org_id":"org-{org:04}","user":"u-{u}","action":"event","ip":"10.0.0.{ip}"}}"#,
                u = org % 1000,
                ip = org % 255,
            );
            out.push(s.into_bytes());
        }
    }
    out
}

fn medium_payloads(org_cardinality: u32) -> Vec<Vec<u8>> {
    // ~1-2 KB events with nested objects to exercise sonic-rs nested paths
    // and zstd block-level dictionaries (similar fields → high compression).
    let mut out = Vec::new();
    for org in 0..org_cardinality {
        let s = format!(
            r#"{{"_timestamp":"2026-04-29T12:00:00Z","_source":"api","org_id":"org-{org:04}","request":{{"method":"POST","path":"/v1/orders","headers":{{"user-agent":"Mozilla/5.0","accept":"application/json","x-request-id":"req-abc-123-xyz"}},"body_size":4096}},"response":{{"status":201,"body_size":256,"duration_ms":47}},"client":{{"ip":"203.0.113.42","country":"AU","asn":13335}},"user":{{"id":"u-{org}","tenant":"acme","roles":["admin","ops"]}}}}"#
        );
        out.push(s.into_bytes());

        let s = format!(
            r#"{{"_timestamp":"2026-04-29T12:00:01Z","_source":"auth","org_id":"org-{org:04}","tags":{{"event":{{"category":"authentication","outcome":"success"}}}},"actor":{{"username":"svc-pipeline","entity_type":"service_account","mfa":{{"required":true,"factor":"webauthn","verified":true}}}},"target":{{"resource":"prod-cluster","scope":"read"}},"network":{{"src_ip":"198.51.100.7","dst_ip":"10.10.5.2","port":443}}}}"#
        );
        out.push(s.into_bytes());
    }
    out
}

fn large_payloads(org_cardinality: u32) -> Vec<Vec<u8>> {
    // ~8 KB events with array of 50 sub-records — exercises buffer
    // accumulator over varied-size writes and gives zstd a meaningful
    // compressible block.
    let mut out = Vec::new();
    for org in 0..org_cardinality {
        let mut buf = format!(
            r#"{{"_timestamp":"2026-04-29T12:00:00Z","_source":"api","org_id":"org-{org:04}","batch_id":"b-2026-04-29-001","events":["#
        )
        .into_bytes();
        for i in 0..50 {
            if i > 0 {
                buf.push(b',');
            }
            let rec = format!(
                r#"{{"id":{i},"type":"http.request","path":"/api/v1/users/{i}","method":"GET","status":200,"duration_ms":{ms},"client_ip":"10.0.{a}.{b}","user_agent":"Mozilla/5.0"}}"#,
                ms = 5 + (i % 30),
                a = i % 255,
                b = (i * 7) % 255,
            );
            buf.extend_from_slice(rec.as_bytes());
        }
        buf.extend_from_slice(b"]}");
        out.push(buf);
    }
    out
}
