// Project:   dfe-archiver
// File:      crates/archiver/tests/e2e/at_least_once.rs
// Purpose:   At-least-once delivery against a real MinIO and Kafka
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! At-least-once delivery, proved against a real `MinIO` and a real Kafka.
//!
//! Every test starts the containers it needs, mapped to host ports below 10240,
//! and removes them when it ends. Not `#[ignore]`d, as with the other
//! object-store e2e tests: the default run needs a Docker daemon.

use crate::common::{self, KafkaFixture, MinioFixture};
use dfe_archiver::Archiver;
use dfe_archiver::archive::{ArchiveWriter, RollingPolicy};
use dfe_archiver::compression::create_compressor;
use dfe_archiver::config::{ArchiveConfig, Config, SharedConfig, TRANSPORT_GRPC};
use dfe_archiver::io::create_backend;
use dfe_archiver::metrics::ArchiverMetrics;
use scalo::memory::{MemoryGuard, MemoryGuardConfig};
use scalo::metrics::{MetricsConfig, MetricsManager};
use scalo::transport::{GrpcConfig, GrpcTransport, SendResult, TransportSender};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Consumer group every archiver in a test joins.
const GROUP: &str = "archivers";

/// The bucket every test archives into.
const BUCKET: &str = "archive";

/// A record carrying `id`, padded past the refusing DLQ topic's 1 KiB ceiling
/// on its own. The padding is pseudo-random so the DLQ producer's compression
/// cannot bring a record back under the ceiling.
fn record(id: u64) -> Vec<u8> {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut state = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let pad: String = (0..3000)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(ALPHABET[usize::try_from(state % 36).unwrap_or(0)])
        })
        .collect();
    format!(r#"{{"id":{id},"pad":"{pad}"}}"#).into_bytes()
}

fn records(ids: std::ops::Range<u64>) -> Vec<Vec<u8>> {
    ids.map(record).collect()
}

/// The ids of the archived records, sorted, with any duplicate kept.
fn ids(lines: &[String]) -> Vec<u64> {
    let mut ids: Vec<u64> = lines
        .iter()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|value| value.get("id").and_then(serde_json::Value::as_u64))
                .unwrap_or_else(|| panic!("an archived line carries no id: {line}"))
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// The archiver counter `name` as the offline manager renders it, or 0 before
/// anything recorded it.
fn counter(manager: &MetricsManager, name: &str) -> u64 {
    let prefix = format!("archiver_{name} ");
    manager
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

/// Whether the delivery-guarantee gauge reads 1 for `guarantee` and `reason`.
fn reports_guarantee(manager: &MetricsManager, guarantee: &str, reason: &str) -> bool {
    manager.render().lines().any(|line| {
        line.starts_with("archiver_pipeline_delivery_guarantee{")
            && line.contains(&format!(r#"guarantee="{guarantee}""#))
            && line.contains(&format!(r#"reason="{reason}""#))
            && line.ends_with(" 1")
    })
}

/// Poll `condition` every 100 ms until it holds, failing after 45 s.
async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(45);
    while !condition().await {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Log the pipeline at info, which nextest prints when a test fails or times out.
fn init_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,librdkafka=warn")
        .try_init();
}

/// One manager per test process: the first installs the global recorder, and a
/// second would render nothing.
fn metrics() -> (MetricsManager, Arc<ArchiverMetrics>) {
    let manager = MetricsManager::with_config(MetricsConfig::offline("archiver"));
    let metrics = Arc::new(ArchiverMetrics::register(&manager, "test"));
    (manager, metrics)
}

/// An archiver on `config`, not yet running.
async fn archiver(config: &Config, metrics: &Arc<ArchiverMetrics>) -> Arc<Archiver> {
    Arc::new(
        Archiver::new(
            SharedConfig::new(config.clone()),
            Arc::clone(metrics),
            Arc::new(MemoryGuard::new(MemoryGuardConfig::default())),
            None,
            None,
        )
        .await
        .expect("archiver"),
    )
}

/// Stop `archiver` the way SIGTERM does: end the loop, then drain.
async fn stop(archiver: &Archiver, running: tokio::task::JoinHandle<dfe_archiver::Result<()>>) {
    archiver.shutdown();
    running.await.expect("loop task").expect("loop");
    archiver.drain().await;
}

/// Uncompressed NDJSON under `minio://archive/archive`, flushed into the open
/// file every second and routed by topic.
fn archive_to(minio: &MinioFixture, spool: &Path) -> Config {
    let mut config = Config::default();
    config.archive.destination = format!("minio://{BUCKET}/archive");
    config.archive.minio = Some(minio.config(BUCKET));
    config.compression.enabled = false;
    config.routing.mode = "topic".to_string();
    config.buffer.flush_age_secs = 1;
    config.buffer.spool_dir = spool.display().to_string();
    config.dlq.enabled = false;
    config
}

/// [`archive_to`], reading `topic` from `kafka` in [`GROUP`].
fn kafka_to(kafka: &KafkaFixture, minio: &MinioFixture, spool: &Path, topic: &str) -> Config {
    let mut config = archive_to(minio, spool);
    config.kafka.brokers = vec![kafka.bootstrap.clone()];
    config.kafka.group_id = GROUP.to_string();
    config.kafka.topics = vec![topic.to_string()];
    config.kafka.batch_size = 1000;
    config
}

/// Two replicas archiving the same destination in the same window each run a
/// multipart upload, and the later completion must not replace the earlier
/// object.
#[tokio::test]
async fn two_writers_on_one_destination_and_window_write_two_objects() {
    let Some(minio) = common::acquire_minio("two_writers_on_one_destination").await else {
        return; // no Docker; acquire_minio said so and failed the run in CI
    };
    minio.create_bucket(BUCKET).await;
    let (_staging_dir, staging) = common::staging();

    let archive = ArchiveConfig {
        destination: format!("minio://{BUCKET}/events"),
        minio: Some(minio.config(BUCKET)),
        ..ArchiveConfig::default()
    };
    let writer = || {
        ArchiveWriter::new(
            archive.clone(),
            RollingPolicy::default(),
            create_compressor("none", 0).expect("compressor"),
            create_backend(&archive, &staging).expect("backend"),
        )
    };
    let mut first = writer();
    let mut second = writer();

    // Both files are open before either completes, as on two replicas.
    first
        .write_record(br#"{"replica":1}"#)
        .await
        .expect("first write");
    second
        .write_record(br#"{"replica":2}"#)
        .await
        .expect("second write");
    first.close().await.expect("first close");
    second.close().await.expect("second close");
    common::upload_closed(&mut first).await;
    common::upload_closed(&mut second).await;

    let keys = minio.keys(BUCKET).await;
    assert_eq!(
        keys.len(),
        2,
        "one replica's object replaced the other's: {keys:?}"
    );
    let mut lines = minio.lines(BUCKET).await;
    lines.sort();
    assert_eq!(lines, vec![r#"{"replica":1}"#, r#"{"replica":2}"#]);
}

/// An archiver killed before its file rolls never committed the records the
/// open file held, so the next one reads every one of them again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_before_roll_re_reads_the_records_the_open_file_held() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("kill_before_roll").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("kill_before_roll").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;
    kafka.produce("events", &records(0..50)).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let config = kafka_to(&kafka, &minio, spool.path(), "events");
    let (manager, metrics) = metrics();

    let first = archiver(&config, &metrics).await;
    assert!(
        reports_guarantee(&manager, "at_least_once", "confirmed"),
        "Kafka into an object store must report at_least_once:\n{}",
        manager.render()
    );
    let running = tokio::spawn({
        let first = Arc::clone(&first);
        async move { first.run().await }
    });
    wait_until(
        "the first archiver wrote all 50 records into its open file",
        || async { counter(&manager, "messages_written_total") >= 50 },
    )
    .await;
    assert_eq!(
        kafka.committed(GROUP, "events"),
        None,
        "an offset was committed while the file holding its record was still open"
    );
    assert!(minio.keys(BUCKET).await.is_empty(), "no file has completed");

    // The kill: no drain, so the open upload is abandoned.
    running.abort();
    let _ = running.await;
    drop(first);

    let second = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let second = Arc::clone(&second);
        async move { second.run().await }
    });
    wait_until("the second archiver wrote all 50 records again", || async {
        counter(&manager, "messages_written_total") >= 100
    })
    .await;
    stop(&second, running).await;

    let keys = minio.keys(BUCKET).await;
    assert_eq!(
        keys.len(),
        1,
        "only the second archiver's file completed: {keys:?}"
    );
    assert_eq!(
        ids(&minio.lines(BUCKET).await),
        (0..50).collect::<Vec<_>>(),
        "every record the killed archiver held is archived, once"
    );
    assert_eq!(
        kafka.committed(GROUP, "events"),
        Some(50),
        "the completed file releases its records, and the commit follows"
    );
}

/// A graceful stop closes the listener first and writes every push it
/// already answered: the answer is the only acknowledgement the sender gets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_graceful_stop_under_traffic_writes_every_record_it_answered() {
    init_logs();
    let Some(minio) = common::acquire_minio("graceful_stop_under_traffic").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let port = common::free_low_port(&[]);
    let mut config = archive_to(&minio, spool.path());
    config.transport = TRANSPORT_GRPC.to_string();
    config.grpc.listen = Some(format!("127.0.0.1:{port}"));
    let (manager, metrics) = metrics();

    let archiver = archiver(&config, &metrics).await;
    assert!(
        reports_guarantee(&manager, "best_effort", "sink_cannot_confirm"),
        "the direct path answers at enqueue, so it must report best_effort:\n{}",
        manager.render()
    );
    let running = tokio::spawn({
        let archiver = Arc::clone(&archiver);
        async move { archiver.run().await }
    });

    let sender = GrpcTransport::new(&GrpcConfig {
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        ..GrpcConfig::default()
    })
    .await
    .expect("sender");
    let pushing = Arc::new(AtomicBool::new(true));
    let answered_count = Arc::new(AtomicU64::new(0));
    let traffic = tokio::spawn({
        let pushing = Arc::clone(&pushing);
        let answered_count = Arc::clone(&answered_count);
        async move {
            let mut answered = Vec::new();
            let mut id = 0u64;
            while pushing.load(Ordering::Acquire) {
                match sender.send("traffic", record(id).into()).await {
                    SendResult::Ok => {
                        answered.push(id);
                        answered_count.fetch_add(1, Ordering::Release);
                    }
                    // Refused once the listener closes; a refused push is not acknowledged.
                    _ => tokio::time::sleep(Duration::from_millis(10)).await,
                }
                id += 1;
            }
            answered
        }
    });

    wait_until("records are flowing into the archive files", || async {
        counter(&manager, "messages_written_total") >= 200
    })
    .await;

    // SIGTERM ends the loop first; the listener keeps answering until the
    // drain closes it, so pushes answered from here on wait in its queue.
    archiver.shutdown();
    running.await.expect("loop task").expect("loop");
    let at_stop = answered_count.load(Ordering::Acquire);
    wait_until("pushes are answered after the loop stopped", || async {
        answered_count.load(Ordering::Acquire) >= at_stop + 100
    })
    .await;
    archiver.drain().await;
    pushing.store(false, Ordering::Release);
    let answered = traffic.await.expect("sender task");

    let stored: BTreeSet<u64> = ids(&minio.lines(BUCKET).await).into_iter().collect();
    let lost: Vec<u64> = answered
        .iter()
        .copied()
        .filter(|id| !stored.contains(id))
        .collect();
    assert!(
        lost.is_empty(),
        "{} of {} answered records were not archived, first {:?}",
        lost.len(),
        answered.len(),
        &lost[..lost.len().min(10)]
    );
}

/// A store that fails every upload until it comes back costs time, never
/// records: the staged file and its offsets are kept, the loop keeps running,
/// and once the store answers every record lands once and the commit follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_that_fails_until_it_recovers_loses_nothing() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("store_recovers").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("store_recovers").await else {
        return;
    };
    kafka.create_topic("events", &[]).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    config.archive.roll_interval_secs = Some(2);
    let (manager, metrics) = metrics();

    // The bucket does not exist yet, so every upload fails until it does.
    kafka.produce("events", &records(0..20)).await;
    let archiver = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let archiver = Arc::clone(&archiver);
        async move { archiver.run().await }
    });
    wait_until("three uploads failed", || async {
        counter(&manager, "archive_errors_total") >= 3
    })
    .await;
    assert!(!running.is_finished(), "a failing store ended the loop");
    assert_eq!(
        kafka.committed(GROUP, "events"),
        None,
        "a record was committed before its file reached the store"
    );
    assert_eq!(
        counter(&manager, "messages_archived_total"),
        0,
        "a record was counted archived before its file reached the store"
    );

    minio.create_bucket(BUCKET).await;
    wait_until("every record landed once the store came back", || async {
        counter(&manager, "messages_archived_total") >= 20
    })
    .await;
    stop(&archiver, running).await;
    assert_eq!(
        ids(&minio.lines(BUCKET).await),
        (0..20).collect::<Vec<_>>(),
        "every record lands, once"
    );
    assert_eq!(
        kafka.committed(GROUP, "events"),
        Some(20),
        "the confirmed upload releases the records, and the commit follows"
    );
    assert_eq!(
        counter(&manager, "messages_archived_total"),
        20,
        "each record is counted once, when the store confirms its file"
    );
    assert_eq!(counter(&manager, "messages_dropped_total"), 0);
}

/// A key the store can never accept is refused before any record is staged.
/// With no DLQ to take the batch its records are dropped with the reason and
/// counted, the commit moves past them, and the loop keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_the_store_refuses_for_good_is_dropped_and_the_commit_moves_on() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("store_refuses").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("store_refuses").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    // Past the 1024-byte object key limit, so every file's key is refused.
    config.archive.path_template = format!("{}/{{year}}", "k".repeat(1100));
    let (manager, metrics) = metrics();

    kafka.produce("events", &records(0..20)).await;
    let archiver = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let archiver = Arc::clone(&archiver);
        async move { archiver.run().await }
    });
    wait_until("the refused batch was dropped", || async {
        counter(&manager, "messages_dropped_total") >= 20
    })
    .await;
    wait_until("the commit moved past the dropped records", || async {
        kafka.committed(GROUP, "events") == Some(20)
    })
    .await;
    assert!(!running.is_finished(), "a refused object ended the loop");
    stop(&archiver, running).await;

    assert_eq!(counter(&manager, "messages_dropped_total"), 20);
    assert_eq!(counter(&manager, "messages_archived_total"), 0);
    assert!(
        minio.keys(BUCKET).await.is_empty(),
        "nothing reached the store"
    );
}
