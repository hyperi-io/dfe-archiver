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
use scalo::{AckHeldSource, SelfRegulationConfig};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Consumer group every archiver in a test joins.
const GROUP: &str = "archivers";

/// The bucket every test archives into.
const BUCKET: &str = "archive";

/// A record carrying `id`, padded with `pad` pseudo-random characters so a
/// producer's compression cannot shrink it back under a ceiling.
fn padded(id: u64, pad: usize) -> Vec<u8> {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut state = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let pad: String = (0..pad)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(ALPHABET[usize::try_from(state % 36).unwrap_or(0)])
        })
        .collect();
    format!(r#"{{"id":{id},"pad":"{pad}"}}"#).into_bytes()
}

/// A record carrying `id`, past a 1 KiB topic ceiling on its own.
fn record(id: u64) -> Vec<u8> {
    padded(id, 3000)
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

/// Dead-letter to `topic` on the source broker, and nowhere else.
fn dead_letter_to(config: &mut Config, topic: &str) {
    config.dlq.enabled = true;
    config.dlq.mode = scalo::dlq::DlqMode::KafkaOnly;
    config.dlq.file.enabled = false;
    config.dlq.kafka.enabled = true;
    config.dlq.kafka.routing = scalo::dlq::DlqRouting::Common;
    config.dlq.kafka.common_topic = topic.to_string();
}

/// Put a file where the archiver stages its object-store files, so every new
/// file fails to open locally with an error the store never gave.
fn block_staging(spool: &Path) {
    let staging = spool.join("uploads");
    std::fs::remove_dir_all(&staging).expect("remove staging");
    std::fs::write(&staging, b"").expect("block staging");
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

/// A batch no file took and the DLQ refused, with a refusal that can clear,
/// ends the loop so the process exits, and nothing commits past the batch.
/// The restart reads it again, and once the DLQ confirms it holds the batch
/// the commit moves past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_the_dlq_refuses_ends_the_loop_and_the_restart_dead_letters_it() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("dlq_refuses").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("dlq_refuses").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;
    kafka.create_topic("dlq", &[]).await;
    // Below the DLQ producer's own ceiling, so the refusal reads as one that can clear.
    kafka
        .create_topic("refusing-dlq", &[("max.message.bytes", "1024")])
        .await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    dead_letter_to(&mut config, "refusing-dlq");
    let (manager, metrics) = metrics();

    kafka.produce("events", &records(0..20)).await;
    let first = archiver(&config, &metrics).await;
    block_staging(spool.path());
    let running = tokio::spawn({
        let first = Arc::clone(&first);
        async move { first.run().await }
    });
    let ended = tokio::time::timeout(Duration::from_secs(45), running)
        .await
        .expect("the loop ran on past a batch the DLQ refused")
        .expect("loop task");
    assert!(
        matches!(ended, Err(dfe_archiver::Error::Withheld { records }) if records > 0),
        "the loop must end with the withheld records, for main to exit non-zero: {ended:?}"
    );
    first.drain().await;
    drop(first);
    assert_eq!(
        kafka.records_in("refusing-dlq"),
        0,
        "the DLQ topic took the batch, so this run tests nothing: its ceiling did not refuse"
    );
    assert_eq!(
        kafka.committed(GROUP, "events"),
        None,
        "the commit passed a batch the DLQ refused"
    );
    assert_eq!(
        counter(&manager, "messages_dropped_total"),
        0,
        "a refusal that can clear dropped records"
    );

    // The restart, with a DLQ that takes the batch and staging still failing.
    std::fs::remove_file(spool.path().join("uploads")).expect("unblock staging");
    dead_letter_to(&mut config, "dlq");
    let second = archiver(&config, &metrics).await;
    block_staging(spool.path());
    let running = tokio::spawn({
        let second = Arc::clone(&second);
        async move { second.run().await }
    });
    wait_until(
        "the commit moved past the dead-lettered records",
        || async { kafka.committed(GROUP, "events") == Some(20) },
    )
    .await;
    assert!(
        !running.is_finished(),
        "a batch the DLQ confirmed ended the loop"
    );
    stop(&second, running).await;
    assert_eq!(
        counter(&manager, "messages_dlq_total"),
        20,
        "the restart dead-letters every record the first run read"
    );
    assert!(kafka.records_in("dlq") > 0, "the DLQ topic holds the batch");
    assert_eq!(counter(&manager, "messages_dropped_total"), 0);
    assert!(
        minio.keys(BUCKET).await.is_empty(),
        "nothing reached the store"
    );
}

/// Under the 16 MiB record ceiling as a record, over every DLQ backend's
/// ceiling as a dead letter once base64 grows the payload by a third.
fn oversize_record(id: u64) -> Vec<u8> {
    padded(id, 13_000_000)
}

/// A batch the store refused for good, over every DLQ backend's ceiling as a
/// whole, is dead-lettered a record at a time: each record the DLQ can hold
/// reaches it intact, only the record too large on its own is dropped and
/// counted, the commit moves past all of them, and the loop runs on instead
/// of restarting into the same refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_batch_drops_only_the_record_no_dlq_can_hold() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("dlq_never_holds").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("dlq_never_holds").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;
    kafka.create_topic("dlq", &[]).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    // Past the 1024-byte object key limit, so every file's key is refused.
    config.archive.path_template = format!("{}/{{year}}", "k".repeat(1100));
    // Long enough that the small records wait for the oversize one, which
    // flushes the buffer as one batch.
    config.buffer.flush_age_secs = 10;
    dead_letter_to(&mut config, "dlq");
    let (manager, metrics) = metrics();

    kafka.produce("events", &records(0..20)).await;
    kafka.produce("events", &[oversize_record(20)]).await;
    let archiver = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let archiver = Arc::clone(&archiver);
        async move { archiver.run().await }
    });
    wait_until("the oversize record was dropped", || async {
        counter(&manager, "messages_dropped_total") >= 1
    })
    .await;
    wait_until("the commit moved past every record", || async {
        kafka.committed(GROUP, "events") == Some(21)
    })
    .await;
    assert!(
        !running.is_finished(),
        "a dead letter no DLQ can hold ended the loop"
    );
    stop(&archiver, running).await;

    assert_eq!(
        counter(&manager, "messages_dropped_total"),
        1,
        "only the record too large on its own is dropped"
    );
    assert_eq!(counter(&manager, "messages_dlq_total"), 20);
    assert_dead_letters_are(&kafka.read_all("dlq"), 0..20);
    assert!(
        minio.keys(BUCKET).await.is_empty(),
        "nothing reached the store"
    );
}

/// A batch that failed on local disk is never dropped, even when no DLQ
/// backend can hold its dead letter: the loop ends, nothing commits, and once
/// the disk recovers the restart archives the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_failure_no_dlq_can_hold_replays_and_lands_after_the_restart() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("local_failure_oversize").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("local_failure_oversize").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;
    kafka.create_topic("dlq", &[]).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    config.archive.roll_interval_secs = Some(2);
    dead_letter_to(&mut config, "dlq");
    let (manager, metrics) = metrics();

    kafka.produce("events", &[oversize_record(0)]).await;
    let first = archiver(&config, &metrics).await;
    block_staging(spool.path());
    let running = tokio::spawn({
        let first = Arc::clone(&first);
        async move { first.run().await }
    });
    let ended = tokio::time::timeout(Duration::from_secs(45), running)
        .await
        .expect("the loop ran on past a batch it could neither write nor dead-letter")
        .expect("loop task");
    assert!(
        matches!(ended, Err(dfe_archiver::Error::Withheld { records }) if records > 0),
        "the loop must end with the withheld record, for main to exit non-zero: {ended:?}"
    );
    first.drain().await;
    drop(first);
    assert_eq!(
        kafka.committed(GROUP, "events"),
        None,
        "the commit passed a record that failed locally"
    );
    assert_eq!(
        counter(&manager, "messages_dropped_total"),
        0,
        "a local failure dropped a record"
    );
    assert_eq!(kafka.records_in("dlq"), 0, "nothing reached the DLQ topic");

    // The restart, with local staging working again.
    std::fs::remove_file(spool.path().join("uploads")).expect("unblock staging");
    let second = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let second = Arc::clone(&second);
        async move { second.run().await }
    });
    wait_until("the restart archived the record", || async {
        counter(&manager, "messages_archived_total") >= 1
    })
    .await;
    stop(&second, running).await;
    assert_eq!(
        ids(&minio.lines(BUCKET).await),
        vec![0],
        "the restart archives the record, once"
    );
    assert_eq!(
        kafka.committed(GROUP, "events"),
        Some(1),
        "the commit follows once the record is archived"
    );
    assert_eq!(counter(&manager, "messages_dropped_total"), 0);
}

/// A file the store refuses for good at upload is read back and dead-lettered
/// a record at a time: every record reaches the DLQ intact, none is dropped,
/// and the commit moves past them once the DLQ confirms them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_the_store_refuses_at_upload_dead_letters_every_record() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("upload_refused").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("upload_refused").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;
    kafka.create_topic("dlq", &[]).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    config.archive.roll_interval_secs = Some(2);
    // Under the 1024-byte key limit checked before staging, one segment past
    // the store's 255, so the store refuses the file when it uploads.
    config.archive.path_template = format!("{}/{{year}}", "s".repeat(300));
    // The file is read back through the codec it was written with.
    config.compression.enabled = true;
    dead_letter_to(&mut config, "dlq");
    let (manager, metrics) = metrics();

    kafka.produce("events", &records(0..20)).await;
    let archiver = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let archiver = Arc::clone(&archiver);
        async move { archiver.run().await }
    });
    wait_until("the commit moved past every record", || async {
        kafka.committed(GROUP, "events") == Some(20)
    })
    .await;
    assert!(
        !running.is_finished(),
        "a file the store refused ended the loop"
    );
    stop(&archiver, running).await;

    assert_eq!(
        counter(&manager, "messages_dropped_total"),
        0,
        "nothing is dropped"
    );
    assert_eq!(counter(&manager, "messages_dlq_total"), 20);
    assert_eq!(counter(&manager, "messages_archived_total"), 0);
    assert_dead_letters_are(&kafka.read_all("dlq"), 0..20);
    assert!(
        minio.keys(BUCKET).await.is_empty(),
        "nothing reached the store"
    );
}

/// Assert the DLQ `entries` are one per record of `ids`, each intact and
/// carrying the routed destination a batch carries, not the file path.
fn assert_dead_letters_are(entries: &[Vec<u8>], ids_expected: std::ops::Range<u64>) {
    let mut dead: Vec<(u64, Vec<u8>)> = entries
        .iter()
        .map(|bytes| {
            let entry: scalo::dlq::DlqEntry = serde_json::from_slice(bytes).expect("a DLQ entry");
            assert_eq!(
                entry.destination.as_deref(),
                Some("events"),
                "a dead letter names the routed destination"
            );
            let id = ids(&[String::from_utf8(entry.payload.clone()).expect("utf-8")])[0];
            (id, entry.payload)
        })
        .collect();
    dead.sort();
    assert_eq!(
        dead.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        ids_expected.collect::<Vec<_>>(),
        "one dead letter per record"
    );
    for (id, payload) in &dead {
        assert_eq!(payload, &record(*id), "record {id} reached the DLQ intact");
    }
}

/// A file the store refuses for good at upload, holding one record too large
/// for any DLQ backend beside records that fit, dead-letters every record
/// that fits, intact, and drops and counts only the one too large.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_refused_at_upload_drops_only_the_record_no_dlq_can_hold() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("upload_refused_oversize").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("upload_refused_oversize").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;
    kafka.create_topic("dlq", &[]).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    config.archive.roll_interval_secs = Some(5);
    // Under the 1024-byte key limit checked before staging, one segment past
    // the store's 255, so the store refuses the file when it uploads.
    config.archive.path_template = format!("{}/{{year}}", "s".repeat(300));
    config.compression.enabled = true;
    dead_letter_to(&mut config, "dlq");
    let (manager, metrics) = metrics();

    kafka.produce("events", &records(0..20)).await;
    kafka.produce("events", &[oversize_record(20)]).await;
    let archiver = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let archiver = Arc::clone(&archiver);
        async move { archiver.run().await }
    });
    wait_until("the commit moved past every record", || async {
        kafka.committed(GROUP, "events") == Some(21)
    })
    .await;
    assert!(
        !running.is_finished(),
        "a file the store refused ended the loop"
    );
    stop(&archiver, running).await;

    assert_eq!(
        counter(&manager, "messages_dropped_total"),
        1,
        "only the record too large on its own is dropped"
    );
    assert_eq!(counter(&manager, "messages_dlq_total"), 20);
    assert_eq!(counter(&manager, "messages_archived_total"), 0);
    assert_dead_letters_are(&kafka.read_all("dlq"), 0..20);
    assert!(
        minio.keys(BUCKET).await.is_empty(),
        "nothing reached the store"
    );
}

/// While the governor holds intake the Push listener refuses every push as
/// backpressure, so the sender keeps the record, and it takes pushes again
/// once the pressure clears.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_push_listener_refuses_pushes_while_the_governor_holds_intake() {
    init_logs();
    let Some(minio) = common::acquire_minio("push_under_pressure").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let port = common::free_low_port(&[]);
    let mut config = archive_to(&minio, spool.path());
    config.transport = TRANSPORT_GRPC.to_string();
    config.grpc.listen = Some(format!("127.0.0.1:{port}"));
    let (manager, metrics) = metrics();

    let governor = SelfRegulationConfig::default()
        .build(Arc::new(MemoryGuard::new(MemoryGuardConfig::default())))
        .expect("self-regulation is on by default");
    // A hard source that holds intake while it reads its cap.
    let brake = Arc::new(AtomicU64::new(0));
    governor
        .pressure()
        .attach_source(Arc::new(AckHeldSource::new(Arc::clone(&brake), 1)));
    let archiver = Arc::new(
        Archiver::new(
            SharedConfig::new(config),
            Arc::clone(&metrics),
            Arc::new(MemoryGuard::new(MemoryGuardConfig::default())),
            Some(&governor),
            None,
        )
        .await
        .expect("archiver"),
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
    let accepted = |id: u64| {
        let sender = &sender;
        async move {
            matches!(
                sender.send("traffic", record(id).into()).await,
                SendResult::Ok
            )
        }
    };
    wait_until("the listener takes pushes", || accepted(0)).await;

    brake.store(1, Ordering::Release);
    let refused = sender.send("traffic", record(1).into()).await;
    assert!(
        refused.is_backpressured(),
        "a push under pressure was answered: {refused:?}"
    );

    brake.store(0, Ordering::Release);
    wait_until("the listener takes pushes again", || accepted(2)).await;
    wait_until("both accepted pushes were written", || async {
        counter(&manager, "messages_written_total") >= 2
    })
    .await;
    stop(&archiver, running).await;

    assert_eq!(
        ids(&minio.lines(BUCKET).await),
        vec![0, 2],
        "only the pushes the listener answered are archived"
    );
}

/// The reason every depth refusal carries into the DLQ.
const TOO_DEEP: &str = "payload nesting exceeds the maximum parse depth of 64";

fn nested_array(depth: usize) -> Vec<u8> {
    format!("{}1{}", "[".repeat(depth), "]".repeat(depth)).into_bytes()
}

fn nested_object(depth: usize) -> Vec<u8> {
    format!("{}1{}", "{\"a\":".repeat(depth), "}".repeat(depth)).into_bytes()
}

/// Records nested far past the parse depth, each of which overflows a 2 MiB
/// worker stack if expression routing parses it.
fn too_deep_records() -> Vec<Vec<u8>> {
    vec![nested_object(20_000), nested_array(100_000)]
}

/// A record nested past the parse depth is dead-lettered as it arrived, under
/// its topic, while the records around it are archived: the commit moves past
/// all of them and the loop keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deeply_nested_record_is_dead_lettered_and_the_rest_are_archived() {
    init_logs();
    let Some(kafka) = common::acquire_kafka("too_deep").await else {
        return;
    };
    let Some(minio) = common::acquire_minio("too_deep").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;
    kafka.create_topic("events", &[]).await;
    kafka.create_topic("dlq", &[]).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let mut config = kafka_to(&kafka, &minio, spool.path(), "events");
    // Expression routing is the mode that parses each record.
    config.routing.mode = "expression".to_string();
    config.archive.roll_interval_secs = Some(2);
    dead_letter_to(&mut config, "dlq");
    let (manager, metrics) = metrics();

    let deep = too_deep_records();
    kafka.produce("events", &records(0..10)).await;
    kafka.produce("events", &deep).await;
    kafka.produce("events", &records(10..20)).await;
    let archiver = archiver(&config, &metrics).await;
    let running = tokio::spawn({
        let archiver = Arc::clone(&archiver);
        async move { archiver.run().await }
    });
    wait_until("the commit moved past every record", || async {
        kafka.committed(GROUP, "events") == Some(22)
    })
    .await;
    assert!(
        !running.is_finished(),
        "a deeply nested record ended the loop"
    );
    stop(&archiver, running).await;

    assert_eq!(counter(&manager, "messages_dlq_total"), 2);
    assert_eq!(counter(&manager, "messages_dropped_total"), 0);
    assert_eq!(
        ids(&minio.lines(BUCKET).await),
        (0..20).collect::<Vec<_>>(),
        "every record around the deep ones is archived, once"
    );
    let mut dead: Vec<Vec<u8>> = kafka
        .read_all("dlq")
        .iter()
        .map(|bytes| {
            let entry: scalo::dlq::DlqEntry = serde_json::from_slice(bytes).expect("a DLQ entry");
            assert_eq!(entry.reason, TOO_DEEP);
            assert_eq!(entry.destination.as_deref(), Some("events"));
            let source = entry
                .source
                .expect("a dead letter names where it came from");
            assert_eq!(source.topic.as_deref(), Some("events"));
            entry.payload
        })
        .collect();
    dead.sort();
    let mut want = deep;
    want.sort();
    assert!(
        dead == want,
        "each deep record reaches the DLQ as it arrived"
    );
}

/// With no DLQ, as on the direct transport, a record nested past the parse
/// depth is dropped and counted, and the records around it are archived.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deeply_nested_record_with_no_dlq_is_dropped_and_the_rest_are_archived() {
    init_logs();
    let Some(minio) = common::acquire_minio("too_deep_no_dlq").await else {
        return;
    };
    minio.create_bucket(BUCKET).await;

    let spool = tempfile::TempDir::new().expect("spool");
    let port = common::free_low_port(&[]);
    let mut config = archive_to(&minio, spool.path());
    config.transport = TRANSPORT_GRPC.to_string();
    config.grpc.listen = Some(format!("127.0.0.1:{port}"));
    config.routing.mode = "expression".to_string();
    let (manager, metrics) = metrics();

    let archiver = archiver(&config, &metrics).await;
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
    let push = |payload: Vec<u8>| {
        let sender = &sender;
        async move {
            let deadline = Instant::now() + Duration::from_secs(45);
            while !matches!(
                sender.send("traffic", payload.clone().into()).await,
                SendResult::Ok
            ) {
                assert!(Instant::now() < deadline, "the listener never took a push");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    push(record(0)).await;
    for payload in too_deep_records() {
        push(payload).await;
    }
    push(record(1)).await;

    wait_until("both deep records were dropped", || async {
        counter(&manager, "messages_dropped_total") >= 2
    })
    .await;
    wait_until("both records around them were written", || async {
        counter(&manager, "messages_written_total") >= 2
    })
    .await;
    assert!(
        !running.is_finished(),
        "a deeply nested record ended the loop"
    );
    stop(&archiver, running).await;

    assert_eq!(counter(&manager, "messages_dropped_total"), 2);
    assert_eq!(counter(&manager, "messages_dlq_total"), 0);
    assert_eq!(
        ids(&minio.lines(BUCKET).await),
        vec![0, 1],
        "the records around the deep ones are archived"
    );
}
