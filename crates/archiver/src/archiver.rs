// Project:   dfe-archiver
// File:      crates/archiver/src/archiver.rs
// Purpose:   Main archiver pipeline orchestrator
// Language:  Rust
//
// License:      BUSL-1.1
// Copyright:    (c) 2026 HyperI Pty Ltd

//! # Archiver Pipeline
//!
//! Orchestrates the complete Kafka -> Buffer -> Archive -> Storage pipeline.
//!
//! ```text
//! Kafka Consumer -> Router -> Buffer Manager -> Archive Writer -> Storage
//!       |             |           |                |              |
//!   Batch recv    Dest key    Per-dest        Compressed      S3/MinIO/
//!   (10K msgs)    routing     buffering       rolling         File/etc
//! ```
//!
//! A record's offset is released only when the archive file holding it is
//! durable. An object-store file is written to local staging and uploaded
//! whole once it closes, in a background task that retries a failed upload
//! with backoff and jitter until the store takes it, so an outage costs disk
//! and consumer lag, never records. On Kafka the armed consumer commits each
//! partition up to its lowest offset not yet released, so one destination's
//! roll never commits past a record another destination still holds.

use crate::config::{Config, SharedConfig};
use crate::metrics::ArchiverMetrics;
use dfe_archiver_core::archive::{ArchiveWriter, PendingFile, RollingPolicy, Settled};
use dfe_archiver_core::buffer::{StagedBatch, TieredBufferManager};
use dfe_archiver_core::compression::{Compressor, compressor_for};
use dfe_archiver_core::config::{ArchiveConfig, HELD_OFFSET_BYTES};
use dfe_archiver_core::routing::Router;
use dfe_archiver_core::routing::depth::MAX_PARSE_DEPTH;
use dfe_archiver_core::storage::{PendingUpload, probe_sink};
use dfe_archiver_core::types::{KafkaMessage, KafkaOffset, OffsetSet};
use dfe_archiver_core::{Error, Result};
use dfe_archiver_io::storage::create_backend;
use dfe_archiver_io::{KafkaStatsEmitter, ReceivedBatch, SourceTransport, Staging};
use lru::LruCache;
use rayon::prelude::*;
use scalo::dlq::{Dlq, DlqEntry, DlqSource};
use scalo::logger::helpers::{log_debounced, log_sampled, log_state_change};
use scalo::memory::MemoryGuard;
use scalo::metrics::FlushTrigger;
use scalo::scaling::ScalingPressure;
use scalo::transport::filter::FilteredDlqEntry;
use scalo::transport::{DeadLetterReason, DeliveryStatus, KafkaToken, SinkConfirmation};
use scalo::{AckHeldSource, SelfRegulationGovernor};
use std::hash::BuildHasher;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

/// How long the shutdown drain waits on a closed source that returns nothing
/// and never reports it is empty.
const DRAIN_IDLE_LIMIT: Duration = Duration::from_secs(5);

/// How long the shutdown drain waits for staged files to upload. What is still
/// uploading then is read again after the restart.
const DRAIN_UPLOAD_LIMIT: Duration = Duration::from_secs(20);

/// The directory under `buffer.spool_dir` where object-store files are staged.
const STAGING_DIR: &str = "uploads";

/// The first retry of a failed upload, doubling to `UPLOAD_RETRY_MAX`.
const UPLOAD_RETRY_FIRST: Duration = Duration::from_millis(500);

/// The longest wait between two attempts at one upload.
const UPLOAD_RETRY_MAX: Duration = Duration::from_secs(60);

/// The share of the memory limit held offsets may take before intake pauses.
const HELD_RECORDS_MEMORY_SHARE: u64 = 4;

/// How often the held-record count is taken for the inbound brake.
const HELD_REFRESH: Duration = Duration::from_millis(250);

/// Staged bytes past which intake pauses. Kept under the spool volume's 10 GiB
/// size limit, because kubelet evicts a pod whose `emptyDir` passes it.
const STAGED_BYTES_CAP: u64 = 8 * 1024 * 1024 * 1024;

/// Held records past which intake pauses: a quarter of `memory_limit_bytes`
/// at [`HELD_OFFSET_BYTES`] a record.
fn held_record_cap(memory_limit_bytes: u64) -> u64 {
    (memory_limit_bytes / HELD_RECORDS_MEMORY_SHARE / HELD_OFFSET_BYTES).max(1)
}

/// The wait before retry `attempt` of an upload: exponential from
/// `UPLOAD_RETRY_FIRST` to `UPLOAD_RETRY_MAX`, drawn from its upper half so
/// uploads that failed together do not retry together.
fn upload_retry_delay(attempt: u32) -> Duration {
    let ceiling = UPLOAD_RETRY_FIRST
        .saturating_mul(2u32.saturating_pow(attempt.min(16)))
        .min(UPLOAD_RETRY_MAX);
    let jitter = std::hash::RandomState::new().hash_one((attempt, Instant::now()));
    let fraction = 0.5 + (jitter % 1000) as f64 / 2000.0;
    ceiling.mul_f64(fraction)
}

/// Pause intake as held records or staged bytes near their caps.
///
/// Both are HARD sources on the latch that pauses the Kafka partitions, so a
/// store outage turns into consumer lag, not memory or disk.
fn attach_intake_caps(
    governor: Option<&SelfRegulationGovernor>,
    memory_guard: &MemoryGuard,
    held_records: &Arc<AtomicU64>,
    staging: &Staging,
) {
    let Some(governor) = governor else {
        warn!(
            "Self-regulation is off, so nothing pauses intake while uploads fail and held records and staged files grow"
        );
        return;
    };
    let record_cap = held_record_cap(memory_guard.limit_bytes());
    let pressure = governor.pressure();
    pressure.attach_source(Arc::new(AckHeldSource::new(
        Arc::clone(held_records),
        record_cap,
    )));
    pressure.attach_source(Arc::new(AckHeldSource::new(
        staging.bytes(),
        STAGED_BYTES_CAP,
    )));
    info!(
        held_record_cap = record_cap,
        staged_byte_cap = STAGED_BYTES_CAP,
        staging = %staging.dir().display(),
        "Intake pauses when held records or staged bytes near their caps"
    );
}

/// The object-store sink's health latch, shared with the upload tasks.
struct SinkCircuit {
    open: AtomicBool,
    scaling: Arc<ScalingPressure>,
    metrics: Arc<ArchiverMetrics>,
}

impl SinkCircuit {
    /// Latch the circuit and publish it everywhere it is read. The engine
    /// zeroes the scaling composite while it is open, because more pods
    /// cannot relieve a dead sink.
    fn set(&self, open: bool) {
        self.open.store(open, Ordering::Relaxed);
        self.scaling.set_circuit_open(open);
        self.metrics.set_scaling_circuit_open(open);
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::Relaxed)
    }
}

/// What every upload task shares.
struct Uploader {
    /// One permit per upload attempt running at once.
    slots: Arc<Semaphore>,
    sink: Arc<SinkCircuit>,
    metrics: Arc<ArchiverMetrics>,
    backend: &'static str,
    /// Where the records of a file the store refuses for good go.
    dlq: Arc<Dlq>,
    /// Reads a refused file's blocks back into records.
    compressor: Arc<dyn Compressor + Send + Sync>,
}

/// Upload one staged file until the store takes it or refuses it for good,
/// and settle its records accordingly. A transient failure keeps the file and
/// its held offsets, and the next attempt starts a fresh upload. A refused
/// file's records go to the DLQ when one is enabled, and are dropped when not.
async fn upload_until_settled(file: PendingFile, uploader: Arc<Uploader>) -> Settled {
    let PendingFile {
        upload,
        offsets,
        records,
        destination,
    } = file;
    let mut attempt: u32 = 0;
    loop {
        let result = {
            let _slot = uploader.slots.acquire().await.ok();
            upload.attempt().await
        };
        match result {
            Ok(()) => {
                uploader.sink.set(false);
                return Settled::delivered(offsets, records);
            }
            Err(e) if e.is_refused() && uploader.dlq.is_enabled() => {
                warn!(
                    path = %upload.path(),
                    records,
                    error = %e,
                    "The store refused an archive file for good; its records go to the DLQ"
                );
                let refused = RefusedFile {
                    upload: upload.as_ref(),
                    records,
                    destination: &destination,
                    refusal: &e,
                };
                return dead_letter_refused_file(&refused, offsets, &uploader).await;
            }
            Err(e) if e.is_refused() => {
                upload.discard().await;
                error!(
                    path = %upload.path(),
                    records,
                    error = %e,
                    "The store refused an archive file for good; its records are dropped"
                );
                return Settled::dropped(offsets, records, e.to_string());
            }
            Err(e) => {
                uploader.metrics.record_error();
                uploader.metrics.record_sink_error(uploader.backend);
                uploader.sink.set(true);
                let delay = upload_retry_delay(attempt);
                attempt = attempt.saturating_add(1);
                if attempt == 1 || attempt.is_multiple_of(10) {
                    warn!(
                        path = %upload.path(),
                        attempt,
                        retry_in_ms = delay.as_millis(),
                        error = %e,
                        "Archive upload failed; the file and its records are kept and the upload retried"
                    );
                } else {
                    debug!(path = %upload.path(), attempt, error = %e, "Archive upload failed again");
                }
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// The lines in `plain`, each ending in a newline but perhaps the last.
fn line_count(plain: &[u8]) -> u64 {
    plain.split_inclusive(|byte| *byte == b'\n').count() as u64
}

/// A staged file the store refused for good, and what it holds.
struct RefusedFile<'a> {
    upload: &'a dyn PendingUpload,
    records: u64,
    /// The routed destination its records were written for.
    destination: &'a str,
    refusal: &'a Error,
}

impl RefusedFile<'_> {
    /// The decompressed `index`th block, `None` past the last.
    async fn block(
        &self,
        index: usize,
        compressor: &(dyn Compressor + Send + Sync),
    ) -> Result<Option<Vec<u8>>> {
        match self.upload.block(index).await? {
            Some(block) => compressor.decompress(&block).map(Some),
            None => Ok(None),
        }
    }

    /// The lines across every block of the file.
    async fn lines(&self, compressor: &(dyn Compressor + Send + Sync)) -> Result<u64> {
        let mut lines: u64 = 0;
        for index in 0.. {
            let Some(plain) = self.block(index, compressor).await? else {
                break;
            };
            lines += line_count(&plain);
        }
        Ok(lines)
    }
}

/// Dead-letter the records of a staged file the store refused for good, read
/// back block by block, each block one flush decompressed on its own.
///
/// An archive file holds one record per line, so each line goes to the DLQ as
/// its own entry. When the file holds more lines than records, a payload held
/// a newline of its own, and each block goes whole as one entry instead, so no
/// record is ever split across entries.
///
/// The file's offsets are released `Rejected` once the DLQ confirms every
/// entry it can hold. An entry no DLQ backend can hold is counted dropped, by
/// its lines, and the offsets are released `Dropped` only when nothing reached
/// the DLQ. A file that cannot be read back, or a DLQ write that fails, is
/// `Errored`, so a restart writes the records again.
async fn dead_letter_refused_file(
    file: &RefusedFile<'_>,
    offsets: OffsetSet,
    uploader: &Uploader,
) -> Settled {
    let path = file.upload.path();
    let compressor = uploader.compressor.as_ref();
    let lines = match file.lines(compressor).await {
        Ok(lines) if lines > 0 || file.records == 0 => lines,
        Ok(_) => {
            let empty = Error::storage("no record could be read back");
            return settle_unreadable(offsets, path, &empty);
        }
        Err(e) => return settle_unreadable(offsets, path, &e),
    };
    let whole_blocks = lines != file.records;
    let reason = if whole_blocks {
        warn!(
            path,
            records = file.records,
            lines,
            "A refused file holds more lines than records: a payload held a newline of its own, so each block goes to the DLQ whole"
        );
        format!(
            "storage_write_failed: {}; a record held a newline of its own, so this is a whole block of newline-joined records",
            file.refusal
        )
    } else {
        format!("storage_write_failed: {}", file.refusal)
    };

    let mut written: u64 = 0;
    let mut too_large: u64 = 0;
    let mut too_large_reason = None;
    for index in 0.. {
        let plain = match file.block(index, compressor).await {
            Ok(Some(plain)) => plain,
            Ok(None) => break,
            Err(e) => return settle_unreadable(offsets, path, &e),
        };
        let entry = |payload: Vec<u8>, lines: u64| {
            let entry = DlqEntry::new("dfe-archiver", reason.as_str(), payload)
                .with_destination(file.destination);
            (entry, lines)
        };
        let entries = if whole_blocks {
            let block_lines = line_count(&plain);
            vec![entry(plain, block_lines)]
        } else {
            plain
                .split_inclusive(|byte| *byte == b'\n')
                .map(|line| entry(line.strip_suffix(b"\n").unwrap_or(line).to_vec(), 1))
                .collect()
        };
        let Screened { writable, refused } = screen_dead_letters(&uploader.dlq, entries);
        if let Some((why, _)) = refused.first() {
            too_large_reason.get_or_insert_with(|| why.to_string());
        }
        too_large += refused.iter().map(|(_, lines)| lines).sum::<u64>();
        if writable.is_empty() {
            continue;
        }
        let (writable, counts): (Vec<DlqEntry>, Vec<u64>) = writable.into_iter().unzip();
        if let Err(dlq_err) = uploader.dlq.write_confirmed(writable).await {
            error!(
                path,
                records = file.records,
                error = %dlq_err,
                "The DLQ could not take the records of a file the store refused; a restart writes them again"
            );
            return Settled {
                errored: offsets,
                ..Settled::default()
            };
        }
        written += counts.iter().sum::<u64>();
    }

    file.upload.discard().await;
    // Lines stand in for records, and a whole block of them holds more lines than records.
    let dropped = too_large.min(file.records);
    let mut settled = Settled::default();
    if let Some(why) = too_large_reason {
        settled.dropped_records = dropped;
        settled.dropped_reason = Some(format!(
            "{}, and no DLQ backend can hold them: {why}",
            file.refusal
        ));
    }
    if written > 0 {
        settled.rejected = offsets;
        settled.rejected_records = file.records - dropped;
    } else {
        settled.dropped = offsets;
    }
    settled
}

/// Settle a refused file that cannot be read back for the DLQ `Errored`, so a
/// restart writes its records again.
fn settle_unreadable(offsets: OffsetSet, path: &str, error: &Error) -> Settled {
    error!(
        path,
        error = %error,
        "A file the store refused could not be read back for the DLQ; a restart writes its records again"
    );
    Settled {
        errored: offsets,
        ..Settled::default()
    }
}

/// Per-instance log-spam guards. Live on `Archiver` (not as module statics)
/// so state does not leak across nextest test runs that share a process.
#[derive(Default)]
struct LogSpamGuards {
    recv_error_last: AtomicU64,
    route_error_count: AtomicU64,
    backpressure_active: AtomicBool,
}

/// What routing one received block produced.
struct RoutedBatch {
    /// Batches the buffer closed, for the writers.
    staged: Vec<StagedBatch>,
    /// Set when the buffer refused a push, so the caller pauses.
    backpressure: bool,
    /// Records nested past the parse depth, which never reach a buffer.
    too_deep: Vec<KafkaMessage>,
}

/// How one staged batch's write ended.
enum Written {
    /// In its destination's open file, which holds the offsets until it is
    /// durable.
    Held { records: usize },
    /// No file took it, so the batch comes back for the dead-letter path.
    Failed(StagedBatch, Error),
}

/// Main archiver pipeline
pub struct Archiver {
    /// Startup config snapshot (for restart-required fields: transport, archive, routing, etc.)
    startup_config: Config,
    /// Shared config for hot-reloadable fields (buffer, memory, scaling tunables)
    shared_config: SharedConfig<Config>,
    /// The inbound transport, bus or direct. Its methods take `&self` and the
    /// scalo transports are `Send+Sync` with internal locking on the recv hot
    /// path, so no outer `Mutex` is required. This lets recv and release run
    /// concurrently from different async tasks.
    transport: SourceTransport,
    router: Router,
    buffer: TieredBufferManager,
    metrics: Arc<ArchiverMetrics>,
    /// Active archive writers per destination, capped LRU cache.
    /// On overflow the least-recently-used writer is evicted and closed in
    /// a spawned task. Each writer has its own `tokio::Mutex` for
    /// independent async I/O -- the outer `parking_lot::Mutex` only guards
    /// map ops (lookup / insert / evict-pop), which are O(1).
    writers: parking_lot::Mutex<LruCache<String, Arc<Mutex<ArchiveWriter>>>>,
    /// Closes of LRU-evicted writers, each returning what its file settled.
    /// Collected every cycle and awaited at drain, so an evicted file's
    /// offsets are released once it completes and never before.
    evictions: parking_lot::Mutex<JoinSet<Settled>>,
    /// Local disk where object-store files wait for their upload.
    staging: Staging,
    /// Uploads of staged files, each returning what its file settled.
    /// Collected every cycle and waited on, with a limit, at drain.
    uploads: parking_lot::Mutex<JoinSet<Settled>>,
    /// What every upload task shares, its slots bounding the uploads running
    /// at once and with them upload memory.
    uploader: Arc<Uploader>,
    /// Records handed out and not yet released, read by the inbound brake.
    held_records: Arc<AtomicU64>,
    /// When `held_records` was last counted.
    held_refreshed: parking_lot::Mutex<Instant>,
    /// Records released `Errored`: written nowhere, and read again only after
    /// a restart or rebalance, so the loop ends once any are counted.
    withheld: AtomicU64,
    /// Cancellation token for graceful shutdown
    cancel: CancellationToken,
    /// The unified KEDA `ScalingPressure` engine, reaching KEDA as the
    /// `dfe_scaling_pressure` gauge on the metrics listener -- the runtime
    /// starts that listener without the `/scaling/pressure` route, so nothing
    /// serves the composite over HTTP. scalo 2.9 collapsed the old dual model (the app's
    /// weighted pressure + a separate runtime signal cell) into this single
    /// engine: the components are registered via `ServiceApp::scaling_components`
    /// and the archiver's loops drive their values directly (`kafka_lag` from
    /// the consumer's position lag, `buffer_depth` from hot-buffer count,
    /// `memory` from the cgroup guard) plus the circuit gate. Shared from the
    /// runtime when `scaling` is enabled, a standalone fallback otherwise.
    scaling: Arc<ScalingPressure>,
    /// Sink circuit latch driving `ScalingPressure::set_circuit_open`: opened
    /// by a failed upload, or for a local destination by a write cycle that
    /// failed with no success, and closed by the next success.
    sink: Arc<SinkCircuit>,
    /// Cgroup-aware memory guard. This is the SAME guard the self-regulation
    /// governor reads, so the bytes accounted here (`add_bytes` on recv,
    /// `release` after write) drive the inbound pause-partitions brake.
    memory_guard: Arc<MemoryGuard>,
    /// rdkafka stats emitter (sidecar consumer for broker/partition metrics)
    _stats_emitter: Option<KafkaStatsEmitter>,
    /// Dead letter queue for failed messages
    dlq: Arc<Dlq>,
    /// Log-spam guards (per-instance -- see `LogSpamGuards` doc)
    log_guards: LogSpamGuards,
    /// One debounce cell per configured expression field, indexed as
    /// `routing.expression_fields`. A field no source carries falls through on
    /// every record, so the warn is debounced per field rather than per record.
    routing_fallback_guards: Vec<AtomicU64>,
}

/// The config sections a reload cannot reach, listed for the operator.
///
/// `Archiver::new` snapshots the config into `startup_config` and builds the
/// transport, router, buffer, DLQ and writers from it, so only
/// `kafka.batch_size` and `buffer.backpressure_pause_secs` are re-read while
/// the process runs. Everything else keeps its startup value until a restart,
/// and the reloader must say so rather than report a reload that was applied to
/// nothing.
#[must_use]
pub fn restart_required_changes(old: &Config, new: &Config) -> Vec<&'static str> {
    let mut changed = Vec::new();

    if old.transport != new.transport {
        changed.push("transport");
    }

    // The recv loop re-reads kafka.batch_size, so hold it equal and compare the
    // rest of the section.
    let mut kafka = new.kafka.clone();
    kafka.batch_size = old.kafka.batch_size;
    if old.kafka != kafka {
        changed.push("kafka");
    }

    if old.grpc != new.grpc {
        changed.push("grpc");
    }
    if old.archive != new.archive {
        changed.push("archive");
    }

    // Same treatment for the one buffer field the backpressure pause re-reads.
    let mut buffer = new.buffer.clone();
    buffer.backpressure_pause_secs = old.buffer.backpressure_pause_secs;
    if old.buffer != buffer {
        changed.push("buffer");
    }

    if old.routing != new.routing {
        changed.push("routing");
    }
    if old.compression != new.compression {
        changed.push("compression");
    }

    // scalo's DlqConfig has no PartialEq, so the section is compared as JSON --
    // exact here because it carries no redacted field. A serialisation failure
    // reports the change rather than swallowing it.
    match (
        serde_json::to_value(&old.dlq),
        serde_json::to_value(&new.dlq),
    ) {
        (Ok(before), Ok(after)) if before == after => {}
        _ => changed.push("dlq"),
    }

    changed
}

/// Drain the files a writer has opened into `files_created_total`.
///
/// Every production write path goes through here, so the success counter has
/// the same coverage as `files_closed_total` -- it is the denominator the
/// sink-failure alert divides the error rate by.
fn record_files_opened(metrics: &ArchiverMetrics, writer: &mut ArchiveWriter) {
    let opened = writer.take_files_opened();
    if opened > 0 {
        metrics.record_files_created(opened);
    }
}

/// Drain the rolls a writer completed into `archive_roll_total` and
/// `files_closed_total`.
///
/// Drained rather than returned because one `write_record` rolls on either
/// half, so a single returned `CloseStats` would report one of two closes.
fn record_rolls(metrics: &ArchiverMetrics, writer: &mut ArchiveWriter, destination: &str) {
    for stats in writer.take_rolls() {
        if let Some(trigger) = stats.trigger {
            debug!(
                destination,
                trigger,
                compressed_bytes = stats.compressed_bytes,
                "Archive file rolled"
            );
            metrics.record_archive_roll(trigger);
        }
        metrics.record_file_closed(stats.compressed_bytes);
    }
}

/// Count the expression-routing fields a record did not carry, and warn once
/// per field per minute.
///
/// `crates/core` owns no metrics, so the router reports the fallback and this
/// records it: read against `messages_received_total`, a sustained ratio of 1
/// is a field name no source in the deployment sets, which otherwise looks
/// exactly like a tenant called `unknown`.
fn record_routing_fallbacks(
    metrics: &ArchiverMetrics,
    fields: &[String],
    guards: &[AtomicU64],
    fallbacks: &[usize],
) {
    for &index in fallbacks {
        let Some(field) = fields.get(index) else {
            continue;
        };
        metrics.record_routing_fallback(field);
        if guards
            .get(index)
            .is_some_and(|guard| log_debounced(guard, 60_000))
        {
            warn!(
                field = %field,
                "Expression-routing field missing from the record -- archiving under the default segment (debounced, max 1/60s)"
            );
        }
    }
}

/// The commit tokens of `offsets`.
fn tokens_of(offsets: Vec<KafkaOffset>) -> Vec<KafkaToken> {
    offsets.into_iter().map(KafkaOffset::into_token).collect()
}

/// Publish `lag`, the records past this pod's read position, to the
/// `dfe_archiver_kafka_lag` gauge and the composite's `kafka_lag` term.
/// `None`, from the direct transport, leaves both as they were.
fn publish_kafka_lag(lag: Option<i64>, metrics: &ArchiverMetrics, scaling: &ScalingPressure) {
    let Some(lag) = lag else {
        return;
    };
    let lag = u64::try_from(lag).unwrap_or(0);
    metrics.set_kafka_lag(lag);
    scaling.set_component("kafka_lag", lag as f64);
}

/// Dead letters split by whether a DLQ backend can hold them, each kept with
/// its tag.
struct Screened<T> {
    writable: Vec<(DlqEntry, T)>,
    refused: Vec<(DeadLetterReason, T)>,
}

/// Split `entries` into those a backend of `dlq` can hold, and the reasons no
/// backend can ever hold the rest.
fn screen_dead_letters<T>(dlq: &Dlq, entries: Vec<(DlqEntry, T)>) -> Screened<T> {
    let mut writable = Vec::with_capacity(entries.len());
    let mut refused = Vec::new();
    for (entry, tag) in entries {
        match dlq.refusal(&entry) {
            Some(reason) => refused.push((reason, tag)),
            None => writable.push((entry, tag)),
        }
    }
    Screened { writable, refused }
}

/// One DLQ entry per record of `batch`, each with its offset, or the offsets
/// alone when the batch's records and offsets disagree.
fn record_dead_letters(
    batch: StagedBatch,
    reason: &str,
) -> std::result::Result<Vec<(DlqEntry, KafkaOffset)>, Vec<KafkaOffset>> {
    let payloads: Vec<Vec<u8>> = batch.records().map(<[u8]>::to_vec).collect();
    let StagedBatch {
        destination,
        offsets,
        ..
    } = batch;
    if payloads.len() != offsets.len() {
        return Err(offsets);
    }
    Ok(payloads
        .into_iter()
        .zip(offsets)
        .map(|(payload, offset)| {
            let entry = DlqEntry::new("dfe-archiver", reason, payload)
                .with_destination(destination.as_str());
            (entry, offset)
        })
        .collect())
}

/// What a completed archive file proves: a completion the object store
/// answered, or a file written on this node.
///
/// Decided by the destination scheme, as `create_backend` decides the backend.
fn sink_confirmation(archive: &ArchiveConfig) -> SinkConfirmation {
    let destination = archive.destination.as_str();
    if destination.starts_with("file://") || !destination.contains("://") {
        SinkConfirmation::Local
    } else {
        SinkConfirmation::Remote
    }
}

/// Start the DLQ. The Kafka backend rides the same config conversion as the
/// consumer transport, so dead letters land on the broker the data came from.
fn spawn_dlq(config: &Config) -> Result<Dlq> {
    let mut dlq_config = config.dlq.clone();
    let dlq_kafka = if config.is_direct() {
        // No broker to dead-letter to, and the file backend is an EROFS
        // no-op on a read-only rootfs, so there is no backend to offer.
        if dlq_config.enabled {
            warn!("no broker on the direct transport -- the DLQ is disabled");
            dlq_config.enabled = false;
        }
        None
    } else {
        Some(dfe_archiver_io::kafka::convert_config(&config.kafka))
    };
    // Not tied to the loop's cancel token: the shutdown drain still
    // dead-letters after the loop stops, and `drain` shuts the DLQ down last.
    let dlq = Dlq::spawn(
        &dlq_config,
        "dfe-archiver",
        dlq_kafka.as_ref(),
        CancellationToken::new(),
    )
    .map_err(|e| Error::Config(format!("DLQ init failed: {e}")))?;
    if dlq_config.enabled {
        info!(mode = ?dlq_config.mode, "DLQ enabled");
    }
    Ok(dlq)
}

/// One permit per upload attempt running at once: `buffer.writer_parallelism`.
/// Zero would never upload, so it counts as one.
fn upload_slots(config: &Config) -> Arc<Semaphore> {
    Arc::new(Semaphore::new(config.buffer.writer_parallelism.max(1)))
}

/// Map the operator's `buffer` section onto the tiered buffer's own config.
fn buffer_config(config: &Config) -> dfe_archiver_core::buffer::TieredBufferConfig {
    dfe_archiver_core::buffer::TieredBufferConfig {
        max_hot_buffers: 64,
        hot_buffer_size: config.buffer.flush_bytes,
        hot_buffer_records: config.buffer.flush_records,
        hot_buffer_age_secs: config.buffer.flush_age_secs,
        spool_dir: config.buffer.spool_dir.clone().into(),
        max_spool_bytes: 10 * 1024 * 1024 * 1024, // 10GB
        min_free_disk_bytes: 1024 * 1024 * 1024,  // 1GB
        spool_compression: true,
    }
}

impl Archiver {
    /// Create new archiver from shared configuration.
    ///
    /// Takes a snapshot of the config for startup-bound fields (transport,
    /// archive, routing, compression). Hot-reloadable fields (buffer thresholds,
    /// memory limits, scaling tunables) are read from `shared_config` each iteration.
    ///
    /// `metrics` must be the pre-initialised `ArchiverMetrics` from `init_metrics()`,
    /// which shares the same `MetricsManager` as the HTTP server.
    ///
    /// `memory_guard` is the runtime's cgroup-aware guard (the one feeding the
    /// self-regulation governor); the archiver accounts in-flight bytes on it so
    /// the inbound brake reflects real archiver pressure. `governor`, when
    /// `Some`, attaches the inbound brake to the receiver -- the Kafka
    /// pause-partitions gate, or the Push listener's `Unavailable` shed
    /// (default-on self-regulation); `None` means self-regulation is disabled.
    ///
    /// `scaling` is the runtime's unified `ScalingPressure` engine, shared so
    /// the archiver's loops drive the `dfe_scaling_pressure` gauge KEDA scales
    /// on. `None` when the runtime's `scaling` feature is off; a standalone
    /// engine (registering the same components) is built as a fallback so the
    /// local pressure gauge keeps working.
    pub async fn new(
        shared_config: SharedConfig<Config>,
        metrics: Arc<ArchiverMetrics>,
        memory_guard: Arc<MemoryGuard>,
        governor: Option<&SelfRegulationGovernor>,
        scaling: Option<Arc<ScalingPressure>>,
    ) -> Result<Self> {
        let config = shared_config.get();
        let transport = SourceTransport::from_config(&config, governor).await?;

        let (guarantee, reason) = transport.guarantee(sink_confirmation(&config.archive));
        metrics.set_delivery_guarantee(guarantee.as_str(), reason.as_str());
        info!(
            guarantee = guarantee.as_str(),
            reason = reason.as_str(),
            "pipeline delivery guarantee"
        );

        let router = Router::new(config.routing.clone());

        let buffer = TieredBufferManager::new(buffer_config(&config))?;
        let slots = upload_slots(&config);

        // The runtime's unified ScalingPressure, so the archiver's loops drive
        // the engine that emits the KEDA gauge. Its thresholds come from the
        // `scaling` cascade section (`ARCHIVER_SCALING__*`), not from this
        // file's config. `None` only when the `scaling` feature is compiled
        // out, where a standalone engine keeps the components registered.
        let scaling = scaling.unwrap_or_else(|| {
            Arc::new(ScalingPressure::new(
                scalo::scaling::ScalingPressureConfig::from_cascade(),
                crate::scaling_components(),
            ))
        });

        // Start rdkafka stats sidecar (non-fatal if it fails). Skipped on the
        // direct transport, which reaches no broker to collect stats from.
        let stats_emitter = if config.is_direct() {
            None
        } else {
            match KafkaStatsEmitter::new(&config.kafka) {
                Ok(emitter) => Some(emitter),
                Err(e) => {
                    warn!(error = %e, "Failed to start Kafka stats emitter (non-fatal)");
                    None
                }
            }
        };

        let cancel = CancellationToken::new();

        let dlq = Arc::new(spawn_dlq(&config)?);

        info!(
            transport = %config.transport,
            brokers = %config.kafka.brokers.join(","),
            topics = %config.kafka.topics.join(","),
            destination = %config.archive.destination,
            "Archiver initialized"
        );

        // Writer LRU cache -- clamp to at least 1 to satisfy NonZeroUsize.
        let writer_cap = NonZeroUsize::new(config.archive.max_writers).unwrap_or(NonZeroUsize::MIN);

        let routing_fallback_guards = config
            .routing
            .expression_fields
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect();

        let staging = Staging::open(Path::new(&config.buffer.spool_dir).join(STAGING_DIR))?;
        let held_records = Arc::new(AtomicU64::new(0));
        attach_intake_caps(governor, &memory_guard, &held_records, &staging);

        let sink = Arc::new(SinkCircuit {
            open: AtomicBool::new(false),
            scaling: Arc::clone(&scaling),
            metrics: Arc::clone(&metrics),
        });
        let uploader = Arc::new(Uploader {
            slots,
            sink: Arc::clone(&sink),
            metrics: Arc::clone(&metrics),
            backend: config.archive.backend_name(),
            dlq: Arc::clone(&dlq),
            compressor: Arc::from(compressor_for(&config.compression)?),
        });

        Ok(Self {
            startup_config: config,
            shared_config,
            transport,
            router,
            buffer,
            metrics,
            writers: parking_lot::Mutex::new(LruCache::new(writer_cap)),
            evictions: parking_lot::Mutex::new(JoinSet::new()),
            staging,
            uploads: parking_lot::Mutex::new(JoinSet::new()),
            uploader,
            held_records,
            held_refreshed: parking_lot::Mutex::new(Instant::now()),
            withheld: AtomicU64::new(0),
            cancel,
            scaling,
            sink,
            memory_guard,
            _stats_emitter: stats_emitter,
            dlq,
            log_guards: LogSpamGuards::default(),
            routing_fallback_guards,
        })
    }

    /// Check the inbound transport (it connects or binds on creation) and prove
    /// the archive sink answers.
    ///
    /// # Errors
    /// Returns an error when the inbound transport is not serving.
    pub async fn check_connection(&self) -> Result<()> {
        if !self.transport.is_healthy() {
            return Err(Error::transport("inbound transport is not healthy"));
        }
        info!(transport = %self.startup_config.transport, "Inbound transport verified");
        self.verify_sink().await;
        Ok(())
    }

    /// List one object under the configured prefix and latch what came back.
    ///
    /// A sink that is out never blocks startup -- refusing to start brings no
    /// object store back, and the archiver keeps consuming, buffering and
    /// dead-lettering while it recovers.
    async fn verify_sink(&self) {
        let destination = &self.startup_config.archive.destination;
        let backend = self.startup_config.archive.backend_name();
        let probe = match create_backend(&self.startup_config.archive, &self.staging) {
            Ok(client) => probe_sink(client.as_ref()).await,
            Err(e) => Err(e),
        };
        match probe {
            Ok(()) => {
                info!(backend, destination = %destination, "Archive sink verified");
                self.sink.set(false);
            }
            Err(e) => {
                warn!(
                    error = %e,
                    backend,
                    destination = %destination,
                    "Archive sink did not answer -- degraded until a write succeeds"
                );
                self.sink.set(true);
            }
        }
    }

    /// Run the main archiver loop
    ///
    /// This runs until shutdown is signaled via the cancellation token. A
    /// failed upload never ends it: the upload retries while intake pauses.
    ///
    /// # Errors
    /// [`Error::Withheld`] once a record could be written neither to local
    /// disk nor to the DLQ: its offset holds the commit below it and nothing
    /// short of a restart reads it again, so consuming on would only grow the
    /// replay.
    #[instrument(skip(self))]
    pub async fn run(&self) -> Result<()> {
        info!("Starting archiver main loop");

        // Independent flush timer -- ensures aged batches flush even when no data is flowing.
        // Without this, flush_aged() only runs inside process_messages() which requires
        // new Kafka data to arrive (fixes #14).
        let flush_age_secs = self.shared_config.with(|c| c.buffer.flush_age_secs);
        let mut flush_interval = tokio::time::interval(Duration::from_secs(flush_age_secs.max(1)));
        // Consume the immediate first tick so we don't flush at startup
        flush_interval.tick().await;

        // Memory backpressure is handled by the self-regulation governor: the
        // inbound pause-partitions gate (attached to the Kafka receiver) pauses
        // the consumer's ASSIGNED partitions under pressure -- the member stays
        // in the group (no rebalance), consumer lag rises, KEDA scales up. The
        // gate is evaluated automatically inside `recv`, so the old hand-rolled
        // Pattern-B pause loop here is gone. We never gate the outbound archive
        // drain -- gating the sink would deadlock the pipeline.
        loop {
            let withheld = self.withheld.load(Ordering::Relaxed);
            if withheld > 0 {
                error!(
                    records = withheld,
                    "Records were written neither to local disk nor to the DLQ; stopping so a restart reads them again"
                );
                return Err(Error::Withheld { records: withheld });
            }
            self.refresh_held_records();
            tokio::select! {
                biased; // Prioritise shutdown over data processing

                () = self.cancel.cancelled() => {
                    info!("Shutdown requested, exiting main loop");
                    return Ok(());
                }

                // Independent flush timer: flush aged batches even when no data is flowing.
                // Without this, aged data sits in memory until new Kafka data arrives (fixes #14).
                _ = flush_interval.tick() => {
                    // Refresh the per-pod Kafka inbound scaling signal even when no
                    // data is flowing -- an idle-but-lagging pod (e.g. paused under
                    // memory pressure) must still surface its backlog so KEDA can
                    // scale the group out. Cheaper + fresher than the engine's own
                    // 15s tick.
                    self.push_kafka_lag_signal();
                    let aged = self.buffer.flush_aged();
                    if !aged.is_empty() {
                        debug!(count = aged.len(), "Flushing aged batches (timer)");
                    }
                    let mut settled = self.write_staged(aged).await;
                    // Runs after the aged batches so an active destination rolls
                    // through the write path, leaving only idle files here.
                    settled.absorb(self.close_aged_writers().await);
                    settled.absorb(self.collect_evictions());
                    settled.absorb(self.collect_uploads());
                    self.release_settled(settled).await;
                }

                result = async {
                    let batch_size = self.shared_config.with(|c| c.kafka.batch_size);
                    let recv_start = Instant::now();
                    let result = self.transport.recv(batch_size).await;
                    self.metrics.record_recv_duration(recv_start.elapsed().as_secs_f64());
                    result
                } => {
                    // Push the per-pod Kafka inbound scaling signal each poll
                    // (mirrors dfe-loader). position_lag() sums records past THIS
                    // pod's read position -- scale-invariant.
                    self.push_kafka_lag_signal();
                    match result {
                        Ok(batch) if !batch.is_empty() => self.handle_batch(batch).await,
                        Ok(_) => {
                            // Empty batch -- no messages available (or partitions paused)
                        }
                        Err(Error::Shutdown) => {
                            info!("The inbound transport closed, exiting main loop");
                            return Ok(());
                        }
                        Err(e) => {
                            if log_debounced(&self.log_guards.recv_error_last, 5000) {
                                error!(error = %e, "Failed to receive from the inbound transport (debounced, max 1/5s)");
                            }
                            self.metrics.record_error();
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    /// Route, write and release one received block.
    async fn handle_batch(&self, batch: ReceivedBatch) {
        let ReceivedBatch {
            messages,
            filtered,
            dlq_entries,
        } = batch;
        self.route_filter_dlq(dlq_entries, filtered).await;
        if !messages.is_empty() {
            self.process_messages(messages).await;
            self.metrics.update_eps();
        }
    }

    /// Process a batch of messages: route -> write -> release -> update metrics.
    /// Each phase lives in its own helper for testability and clarity.
    async fn process_messages(&self, messages: Vec<dfe_archiver_core::KafkaMessage>) {
        let batch_len = messages.len();
        if tracing::enabled!(tracing::Level::DEBUG) {
            let mut topics: Vec<&str> = messages.iter().map(|m| m.topic.as_str()).collect();
            topics.sort_unstable();
            topics.dedup();
            debug!(count = batch_len, topics = ?topics, "Received batch from Kafka");
        }
        self.metrics.record_received(batch_len as u64);

        // Track incoming bytes in the memory guard (the inbound brake watches
        // this). The `kafka_lag` scaling component is driven by the pod's
        // position lag in `push_kafka_lag_signal` (scale-invariant), NOT by
        // per-batch throughput, so nothing is set here.
        let batch_bytes: u64 = messages.iter().map(|m| m.payload.len() as u64).sum();
        self.memory_guard.add_bytes(batch_bytes);

        // Phase 1: route + buffer accumulate (returns staged batches, a
        // backpressure flag set when the buffer rejects a push, and the records
        // routing refused).
        let RoutedBatch {
            staged,
            backpressure,
            too_deep,
        } = self.route_batch(messages);
        self.dead_letter_too_deep(too_deep).await;

        // Phase 2: write into each destination's open file. A roll completes
        // the previous file, which settles its offsets or hands them to its
        // upload.
        let mut settled = self.write_staged(staged).await;
        settled.absorb(self.collect_evictions());
        settled.absorb(self.collect_uploads());

        // Phase 3: release the offsets of durable files -- the at-least-once
        // release point.
        self.release_settled(settled).await;

        if backpressure {
            let pause_secs = self
                .shared_config
                .with(|c| c.buffer.backpressure_pause_secs);
            tokio::time::sleep(Duration::from_secs(pause_secs)).await;
        } else if log_state_change(&self.log_guards.backpressure_active, false) {
            info!("Backpressure cleared -- normal processing resumed");
        }

        self.update_pipeline_metrics();
    }

    /// Where one record goes, or `None` for a record refused before the
    /// routing parse for its nesting depth. Any other routing failure falls
    /// back to the record's topic.
    fn route_one(
        &self,
        msg: &dfe_archiver_core::KafkaMessage,
    ) -> Option<dfe_archiver_core::routing::Routed> {
        match self.router.route(msg) {
            Ok(routed) => Some(routed),
            Err(e @ Error::TooDeep { .. }) => {
                scalo::logger::security::input_validation_failure("routing", &e.to_string(), None);
                None
            }
            Err(e) => {
                self.metrics.record_routing_error();
                if log_sampled(&self.log_guards.route_error_count, 1000) {
                    warn!(
                        error = %e,
                        total = self
                            .log_guards
                            .route_error_count
                            .load(std::sync::atomic::Ordering::Relaxed),
                        "Routing failed, using topic (sampled 1/1000)"
                    );
                }
                scalo::logger::security::input_validation_failure("routing", &e.to_string(), None);
                Some(dfe_archiver_core::routing::Routed {
                    destination: msg.topic.clone(),
                    fallback_fields: Vec::new(),
                })
            }
        }
    }

    /// Phase 1: parallel route + sequential buffer push.
    ///
    /// Returns the staged batches (including any aged buffers picked up
    /// while we're here), a `backpressure` flag -- set if the buffer
    /// rejected a push, in which case aged-flush is skipped and the caller
    /// is expected to pause -- and the records nested too deep to route.
    fn route_batch(&self, messages: Vec<dfe_archiver_core::KafkaMessage>) -> RoutedBatch {
        // Phase 1a: Parallel route computation. `Router::route` is pure
        // (`&self, &KafkaMessage`) so par_iter is sound. Expression-routed
        // configs do a sonic-rs JSON parse per message here.
        let route_results: Vec<Option<dfe_archiver_core::routing::Routed>> =
            messages.par_iter().map(|msg| self.route_one(msg)).collect();

        // Phase 1b: Sequential buffer push (mutable buffer state).
        let mut all_staged: Vec<StagedBatch> = Vec::new();
        let mut backpressure = false;
        let mut too_deep = Vec::new();
        for (message, routed) in messages.into_iter().zip(route_results) {
            let Some(routed) = routed else {
                too_deep.push(message);
                continue;
            };
            let dfe_archiver_core::routing::Routed {
                destination,
                fallback_fields,
            } = routed;
            record_routing_fallbacks(
                &self.metrics,
                self.router.expression_fields(),
                &self.routing_fallback_guards,
                &fallback_fields,
            );
            trace!(
                topic = %message.topic,
                partition = message.partition,
                offset = message.offset,
                destination = %destination,
                payload_bytes = message.payload.len(),
                "Routed message"
            );

            match self.buffer.push(&destination, message) {
                Ok(staged_batches) => {
                    for batch in &staged_batches {
                        debug!(
                            destination = %batch.destination,
                            records = batch.record_count,
                            bytes = batch.data.len(),
                            trigger = "size_or_eviction",
                            "Buffer staged batch"
                        );
                    }
                    all_staged.extend(staged_batches);
                }
                Err(e) => {
                    if log_state_change(&self.log_guards.backpressure_active, true) {
                        warn!(error = %e, "Buffer push failed -- backpressure active");
                    }
                    self.metrics.record_disk_pressure();
                    backpressure = true;
                    break;
                }
            }
        }

        // Picking up aged batches here keeps them moving even when no fresh
        // batch is closing. Skipped under backpressure so we don't pile more
        // I/O onto an already-saturated downstream.
        if !backpressure {
            let aged = self.buffer.flush_aged();
            if !aged.is_empty() {
                debug!(
                    count = aged.len(),
                    total_bytes = aged.iter().map(|b| b.data.len()).sum::<usize>(),
                    "Flushing aged batches in process_messages"
                );
            }
            all_staged.extend(aged);
        }

        RoutedBatch {
            staged: all_staged,
            backpressure,
            too_deep,
        }
    }

    /// Phase 2: write staged batches concurrently, one task per destination
    /// (each writer has its own Mutex so destinations never contend), and
    /// dead-letter the ones no file took. Returns what the writes settled.
    async fn write_staged(&self, staged: Vec<StagedBatch>) -> Settled {
        // futures::future::join_all runs writes concurrently on this task --
        // maximises I/O overlap without spawning new tasks.
        let written = futures::future::join_all(
            staged
                .into_iter()
                .map(|batch| async move { self.write_batch(batch).await }),
        )
        .await;

        let mut settled = Settled::default();
        let (mut cycle_ok, mut cycle_err) = (0usize, 0usize);
        for (outcome, rolled) in written {
            settled.absorb(rolled);
            match outcome {
                Written::Held { records } => {
                    cycle_ok += 1;
                    self.metrics.record_written(records as u64);
                }
                Written::Failed(batch, e) => {
                    cycle_err += 1;
                    error!(
                        error = %e,
                        destination = %batch.destination,
                        "Failed to write batch"
                    );
                    self.metrics.record_error();
                    // Surface storage-write failures per backend so the
                    // per-backend error counter is non-empty (metrics-gap audit).
                    self.metrics
                        .record_sink_error(self.startup_config.archive.backend_name());
                    self.dead_letter(batch, &e).await;
                }
            }
        }
        // A write reaches the sink only on a local destination. An object-store
        // write is local staging, and its uploads drive the circuit instead.
        if matches!(
            sink_confirmation(&self.startup_config.archive),
            SinkConfirmation::Local
        ) {
            self.update_sink_circuit(cycle_ok, cycle_err);
        }
        settled
    }

    /// Release `tokens` with `status`. A failed commit is counted and logged
    /// and not retried here: the records are settled, and the next commit on
    /// the partition covers them. An `Errored` release is counted in `withheld`,
    /// which ends [`run`](Self::run).
    async fn release(&self, tokens: Vec<KafkaToken>, status: DeliveryStatus) {
        if tokens.is_empty() || !self.transport.holds_offsets() {
            return;
        }
        let count = tokens.len() as u64;
        if status == DeliveryStatus::Errored {
            self.withheld.fetch_add(count, Ordering::Relaxed);
        }
        match self.transport.release(&tokens, status).await {
            Ok(()) => {
                if status.should_commit() {
                    self.metrics.record_commit(count);
                }
            }
            Err(e) => {
                self.metrics.record_commit_error();
                error!(error = %e, count, ?status, "Failed to release offsets");
            }
        }
    }

    /// Start the uploads of staged files, then release the rest of what the
    /// writers settled: the offsets of durable files `Delivered`, of refused
    /// files the DLQ holds `Rejected`, of refused files nothing holds
    /// `Dropped`, and of files that failed on local disk `Errored`, which keeps
    /// them below every later commit and ends the loop.
    async fn release_settled(&self, settled: Settled) {
        let settled = self.dispatch_uploads(settled);
        if settled.delivered_records > 0 {
            self.metrics.record_archived(settled.delivered_records);
        }
        self.release(settled.delivered.tokens(), DeliveryStatus::Delivered)
            .await;
        if settled.rejected_records > 0 {
            self.metrics.record_dlq(settled.rejected_records);
        }
        self.release(settled.rejected.tokens(), DeliveryStatus::Rejected)
            .await;
        if settled.dropped_records > 0 {
            self.metrics.record_dropped(settled.dropped_records);
            error!(
                records = settled.dropped_records,
                reason = settled
                    .dropped_reason
                    .as_deref()
                    .unwrap_or("refused by the store"),
                "Dropped records the store refused for good"
            );
        }
        self.release(settled.dropped.tokens(), DeliveryStatus::Dropped)
            .await;
        self.release(settled.errored.tokens(), DeliveryStatus::Errored)
            .await;
    }

    /// Refresh the held-record count the inbound brake reads on every recv, at
    /// most every [`HELD_REFRESH`], because counting walks every held run.
    fn refresh_held_records(&self) {
        {
            let mut refreshed = self.held_refreshed.lock();
            if refreshed.elapsed() < HELD_REFRESH {
                return;
            }
            *refreshed = Instant::now();
        }
        self.held_records
            .store(self.transport.held_records(), Ordering::Relaxed);
    }

    /// Hand every staged file to an upload task, and return the rest.
    fn dispatch_uploads(&self, mut settled: Settled) -> Settled {
        let files = std::mem::take(&mut settled.uploads);
        if files.is_empty() {
            return settled;
        }
        let mut uploads = self.uploads.lock();
        for file in files {
            uploads.spawn(upload_until_settled(file, Arc::clone(&self.uploader)));
        }
        settled
    }

    /// What the uploads that finished settled, without waiting on the ones
    /// still running.
    fn collect_uploads(&self) -> Settled {
        let mut settled = Settled::default();
        let mut uploads = self.uploads.lock();
        while let Some(joined) = uploads.try_join_next() {
            match joined {
                Ok(done) => settled.absorb(done),
                Err(e) => {
                    error!(error = %e, "An upload task did not finish; its records are read again after a restart");
                }
            }
        }
        settled
    }

    /// Wait up to `limit` for the uploads still running, and return what they
    /// settled. The ones still running then are stopped, and their records are
    /// read again after the restart.
    async fn await_uploads(&self, limit: Duration) -> Settled {
        let mut running = std::mem::take(&mut *self.uploads.lock());
        let mut settled = Settled::default();
        let waited = tokio::time::timeout(limit, async {
            while let Some(joined) = running.join_next().await {
                match joined {
                    Ok(done) => settled.absorb(done),
                    Err(e) => {
                        error!(error = %e, "An upload task did not finish; its records are read again after a restart");
                    }
                }
            }
        })
        .await;
        if waited.is_err() {
            warn!(
                uploads = running.len(),
                "Uploads still running at shutdown are stopped; their records are read again after the restart"
            );
            running.abort_all();
        }
        settled
    }

    /// Push this pod's Kafka position lag into the unified `ScalingPressure`
    /// `kafka_lag` component AND the `dfe_archiver_kafka_lag` gauge.
    ///
    /// The component is the Kafka inbound term of the composite KEDA scales on,
    /// weighted + saturated per `scaling_components()`. Position lag counts the
    /// records past THIS pod's read position, so it is scale-invariant -- as
    /// the consumer group grows each pod's lag falls -- and the commit an open
    /// file holds does not inflate it. The committed-offset lag would, by up to
    /// a roll interval of intake, and scale the archiver out on the hold rather
    /// than on backlog. The direct transport keeps no backlog this pod can
    /// read, so it leaves the component at whatever the engine last held
    /// rather than writing a false zero.
    fn push_kafka_lag_signal(&self) {
        publish_kafka_lag(self.transport.position_lag(), &self.metrics, &self.scaling);
    }

    /// Drive the sink circuit-open scaling gate from a local write cycle's
    /// outcome (mirrors dfe-loader's `ClickHouse` sink latch). The sink is
    /// "dead" when a whole cycle wrote nothing but saw errors; it recovers the
    /// moment any write succeeds.
    fn update_sink_circuit(&self, cycle_ok: usize, cycle_err: usize) {
        let open = if cycle_ok > 0 {
            false
        } else if cycle_err > 0 {
            true
        } else {
            self.sink.is_open()
        };
        self.sink.set(open);
    }

    /// Update buffer stats, scaling pressure, and pipeline gauges
    fn update_pipeline_metrics(&self) {
        self.metrics.set_last_batch_timestamp();

        let stats = self.buffer.stats();
        self.metrics
            .set_hot_buffer_stats(stats.current_hot_buffers, stats.current_hot_bytes);
        self.metrics.set_spool_bytes(stats.current_spool_bytes);
        self.metrics.set_uploads(
            self.uploads.lock().len(),
            self.staging.bytes().load(Ordering::Relaxed),
        );

        let writer_count = self.writers.lock().len();
        self.metrics.set_unique_destinations(writer_count);

        self.scaling
            .set_component("buffer_depth", stats.current_hot_buffers as f64);
        self.scaling
            .set_component("memory", self.memory_guard.pressure_ratio());
        let pressure = self.scaling.calculate();
        self.metrics.set_scaling_pressure(pressure);
        self.metrics
            .set_scaling_memory_pressure(self.memory_guard.pressure_ratio());
        self.metrics
            .set_scaling_circuit_open(self.scaling.snapshot().circuit_open);
        self.metrics.set_memory_usage(
            self.memory_guard.current_bytes(),
            self.memory_guard.limit_bytes(),
        );
    }

    /// The destination's writer, created on first use.
    ///
    /// The LRU lock guards only the map ops (O(1)), and the write happens on
    /// the writer's own Mutex. If an insertion overflows the cap, the oldest
    /// writer is evicted and closed in the background.
    fn writer_for(&self, destination: &str) -> Result<Arc<Mutex<ArchiveWriter>>> {
        let (writer, evicted) = {
            let mut cache = self.writers.lock();
            if let Some(w) = cache.get(destination) {
                (Arc::clone(w), None)
            } else {
                debug!(destination, "Creating new archive writer");
                let writer = Arc::new(Mutex::new(self.create_writer(destination)?));
                let evicted = cache.push(destination.to_string(), Arc::clone(&writer));
                // `push` only returns Some when the inserted key REPLACED an
                // existing key, OR when capacity overflowed. The replacement
                // case is impossible here because we just confirmed the key
                // wasn't present.
                let overflow = evicted.filter(|(k, _)| k != destination);
                (writer, overflow)
            }
        };

        if let Some((evicted_key, evicted_writer)) = evicted {
            self.spawn_evicted_writer_close(evicted_key, evicted_writer);
        }
        Ok(writer)
    }

    /// Write one staged batch into its destination's open file.
    ///
    /// Once written, the batch's offsets are held against that file and
    /// released when it completes. A roll inside the write completes the
    /// previous file, whose offsets come back settled either way.
    #[instrument(skip(self, batch), fields(destination = %batch.destination, records = batch.record_count))]
    async fn write_batch(&self, batch: StagedBatch) -> (Written, Settled) {
        let start = Instant::now();
        trace!(
            destination = %batch.destination,
            records = batch.record_count,
            bytes = batch.data.len(),
            offsets = batch.offsets.len(),
            "Starting batch write"
        );

        let writer_arc = match self.writer_for(batch.destination.as_str()) {
            Ok(writer) => writer,
            Err(e) => return (Written::Failed(batch, e), Settled::default()),
        };

        // Lock only THIS destination's writer -- other destinations can write concurrently
        let mut writer = writer_arc.lock().await;

        // Write data (may trigger a roll), then flush it into the file.
        let written = match writer.write(&batch.data).await {
            Ok(()) => writer.flush().await,
            Err(e) => Err(e),
        };
        record_files_opened(&self.metrics, &mut writer);
        record_rolls(&self.metrics, &mut writer, batch.destination.as_str());
        // The batch leaves memory whether a file took it or the DLQ does.
        self.memory_guard.release(batch.data.len() as u64);

        let flush_stats = match written {
            Ok(flush_stats) => flush_stats,
            Err(e) => return (Written::Failed(batch, e), writer.take_settled()),
        };
        let StagedBatch {
            destination,
            data,
            offsets,
            record_count,
            ..
        } = batch;
        let offsets = if self.transport.holds_offsets() {
            offsets
        } else {
            Vec::new()
        };
        writer.hold(offsets, record_count as u64);
        let settled = writer.take_settled();
        drop(writer);

        if let Some(flush_stats) = flush_stats {
            let ratio = if flush_stats.uncompressed_bytes > 0 {
                flush_stats.compressed_bytes as f64 / flush_stats.uncompressed_bytes as f64
            } else {
                1.0
            };
            debug!(
                destination = %destination,
                uncompressed_bytes = flush_stats.uncompressed_bytes,
                compressed_bytes = flush_stats.compressed_bytes,
                compression_ratio = format!("{ratio:.3}"),
                duration_ms = format!("{:.1}", flush_stats.compression_duration_secs * 1000.0),
                "Compression complete"
            );
            self.metrics
                .record_compression_duration(flush_stats.compression_duration_secs);
            self.metrics.record_bytes_compressed(
                flush_stats.compressed_bytes,
                flush_stats.uncompressed_bytes,
            );
        }

        let duration = start.elapsed();
        let backend = self.startup_config.archive.backend_name();
        self.metrics
            .record_flush(duration.as_secs_f64(), FlushTrigger::Size);
        self.metrics.record_bytes_written(data.len() as u64);
        self.metrics.record_batch_size(data.len() as u64);
        self.metrics
            .record_sink_duration(backend, duration.as_secs_f64());

        debug!(
            destination = %destination,
            records = record_count,
            bytes = data.len(),
            duration_ms = duration.as_millis(),
            backend,
            "Wrote batch to archive"
        );

        (
            Written::Held {
                records: record_count,
            },
            settled,
        )
    }

    /// Close every writer whose open file has outlived the rolling policy,
    /// and return what the closed files settled.
    ///
    /// `write` is the only other thing that consults the policy, so without
    /// this a destination that stops receiving data holds its file open until
    /// shutdown, and an active one overruns the interval by however long it
    /// waits for the next batch.
    async fn close_aged_writers(&self) -> Settled {
        // The parking_lot lock guards the map only, so the writers are
        // snapshotted and it is released before awaiting any writer's mutex.
        let writers: Vec<(String, Arc<Mutex<ArchiveWriter>>)> = {
            let cache = self.writers.lock();
            cache
                .iter()
                .map(|(dest, writer)| (dest.clone(), Arc::clone(writer)))
                .collect()
        };

        let mut settled = Settled::default();
        for (dest, writer_arc) in writers {
            let mut writer = writer_arc.lock().await;
            match writer.close_if_aged().await {
                Ok(Some(close_stats)) => {
                    debug!(
                        destination = %dest,
                        trigger = close_stats.trigger.unwrap_or("age"),
                        compressed_bytes = close_stats.compressed_bytes,
                        "Closed aged archive file (timer)"
                    );
                    if let Some(trigger) = close_stats.trigger {
                        self.metrics.record_archive_roll(trigger);
                    }
                    self.metrics
                        .record_file_closed(close_stats.compressed_bytes);
                }
                Ok(None) => {}
                Err(e) => {
                    error!(error = %e, destination = %dest, "Failed to close aged writer");
                    self.metrics.record_error();
                }
            }
            settled.absorb(writer.take_settled());
        }
        settled
    }

    /// Close an LRU-evicted writer in the background so it never blocks the
    /// hot path. The task returns what the file settled, which
    /// [`collect_evictions`](Self::collect_evictions) and
    /// [`await_evictions`](Self::await_evictions) release. The writer's own
    /// Mutex is held during close, and a write to the same destination
    /// meanwhile goes to a fresh writer, whose file names never collide with
    /// this one's.
    fn spawn_evicted_writer_close(
        &self,
        destination: String,
        writer_arc: Arc<Mutex<ArchiveWriter>>,
    ) {
        let metrics = Arc::clone(&self.metrics);
        debug!(destination = %destination, "Evicting LRU archive writer");
        metrics.record_writer_eviction();
        self.evictions.lock().spawn(async move {
            let mut writer = writer_arc.lock().await;
            // A write that errored after rolling left its stats undrained.
            record_rolls(&metrics, &mut writer, &destination);
            match writer.close().await {
                Ok(Some(close_stats)) => {
                    debug!(
                        destination = %destination,
                        compressed_bytes = close_stats.compressed_bytes,
                        "Closed evicted writer"
                    );
                    metrics.record_file_closed(close_stats.compressed_bytes);
                }
                Ok(None) => {
                    debug!(destination = %destination, "Closed empty evicted writer");
                }
                Err(e) => {
                    error!(error = %e, destination = %destination, "Failed to close evicted writer");
                    metrics.record_error();
                }
            }
            writer.take_settled()
        });
    }

    /// What the evicted writers that finished closing settled, without
    /// waiting on the ones still closing.
    fn collect_evictions(&self) -> Settled {
        let mut settled = Settled::default();
        let mut closing = self.evictions.lock();
        while let Some(joined) = closing.try_join_next() {
            match joined {
                Ok(closed) => settled.absorb(closed),
                Err(e) => {
                    error!(error = %e, "An evicted writer's close did not finish; its offsets stay held until a restart");
                }
            }
        }
        settled
    }

    /// Wait for every evicted writer still closing, and return what they
    /// settled.
    async fn await_evictions(&self) -> Settled {
        let mut closing = std::mem::take(&mut *self.evictions.lock());
        let mut settled = Settled::default();
        while let Some(joined) = closing.join_next().await {
            match joined {
                Ok(closed) => settled.absorb(closed),
                Err(e) => {
                    error!(error = %e, "An evicted writer's close did not finish; its offsets stay held until a restart");
                }
            }
        }
        settled
    }

    /// Create a new archive writer for a destination.
    ///
    /// Uses `startup_config` for archive/compression settings (restart-required).
    fn create_writer(&self, destination: &str) -> Result<ArchiveWriter> {
        let policy = RollingPolicy {
            max_size_bytes: self.startup_config.archive.roll_size_bytes,
            max_age_secs: self.startup_config.roll_interval_secs(),
        };

        let compressor = compressor_for(&self.startup_config.compression)?;

        let mut archive_config = self.startup_config.archive.clone();
        archive_config.path_template = format!(
            "{}/{}",
            destination, self.startup_config.archive.path_template
        );

        let storage = create_backend(&archive_config, &self.staging)?;

        Ok(
            ArchiveWriter::new(archive_config, policy, compressor, storage)
                .with_destination(destination),
        )
    }

    /// Request graceful shutdown
    pub fn shutdown(&self) {
        info!("Shutdown requested");
        self.cancel.cancel();
    }

    /// Drain the pipeline for shutdown: stop the source, write everything it
    /// already accepted, complete every file, upload what it can, then release.
    ///
    /// Call once [`run`](Self::run) has returned. The source closes first, so
    /// the Push listener stops answering new pushes while every push it already
    /// answered is received and written. On Kafka the consumer stops fetching
    /// and its commit still lands. Uploads get `DRAIN_UPLOAD_LIMIT`: a file
    /// still uploading then keeps its records unreleased, to be read again.
    #[instrument(skip(self))]
    pub async fn drain(&self) {
        if let Err(e) = self.transport.close().await {
            warn!(error = %e, "Closing the inbound transport failed; draining what it holds");
        }
        let received = self.drain_source().await;

        let batches = self.buffer.flush_all();
        info!(
            received,
            batches = batches.len(),
            total_bytes = batches.iter().map(|b| b.data.len()).sum::<usize>(),
            "Draining buffers"
        );
        let mut settled = self.write_staged(batches).await;
        settled.absorb(self.await_evictions().await);
        settled.absorb(self.close_all_writers().await);
        self.release_settled(settled).await;
        let uploaded = self.await_uploads(DRAIN_UPLOAD_LIMIT).await;
        self.release_settled(uploaded).await;

        if let Err(e) = self.dlq.shutdown().await {
            warn!(error = %e, "DLQ shutdown failed");
        }
        info!("Drain complete");
    }

    /// Receive from the closed source until it reports it is empty, pushing
    /// each block through the normal path. Returns the records received.
    async fn drain_source(&self) -> usize {
        let batch_size = self.shared_config.with(|c| c.kafka.batch_size);
        let mut received = 0usize;
        let mut idle_since = Instant::now();
        loop {
            match self.transport.recv(batch_size).await {
                Ok(batch) if !batch.is_empty() => {
                    received += batch.messages.len();
                    self.handle_batch(batch).await;
                    idle_since = Instant::now();
                }
                Ok(_) if idle_since.elapsed() < DRAIN_IDLE_LIMIT => {}
                Ok(_) => {
                    warn!(
                        received,
                        "Shutdown drain gave up: the source returned nothing and never reported it was empty"
                    );
                    return received;
                }
                Err(Error::Shutdown) => return received,
                Err(e) => {
                    warn!(error = %e, received, "Shutdown drain stopped on a receive error");
                    return received;
                }
            }
        }
    }

    /// Complete every open file, concurrently, and return what they settled.
    async fn close_all_writers(&self) -> Settled {
        let writers: Vec<(String, Arc<Mutex<ArchiveWriter>>)> = {
            let mut cache = self.writers.lock();
            let mut all = Vec::with_capacity(cache.len());
            while let Some(entry) = cache.pop_lru() {
                all.push(entry);
            }
            all
        };
        debug!(writer_count = writers.len(), "Closing archive writers");

        let closed = futures::future::join_all(writers.into_iter().map(|(dest, writer_arc)| {
            let metrics = &self.metrics;
            async move {
                let mut writer = writer_arc.lock().await;
                // A write that errored after rolling left its stats undrained.
                record_rolls(metrics, &mut writer, &dest);
                match writer.close().await {
                    Ok(Some(close_stats)) => {
                        debug!(
                            destination = %dest,
                            compressed_bytes = close_stats.compressed_bytes,
                            "Closed archive writer"
                        );
                        metrics.record_file_closed(close_stats.compressed_bytes);
                    }
                    Ok(None) => {
                        debug!(destination = %dest, "Closed empty archive writer");
                    }
                    Err(e) => {
                        error!(error = %e, destination = %dest, "Failed to close writer");
                    }
                }
                writer.take_settled()
            }
        }))
        .await;

        let mut settled = Settled::default();
        for one in closed {
            settled.absorb(one);
        }
        settled
    }

    /// Dead-letter a batch no file took, one entry per record, and release
    /// each record's offset: `Rejected` once the DLQ confirms it holds the
    /// record. When the store refused the batch for good, a record no DLQ
    /// backend can ever hold is `Dropped` with the reason, and so is every
    /// record with the DLQ off. Otherwise the whole batch is `Errored`, which
    /// ends the loop so a restart writes it again: a local failure can clear,
    /// and so can a failed DLQ write.
    async fn dead_letter(&self, batch: StagedBatch, error: &Error) {
        if !self.dlq.is_enabled() {
            self.settle_without_dlq(batch, error).await;
            return;
        }
        let records = batch.record_count as u64;
        let destination = batch.destination.clone();
        debug!(
            destination = %destination,
            data_bytes = batch.data.len(),
            records,
            error = %error,
            "Sending failed batch to DLQ"
        );
        let entries = match record_dead_letters(batch, &format!("storage_write_failed: {error}")) {
            Ok(entries) => entries,
            Err(offsets) => {
                // An offset with no record could never be released, and would hold the commit silently.
                error!(
                    records,
                    offsets = offsets.len(),
                    destination = %destination,
                    "A batch's records and offsets disagree; a restart writes it again"
                );
                self.release(tokens_of(offsets), DeliveryStatus::Errored)
                    .await;
                return;
            }
        };
        let Screened { writable, refused } = screen_dead_letters(&self.dlq, entries);
        if let Some((why, _)) = refused.first()
            && !error.is_refused()
        {
            error!(
                records,
                too_large = refused.len(),
                destination = %destination,
                reason = %why,
                error = %error,
                "The DLQ can never hold a record of a batch that failed locally; a restart writes the batch again"
            );
            let all = writable
                .into_iter()
                .map(|(_, offset)| offset)
                .chain(refused.into_iter().map(|(_, offset)| offset))
                .collect();
            self.release(tokens_of(all), DeliveryStatus::Errored).await;
            return;
        }

        let (entries, written): (Vec<DlqEntry>, Vec<KafkaOffset>) = writable.into_iter().unzip();
        let outcome = if entries.is_empty() {
            Ok(())
        } else {
            self.dlq.write_confirmed(entries).await
        };
        if let Err(dlq_err) = outcome {
            error!(
                error = %dlq_err,
                destination = %destination,
                records,
                "The DLQ could not take a batch no file took; a restart writes it again"
            );
            let all = written
                .into_iter()
                .chain(refused.into_iter().map(|(_, offset)| offset))
                .collect();
            self.release(tokens_of(all), DeliveryStatus::Errored).await;
            return;
        }
        if !written.is_empty() {
            self.metrics.record_dlq(written.len() as u64);
            scalo::logger::security::record_dlq(
                "storage_write_failed",
                &error.to_string(),
                Some(destination.as_str()),
            );
            self.release(tokens_of(written), DeliveryStatus::Rejected)
                .await;
        }
        if let Some((why, _)) = refused.first() {
            self.metrics.record_dropped(refused.len() as u64);
            error!(
                records = refused.len(),
                destination = %destination,
                reason = %why,
                error = %error,
                "Dropped records the store refused for good and the DLQ can never hold"
            );
            let dropped = refused.into_iter().map(|(_, offset)| offset).collect();
            self.release(tokens_of(dropped), DeliveryStatus::Dropped)
                .await;
        }
    }

    /// Release a batch no file took while the DLQ is off: `Dropped` with the
    /// reason when the store refused it for good, `Errored` otherwise.
    async fn settle_without_dlq(&self, batch: StagedBatch, error: &Error) {
        let status = if error.is_refused() {
            self.metrics.record_dropped(batch.record_count as u64);
            error!(
                records = batch.record_count,
                destination = %batch.destination,
                reason = %error,
                "Dropped records the store refused for good, with no DLQ to take them"
            );
            DeliveryStatus::Dropped
        } else {
            trace!(destination = %batch.destination, "DLQ disabled for a batch no file took");
            DeliveryStatus::Errored
        };
        self.release(tokens_of(batch.offsets), status).await;
    }

    /// Dead-letter the records routing refused for nesting past
    /// [`MAX_PARSE_DEPTH`], one entry each under its topic.
    ///
    /// The same bytes are refused on every attempt, so a record is released
    /// `Rejected` once the DLQ confirms it, and `Dropped`, counted, with the
    /// DLQ off or when no DLQ backend can hold it. A DLQ write that fails
    /// releases them `Errored`, which ends the loop so a restart refuses them
    /// again.
    async fn dead_letter_too_deep(&self, records: Vec<KafkaMessage>) {
        if records.is_empty() {
            return;
        }
        let count = records.len();
        let reason = Error::TooDeep {
            max: MAX_PARSE_DEPTH,
        }
        .to_string();
        // The records leave memory whether the DLQ takes them or not.
        self.memory_guard
            .release(records.iter().map(|m| m.payload.len() as u64).sum());

        if !self.dlq.is_enabled() {
            self.metrics.record_dropped(count as u64);
            error!(
                records = count,
                reason = %reason,
                "Dropped records nested too deep to route, with no DLQ to take them"
            );
            let offsets = records.iter().map(KafkaOffset::from).collect();
            self.release(tokens_of(offsets), DeliveryStatus::Dropped)
                .await;
            return;
        }

        let entries = records
            .into_iter()
            .map(|message| {
                let source =
                    DlqSource::kafka(message.topic.as_str(), message.partition, message.offset);
                let destination = message.topic.to_string();
                let (payload, offset) = message.into_parts();
                let entry = DlqEntry::new("dfe-archiver", reason.as_str(), payload)
                    .with_destination(destination)
                    .with_source(source);
                (entry, offset)
            })
            .collect();
        let Screened { writable, refused } = screen_dead_letters(&self.dlq, entries);
        let (entries, written): (Vec<DlqEntry>, Vec<KafkaOffset>) = writable.into_iter().unzip();
        let outcome = if entries.is_empty() {
            Ok(())
        } else {
            self.dlq.write_confirmed(entries).await
        };
        if let Err(dlq_err) = outcome {
            error!(
                error = %dlq_err,
                records = count,
                "The DLQ could not take records nested too deep to route; a restart refuses them again"
            );
            let all = written
                .into_iter()
                .chain(refused.into_iter().map(|(_, offset)| offset))
                .collect();
            self.release(tokens_of(all), DeliveryStatus::Errored).await;
            return;
        }
        if !written.is_empty() {
            self.metrics.record_dlq(written.len() as u64);
            scalo::logger::security::record_dlq("routing", &reason, None);
            self.release(tokens_of(written), DeliveryStatus::Rejected)
                .await;
        }
        if let Some((why, _)) = refused.first() {
            self.metrics.record_dropped(refused.len() as u64);
            error!(
                records = refused.len(),
                reason = %why,
                "Dropped records nested too deep to route that the DLQ can never hold"
            );
            let dropped = refused.into_iter().map(|(_, offset)| offset).collect();
            self.release(tokens_of(dropped), DeliveryStatus::Dropped)
                .await;
        }
    }

    /// Route inbound-filter DLQ entries surfaced by the transport, then release
    /// the offsets of the records the filter removed with the outcome.
    ///
    /// These are records the scalo inbound filter took out before they reached
    /// the archiver. The archiver configures no inbound filters, so both are
    /// normally empty -- but the no-silent-drop contract means we route any
    /// entries that do arrive, and a held source waits on every offset it
    /// handed out.
    ///
    /// An entry no DLQ backend can ever hold is dropped, counted and logged. A
    /// DLQ write that can clear releases every filtered offset `Errored`, which
    /// ends the loop so a restart routes the block again.
    async fn route_filter_dlq(&self, entries: Vec<FilteredDlqEntry>, filtered: Vec<KafkaOffset>) {
        if entries.is_empty() && filtered.is_empty() {
            return;
        }
        let status = if entries.is_empty() {
            // Only a drop filter matched: removed by policy, not lost.
            DeliveryStatus::Dropped
        } else {
            let dead_letters = entries
                .into_iter()
                .map(|entry| {
                    let destination = entry.key.as_deref().unwrap_or("filter").to_string();
                    let dead_letter = DlqEntry::new("dfe-archiver", entry.reason, entry.payload)
                        .with_destination(&destination);
                    (dead_letter, ())
                })
                .collect();
            let Screened { writable, refused } = screen_dead_letters(&self.dlq, dead_letters);
            let written = writable.len() as u64;
            let outcome = if writable.is_empty() {
                Ok(())
            } else {
                let writable = writable.into_iter().map(|(entry, ())| entry).collect();
                self.dlq.write_confirmed(writable).await
            };
            if let Err(dlq_err) = outcome {
                error!(
                    error = %dlq_err,
                    "The DLQ could not take inbound-filter dead letters; a restart routes them again"
                );
                DeliveryStatus::Errored
            } else {
                if let Some((reason, ())) = refused.first() {
                    self.metrics.record_dropped(refused.len() as u64);
                    error!(
                        records = refused.len(),
                        reason = %reason,
                        "Dropped inbound-filter dead letters the DLQ can never hold"
                    );
                }
                if written > 0 && self.dlq.is_enabled() {
                    self.metrics.record_dlq(written);
                    DeliveryStatus::Rejected
                } else {
                    // A disabled DLQ counts what it drops in dlq_dropped_total.
                    DeliveryStatus::Dropped
                }
            }
        };
        self.release(tokens_of(filtered), status).await;
    }

    /// Get metrics handle
    pub fn metrics(&self) -> Arc<ArchiverMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Check if archiver is healthy (transport up and not under memory pressure)
    pub fn is_healthy(&self) -> bool {
        self.transport.is_healthy() && !self.memory_guard.under_pressure()
    }

    /// Sync transport health check (for `HealthRegistry` callback).
    pub fn is_transport_healthy(&self) -> bool {
        self.transport.is_healthy()
    }

    /// Archive-sink status for the `HealthRegistry` callback, seeded by the
    /// startup probe and driven thereafter by upload and write outcomes.
    ///
    /// Degraded rather than Unhealthy: taking this pod out of readiness brings
    /// no object store back, and the archiver keeps staging and retrying while
    /// the sink is out.
    pub fn sink_health(&self) -> scalo::health::HealthStatus {
        if self.sink.is_open() {
            scalo::health::HealthStatus::Degraded
        } else {
            scalo::health::HealthStatus::Healthy
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{
        RefusedFile, Screened, SinkCircuit, UPLOAD_RETRY_FIRST, UPLOAD_RETRY_MAX, Uploader,
        buffer_config, dead_letter_refused_file, held_record_cap, publish_kafka_lag,
        record_files_opened, record_rolls, record_routing_fallbacks, restart_required_changes,
        screen_dead_letters, sink_confirmation, upload_retry_delay, upload_slots,
        upload_until_settled,
    };
    use crate::config::validate_config;
    use crate::metrics::ArchiverMetrics;
    use dfe_archiver_core::Error;
    use dfe_archiver_core::OffsetSet;
    use dfe_archiver_core::archive::Settled;
    use dfe_archiver_core::archive::{ArchiveWriter, PendingFile, RollingPolicy};
    use dfe_archiver_core::buffer::DEFAULT_SPOOL_DIR;
    use dfe_archiver_core::compression::{Compressor, compressor_for};
    use dfe_archiver_core::config::{ArchiveConfig, CompressionConfig, Config, RoutingConfig};
    use dfe_archiver_core::routing::Router;
    use dfe_archiver_core::storage::PendingUpload;
    use dfe_archiver_core::types::KafkaMessage;
    use dfe_archiver_io::Staging;
    use dfe_archiver_io::storage::create_backend;
    use scalo::dlq::{Dlq, DlqConfig, DlqEntry, DlqMode, FileDlqConfig, KafkaDlqConfig};
    use scalo::scaling::{ScalingPressure, ScalingPressureConfig};
    use scalo::transport::{DeadLetterReason, SinkConfirmation};
    use std::future::Future;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// Held offsets may take a quarter of the memory limit, at 32 bytes each.
    #[test]
    fn the_held_record_cap_is_a_quarter_of_memory_at_32_bytes_a_record() {
        assert_eq!(held_record_cap(1024 * 1024 * 1024), 8 * 1024 * 1024);
        assert_eq!(held_record_cap(0), 1, "a zero limit still caps");
    }

    /// Retries back off to a ceiling, drawn from the upper half of each step
    /// so uploads that failed together spread out.
    #[test]
    fn upload_retries_back_off_to_a_ceiling_with_jitter() {
        for attempt in 0..40 {
            let step = UPLOAD_RETRY_FIRST
                .saturating_mul(2u32.saturating_pow(attempt.min(16)))
                .min(UPLOAD_RETRY_MAX);
            let delay = upload_retry_delay(attempt);
            assert!(
                delay >= step / 2 && delay <= step,
                "attempt {attempt}: {delay:?} outside half of {step:?}"
            );
        }
        assert!(upload_retry_delay(39) <= UPLOAD_RETRY_MAX);
        assert!(upload_retry_delay(39) >= UPLOAD_RETRY_MAX / 2);
    }

    /// A completed object-store upload is the store's own answer, while a
    /// local file is written on this node only.
    #[test]
    fn the_destination_scheme_decides_what_a_completed_file_proves() {
        let archive = |destination: &str| ArchiveConfig {
            destination: destination.to_string(),
            ..ArchiveConfig::default()
        };
        for remote in [
            "s3://bucket/archive",
            "minio://bucket/archive",
            "gs://bucket/archive",
            "az://container/archive",
        ] {
            assert_eq!(
                sink_confirmation(&archive(remote)),
                SinkConfirmation::Remote,
                "{remote}"
            );
        }
        for local in ["file:///srv/archive", "/srv/archive"] {
            assert_eq!(
                sink_confirmation(&archive(local)),
                SinkConfirmation::Local,
                "{local}"
            );
        }
    }

    #[test]
    fn test_cleared_brokers_fails_validation() {
        let mut config = Config::default();
        config.kafka.brokers.clear();
        assert!(
            validate_config(&config).is_err(),
            "empty brokers should fail"
        );
    }

    /// The configured spool has to be the directory the buffer creates, not a
    /// path compiled into the binary.
    #[test]
    fn test_configured_spool_dir_reaches_the_buffer() {
        let mut config = Config::default();
        config.buffer.spool_dir = "/srv/dfe/spool".to_string();

        assert_eq!(
            buffer_config(&config).spool_dir,
            Path::new("/srv/dfe/spool"),
        );
    }

    /// `buffer.flush_bytes` is the size a destination's buffer flushes at: 1 MiB
    /// by default, and a configured value moves it.
    #[test]
    fn flush_bytes_sets_the_size_a_buffer_flushes_at() {
        let default = first_flush(|_| {});
        assert_eq!(Config::default().buffer.flush_bytes, 1024 * 1024);
        assert_eq!(
            default.data.len(),
            (1024 * 1024usize).div_ceil(RECORD) * RECORD,
            "the default flushes at the first record past 1 MiB"
        );
        assert_eq!(
            first_flush(|config| config.buffer.flush_bytes = 4096)
                .data
                .len(),
            4096usize.div_ceil(RECORD) * RECORD,
            "a configured flush_bytes moves the flush"
        );
    }

    /// `buffer.flush_records` flushes a buffer at a record count, whatever its
    /// size. The default is past what a 1 MiB buffer of these records holds.
    #[test]
    fn flush_records_sets_the_record_count_a_buffer_flushes_at() {
        assert_eq!(Config::default().buffer.flush_records, 100_000);
        assert_eq!(
            first_flush(|config| config.buffer.flush_records = 3).record_count,
            3,
            "a configured flush_records moves the flush"
        );
        assert_eq!(
            first_flush(|config| config.buffer.flush_records = 50).record_count,
            50
        );
    }

    /// Records push this many bytes each: 1000 of payload and a newline.
    const RECORD: usize = 1001;

    /// The first batch a buffer on `configure`d settings flushes, pushing
    /// records of [`RECORD`] bytes to one destination.
    fn first_flush(configure: impl FnOnce(&mut Config)) -> dfe_archiver_core::buffer::StagedBatch {
        let spool = tempfile::TempDir::new().expect("spool");
        let mut config = Config::default();
        config.buffer.spool_dir = spool.path().display().to_string();
        configure(&mut config);
        let buffer = dfe_archiver_core::buffer::TieredBufferManager::new(buffer_config(&config))
            .expect("buffer");
        for offset in 0.. {
            let message = KafkaMessage::for_test(vec![b'x'; RECORD - 1], "events", 0, offset);
            if let Some(batch) = buffer.push("dest", message).expect("push").pop() {
                return batch;
            }
        }
        unreachable!("the offsets never run out")
    }

    /// An upload that takes a while and records the most running at once.
    struct CountedUpload {
        running: Arc<AtomicUsize>,
        most: Arc<AtomicUsize>,
    }

    impl PendingUpload for CountedUpload {
        fn path(&self) -> &'static str {
            "counted"
        }

        fn size(&self) -> u64 {
            0
        }

        fn attempt<'life0, 'async_trait>(
            &'life0 self,
        ) -> Pin<Box<dyn Future<Output = dfe_archiver_core::Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
                self.most.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.running.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn block<'life0, 'async_trait>(
            &'life0 self,
            _index: usize,
        ) -> Pin<
            Box<
                dyn Future<Output = dfe_archiver_core::Result<Option<Vec<u8>>>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(None) })
        }

        fn discard<'life0, 'async_trait>(
            &'life0 self,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async {})
        }
    }

    /// `buffer.writer_parallelism` bounds the upload attempts running at once:
    /// two by default, as many as configured otherwise.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writer_parallelism_bounds_the_uploads_running_at_once() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = Arc::new(ArchiverMetrics::register(&manager, "test"));
        let sink = Arc::new(SinkCircuit {
            open: AtomicBool::new(false),
            scaling: Arc::new(ScalingPressure::new(
                ScalingPressureConfig::default(),
                crate::scaling_components(),
            )),
            metrics: Arc::clone(&metrics),
        });
        let most_at_once = async |config: Config| {
            let uploader = Arc::new(Uploader {
                slots: upload_slots(&config),
                sink: Arc::clone(&sink),
                metrics: Arc::clone(&metrics),
                backend: "memory",
                dlq: Arc::new(Dlq::disabled()),
                compressor: Arc::from(compressor_for(&config.compression).expect("compressor")),
            });
            let running = Arc::new(AtomicUsize::new(0));
            let most = Arc::new(AtomicUsize::new(0));
            let mut uploads = tokio::task::JoinSet::new();
            for _ in 0..8 {
                let file = PendingFile {
                    upload: Box::new(CountedUpload {
                        running: Arc::clone(&running),
                        most: Arc::clone(&most),
                    }),
                    offsets: OffsetSet::default(),
                    records: 1,
                    destination: "events".to_string(),
                };
                uploads.spawn(upload_until_settled(file, Arc::clone(&uploader)));
            }
            while uploads.join_next().await.is_some() {}
            most.load(Ordering::SeqCst)
        };

        assert_eq!(Config::default().buffer.writer_parallelism, 2);
        assert_eq!(
            most_at_once(Config::default()).await,
            2,
            "the default runs two uploads at once"
        );
        let mut config = Config::default();
        config.buffer.writer_parallelism = 3;
        assert_eq!(
            most_at_once(config).await,
            3,
            "a configured writer_parallelism moves the bound"
        );
    }

    /// A default deployment writes under the path the image pre-creates, and
    /// an absolute one, because the container working directory is root-owned.
    #[test]
    fn test_default_spool_dir_is_the_absolute_image_path() {
        let config = Config::default();

        assert_eq!(config.buffer.spool_dir, DEFAULT_SPOOL_DIR);
        assert_eq!(
            buffer_config(&config).spool_dir,
            Path::new("/var/spool/dfe/archiver"),
        );
    }

    /// The success counter is the denominator the sink-failure alert divides
    /// the error rate by, so it has to move when the pipeline opens a file.
    #[tokio::test]
    async fn test_opening_a_file_moves_files_created_total() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = ArchiverMetrics::register(&manager, "test");

        let dir = tempfile::TempDir::new().expect("temp dir");
        let archive = ArchiveConfig {
            destination: format!("file://{}", dir.path().display()),
            path_template: "{year}/{month}/{day}/{hour}/archive".to_string(),
            ..ArchiveConfig::default()
        };
        let compressor = compressor_for(&CompressionConfig::default()).expect("compressor");
        let staging = Staging::open(dir.path().join("uploads")).expect("staging");
        let storage = create_backend(&archive, &staging).expect("backend");
        let mut writer = ArchiveWriter::new(archive, RollingPolicy::default(), compressor, storage);

        writer.write(b"one\n").await.expect("write");
        record_files_opened(&metrics, &mut writer);

        let rendered = manager.render();
        assert!(
            rendered
                .lines()
                .any(|line| line == "archiver_files_created_total 1"),
            "files_created_total did not move after a write:\n{rendered}"
        );
    }

    /// A staged file read back from compressed blocks, as its local copy is.
    struct BlockUpload {
        blocks: Vec<Vec<u8>>,
        discarded: AtomicBool,
    }

    impl PendingUpload for BlockUpload {
        fn path(&self) -> &'static str {
            "events/2026/archive-0001-token.jsonl.zst"
        }

        fn size(&self) -> u64 {
            0
        }

        fn attempt<'life0, 'async_trait>(
            &'life0 self,
        ) -> Pin<Box<dyn Future<Output = dfe_archiver_core::Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(()) })
        }

        fn block<'life0, 'async_trait>(
            &'life0 self,
            index: usize,
        ) -> Pin<
            Box<
                dyn Future<Output = dfe_archiver_core::Result<Option<Vec<u8>>>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { Ok(self.blocks.get(index).cloned()) })
        }

        fn discard<'life0, 'async_trait>(
            &'life0 self,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.discarded.store(true, Ordering::SeqCst) })
        }
    }

    /// Dead-letter a refused file of `records` records in plain `blocks`
    /// through a file DLQ, and return what it settled and the entries the DLQ
    /// holds.
    async fn dead_letter_blocks(blocks: &[&[u8]], records: u64) -> (Settled, Vec<DlqEntry>, bool) {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = Arc::new(ArchiverMetrics::register(&manager, "test"));
        let dir = tempfile::TempDir::new().expect("dlq dir");
        let config = DlqConfig {
            mode: DlqMode::FileOnly,
            file: FileDlqConfig {
                enabled: true,
                path: dir.path().to_path_buf(),
                compress_rotated: false,
                ..FileDlqConfig::default()
            },
            flush_interval_ms: 20,
            ..DlqConfig::default()
        };
        let dlq = Arc::new(
            Dlq::spawn(&config, "dfe-archiver", None, CancellationToken::new()).expect("dlq"),
        );
        let compressor: Arc<dyn Compressor + Send + Sync> =
            Arc::from(compressor_for(&CompressionConfig::default()).expect("compressor"));
        let upload = BlockUpload {
            blocks: blocks
                .iter()
                .map(|block| compressor.compress(block).expect("compress"))
                .collect(),
            discarded: AtomicBool::new(false),
        };
        let uploader = Uploader {
            slots: upload_slots(&Config::default()),
            sink: Arc::new(SinkCircuit {
                open: AtomicBool::new(false),
                scaling: Arc::new(ScalingPressure::new(
                    ScalingPressureConfig::default(),
                    crate::scaling_components(),
                )),
                metrics: Arc::clone(&metrics),
            }),
            metrics,
            backend: "memory",
            dlq: Arc::clone(&dlq),
            compressor,
        };
        let refusal = Error::refused("the store refused the object");
        let file = RefusedFile {
            upload: &upload,
            records,
            destination: "events",
            refusal: &refusal,
        };
        let settled = dead_letter_refused_file(&file, OffsetSet::default(), &uploader).await;
        dlq.shutdown().await.expect("dlq shutdown");

        let mut entries = Vec::new();
        for file in walkdir::WalkDir::new(dir.path()) {
            let file = file.expect("dlq file");
            if !file.file_type().is_file() {
                continue;
            }
            let text = std::fs::read_to_string(file.path()).expect("read dlq file");
            for line in text.lines() {
                entries.push(serde_json::from_str::<DlqEntry>(line).expect("a DLQ entry"));
            }
        }
        (settled, entries, upload.discarded.load(Ordering::SeqCst))
    }

    /// A refused file of one record per line goes to the DLQ a record at a
    /// time, carrying the destination a batch carries, not the file path.
    #[tokio::test]
    async fn a_refused_file_goes_to_the_dlq_a_record_at_a_time() {
        let (settled, entries, discarded) =
            dead_letter_blocks(&[b"{\"id\":0}\n{\"id\":1}\n", b"{\"id\":2}\n"], 3).await;

        let payloads: Vec<&[u8]> = entries
            .iter()
            .map(|entry| entry.payload.as_slice())
            .collect();
        assert_eq!(
            payloads,
            vec![&b"{\"id\":0}"[..], b"{\"id\":1}", b"{\"id\":2}"]
        );
        assert!(
            entries
                .iter()
                .all(|entry| entry.destination.as_deref() == Some("events")),
            "{entries:?}"
        );
        assert_eq!(settled.rejected_records, 3);
        assert_eq!(settled.dropped_records, 0);
        assert!(
            discarded,
            "the staged copy is removed once the DLQ holds it"
        );
    }

    /// A payload holding a newline of its own, text or binary, would split
    /// across entries line by line, so each block of a file holding more lines
    /// than records goes to the DLQ whole, as one entry.
    #[tokio::test]
    async fn a_payload_with_its_own_newline_never_reaches_the_dlq_in_pieces() {
        // A MessagePack map carrying a 0x0a byte, beside a pretty-printed JSON record.
        let binary: &[u8] = &[0x81, 0xa2, b'i', b'd', 0x0a];
        let mut second = b"{\"id\":1,\n  \"note\":\"two lines\"}\n".to_vec();
        second.extend_from_slice(binary);
        second.push(b'\n');
        let first: &[u8] = b"{\"id\":0}\n";

        let (settled, entries, discarded) = dead_letter_blocks(&[first, &second], 3).await;

        let payloads: Vec<&[u8]> = entries
            .iter()
            .map(|entry| entry.payload.as_slice())
            .collect();
        assert_eq!(
            payloads,
            vec![first, second.as_slice()],
            "one entry a block, each block whole"
        );
        assert!(
            entries
                .iter()
                .all(|entry| entry.reason.contains("whole block")
                    && entry.destination.as_deref() == Some("events")),
            "{entries:?}"
        );
        assert_eq!(settled.rejected_records, 3);
        assert_eq!(settled.dropped_records, 0);
        assert!(discarded);
    }

    /// A second write to the same open file must not count another creation.
    #[tokio::test]
    async fn test_writing_to_an_open_file_creates_nothing() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let archive = ArchiveConfig {
            destination: format!("file://{}", dir.path().display()),
            path_template: "{year}/{month}/{day}/{hour}/archive".to_string(),
            ..ArchiveConfig::default()
        };
        let compressor = compressor_for(&CompressionConfig::default()).expect("compressor");
        let staging = Staging::open(dir.path().join("uploads")).expect("staging");
        let storage = create_backend(&archive, &staging).expect("backend");
        let mut writer = ArchiveWriter::new(archive, RollingPolicy::default(), compressor, storage);

        writer.write(b"one\n").await.expect("write");
        assert_eq!(writer.take_files_opened(), 1);

        writer.write(b"two\n").await.expect("write");
        assert_eq!(writer.take_files_opened(), 0);
    }

    /// A field no source carries is otherwise indistinguishable from a tenant
    /// named `unknown`, so the counter has to move on the router's own output
    /// rather than on a hand-built index list.
    #[test]
    fn test_a_missing_routing_field_moves_routing_fallback_total() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = ArchiverMetrics::register(&manager, "test");

        let fields = vec!["org_id".to_string()];
        let router = Router::new(RoutingConfig {
            mode: "expression".to_string(),
            expression_fields: fields.clone(),
            default_segment: "unknown".to_string(),
        });
        let guards: Vec<AtomicU64> = fields.iter().map(|_| AtomicU64::new(0)).collect();

        let carried =
            KafkaMessage::for_test(br#"{"org_id":"acme"}"#.to_vec(), "default_land", 0, 0);
        let missing =
            KafkaMessage::for_test(br#"{"customer":"acme"}"#.to_vec(), "default_land", 0, 1);

        for message in [&carried, &missing] {
            let outcome = router.route(message).expect("route");
            record_routing_fallbacks(&metrics, &fields, &guards, &outcome.fallback_fields);
        }

        let rendered = manager.render();
        assert!(
            rendered
                .lines()
                .any(|line| line == r#"archiver_routing_fallback_total{field="org_id"} 1"#),
            "the record carrying org_id must not count as a fallback:\n{rendered}"
        );
    }

    /// Both halves of a `write_record` can roll, so the close counter is
    /// drained rather than returned -- returning one dropped the other.
    #[tokio::test]
    async fn test_every_roll_reaches_files_closed_total() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = ArchiverMetrics::register(&manager, "test");

        let dir = tempfile::TempDir::new().expect("temp dir");
        let archive = ArchiveConfig {
            destination: format!("file://{}", dir.path().display()),
            path_template: "{year}/{month}/{day}/{hour}/archive".to_string(),
            ..ArchiveConfig::default()
        };
        let compressor = compressor_for(&CompressionConfig::default()).expect("compressor");
        let staging = Staging::open(dir.path().join("uploads")).expect("staging");
        let storage = create_backend(&archive, &staging).expect("backend");
        // A zero-second interval rolls on each half of a write_record.
        let policy = RollingPolicy {
            max_size_bytes: 1024 * 1024,
            max_age_secs: 0,
        };
        let mut writer = ArchiveWriter::new(archive, policy, compressor, storage);

        writer.write_record(b"first").await.expect("write");
        writer.write_record(b"second").await.expect("write");
        record_rolls(&metrics, &mut writer, "default_land");

        let rendered = manager.render();
        assert!(
            rendered
                .lines()
                .any(|line| line == "archiver_files_closed_total 3"),
            "files_closed_total dropped a roll:\n{rendered}"
        );
        assert!(
            rendered
                .lines()
                .any(|line| line == r#"archiver_archive_roll_total{trigger="age"} 3"#),
            "archive_roll_total dropped a roll:\n{rendered}"
        );
    }

    /// A new destination is bound at construction, so the reloader reports it
    /// rather than claiming the running archiver picked it up.
    #[test]
    fn test_changed_destination_is_restart_required() {
        let old = Config::default();
        let mut new = old.clone();
        new.archive.destination = "file:///srv/dfe/archive".to_string();

        assert_eq!(restart_required_changes(&old, &new), vec!["archive"]);
    }

    /// The two fields the running pipeline re-reads are reported as applied.
    #[test]
    fn test_hot_reloaded_fields_are_not_restart_required() {
        let old = Config::default();
        let mut new = old.clone();
        new.kafka.batch_size = old.kafka.batch_size + 1;
        new.buffer.backpressure_pause_secs = old.buffer.backpressure_pause_secs + 1;

        assert!(restart_required_changes(&old, &new).is_empty());
    }

    /// The consumer is armed at construction, so turning acknowledgements off
    /// on a running archiver changes nothing until it restarts.
    #[test]
    fn test_acknowledgements_are_restart_required() {
        let old = Config::default();
        let mut new = old.clone();
        new.kafka.acknowledgements = scalo::transport::AcknowledgementsConfig::new(false);

        assert_eq!(restart_required_changes(&old, &new), vec!["kafka"]);
    }

    /// Every section the archiver snapshots has to be named, including the DLQ
    /// that is compared as JSON.
    #[test]
    fn test_every_startup_bound_section_is_reported() {
        let old = Config::default();
        let mut new = old.clone();
        new.transport = "grpc".to_string();
        new.kafka.topics = vec!["other".to_string()];
        new.grpc.listen = Some("0.0.0.0:6000".to_string());
        new.archive.destination = "file:///srv/dfe/archive".to_string();
        new.buffer.flush_bytes += 1;
        new.routing.mode = "topic".to_string();
        new.compression.codec = "gzip".to_string();
        new.dlq.enabled = !old.dlq.enabled;

        assert_eq!(
            restart_required_changes(&old, &new),
            vec![
                "transport",
                "kafka",
                "grpc",
                "archive",
                "buffer",
                "routing",
                "compression",
                "dlq"
            ]
        );
    }

    /// An entry over the only backend's ceiling is screened out before any
    /// write, with its reason, and the entries around it still go to the write.
    #[tokio::test]
    async fn a_dead_letter_no_backend_can_hold_is_screened_out_with_its_reason() {
        let mut kafka = scalo::transport::KafkaConfig {
            brokers: vec!["127.0.0.1:1".to_string()],
            group: String::new(),
            ..scalo::transport::KafkaConfig::default()
        };
        kafka.sizing.producer.message_max_bytes = Some(4096);
        let config = DlqConfig {
            mode: DlqMode::KafkaOnly,
            file: FileDlqConfig {
                enabled: false,
                ..FileDlqConfig::default()
            },
            kafka: KafkaDlqConfig {
                enabled: true,
                ..KafkaDlqConfig::default()
            },
            ..DlqConfig::default()
        };
        let dlq = Dlq::spawn(
            &config,
            "dfe-archiver",
            Some(&kafka),
            CancellationToken::new(),
        )
        .expect("dlq");
        let entry =
            |bytes: usize| DlqEntry::new("dfe-archiver", "storage_write_failed", vec![b'x'; bytes]);

        let Screened { writable, refused } = screen_dead_letters(
            &dlq,
            vec![(entry(100), 0), (entry(8192), 1), (entry(200), 2)],
        );

        assert_eq!(
            writable.iter().map(|(_, tag)| *tag).collect::<Vec<_>>(),
            vec![0, 2],
            "the entries under the ceiling are written, each with its tag"
        );
        assert!(
            matches!(
                refused.as_slice(),
                [(DeadLetterReason::TooLarge { bytes, limit }, 1)] if *limit == 4096 - 128 && bytes > limit
            ),
            "{refused:?}"
        );

        // Ten records over the ceiling together, each well under it alone.
        let whole = screen_dead_letters(&dlq, vec![(entry(10 * 1000), ())]);
        assert_eq!(whole.refused.len(), 1, "the batch as one entry is refused");
        let per_record = screen_dead_letters(&dlq, (0..10).map(|i| (entry(1000), i)).collect());
        assert_eq!(
            per_record.writable.len(),
            10,
            "every record fits on its own"
        );
        assert!(per_record.refused.is_empty(), "{:?}", per_record.refused);

        assert!(
            screen_dead_letters(&Dlq::disabled(), vec![(entry(8192), ())])
                .refused
                .is_empty(),
            "a disabled DLQ refuses nothing"
        );
        dlq.shutdown().await.expect("dlq shutdown");
    }

    /// Position lag reaches the `kafka_lag` term the composite registers, the
    /// one KEDA scales on, and the direct transport's `None` leaves it alone.
    #[test]
    fn position_lag_drives_the_kafka_lag_scaling_term() {
        let manager = scalo::metrics::MetricsManager::with_config(
            scalo::metrics::MetricsConfig::offline("archiver"),
        );
        let metrics = ArchiverMetrics::register(&manager, "test");
        let scaling = ScalingPressure::new(
            ScalingPressureConfig::default(),
            crate::scaling_components(),
        );
        let kafka_lag = |scaling: &ScalingPressure| {
            scaling
                .snapshot()
                .components
                .into_iter()
                .find(|component| component.name == "kafka_lag")
                .map(|component| component.raw_value)
        };
        assert_eq!(kafka_lag(&scaling), Some(0.0), "the term is registered");

        publish_kafka_lag(Some(50_000), &metrics, &scaling);
        assert_eq!(kafka_lag(&scaling), Some(50_000.0));
        assert!(
            scaling.calculate() > 0.0,
            "the lag moves the composite KEDA reads"
        );
        assert!(
            manager
                .render()
                .lines()
                .any(|line| line == "archiver_kafka_lag 50000"),
            "the gauge reads the same lag:\n{}",
            manager.render()
        );

        publish_kafka_lag(None, &metrics, &scaling);
        assert_eq!(
            kafka_lag(&scaling),
            Some(50_000.0),
            "the direct transport leaves the term alone"
        );
    }
}
