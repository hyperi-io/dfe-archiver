<!--
  Project:      dfe-archiver
  File:         docs/metrics.md
  Purpose:      Reference for the metrics the archiver records itself
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Metrics reference

The metrics listener, `0.0.0.0:9090` by default, serves `/metrics`, `/livez`, `/readyz` and `/scaling/pressure`. Names are bare unless `metrics.namespace` sets a prefix, and none is set by default. `dfe-archiver metrics-manifest` prints every series with its type, labels and description, scalo's runtime, transport and spool series included. The tables below are the series the archiver records itself.

## Counters

| Metric | What it counts |
|---|---|
| `messages_received_total` | Records received from the transport |
| `messages_written_total` | Records written into an open archive file, before the store confirms it |
| `messages_archived_total` | Records in archive files the store confirmed |
| `messages_dlq_total` | Records the DLQ confirmed it holds |
| `messages_dropped_total{reason}` | Records dropped for good: `refused` (the store or the codec, with the DLQ off), `dlq_too_large`, `too_deep` (with the DLQ off) or `unreplayable` (written nowhere, on `grpc` or with acknowledgements off) |
| `bytes_written_total` | Uncompressed bytes written into archive files |
| `bytes_compressed_total` | Compressed bytes written into archive files |
| `files_created_total` | Archive files opened |
| `files_closed_total` | Archive files closed, by a roll, an eviction or shutdown |
| `archive_roll_total{trigger}` | Files rolled, by `size` or `age` |
| `writer_evictions_total` | Writers closed because `archive.max_writers` was reached |
| `hot_buffer_evictions_total` | Hot buffers flushed early because a record for another destination needed their slot |
| `flush_operations_total` | Batches written into archive files |
| `archive_errors_total` | Failed write, close and upload attempts, each counted |
| `kafka_commit_errors_total` | Offset releases the transport failed |
| `routing_errors_total` | Records routing could not read, archived under their topic |
| `routing_fallback_total{field}` | Expression-routing fields a record did not carry, by field path |
| `staged_files_recovered_total` | Complete staged files a previous process left, uploaded by this one |
| `staged_files_quarantined_total{reason}` | Staged files moved to quarantine: `unreadable`, `incomplete`, `corrupt` or `no_store` |
| `staged_files_removed_total{reason}` | Staged files removed at startup: `replayed` or `quarantine_full` |

## Gauges

| Metric | What it holds |
|---|---|
| `kafka_lag` | Records past this pod's read position, summed across its partitions. The commit an open file holds does not inflate it |
| `pipeline_delivery_guarantee{guarantee,reason}` | 1 for the delivery guarantee in force |
| `hot_buffers_active` | Hot buffers, at most 64, an empty one included until it is evicted |
| `hot_buffers_bytes` | Bytes in the hot buffers |
| `buffer_bytes` | Bytes in the hot buffers, the same value as `hot_buffers_bytes` |
| `buffer_records` | The hot-buffer count, the same value as `hot_buffers_active`, not a record count |
| `unique_destinations` | Archive writers open, one per destination |
| `uploads_pending` | Archive files staged locally and not yet confirmed by the store |
| `staged_bytes` | Bytes of archive files staged locally and not yet uploaded |
| `compression_ratio` | Compressed over uncompressed bytes of the last batch written |
| `events_per_second` | Records received per second since the last batch |
| `pipeline_last_batch_timestamp_seconds` | Unix time of the last batch processed |

## Histograms

| Metric | What it measures |
|---|---|
| `batch_size_bytes` | Uncompressed size of each batch written |
| `flush_duration_seconds` | Time to write a batch into its file, retries included |
| `compression_duration_seconds` | Time to compress a batch |
| `archive_file_size_bytes` | Compressed size of each file closed |
