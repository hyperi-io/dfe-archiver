<!--
  Project:      dfe-archiver
  File:         docs/delivery.md
  Purpose:      When a record is released, what can drop one, and how disk and memory stay bounded
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Delivery

Delivery makes a closed file durable and only then releases its records, and is one part of [dfe-archiver](architecture.md).

| Fact | Value |
|---|---|
| Repo | `hyperi-io/dfe-archiver`, `crates/archiver/src/archiver.rs` and `crates/io/src/storage.rs` |
| Image and chart | `ghcr.io/hyperi-io/dfe-archiver`, and the thin Helm chart each release assembles from the deployment contract |
| Config | `kafka.acknowledgements.enabled`, `archive.roll_interval_secs`, the `buffer.*` spool caps ([configuration.md](configuration.md)) |
| Endpoints | `/livez`, `/readyz`, `/metrics` and `/scaling/pressure` on port 9090 |
| Metrics | `messages_archived_total`, `uploads_pending`, `staged_bytes`, `messages_dropped_total{reason}` ([metrics.md](metrics.md)) |

A record's offset is released only after its file is synced or uploaded.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
  accTitle: L4 Detail -- Delivery
  accDescr: A batch is received, buffered and written into its destination's open file. When the file closes, a local file is synced or a staged file is uploaded in a background task, and only then are the file's offsets released.
  subgraph pipe["Into a file"]
    direction LR
    receive("<b>Receive</b><br/><span>a batch from the transport</span>")
    buffer("<b>Buffer</b><br/><span>held per destination</span>")
    write("<b>Write</b><br/><span>into the open file</span>")
    receive --> buffer --> write
  end
  subgraph dl["Delivery"]
    direction LR
    sync("<b>Sync local file</b><br/><span>on close</span>")
    upload("<b>Upload staged file</b><br/><span>in a background task</span>")
    release("<b>Release offsets</b><br/><span>once the file is durable</span>")
    sync -->|synced| release
    upload -->|stored| release
  end
  pipe -->|file closes| dl
  class receive,buffer,write,sync,upload,release part
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  style pipe fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style dl fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Delivery. Offsets are released last, once the file is synced or stored.* Up: [L3 Components -- dfe-archiver](architecture.md). Down: none.

On `kafka` with `kafka.acknowledgements.enabled` (the default) no record is lost as long as the topic holds it. A kill, a failed upload, a failing local disk or a store outage costs duplicates or consumer lag. Kafka's retention deletes records whether or not the archiver has read them, and a commit that falls below the log start resets to the earliest record left, so size retention for the longest outage you will ride out. One case drops a record: it is refused for good, by the store, the codec or the nesting check, and no DLQ can take it. That record is counted. On `grpc` the archive copy is at-most-once, because the listener answers each push once its records are queued ([internals.md](internals.md#two-transports-one-pipeline)). How the pipeline gets a record into a file is [internals.md](internals.md).

## A record is released when its file is durable

The consumer records every offset it hands out, and the writer holds each record's offset on the file the record went into. The sync or upload starts when the file closes, on a roll, an age close, an eviction or shutdown.

Default routing on `_org_id` sends one partition's records to several files that complete at different times, so a partition commits up to its lowest offset not yet released. `kafka.acknowledgements.enabled: false` commits each batch at receipt instead, and a kill then loses what the buffers and open files held.

An object-store file is written to `<buffer.spool_dir>/uploads` and uploaded whole once it closes, in a background task the loop collects each cycle, so the loop never waits on the store. A failed upload keeps the staged file and its held offsets and tries again. The waits double from 0.5 s to a 60 s ceiling, each drawn from the upper half of its step so uploads that failed together spread out. Every attempt is a fresh multipart upload from the staged copy, so it signs its requests afresh however long the store has been down. A failed attempt aborts its upload.

A staged copy that cannot be read back, because it is missing, unreadable by permission or shorter than the bytes staged, moves to `<buffer.spool_dir>/uploads/quarantine` and its offsets are released `Errored`. Any other read error, EIO included, is retried.

### Holding costs memory

A held record costs about 32 bytes until its file is durable. At 10,000 records a second that is 96 MB over a 300 s roll and 1.15 GB over an hour, so when offsets are held an unset `archive.roll_interval_secs` is 300 s rather than 3600 s. A configured value always wins, and one above 900 logs a startup warning with the cost.

As uploads fail, held records and staged bytes grow, so both are pressure sources on the self-regulation latch. Held records read against a quarter of the memory limit at 32 bytes each, and staged bytes against `buffer.staged_bytes_cap`. Past the latch's `pause_above` (0.8 by default) the consumer pauses its partitions and the Push listener sheds pushes with `Unavailable`, so a store outage turns into lag rather than memory or disk growth. With self-regulation off nothing pauses, and the archiver logs that at startup.

## Refused for good goes to the DLQ

A refusal of the object itself is the one permanent failure. That is an invalid path, a key past 1024 bytes, checked before any record is staged, a name the local filesystem cannot hold, or an `EntityTooLarge`, `KeyTooLongError` or `InvalidObjectName` answer. Anything else, credentials and a missing bucket included, is retried, because retrying costs lag where dropping costs records.

A file the store rejects is read back a block at a time, each block one flush, decompressed separately, and its records go to the DLQ one entry per line with the routed destination. A payload that holds a newline spans lines, so when a file holds more lines than records each block goes whole, as one entry saying so, and no record is split across entries. The file's offsets are released `Rejected` once the DLQ confirms them.

A batch the codec rejects as it is written goes to the DLQ through scalo's confirming write, one entry per record, and a record's offset is released `Rejected` once the DLQ confirms its write. Before any write, `Dlq::refusal` screens each entry. The Kafka backend is the one with a ceiling, the producer's `message.max.bytes`, against the entry as written: a JSON object goes in as itself and anything else as text or base64. A file backend beside Kafka, as under the default `dlq.mode: cascade`, takes what Kafka rejects, so a DLQ rejects an entry when Kafka is its one backend. A record no backend can hold is released `Dropped`, counted and logged, and the rest still go. A broker or topic ceiling below the producer's is not seen there, and fails the write instead.

With the DLQ off, as it always is on `grpc`, a refused batch or file is released `Dropped` with its reason. A failed DLQ write is released `Errored`, because it can clear. Inbound-filter dead letters are screened the same way, and one no backend can hold is dropped, because the filter routed it out of the archive by policy.

Every dropped record is counted in `messages_dropped_total` under one `reason`: `refused` (the store or the codec, with the DLQ off), `dlq_too_large`, `too_deep` (nested past the parse depth, with the DLQ off) or `unreplayable`.

## A local write that fails is held, never dropped

A local write that fails in a way that can clear, a full or failing disk or staging that cannot be created, is never dead-lettered. The failed append is cut back to the file's last whole block, and the batch and its offsets are held as the write is retried, the waits doubling from 0.1 s to a 5 s ceiling. The loop receives nothing until the write lands, so nothing later is read or committed past it, and a disk that stays broken shows as lag and one warning.

As it waits, a file the write rolled uploads at once, and another destination's file closes at its roll interval and uploads, because nothing but the store taking files frees a full staging disk. Once shutdown begins, a write still failing gets 10 s more, then its batch is released `Errored`. A file that cannot be cut back, or cannot complete because its sync or staging failed, is given up, with its offsets `Errored`.

Nothing reads an `Errored` span again short of a restart or rebalance, and scalo's consumer has no seek back to the commit floor, so the loop ends with `Error::Withheld`. The process drains, exits non-zero, and the restart reads again from the committed offset. A source that cannot deliver the records again, `grpc` or `kafka` with acknowledgements off, let them go at receipt, so they are counted in `messages_dropped_total{reason="unreplayable"}` instead.

## Failures and what they cost

| Failure | Outcome | Records |
|---|---|---|
| Kill before the file is durable | The restart reads every record the file held again, and removes the staged files it left | Duplicates, up to one roll interval plus pending uploads |
| Upload fails | File and offsets kept, upload aborted and retried, intake paused near the caps | Delayed, never lost |
| Staged copy cannot be read back | File quarantined, offsets released `Errored`, the process exits and restarts | Duplicates |
| Store rejects the object for good, DLQ confirms | Records read back into the DLQ, offsets released | In the DLQ |
| Store rejects the object for good, DLQ off | Offsets released `Dropped`, counted, reason logged | Dropped |
| Local write fails in a way that can clear | Append cut back, batch and offsets held, write retried, intake held | Delayed, never lost |
| Local write still failing 10 s into shutdown | Offsets released `Errored`, the restart reads them again | Duplicates |
| Local file cannot complete or be cut back | Offsets released `Errored`, the process exits and restarts | Duplicates |
| Codec rejects a batch, DLQ confirms | Offsets released `Rejected` | In the DLQ |
| Refused for good, no DLQ backend can hold one record | That record's offset released `Dropped`, counted, reason logged, the rest to the DLQ | That record dropped |
| Refused for good, DLQ write fails | Offsets released `Errored`, the process exits and restarts | Duplicates |

## Shutdown and restart

The run loop stops first. The drain then closes the source, so the Push listener stops answering and the Kafka consumer stops fetching but can still commit, and receives until the source reports it is empty, so every push answered before the drain is written. A source that returns nothing for 5 s without reporting empty ends the drain. Buffers flush into their files, evicted writers finish closing, every open file completes, and the uploads get 20 s. A file still uploading then keeps its offsets unreleased and its staged copy on disk.

A completed staged file has a manifest beside it, `<name>.part.json`, with its object path and the length of each block. At startup the staging directory is settled before anything further is staged:

- On `kafka` with acknowledgements on, every staged file is removed, because its records are read again from the committed offset.
- On `grpc`, or with acknowledgements off, nothing delivers those records again. A file with a manifest that matches its size is uploaded. One never completed, or with an unreadable manifest, moves to `<buffer.spool_dir>/uploads/quarantine` for an operator.

Quarantine holds at most `buffer.quarantine_bytes_cap`, and a file past it is removed. Each outcome is counted in `staged_files_recovered_total`, `staged_files_quarantined_total{reason}` and `staged_files_removed_total{reason}`.

## Disk stays under the spool volume's limit

Archive files are all that sit on local disk: the open file of a local destination, and under `<buffer.spool_dir>/uploads` the staged copy of each object-store file until the store takes it, with the quarantine beside them.

1. At startup the archiver does not start with less than `buffer.min_free_spool_bytes` free on the staging volume, read once staging is settled. When it kept complete files to upload it starts anyway and warns, because nothing but a running archiver uploads them.
2. Staged bytes pause intake at the latch's `pause_above` share of `buffer.staged_bytes_cap`, which sits under the volume's `emptyDir` limit, past which kubelet evicts the pod.
3. Quarantine holds at most `buffer.quarantine_bytes_cap`, and a file past it is removed and counted.
4. A local write that finds the disk full is held and retried, never dropped, as uploads free the staging disk.

`staged_bytes` and `uploads_pending` show what is waiting for the store ([metrics.md](metrics.md)).
