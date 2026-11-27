<!--
  Project:      dfe-archiver
  File:         docs/architecture.md
  Purpose:      The archiver's parts, how a record flows through them, and why
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# dfe-archiver architecture

dfe-archiver is the sink at the end of the DFE pipeline: it writes every record it is given into compressed, rolling files on a local path or in a bucket. It keeps each record exactly as it arrived, so a deployment can restore or replay it.

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
  accTitle: L3 Components -- dfe-archiver
  accDescr: Records arrive from Kafka topics or are pushed by the previous stage. Inside, they are received, routed and buffered, then written to files and delivered. Files land in a local path or a bucket, and records refused for good go to the DLQ topic when the DLQ is on.
  topics[("<b>Kafka topics</b><br/><span>the records to archive</span>")]
  stage("<b>Previous stage</b><br/><span>pushes on the grpc transport</span>")
  subgraph archiver["dfe-archiver"]
    direction TB
    subgraph intake["Intake"]
      direction LR
      receive("<b>Receive</b><br/><span>a batch a call</span>")
      route("<b>Route</b><br/><span>picks the destination</span>")
      buffer("<b>Buffer</b><br/><span>memory, per destination</span>")
      receive --> route --> buffer
    end
    subgraph output["Output"]
      direction LR
      write("<b>Write</b><br/><span>compress and roll the file</span>")
      delivery[["<b>Delivery</b><br/><span>sync or upload, then release</span>"]]
      write --> delivery
    end
    intake --> output
  end
  store[("<b>Local path or bucket</b><br/><span>the archive files</span>")]
  dlq[("<b>DLQ topic</b><br/><span>when the DLQ is on</span>")]
  topics -->|records| archiver
  stage -->|pushes| archiver
  archiver -->|files| store
  archiver -.->|refused| dlq
  class topics,dlq bus
  class stage ext
  class receive,route,buffer,write,delivery part
  class store store
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
  style archiver fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style intake fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style output fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L3 Components -- dfe-archiver. The grey dashed box is external and the dotted arrow takes refused records to the DLQ when it is on.* Up: top of dfe-archiver. Down: [L4 Detail -- Delivery](delivery.md).

## How data flows

1. Receive takes up to `kafka.batch_size` records a call, 10,000 by default, from a Kafka consumer group or the scalo Push listener. Nothing after it knows which transport delivered them.
2. Route picks each record's destination: its topic, then the value of each routing field, `_org_id` by default.
3. Buffer holds records in memory per destination and flushes by size, count or age.
4. Write compresses each flush into that destination's open file. A file rolls at 1 GiB compressed or at the roll interval.
5. Delivery syncs a closed local file, or uploads a staged one, and only then releases its records. On `kafka` that release is the offset commit.

A record refused for good, by the store, the codec or the nesting check, goes to the DLQ topic when the DLQ is on.

## Components

| Part | Role | Page |
|---|---|---|
| Receive | The Kafka consumer or the Push listener, picked by `transport` | [Two transports, one pipeline](internals.md#two-transports-one-pipeline) |
| Route | Topic, then routing fields, each segment encoded so it never names another directory | [Routing](internals.md#routing) |
| Buffer | At most 64 hot buffers, a quarter of the memory limit between them | [Buffers and writers](internals.md#buffers-and-writers) |
| Write | One open file per destination, compressed frames, rolled by size or age | [Files roll by compressed size or age](internals.md#files-roll-by-compressed-size-or-age) |
| Delivery | Sync or upload, then release. The DLQ, shutdown and disk limits | [delivery.md](delivery.md) |

## External dependencies

| Dependency | Needed for | Swappable |
|---|---|---|
| Kafka | The `kafka` transport and the DLQ | Any Kafka, or none on `grpc` |
| A previous stage | The `grpc` transport, pushing to port 6000 | dfe-receiver or any scalo Push sender |
| A local path or object store | The archive files | `file://`, S3, MinIO, GCS or Azure Blob |
| A spool volume | Staged uploads and quarantine | Any writable volume. A size limit sets the caps |

## Deployment variants

The image is the same everywhere. What changes is the transport and the destination.

- On Kubernetes the thin Helm chart deploys it, generated from the deployment contract, and dfe-infra pins the chart and image in `versions.yaml`. The spool is an `emptyDir` with a size limit, which sets the staging caps.
- On Docker, or a bare host, the spool caps default to 8, 1 and 1 GiB.
- `transport: kafka` is the default and gives at-least-once delivery. `transport: grpc` lets a deployment with no broker archive, at-most-once, with no DLQ.
- With no `archive.destination` it starts, reports Ready and idles until a config reload names one.

## Design considerations

| Aspect | How it is handled |
|---|---|
| Security | Every path segment is encoded, so a field value never names another directory, and every backend rejects an absolute path or a `..` segment. Store credentials come from each SDK's own chain or from a Secret as environment variables |
| Reliability | On `kafka` no record is lost while the topic holds it. A kill or store outage costs duplicates or lag. A failed upload keeps its file and offsets and retries with backoff |
| Operations | `/livez`, `/readyz`, `/metrics` and `/scaling/pressure` on port 9090. A config change or SIGHUP reloads `kafka.batch_size` and starts an idle archiver, and any other change logs that it needs a restart |
| Performance | Upload memory stays near 64 MiB whatever the file size. Held offsets cost about 32 bytes each, which is why the roll interval drops to 300 s when they are held |

## Why it's built this way

| Decision | Reason |
|---|---|
| Release on a durable file, not on write | A file is an upload in progress until it rolls, and a kill abandons it. Committing at write would lose what the open file held |
| Stage, then upload whole | A multipart upload read from the staged copy signs its requests afresh on every attempt, however long the store was down, and the loop never waits on the store |
| Pressure pauses intake | Held records and staged bytes feed the self-regulation latch, so a store outage turns into lag rather than memory or disk growth |
| A unique writer id in every key | A staged file is never checked against the store. Without it two replicas could pick one key and the later upload would replace the earlier object |
| `_land` over `_load` on discovery | The archive keeps the record as it arrived. dfe-loader keeps `_load` |

## Diagram map

- Figure 1. L3 Components -- dfe-archiver (this page)
  - [L4 Detail -- Delivery](delivery.md)
- [L4 Detail -- Crate dependencies](../CONTRIBUTING.md#where-things-live)
- [L4 Detail -- CI pipeline](design.md)

## What's next

- [How the archiver works](internals.md): every part in detail
- [Archiving](archiving.md): what a file holds, where it goes and how to restore
- [Configuration reference](configuration.md) and [Metrics reference](metrics.md)
- [CONTRIBUTING.md Context](../CONTRIBUTING.md#context): the crate map and the rules between the crates
