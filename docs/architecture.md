<!--
  Project:      dfe-archiver
  File:         docs/architecture.md
  Purpose:      Crate map, the one-way rules between them, and the build graph
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# dfe-archiver architecture

Where things live and which way the dependencies point. The design itself -- the pipeline, the rolling model, the storage backends -- is [internals.md](internals.md), and the delivery guarantee [delivery.md](delivery.md).

The archiver crate depends on both libraries, and io depends on core alone.

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
flowchart LR
  accTitle: L4 Detail -- Crate dependencies
  accDescr: The archiver crate depends on both library crates, io and core. The io crate depends on core alone.
  archiver("<b>archiver</b><br/><span>the binary and the pipeline</span>")
  subgraph libs["Libraries"]
    direction LR
    io("<b>io</b><br/><span>transports and storage backends</span>")
    core("<b>core</b><br/><span>archive logic, no other crate</span>")
    io --> core
  end
  archiver --> libs
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
  class archiver,io,core part
```

*Figure 1. L4 Detail -- Crate dependencies. An arrow points at what a crate depends on, so archiver depends on io and core alike.* Up: [dfe-archiver documentation](README.md).

| Crate | Package | What it holds |
|---|---|---|
| `crates/core` | `dfe-archiver-core` | The archive logic: config types and the flat environment names (`config.rs`), the rolling archive writer (`archive/`), the hot buffers (`buffer/`), destination routing (`routing/`), the compression codecs (`compression/`), and the storage trait with the sink probe (`storage.rs`). The one I/O it does is creating the spool directory |
| `crates/io` | `dfe-archiver-io` | Everything that talks to something else: the inbound transports, Kafka (`kafka.rs`) or the Push listener (`grpc.rs`), chosen by `transport.rs`, and the staging directory and the backends behind `create_backend` (`storage.rs`): file, S3, GCS, Azure Blob and MinIO |
| `crates/archiver` | `dfe-archiver` | The binary and the pipeline that joins the other two. `archiver.rs` is the loop (receive, route, buffer, write, hand closed object-store files to background upload tasks, and release once a file is durable), `main.rs` the scalo `ServiceApp` wiring, CLI and config reloader, `config/loader.rs` the file loading and `validate_config`, `metrics.rs` the Prometheus surface, and `contract.rs` the deployment contract the checked-in `Dockerfile` and the Helm chart are generated from |

The rules between the crates, which nothing may break:

- `core` depends on no other workspace crate.
- Nothing in `core` records a metric: it counts what it did and the caller records it, which is why `ArchiveWriter::take_files_opened` exists.
- Releasing an offset and uploading a file are never `core`'s. The writer settles the offsets held on a file as it closes, or hands them to the file's pending upload, and the caller drains both with `ArchiveWriter::take_settled`, uploads, and releases through the transport.
- `io` depends on `core` and never on `archiver`.
- No crate but `archiver` has a binary or reads the deployment contract, so the Dockerfile and the chart have one source.
- No crate is publishable: all three set `publish = false`, and their version
  is inherited from `[workspace.package]` so the release stamp reaches every
  one of them.

The cold build is spent on dependencies: `object_store` with its aws, gcp and azure features, `aws-lc-sys`, which compiles C, and `rdkafka-sys` through scalo, which on Linux finds librdkafka through pkg-config (its `dynamic-linking` feature). The release build points pkg-config at a static-only prefix, so the shipped binary links librdkafka statically and needs libc, libm, libgcc_s and the dynamic loader alone at run time.

Change propagation follows the arrows: a `core` edit rebuilds all three, an `io`
edit rebuilds `io` and `archiver`, and an `archiver` edit rebuilds itself alone.
