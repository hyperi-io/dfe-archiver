<!--
  Project:      dfe-archiver
  File:         docs/design.md
  Purpose:      Build, release and design decisions for contributors
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# DFE Archiver build and design notes

How the archiver is built and released, and the design decisions behind it. The operator-facing design is [internals.md](internals.md).

The build needs the tools in the README's [quick start](../README.md#quick-start). `cargo build` writes a debug binary to `target/debug/dfe-archiver`, and `cargo build --release --features jemalloc` a release one, with the allocator CI uses, to `target/release/dfe-archiver`. jemalloc is off by default, so a plain `cargo build` uses the system allocator.

CI builds each target on a runner of the same architecture, never cross-compiling, with `--target`, so its binaries land under `target/x86_64-unknown-linux-gnu/release/` and `target/aarch64-unknown-linux-gnu/release/`. `rdkafka` takes its `dynamic-linking` feature, which finds librdkafka through pkg-config, and the release build points pkg-config at a static-only prefix (scalo-rs `docs/deployment/native-deps.md`), so the shipped binary links librdkafka statically.

```toml
[profile.release]
lto = "thin"
codegen-units = 1
strip = true
panic = "abort"
opt-level = 3
```

That is the committed profile. hyperi-ci switches LTO to `fat` on the beta and release channels, and a full release adds PGO, driven by `scripts/pgo-workload.sh`, and BOLT.

`.github/workflows/ci.yml` calls hyperi-ci's reusable `rust-ci.yml` workflow, configured by `.hyperi-ci.yaml`.

Quality and test both gate the build, and the build feeds the release tail and the gate.

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
    nodeBorder: "#334155"
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
    .icon-shape path[stroke-width="0"][fill="none"]{fill:#FFFFFF;stroke:#FFFFFF;stroke-width:12px;stroke-linejoin:round}
    .icon-shape p{background:transparent!important;color:#0F172A}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
  accTitle: L4 Detail -- CI pipeline
  accDescr: Every trigger starts the plan, and a push to main, a pull request or the merge queue starts the commit message check. The plan starts quality and test, both feed the build, and the build feeds the release tail on a release and the gate on every run.
  trigger(["<b>Push, PR,</b><br/><span>schedule or dispatch</span>"])
  commits("<b>Commit messages</b><br/><span>checked on landing</span>")
  subgraph checks["Plan and check"]
    direction LR
    plan("<b>Plan</b><br/><span>predicts the version and jobs</span>")
    quality("<b>Quality</b><br/><span>fmt, clippy, audit, deny</span>")
    test("<b>Test</b><br/><span>the test tier, with coverage</span>")
    plan --> quality
    plan --> test
  end
  subgraph out["Build and report"]
    direction LR
    build("<b>Build</b><br/><span>x86_64 and aarch64</span>")
    tail("<b>Release tail</b><br/><span>tag, release, image, binaries, chart</span>")
    gate("<b>Gate</b><br/><span>plan, quality, test and build</span>")
    build -->|release| tail
    build -->|always| gate
  end
  trigger -->|landing| commits
  trigger -->|every run| checks
  checks -->|quality and test| out
  class trigger ext
  class plan,commits,quality,test,build,tail,gate part
  class trigger ext
  class plan,commits,quality,test,build,tail,gate part
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
  style checks fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style out fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- CI pipeline. The gate checks plan, quality, test and build, and the commit check and release tail report separately.* Up: [dfe-archiver documentation](README.md).

The commit check runs on landing: a push to main, a pull request or a merge queue group.

| Job | What it does |
|---|---|
| Plan | Predicts the version and which jobs run |
| Quality | fmt, clippy, audit and deny |
| Test | The test tier, with coverage |
| Build | x86_64 and aarch64, each on a runner of that arch, with PGO then BOLT on a full release |
| Release tail | The tag, the GitHub release, the ghcr image, the binaries to R2 and the Helm chart |

A schedule run tests and publishes nothing. A release needs a `Release: true` trailer on the commit pushed to `main`, or a dispatch, as [CONTRIBUTING.md](../CONTRIBUTING.md#cicd-workflow) says. No crate is published, because all three set `publish = false`.

| Artefact | Where it goes |
|---|---|
| Container image | `ghcr.io/hyperi-io/dfe-archiver`, for `linux/amd64` and `linux/arm64` |
| Binaries | hyperi-ci's `release.destinations.binaries`, Cloudflare R2 (`downloads.hyperi.io`) by default, under `dfe-archiver/v<version>/` and `dfe-archiver/latest/` |
| Helm chart | the thin chart hyperi-ci assembles from the deployment contract on the scalo-service library, pushed to `release.helm.registry`, `oci://ghcr.io/hyperi-io/charts` by default |
| Release notes | `https://github.com/hyperi-io/dfe-archiver/releases/`, with no binaries attached |

The binaries are `dfe-archiver-linux-amd64` and `dfe-archiver-linux-arm64`. Each has a `<binary>.sha256` beside it, in the format `sha256sum -c` reads.

The design decisions behind it:

| Decision | Why | Rejected |
|---|---|---|
| scalo for config, logging, metrics, the transports, the DLQ and scaling | The same patterns as the other DFE apps, and less code here | Direct rdkafka, a bespoke config system |
| zstd at level 3 by default, with LZ4, Snappy and gzip | zstd gives the best ratio at an acceptable speed, LZ4 suits speed-critical work and gzip compatibility | brotli and lzma, both too slow |
| Roll on compressed file size, 1 GiB by default | Inbound size does not predict the size a file reaches in the store | Inbound size, record count |
| Hot buffers in memory under a ceiling per destination and a cap on all of them, then the writers, with nothing spilled to disk ahead of them | Memory and open files stay bounded however many destinations routing produces | One large shared buffer, an unbounded file per destination |
| Hold each Kafka offset on the file its record is in, and release it when the file is durable | At-least-once with no transactional writes, at the cost of duplicates after a restart ([delivery.md](delivery.md)) | Exactly-once, which needs transactional writes |
