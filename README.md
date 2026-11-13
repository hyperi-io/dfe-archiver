# dfe-archiver

Writes a DFE deployment's streams to compressed files in a local path or object store.

[![Build Status](https://github.com/hyperi-io/dfe-archiver/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-archiver/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

## What it does

- Reads the same Kafka topics dfe-loader reads, or takes a gRPC push on a deployment with no broker, and writes each record's payload as one line, exactly as it arrived.
- Writes to a local path, MinIO, S3, GCS or Azure Blob, in zstd (default), LZ4, Snappy or gzip frames, so the codec's own tools read a file whole.
- Routes files by a JSON field (`_org_id` by default) or by topic, and rolls a file at 1 GiB compressed or at the roll interval.
- On Kafka, commits an offset only once the file holding its record is durable, so a kill, a failed upload or a store outage costs duplicates or lag, never records.
- Holds every destination's buffers inside a quarter of the memory limit, and pauses intake as staged uploads near the spool volume's cap, so an outage never fills memory or disk.

Kafka keys, offsets and timestamps are not kept. A replay gets the payloads back, not their Kafka metadata. What a file holds, where it lands and how to restore from one: [docs/archiving.md](docs/archiving.md).

## Quick start

```bash
cargo build --release
./target/release/dfe-archiver --config config.dev.yaml
```

[config.dev.yaml](config.dev.yaml) points at a local Kafka on `localhost:19092` and writes under `./data/archive`. The same settings go in as environment variables:

```bash
env DFE_ARCHIVER_KAFKA_BROKERS=localhost:9092 DFE_ARCHIVER_KAFKA_TOPICS=events DFE_ARCHIVER_DESTINATION=file://./data/archive ./target/release/dfe-archiver
```

Metrics and health answer on port 9090 (`/metrics`, `/livez`, `/readyz`). With no destination set the archiver starts, passes readiness and idles until a config change names one.

## Configuration

Settings load through a cascade: CLI, then `DFE_ARCHIVER_*` environment variables, then `.env`, then the config file, then defaults. The pipeline snapshots its config at startup, so a change to any section below takes a restart, and a reload logs which sections changed. The archiver's own keys take a single underscore (`DFE_ARCHIVER_KAFKA_BROKERS`), and the scalo runtime sections (`version_check`, `metrics`, `logger`, `self_regulation`, `scaling`) take two (`DFE_ARCHIVER_VERSION_CHECK__ENABLED`).

| Key | What it sets | Default |
|-----|--------------|---------|
| `transport` | `kafka` (a consumer group) or `grpc` (the Push listener on `0.0.0.0:6000`) | `kafka` |
| `kafka.brokers`, `kafka.topics` | Brokers, and the topics to read. Empty topics discover every topic, narrowed by `topic_include` and `topic_exclude` | `localhost:9092`, discover |
| `archive.destination` | `file:///path`, `s3://bucket/prefix`, `gs://bucket/prefix` or `az://container/prefix` | none, idles |
| `compression.codec` | `zstd`, `lz4`, `snappy`, `gzip` or `none` | `zstd` |
| `archive.roll_size_bytes`, `archive.roll_interval_secs` | When a file closes | 1 GiB, 300 s while Kafka offsets are held, else 3600 |
| `buffer.spool_dir` and its three caps | Where staged uploads wait, and how much disk they may take | 8/9, 1/9 and 1/9 of the volume's declared limit |

Each store takes its own block (`archive.s3`, `archive.gcs`, `archive.azure`) for the endpoint and credentials. [docs/config-schema.yaml](docs/config-schema.yaml) lists every key with its type and default, and [docs/internals.md](docs/internals.md) explains the delivery guarantee, the rolling policy, routing, disk protection and the metrics.

## Documentation

- [docs/archiving.md](docs/archiving.md): what a file holds, where it goes, how long it stays and how to restore.
- [docs/internals.md](docs/internals.md): the pipeline, the tiered buffers, at-least-once delivery, disk protection, every metric.
- [docs/architecture.md](docs/architecture.md): the three crates (`core`, `io`, `archiver`), the one-way rules between them and the build graph.
- [docs/design.md](docs/design.md): the decisions behind the shape.
- [docs/testing.md](docs/testing.md): the test tiers and the object-store containers they start.

The suite's published docs are at <https://dfe.docs.hyperi.io>.

## Help and contributing

How to work in this repo, the commands that prove a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in a new issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-archiver/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
