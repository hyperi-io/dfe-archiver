# dfe-archiver

Writes a DFE deployment's streams to compressed files in a local path or object store.

[![Build Status](https://github.com/hyperi-io/dfe-archiver/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-archiver/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

## What it does

- Reads every Kafka topic discovery keeps, the landing topic where a source has both a landing and a transformed one, or takes a gRPC push on a deployment with no broker. Each record's payload is written as one line, exactly as it arrived.
- Writes to a local path, MinIO, S3, GCS or Azure Blob, in zstd (default), LZ4, Snappy or gzip frames, so the codec's standard tools read a file whole.
- Files each record under its topic and a JSON field (`_org_id` by default), or under its topic alone, and rolls a file at 1 GiB compressed or at the roll interval.
- On Kafka, commits an offset once the file holding its record is durable and never before, so a kill, a failed upload or a store outage costs duplicates or lag, never records, as long as the topic's retention holds them.
- Holds every destination's buffers inside a quarter of the memory limit, and pauses intake as staged uploads near the spool volume's cap, so an outage never fills memory or disk.

Kafka keys, offsets and timestamps are not kept. A replay gets the payloads back, not their Kafka metadata. What a file holds, where it lands and how to restore from one: [docs/archiving.md](docs/archiving.md).

## Quick start

On Linux the build needs `protoc`, a C toolchain, `pkg-config` and librdkafka. scalo compiles its gRPC `.proto` files, `aws-lc-sys` compiles C, and `rdkafka-sys` links librdkafka through pkg-config. It rejects a librdkafka older than the version after the `+` in its `Cargo.lock` entry, and a Debian or Ubuntu package can be older than that, so there [build librdkafka from source](https://github.com/confluentinc/librdkafka#build-from-source) first.

```bash
sudo apt-get install -y protobuf-compiler build-essential pkg-config
cargo build --release
./target/release/dfe-archiver --config config.dev.yaml
```

[config.dev.yaml](config.dev.yaml) reads `test-events` from the Redpanda in [docker-compose.dev.yaml](docker-compose.dev.yaml) on `localhost:19092`, writes under `./data/archive` and stages under `./data/spool`. The essentials can be set as environment variables instead:

```bash
env DFE_ARCHIVER_KAFKA_BROKERS=localhost:19092 DFE_ARCHIVER_KAFKA_TOPICS=test-events DFE_ARCHIVER_DESTINATION=file://./data/archive DFE_ARCHIVER_SPOOL_DIR=./data/spool ./target/release/dfe-archiver
```

Without a writable spool directory the archiver does not start, and the default, `/var/spool/dfe/archiver`, is one the image creates. Metrics and health answer on port 9090 (`/metrics`, `/livez`, `/readyz`), and `curl -s localhost:9090/readyz` prints `{"status":"ready"}` once the pipeline runs. With no destination set the archiver starts, passes readiness and idles until a config change names one.

## Configuration

Settings load through a cascade: `DFE_ARCHIVER_*` environment variables, then `.env`, then the config file, then defaults. `--config` names the file, and the other flags set logging and the metrics address ([docs/configuration.md](docs/configuration.md#scalo-runtime-variables)). The pipeline reads its config at startup, so a change to any section below but `kafka.batch_size` takes a restart, and a reload logs the sections that need one. The archiver's keys take a single underscore (`DFE_ARCHIVER_KAFKA_BROKERS`), and the scalo runtime sections (`version_check`, `metrics`, `logger`, `self_regulation`, `scaling`) take two (`DFE_ARCHIVER_VERSION_CHECK__ENABLED`).

| Key | What it sets | Default |
|-----|--------------|---------|
| `transport` | `kafka` (a consumer group) or `grpc` (the Push listener on `0.0.0.0:6000`) | `kafka` |
| `kafka.brokers`, `kafka.topics` | Brokers, and the topics to read. Empty topics discover every topic, narrowed by `topic_include` and `topic_exclude` | `localhost:9092`, discover |
| `archive.destination` | `file:///path`, `s3://bucket/prefix`, `minio://bucket/prefix`, `gs://bucket/prefix` or `az://container/prefix` | none, idles |
| `compression.codec` | `zstd`, `lz4`, `snappy`, `gzip` or `none` | `zstd` |
| `archive.roll_size_bytes`, `archive.roll_interval_secs` | When a file closes | 1 GiB, and 300 s when Kafka offsets are held or on `grpc`, else 3600 |
| `buffer.spool_dir` and its three caps | Where staged uploads wait, and how much disk they may take | `/var/spool/dfe/archiver`, and 80%, 10% and 10% of the volume's declared size limit |

Each store takes a block (`archive.s3`, `archive.minio`, `archive.gcs`, `archive.azure`) for the endpoint and credentials. [docs/config-schema.yaml](docs/config-schema.yaml) lists every archiver key with its type and default, and [docs/configuration.md](docs/configuration.md) the cascade and every environment variable.

## Storage Backends

Each destination is one URL plus its store's block. These four load and validate as written, and a test keeps them that way. The file is read as written, so `${VAR}` in it stays a literal string. Set store keys in the environment instead: `DFE_ARCHIVER_S3_ACCESS_KEY_ID` and `DFE_ARCHIVER_S3_SECRET_ACCESS_KEY` for S3, `AZURE_STORAGE_ACCOUNT_KEY` for Azure, or each SDK's credential chain ([docs/internals.md](docs/internals.md#storage-backends)).

```yaml
archive:
  destination: file:///var/data/archive
```

```yaml
archive:
  destination: s3://bucket-name/prefix
  s3:
    endpoint: http://minio:9000
    region: us-east-1
    allow_http: true
```

```yaml
archive:
  destination: gs://bucket-name/prefix
  gcs:
    credentials_path: /path/to/key.json
```

```yaml
archive:
  destination: az://container-name/prefix
  azure:
    account_name: myaccount
```

## Documentation

- [docs/archiving.md](docs/archiving.md): what a file holds, where it goes, how long it stays and how to restore.
- [docs/internals.md](docs/internals.md): the pipeline, buffers and writers, rolling, routing, compression and the storage backends.
- [docs/delivery.md](docs/delivery.md): at-least-once delivery, the DLQ, shutdown, restart and disk protection.
- [docs/configuration.md](docs/configuration.md): the config cascade and every environment variable.
- [docs/metrics.md](docs/metrics.md): every metric the archiver records itself.
- [docs/architecture.md](docs/architecture.md): the parts, how a record flows through them, and why it is built this way.
- [docs/design.md](docs/design.md): how it is built and released, and the decisions behind it.
- [docs/testing.md](docs/testing.md): the test tiers and the containers they start.

The suite's published docs are at <https://dfe.docs.hyperi.io>, and this repository's pages at <https://dfe.docs.hyperi.io/dfe-archiver/>.

## Help and contributing

How to work in this repo, the commands that test a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in an issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-archiver/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, see [LICENSE](LICENSE).
