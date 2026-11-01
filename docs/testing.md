<!--
  Project:      dfe-archiver
  File:         docs/testing.md
  Purpose:      Which tests run where, and the containers they start
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Testing

The general rules are hyperi-ai's testing standard (`standards/universal/testing.md` in hyperi-ai). This page covers what dfe-archiver does on top of them.

## Levels

| Level | Runs | Command |
|---|---|---|
| core | locally, every push and PR | `cargo nextest run` |
| full | nightly (`schedule` in `.github/workflows/ci.yml`) and before every release (`test.full.required_for_release: true`) | `cargo nextest run --run-ignored all` |

Only nextest runs the suite (`test.rust.nextest: true`). The metric tests read a process-global recorder, so under `cargo test`, where every test shares one process, they fail.

Core is everything not `#[ignore]`d: the unit tests in `crates/`, `tests/smoke.rs`, `tests/integration/`, and the e2e tests in `tests/e2e/` against containers each test starts. That covers at-least-once delivery against Kafka and an S3 store, the disk-full writes (capped with `prlimit`, util-linux), Azure on Azurite, S3 on LocalStack, the GCS reads on fake-gcs-server, and the Kafka consumer over TLS. Docker is required: a missing daemon fails the run in CI and skips with the reason locally.

Full adds the ignored tests, and in CI it skips them all again (`test.full.rust.skip` in `.hyperi-ci.yaml`) because each needs something a runner does not have:

- the GCS write tests need a live bucket and credentials, because fake-gcs-server lacks the XML multipart API object_store uploads with
- the `minio::` tests need the developer's compose stack (`docker compose -f docker-compose.dev.yaml up -d s3`)
- `kafka::test_kafka_*` and `pipeline::test_e2e_kafka_to_*` need a broker named by `KAFKA_BROKERS`, or `TEST_MODE=docker` with a dfe-docker stack

Run them by hand with those in place.

There is no shared service container. Each e2e test starts the containers it needs, named `dfe-archiver-test-integration-<test>-<service>`, labelled `io.hyperi.test.suite=dfe-archiver-integration` with the owning process, and removed when the test drops them. A leftover from a killed run is removed by the next test that needs the name (`reap_stale`).

| Service | Image |
|---|---|
| Kafka | `apache/kafka:4.3.1@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` |
| S3 store | `localstack/localstack:4.14` |
| Azure Blob | `mcr.microsoft.com/azure-storage/azurite:3.37.0` |
| GCS | `fsouza/fake-gcs-server:1.56.1` |
| Kafka over TLS | scalo's `TlsBroker` on the same Kafka image, with certificates made in `alpine/openssl:3.5.9@sha256:1c361a204e9791bec6202bccbd1065eafaa65cb5a1a9a06ae000238569fba6a2` |

The pins live in `scripts/pull-test-images.sh`. nextest runs it as a setup script before the e2e binary, so no test spends its timeout on a pull, and in CI a fixture fails when its image was not pulled first. The same steps run locally and in CI, against whatever daemon `DOCKER_HOST` or the current docker context names.
