<!--
  Project:      dfe-archiver
  File:         docs/testing.md
  Purpose:      Which tests run where, and the containers they start
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Testing

Which of dfe-archiver's tests run where, what they need, and the containers they start.

## Levels

| Level | Runs | Command |
|---|---|---|
| core | locally, on every pull request, and on every push that touches more than `docs/**` and Markdown | `cargo nextest run` |
| full | nightly (`schedule` in `.github/workflows/ci.yml`) and before every release (`test.full.required_for_release: true`) | `cargo nextest run --run-ignored all` |

Only nextest runs the suite (`test.rust.nextest: true`). The metric tests read a process-global recorder, so under `cargo test`, where every test in a binary shares one process, they fail.

Core is everything not `#[ignore]`d: the unit tests under `crates/`, and in `crates/archiver/tests/` the smoke tests (`smoke.rs`), the integration tests (`integration/`) and the e2e tests (`e2e/`) against containers each test starts. That covers at-least-once delivery against Kafka and an S3 store, the disk-full writes (capped with `prlimit`, from util-linux), Azure on Azurite, S3 on LocalStack, the GCS reads on fake-gcs-server, and the Kafka consumer over TLS. Docker is required: a missing daemon fails the run in CI and skips with the reason locally.

Full adds the ignored tests, and in CI it skips them all again (`test.full.rust.skip` in `.hyperi-ci.yaml`) because each needs something a runner does not have:

- the GCS write tests need a live bucket and credentials (`GCS_BUCKET` with `GCS_SERVICE_ACCOUNT_KEY` or `GOOGLE_APPLICATION_CREDENTIALS`), because fake-gcs-server lacks the XML multipart API `object_store` uploads with
- the `minio::` tests need the developer's compose stack (`docker compose -f docker-compose.dev.yaml up -d s3`)
- `kafka::test_kafka_*` and `pipeline::test_e2e_kafka_to_*` need a broker: `DFE_ARCHIVER_KAFKA_BROKERS` and its SASL settings from the environment or the workspace `.env`, or with `TEST_MODE=docker` one on `localhost:19092`, which the compose stack's Redpanda serves

Run them by hand with those in place. A Kafka test with no broker it can reach skips and says so.

There is no shared service container. Each e2e test starts the containers it needs, named `dfe-archiver-test-integration-<test>-<service>`, labelled `io.hyperi.test.suite=dfe-archiver-integration` with the owning process, and removed when the test drops them. A dead leftover from a killed run is removed by the next test that needs the name (`reap_stale`), and a running one is left alone.

| Service | Image |
|---|---|
| Kafka | `apache/kafka` |
| S3 store | `localstack/localstack` |
| Azure Blob | `mcr.microsoft.com/azure-storage/azurite` |
| GCS | `fsouza/fake-gcs-server` |
| Kafka over TLS | scalo's `TlsBroker` on the same Kafka image, with certificates made in `alpine/openssl` |

The tags, and the digests for Kafka and OpenSSL, live in `scripts/pull-test-images.sh`. nextest runs it as a setup script before the e2e binary, so no test spends its timeout on a pull, and in CI a fixture fails when its image was not pulled first.

The pull script and the harness's own `docker` calls use the docker CLI, which follows `DOCKER_HOST` or the current docker context. testcontainers starts the containers on `DOCKER_HOST`, or on the local socket when it is unset, and never reads the docker context. So to run the suite against another daemon, set `DOCKER_HOST`.
