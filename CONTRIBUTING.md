# Contributing

We welcome contributions to this project. By contributing, you agree to the
terms outlined below.

## Commit Message Format

HyperI projects use [Conventional Commits](https://www.conventionalcommits.org/)
and [semantic-release](https://semantic-release.gitbook.io/) for automated
versioning and changelog generation. All commits must follow this format:

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

### Types

| Type | Description | Version Bump |
|------|-------------|--------------|
| `feat` | A new feature | Minor (0.X.0) |
| `fix` | A bug fix | Patch (0.0.X) |
| `docs` | Documentation only | None |
| `style` | Code style (formatting, semicolons, etc.) | None |
| `refactor` | Code change that neither fixes a bug nor adds a feature | None |
| `perf` | Performance improvement | Patch (0.0.X) |
| `test` | Adding or correcting tests | None |
| `build` | Changes to build system or dependencies | None |
| `ci` | Changes to CI configuration | None |
| `chore` | Other changes that don't modify src or test files | None |
| `revert` | Reverts a previous commit | Patch (0.0.X) |

### Breaking Changes

For breaking changes that require a major version bump, add `!` after the type
or include `BREAKING CHANGE:` in the footer:

```text
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/users endpoint has been removed. Use /v2/users instead.
```

### Examples

```text
feat(auth): add OAuth2 support for Google login

fix(api): handle null response from upstream service

docs: update installation instructions for Windows

refactor(core)!: restructure module exports

BREAKING CHANGE: Named exports are now used instead of default exports.
```

### Scope

Scope is optional but recommended. Use it to indicate the area of the codebase
affected (e.g., `api`, `auth`, `core`, `cli`, `docs`).

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/):

- MAJOR (X.0.0): breaking changes that require users to modify their code
- MINOR (0.X.0): new features that are backwards-compatible
- PATCH (0.0.X): bug fixes and minor improvements

Versions are automatically determined by semantic-release based on commit
messages. Do not manually update version numbers.

## Developer Certificate of Origin

This project uses the [Developer Certificate of Origin 1.1](https://developercertificate.org/). A `Signed-off-by` line on a commit certifies, for that contribution, the four statements in that text.

### How to Sign Off Your Commits

You must sign off each commit to indicate your acceptance of the DCO. Combine
the signoff with your conventional commit message:

```bash
git commit --signoff -m "feat(auth): add two-factor authentication"
```

This produces:

```text
feat(auth): add two-factor authentication

Signed-off-by: Your Name <your.email@example.com>
```

Make sure your Git configuration has your correct name and email:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## License for Contributions

All contributions to this project are licensed under the Business Source
License 1.1 (BUSL-1.1), the same license that covers the project.

Each version of the software (including your contributions) will automatically
become available under the Apache License, Version 2.0 on the third
anniversary of its release.

## How to Contribute

1. Fork the repository and create your branch from `main`
2. Make your changes following the commit message format above
3. Sign off your commits with the DCO
4. Run the tests that cover your change
5. Submit a pull request with a clear description of what you've done

### Pull Request Checklist

- [ ] Commits follow the conventional commit format
- [ ] All commits are signed off (DCO)
- [ ] Tests pass (if applicable)
- [ ] Documentation is updated (if applicable)

## Run the checks locally before you push

HyperI projects gate every push through CI (lint, format, tests, secret scan,
dependency audit, build). Run the SAME checks locally first so your change lands
green instead of bouncing:

```bash
hyperi-ci check
```

`hyperi-ci` is the HyperI CI CLI (public on PyPI). If you do not have it, install
it once with `uv tool install hyperi-ci` (or `pipx install hyperi-ci`), or run it
ad hoc with `uvx hyperi-ci check`. It runs the project's full local validation --
the same suite CI runs -- and reports what to fix. Run it before every push and before opening a
pull request. It is the single best way to give your change the best chance of
surviving CI.

## CI/CD Workflow

A merge to `main` runs CI and releases nothing by itself. A release is cut when the squash-merge commit ends with a `Release: true` trailer, or when a maintainer dispatches the CI workflow with `from-head: true`. semantic-release then reads the commit messages since the last release and picks the next version, the release commit updates `CHANGELOG.md` and `VERSION`, and hyperi-ci builds and publishes the release.

## Context

### What this is

dfe-archiver is the sink at the end of the DFE pipeline. Records arrive from Kafka or the scalo Push gRPC listener, are buffered per destination, compressed, and written as rolling files to `file`, `s3`, `gs`, `az` or MinIO. It parses a record only to read its routing field, enriches nothing, routes between no topics and answers no queries. Not a library either -- all three crates set `publish = false`, and a release ships a container image on ghcr, Linux binaries on downloads.hyperi.io and a thin Helm chart. With nothing configured it starts and idles rather than crash-looping -- see [idle until configured](docs/internals.md#idle-until-configured).

### Where things live

| Path | What it holds |
|------|---------------|
| `crates/core/` | Config types, the rolling writer, the hot buffers, routing, codecs, the storage trait. It records no metrics, and its only I/O is creating the spool directory. |
| `crates/io/` | Kafka and gRPC transports, staging, and the object-store backends behind `create_backend`. Its dependencies, `object_store` and librdkafka through scalo, are the cold-build bottleneck. |
| `crates/archiver/` | The binary. `archiver.rs` the pipeline loop, `main.rs` the scalo `ServiceApp` wiring, `metrics.rs` the Prometheus surface, `contract.rs` the deployment contract, `writers.rs` the writer cache, `spool_limits.rs` the spool caps. |
| `crates/archiver/src/config/loader.rs` | `load_config` reads the file and the flat `DFE_ARCHIVER_*` names, and `validate_config` refuses structural faults. Its `init_cascade` takes effect only where nothing set scalo's cascade up first, and in the binary `run_app` already has, from `--config`. |
| `crates/archiver/src/bin/pgo_driver.rs` | The second `[[bin]]`, gated on `required-features = ["pgo-driver"]`. The gate keeps it out of the release payload: hyperi-ci skips feature-gated binaries when packaging, and verifies BOLT on the first packaged binary only. |
| `Dockerfile` | Generated from `contract.rs`. Its header names the generator and schema version. |
| `docs/architecture.md` | Crate map and the one-way rules. `docs/internals.md` is the pipeline design, `docs/delivery.md` the delivery guarantee. |
| `.hyperi-ci.yaml` | The PGO workload's setup command, the static-libs build, the full tier's skip list, and which gates block versus warn. `.config/nextest.toml` and `scripts/pgo-workload.sh` are what it drives. |

### Commands that prove a change

| Command | What it covers |
|---------|----------------|
| `hyperi-ci check` | The pre-push gate. `make check` is the same. |
| `cargo nextest run` | Unit, integration and container e2e. Those are not `#[ignore]`d, so it starts Kafka, LocalStack, Azurite and fake-gcs-server via testcontainers and needs a Docker daemon. |
| `TEST_MODE=docker cargo nextest run --run-ignored all` | Adds the Kafka and `minio://` tests, against the Redpanda and S3 store `docker compose -f docker-compose.dev.yaml up -d` starts. Without `TEST_MODE=docker` the Kafka tests read `DFE_ARCHIVER_KAFKA_BROKERS` and its SASL settings from the environment or `.env`. The GCS write tests need a real bucket: `GCS_BUCKET` with `GCS_SERVICE_ACCOUNT_KEY` or `GOOGLE_APPLICATION_CREDENTIALS`. |
| `cargo deny check advisories` | Advisories and yanked crates. `quality.rust.audit` and `quality.rust.deny` are `blocking`, so a red advisory fails `hyperi-ci check` and CI too. |

Green says less than it looks, two ways:

- the `default` nextest profile sets `retries = 0` deliberately, because hyperi-ci never selects `--profile ci`. A retry there hides a real intermittent. A test pins it.
- the `push` trigger ignores `docs/**` and `**.md`, so a docs-only push runs nothing. `pull_request` has no such filter.

### What tends to bite

| Don't | Do | Why |
|-------|----|-----|
| Commit the Kafka offsets of a batch once it is written into a file. | Hold them on the file and release them when it completes. The armed consumer commits each partition up to its lowest offset not yet released. | A file is an upload in progress until it rolls, and a kill abandons it. Default routing is expression on `_org_id`, so one partition fans out to several files that complete at different times (#82). |
| Build a second `MetricsManager` in a test and assert on `render()`. | Share one manager, assert on the delta. | `set_global_recorder` succeeds once per process. Later managers keep the existing recorder and render an empty string. nextest runs each test in its own process and hides it, `cargo-llvm-cov` runs one process and does not (#84). |
| Trust `cargo update -p <crate>` to clear an advisory. | `cargo update -p <crate> --precise <fixed version>`, then build both arches. | Plain `-p` stops short of the fix when it needs a sibling such as aws-lc-rs to move too. `--precise` drags the sibling along, and `aws-lc-sys` compiles C. |
| Point `scalo` at a local path to try an unreleased change. | Keep the crates.io range. Read the local clone instead. | A path override builds against uncommitted work, so the release does not reproduce. |
| Hand-edit `Dockerfile`. | `dfe-archiver emit-dockerfile > Dockerfile`. | It is generated from the deployment contract, and a scalo release can move the generator or its schema version. |
| Give PGO a port check or a startup probe as its workload. | Drive consume, route, compress and write for 60s or more. | Shallow workloads bias the profile toward startup paths and give NEGATIVE gains. Measured on dfe-loader and dfe-receiver. |

### Where this sits

Inbound:

- hyperi-io/scalo-rs, cargo dependency. The workspace declares one `scalo` range in `[workspace.dependencies]` and all three crates inherit it, so a scalo release is a range check, a bump and a rebuild.
- hyperi-io/scalo-rs again, generator, lockstep. `Dockerfile` comes from `scalo::deployment::generate_dockerfile()` over this repo's contract, so a generator or schema-version change means regenerate and commit the diff.

Outbound -- the repo a change here breaks:

- hyperi-io/dfe-infra, image pin, lockstep. `versions.yaml` pins the image and the thin chart released here, and Argo deploys that published chart. A release here is not deployed until that pin moves. This repo commits no chart: the release assembles it from the deployment contract.

Regenerate both from dfe-infra: `python3 scripts/dfe-stack suite --consumer dfe-archiver`, and again with `--producer`.
