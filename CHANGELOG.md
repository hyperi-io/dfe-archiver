# [1.3.0-dev.10](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.9...v1.3.0-dev.10) (2026-03-25)


### Bug Fixes

* add SensitiveString, config registry, bump rustlib 1.19.6 ([74e8abc](https://github.com/hyperi-io/dfe-archiver/commit/74e8abcaa2974041073095fa80d3fdfe10daae16))
* restructure tests to match HyperI testing standard ([6b94663](https://github.com/hyperi-io/dfe-archiver/commit/6b94663ffd1e75a33159bb636550c0b448641843))

# [1.3.0-dev.9](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.8...v1.3.0-dev.9) (2026-03-22)


### Bug Fixes

* inline Renovate config (preset resolution broken) ([3b6fedc](https://github.com/hyperi-io/dfe-archiver/commit/3b6fedccc9155802e752d336a34bb8a12dad6868))

# [1.3.0-dev.8](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.7...v1.3.0-dev.8) (2026-03-21)


### Bug Fixes

* update deps to resolve 3 security advisories ([fd6cf95](https://github.com/hyperi-io/dfe-archiver/commit/fd6cf9565d103303ee1f2fdfa39738cf01387f23))

# [1.3.0-dev.7](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.6...v1.3.0-dev.7) (2026-03-20)


### Features

* adopt DFE metrics standard with rustlib metric groups ([c9d5bc8](https://github.com/hyperi-io/dfe-archiver/commit/c9d5bc85b96baa568bc2c10a27401a1c8b952c7c))

# [1.3.0-dev.6](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.5...v1.3.0-dev.6) (2026-03-20)


### Bug Fixes

* bump hyperi-rustlib to >=1.16.7 ([aa56762](https://github.com/hyperi-io/dfe-archiver/commit/aa5676242da242e385cba8656dda8293cf5d177c))
* consolidate MetricsManager, wire readiness, add test infra ([b48e0c1](https://github.com/hyperi-io/dfe-archiver/commit/b48e0c166f4fb89eccb1afe808db2005558b3455))
* trigger CI on PRs to release branch ([19fa5ed](https://github.com/hyperi-io/dfe-archiver/commit/19fa5edb59f78c750f8ed2be504db4f8415da99e))

# [1.3.0-dev.5](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.4...v1.3.0-dev.5) (2026-03-20)


### Features

* add cgroup-aware MemoryGuard backpressure ([344a55f](https://github.com/hyperi-io/dfe-archiver/commit/344a55ff1e9bfbfea8cf35232dd5bf2712756de9))

# [1.3.0-dev.4](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.3...v1.3.0-dev.4) (2026-03-19)


### Bug Fixes

* trigger release build for GA ([1a3cfb8](https://github.com/hyperi-io/dfe-archiver/commit/1a3cfb80a5d75cbd0f0b155ccdb6442be9d6bf18))

# [1.3.0-dev.3](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.2...v1.3.0-dev.3) (2026-03-19)


### Features

* rustlib v1.16.3 observability remediation ([d84909b](https://github.com/hyperi-io/dfe-archiver/commit/d84909b8dfc6489bf13c424e976eb6ea70dd53f5))

# [1.3.0-dev.2](https://github.com/hyperi-io/dfe-archiver/compare/v1.3.0-dev.1...v1.3.0-dev.2) (2026-03-19)


### Bug Fixes

* address code review findings from Rust standards audit ([46ed024](https://github.com/hyperi-io/dfe-archiver/commit/46ed0243583e2db04f1edfa000b13bc23e188e4f))


### Features

* add config hot-reload via rustlib SharedConfig + ConfigReloader ([4b6c3e5](https://github.com/hyperi-io/dfe-archiver/commit/4b6c3e5241d7116d94f9c5f3b8bd5fb0d933ef98))

# [1.3.0-dev.1](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.6-dev.3...v1.3.0-dev.1) (2026-03-18)


### Features

* add KEDA scaling metrics, DeploymentContract, and DfeApp CLI ([2ab1245](https://github.com/hyperi-io/dfe-archiver/commit/2ab1245b264ab7aef9e1272b076f9f2ae70b037b))

## [1.2.6-dev.3](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.6-dev.2...v1.2.6-dev.3) (2026-03-16)


### Bug Fixes

* remove cmake-build rdkafka, add tooling config, create Dockerfile ([3498dd3](https://github.com/hyperi-io/dfe-archiver/commit/3498dd33ff809b418ed831661b524424572b5b1b))

## [1.2.6-dev.2](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.6-dev.1...v1.2.6-dev.2) (2026-03-16)


### Bug Fixes

* use crates.io for hyperi-rustlib, remove transport-zenoh ([f542230](https://github.com/hyperi-io/dfe-archiver/commit/f542230c5d43f540885835ff2f11363b0ed7301e))

## [1.2.6-dev.1](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.5...v1.2.6-dev.1) (2026-03-16)


### Bug Fixes

* migrate to 3-crate workspace (core, io, archiver) [skip ci] ([6079398](https://github.com/hyperi-io/dfe-archiver/commit/6079398e38f09d8caabdf94c18e78108db37b37a))
* migrate to hyperi-ci and fix clippy quality debt ([0aba816](https://github.com/hyperi-io/dfe-archiver/commit/0aba816f55db53afb625f9a128eae3175d5349cc))
* migrate to hyperi-ci from legacy ci submodule ([a6b16c3](https://github.com/hyperi-io/dfe-archiver/commit/a6b16c310ec627e460f7fd3d818d8fc85161cf2a))
* update bytes 1.11.1 (RUSTSEC-2026-0007), time 0.3.47 (RUSTSEC-2026-0009) [skip ci] ([fea31cb](https://github.com/hyperi-io/dfe-archiver/commit/fea31cb44bda84421368a213eda0ef7150a250d2))

## [1.2.5](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.4...v1.2.5) (2026-03-06)


### Bug Fixes

* exclude ai, ci, docs dirs from cargo publish package [skip ci] ([e0ba986](https://github.com/hyperi-io/dfe-archiver/commit/e0ba986899e7d95a8d9cb095761af5ccf84270b2))

## [1.2.4](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.3...v1.2.4) (2026-02-24)


### Bug Fixes

* comment out empty gitleaks allowlist for 8.30.0 compat ([24548b2](https://github.com/hyperi-io/dfe-archiver/commit/24548b24ca5438543efe9a18cf736172b4594158))

## [1.2.3](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.2...v1.2.3) (2026-02-19)


### Bug Fixes

* add transport-zenoh support via hyperi-rustlib ([d691b30](https://github.com/hyperi-io/dfe-archiver/commit/d691b30fd41436cf3f73e0d41983b5f94ced46bc))

## [1.2.2](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.1...v1.2.2) (2026-02-19)


### Bug Fixes

* add binary app build CI with cross-compilation for x86_64 and aarch64 ([245f2a4](https://github.com/hyperi-io/dfe-archiver/commit/245f2a45cae5e5b4f4b5a4fab474beeca357f4c1))
* add file sequence counter for rolling and list_prefix for storage backends ([b063f05](https://github.com/hyperi-io/dfe-archiver/commit/b063f05ebd4625161267b1ba61fca02e4bf649b0))

## [1.2.1](https://github.com/hyperi-io/dfe-archiver/compare/v1.2.0...v1.2.1) (2026-02-17)


### Bug Fixes

* use from_env() for cloud storage builders to resolve credentials from environment ([c0b2716](https://github.com/hyperi-io/dfe-archiver/commit/c0b27168374c323f4e6d52cbba8ef913f93baa35))

# [1.2.0](https://github.com/hyperi-io/dfe-archiver/compare/v1.1.5...v1.2.0) (2026-02-17)


### Features

* replace S3Backend with ObjectStoreBackend for streaming multipart uploads ([57b97a8](https://github.com/hyperi-io/dfe-archiver/commit/57b97a88f3f1e76bcb13aa0bf7490b7d5abff95c))

## [1.1.5](https://github.com/hyperi-io/dfe-archiver/compare/v1.1.4...v1.1.5) (2026-02-17)


### Bug Fixes

* complete Phase 1 with at-least-once delivery fix and test restructuring ([489510e](https://github.com/hyperi-io/dfe-archiver/commit/489510e859839cd9e3aae0d72695f000314dab6b))

## [1.1.4](https://github.com/hyperi-io/dfe-archiver/compare/v1.1.3...v1.1.4) (2026-02-17)


### Bug Fixes

* **ci:** use default runner for release workflow ([9d6072f](https://github.com/hyperi-io/dfe-archiver/commit/9d6072fcdf7a163c85f0587d8de99d57ae9be2c8))

## [1.1.3](https://github.com/hyperi-io/dfe-archiver/compare/v1.1.2...v1.1.3) (2026-02-17)


### Bug Fixes

* add typos config and fix spelling for CI quality checks ([c1ddb36](https://github.com/hyperi-io/dfe-archiver/commit/c1ddb363a174f7deec5bbb2ff095b9c5121d2a60))

## [1.1.2](https://github.com/hyperi-io/dfe-archiver/compare/v1.1.1...v1.1.2) (2026-02-17)


### Bug Fixes

* update benchmarks for hyperi-rustlib API changes ([09c10eb](https://github.com/hyperi-io/dfe-archiver/commit/09c10eb04e457bfa898cabd11655ce8f16fac2a3))

## [1.1.1](https://github.com/hypersec-io/dfe-archiver/compare/v1.1.0...v1.1.1) (2026-02-03)


### Bug Fixes

* **ci:** allow clippy warnings in release workflow for now ([0bdd57a](https://github.com/hypersec-io/dfe-archiver/commit/0bdd57af2e3d65cef59312eee3112be5835b0621))

# [1.1.0](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.8...v1.1.0) (2026-02-03)


### Features

* **ci:** configure feature matrix testing with nextest ([dca88b6](https://github.com/hypersec-io/dfe-archiver/commit/dca88b62878f81bdabc881c532a9fc998e5fe316))

## [1.0.8](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.7...v1.0.8) (2026-02-03)


### Bug Fixes

* **ci:** add --allow-dirty flag to cargo publish ([ed003ac](https://github.com/hypersec-io/dfe-archiver/commit/ed003ac43c97cff6c92ac5d7895e8134e5dc6855))

## [1.0.7](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.6...v1.0.7) (2026-02-03)


### Bug Fixes

* use hypersec registry for hs-rustlib dependency ([9acd087](https://github.com/hypersec-io/dfe-archiver/commit/9acd087bcc1a0a8871f7100b63388e643cb5fb1b))

## [1.0.6](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.5...v1.0.6) (2026-02-03)


### Bug Fixes

* handle mutually exclusive allocator features with --all-features ([d3c9659](https://github.com/hypersec-io/dfe-archiver/commit/d3c96592d93ae2b06c65640c6cb1658abf120524))

## [1.0.5](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.4...v1.0.5) (2026-02-03)


### Bug Fixes

* restore allocator dependency versions (final fix with updated CI) ([9e565b7](https://github.com/hypersec-io/dfe-archiver/commit/9e565b7f260d085155c5d469bdbaa5d7ee9792f9))

## [1.0.4](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.3...v1.0.4) (2026-02-03)


### Bug Fixes

* update ci submodule with Cargo.toml version fix ([853580f](https://github.com/hypersec-io/dfe-archiver/commit/853580f911d4a97aa6ed7d89c0b5b11b65f4bda7))

## [1.0.3](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.2...v1.0.3) (2026-02-03)


### Bug Fixes

* restore allocator dependency versions corrupted by semantic-release ([ab668d6](https://github.com/hypersec-io/dfe-archiver/commit/ab668d6c06a8a4a4a88f8c68ad0caa21ceb4b005))

## [1.0.2](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.1...v1.0.2) (2026-02-03)


### Bug Fixes

* **ci:** add GitHub App token authentication to release workflow ([2a8b250](https://github.com/hypersec-io/dfe-archiver/commit/2a8b25075ac432c78996357f1d9ee14d170c8d80))

## [1.0.1](https://github.com/hypersec-io/dfe-archiver/compare/v1.0.0...v1.0.1) (2026-02-03)


### Bug Fixes

* clean up unused imports and fix allocator dependency versions ([3efa3c7](https://github.com/hypersec-io/dfe-archiver/commit/3efa3c759706714a2b54fa949feb82d1b48be070))

# 1.0.0 (2026-02-03)


### Features

* initial implementation of DFE Archiver ([fea4117](https://github.com/hypersec-io/dfe-archiver/commit/fea4117c9443ec91838c95087d7b30bafd9af730)), closes [Hi#volume](https://github.com/Hi/issues/volume)
