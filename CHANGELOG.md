## [1.6.5](https://github.com/hyperi-io/dfe-archiver/compare/v1.6.4...v1.6.5) (2026-04-16)


### Bug Fixes

* add ARCHIVER_S3_ALLOW_HTTP env var support ([7c3f831](https://github.com/hyperi-io/dfe-archiver/commit/7c3f8310ecd985d73153d7ed5effa30f5d8fc12b))
* add tests for S3 allow_http default and serde round-trip ([0f88087](https://github.com/hyperi-io/dfe-archiver/commit/0f8808765009c5c1d98e7b07f8e00d0afdef62a1))
* address security and robustness issues from code review ([e96b03a](https://github.com/hyperi-io/dfe-archiver/commit/e96b03ad795b2d99c17c97028bdd4fa68a5afe03))
* ensure e2e tests stop docker containers they started ([1fc7e2c](https://github.com/hyperi-io/dfe-archiver/commit/1fc7e2ce4240975c3072c77e71a111853c6a6ed0))
* load .env in e2e tests so they use host-configured credentials ([a615298](https://github.com/hyperi-io/dfe-archiver/commit/a6152980dd9b55391f5c635c35287a5962a2d3ab))
* redact deleted AWS access key in TODO.md (gitleaks) ([347b821](https://github.com/hyperi-io/dfe-archiver/commit/347b82156d3454757bac3311c9817ed564ae261f))

## [1.6.4](https://github.com/hyperi-io/dfe-archiver/compare/v1.6.3...v1.6.4) (2026-04-09)


### Bug Fixes

* consume staged batches by value, eliminate offset clones ([554dd5d](https://github.com/hyperi-io/dfe-archiver/commit/554dd5d1f647fb7fe4b0f62ad9376c0b43592235))
* direct-append hot buffer eliminates serialisation phase ([36b6c76](https://github.com/hyperi-io/dfe-archiver/commit/36b6c7613ae56e2f6ec906d8f957111573b92562))
* remove double Prometheus recorder installation on startup ([130c6cd](https://github.com/hyperi-io/dfe-archiver/commit/130c6cd07a250ad0fe6c64c02f39f26e9f3c0cab)), closes [#16](https://github.com/hyperi-io/dfe-archiver/issues/16)
* replace LRU VecDeque linear scan with IndexMap hash lookup ([fa00f6f](https://github.com/hyperi-io/dfe-archiver/commit/fa00f6feff3a771c5aa8c37e4bf6f11b7ed4c2e1))
* slim KafkaOffset to newtype around KafkaToken ([e197e19](https://github.com/hyperi-io/dfe-archiver/commit/e197e19da4d7744389752ceeb12df324bda074d3))
* suppress cast_possible_wrap clippy lint in test assertion ([1523bfe](https://github.com/hyperi-io/dfe-archiver/commit/1523bfec3cacd97b740f5c6c99663d9b728d8e6a))
* zero-copy kafka recv, move-based commit ([9ea19ca](https://github.com/hyperi-io/dfe-archiver/commit/9ea19ca49e76dd42cbee4a18bfb65a8a062cc7a7))


### Performance Improvements

* offload compression to spawn_blocking ([8df77cc](https://github.com/hyperi-io/dfe-archiver/commit/8df77cc40dbcdf0ec2760fe0e12f6bcd727a8b37))
* parallel message routing via rayon par_iter ([6e63357](https://github.com/hyperi-io/dfe-archiver/commit/6e63357170dfd21b29b17511f0da160f7d6cfdb3))

## [1.6.3](https://github.com/hyperi-io/dfe-archiver/compare/v1.6.2...v1.6.3) (2026-04-08)


### Bug Fixes

* remove double Prometheus recorder installation on startup ([e4b9a53](https://github.com/hyperi-io/dfe-archiver/commit/e4b9a5379c981a0bb2959c4d5be037e9ad9aa197))
* Remove unused imports (ci fix) ([6ea0c24](https://github.com/hyperi-io/dfe-archiver/commit/6ea0c24dd5658e66ac32bfae79e46adb29ed13b7))

## [1.6.2](https://github.com/hyperi-io/dfe-archiver/compare/v1.6.1...v1.6.2) (2026-04-03)


### Bug Fixes

* concurrent batch writes, parallel routing, SOC2 audit logging ([12bc9de](https://github.com/hyperi-io/dfe-archiver/commit/12bc9de4cd59710db7d29b5eea42a561a99c6b03))

## [1.6.1](https://github.com/hyperi-io/dfe-archiver/compare/v1.6.0...v1.6.1) (2026-04-02)


### Bug Fixes

* add comprehensive debug/trace logging across all pipeline stages ([44d1749](https://github.com/hyperi-io/dfe-archiver/commit/44d17495c05e0029dd97b8f29a89b6aa6cddacf9))
* add independent flush timer + fix {topic} path template ([4b87a4c](https://github.com/hyperi-io/dfe-archiver/commit/4b87a4c41d860a50e6fa7623afda34cfa23ba319))
* bump hyperi-rustlib to >=2.4.3 ([5c40044](https://github.com/hyperi-io/dfe-archiver/commit/5c4004470e7616436b24504bfbd6d00d4af148e8))
* per-destination writer locks — concurrent writes to different destinations ([f18a6df](https://github.com/hyperi-io/dfe-archiver/commit/f18a6df10029a70a60653cb0739c8c632ed92cc3))
* remove tracked target symlink — breaks CI runners ([059720e](https://github.com/hyperi-io/dfe-archiver/commit/059720e5dc7f4b939523057c7ea86def8ceb93b2))
* resolve rustlib v2.4.3 compile errors — MemoryGuardConfig import, DeploymentContract fields ([a77cc2c](https://github.com/hyperi-io/dfe-archiver/commit/a77cc2c3b0a1c593ea7f7f9db8689ce5dd55506a))
* separate route+buffer from write phase for future parallel compression ([3007549](https://github.com/hyperi-io/dfe-archiver/commit/3007549f9c820a8fc457fc4328953295d2a1a565))
* update DfeMetrics::register() to pass &MetricsManager for manifest ([af3cd11](https://github.com/hyperi-io/dfe-archiver/commit/af3cd11213ea18dd2a551355ca1478049f33198b))
* update to rustlib v2.x ServiceRuntime + releaserc breaking rule ([f65b724](https://github.com/hyperi-io/dfe-archiver/commit/f65b724ce7c8d519ce4a5312321f489dcd302b20)), closes [hyperi-ci#14](https://github.com/hyperi-ci/issues/14)

# [1.6.0](https://github.com/hyperi-io/dfe-archiver/compare/v1.5.3...v1.6.0) (2026-03-29)


### Bug Fixes

* wire compression, roll, close, and sink metrics into pipeline ([fb52298](https://github.com/hyperi-io/dfe-archiver/commit/fb52298353efa8111554afb46a1024649d82df37))


### Features

* add DLQ support via rustlib dlq module ([091abe4](https://github.com/hyperi-io/dfe-archiver/commit/091abe41098e66325dfcda03e2777682f5ffaa58))

## [1.5.3](https://github.com/hyperi-io/dfe-archiver/compare/v1.5.2...v1.5.3) (2026-03-27)


### Bug Fixes


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
