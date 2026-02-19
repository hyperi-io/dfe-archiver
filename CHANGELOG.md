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
