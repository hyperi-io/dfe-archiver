# Changelog

Rendered by CI and committed back at the end of a release -- do not edit by
hand. Release notes also appear on the GitHub Releases page, one per tag.

## [1.7.26](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.25...v1.7.26) (2026-09-27)

### Bug Fixes

* **archiver:** hold Kafka offsets until the archive file is durable ([#98](https://github.com/hyperi-io/dfe-archiver/issues/98)) ([e8fd4f7](https://github.com/hyperi-io/dfe-archiver/commit/e8fd4f7314090e45e93dd1df998cfa0a5e393808))
* **metrics:** count records and bytes written once ([#97](https://github.com/hyperi-io/dfe-archiver/issues/97)) ([6d77b54](https://github.com/hyperi-io/dfe-archiver/commit/6d77b54ce826a62fb4e3a8b1de4e7e82314a738f))

## [1.7.25](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.24...v1.7.25) (2026-09-24)

### Bug Fixes

* **archiver:** hold the offset floor across per-org buffers ([#95](https://github.com/hyperi-io/dfe-archiver/issues/95)) ([799cf8a](https://github.com/hyperi-io/dfe-archiver/commit/799cf8acbbc4fc940dc1795b4e381146293b2ec2)), closes [#82](https://github.com/hyperi-io/dfe-archiver/issues/82) [#83](https://github.com/hyperi-io/dfe-archiver/issues/83)
* rebuild on scalo 2.12.9 ([e5190b0](https://github.com/hyperi-io/dfe-archiver/commit/e5190b0c0c43a88d705d95dfc1a42f71f8a9114c))

## [1.7.24](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.23...v1.7.24) (2026-09-24)

### Bug Fixes

* rebuild on scalo 2.12.7 ([5a9617c](https://github.com/hyperi-io/dfe-archiver/commit/5a9617c66160acbd1329ea255a2e97fc48c81ca8))

## [1.7.23](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.22...v1.7.23) (2026-09-24)

### Bug Fixes

* **ci:** skip PGO and BOLT for the rc.14 workstream ([#92](https://github.com/hyperi-io/dfe-archiver/issues/92)) ([b4bfa15](https://github.com/hyperi-io/dfe-archiver/commit/b4bfa159e3ff4abfed4d22baa81ee61ebaf8a980))
* **deps:** move rustls off RUSTSEC-2026-0285 and chacha20 off a yank ([#91](https://github.com/hyperi-io/dfe-archiver/issues/91)) ([72648db](https://github.com/hyperi-io/dfe-archiver/commit/72648db1c5aae744eabdf127e89a883d30d9665b))
* **deps:** move to scalo 2.12.3 for the KEDA Kafka trigger ([#80](https://github.com/hyperi-io/dfe-archiver/issues/80)) ([185f12e](https://github.com/hyperi-io/dfe-archiver/commit/185f12e8983c388886517605f5596e2483a41b1c))
* **docs:** move the architecture doc under docs/ and add the README Context section ([#86](https://github.com/hyperi-io/dfe-archiver/issues/86)) ([804d6d4](https://github.com/hyperi-io/dfe-archiver/commit/804d6d4172b27ce187d68c1e843689c3cc6cbf18))
* **docs:** name scalo, not rustlib ([#90](https://github.com/hyperi-io/dfe-archiver/issues/90)) ([459d942](https://github.com/hyperi-io/dfe-archiver/commit/459d942b74679a76396e1fbe5068925f690bf79f))
* give the archiver spool a config key with an absolute default ([9f8fbb8](https://github.com/hyperi-io/dfe-archiver/commit/9f8fbb817f43317e850aee096cbe3f013f20c556)), closes [hyperi-io/scalo-rs#107](https://github.com/hyperi-io/scalo-rs/issues/107) [#29](https://github.com/hyperi-io/dfe-archiver/issues/29) [scalo-rs#59](https://github.com/hyperi-io/scalo-rs/issues/59) [#93](https://github.com/hyperi-io/dfe-archiver/issues/93) [hyperi-ci#134](https://github.com/hyperi-io/hyperi-ci/issues/134)
* keep a tokio worker for the probe surface and record the transport menu ([44e660e](https://github.com/hyperi-io/dfe-archiver/commit/44e660e000f82f405eae4456e29e87e5646c1e15)), closes [scalo-rs#10](https://github.com/hyperi-io/scalo-rs/issues/10) [#56](https://github.com/hyperi-io/dfe-archiver/issues/56)
* rebuild on scalo 2.12.6 with release consent ([d9cb3a2](https://github.com/hyperi-io/dfe-archiver/commit/d9cb3a246a62adce4046d8357c4202f52dbd5a07))
* reject unknown path placeholders, count routing fallbacks and roll on the timer ([d2f27a6](https://github.com/hyperi-io/dfe-archiver/commit/d2f27a65e8a007c5b67a7c6ab1db879a13a8e83a)), closes [#48](https://github.com/hyperi-io/dfe-archiver/issues/48) [#59](https://github.com/hyperi-io/dfe-archiver/issues/59) [#53](https://github.com/hyperi-io/dfe-archiver/issues/53) [#25](https://github.com/hyperi-io/dfe-archiver/issues/25)
* report the release version, the files created and the reloads that need a restart ([e22abf5](https://github.com/hyperi-io/dfe-archiver/commit/e22abf5cd770937d78dc028958ddcfce8e48f1bd)), closes [#65](https://github.com/hyperi-io/dfe-archiver/issues/65) [#49](https://github.com/hyperi-io/dfe-archiver/issues/49) [#64](https://github.com/hyperi-io/dfe-archiver/issues/64) [#68](https://github.com/hyperi-io/dfe-archiver/issues/68) [#69](https://github.com/hyperi-io/dfe-archiver/issues/69) [#70](https://github.com/hyperi-io/dfe-archiver/issues/70)

## [1.7.22](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.21...v1.7.22) (2026-09-12)

### Bug Fixes

* rebuild on scalo 2.12.2 ([5dadc69](https://github.com/hyperi-io/dfe-archiver/commit/5dadc69e59eade49b5f73a5c82ceed81776c5a9d))

## [1.7.21](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.20...v1.7.21) (2026-09-10)

### Bug Fixes

* **config:** make the deployment's env vars reach the settings they name ([8365f44](https://github.com/hyperi-io/dfe-archiver/commit/8365f44ea09fd8bd9666068aa9f5128e3ae4923d))
* **health:** prove the archive sink answers before claiming healthy ([63b4023](https://github.com/hyperi-io/dfe-archiver/commit/63b4023f3de776b26fda180ca7a91e8085be34d0))

## [1.7.20](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.19...v1.7.20) (2026-09-09)

### Bug Fixes

* the archiver idles until configured and receives on direct ([732f1be](https://github.com/hyperi-io/dfe-archiver/commit/732f1be878021a85ea4ceab30d94dde08c5786ac))

## [1.7.19](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.18...v1.7.19) (2026-08-28)

### Bug Fixes

* clear local quality gate findings ([6d21091](https://github.com/hyperi-io/dfe-archiver/commit/6d210912ed37cf44576acecac1927d31ffe67618))
* default to split on expression with org_id (instead of topic) ([3d04ac4](https://github.com/hyperi-io/dfe-archiver/commit/3d04ac47cf8662af15cfb6b7309dc38a8fa12962))
* version check on by default via the releases endpoint ([bce6932](https://github.com/hyperi-io/dfe-archiver/commit/bce6932d5eeceb8bd9d10108490c73fa5ea7a382))

## [1.7.18](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.17...v1.7.18) (2026-08-27)

### Bug Fixes

* never overwrite an existing archive file ([8ff3bb8](https://github.com/hyperi-io/dfe-archiver/commit/8ff3bb8f1a9ac1632a6de1951041d0ed7c8592b0))
* reset archive file sequence per hour ([caa7a36](https://github.com/hyperi-io/dfe-archiver/commit/caa7a36fa6d881de1db8926a8993485906480049))
* scalo 2.10.14 + startup version check ([b974c3d](https://github.com/hyperi-io/dfe-archiver/commit/b974c3d67d067f5b80b0166e9792acf4e25ed181))

## [1.7.17](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.16...v1.7.17) (2026-08-23)

### Bug Fixes

* **ci:** pin LOG_FORMAT=json for the PGO workload ([3d885ce](https://github.com/hyperi-io/dfe-archiver/commit/3d885ce960b9a79a471e62903f9d6ce3ace5af42))
* **deps:** adopt scalo 2.10.13 ([#51](https://github.com/hyperi-io/dfe-archiver/issues/51)) ([6c6dd35](https://github.com/hyperi-io/dfe-archiver/commit/6c6dd35120a25a0c79b80b740096aebb105d5276))

## [1.7.16](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.15...v1.7.16) (2026-08-18)

## [1.7.15](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.14...v1.7.15) (2026-08-18)

## [1.7.14](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.13...v1.7.14) (2026-08-18)

## [1.7.13](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.12...v1.7.13) (2026-08-17)

## [1.7.12](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.11...v1.7.12) (2026-08-04)

## [1.7.11](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.10...v1.7.11) (2026-08-03)

## [1.7.3](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.2...v1.7.3) (2026-05-28)


### Bug Fixes

* adopt v2.7.1 DLQ API (Dlq::spawn + queue-admission send semantics) ([2dd55ee](https://github.com/hyperi-io/dfe-archiver/commit/2dd55ee4bdfb354af4e27296001ba8451349cb72))
* **ci:** correct stale Kafka references in PGO workload header (now Redpanda) ([646eb0b](https://github.com/hyperi-io/dfe-archiver/commit/646eb0b09f0f60bb8924735a292127dbd27ca681))
* **ci:** detect archiver readiness from log line, not unreliable /readyz probe ([36a6d9f](https://github.com/hyperi-io/dfe-archiver/commit/36a6d9f90bc0438376b287bda44dcc08c33fd758))
* **ci:** pre-create Redpanda topic for PGO workload (no auto-create on consumer subscribe) ([afab57b](https://github.com/hyperi-io/dfe-archiver/commit/afab57b126be86cf3057a81db669cc4ba13eda43))
* **ci:** use Redpanda (dev-container, 512M) for PGO workload broker; Kafka JVM OOMs 4GB runners ([546a0e6](https://github.com/hyperi-io/dfe-archiver/commit/546a0e66136a4e381b4586f8606ab13788b383c1))
* **deps:** adopt rustlib v2.8.0 — final call sites ([c0c340b](https://github.com/hyperi-io/dfe-archiver/commit/c0c340b3c9018d0676c023549806b04f2a99fb8a))
* **deps:** adopt rustlib v2.8.0 — remaining call sites ([962fb05](https://github.com/hyperi-io/dfe-archiver/commit/962fb05c62c9cc81fe867dfe75b67ff456b39bab))
* **deps:** adopt rustlib v2.8.0 — typed metric label enums + generate_*() extra arg ([aad32d4](https://github.com/hyperi-io/dfe-archiver/commit/aad32d4695025faebab812c394b2ced51fafb434))
* **deps:** bump hyperi-rustlib to >=2.7.1 ([8af5489](https://github.com/hyperi-io/dfe-archiver/commit/8af548937a227040856be303a66e5fbd35de7c30))
* **deps:** bump hyperi-rustlib to >=2.8.0 ([9bbd234](https://github.com/hyperi-io/dfe-archiver/commit/9bbd2343b727e8c66d603390a457327a81a5ea52))
* **release:** force patch bump v1.7.3 ([cb37d69](https://github.com/hyperi-io/dfe-archiver/commit/cb37d69c3fdf5e83283f56751102a5137833060a))
* **release:** force patch bump v1.7.4 ([5101868](https://github.com/hyperi-io/dfe-archiver/commit/5101868435d8cc63bcee5e1ca28db45b0c71c77f))

## [1.7.2](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.1...v1.7.2) (2026-05-07)


### Bug Fixes

* **release:** force patch bump v1.7.2 ([0a468f6](https://github.com/hyperi-io/dfe-archiver/commit/0a468f6d97452677436f493daf37de79f33a9912))
* **release:** retrigger publish under hyperi-ci v2.1.5 ([7db3e85](https://github.com/hyperi-io/dfe-archiver/commit/7db3e856050f7dacdd5f6639885f04694c3c8add))

## [1.7.1](https://github.com/hyperi-io/dfe-archiver/compare/v1.7.0...v1.7.1) (2026-05-02)


### Bug Fixes

* **ci:** pgo-workload — disable DLQ file backend (unwriteable in runner) ([4524490](https://github.com/hyperi-io/dfe-archiver/commit/45244901342aaa0ddc51f67697e913d7fa8bd97c))
* **deployment:** wire DfeApp::deployment_contract trait hook + bump rustlib to >=2.7.0 ([5716880](https://github.com/hyperi-io/dfe-archiver/commit/57168801b16233581f432a788429f64ab1b06b85))
* **deps:** track rustlib 2.6.1 (cli→cli-service, worker→worker-pool) ([1864bde](https://github.com/hyperi-io/dfe-archiver/commit/1864bde75f21d65dbeb701554dc527f5720fa291))

# [1.7.0](https://github.com/hyperi-io/dfe-archiver/compare/v1.6.5...v1.7.0) (2026-04-29)


### Bug Fixes

* **ci:** unblock hyperi-ci check — gitleaks/cargo-deny allowlists, infra status ([f3bf82a](https://github.com/hyperi-io/dfe-archiver/commit/f3bf82ad69191a0f5b59eb12bf4d839841349675))
* code review batch — bound writers, drop Mutex, preserve error chain ([87c6254](https://github.com/hyperi-io/dfe-archiver/commit/87c6254cf1b52d7d3674fa6dd76665568e598237)), closes [hi#cardinality](https://github.com/hi/issues/cardinality) [hi#cardinality](https://github.com/hi/issues/cardinality)


### Features

* **ci:** wire hyperi-ci Tier 2 PGO + BOLT release optimisation ([9c9e5c0](https://github.com/hyperi-io/dfe-archiver/commit/9c9e5c0675312fcb353a6482c6a08c5ef93e1254))

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
