# Changelog

## [1.10.0](https://github.com/ardent-data/weevr/compare/weevr-v1.9.0...weevr-v1.10.0) (2026-03-28)


### Features

* add concat, map, format steps and type-aware fill_null ([#108](https://github.com/ardent-data/weevr/issues/108)) ([fae398c](https://github.com/ardent-data/weevr/commit/fae398c5d645a32e7bb5469b16bbd1b471c63090))
* **engine:** add analytical target modes with dimension and fact blocks ([#109](https://github.com/ardent-data/weevr/issues/109)) ([0046cc2](https://github.com/ardent-data/weevr/commit/0046cc2a972516456982561285aa91b26d27602e))
* **engine:** add audit column templates with built-in presets ([#107](https://github.com/ardent-data/weevr/issues/107)) ([ae729fe](https://github.com/ardent-data/weevr/commit/ae729fe8922cb6bede35f48a2abaf555481b4e18))
* **engine:** add resolve step for FK resolution with batch mode and fk_sentinel_rate assertion ([#110](https://github.com/ardent-data/weevr/issues/110)) ([860ca34](https://github.com/ardent-data/weevr/commit/860ca342a3992f8edb4a130b8962ea21b3bde5d7))
* **engine:** add shared resource universality across loom, weave, and thread levels ([#105](https://github.com/ardent-data/weevr/issues/105)) ([1715b71](https://github.com/ardent-data/weevr/commit/1715b7198581d1aed31958b583baa6ba0af01a30))

## [1.9.0](https://github.com/ardent-data/weevr/compare/weevr-v1.8.0...weevr-v1.9.0) (2026-03-26)


### Features

* **engine:** add reserved word presets for Power BI, DAX, M, and T-SQL ([#101](https://github.com/ardent-data/weevr/issues/101)) ([bd16bf4](https://github.com/ardent-data/weevr/commit/bd16bf4949cb1cfd6014e32022c69cded51f4dc8))

## [1.8.0](https://github.com/ardent-data/weevr/compare/weevr-v1.7.1...weevr-v1.8.0) (2026-03-24)


### Features

* **engine:** add dictionary rename and naming enhancements ([#97](https://github.com/ardent-data/weevr/issues/97)) ([e22bd49](https://github.com/ardent-data/weevr/commit/e22bd49d17ddcdfc2caf3c91ca202762a455ee79))

## [1.7.1](https://github.com/ardent-data/weevr/compare/weevr-v1.7.0...weevr-v1.7.1) (2026-03-15)


### Bug Fixes

* **engine:** rewrite Sankey waterfall with proper visualization ([#93](https://github.com/ardent-data/weevr/issues/93)) ([19317e9](https://github.com/ardent-data/weevr/commit/19317e90cd9a230e872b314a887a311bea947fc9))

## [1.7.0](https://github.com/ardent-data/weevr/compare/weevr-v1.6.0...weevr-v1.7.0) (2026-03-15)


### Features

* **engine:** add export secondary outputs with cascade and format support ([#90](https://github.com/ardent-data/weevr/issues/90)) ([e38addc](https://github.com/ardent-data/weevr/commit/e38addc3874fdd1f8fa7dc725029f3ed6a5b7934))

## [1.6.0](https://github.com/ardent-data/weevr/compare/weevr-v1.5.0...weevr-v1.6.0) (2026-03-14)


### Features

* **engine:** add audit column injection with additive cascade ([#87](https://github.com/ardent-data/weevr/issues/87)) ([4505c15](https://github.com/ardent-data/weevr/commit/4505c1538a3716ce0222f4a9e65cd22418ff9a86))

## [1.5.0](https://github.com/ardent-data/weevr/compare/weevr-v1.4.0...weevr-v1.5.0) (2026-03-13)


### Features

* **engine:** add thread flow, timeline, and rich result rendering ([#82](https://github.com/ardent-data/weevr/issues/82)) ([55359e4](https://github.com/ardent-data/weevr/commit/55359e40680b59c39e79f90c4c92733a0c5f0bd6))

## [1.4.0](https://github.com/ardent-data/weevr/compare/weevr-v1.3.0...weevr-v1.4.0) (2026-03-13)


### Features

* **engine:** add plan display, DAG visualization, and rich result rendering ([#79](https://github.com/ardent-data/weevr/issues/79)) ([937be3b](https://github.com/ardent-data/weevr/commit/937be3bdfcf445ebc8cd76db73037c3b5d7d16af))

## [1.3.0](https://github.com/ardent-data/weevr/compare/weevr-v1.2.1...weevr-v1.3.0) (2026-03-10)


### Features

* **engine:** defer lookup materialization to schedule-aware group boundaries ([#77](https://github.com/ardent-data/weevr/issues/77)) ([9faa8c8](https://github.com/ardent-data/weevr/commit/9faa8c8a9c85ee75aca26a22f015988a961ec08a))

## [1.2.1](https://github.com/ardent-data/weevr/compare/weevr-v1.2.0...weevr-v1.2.1) (2026-03-05)


### Bug Fixes

* **config:** resolve relative source paths against project root ([#75](https://github.com/ardent-data/weevr/issues/75)) ([c574b68](https://github.com/ardent-data/weevr/commit/c574b68c1146beeff9828a73020727b491a464c4))
* **examples:** create schemas before staging demo tables ([#73](https://github.com/ardent-data/weevr/issues/73)) ([be396dc](https://github.com/ardent-data/weevr/commit/be396dc70d312dbc9e4cd432dc001c6b17adeb52))
* Fabric runtime compatibility and engine bug fixes ([#76](https://github.com/ardent-data/weevr/issues/76)) ([43716e5](https://github.com/ardent-data/weevr/commit/43716e59da7c895e66d415a99bcaea721bec1f5a))

## [1.2.0](https://github.com/ardent-data/weevr/compare/weevr-v1.1.0...weevr-v1.2.0) (2026-03-03)


### Features

* **engine:** add narrow lookup projection, filtering, and key validation ([#65](https://github.com/ardent-data/weevr/issues/65)) ([1db7eeb](https://github.com/ardent-data/weevr/commit/1db7eeb8dc4145b720460ab52de1f61b2df9915d))
* **engine:** expand hash algorithms and add output mode ([#66](https://github.com/ardent-data/weevr/issues/66)) ([311c2a2](https://github.com/ardent-data/weevr/commit/311c2a21d59800a74dc83fc70d01ebfaa898de9a))


### Documentation

* **examples:** add M101 lookups and sample data to Fabcon demos ([#63](https://github.com/ardent-data/weevr/issues/63)) ([d86464b](https://github.com/ardent-data/weevr/commit/d86464b529bf6c8de5fcb6d0eb0b4a9d7e44eaeb))
* **examples:** enhance Fabcon demo configs with narrow lookups and hooks ([#67](https://github.com/ardent-data/weevr/issues/67)) ([2d11bea](https://github.com/ardent-data/weevr/commit/2d11bea90533f1241080366120aef3eb3940ba15))

## [1.1.0](https://github.com/ardent-data/weevr/compare/weevr-v1.0.6...weevr-v1.1.0) (2026-03-02)


### Features

* **engine:** add execution hooks, lookups, and quality gates ([#62](https://github.com/ardent-data/weevr/issues/62)) ([7b1f79b](https://github.com/ardent-data/weevr/commit/7b1f79b663b1110eab786de166b02d39117cea83))
* **examples:** add Fabcon 2026 progressive demo configs ([#60](https://github.com/ardent-data/weevr/issues/60)) ([d11899a](https://github.com/ardent-data/weevr/commit/d11899ad81e7b3bd420e642c3390ee56d3fb3bd0))

## [1.0.6](https://github.com/ardent-data/weevr/compare/weevr-v1.0.5...weevr-v1.0.6) (2026-02-28)


### Bug Fixes

* **ci:** disable D2 plugin cache to prevent gdbm lock error ([#56](https://github.com/ardent-data/weevr/issues/56)) ([2c93721](https://github.com/ardent-data/weevr/commit/2c93721a6b59d2a93221d9b2761bc5ee2ac21e6f))
* **state:** resolve CodeQL warnings for unreachable statements and cyclic imports ([#58](https://github.com/ardent-data/weevr/issues/58)) ([6465540](https://github.com/ardent-data/weevr/commit/6465540f77e4d1b595371289ea7171bf5085c2aa))

## [1.0.5](https://github.com/ardent-data/weevr/compare/weevr-v1.0.4...weevr-v1.0.5) (2026-02-27)


### Bug Fixes

* **ci:** add D2 binary install to release and release-quality workflows ([#54](https://github.com/ardent-data/weevr/issues/54)) ([d7f7760](https://github.com/ardent-data/weevr/commit/d7f77608983f4a10dc1268b749c553a8f655a2b9))

## [1.0.4](https://github.com/ardent-data/weevr/compare/weevr-v1.0.3...weevr-v1.0.4) (2026-02-27)


### Bug Fixes

* **config:** prevent double .weevr extension in project path resolution ([#47](https://github.com/ardent-data/weevr/issues/47)) ([89b429f](https://github.com/ardent-data/weevr/commit/89b429f1425b8389ca0476e61141ef446c805308))
* **engine:** surface thread errors in result detail and summary ([#49](https://github.com/ardent-data/weevr/issues/49)) ([c9fce00](https://github.com/ardent-data/weevr/commit/c9fce00e41b5047c1bb45cbb09145c305c365dbe))


### Documentation

* **site:** add D2 diagrams, architecture guides, and error catalog ([#53](https://github.com/ardent-data/weevr/issues/53)) ([790f63b](https://github.com/ardent-data/weevr/commit/790f63be4d35eedc0f3e48a99cd99e0ef423e37e))

## [1.0.3](https://github.com/ardent-data/weevr/compare/weevr-v1.0.2...weevr-v1.0.3) (2026-02-26)


### Bug Fixes

* **ci:** add production PyPI publishing to release workflow ([#45](https://github.com/ardent-data/weevr/issues/45)) ([0aec813](https://github.com/ardent-data/weevr/commit/0aec813f99305f8bab188aa7727a42547e3fd5b7))

## [1.0.2](https://github.com/ardent-data/weevr/compare/weevr-v1.0.1...weevr-v1.0.2) (2026-02-26)


### Bug Fixes

* **ci:** add root redirect for GitHub Pages docs ([#43](https://github.com/ardent-data/weevr/issues/43)) ([33a7572](https://github.com/ardent-data/weevr/commit/33a7572a492684a513184ad1c30a975d06d27227))

## [1.0.1](https://github.com/ardent-data/weevr/compare/weevr-v1.0.0...weevr-v1.0.1) (2026-02-26)


### Bug Fixes

* **ci:** move docs deployment into release workflow ([#41](https://github.com/ardent-data/weevr/issues/41)) ([8743ba0](https://github.com/ardent-data/weevr/commit/8743ba0e8bc953c643d2278f296e80a9720731da))

## [1.0.0](https://github.com/ardent-data/weevr/compare/weevr-v0.7.4...weevr-v1.0.0) (2026-02-26)


### ⚠ BREAKING CHANGES

* **config:** typed extensions and project-centric context ([#40](https://github.com/ardent-data/weevr/issues/40))

### Features

* **config:** typed extensions and project-centric context ([#40](https://github.com/ardent-data/weevr/issues/40)) ([ece134a](https://github.com/ardent-data/weevr/commit/ece134a4b53d2928674999647ac812ddbd94e7cc))
* **engine:** add advanced transforms, config macros, and naming normalization ([#37](https://github.com/ardent-data/weevr/issues/37)) ([cfefb4c](https://github.com/ardent-data/weevr/commit/cfefb4c92bf524ad1a627b5dc79ecb53256c3f56))

## [0.7.4](https://github.com/ardent-data/weevr/compare/weevr-v0.7.3...weevr-v0.7.4) (2026-02-24)


### Bug Fixes

* **ci:** disable attestations and enable verbose for TestPyPI publish ([#35](https://github.com/ardent-data/weevr/issues/35)) ([a89e38a](https://github.com/ardent-data/weevr/commit/a89e38a91b47087a254303cdef9dfc0e44e7605b))

## [0.7.3](https://github.com/ardent-data/weevr/compare/weevr-v0.7.2...weevr-v0.7.3) (2026-02-24)


### Bug Fixes

* **ci:** merge publish into release workflow ([#33](https://github.com/ardent-data/weevr/issues/33)) ([d5a32da](https://github.com/ardent-data/weevr/commit/d5a32daa269df7ec2505e08c93e043f72f555796))

## [0.7.2](https://github.com/ardent-data/weevr/compare/weevr-v0.7.1...weevr-v0.7.2) (2026-02-24)


### Bug Fixes

* **ci:** trigger publish on release event instead of tag push ([#31](https://github.com/ardent-data/weevr/issues/31)) ([4ad3d3a](https://github.com/ardent-data/weevr/commit/4ad3d3a728a64c6819c115b96c28aa23ce7ac3e2))

## [0.7.1](https://github.com/ardent-data/weevr/compare/weevr-v0.7.0...weevr-v0.7.1) (2026-02-24)


### Bug Fixes

* **ci:** correct publish workflow tag pattern ([#29](https://github.com/ardent-data/weevr/issues/29)) ([8244f5e](https://github.com/ardent-data/weevr/commit/8244f5ec39aa941ed6ea3f5dc0079377383b97b6))

## [0.7.0](https://github.com/ardent-data/weevr/compare/weevr-v0.6.0...weevr-v0.7.0) (2026-02-24)


### Features

* **engine:** add incremental processing with watermark persistence and CDC support ([#27](https://github.com/ardent-data/weevr/issues/27)) ([6bf54f7](https://github.com/ardent-data/weevr/commit/6bf54f7da281e65a69a17356e146e5df195f63b2))

## [0.6.0](https://github.com/ardent-data/weevr/compare/weevr-v0.5.1...weevr-v0.6.0) (2026-02-23)


### Features

* **api:** add Context-based Python API with run/load and verification modes ([#25](https://github.com/ardent-data/weevr/issues/25)) ([4c4b57f](https://github.com/ardent-data/weevr/commit/4c4b57f31b82db3b75e1fc5ac26e953a0a566f04))

## [0.5.1](https://github.com/ardent-data/weevr/compare/weevr-v0.5.0...weevr-v0.5.1) (2026-02-23)


### Documentation

* **readme:** update status and capabilities to reflect implemented milestones ([#23](https://github.com/ardent-data/weevr/issues/23)) ([a681ad8](https://github.com/ardent-data/weevr/commit/a681ad80951306d937927ed90465f51d4facc4d3))

## [0.5.0](https://github.com/ardent-data/weevr/compare/weevr-v0.4.0...weevr-v0.5.0) (2026-02-23)


### Features

* **engine:** add telemetry, validation, and assertion execution ([#21](https://github.com/ardent-data/weevr/issues/21)) ([3de36b0](https://github.com/ardent-data/weevr/commit/3de36b099d63d8cff90d421074a37e6a8a65c3c1))

## [0.4.0](https://github.com/ardent-data/weevr/compare/weevr-v0.3.1...weevr-v0.4.0) (2026-02-23)


### Features

* **engine:** add DAG orchestration for weave and loom execution ([#19](https://github.com/ardent-data/weevr/issues/19)) ([99964a2](https://github.com/ardent-data/weevr/commit/99964a25ba088c89516866f3c99afd4ff527e3a3))

## [0.3.1](https://github.com/ardent-data/weevr/compare/weevr-v0.3.0...weevr-v0.3.1) (2026-02-21)


### Bug Fixes

* **devcontainer:** update image and add node feature to resolve yarn GPG error ([#16](https://github.com/ardent-data/weevr/issues/16)) ([00a3346](https://github.com/ardent-data/weevr/commit/00a3346e167f38de8dd767cb75e7e3931b246481))

## [0.3.0](https://github.com/ardent-data/weevr/compare/weevr-v0.2.0...weevr-v0.3.0) (2026-02-20)


### Features

* **engine:** add thread execution pipeline with source readers, transforms, and Delta writers ([#13](https://github.com/ardent-data/weevr/issues/13)) ([3fc7857](https://github.com/ardent-data/weevr/commit/3fc78577ead715798471ddc4c17f53fd6e50d44f))

## [0.2.0](https://github.com/ardent-data/weevr/compare/weevr-v0.1.2...weevr-v0.2.0) (2026-02-19)


### Features

* **config:** add error hierarchy and config loading pipeline ([#10](https://github.com/ardent-data/weevr/issues/10)) ([0db6c4f](https://github.com/ardent-data/weevr/commit/0db6c4fefacd82ba76b2507eb59d21d238b8a5d8))
* **model:** add typed domain object model with config hydration ([#11](https://github.com/ardent-data/weevr/issues/11)) ([60fa407](https://github.com/ardent-data/weevr/commit/60fa407eb245f2899bce4a6d8960c22bf70d8f13))

## [0.1.2](https://github.com/ardent-data/weevr/compare/weevr-v0.1.1...weevr-v0.1.2) (2026-02-10)


### Bug Fixes

* **ci:** sync uv.lock version on release ([#7](https://github.com/ardent-data/weevr/issues/7)) ([7e0a45c](https://github.com/ardent-data/weevr/commit/7e0a45c729179aa6c5ba6b47aab4e80d3c72525f))

## [0.1.1](https://github.com/ardent-data/weevr/compare/weevr-v0.1.0...weevr-v0.1.1) (2026-02-10)


### Documentation

* **readme:** align with technical specification ([#5](https://github.com/ardent-data/weevr/issues/5)) ([8df6f8c](https://github.com/ardent-data/weevr/commit/8df6f8c8090c15ddd76ca274b90aee07b44165d3))
