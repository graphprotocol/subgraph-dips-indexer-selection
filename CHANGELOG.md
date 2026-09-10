# Changelog

## [2.9.4](https://github.com/graphprotocol/subgraph-dips-indexer-selection/compare/v2.9.3...v2.9.4) (2026-09-10)


### Changed

* **deps:** bump google/osv-scanner-action/.github/workflows/osv-scanner-reusable.yml ([#232](https://github.com/graphprotocol/subgraph-dips-indexer-selection/issues/232)) ([12d6238](https://github.com/graphprotocol/subgraph-dips-indexer-selection/commit/12d6238e88d982bafc8ffd9672ebac3bebfe5e64))

## [2.9.3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.9.2...v2.9.3) (2026-08-11)


### Fixed

* **deps:** upgrade aiohttp past its 3 known vulnerabilities ([#229](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/229)) ([0b4110b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/0b4110b22bac022b27c7ef78cabdca97148364cb))


### Changed

* **deps:** bump actions/setup-python from 6 to 7 in the actions group ([#226](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/226)) ([07f4ff4](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/07f4ff4fb839da48b60d0c4f11be0729b59d7534))
* **k8s:** update cronjob image to sha-0b4110b ([#230](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/230)) ([76dfef5](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/76dfef593690eb6fccb18f1e40667bb6f7922da3))


### Documentation

* **readme:** add a visual header and at-a-glance table ([#227](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/227)) ([48e2a0a](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/48e2a0a5db9947224b9bf21d8ddbb82c0184d3ca))

## [2.9.2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.9.1...v2.9.2) (2026-07-15)


### Fixed

* **k8s:** pin the service and network policy to the iisa namespace ([#223](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/223)) ([d38e13a](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/d38e13a2db187a8bca4ef7ee977dc811bd1f3a14))

## [2.9.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.9.0...v2.9.1) (2026-07-15)


### Fixed

* **audit:** upgrade the runner's setuptools before running pip-audit ([#221](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/221)) ([ae1c7c5](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/ae1c7c515a5248ef0f9cb5e2ac78ee53e5130b8e))
* **k8s:** let dipper reach IISA from its own namespace ([#220](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/220)) ([8b7591f](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/8b7591f816f09f60715679ba2dd7be3aab28e69a))

## [2.9.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.8.0...v2.9.0) (2026-07-13)


### Added

* expose which indexers accept direct payments ([#214](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/214)) ([01766e7](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/01766e7680ec4e66d4395182d73eb4cae3f58cf5))


### Fixed

* **deps:** upgrade click to 8.4.2 to clear a high-severity advisory ([#219](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/219)) ([2688102](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/26881022ef4bcda68f7cb0da50495b0a8c985afd))


### Changed

* **deps:** bump actions/checkout from 6 to 7 in the actions group ([#217](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/217)) ([57ac3b9](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/57ac3b955f79a69cd058fa37a60a06138512674a))
* **k8s:** update cronjob image to sha-57ac3b9 ([#218](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/218)) ([47eac10](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/47eac10c3296b0df76d9d1708890853dc8377590))

## [2.8.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.7.3...v2.8.0) (2026-06-23)


### Added

* log scoring weights so indexer scores are auditable ([#211](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/211)) ([9633f31](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/9633f31aa03d691750afa93985ac012aa280ed15))


### Changed

* **deps:** bump pydantic-settings to 2.14.2 to fix advisory ([#212](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/212)) ([6166d80](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/6166d80f9783dbc6617963f273d5431f600c9a07))
* **k8s:** update cronjob image to sha-c8c33d4 ([#210](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/210)) ([961e92a](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/961e92a1072bd04d209b0c20e4f59e314ff59405))

## [2.7.3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.7.2...v2.7.3) (2026-06-16)


### Fixed

* **deps:** update starlette to 1.3.1 to clear two known advisories ([#206](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/206)) ([cf59056](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/cf59056b3defca887f36d01495ec0011134cedfb))


### Documentation

* add a README introducing the indexer selection service ([#205](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/205)) ([a36c317](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/a36c317e1bebd367ee54397fd8c27fdc79235571))
* **readme:** correct DIPs name and who funds the agreements ([#208](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/208)) ([1dc5462](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/1dc5462889a8f7781805e7da400b83224329e20c))

## [2.7.2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.7.1...v2.7.2) (2026-06-15)


### Fixed

* **deps:** update aiohttp to 3.14 to clear two known advisories ([#200](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/200)) ([d606f5b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/d606f5bcf9fe0fcd047295f2128cf7dfe02a1a0c))


### Changed

* **k8s:** update cronjob image to sha-117fa5a ([#204](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/204)) ([81b8d7e](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/81b8d7ec6d72540b69c2308fa57e266d6e5c838a))
* **license:** add MIT license and declare it in package metadata ([#198](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/198)) ([70cbfbb](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/70cbfbb1cb73a7fd9a73f870f37dcc87eebd431a))


### Documentation

* **iisa:** describe the push-based scores flow, not the old shared PVC ([#199](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/199)) ([117fa5a](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/117fa5aa6435e4c3d360b9cb77e43083135592e6))


### CI/CD

* **permissions:** restrict the CI workflow token to read-only ([#203](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/203)) ([14eae18](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/14eae1883cdb56477e412c2a6412b63733de5cce))

## [2.7.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.7.0...v2.7.1) (2026-06-08)


### Changed

* **release:** remind to deploy the service after a release lands ([#194](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/194)) ([3946178](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/3946178873d5b303aea4864f04bc9cd267ee82c1))

## [2.7.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.6.1...v2.7.0) (2026-06-03)


### Added

* add bulk endpoint for all indexer weighted scores ([#182](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/182)) ([c729359](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/c72935907c0ef31a2fef1752ac3ca1b6b8dd469a))
* **health:** expose score age and degrade status when scores go stale ([#186](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/186)) ([10627ba](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/10627babc313b950ad91a4e75b27149c9015d6ac))
* **scores:** reject score pushes missing required columns ([#187](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/187)) ([95e6097](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/95e60978c9c236bb5609e9cb12cfb58ef452f8d3))


### Fixed

* **iisa:** make /get-score and /scores/weighted return the same score ([#192](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/192)) ([9c62a36](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/9c62a36c0a959f2f24e1993ed55a5b60d1cd5238))
* **scoring:** log the actual mode in the summary, not the input flag ([#179](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/179)) ([a923c89](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/a923c896cd7d3a4f79e5e966d78835a3c7355605))


### Changed

* **deps:** bump the actions group with 2 updates ([#191](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/191)) ([4b4e78b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/4b4e78bd4717c5ce85e99f4302febd067204b94b))
* **k8s:** update cronjob image to sha-4b4e78b ([#193](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/193)) ([fbb7568](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/fbb7568a83ac3592287369fb9ea3e0519b4bc9e4))
* put score column names in one shared place ([#188](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/188)) ([0f86810](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/0f86810de166a2b8f9bdbb8093e4742928044f49))


### Performance

* compute indexer scores in one pass, not row by row ([#190](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/190)) ([4c3898b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/4c3898bb4e8f8670ce697af502d1613a3f67c355))


### Documentation

* **selection:** trim over-long docstrings and fix two inaccurate ones ([#189](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/189)) ([f8c0612](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f8c0612aa17e82060993fbba33d0d0ccf7a72f83))
* shorten long comments and docstrings in IISA HTTP API ([#183](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/183)) ([075c246](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/075c2463b5cf0182529806be621576f88f3e96e4))

## [2.6.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.6.0...v2.6.1) (2026-05-28)


### Fixed

* **scoring:** fall back to partial when no indexer has a public IP ([#174](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/174)) ([6f5a1d2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/6f5a1d29a5d2c6ec1a430430b963d2a7a0352af6))


### Changed

* **k8s:** update cronjob image to sha-6f5a1d2 ([#178](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/178)) ([51869c5](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/51869c5589eaf896a950b02579fa0d1e33d3c449))
* **k8s:** update cronjob image to sha-e282a73 ([#177](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/177)) ([06a3482](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/06a3482f1e121305610d20c6a4b62935867b0b47))


### Documentation

* **scoring:** trim long docstrings and comment blocks ([#175](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/175)) ([e282a73](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/e282a730fd934f572437a635cbd0e79d433af241))

## [2.6.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.5.2...v2.6.0) (2026-05-27)


### Added

* **scoring:** default the graph-node version filter to strict mode ([#171](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/171)) ([df256a8](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/df256a86d5cb9a179c5bf09df378f8a413f0c9ba))


### Changed

* **k8s:** update cronjob image to sha-df256a8 ([#173](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/173)) ([11eb9c5](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/11eb9c5c074887413ceb4873a3e1d7ce25e26670))

## [2.5.2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.5.1...v2.5.2) (2026-05-26)


### Changed

* **k8s:** add a kustomization for the workload manifests ([#167](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/167)) ([7b498ca](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/7b498ca09736d3a31eddfb4a9c87a674bf0329e5))
* **k8s:** update cronjob image to sha-d928692 ([#170](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/170)) ([cecc571](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/cecc57165d4375b2855d680591cd9dc4d6511fdd))


### CI/CD

* scan dependencies for known vulnerabilities on every PR ([#169](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/169)) ([d928692](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/d9286929d0aa6941e30d1538a7cd2c43a14c64ea))

## [2.5.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.5.0...v2.5.1) (2026-05-26)


### Changed

* **k8s:** update cronjob image to sha-e126be4 ([#166](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/166)) ([3f16736](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/3f167366969ae6e9366f17f10265d7789a44c094))


### CI/CD

* **release:** document merge order on the auto-generated PRs ([#164](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/164)) ([e126be4](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/e126be4adf603d631189c5831a733dbc2f2423ae))

## [2.5.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.4.0...v2.5.0) (2026-05-26)


### Added

* exclude indexers below a minimum graph-node version ([#160](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/160)) ([81353da](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/81353da1c6c5daf940866eb920e8ca3d237ed141))


### Changed

* **k8s:** update cronjob image to sha-81353da ([#163](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/163)) ([94d646a](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/94d646ab1c88d6f7214010d66887a4628bcebe58))

## [2.4.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.3.3...v2.4.0) (2026-05-21)


### Added

* **dips:** tolerate both legacy and flat /dips/info shapes ([#157](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/157)) ([5a2ede2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/5a2ede2146fd91a527419f0f7db0eec7b76eed38))


### Changed

* **k8s:** update cronjob image to sha-5a2ede2 ([#159](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/159)) ([121e3f0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/121e3f085e403a5c2b24f8f2d3cb87970a8a8873))

## [2.3.3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.3.2...v2.3.3) (2026-05-19)


### Changed

* **k8s:** update cronjob image to sha-06472da ([#155](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/155)) ([8cab092](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/8cab0927e5a3d4c54147d5ebc68255cbf47c7109))

## [2.3.2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.3.1...v2.3.2) (2026-05-19)


### Fixed

* protect MaxMind license key and clarify GeoIP errors ([#153](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/153)) ([06472da](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/06472dab3efba87eb5ac7fb53402823b92972309))

## [2.3.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.3.0...v2.3.1) (2026-05-06)


### Changed

* **deps:** bump the actions group with 2 updates ([#148](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/148)) ([b2e4b52](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/b2e4b52a4b3da050d67ad75e9fabf7239eee3aeb))
* **k8s:** update cronjob image to sha-b2e4b52 ([#150](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/150)) ([f24ddb8](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f24ddb82366aed62eb410e789faca78e88b3005f))


### CI/CD

* publish multi-arch (amd64+arm64) service and release images ([#146](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/146)) ([386c54f](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/386c54fcf084f34fec5dc53d386c0f97fa36271d))

## [2.3.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.2.1...v2.3.0) (2026-04-21)


### Added

* **iisa:** add GET /scores endpoint for snapshot read-back ([#144](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/144)) ([7c9e85f](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/7c9e85f89731193199ab863d6baa37f60a93b808))


### Changed

* **cronjob:** make score-computation one-shot, drop HTTP+loop ([#141](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/141)) ([59fc33c](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/59fc33cbd5540c00accd090ee525e1c8f5758e73))
* **k8s:** update cronjob image to sha-59fc33c ([#143](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/143)) ([c865c34](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/c865c34223bcbc636e776e899e63a1f6b4c125bf))

## [2.2.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.2.0...v2.2.1) (2026-04-21)


### Fixed

* **k8s:** allow score-computation cronjob to push scores to iisa ([#139](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/139)) ([5b8f43b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/5b8f43bcc5f39f2849da63ba87563f6f7de93ba7))

## [2.2.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.1.1...v2.2.0) (2026-04-21)


### Added

* **cronjob:** add progress % and ETA to worker heartbeat ([#135](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/135)) ([0625077](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/0625077684f90530c6222042c63c431a2c0aa0f7))


### Changed

* **k8s:** update cronjob image to sha-0625077 ([#138](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/138)) ([053bc03](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/053bc0355b7ca9cd692cfa324dae96fa7fed8741))


### Tests

* **sync-status:** stub _fetch_all_statuses, not asyncio.run ([#136](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/136)) ([af3cbaf](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/af3cbaf827a0f86722ff8a840866c5b86af3b9ca))

## [2.1.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.1.0...v2.1.1) (2026-04-20)


### Changed

* **k8s:** update cronjob image to sha-89cd68e ([#133](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/133)) ([bae3be3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/bae3be37b9e4633a224221dcf5e799c49fb91b5d))

## [2.1.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.0.2...v2.1.0) (2026-04-20)


### Added

* **cronjob:** print 120s progress heartbeat from redpanda workers ([#128](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/128)) ([fa11e28](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/fa11e283db530e9483a644951d4da67d40eee4ff))


### Performance

* **cronjob:** cut sample-worker row memory and bound merge peak ([#130](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/130)) ([89cd68e](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/89cd68e38fc25d58572efd47f11d17bce87d8c89))

## [2.0.2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.0.1...v2.0.2) (2026-04-16)


### Fixed

* **k8s:** add imagePullSecrets to iisa Deployment ([#126](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/126)) ([5e1bc7f](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/5e1bc7fb04a0c14c90d6e3119170b34d43d4e0fc))

## [2.0.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v2.0.0...v2.0.1) (2026-04-14)


### Changed

* **k8s:** update cronjob image to sha-fa79810 ([#125](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/125)) ([328fb13](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/328fb139566e9d9ecc1df3e10f3d0a0cd9835c11))


### CI/CD

* bump deprecated github actions off node 20 runtime ([#123](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/123)) ([fa79810](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/fa7981053a221742459c6609cbb4b85b1dfc931a))

## [2.0.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v1.1.1...v2.0.0) (2026-04-14)


### ⚠ BREAKING CHANGES

* **api:** This consolidates /select-one and /select-many into a single /select-indexers endpoint. The Rust client in dipper-iisa will need updating.

### Added

* add comprehensive logging to selection pipeline ([#67](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/67)) ([abc6d0f](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/abc6d0f8ae106b77e02f994496adf1d025bf4ce4))
* add image tag to docker-compose for local builds ([1d90e58](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/1d90e583750f813db8ee0b12f4943c490a285fab))
* add indexer_scores BigQuery table schema ([#13](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/13)) ([ce4ca2c](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/ce4ca2c53af3421ce6cb4ba198055daeb722d90c))
* add justfile with build-image target ([3ea39ad](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/3ea39ad30f569fd22b73bcf945734a1eb1e5b338))
* add score computation CronJob ([#14](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/14)) ([f6083ad](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f6083add3664ee78530701062a0c9b10d930730f))
* add sync status fetcher service ([#94](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/94)) ([1e799b4](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/1e799b49e3400dece0df4c0ed288130d13de4021))
* add sync status loader and reverse index ([#93](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/93)) ([4622cd7](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/4622cd7611fb6e3db5711eecc90fef743f58b02d))
* **api:** consolidate endpoints, remove acceptance_latency, simplify selection ([#51](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/51)) ([d09d11c](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/d09d11cf93680b338460868b1859f201485f2db5))
* **ci:** add SHA-based image tags with auto-update of k8s yaml ([#42](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/42)) ([c95e2a1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/c95e2a1531d4e60a31fea92faf95767af0c3fa59))
* **ci:** migrate container registry from GCR to Docker Hub ([#31](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/31)) ([590d119](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/590d119384bb52855c2995f4f386d568e3e62dd8))
* compute real scores without GeoIP, fall back to neutral latency only ([#73](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/73)) ([0816f83](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/0816f8307380b61da7adf3cbdde34543154e6364))
* **cronjob:** add fail-fast validation for GeoIP, source data, and URL cache ([#41](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/41)) ([917bdfd](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/917bdfde56b9312ee08a0e114cd866481bf5e72c))
* **cronjob:** migrate from DB-IP to MaxMind GeoLite2 ([#40](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/40)) ([383af92](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/383af921c72e40a47742d6e7f7e91d581a65f9e1))
* deploy score computation CronJob to GKE ([#19](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/19)) ([2c3c5dd](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/2c3c5dda715135dbe6b6cd58d1e78eac92fd0fcb))
* fetch prices from indexers `/dips/info` endpoint and use prices in IISA scoring ([#61](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/61)) ([fe0d3c2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/fe0d3c2894d24dd482932a209e5a102f099a3aba))
* **http:** auto-refresh on startup, remove random fallback ([#49](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/49)) ([1999adf](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/1999adf8d2c155f871c1ae97e074dd5d8614f43c))
* improve pipeline logging and fix lat/lon schema ([#21](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/21)) ([101c3b4](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/101c3b4f83d93ac870e63a0ca831c5bdbb0688f3))
* log selection reasoning for each selected indexer ([#103](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/103)) ([3ccf904](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/3ccf904cb9710899adbfd57f746f410dbc9d5802))
* optimistic DIPs fees in stake_to_fees ([#76](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/76)) ([f9204a5](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f9204a508519e3f4962c51d338b9208391dabf78))
* optional gateway_id filter on Redpanda consumption ([#84](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/84)) ([cbfc1c9](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/cbfc1c91058f6409a420e432d3ddf4f217ee3bab))
* refactor IISA to read pre-computed scores from BigQuery ([#20](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/20)) ([31586fb](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/31586fb7ec540962218b27b55c129a7b3527e713))
* replace BigQuery with Redpanda for score computation ([#59](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/59)) ([30451e2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/30451e268cf1b038d078b6678035e4c3c3116ce5))
* scoring service with degraded mode ([#71](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/71)) ([5c5fa39](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/5c5fa3969e9485233146eb4bf09993d2347181f0))
* two-pool selection preferring synced indexers ([#99](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/99)) ([89c8b5c](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/89c8b5cb9e412f0be68e151869f9c8014689c1ef))
* wire sync status into API and selection endpoint ([#100](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/100)) ([3af9796](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/3af9796cd6d802fc3706dafbd4e726f78723b2b6))


### Fixed

* API blocklist field alignment and logging improvements ([#30](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/30)) ([81979ba](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/81979ba77b8f0a9c648afb82c058d88fcb0c03c9))
* auto-reload scores in IISA API after cronjob writes them ([c3d407a](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/c3d407a88b077a48627be09ab45a2e940fcb794e))
* **ci:** create PR instead of pushing directly to main ([#43](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/43)) ([6ad3995](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/6ad39951844f64da2b1d8a01493104faaf88e63b))
* **ci:** repair YAML block-scalar parse error in sync-manifest action ([#115](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/115)) ([ee7d509](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/ee7d5096ab567fb4347dfb1ee5eeb625b28674d7))
* correct declined_indexers type, bump coverage, expand lint scope ([#110](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/110)) ([560aa92](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/560aa9225918b3e935f2a3697691b2952ed72c17))
* **cronjob:** add fail-fast permission validation ([#34](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/34)) ([d160f78](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/d160f788247b53680f678bd1bacc7298b256eb3a))
* **cronjob:** add fail-fast validation and fix multiple bugs ([#27](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/27)) ([6937d02](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/6937d0220241b2f7b45c17c1d0421f2f3aa99075))
* **cronjob:** add timeout protection and improve logging ([#26](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/26)) ([c221ba8](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/c221ba8c832033f1ff1ffc163aec8508521d0a63))
* **cronjob:** delay bigframes import until after auth setup ([#33](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/33)) ([112cfd3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/112cfd3fedc54afc1079623878eee5209a4b6325))
* **cronjob:** ensure dst_lat/dst_lon are float64 for schema stability ([#45](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/45)) ([2fadc44](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/2fadc44911983c8b657676a04ab77cad1946996c))
* **cronjob:** improve permission error detection ([#38](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/38)) ([237d0d3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/237d0d373bcdeefb297eefc41ea1e51b5f548122))
* **cronjob:** use bigframes.connect() with explicit context ([#32](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/32)) ([60ef8bd](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/60ef8bdc8023ebd265353da0ed2c8a8c0c3b12f2))
* **cronjob:** use explicit service account auth for BigQuery ([b8223b8](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/b8223b84755799bfaae00419b69ad18d99b242cf))
* extend Redpanda consumer window to include today's data ([#82](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/82)) ([ee45bf3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/ee45bf3cece6148ee0eccf530ab3ac03444dd218))
* Get `indexer_url` from new table `metrics_subgraph_gateway_logs` ([#18](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/18)) ([edeb5db](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/edeb5db33b4e568a24d8b3328de84e1d3251790c))
* **k8s:** rename cronjob pull secret to match org convention ([#117](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/117)) ([24a2e78](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/24a2e787f2c814f77c01fd029f317987277084e4))
* make score computation deterministic and add gateway ID filter ([#106](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/106)) ([99e1a03](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/99e1a0364f770487a808d617019491c0a5f8a2ef))
* move score computation cronjob to 09:00 UTC ([#56](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/56)) ([89b3f6e](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/89b3f6e178b4507ce0a91fe5dc46220dc1bcc51b))
* **normalization:** handle NA values with optimistic scoring ([#48](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/48)) ([9675643](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/9675643e005bcb307987964fb84a7e9bc3f4a743)), closes [#47](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/47)
* remove nodeSelector for GKE Autopilot compatibility ([#23](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/23)) ([9a3aa4f](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/9a3aa4f2257920834d0a61bd222837e65b816f46))
* use network subgraph for indexer discovery ([#69](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/69)) ([745b307](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/745b3078250269b9d4978d7e0c4bf7f77c76578f))


### Changed

* **6e:** cleanup, refactor, and flatten IISA ([#24](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/24)) ([b13f83b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/b13f83b4eb4bdb4f7b0445648dca2d6bbdc315d9))
* add missing env vars to K8s manifests ([#85](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/85)) ([13f2cec](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/13f2cec6382513f076029c58b8c6b6fd1a547817))
* **cronjob:** replace ipinfo.io with DB-IP for offline GeoIP ([#29](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/29)) ([355e341](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/355e341384588fb283b1863b0b55dc2b968b85a1)), closes [#16](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/16)
* fix pricing normalisation, removal scoring, and dead IQR code ([#87](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/87)) ([0090a4b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/0090a4bd470fe9f558d43c735692f9471f881301))
* gitignore .claude/ worktree state ([b81a3bb](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/b81a3bbfedc99711e5e1cdcc004a81680a6fd990))
* **k8s:** align cronjob secret refs with graph-infra ([#66](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/66)) ([f283736](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f2837360d0715f76fb0683eec91b2811e28fcfc7))
* **k8s:** update cronjob image to sha-2fadc44 ([#46](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/46)) ([5ed4e72](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/5ed4e72408293bf76e1f0be7f22b99ad9e68551e))
* **k8s:** update cronjob image to sha-6ad3995 ([#44](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/44)) ([a61558e](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/a61558eb6fa931480b6d574b27ccb8bcdb1b9b32))
* **k8s:** update cronjob image to sha-d09d11c ([#54](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/54)) ([c61e201](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/c61e20134dba15ba200f02cd1312b54ee8d2a85b))
* push scores to iisa over HTTP, drop shared Filestore PVC ([#119](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/119)) ([57a965b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/57a965b4e669ce5057f5a0faa4310fab81dfb221))
* remove 3 skipped flaky tests ([#101](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/101)) ([a2f805c](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/a2f805c8f24bd79692cf34e93809005955b38a50))
* remove dead avg_sync_duration column ([#89](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/89)) ([679a302](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/679a3025380507f29ec2000b19040904dc7c0fe6))
* remove dead test code for existing_dips_agreements and orphaned methods ([#80](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/80)) ([2cfc036](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/2cfc0362b847eddc1c7244bde3024a4e7d491e8c))
* remove IQR deviation from stake_to_fees scoring ([#78](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/78)) ([fa4ad70](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/fa4ad70891d071a1b6d4dae7e0ae463efe7b240b))
* rename _notify_iisa_refresh to _refresh_iisa_scores ([e17881b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/e17881bc0a1fd20473d9d236012e597800057772))
* use GRT per billion entities instead of per million ([#64](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/64)) ([d13fbe8](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/d13fbe856fcf55c33764746082c7b7b2f2b8e919))


### Performance

* use ProcessPoolExecutor for parallel partition consumption ([#97](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/97)) ([818a470](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/818a470644908aa2b68b058d6a38e05858982679))


### Tests

* add coverage for pricing extraction and filtering functions ([#104](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/104)) ([f0e9284](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f0e9284063881f99d94d087df273d679302ab671))
* **iisa:** add comprehensive tests for iisa_http_endpoints.py ([#35](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/35)) ([4c39404](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/4c39404acb18e51ab11d25e20a4a44b21e110e7a)), closes [#25](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/25)


### CI/CD

* add GHCR workflow for IISA service image ([#63](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/63)) ([bdfac35](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/bdfac35ca9c57fb466ba7ba9c0da66eec2a7dbbd))
* add mypy type checking to CI workflow ([#107](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/107)) ([7ad8e2a](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/7ad8e2a0cae1f4b70123f2e6acff34c186114f93))
* bridge release-please releases to build-release-image via dispatch ([#122](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/122)) ([d652289](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/d652289b72372fe5f02c7501385d180b9d4037d6))
* publish versioned iisa image on v* tag push ([#121](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/121)) ([0b034c1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/0b034c1e611aaf0f7ee2fef5d60b04f02460c41a))
* rename docker images to match repo name and move cronjob to GHCR ([#113](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/113)) ([3e530ce](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/3e530ceb402ff9d79eee8fd26eef04b027db697a))
* use github.repository for image name ([f40e347](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f40e347b1f43d0312d3581797cbc62b8f06d1c9b))

## [1.1.1](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v1.1.0...v1.1.1) (2026-01-14)


### Fixed

* **ci:** prevent sync-manifest from downgrading manifest version ([ac8954c](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/ac8954c5b3b17147996819bceca301cb22261031))


### Changed

* auto-sync manifest to 1.0.0 (was 1.1.0) [skip ci] ([2cd2c2d](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/2cd2c2da0e66c801b835c4eb6592959ff8232b78))
* auto-sync manifest to 1.1.0 (was 1.0.0) [skip ci] ([7a7b182](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/7a7b1828fef3e41d82ac6f4c0e6d24d1a76a895b))

## [1.1.0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/compare/v1.0.0...v1.1.0) (2026-01-14)


### Added

* **api:** add blocklist and declined_indexers to selection request ([#8](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/8)) ([0092520](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/0092520f45dd745477b39f6c2a20fb8c5e7c7688))


### Fixed

* **dev:** add missing test dependencies ([#7](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/7)) ([885d70b](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/885d70bfce216de3f0f01b0a2927cd5e94893a11))


### Changed

* auto-sync manifest to 0.0.0 (was 1.0.0) [skip ci] ([ce65387](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/ce65387bbe94338b5bf3f05d67ac8894f443c8a6))
* auto-sync manifest to 1.0.0 (was 0.0.0) [skip ci] ([ed63176](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/ed6317609cf61f586f61bacd8f1ba9aa9292b572))

## 1.0.0 (2026-01-13)


### Added

* **iisa-service:** add FastAPI HTTP service ([bdd4d4e](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/bdd4d4e08eefe144e54110d4c923f890f8f0236f))
* **iisa:** add core IISA library ([1c426d0](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/1c426d06b39ee1dbef30b88659ef5f2b8be4ff56))


### Changed

* auto-sync manifest to 0.0.0 (was 0.1.0) [skip ci] ([9f97100](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/9f9710050b0b42b436ee2d535e2c9c061fe977e6))
* initial project setup and containerization ([f6bffb4](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/f6bffb484acc1ca3323a08fdc5f78fec56821ac4))


### Infrastructure

* **k8s:** add Kubernetes deployment manifests for IISA ([#2](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/2)) ([51ef538](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/51ef53897f9a2ff6a3cc6cac8b2266b3d3a9fb94))


### Tests

* add comprehensive test suite ([b76e390](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/b76e3907a1e19ce95c4c188a5d8e3146a544b341))


### CI/CD

* **release:** add release-please for automated releases ([#3](https://github.com/edgeandnode/subgraph-dips-indexer-selection/issues/3)) ([a4d015c](https://github.com/edgeandnode/subgraph-dips-indexer-selection/commit/a4d015c57620e3107de0cad0ba29da23cc533714))
