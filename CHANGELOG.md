# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Fix shell reuse collision across execution locations ([#1045](https://github.com/alpha-unito/streamflow/pull/1045))
- Fix operators in `Hardware` and `Storage` ([#1044](https://github.com/alpha-unito/streamflow/pull/1044))
- Fix Docker Compose NDJSON location parsing ([#1043](https://github.com/alpha-unito/streamflow/pull/1043))

### Removed

- Remove `OccamConnector` and all associated files ([#1041](https://github.com/alpha-unito/streamflow/pull/1041))

## [0.2.0rc2] - 2026-04-23

### Added

- Add `CHANGELOG.md` and changelog update skill ([#1037](https://github.com/alpha-unito/streamflow/pull/1037))
- Add AI agent guidelines via `AGENTS.md` and task-specific skills ([#937](https://github.com/alpha-unito/streamflow/pull/937))

### Changed

- Improve `StreamFlowExecutor` termination protocol ([#1000](https://github.com/alpha-unito/streamflow/pull/1000))
- Improve save protocol and SQLite pragmas ([#1036](https://github.com/alpha-unito/streamflow/pull/1036))
- Refactor `load` API to drop the `context` parameter ([#1035](https://github.com/alpha-unito/streamflow/pull/1035))
- Refactor `save` API to take a `Database` argument ([#1034](https://github.com/alpha-unito/streamflow/pull/1034))
- Migrate linting from `flake8`+`isort` to `ruff` ([#1033](https://github.com/alpha-unito/streamflow/pull/1033))
- Make dependabot ignore `setuptools` and `wheel` ([#1013](https://github.com/alpha-unito/streamflow/pull/1013))

### Fixed

- Fix root folder always added to hardware storage ([#1040](https://github.com/alpha-unito/streamflow/pull/1040))
- Correctly skip nested subworkflows ([#1019](https://github.com/alpha-unito/streamflow/pull/1019))
- Fix container `mount` parsing ([#1032](https://github.com/alpha-unito/streamflow/pull/1032))
- Fix lock releasing logic in scheduler ([#1031](https://github.com/alpha-unito/streamflow/pull/1031))

### Dependencies

- Bump aiohttp from 3.13.4 to 3.13.5 ([#1015](https://github.com/alpha-unito/streamflow/pull/1015))
- Bump cachebox from 5.2.2 to 5.2.3 ([#1027](https://github.com/alpha-unito/streamflow/pull/1027))
- Bump cryptography from 46.0.6 to 46.0.7 ([#1025](https://github.com/alpha-unito/streamflow/pull/1025))
- Bump cwl-utils from 0.40 to 0.41 ([#1012](https://github.com/alpha-unito/streamflow/pull/1012))
- Bump lxml from 6.0.2 to 6.1.0 ([#1038](https://github.com/alpha-unito/streamflow/pull/1038))
- Bump plotly from 6.6.0 to 6.7.0 ([#1026](https://github.com/alpha-unito/streamflow/pull/1026))

### Dev Dependencies

- Bump cwltool from 3.1.20260315121657 to 3.2.20260413085819 ([#1028](https://github.com/alpha-unito/streamflow/pull/1028), [#1030](https://github.com/alpha-unito/streamflow/pull/1030))
- Bump mypy from 1.19.1 to 1.20.2 ([#1014](https://github.com/alpha-unito/streamflow/pull/1014), [#1029](https://github.com/alpha-unito/streamflow/pull/1029), [#1039](https://github.com/alpha-unito/streamflow/pull/1039)
- Bump pytest from 9.0.2 to 9.0.3 ([#1021](https://github.com/alpha-unito/streamflow/pull/1021))
- Bump types-antlr4-python3-runtime ([#1017](https://github.com/alpha-unito/streamflow/pull/1017), [#1020](https://github.com/alpha-unito/streamflow/pull/1020))
- Bump types-cachetools from 6.2.0.20260317 to 6.2.0.20260408 ([#1022](https://github.com/alpha-unito/streamflow/pull/1022))
- Bump types-jsonschema from 4.26.0.20260325 to 4.26.0.20260408 ([#1016](https://github.com/alpha-unito/streamflow/pull/1016), [#1024](https://github.com/alpha-unito/streamflow/pull/1024))
- Bump types-psutil from 7.2.2.20260130 to 7.2.2.20260408 ([#1018](https://github.com/alpha-unito/streamflow/pull/1018), [#1023](https://github.com/alpha-unito/streamflow/pull/1023))

## [0.2.0rc1] - 2026-03-31

### Added

- Implement `__hash__` method in the `ExecutionLocation` ([#933](https://github.com/alpha-unito/streamflow/pull/933))
- Added `restore` method ([#864](https://github.com/alpha-unito/streamflow/pull/864))
- Added type checking with `mypy` ([#938](https://github.com/alpha-unito/streamflow/pull/938))
- Add `UnrecoverableWorkflowException` ([#954](https://github.com/alpha-unito/streamflow/pull/954))
- Implement persistent shells for improved performance ([#960](https://github.com/alpha-unito/streamflow/pull/960))
- Support files deduplication in RunCrate ([#978](https://github.com/alpha-unito/streamflow/pull/978))
- Added `--validate` flag to `streamflow run` ([#995](https://github.com/alpha-unito/streamflow/pull/995))
- Add tag to `JobToken` ([#999](https://github.com/alpha-unito/streamflow/pull/999))
- Add `Status.RECOVERED` ([#958](https://github.com/alpha-unito/streamflow/pull/958))
- Implemented template validation in the `CommandTemplateMap` class ([#916](https://github.com/alpha-unito/streamflow/pull/916))
- Add `FaultTolerance` documentation ([#1004](https://github.com/alpha-unito/streamflow/pull/1004))
- Validate CWL `when` condition dependencies ([#825](https://github.com/alpha-unito/streamflow/pull/825))

### Changed

- Refactor `DirectedGraph` class ([#919](https://github.com/alpha-unito/streamflow/pull/919))
- Migrate from `cachetools` to `cachebox` ([#945](https://github.com/alpha-unito/streamflow/pull/945))
- Substitute manual quoting with `shlex` and `mslex` ([#946](https://github.com/alpha-unito/streamflow/pull/946))
- Improved `InterWorkflowPort` ([#951](https://github.com/alpha-unito/streamflow/pull/951))
- Use official Docker setup action ([#956](https://github.com/alpha-unito/streamflow/pull/956))
- Improve `LoopCombinator` ([#952](https://github.com/alpha-unito/streamflow/pull/952))
- Improve `RemotePathMapper` performance ([#970](https://github.com/alpha-unito/streamflow/pull/970))
- Adjust the `get_path_processor` method ([#973](https://github.com/alpha-unito/streamflow/pull/973))
- Optimize SQLite performance and concurrency ([#974](https://github.com/alpha-unito/streamflow/pull/974))
- Improve `LocalStreamFlowPath.checksum()` performance ([#975](https://github.com/alpha-unito/streamflow/pull/975))
- Change propagation condition of `InterWorkflowPort` ([#1001](https://github.com/alpha-unito/streamflow/pull/1001))
- Propagate mounts to wrapped locations in `QueueManagerConnector` ([#947](https://github.com/alpha-unito/streamflow/pull/947))

### Fixed

- Fix `SSHContextFactory` retrieval logic ([#931](https://github.com/alpha-unito/streamflow/pull/931))
- Fix `uv pip uninstall` in cwl conformance tests script ([#934](https://github.com/alpha-unito/streamflow/pull/934))
- Fix trivial mypy errors ([#943](https://github.com/alpha-unito/streamflow/pull/943))
- Fix trivial mypy `[var-annotated]` errors ([#944](https://github.com/alpha-unito/streamflow/pull/944))
- Fix `StreamFlowPath` comparison ([#948](https://github.com/alpha-unito/streamflow/pull/948))
- Fix optional empty list output in CWL ([#959](https://github.com/alpha-unito/streamflow/pull/959))
- Close `Shell` if an error occurs ([#962](https://github.com/alpha-unito/streamflow/pull/962))
- Fix database relative path ([#969](https://github.com/alpha-unito/streamflow/pull/969))
- Fix provenance with multiple equal files ([#988](https://github.com/alpha-unito/streamflow/pull/988))
- Fix empty glob ([#989](https://github.com/alpha-unito/streamflow/pull/989))
- Fixed recovery of loops ([#826](https://github.com/alpha-unito/streamflow/pull/826))
- Fixed synchronization of multiple recovery workflows ([#822](https://github.com/alpha-unito/streamflow/pull/822))
- Fix `notify` of stacked locations ([#1006](https://github.com/alpha-unito/streamflow/pull/1006))
- Fixed retrieving disk information in `SingularityConnector` ([#894](https://github.com/alpha-unito/streamflow/pull/894))

### Dependencies

- Bump aiohttp from 3.13.3 to 3.13.4 ([#1010](https://github.com/alpha-unito/streamflow/pull/1010))
- Bump cachetools from 6.2.4 to 7.0.0 ([#932](https://github.com/alpha-unito/streamflow/pull/932), [#936](https://github.com/alpha-unito/streamflow/pull/936))
- Bump cryptography from 46.0.3 to 46.0.6 ([#949](https://github.com/alpha-unito/streamflow/pull/949), [#1009](https://github.com/alpha-unito/streamflow/pull/1009))
- Bump importlib-metadata from 8.7.1 to 9.0.0 ([#996](https://github.com/alpha-unito/streamflow/pull/996), [#997](https://github.com/alpha-unito/streamflow/pull/997))
- Bump kubernetes-asyncio from 34.3.3 to 35.0.1 ([#953](https://github.com/alpha-unito/streamflow/pull/953), [#967](https://github.com/alpha-unito/streamflow/pull/967))
- Bump orjson from 3.11.5 to 3.11.6 ([#991](https://github.com/alpha-unito/streamflow/pull/991))
- Bump plotly from 6.5.2 to 6.6.0 ([#980](https://github.com/alpha-unito/streamflow/pull/980))
- Bump psutil from 7.2.1 to 7.2.2 ([#935](https://github.com/alpha-unito/streamflow/pull/935))
- Bump pygments from 2.19.2 to 2.20.0 ([#1011](https://github.com/alpha-unito/streamflow/pull/1011))
- Bump rdflib from 7.5.0 to 7.6.0 ([#993](https://github.com/alpha-unito/streamflow/pull/993))
- Bump requests from 2.32.5 to 2.33.0 ([#1007](https://github.com/alpha-unito/streamflow/pull/1007))

### Dev Dependencies

- Bump actions/download-artifact from 7 to 8 ([#971](https://github.com/alpha-unito/streamflow/pull/971))
- Bump actions/upload-artifact from 6 to 7 ([#972](https://github.com/alpha-unito/streamflow/pull/972))
- Bump bandit from 1.9.3 to 1.9.4 ([#966](https://github.com/alpha-unito/streamflow/pull/966))
- Bump black from 26.1.0 to 26.3.1 ([#987](https://github.com/alpha-unito/streamflow/pull/987), [#990](https://github.com/alpha-unito/streamflow/pull/990))
- Bump codecov/codecov-action from 5 to 6 ([#1008](https://github.com/alpha-unito/streamflow/pull/1008))
- Bump codespell from 2.4.1 to 2.4.2 ([#986](https://github.com/alpha-unito/streamflow/pull/986))
- Bump cwltool from 3.1.20260108082145 to 3.1.20260315121657 ([#992](https://github.com/alpha-unito/streamflow/pull/992))
- Bump docker/build-push-action from 6 to 7 ([#985](https://github.com/alpha-unito/streamflow/pull/985))
- Bump docker/login-action from 3 to 4 ([#983](https://github.com/alpha-unito/streamflow/pull/983))
- Bump docker/setup-buildx-action from 3 to 4 ([#984](https://github.com/alpha-unito/streamflow/pull/984))
- Bump docker/setup-compose-action from 1 to 2 ([#976](https://github.com/alpha-unito/streamflow/pull/976))
- Bump docker/setup-docker-action from 4 to 5 ([#979](https://github.com/alpha-unito/streamflow/pull/979))
- Bump docker/setup-qemu-action from 3 to 4 ([#982](https://github.com/alpha-unito/streamflow/pull/982))
- Bump helm/kind-action from 1.13.0 to 1.14.0 ([#957](https://github.com/alpha-unito/streamflow/pull/957))
- Bump isort from 7.0.0 to 8.0.1 ([#961](https://github.com/alpha-unito/streamflow/pull/961), [#977](https://github.com/alpha-unito/streamflow/pull/977))
- Bump mypy from 1.19.0 to 1.19.1 ([#940](https://github.com/alpha-unito/streamflow/pull/940))
- Bump pandas-stubs from 2.3.3.251201 to 2.3.3.260113 ([#942](https://github.com/alpha-unito/streamflow/pull/942))
- Bump pytest-cov from 7.0.0 to 7.1.0 ([#1002](https://github.com/alpha-unito/streamflow/pull/1002))
- Bump types-cachetools from 6.2.0.20251022 to 6.2.0.20260317 ([#994](https://github.com/alpha-unito/streamflow/pull/994))
- Bump types-jsonschema from 4.25.1.20251009 to 4.26.0.20260325 ([#941](https://github.com/alpha-unito/streamflow/pull/941), [#1003](https://github.com/alpha-unito/streamflow/pull/1003), [#1005](https://github.com/alpha-unito/streamflow/pull/1005))
- Bump types-psutil from 7.1.3.20251202 to 7.2.2.20260130 ([#939](https://github.com/alpha-unito/streamflow/pull/939))

## [0.2.0.dev14] - 2026-01-24

### Breaking Changes

- Drop support for Python 3.9 ([#827](https://github.com/alpha-unito/streamflow/pull/827))

### Added

- Add support for CWL v1.3 ([#802](https://github.com/alpha-unito/streamflow/pull/802))
- Add support for Python 3.14 ([#820](https://github.com/alpha-unito/streamflow/pull/820))
- Added checks for CWL feature requirement ([#866](https://github.com/alpha-unito/streamflow/pull/866))
- Added `MatchingBindingFilter` ([#872](https://github.com/alpha-unito/streamflow/pull/872))

### Changed

- Refactor processors' logic for async execution ([#789](https://github.com/alpha-unito/streamflow/pull/789))
- Make `CommandOutputProcessor` handle `Future` objs ([#790](https://github.com/alpha-unito/streamflow/pull/790))
- Refactor `TokenProcessor` and `CommandOutputProcessor` classes ([#793](https://github.com/alpha-unito/streamflow/pull/793))
- Update versions in CI/CD pipeline ([#806](https://github.com/alpha-unito/streamflow/pull/806))
- Refactor exit code evaluation ([#799](https://github.com/alpha-unito/streamflow/pull/799))
- Added a check for the chosen deployment in the tests ([#814](https://github.com/alpha-unito/streamflow/pull/814))
- Refactor handling of `File` and `Directory` literals ([#811](https://github.com/alpha-unito/streamflow/pull/811))
- Remove `disable-content-trust` in DockerConnector ([#856](https://github.com/alpha-unito/streamflow/pull/856))
- Remove Lima version from MacOS CI ([#859](https://github.com/alpha-unito/streamflow/pull/859))
- Remove `TransferStep` from CWL `ExpressionTool` ([#860](https://github.com/alpha-unito/streamflow/pull/860))
- Adjust Python cache in GitHub CI/CD ([#865](https://github.com/alpha-unito/streamflow/pull/865))
- Update MacOS X CI/CD to version 15 ([#819](https://github.com/alpha-unito/streamflow/pull/819))
- Reduced overhead of the `transfer_data` method ([#883](https://github.com/alpha-unito/streamflow/pull/883))
- Revert `sphinx` to `v8.1.3` ([#888](https://github.com/alpha-unito/streamflow/pull/888))
- Migrate to `uv` package manager ([#889](https://github.com/alpha-unito/streamflow/pull/889))
- Respect available CPUs in `LocalConnector` ([#896](https://github.com/alpha-unito/streamflow/pull/896))

### Fixed

- Fixed issues related to `default` values in CWL ([#808](https://github.com/alpha-unito/streamflow/pull/808))
- Fixed `recoverable` attribute in the `ListToken` and `ObjectToken` classes ([#752](https://github.com/alpha-unito/streamflow/pull/752))
- Fixed issue on multiple `exampleOfWork` on `RunCrate` provenance ([#841](https://github.com/alpha-unito/streamflow/pull/841))
- Fixed issue in `DependencyResolver` ([#839](https://github.com/alpha-unito/streamflow/pull/839))
- Fix Lima version to v1.2.2 ([#850](https://github.com/alpha-unito/streamflow/pull/850))
- Fixed `loadContents` in the collector step ([#846](https://github.com/alpha-unito/streamflow/pull/846))
- Fix race condition with output unique paths ([#863](https://github.com/alpha-unito/streamflow/pull/863))
- Fix minor issues on recovery ([#862](https://github.com/alpha-unito/streamflow/pull/862))
- Fixed `CWLObjectCommandOutputProcessor` to handle arrays of records ([#858](https://github.com/alpha-unito/streamflow/pull/858))
- Fixed `eval` method of the `DependencyResolver` ([#867](https://github.com/alpha-unito/streamflow/pull/867))
- Handle location storage retrieval errors ([#871](https://github.com/alpha-unito/streamflow/pull/871))
- Fix `StreamFlowContext` ([#880](https://github.com/alpha-unito/streamflow/pull/880))
- Fixed directory registration ([#881](https://github.com/alpha-unito/streamflow/pull/881))
- Fixed type checking in `CWLCommandTokenProcessor` ([#895](https://github.com/alpha-unito/streamflow/pull/895))
- Fix issues in the `ValueFromTransformer` and `LoopValueFromTransformer` classes ([#882](https://github.com/alpha-unito/streamflow/pull/882))
- Fixed `StreamFlowContext` creation ([#901](https://github.com/alpha-unito/streamflow/pull/901))
- Fixed transfer of `hardlink` ([#891](https://github.com/alpha-unito/streamflow/pull/891))
- Fixed `ExpressionTool` outputs processing ([#902](https://github.com/alpha-unito/streamflow/pull/902))
- Fix `cwl.output.json` behaviour ([#851](https://github.com/alpha-unito/streamflow/pull/851))
- Fix `ext show` command ([#927](https://github.com/alpha-unito/streamflow/pull/927))

### Dependencies

- Bump aiohttp from 3.12.15 to 3.13.2 ([#821](https://github.com/alpha-unito/streamflow/pull/821), [#832](https://github.com/alpha-unito/streamflow/pull/832), [#840](https://github.com/alpha-unito/streamflow/pull/840))
- Bump aiosqlite from 0.21.0 to 0.22.1 ([#898](https://github.com/alpha-unito/streamflow/pull/898), [#906](https://github.com/alpha-unito/streamflow/pull/906))
- Bump asyncssh from 2.21.0 to 2.22.0 ([#815](https://github.com/alpha-unito/streamflow/pull/815), [#918](https://github.com/alpha-unito/streamflow/pull/918))
- Bump bcrypt from 4.3.0 to 5.0.0 ([#812](https://github.com/alpha-unito/streamflow/pull/812))
- Bump cachetools from 6.1.0 to 6.2.3 ([#792](https://github.com/alpha-unito/streamflow/pull/792), [#828](https://github.com/alpha-unito/streamflow/pull/828), [#861](https://github.com/alpha-unito/streamflow/pull/861), [#897](https://github.com/alpha-unito/streamflow/pull/897))
- Bump cwl-utils from 0.39 to 0.40 ([#803](https://github.com/alpha-unito/streamflow/pull/803))
- Bump filelock from 3.20.0 to 3.20.3 ([#917](https://github.com/alpha-unito/streamflow/pull/917))
- Bump importlib-metadata from 8.7.0 to 8.7.1 ([#907](https://github.com/alpha-unito/streamflow/pull/907))
- Bump jsonschema from 4.25.0 to 4.26.0 ([#786](https://github.com/alpha-unito/streamflow/pull/786), [#923](https://github.com/alpha-unito/streamflow/pull/923))
- Bump kaleido from 1.0.0 to 1.2.0 ([#804](https://github.com/alpha-unito/streamflow/pull/804), [#848](https://github.com/alpha-unito/streamflow/pull/848))
- Bump kubernetes-asyncio from 32.3.2 to 34.3.3 ([#783](https://github.com/alpha-unito/streamflow/pull/783), [#915](https://github.com/alpha-unito/streamflow/pull/915))
- Bump pandas from 2.3.1 to 2.3.3 ([#788](https://github.com/alpha-unito/streamflow/pull/788), [#816](https://github.com/alpha-unito/streamflow/pull/816))
- Bump plotly from 6.2.0 to 6.5.2 ([#784](https://github.com/alpha-unito/streamflow/pull/784), [#818](https://github.com/alpha-unito/streamflow/pull/818), [#847](https://github.com/alpha-unito/streamflow/pull/847), [#869](https://github.com/alpha-unito/streamflow/pull/869), [#905](https://github.com/alpha-unito/streamflow/pull/905), [#920](https://github.com/alpha-unito/streamflow/pull/920))
- Bump psutil from 7.0.0 to 7.2.1 ([#807](https://github.com/alpha-unito/streamflow/pull/807), [#833](https://github.com/alpha-unito/streamflow/pull/833), [#837](https://github.com/alpha-unito/streamflow/pull/837), [#844](https://github.com/alpha-unito/streamflow/pull/844), [#914](https://github.com/alpha-unito/streamflow/pull/914))
- Bump rdflib from 7.1.4 to 7.5.0 ([#809](https://github.com/alpha-unito/streamflow/pull/809), [#842](https://github.com/alpha-unito/streamflow/pull/842), [#877](https://github.com/alpha-unito/streamflow/pull/877))
- Bump referencing from 0.36.2 to 0.37.0 ([#830](https://github.com/alpha-unito/streamflow/pull/830))
- Bump typing-extensions from 4.14.1 to 4.15.0 ([#791](https://github.com/alpha-unito/streamflow/pull/791))
- Bump urllib3 from 2.6.1 to 2.6.3 ([#921](https://github.com/alpha-unito/streamflow/pull/921))

### Dev Dependencies

- Bump actions/checkout from 4 to 6 ([#782](https://github.com/alpha-unito/streamflow/pull/782), [#874](https://github.com/alpha-unito/streamflow/pull/874))
- Bump actions/download-artifact from 4 to 7 ([#780](https://github.com/alpha-unito/streamflow/pull/780), [#836](https://github.com/alpha-unito/streamflow/pull/836), [#899](https://github.com/alpha-unito/streamflow/pull/899))
- Bump actions/setup-node from 4 to 6 ([#796](https://github.com/alpha-unito/streamflow/pull/796), [#831](https://github.com/alpha-unito/streamflow/pull/831))
- Bump actions/setup-python from 5 to 6 ([#795](https://github.com/alpha-unito/streamflow/pull/795))
- Bump actions/upload-artifact from 4 to 6 ([#838](https://github.com/alpha-unito/streamflow/pull/838), [#900](https://github.com/alpha-unito/streamflow/pull/900))
- Bump bandit from 1.8.6 to 1.9.3 ([#868](https://github.com/alpha-unito/streamflow/pull/868), [#875](https://github.com/alpha-unito/streamflow/pull/875), [#924](https://github.com/alpha-unito/streamflow/pull/924))
- Bump black from 25.1.0 to 26.1.0 ([#810](https://github.com/alpha-unito/streamflow/pull/810), [#854](https://github.com/alpha-unito/streamflow/pull/854), [#886](https://github.com/alpha-unito/streamflow/pull/886), [#925](https://github.com/alpha-unito/streamflow/pull/925))
- Bump cwltest from 2.6.20250314152537 to 2.6.20251216093331 ([#785](https://github.com/alpha-unito/streamflow/pull/785), [#909](https://github.com/alpha-unito/streamflow/pull/909))
- Bump cwltool from 3.1.20250715140722 to 3.1.20260108082145 ([#813](https://github.com/alpha-unito/streamflow/pull/813), [#843](https://github.com/alpha-unito/streamflow/pull/843), [#911](https://github.com/alpha-unito/streamflow/pull/911))
- Bump flake8-bugbear from 24.12.12 to 25.11.29 ([#834](https://github.com/alpha-unito/streamflow/pull/834), [#879](https://github.com/alpha-unito/streamflow/pull/879))
- Bump github/codeql-action from 3 to 4 ([#823](https://github.com/alpha-unito/streamflow/pull/823))
- Bump helm/kind-action from 1.12.0 to 1.13.0 ([#845](https://github.com/alpha-unito/streamflow/pull/845))
- Bump isort from 6.0.1 to 7.0.0 ([#817](https://github.com/alpha-unito/streamflow/pull/817), [#829](https://github.com/alpha-unito/streamflow/pull/829))
- Bump mukunku/tag-exists-action from 1.6.0 to 1.7.0 ([#890](https://github.com/alpha-unito/streamflow/pull/890))
- Bump pytest from 8.4.1 to 9.0.2 ([#797](https://github.com/alpha-unito/streamflow/pull/797), [#887](https://github.com/alpha-unito/streamflow/pull/887))
- Bump pytest-asyncio from 1.1.0 to 1.3.0 ([#805](https://github.com/alpha-unito/streamflow/pull/805), [#855](https://github.com/alpha-unito/streamflow/pull/855))
- Bump pytest-cov from 6.2.1 to 7.0.0 ([#801](https://github.com/alpha-unito/streamflow/pull/801))
- Bump pyupgrade from 3.20.0 to 3.21.2 ([#824](https://github.com/alpha-unito/streamflow/pull/824), [#852](https://github.com/alpha-unito/streamflow/pull/852), [#870](https://github.com/alpha-unito/streamflow/pull/870))
- Bump sphinx-jsonschema from 1.19.1 to 1.19.2 ([#878](https://github.com/alpha-unito/streamflow/pull/878))
- Bump sphinx-rtd-theme from 3.0.2 to 3.1.0 ([#928](https://github.com/alpha-unito/streamflow/pull/928))
- Bump sphinx-llms-txt from 0.3.1 to 0.7.1 ([#929](https://github.com/alpha-unito/streamflow/pull/929))

## [0.2.0.dev13] - 2025-08-07

### Added

- Added `ROLLBACK` status ([#697](https://github.com/alpha-unito/streamflow/pull/697))
- Added `recoverable` attribute to the `Token` object in the `FailureManager` ([#699](https://github.com/alpha-unito/streamflow/pull/699))
- Implemented fault tolerance based on `Database` module ([#169](https://github.com/alpha-unito/streamflow/pull/169))
- Check if bindings exist in the workflow ([#708](https://github.com/alpha-unito/streamflow/pull/708))
- Added `llms.txt` to StreamFlow documentation ([#754](https://github.com/alpha-unito/streamflow/pull/754))

### Changed

- Refactor private functions ([#684](https://github.com/alpha-unito/streamflow/pull/684))
- Always wait for `DataLocation` to be `available` ([#690](https://github.com/alpha-unito/streamflow/pull/690))
- Refactor unit tests ([#691](https://github.com/alpha-unito/streamflow/pull/691))
- Improved `WorkflowBuilder` ([#692](https://github.com/alpha-unito/streamflow/pull/692))
- Improved `is_available` method of the `FileToken` class ([#696](https://github.com/alpha-unito/streamflow/pull/696))
- Refactored `TransferStep` ([#698](https://github.com/alpha-unito/streamflow/pull/698))
- Refactored the no-cycles check for stacked deployment definitions ([#705](https://github.com/alpha-unito/streamflow/pull/705))
- Improved handling of `File` literals ([#693](https://github.com/alpha-unito/streamflow/pull/693))
- Improve JSONSchema specification ([#745](https://github.com/alpha-unito/streamflow/pull/745))
- Add a check to verify object instantiation in persistence tests ([#741](https://github.com/alpha-unito/streamflow/pull/741))
- Refactor the invocation of `FailureManager` within the `Step` ([#753](https://github.com/alpha-unito/streamflow/pull/753))
- Improved token processing logic in `CWLObjectTokenProcessor` ([#758](https://github.com/alpha-unito/streamflow/pull/758))
- Make `Scheduler` helper methods raise exceptions ([#759](https://github.com/alpha-unito/streamflow/pull/759))
- Improve StreamFlow type system ([#762](https://github.com/alpha-unito/streamflow/pull/762))
- Make `StreamFlowContext` constructor self-contained ([#767](https://github.com/alpha-unito/streamflow/pull/767))
- Improved `Combinator` persistence tests ([#764](https://github.com/alpha-unito/streamflow/pull/764))
- Improved provenance of the task executions ([#772](https://github.com/alpha-unito/streamflow/pull/772))
- Remove support for nested `Token` objects ([#777](https://github.com/alpha-unito/streamflow/pull/777))

### Fixed

- Fix requirement propagation in `CWLTranslator` ([#688](https://github.com/alpha-unito/streamflow/pull/688))
- Fix concurrent output retrieval ([#689](https://github.com/alpha-unito/streamflow/pull/689))
- Fixed cleanup in unit tests ([#695](https://github.com/alpha-unito/streamflow/pull/695))
- Fixed `invalidate_location` method ([#694](https://github.com/alpha-unito/streamflow/pull/694))
- Fixed `CloneTransformer` persistence ([#719](https://github.com/alpha-unito/streamflow/pull/719))
- Fix `InitialWorkDirRequirement` for nested dirs ([#735](https://github.com/alpha-unito/streamflow/pull/735))
- Capture `TimeoutError` when retrieving disk usage ([#737](https://github.com/alpha-unito/streamflow/pull/737))
- Fixed persistence of `CWLCommandOutputProcessor` classes ([#740](https://github.com/alpha-unito/streamflow/pull/740))
- Fixed preparation of workdir for list and object inputs ([#732](https://github.com/alpha-unito/streamflow/pull/732))
- Fixed tag sorting in the `GatherStep` ([#761](https://github.com/alpha-unito/streamflow/pull/761))
- Fixed `CWLMapCommandOutputProcessor` persistence ([#765](https://github.com/alpha-unito/streamflow/pull/765))
- Fixed `CWLCommand` persistence ([#771](https://github.com/alpha-unito/streamflow/pull/771))
- Fixed the `transferBufferSize` parameter in the `FutureConnector` ([#775](https://github.com/alpha-unito/streamflow/pull/775))
- Fixed subworkflows case in the `RunCrateProvenanceManager` ([#774](https://github.com/alpha-unito/streamflow/pull/774))
- Fixed `DefaultRetagTransformer` within scatter ([#742](https://github.com/alpha-unito/streamflow/pull/742))

### Removed

- Removed comparison operators from `Hardware` class ([#711](https://github.com/alpha-unito/streamflow/pull/711))
- Remove the `StreamWrapperContextManager` class ([#760](https://github.com/alpha-unito/streamflow/pull/760))

### Dependencies

- Bump aiohttp from 3.11.13 to 3.12.15 ([#700](https://github.com/alpha-unito/streamflow/pull/700), [#712](https://github.com/alpha-unito/streamflow/pull/712), [#715](https://github.com/alpha-unito/streamflow/pull/715), [#724](https://github.com/alpha-unito/streamflow/pull/724), [#729](https://github.com/alpha-unito/streamflow/pull/729), [#731](https://github.com/alpha-unito/streamflow/pull/731), [#733](https://github.com/alpha-unito/streamflow/pull/733), [#738](https://github.com/alpha-unito/streamflow/pull/738), [#744](https://github.com/alpha-unito/streamflow/pull/744), [#757](https://github.com/alpha-unito/streamflow/pull/757), [#773](https://github.com/alpha-unito/streamflow/pull/773))
- Bump asyncssh from 2.20.0 to 2.21.0 ([#718](https://github.com/alpha-unito/streamflow/pull/718))
- Bump bcrypt from 4.2.1 to 4.3.0 ([#683](https://github.com/alpha-unito/streamflow/pull/683))
- Bump cachetools from 5.5.2 to 6.1.0 ([#722](https://github.com/alpha-unito/streamflow/pull/722), [#746](https://github.com/alpha-unito/streamflow/pull/746))
- Bump cwl-utils from 0.37 to 0.39 ([#726](https://github.com/alpha-unito/streamflow/pull/726), [#770](https://github.com/alpha-unito/streamflow/pull/770))
- Bump importlib-metadata from 8.6.1 to 8.7.0 ([#716](https://github.com/alpha-unito/streamflow/pull/716))
- Bump jinja2 from 3.1.5 to 3.1.6 ([#687](https://github.com/alpha-unito/streamflow/pull/687))
- Bump jsonschema from 4.23.0 to 4.25.0 ([#723](https://github.com/alpha-unito/streamflow/pull/723), [#766](https://github.com/alpha-unito/streamflow/pull/766), [#768](https://github.com/alpha-unito/streamflow/pull/768))
- Bump kaleido from 0.4.2 to 1.0.0 ([#749](https://github.com/alpha-unito/streamflow/pull/749))
- Bump kubernetes-asyncio from 32.0.0 to 32.3.2 ([#707](https://github.com/alpha-unito/streamflow/pull/707), [#717](https://github.com/alpha-unito/streamflow/pull/717))
- Bump pandas from 2.2.3 to 2.3.1 ([#734](https://github.com/alpha-unito/streamflow/pull/734), [#756](https://github.com/alpha-unito/streamflow/pull/756))
- Bump plotly from 6.0.0 to 6.2.0 ([#702](https://github.com/alpha-unito/streamflow/pull/702), [#727](https://github.com/alpha-unito/streamflow/pull/727), [#750](https://github.com/alpha-unito/streamflow/pull/750))
- Bump rdflib from 7.1.3 to 7.1.4 ([#706](https://github.com/alpha-unito/streamflow/pull/706))

### Dev Dependencies

- Bump bandit from 1.8.3 to 1.8.6 ([#747](https://github.com/alpha-unito/streamflow/pull/747), [#755](https://github.com/alpha-unito/streamflow/pull/755))
- Bump cwltest from 2.5.20241122133319 to 2.6.20250314152537 ([#701](https://github.com/alpha-unito/streamflow/pull/701))
- Bump cwltool from 3.1.20250110105449 to 3.1.20250715140722 ([#769](https://github.com/alpha-unito/streamflow/pull/769))
- Bump pytest from 8.3.4 to 8.4.1 ([#685](https://github.com/alpha-unito/streamflow/pull/685), [#730](https://github.com/alpha-unito/streamflow/pull/730), [#748](https://github.com/alpha-unito/streamflow/pull/748))
- Bump pytest-asyncio from 0.25.3 to 1.1.0 ([#704](https://github.com/alpha-unito/streamflow/pull/704), [#743](https://github.com/alpha-unito/streamflow/pull/743), [#763](https://github.com/alpha-unito/streamflow/pull/763))
- Bump pytest-cov from 6.0.0 to 6.2.1 ([#709](https://github.com/alpha-unito/streamflow/pull/709), [#713](https://github.com/alpha-unito/streamflow/pull/713), [#739](https://github.com/alpha-unito/streamflow/pull/739))
- Bump pytest-xdist from 3.6.1 to 3.8.0 ([#725](https://github.com/alpha-unito/streamflow/pull/725), [#751](https://github.com/alpha-unito/streamflow/pull/751))
- Bump pyupgrade from 3.19.1 to 3.20.0 ([#721](https://github.com/alpha-unito/streamflow/pull/721))
- Bump sphinx from 8.2.1 to 8.2.3 ([#686](https://github.com/alpha-unito/streamflow/pull/686))
- Bump sphinx-llms-txt from 0.3.0 to 0.3.1 ([#776](https://github.com/alpha-unito/streamflow/pull/776))

## [0.2.0.dev12] - 2025-02-28

### Breaking Changes

- Drop support for Python 3.8 ([#561](https://github.com/alpha-unito/streamflow/pull/561))

### Added

- Added `NoContainerCWLDockerTranslator` ([#453](https://github.com/alpha-unito/streamflow/pull/453))
- Check job status when it terminates ([#452](https://github.com/alpha-unito/streamflow/pull/452))
- Added `get_stream_writer` method to `Connector` ([#477](https://github.com/alpha-unito/streamflow/pull/477))
- Implementing stacked locations scheduling ([#480](https://github.com/alpha-unito/streamflow/pull/480))
- Created `CWLWorkflow` class ([#504](https://github.com/alpha-unito/streamflow/pull/504))
- Enable StreamFlow JSON Schema dump ([#543](https://github.com/alpha-unito/streamflow/pull/543))
- Add support for Python 3.13 ([#539](https://github.com/alpha-unito/streamflow/pull/539))
- Support `stacked` storage in scheduling ([#552](https://github.com/alpha-unito/streamflow/pull/552))
- Enable multi-workflow reports ([#581](https://github.com/alpha-unito/streamflow/pull/581))
- Add `local` field to `ExecutionLocation` ([#593](https://github.com/alpha-unito/streamflow/pull/593))
- Add `isort` to the lint pipeline ([#621](https://github.com/alpha-unito/streamflow/pull/621))
- Added `timeout` to `SSHConnector` ([#644](https://github.com/alpha-unito/streamflow/pull/644))

### Changed

- Refactor command tokens ([#469](https://github.com/alpha-unito/streamflow/pull/469))
- Optimize scheduling logic ([#474](https://github.com/alpha-unito/streamflow/pull/474))
- Improve performance of local copies ([#476](https://github.com/alpha-unito/streamflow/pull/476))
- Update Apptainer version in CI pipeline ([#482](https://github.com/alpha-unito/streamflow/pull/482))
- Make `ContainerConnector` extend `ConnectorWrapper` ([#481](https://github.com/alpha-unito/streamflow/pull/481))
- Removed duplicate package import ([#492](https://github.com/alpha-unito/streamflow/pull/492))
- Moved `Graph` reference from `CWLTokenProcessor` to `CWLWorkflow` ([#516](https://github.com/alpha-unito/streamflow/pull/516))
- Changed propagation of failure termination ([#373](https://github.com/alpha-unito/streamflow/pull/373))
- Improved description about location storage in the `Hardware` class ([#423](https://github.com/alpha-unito/streamflow/pull/423))
- Temporarily disable MacOS X GitHub Actions ([#459](https://github.com/alpha-unito/streamflow/pull/459))
- Improved job propagation after scheduling ([#580](https://github.com/alpha-unito/streamflow/pull/580))
- Simplify `DataManager` logic for local transfers ([#582](https://github.com/alpha-unito/streamflow/pull/582))
- Simplify data movement logic ([#583](https://github.com/alpha-unito/streamflow/pull/583))
- Improve `LocalConnector` performance ([#585](https://github.com/alpha-unito/streamflow/pull/585))
- Refactor `DataManager` class and fix local copy ([#587](https://github.com/alpha-unito/streamflow/pull/587))
- Stop loading contents on `Directory` objects ([#594](https://github.com/alpha-unito/streamflow/pull/594))
- Improve logging of copy methods ([#599](https://github.com/alpha-unito/streamflow/pull/599))
- Modernize pytest suite ([#600](https://github.com/alpha-unito/streamflow/pull/600))
- Improve mount handling on `SingularityConnector` ([#601](https://github.com/alpha-unito/streamflow/pull/601))
- Refactored unit tests ([#615](https://github.com/alpha-unito/streamflow/pull/615))
- Improve MacOS CI performance ([#609](https://github.com/alpha-unito/streamflow/pull/609))
- Propagate `DataLocation` relations to stacked envs ([#608](https://github.com/alpha-unito/streamflow/pull/608))
- Migrate CWL parsing from cwltool to cwl-utils ([#58](https://github.com/alpha-unito/streamflow/pull/58))
- Refactored `remotepath` module ([#618](https://github.com/alpha-unito/streamflow/pull/618))
- Update coverage command and add badge ([#625](https://github.com/alpha-unito/streamflow/pull/625))
- Improved ssh retry mechanism ([#501](https://github.com/alpha-unito/streamflow/pull/501))
- Optimize remote commands through `StreamFlowPath` ([#626](https://github.com/alpha-unito/streamflow/pull/626))
- Modernize base Docker image and Helm version ([#655](https://github.com/alpha-unito/streamflow/pull/655))
- Migrate Docker container to Debian ([#658](https://github.com/alpha-unito/streamflow/pull/658))
- Refactor `SSHConnector` class ([#661](https://github.com/alpha-unito/streamflow/pull/661))
- Improve CWL parsing performance ([#660](https://github.com/alpha-unito/streamflow/pull/660))
- Remove useless `LocalConnector` deps ([#665](https://github.com/alpha-unito/streamflow/pull/665))
- Uniform StreamFlow output format to CWL ecosystem ([#667](https://github.com/alpha-unito/streamflow/pull/667))
- Improved remote file creation ([#679](https://github.com/alpha-unito/streamflow/pull/679))
- Update Linux GitHub runners to 24.04 ([#680](https://github.com/alpha-unito/streamflow/pull/680))
- Update PostgreSQL plugin documentation ([#681](https://github.com/alpha-unito/streamflow/pull/681))

### Fixed

- Fix PyPI release pipeline ([#450](https://github.com/alpha-unito/streamflow/pull/450))
- Moved the `_retrieve_output` calls inside `try`-`except` clauses ([#451](https://github.com/alpha-unito/streamflow/pull/451))
- Fix `workdir` handling in `SlurmConnector` ([#456](https://github.com/alpha-unito/streamflow/pull/456))
- Re-introduced log level option in `DockerComposeConnector` ([#457](https://github.com/alpha-unito/streamflow/pull/457))
- Fixed StreamFlow name ([#458](https://github.com/alpha-unito/streamflow/pull/458))
- Fixed prefix of input union types ([#455](https://github.com/alpha-unito/streamflow/pull/455))
- Fixed record field name in `CWLObjectTokenProcessor` ([#460](https://github.com/alpha-unito/streamflow/pull/460))
- Fixed `workdir` of wrapped deployments ([#461](https://github.com/alpha-unito/streamflow/pull/461))
- Fix remote write ([#471](https://github.com/alpha-unito/streamflow/pull/471))
- Fix `SingularityConnector` environment variables ([#470](https://github.com/alpha-unito/streamflow/pull/470))
- Fixed output step when it is an empty list ([#472](https://github.com/alpha-unito/streamflow/pull/472))
- Fix input directory path ([#475](https://github.com/alpha-unito/streamflow/pull/475))
- Fixed `CONTENT_LIMIT` checks ([#491](https://github.com/alpha-unito/streamflow/pull/491))
- Added explicit `return` in `_copy_remote_to_remote` ([#515](https://github.com/alpha-unito/streamflow/pull/515))
- Fixed minor bugs ([#535](https://github.com/alpha-unito/streamflow/pull/535))
- Fix MacOS X CI on GitHub Actions ([#547](https://github.com/alpha-unito/streamflow/pull/547))
- Fixed job input for the CWL `InitialWorkDirRequirement` ([#536](https://github.com/alpha-unito/streamflow/pull/536))
- Fix `follow_symlink` function return value ([#542](https://github.com/alpha-unito/streamflow/pull/542))
- Fix `ValueFromTransformer` without input deps ([#553](https://github.com/alpha-unito/streamflow/pull/553))
- Handle lists in CWL `glob` field ([#555](https://github.com/alpha-unito/streamflow/pull/555))
- Fixed optional CWL parameter ([#466](https://github.com/alpha-unito/streamflow/pull/466))
- Fix report feature in case of empty workflow ([#568](https://github.com/alpha-unito/streamflow/pull/568))
- Improved `SSHConnector` error message ([#574](https://github.com/alpha-unito/streamflow/pull/574))
- Fix requirement eval in CWL `WorkflowStep` ([#578](https://github.com/alpha-unito/streamflow/pull/578))
- Fixed `InputInjectorStep` in remote location data case ([#566](https://github.com/alpha-unito/streamflow/pull/566))
- Fix concurrency in `DefaultScheduler` ([#584](https://github.com/alpha-unito/streamflow/pull/584))
- Fixed `workdir` inheritance in wrapped deployments ([#588](https://github.com/alpha-unito/streamflow/pull/588))
- Added check to `mkdir` return code ([#595](https://github.com/alpha-unito/streamflow/pull/595))
- Fix `PBSConnector` behaviour ([#606](https://github.com/alpha-unito/streamflow/pull/606))
- Fixed `Hardware` operation methods ([#616](https://github.com/alpha-unito/streamflow/pull/616))
- Fixed some CWL translator issues ([#628](https://github.com/alpha-unito/streamflow/pull/628))
- Fixed remote ports ([#643](https://github.com/alpha-unito/streamflow/pull/643))
- Fix remote files handling ([#648](https://github.com/alpha-unito/streamflow/pull/648))
- Fixed `ChannelOpenError` handling on the `SSHConnector` ([#664](https://github.com/alpha-unito/streamflow/pull/664))
- Fixed tag hierarchy check ([#670](https://github.com/alpha-unito/streamflow/pull/670))
- Fix `ANTLR4` script generator ([#674](https://github.com/alpha-unito/streamflow/pull/674))
- Handle single-element output lists in CWL ([#672](https://github.com/alpha-unito/streamflow/pull/672))
- Fix CWL `pickValue` with `all_non_null` option ([#677](https://github.com/alpha-unito/streamflow/pull/677))

### Removed

- Remove unused code ([#673](https://github.com/alpha-unito/streamflow/pull/673))

### Dependencies

- Bump aiohttp from 3.9.5 to 3.11.13 ([#514](https://github.com/alpha-unito/streamflow/pull/514), [#521](https://github.com/alpha-unito/streamflow/pull/521), [#524](https://github.com/alpha-unito/streamflow/pull/524), [#526](https://github.com/alpha-unito/streamflow/pull/526), [#551](https://github.com/alpha-unito/streamflow/pull/551), [#554](https://github.com/alpha-unito/streamflow/pull/554), [#557](https://github.com/alpha-unito/streamflow/pull/557), [#563](https://github.com/alpha-unito/streamflow/pull/563), [#592](https://github.com/alpha-unito/streamflow/pull/592), [#597](https://github.com/alpha-unito/streamflow/pull/597), [#602](https://github.com/alpha-unito/streamflow/pull/602), [#605](https://github.com/alpha-unito/streamflow/pull/605), [#607](https://github.com/alpha-unito/streamflow/pull/607), [#612](https://github.com/alpha-unito/streamflow/pull/612), [#613](https://github.com/alpha-unito/streamflow/pull/613), [#617](https://github.com/alpha-unito/streamflow/pull/617), [#632](https://github.com/alpha-unito/streamflow/pull/632), [#657](https://github.com/alpha-unito/streamflow/pull/657), [#678](https://github.com/alpha-unito/streamflow/pull/678))
- Bump aiosqlite from 0.20.0 to 0.21.0 ([#656](https://github.com/alpha-unito/streamflow/pull/656))
- Bump antlr4-python3-runtime from 4.13.1 to 4.13.2 ([#519](https://github.com/alpha-unito/streamflow/pull/519))
- Bump asyncssh from 2.14.2 to 2.20.0 ([#534](https://github.com/alpha-unito/streamflow/pull/534), [#575](https://github.com/alpha-unito/streamflow/pull/575), [#622](https://github.com/alpha-unito/streamflow/pull/622), [#668](https://github.com/alpha-unito/streamflow/pull/668))
- Bump bcrypt from 4.1.3 to 4.2.1 ([#508](https://github.com/alpha-unito/streamflow/pull/508), [#604](https://github.com/alpha-unito/streamflow/pull/604))
- Bump cachetools from 5.3.3 to 5.5.2 ([#499](https://github.com/alpha-unito/streamflow/pull/499), [#527](https://github.com/alpha-unito/streamflow/pull/527), [#647](https://github.com/alpha-unito/streamflow/pull/647), [#671](https://github.com/alpha-unito/streamflow/pull/671))
- Bump cwl-utils from 0.33 to 0.37 ([#538](https://github.com/alpha-unito/streamflow/pull/538), [#564](https://github.com/alpha-unito/streamflow/pull/564), [#663](https://github.com/alpha-unito/streamflow/pull/663))
- Bump importlib-metadata from 7.1.0 to 8.6.1 ([#487](https://github.com/alpha-unito/streamflow/pull/487), [#488](https://github.com/alpha-unito/streamflow/pull/488), [#510](https://github.com/alpha-unito/streamflow/pull/510), [#529](https://github.com/alpha-unito/streamflow/pull/529), [#544](https://github.com/alpha-unito/streamflow/pull/544), [#645](https://github.com/alpha-unito/streamflow/pull/645))
- Bump jinja2 from 3.1.4 to 3.1.5 ([#635](https://github.com/alpha-unito/streamflow/pull/635))
- Bump jsonschema from 4.22.0 to 4.23.0 ([#495](https://github.com/alpha-unito/streamflow/pull/495))
- Bump kaleido from 0.2.1 to 0.4.2 ([#598](https://github.com/alpha-unito/streamflow/pull/598), [#603](https://github.com/alpha-unito/streamflow/pull/603))
- Bump kubernetes-asyncio from 29.0.0 to 32.0.0 ([#479](https://github.com/alpha-unito/streamflow/pull/479), [#489](https://github.com/alpha-unito/streamflow/pull/489), [#517](https://github.com/alpha-unito/streamflow/pull/517), [#522](https://github.com/alpha-unito/streamflow/pull/522), [#545](https://github.com/alpha-unito/streamflow/pull/545), [#619](https://github.com/alpha-unito/streamflow/pull/619), [#631](https://github.com/alpha-unito/streamflow/pull/631))
- Bump pandas from 2.2.2 to 2.2.3 ([#548](https://github.com/alpha-unito/streamflow/pull/548))
- Bump plotly from 5.22.0 to 6.0.0 ([#507](https://github.com/alpha-unito/streamflow/pull/507), [#533](https://github.com/alpha-unito/streamflow/pull/533), [#546](https://github.com/alpha-unito/streamflow/pull/546), [#651](https://github.com/alpha-unito/streamflow/pull/651))
- Bump psutil from 5.9.8 to 7.0.0 ([#485](https://github.com/alpha-unito/streamflow/pull/485), [#570](https://github.com/alpha-unito/streamflow/pull/570), [#633](https://github.com/alpha-unito/streamflow/pull/633), [#662](https://github.com/alpha-unito/streamflow/pull/662))
- Bump rdflib from 7.0.0 to 7.1.3 ([#576](https://github.com/alpha-unito/streamflow/pull/576), [#640](https://github.com/alpha-unito/streamflow/pull/640), [#642](https://github.com/alpha-unito/streamflow/pull/642))
- Bump referencing from 0.35.1 to 0.36.2 ([#641](https://github.com/alpha-unito/streamflow/pull/641), [#649](https://github.com/alpha-unito/streamflow/pull/649))
- Bump yattag from 1.15.2 to 1.16.1 ([#520](https://github.com/alpha-unito/streamflow/pull/520), [#586](https://github.com/alpha-unito/streamflow/pull/586))

### Dev Dependencies

- Bump bandit from 1.7.8 to 1.8.3 ([#483](https://github.com/alpha-unito/streamflow/pull/483), [#550](https://github.com/alpha-unito/streamflow/pull/550), [#611](https://github.com/alpha-unito/streamflow/pull/611), [#639](https://github.com/alpha-unito/streamflow/pull/639), [#666](https://github.com/alpha-unito/streamflow/pull/666))
- Bump black from 24.4.2 to 25.1.0 ([#513](https://github.com/alpha-unito/streamflow/pull/513), [#559](https://github.com/alpha-unito/streamflow/pull/559), [#653](https://github.com/alpha-unito/streamflow/pull/653))
- Bump codecov/codecov-action from 4 to 5 ([#596](https://github.com/alpha-unito/streamflow/pull/596))
- Bump codespell from 2.2.6 to 2.4.1 ([#465](https://github.com/alpha-unito/streamflow/pull/465), [#646](https://github.com/alpha-unito/streamflow/pull/646), [#654](https://github.com/alpha-unito/streamflow/pull/654))
- Bump cwltest from 2.5.20240425111257 to 2.5.20241122133319 ([#497](https://github.com/alpha-unito/streamflow/pull/497), [#537](https://github.com/alpha-unito/streamflow/pull/537), [#610](https://github.com/alpha-unito/streamflow/pull/610))
- Bump cwltool from 3.1.20240404144621 to 3.1.20250110105449 ([#454](https://github.com/alpha-unito/streamflow/pull/454), [#493](https://github.com/alpha-unito/streamflow/pull/493), [#591](https://github.com/alpha-unito/streamflow/pull/591), [#630](https://github.com/alpha-unito/streamflow/pull/630), [#638](https://github.com/alpha-unito/streamflow/pull/638))
- Bump docker/build-push-action from 5 to 6 ([#484](https://github.com/alpha-unito/streamflow/pull/484))
- Bump flake8-bugbear from 24.4.26 to 24.12.12 ([#532](https://github.com/alpha-unito/streamflow/pull/532), [#579](https://github.com/alpha-unito/streamflow/pull/579), [#623](https://github.com/alpha-unito/streamflow/pull/623))
- Bump helm/kind-action from 1.10.0 to 1.12.0 ([#627](https://github.com/alpha-unito/streamflow/pull/627), [#634](https://github.com/alpha-unito/streamflow/pull/634))
- Bump isort from 5.13.2 to 6.0.1 ([#650](https://github.com/alpha-unito/streamflow/pull/650), [#682](https://github.com/alpha-unito/streamflow/pull/682))
- Bump pytest from 8.1.1 to 8.3.4 ([#462](https://github.com/alpha-unito/streamflow/pull/462), [#473](https://github.com/alpha-unito/streamflow/pull/473), [#505](https://github.com/alpha-unito/streamflow/pull/505), [#512](https://github.com/alpha-unito/streamflow/pull/512), [#549](https://github.com/alpha-unito/streamflow/pull/549), [#614](https://github.com/alpha-unito/streamflow/pull/614))
- Bump pytest-asyncio from 0.24.0 to 0.25.3 ([#624](https://github.com/alpha-unito/streamflow/pull/624), [#636](https://github.com/alpha-unito/streamflow/pull/636), [#652](https://github.com/alpha-unito/streamflow/pull/652))
- Bump pytest-cov from 5.0.0 to 6.0.0 ([#577](https://github.com/alpha-unito/streamflow/pull/577))
- Bump pyupgrade from 3.15.2 to 3.19.1 ([#478](https://github.com/alpha-unito/streamflow/pull/478), [#511](https://github.com/alpha-unito/streamflow/pull/511), [#567](https://github.com/alpha-unito/streamflow/pull/567), [#572](https://github.com/alpha-unito/streamflow/pull/572), [#629](https://github.com/alpha-unito/streamflow/pull/629))
- Bump sphinx from 7.3.7 to 8.2.1 ([#500](https://github.com/alpha-unito/streamflow/pull/500), [#502](https://github.com/alpha-unito/streamflow/pull/502), [#506](https://github.com/alpha-unito/streamflow/pull/506), [#518](https://github.com/alpha-unito/streamflow/pull/518), [#565](https://github.com/alpha-unito/streamflow/pull/565), [#669](https://github.com/alpha-unito/streamflow/pull/669), [#676](https://github.com/alpha-unito/streamflow/pull/676))
- Bump sphinx-rtd-theme from 2.0.0 to 3.0.2 ([#556](https://github.com/alpha-unito/streamflow/pull/556), [#560](https://github.com/alpha-unito/streamflow/pull/560), [#590](https://github.com/alpha-unito/streamflow/pull/590))

## [0.2.0.dev11] - 2024-05-07

### Added

- Add support for Python 3.12 ([#256](https://github.com/alpha-unito/streamflow/pull/256))
- Added possibility to load ports and steps in a different workflow ([#274](https://github.com/alpha-unito/streamflow/pull/274))
- Add warning messages for default connector params ([#294](https://github.com/alpha-unito/streamflow/pull/294))
- Add `optional` param to composite processors ([#295](https://github.com/alpha-unito/streamflow/pull/295))
- Added missing task creation in some gathers ([#327](https://github.com/alpha-unito/streamflow/pull/327))
- Added `StreamWrapper` documentation ([#329](https://github.com/alpha-unito/streamflow/pull/329))
- Added new queries to the Database ([#356](https://github.com/alpha-unito/streamflow/pull/356))
- Added `retries` and `retryDelay` parameters in the `SSHConnector` configuration ([#361](https://github.com/alpha-unito/streamflow/pull/361))
- Support for nested Location objects ([#383](https://github.com/alpha-unito/streamflow/pull/383))

### Changed

- Update Node.js version in CI pipeline ([#286](https://github.com/alpha-unito/streamflow/pull/286))
- Update CWL v1.2 conformance tests ([#288](https://github.com/alpha-unito/streamflow/pull/288))
- Refactor conftest.py ([#287](https://github.com/alpha-unito/streamflow/pull/287))
- Removed deprecated `pkg_resources` API ([#291](https://github.com/alpha-unito/streamflow/pull/291))
- Replace `cgi` and `logging.warn` with new APIs ([#292](https://github.com/alpha-unito/streamflow/pull/292))
- Improve `test_download` logic ([#293](https://github.com/alpha-unito/streamflow/pull/293))
- Improved error message of batch systems when job fails ([#332](https://github.com/alpha-unito/streamflow/pull/332))
- Add no regression test for Sphinx documentation ([#337](https://github.com/alpha-unito/streamflow/pull/337))
- Reduce size of database metadata ([#338](https://github.com/alpha-unito/streamflow/pull/338))
- Move CWL 1.2 conformance to main branch ([#346](https://github.com/alpha-unito/streamflow/pull/346))
- Improved scatter/gather performance ([#348](https://github.com/alpha-unito/streamflow/pull/348))
- Fix Codecov GitHub action ([#374](https://github.com/alpha-unito/streamflow/pull/374))
- Fix CWL Docker Requirement config example ([#375](https://github.com/alpha-unito/streamflow/pull/375))
- Remove SSH deployment from MacOS CI ([#376](https://github.com/alpha-unito/streamflow/pull/376))
- Fix Codecov GitHub Action ([#379](https://github.com/alpha-unito/streamflow/pull/379))
- Fix Codecov GitHub Action ([#380](https://github.com/alpha-unito/streamflow/pull/380))
- Add `codecov.yml` configuration file ([#382](https://github.com/alpha-unito/streamflow/pull/382))
- Add `__slots__` attribute to `Token` objects ([#385](https://github.com/alpha-unito/streamflow/pull/385))
- Remove `__eq__` and `__hash__` from `Location` ([#389](https://github.com/alpha-unito/streamflow/pull/389))
- Refactor the `Location` class hierarchy ([#390](https://github.com/alpha-unito/streamflow/pull/390))
- Update MacOS X CI/CD pipeline ([#408](https://github.com/alpha-unito/streamflow/pull/408))
- Removed `scheduling_policy` attribute in `Target` class ([#421](https://github.com/alpha-unito/streamflow/pull/421))
- Removed `scheduling_groups` attribute ([#422](https://github.com/alpha-unito/streamflow/pull/422))
- Changed retrieval of location hardware in the `SSHConnector` class ([#424](https://github.com/alpha-unito/streamflow/pull/424))
- Refactor `test_scheduler` ([#425](https://github.com/alpha-unito/streamflow/pull/425))

### Fixed

- Fixed minor issue with port-targets documentation ([#276](https://github.com/alpha-unito/streamflow/pull/276))
- Fixed Config persistence ([#289](https://github.com/alpha-unito/streamflow/pull/289))
- Fixed loop termination combinator persistence ([#290](https://github.com/alpha-unito/streamflow/pull/290))
- Fix copy remote-to-remote using ConnectorWrapper ([#301](https://github.com/alpha-unito/streamflow/pull/301))
- Adjusted local-to-remote copy with folders ([#308](https://github.com/alpha-unito/streamflow/pull/308))
- Fixed remote-to-local copy of nested directories ([#309](https://github.com/alpha-unito/streamflow/pull/309))
- Fixed template environment variables ([#310](https://github.com/alpha-unito/streamflow/pull/310))
- Fixed SSHClientConnection closing ([#311](https://github.com/alpha-unito/streamflow/pull/311))
- Fixed incomplete stream reading ([#314](https://github.com/alpha-unito/streamflow/pull/314))
- Fix `valueFrom` type checking behaviour ([#323](https://github.com/alpha-unito/streamflow/pull/323))
- Fixed data location invalidation ([#324](https://github.com/alpha-unito/streamflow/pull/324))
- Fixed job name ([#325](https://github.com/alpha-unito/streamflow/pull/325))
- Fixed typo errors ([#326](https://github.com/alpha-unito/streamflow/pull/326))
- Fixed symbolic link remote input data error ([#328](https://github.com/alpha-unito/streamflow/pull/328))
- Fixed `CWLConditionalStep` persistence ([#330](https://github.com/alpha-unito/streamflow/pull/330))
- Fixed provenance issues ([#331](https://github.com/alpha-unito/streamflow/pull/331))
- Fixed some bugs ([#333](https://github.com/alpha-unito/streamflow/pull/333))
- Added check after the creation data ([#336](https://github.com/alpha-unito/streamflow/pull/336))
- Fixed `get` method of `RemotePathMapper` ([#343](https://github.com/alpha-unito/streamflow/pull/343))
- Fixed provenance for the `size` tokens ([#370](https://github.com/alpha-unito/streamflow/pull/370))
- Fixed `asyncio.Event` synchronizations when an exception occurs ([#395](https://github.com/alpha-unito/streamflow/pull/395))
- Fixed input ports of `DotProductSizeTransformer` ([#396](https://github.com/alpha-unito/streamflow/pull/396))
- Fixed wrapped connector calls for `QueueManagerConnector` classes ([#403](https://github.com/alpha-unito/streamflow/pull/403))
- Fix `ListMergeCombinator` port names ([#407](https://github.com/alpha-unito/streamflow/pull/407))
- Fixed glob on symbolic link path ([#417](https://github.com/alpha-unito/streamflow/pull/417))
- Fixed `ValueFromTransformer` ([#446](https://github.com/alpha-unito/streamflow/pull/446))
- Fixed scheduler timeout in Python<3.11 ([#449](https://github.com/alpha-unito/streamflow/pull/449))

### Dependencies

- Bump aiohttp from 3.8.6 to 3.9.5 ([#283](https://github.com/alpha-unito/streamflow/pull/283), [#296](https://github.com/alpha-unito/streamflow/pull/296), [#364](https://github.com/alpha-unito/streamflow/pull/364), [#367](https://github.com/alpha-unito/streamflow/pull/367), [#429](https://github.com/alpha-unito/streamflow/pull/429), [#431](https://github.com/alpha-unito/streamflow/pull/431))
- Bump aiosqlite from 0.19.0 to 0.20.0 ([#388](https://github.com/alpha-unito/streamflow/pull/388))
- Bump asyncssh from 2.14.0 to 2.14.2 ([#277](https://github.com/alpha-unito/streamflow/pull/277), [#322](https://github.com/alpha-unito/streamflow/pull/322))
- Bump bcrypt from 4.0.1 to 4.1.3 ([#299](https://github.com/alpha-unito/streamflow/pull/299), [#320](https://github.com/alpha-unito/streamflow/pull/320), [#448](https://github.com/alpha-unito/streamflow/pull/448))
- Bump cachetools from 5.3.1 to 5.3.3 ([#265](https://github.com/alpha-unito/streamflow/pull/265), [#394](https://github.com/alpha-unito/streamflow/pull/394))
- Bump cwltool from 3.1.20230906142556 to 3.1.20240404144621 ([#259](https://github.com/alpha-unito/streamflow/pull/259), [#262](https://github.com/alpha-unito/streamflow/pull/262), [#281](https://github.com/alpha-unito/streamflow/pull/281), [#307](https://github.com/alpha-unito/streamflow/pull/307), [#350](https://github.com/alpha-unito/streamflow/pull/350), [#427](https://github.com/alpha-unito/streamflow/pull/427))
- Bump cwl-utils from 0.29 to 0.33 ([#271](https://github.com/alpha-unito/streamflow/pull/271), [#279](https://github.com/alpha-unito/streamflow/pull/279), [#302](https://github.com/alpha-unito/streamflow/pull/302), [#426](https://github.com/alpha-unito/streamflow/pull/426))
- Bump importlib-metadata from 6.8.0 to 7.1.0 ([#305](https://github.com/alpha-unito/streamflow/pull/305), [#334](https://github.com/alpha-unito/streamflow/pull/334), [#402](https://github.com/alpha-unito/streamflow/pull/402), [#416](https://github.com/alpha-unito/streamflow/pull/416))
- Bump importlib-resources from 6.1.1 to 6.4.0 ([#391](https://github.com/alpha-unito/streamflow/pull/391), [#401](https://github.com/alpha-unito/streamflow/pull/401), [#412](https://github.com/alpha-unito/streamflow/pull/412), [#414](https://github.com/alpha-unito/streamflow/pull/414))
- Bump jinja2 from 3.1.2 to 3.1.4 ([#347](https://github.com/alpha-unito/streamflow/pull/347), [#447](https://github.com/alpha-unito/streamflow/pull/447))
- Bump jsonschema from 4.19.1 to 4.22.0 ([#270](https://github.com/alpha-unito/streamflow/pull/270), [#282](https://github.com/alpha-unito/streamflow/pull/282), [#352](https://github.com/alpha-unito/streamflow/pull/352), [#358](https://github.com/alpha-unito/streamflow/pull/358), [#444](https://github.com/alpha-unito/streamflow/pull/444))
- Bump kubernetes-asyncio from 28.2.0 to 29.0.0 ([#284](https://github.com/alpha-unito/streamflow/pull/284), [#355](https://github.com/alpha-unito/streamflow/pull/355))
- Bump pandas from 2.1.1 to 2.2.2 ([#269](https://github.com/alpha-unito/streamflow/pull/269), [#280](https://github.com/alpha-unito/streamflow/pull/280), [#312](https://github.com/alpha-unito/streamflow/pull/312), [#357](https://github.com/alpha-unito/streamflow/pull/357), [#392](https://github.com/alpha-unito/streamflow/pull/392), [#428](https://github.com/alpha-unito/streamflow/pull/428))
- Bump plotly from 5.17.0 to 5.22.0 ([#267](https://github.com/alpha-unito/streamflow/pull/267), [#384](https://github.com/alpha-unito/streamflow/pull/384), [#415](https://github.com/alpha-unito/streamflow/pull/415), [#434](https://github.com/alpha-unito/streamflow/pull/434), [#445](https://github.com/alpha-unito/streamflow/pull/445))
- Bump psutil from 5.9.5 to 5.9.8 ([#258](https://github.com/alpha-unito/streamflow/pull/258), [#321](https://github.com/alpha-unito/streamflow/pull/321), [#359](https://github.com/alpha-unito/streamflow/pull/359))
- Bump rdflib from 6.3.2 to 7.0.0 ([#193](https://github.com/alpha-unito/streamflow/pull/193))
- Bump yattag from 1.15.1 to 1.15.2 ([#268](https://github.com/alpha-unito/streamflow/pull/268))

### Dev Dependencies

- Bump actions/setup-node from 3 to 4 ([#263](https://github.com/alpha-unito/streamflow/pull/263))
- Bump actions/setup-python from 4 to 5 ([#306](https://github.com/alpha-unito/streamflow/pull/306))
- Bump actions/upload-artifact from 3 to 4 ([#318](https://github.com/alpha-unito/streamflow/pull/318))
- Bump bandit from 1.7.5 to 1.7.8 ([#313](https://github.com/alpha-unito/streamflow/pull/313), [#360](https://github.com/alpha-unito/streamflow/pull/360), [#405](https://github.com/alpha-unito/streamflow/pull/405))
- Bump black from 23.9.1 to 24.4.2 ([#260](https://github.com/alpha-unito/streamflow/pull/260), [#264](https://github.com/alpha-unito/streamflow/pull/264), [#275](https://github.com/alpha-unito/streamflow/pull/275), [#315](https://github.com/alpha-unito/streamflow/pull/315), [#335](https://github.com/alpha-unito/streamflow/pull/335), [#362](https://github.com/alpha-unito/streamflow/pull/362), [#366](https://github.com/alpha-unito/streamflow/pull/366), [#381](https://github.com/alpha-unito/streamflow/pull/381), [#413](https://github.com/alpha-unito/streamflow/pull/413), [#430](https://github.com/alpha-unito/streamflow/pull/430), [#439](https://github.com/alpha-unito/streamflow/pull/439), [#440](https://github.com/alpha-unito/streamflow/pull/440))
- Bump codecov/codecov-action from 3 to 4 ([#369](https://github.com/alpha-unito/streamflow/pull/369))
- Bump cwltest from 2.3.20230825125225 to 2.5.20240425111257 ([#273](https://github.com/alpha-unito/streamflow/pull/273), [#344](https://github.com/alpha-unito/streamflow/pull/344), [#368](https://github.com/alpha-unito/streamflow/pull/368), [#398](https://github.com/alpha-unito/streamflow/pull/398), [#438](https://github.com/alpha-unito/streamflow/pull/438))
- Bump flake8-bugbear from 23.9.16 to 24.4.26 ([#297](https://github.com/alpha-unito/streamflow/pull/297), [#300](https://github.com/alpha-unito/streamflow/pull/300), [#304](https://github.com/alpha-unito/streamflow/pull/304), [#353](https://github.com/alpha-unito/streamflow/pull/353), [#372](https://github.com/alpha-unito/streamflow/pull/372), [#436](https://github.com/alpha-unito/streamflow/pull/436), [#442](https://github.com/alpha-unito/streamflow/pull/442))
- Bump github/codeql-action from 2 to 3 ([#316](https://github.com/alpha-unito/streamflow/pull/316))
- Bump helm/kind-action from 1.8.0 to 1.10.0 ([#377](https://github.com/alpha-unito/streamflow/pull/377), [#437](https://github.com/alpha-unito/streamflow/pull/437))
- Bump mukunku/tag-exists-action from 1.4.0 to 1.6.0 ([#317](https://github.com/alpha-unito/streamflow/pull/317), [#371](https://github.com/alpha-unito/streamflow/pull/371))
- Bump pytest from 7.4.2 to 8.1.1 ([#266](https://github.com/alpha-unito/streamflow/pull/266), [#341](https://github.com/alpha-unito/streamflow/pull/341), [#365](https://github.com/alpha-unito/streamflow/pull/365), [#387](https://github.com/alpha-unito/streamflow/pull/387), [#393](https://github.com/alpha-unito/streamflow/pull/393), [#406](https://github.com/alpha-unito/streamflow/pull/406))
- Bump pytest-cov from 4.1.0 to 5.0.0 ([#419](https://github.com/alpha-unito/streamflow/pull/419))
- Bump pytest-xdist from 3.3.1 to 3.6.1 ([#278](https://github.com/alpha-unito/streamflow/pull/278), [#285](https://github.com/alpha-unito/streamflow/pull/285), [#441](https://github.com/alpha-unito/streamflow/pull/441))
- Bump pyupgrade from 3.14.0 to 3.15.2 ([#257](https://github.com/alpha-unito/streamflow/pull/257), [#386](https://github.com/alpha-unito/streamflow/pull/386), [#420](https://github.com/alpha-unito/streamflow/pull/420))
- Bump sphinx from 7.2.6 to 7.3.7 ([#435](https://github.com/alpha-unito/streamflow/pull/435))
- Bump sphinx-rtd-theme from 1.3.0 to 2.0.0 ([#298](https://github.com/alpha-unito/streamflow/pull/298))

## [0.2.0.dev10] - 2023-10-10

### Added

- Added parameter to choose the deployments to use in pytests ([#232](https://github.com/alpha-unito/streamflow/pull/232))

### Fixed

- Fix `_get_existing_parent` behaviour ([#253](https://github.com/alpha-unito/streamflow/pull/253))

### Dependencies

- Bump aiohttp from 3.8.5 to 3.8.6 ([#255](https://github.com/alpha-unito/streamflow/pull/255))
- Bump asyncssh from 2.13.2 to 2.14.0 ([#250](https://github.com/alpha-unito/streamflow/pull/250))
- Bump jsonschema from 4.19.0 to 4.19.1 ([#244](https://github.com/alpha-unito/streamflow/pull/244))
- Bump kubernetes-asyncio from 25.11.0 to 28.2.0 ([#247](https://github.com/alpha-unito/streamflow/pull/247), [#249](https://github.com/alpha-unito/streamflow/pull/249), [#254](https://github.com/alpha-unito/streamflow/pull/254))
- Bump pandas from 2.1.0 to 2.1.1 ([#245](https://github.com/alpha-unito/streamflow/pull/245))
- Bump plotly from 5.16.1 to 5.17.0 ([#240](https://github.com/alpha-unito/streamflow/pull/240))

### Dev Dependencies

- Bump black from 23.7.0 to 23.9.1 ([#233](https://github.com/alpha-unito/streamflow/pull/233))
- Bump codespell from 2.2.5 to 2.2.6 ([#252](https://github.com/alpha-unito/streamflow/pull/252))
- Bump docker/build-push-action from 4 to 5 ([#237](https://github.com/alpha-unito/streamflow/pull/237))
- Bump docker/login-action from 2 to 3 ([#235](https://github.com/alpha-unito/streamflow/pull/235))
- Bump docker/setup-buildx-action from 2 to 3 ([#236](https://github.com/alpha-unito/streamflow/pull/236))
- Bump docker/setup-qemu-action from 2 to 3 ([#234](https://github.com/alpha-unito/streamflow/pull/234))
- Bump flake8-bugbear from 23.7.10 to 23.9.16 ([#239](https://github.com/alpha-unito/streamflow/pull/239))
- Bump mukunku/tag-exists-action from 1.3.0 to 1.4.0 ([#242](https://github.com/alpha-unito/streamflow/pull/242))
- Bump pyupgrade from 3.10.1 to 3.14.0 ([#246](https://github.com/alpha-unito/streamflow/pull/246), [#248](https://github.com/alpha-unito/streamflow/pull/248), [#251](https://github.com/alpha-unito/streamflow/pull/251))
- Bump sphinx from 7.2.5 to 7.2.6 ([#238](https://github.com/alpha-unito/streamflow/pull/238))

## [0.2.0.dev9] - 2023-09-10

### Added

- Add configurable time option in HPC connectors ([#227](https://github.com/alpha-unito/streamflow/pull/227))

### Changed

- Avoid opening too many SSH sessions ([#229](https://github.com/alpha-unito/streamflow/pull/229))
- Update CWL v1.2 conformance tests ([#231](https://github.com/alpha-unito/streamflow/pull/231))

### Fixed

- Fix Python dependencies conflict in Mac OS CI ([#214](https://github.com/alpha-unito/streamflow/pull/214))
- Connector and CWL Runner fixes ([#216](https://github.com/alpha-unito/streamflow/pull/216))
- Fix `template_map` error when service has no file ([#223](https://github.com/alpha-unito/streamflow/pull/223))
- Fix scheduler behaviour when retry_delay is set ([#224](https://github.com/alpha-unito/streamflow/pull/224))
- Fix potential race condition on sqlite `row_factory` ([#225](https://github.com/alpha-unito/streamflow/pull/225))
- Fix Flux Docker version to 0.28.0 ([#230](https://github.com/alpha-unito/streamflow/pull/230))

### Dependencies

- Bump antlr4-python3-runtime from 4.13.0 to 4.13.1 ([#222](https://github.com/alpha-unito/streamflow/pull/222))
- Bump cwltool from 3.1.20230719185429 to 3.1.20230906142556 ([#226](https://github.com/alpha-unito/streamflow/pull/226))
- Bump cwl-utils from 0.28 to 0.29 ([#220](https://github.com/alpha-unito/streamflow/pull/220))
- Bump pandas from 2.0.3 to 2.1.0 ([#217](https://github.com/alpha-unito/streamflow/pull/217))

### Dev Dependencies

- Bump actions/checkout from 3 to 4 ([#221](https://github.com/alpha-unito/streamflow/pull/221))
- Bump pytest from 7.4.0 to 7.4.2 ([#219](https://github.com/alpha-unito/streamflow/pull/219), [#228](https://github.com/alpha-unito/streamflow/pull/228))
- Bump sphinx from 7.2.3 to 7.2.5 ([#215](https://github.com/alpha-unito/streamflow/pull/215), [#218](https://github.com/alpha-unito/streamflow/pull/218))

## [0.2.0.dev8] - 2023-08-28

### Added

- Enable distributed inheritance for plugins ([#208](https://github.com/alpha-unito/streamflow/pull/208))

### Changed

- Updated MacOS CI in GitHub Actions ([#204](https://github.com/alpha-unito/streamflow/pull/204))
- Change jsonschema loader ([#205](https://github.com/alpha-unito/streamflow/pull/205))
- Adjust JSONSchema ids ([#211](https://github.com/alpha-unito/streamflow/pull/211))
- Update docs to v0.2.0 ([#213](https://github.com/alpha-unito/streamflow/pull/213))

### Fixed

- Fix Flux and Mac OS CI steps ([#209](https://github.com/alpha-unito/streamflow/pull/209))

### Dependencies

- Bump jsonschema from 4.18.6 to 4.19.0 ([#197](https://github.com/alpha-unito/streamflow/pull/197))
- Bump kubernetes-asyncio from 24.2.3 to 25.11.0 ([#206](https://github.com/alpha-unito/streamflow/pull/206))
- Bump plotly from 5.15.0 to 5.16.1 ([#199](https://github.com/alpha-unito/streamflow/pull/199), [#201](https://github.com/alpha-unito/streamflow/pull/201))

### Dev Dependencies

- Bump cwltest from 2.3.20230527113600 to 2.3.20230825125225 ([#212](https://github.com/alpha-unito/streamflow/pull/212))
- Bump mukunku/tag-exists-action from 1.2.0 to 1.3.0 ([#198](https://github.com/alpha-unito/streamflow/pull/198))
- Bump sphinx from 7.1.2 to 7.2.3 ([#203](https://github.com/alpha-unito/streamflow/pull/203), [#210](https://github.com/alpha-unito/streamflow/pull/210))
- Bump sphinx-rtd-theme from 1.2.2 to 1.3.0 ([#207](https://github.com/alpha-unito/streamflow/pull/207))

## [0.2.0.dev7] - 2023-08-06

### Added

- Added type definitions to ext CLI ([#180](https://github.com/alpha-unito/streamflow/pull/180))

### Changed

- Updated CWL v1.2 conformance tests ([#168](https://github.com/alpha-unito/streamflow/pull/168))
- Migrated JSONSchema to Draft 2019-09 ([#185](https://github.com/alpha-unito/streamflow/pull/185))
- Moved black configuration to `pyproject.toml` ([#196](https://github.com/alpha-unito/streamflow/pull/196))

### Fixed

- Fixed type checking in conftest ([#195](https://github.com/alpha-unito/streamflow/pull/195))

### Dependencies

- Bump aiohttp from 3.8.4 to 3.8.5 ([#187](https://github.com/alpha-unito/streamflow/pull/187))
- Bump asyncssh from 2.13.1 to 2.13.2 ([#170](https://github.com/alpha-unito/streamflow/pull/170))
- Bump cwltool from 3.1.20230601100705 to 3.1.20230719185429 ([#172](https://github.com/alpha-unito/streamflow/pull/172), [#189](https://github.com/alpha-unito/streamflow/pull/189))
- Bump importlib-metadata from 6.6.0 to 6.8.0 ([#167](https://github.com/alpha-unito/streamflow/pull/167), [#177](https://github.com/alpha-unito/streamflow/pull/177))
- Bump jsonschema from 4.17.3 to 4.18.6 ([#175](https://github.com/alpha-unito/streamflow/pull/175), [#184](https://github.com/alpha-unito/streamflow/pull/184), [#186](https://github.com/alpha-unito/streamflow/pull/186), [#194](https://github.com/alpha-unito/streamflow/pull/194))
- Bump pandas from 2.0.2 to 2.0.3 ([#173](https://github.com/alpha-unito/streamflow/pull/173))

### Dev Dependencies

- Bump black from 23.3.0 to 23.7.0 ([#178](https://github.com/alpha-unito/streamflow/pull/178))
- Bump codespell from 2.2.4 to 2.2.5 ([#165](https://github.com/alpha-unito/streamflow/pull/165))
- Bump flake8-bugbear from 23.6.5 to 23.7.10 ([#179](https://github.com/alpha-unito/streamflow/pull/179))
- Bump pytest from 7.3.2 to 7.4.0 ([#171](https://github.com/alpha-unito/streamflow/pull/171))
- Bump pytest-asyncio from 0.21.0 to 0.21.1 ([#182](https://github.com/alpha-unito/streamflow/pull/182))
- Bump pyupgrade from 3.6.0 to 3.10.1 ([#166](https://github.com/alpha-unito/streamflow/pull/166), [#174](https://github.com/alpha-unito/streamflow/pull/174), [#176](https://github.com/alpha-unito/streamflow/pull/176), [#191](https://github.com/alpha-unito/streamflow/pull/191))
- Bump sphinx from 7.0.1 to 7.1.2 ([#188](https://github.com/alpha-unito/streamflow/pull/188), [#190](https://github.com/alpha-unito/streamflow/pull/190), [#192](https://github.com/alpha-unito/streamflow/pull/192))

## [0.2.0.dev6] - 2023-06-15

### Added

- Added `streamflow ext` subcommand ([#145](https://github.com/alpha-unito/streamflow/pull/145))
- Added no regression tests for provenance ([#150](https://github.com/alpha-unito/streamflow/pull/150))
- Adding explicit options to queue connectors ([#151](https://github.com/alpha-unito/streamflow/pull/151))
- Added curl flags in Dockerfile to retry server connection when it fails ([#158](https://github.com/alpha-unito/streamflow/pull/158))
- Handling `allOf` directives in config schemas ([#164](https://github.com/alpha-unito/streamflow/pull/164))

### Fixed

- Fix additional files in RunCrate provenance ([#136](https://github.com/alpha-unito/streamflow/pull/136))
- Fix `list` and `report` subcommands ([#144](https://github.com/alpha-unito/streamflow/pull/144))

### Dependencies

- Bump antlr4-python3-runtime from 4.12.0 to 4.13.0 ([#147](https://github.com/alpha-unito/streamflow/pull/147))
- Bump cachetools from 5.3.0 to 5.3.1 ([#152](https://github.com/alpha-unito/streamflow/pull/152))
- Bump cwltool from 3.1.20230425144158 to 3.1.20230601100705 ([#140](https://github.com/alpha-unito/streamflow/pull/140), [#154](https://github.com/alpha-unito/streamflow/pull/154), [#156](https://github.com/alpha-unito/streamflow/pull/156))
- Bump cwl-utils from 0.25 to 0.28 ([#138](https://github.com/alpha-unito/streamflow/pull/138), [#142](https://github.com/alpha-unito/streamflow/pull/142), [#157](https://github.com/alpha-unito/streamflow/pull/157))
- Bump pandas from 2.0.1 to 2.0.2 ([#153](https://github.com/alpha-unito/streamflow/pull/153))
- Bump plotly from 5.14.1 to 5.15.0 ([#161](https://github.com/alpha-unito/streamflow/pull/161))

### Dev Dependencies

- Bump cwltest from 2.3.20230108193615 to 2.3.20230527113600 ([#155](https://github.com/alpha-unito/streamflow/pull/155))
- Bump flake8-bugbear from 23.3.23 to 23.6.5 ([#137](https://github.com/alpha-unito/streamflow/pull/137), [#159](https://github.com/alpha-unito/streamflow/pull/159))
- Bump pytest from 7.3.1 to 7.3.2 ([#162](https://github.com/alpha-unito/streamflow/pull/162))
- Bump pytest-cov from 4.0.0 to 4.1.0 ([#149](https://github.com/alpha-unito/streamflow/pull/149))
- Bump pytest-xdist from 3.2.1 to 3.3.1 ([#141](https://github.com/alpha-unito/streamflow/pull/141), [#143](https://github.com/alpha-unito/streamflow/pull/143))
- Bump pyupgrade from 3.3.2 to 3.6.0 ([#135](https://github.com/alpha-unito/streamflow/pull/135), [#163](https://github.com/alpha-unito/streamflow/pull/163))
- Bump sphinx from 7.0.0 to 7.0.1 ([#139](https://github.com/alpha-unito/streamflow/pull/139))
- Bump sphinx-rtd-theme from 1.2.0 to 1.2.2 ([#148](https://github.com/alpha-unito/streamflow/pull/148), [#160](https://github.com/alpha-unito/streamflow/pull/160))

## [0.2.0.dev5] - 2023-05-06

### Added

- Added package and version to plugin description ([#134](https://github.com/alpha-unito/streamflow/pull/134))

### Changed

- Improved StreamFlow provenance support ([#122](https://github.com/alpha-unito/streamflow/pull/122))
- Improved CLI for plugin inspection ([#130](https://github.com/alpha-unito/streamflow/pull/130))

### Fixed

- Fix InitialWorkDirRequirement with subdirs ([#118](https://github.com/alpha-unito/streamflow/pull/118))

### Dependencies

- Bump aiosqlite from 0.18.0 to 0.19.0 ([#115](https://github.com/alpha-unito/streamflow/pull/115))
- Bump cwltool from 3.1.20230325110543 to 3.1.20230425144158 ([#129](https://github.com/alpha-unito/streamflow/pull/129))
- Bump cwl-utils from 0.23 to 0.25 ([#114](https://github.com/alpha-unito/streamflow/pull/114), [#133](https://github.com/alpha-unito/streamflow/pull/133))
- Bump importlib-metadata from 6.3.0 to 6.6.0 ([#119](https://github.com/alpha-unito/streamflow/pull/119), [#124](https://github.com/alpha-unito/streamflow/pull/124))
- Bump kubernetes-asyncio from 24.2.2 to 24.2.3 ([#132](https://github.com/alpha-unito/streamflow/pull/132))
- Bump pandas from 2.0.0 to 2.0.1 ([#125](https://github.com/alpha-unito/streamflow/pull/125))
- Bump psutil from 5.9.4 to 5.9.5 ([#120](https://github.com/alpha-unito/streamflow/pull/120))

### Dev Dependencies

- Bump pytest from 7.3.0 to 7.3.1 ([#117](https://github.com/alpha-unito/streamflow/pull/117))
- Bump pyupgrade from 3.3.1 to 3.3.2 ([#128](https://github.com/alpha-unito/streamflow/pull/128))
- Bump sphinx from 6.1.3 to 7.0.0 ([#123](https://github.com/alpha-unito/streamflow/pull/123), [#126](https://github.com/alpha-unito/streamflow/pull/126), [#131](https://github.com/alpha-unito/streamflow/pull/131))

## [0.1.6] - 2023-04-20

### Changed

- Updated JS imports from cwltool ([#121](https://github.com/alpha-unito/streamflow/pull/121))

## [0.2.0.dev4] - 2023-04-14

### Added

- Added explicit version check for DockerCompose ([#96](https://github.com/alpha-unito/streamflow/pull/96))
- Added flux framework connector ([#107](https://github.com/alpha-unito/streamflow/pull/107))
- Added configurable DockerRequirement conversion ([#111](https://github.com/alpha-unito/streamflow/pull/111))

### Fixed

- Fix StreamFlow MPI example ([#95](https://github.com/alpha-unito/streamflow/pull/95))
- Updated and fixed StreamFlow Dockerfile ([#97](https://github.com/alpha-unito/streamflow/pull/97))
- Fixed Docker and Helm features ([#109](https://github.com/alpha-unito/streamflow/pull/109))

### Dependencies

- Bump cwltool from 3.1.20230302145532 to 3.1.20230325110543 ([#103](https://github.com/alpha-unito/streamflow/pull/103))
- Bump importlib-metadata from 6.0.0 to 6.3.0 ([#100](https://github.com/alpha-unito/streamflow/pull/100), [#112](https://github.com/alpha-unito/streamflow/pull/112))
- Bump pandas from 1.5.2 to 2.0.0 ([#92](https://github.com/alpha-unito/streamflow/pull/92), [#108](https://github.com/alpha-unito/streamflow/pull/108))
- Bump plotly from 5.11.0 to 5.14.1 ([#93](https://github.com/alpha-unito/streamflow/pull/93), [#106](https://github.com/alpha-unito/streamflow/pull/106), [#110](https://github.com/alpha-unito/streamflow/pull/110))
- Bump rdflib from 6.2.0 to 6.3.2 ([#104](https://github.com/alpha-unito/streamflow/pull/104))

### Dev Dependencies

- Bump black from 23.1.0 to 23.3.0 ([#105](https://github.com/alpha-unito/streamflow/pull/105))
- Bump flake8-bugbear from 23.2.13 to 23.3.23 ([#91](https://github.com/alpha-unito/streamflow/pull/91), [#102](https://github.com/alpha-unito/streamflow/pull/102))
- Bump pytest from 7.2.2 to 7.3.0 ([#113](https://github.com/alpha-unito/streamflow/pull/113))
- Bump pytest-asyncio from 0.20.3 to 0.21.0 ([#101](https://github.com/alpha-unito/streamflow/pull/101))
- Bump pytest-xdist from 3.2.0 to 3.2.1 ([#90](https://github.com/alpha-unito/streamflow/pull/90))

## [0.2.0.dev3] - 2023-03-13

### Added

- Colored output for Streamflow Run ([#17](https://github.com/alpha-unito/streamflow/pull/17))
- Added regression test scheduler ([#39](https://github.com/alpha-unito/streamflow/pull/39))
- Workflow Run RO-Crate support ([#45](https://github.com/alpha-unito/streamflow/pull/45))
- Added extension points to StreamFlowPlugin ([#46](https://github.com/alpha-unito/streamflow/pull/46))
- Added nested files to main entity ([#53](https://github.com/alpha-unito/streamflow/pull/53))
- Added no regression tests for persistence ([#56](https://github.com/alpha-unito/streamflow/pull/56))
- Added persistence of Command class structure ([#74](https://github.com/alpha-unito/streamflow/pull/74))
- Added support for nested connectors ([#86](https://github.com/alpha-unito/streamflow/pull/86))

### Changed

- Changed test pipeline ([#33](https://github.com/alpha-unito/streamflow/pull/33))
- Prepend `isEnabledFor` to logger calls ([#37](https://github.com/alpha-unito/streamflow/pull/37))
- Code reformat with pyupgrade ([#40](https://github.com/alpha-unito/streamflow/pull/40))
- Revised setup.py classifiers ([#49](https://github.com/alpha-unito/streamflow/pull/49))
- Better job naming strategy for CWL ([#51](https://github.com/alpha-unito/streamflow/pull/51))
- Use JSON format for PBS output ([#63](https://github.com/alpha-unito/streamflow/pull/63))
- Moving aiotarstream to the deployment module ([#64](https://github.com/alpha-unito/streamflow/pull/64))
- Moving some logic from Translator to utils ([#66](https://github.com/alpha-unito/streamflow/pull/66))
- Stricter type checking for CWL outputs ([#70](https://github.com/alpha-unito/streamflow/pull/70))
- Migrate from `setup.py` to `pyproject.toml` ([#89](https://github.com/alpha-unito/streamflow/pull/89))

### Fixed

- Fix scheduler validation path ([#50](https://github.com/alpha-unito/streamflow/pull/50))
- Fixed cwl-runner entrypoint with no workflows entry ([#52](https://github.com/alpha-unito/streamflow/pull/52))
- Avoid Sqlite race conditions during tests ([#57](https://github.com/alpha-unito/streamflow/pull/57))
- Register job directories after creating them ([#65](https://github.com/alpha-unito/streamflow/pull/65))
- Fixed MacOS compatibility ([#77](https://github.com/alpha-unito/streamflow/pull/77))
- Fix CWL map processors behaviour ([#81](https://github.com/alpha-unito/streamflow/pull/81))

### Dependencies

- Bump aiohttp from 3.8.3 to 3.8.4 ([#72](https://github.com/alpha-unito/streamflow/pull/72))
- Bump antlr4-python3-runtime from 4.10 to 4.12.0 ([#23](https://github.com/alpha-unito/streamflow/pull/23), [#76](https://github.com/alpha-unito/streamflow/pull/76))
- Bump asyncssh from 2.13.0 to 2.13.1 ([#75](https://github.com/alpha-unito/streamflow/pull/75))
- Bump cachetools from 5.2.0 to 5.3.0 ([#35](https://github.com/alpha-unito/streamflow/pull/35), [#47](https://github.com/alpha-unito/streamflow/pull/47))
- Bump cwltool from 3.1.20221008225030 to 3.1.20230302145532 ([#25](https://github.com/alpha-unito/streamflow/pull/25), [#55](https://github.com/alpha-unito/streamflow/pull/55), [#61](https://github.com/alpha-unito/streamflow/pull/61), [#69](https://github.com/alpha-unito/streamflow/pull/69), [#71](https://github.com/alpha-unito/streamflow/pull/71), [#79](https://github.com/alpha-unito/streamflow/pull/79))
- Bump cwl-utils from 0.20 to 0.23 ([#26](https://github.com/alpha-unito/streamflow/pull/26), [#54](https://github.com/alpha-unito/streamflow/pull/54), [#62](https://github.com/alpha-unito/streamflow/pull/62))
- Bump importlib-metadata from 5.2.0 to 6.0.0 ([#30](https://github.com/alpha-unito/streamflow/pull/30))
- Bump jsonref from 1.0.1 to 1.1.0 ([#43](https://github.com/alpha-unito/streamflow/pull/43))
- Bump yattag from 1.15.0 to 1.15.1 ([#78](https://github.com/alpha-unito/streamflow/pull/78))

### Dev Dependencies

- Bump actions/checkout from 2 to 3 ([#29](https://github.com/alpha-unito/streamflow/pull/29))
- Bump actions/setup-node from 2 to 3 ([#22](https://github.com/alpha-unito/streamflow/pull/22))
- Bump actions/setup-python from 2 to 4 ([#20](https://github.com/alpha-unito/streamflow/pull/20))
- Bump actions/upload-artifact from 2 to 3 ([#36](https://github.com/alpha-unito/streamflow/pull/36))
- Bump bandit from 1.7.4 to 1.7.5 ([#85](https://github.com/alpha-unito/streamflow/pull/85))
- Bump black from 22.12.0 to 23.1.0 ([#60](https://github.com/alpha-unito/streamflow/pull/60))
- Bump codespell from 2.2.2 to 2.2.4 ([#84](https://github.com/alpha-unito/streamflow/pull/84))
- Bump docker/build-push-action from 2 to 4 ([#21](https://github.com/alpha-unito/streamflow/pull/21), [#59](https://github.com/alpha-unito/streamflow/pull/59))
- Bump docker/login-action from 1 to 2 ([#19](https://github.com/alpha-unito/streamflow/pull/19))
- Bump docker/setup-buildx-action from 1 to 2 ([#24](https://github.com/alpha-unito/streamflow/pull/24))
- Bump docker/setup-qemu-action from 1 to 2 ([#28](https://github.com/alpha-unito/streamflow/pull/28))
- Bump flake8-bugbear from 22.12.6 to 23.2.13 ([#44](https://github.com/alpha-unito/streamflow/pull/44), [#48](https://github.com/alpha-unito/streamflow/pull/48), [#73](https://github.com/alpha-unito/streamflow/pull/73))
- Bump mukunku/tag-exists-action from 1.0.0 to 1.2.0 ([#27](https://github.com/alpha-unito/streamflow/pull/27))
- Bump pytest from 7.2.0 to 7.2.2 ([#41](https://github.com/alpha-unito/streamflow/pull/41), [#80](https://github.com/alpha-unito/streamflow/pull/80))
- Bump pytest-xdist from 3.1.0 to 3.2.0 ([#67](https://github.com/alpha-unito/streamflow/pull/67))
- Bump sphinx from 6.0.0 to 6.1.3 ([#31](https://github.com/alpha-unito/streamflow/pull/31), [#34](https://github.com/alpha-unito/streamflow/pull/34), [#38](https://github.com/alpha-unito/streamflow/pull/38))
- Bump sphinx-rtd-theme from 1.1.1 to 1.2.0 ([#68](https://github.com/alpha-unito/streamflow/pull/68))

## [0.2.0.dev2] - 2022-10-09

### Added

- Added services for SSH and `QueueManagerConnector` connectors ([b7203d8](https://github.com/alpha-unito/streamflow/commit/b7203d8))
- Added CITATION file ([2baa905](https://github.com/alpha-unito/streamflow/commit/2baa905))

### Changed

- Updated CWL toolchain compatibility ([7041950](https://github.com/alpha-unito/streamflow/commit/7041950))
- Updated GitHub pipelines ([3a5de2a](https://github.com/alpha-unito/streamflow/commit/3a5de2a))
- Improved performance by removing remote commands ([5c5b457](https://github.com/alpha-unito/streamflow/commit/5c5b457))
- Moved helper file to inline encoded command ([3814f1d](https://github.com/alpha-unito/streamflow/commit/3814f1d))
- Updated Loop support ([ed1056b](https://github.com/alpha-unito/streamflow/commit/ed1056b))

### Fixed

- Fixed `ListMergeCombinator` behaviour ([0695732](https://github.com/alpha-unito/streamflow/commit/0695732))

### Dependencies

- Added psutil to requirements ([f3dbba7](https://github.com/alpha-unito/streamflow/commit/f3dbba7))

## [0.2.0.dev1] - 2022-08-29

### Added

- Added port targets ([6712a6d](https://github.com/alpha-unito/streamflow/commit/6712a6d))
- Added persistence loading logic ([2240ef5](https://github.com/alpha-unito/streamflow/commit/2240ef5))
- Extension points implementation ([7d3a937](https://github.com/alpha-unito/streamflow/commit/7d3a937))
- Enriched persistence layer ([49c727e](https://github.com/alpha-unito/streamflow/commit/49c727e))
- Added cwltool:Loop support ([8a3adf7](https://github.com/alpha-unito/streamflow/commit/8a3adf7))
- Implemented cross-connector streaming copy ([0e5221b](https://github.com/alpha-unito/streamflow/commit/0e5221b))
- Created the `aiotarstream` library for async tar ([43fab68](https://github.com/alpha-unito/streamflow/commit/43fab68))
- Added `aiotar` library for remote copy ([7647e5f](https://github.com/alpha-unito/streamflow/commit/7647e5f))

### Changed

- Moved `loop_source` and `loop_when` to camelCase ([1a509f2](https://github.com/alpha-unito/streamflow/commit/1a509f2))
- Modified `valueFrom` behaviour in loops ([55b281a](https://github.com/alpha-unito/streamflow/commit/55b281a))
- Improved SSH connector performances ([04751e8](https://github.com/alpha-unito/streamflow/commit/04751e8))
- Added file-system traversal to find existing locations ([b81bf6a](https://github.com/alpha-unito/streamflow/commit/b81bf6a))
- Changed `DataLocation` creation strategy ([f09df93](https://github.com/alpha-unito/streamflow/commit/f09df93))
- Modified GitHub Actions to run on success ([39ef379](https://github.com/alpha-unito/streamflow/commit/39ef379))

### Fixed

- Fixed multiple errors on remote executions ([ff5b641](https://github.com/alpha-unito/streamflow/commit/ff5b641))
- Fixed remote-to-remote copy behaviour ([48bfd13](https://github.com/alpha-unito/streamflow/commit/48bfd13))
- Fix directory transfer bug and improve connector supports ([#13](https://github.com/alpha-unito/streamflow/pull/13))

## [0.2.0.dev0] - 2022-04-24

### Changed

- Heavy refactor of workflow management logic ([d4b7b24](https://github.com/alpha-unito/streamflow/commit/d4b7b24))
- Step class refactor ([eda8963](https://github.com/alpha-unito/streamflow/commit/eda8963))
- Moved ports outside steps ([980c65e](https://github.com/alpha-unito/streamflow/commit/980c65e))
- Moved token processors to steps ([78c0cf5](https://github.com/alpha-unito/streamflow/commit/78c0cf5))
- Substituted `resource` term with `location` ([191b5b5](https://github.com/alpha-unito/streamflow/commit/191b5b5))
- Substituted `model` term with `deployment` ([e939114](https://github.com/alpha-unito/streamflow/commit/e939114))
- Moved most log messages to DEBUG level ([d0f5d60](https://github.com/alpha-unito/streamflow/commit/d0f5d60))
- Updated CWL-v1.2 conformance test commit ([8ad4db5](https://github.com/alpha-unito/streamflow/commit/8ad4db5))
- Removed token deepcopy in `Port.put` method ([aabdb70](https://github.com/alpha-unito/streamflow/commit/aabdb70))

### Fixed

- Fixed the sorting of parameters in commands ([a0a1aa0](https://github.com/alpha-unito/streamflow/commit/a0a1aa0))
- Fixed `loadContents` in CWL ([ff5b9e6](https://github.com/alpha-unito/streamflow/commit/ff5b9e6))
- Fixed bug in checking available resources ([e3b7086](https://github.com/alpha-unito/streamflow/commit/e3b7086))
- Fixed error in input token grouping ([23e42fe](https://github.com/alpha-unito/streamflow/commit/23e42fe))

### Removed

- Removed `Helm2Connector` from source ([53284af](https://github.com/alpha-unito/streamflow/commit/53284af))

### Dependencies

- Updated ANTLR version to 4.10 ([3de01f2](https://github.com/alpha-unito/streamflow/commit/3de01f2))
- Updated kubernetes-asyncio to version 22.6.3 ([4ffb543](https://github.com/alpha-unito/streamflow/commit/4ffb543))

## [0.1.5] - 2022-04-15

### Dependencies

- Constrained ANTLR version to <4.10 ([aee4851](https://github.com/alpha-unito/streamflow/commit/aee4851))

## [0.1.4] - 2022-04-02

### Added

- Added support for remote CWL definitions ([47324bf](https://github.com/alpha-unito/streamflow/commit/47324bf))

### Changed

- Added Workflows Community info ([9643692](https://github.com/alpha-unito/streamflow/commit/9643692))

## [0.1.3] - 2022-02-23

### Fixed

- Solved Python >= 3.10 compatibility issue ([252fa97](https://github.com/alpha-unito/streamflow/commit/252fa97))
- Solved SSH compatibility issue with Python >= 3.9 ([1c0a872](https://github.com/alpha-unito/streamflow/commit/1c0a872))

## [0.1.2] - 2021-12-29

### Fixed

- Improved support for Windows platform ([1a6b335](https://github.com/alpha-unito/streamflow/commit/1a6b335))
- Fixed Helm Connector, apsw and Windows execution ([03c79d7](https://github.com/alpha-unito/streamflow/commit/03c79d7))
- Fixed Helm Connector checks ([d4ff604](https://github.com/alpha-unito/streamflow/commit/d4ff604))
- Fixed docker GitHub Actions ([4f7140b](https://github.com/alpha-unito/streamflow/commit/4f7140b))

## [0.1.1] - 2021-12-25

### Added

- Added `--streamflow-file` option to `cwl-runner` ([b9f4108](https://github.com/alpha-unito/streamflow/commit/b9f4108))
- Added Sphinx documentation ([76f2ea7](https://github.com/alpha-unito/streamflow/commit/76f2ea7))

### Changed

- Changed contributing instructions in README.md ([ba0be7e](https://github.com/alpha-unito/streamflow/commit/ba0be7e))
- Better organised CWL badges ([1ba5030](https://github.com/alpha-unito/streamflow/commit/1ba5030))

### Fixed

- Fixed bug with SSH resources retrieving ([dd0789b](https://github.com/alpha-unito/streamflow/commit/dd0789b))
- Fixed Release GitHub action ([0470f8a](https://github.com/alpha-unito/streamflow/commit/0470f8a))
- Minor pre-release fixes ([d3a2752](https://github.com/alpha-unito/streamflow/commit/d3a2752))

## [0.1.0] - 2021-12-19

### Changed

- Switched to GitHub Actions for CI/CD ([8f05da2](https://github.com/alpha-unito/streamflow/commit/8f05da2))
- Added CWL conformance badges ([85e5209](https://github.com/alpha-unito/streamflow/commit/85e5209))
- Minor fixes to dependencies and Docker ([cab297a](https://github.com/alpha-unito/streamflow/commit/cab297a))
- Replace Travis by GH Actions in the readme ([#9](https://github.com/alpha-unito/streamflow/pull/9))

## [0.0.34] - 2021-11-15

### Added

- Added CWL conformance tests ([0276b3c](https://github.com/alpha-unito/streamflow/commit/0276b3c))
- Added step-to-step bindings to the StreamFlow file ([a5565ed](https://github.com/alpha-unito/streamflow/commit/a5565ed))
- Added SQLite database and report class ([e57e643](https://github.com/alpha-unito/streamflow/commit/e57e643))

### Changed

- Optimised dependencies in nested workflows ([52232c9](https://github.com/alpha-unito/streamflow/commit/52232c9))
- Improved performances of StreamFlow runtime ([7421380](https://github.com/alpha-unito/streamflow/commit/7421380))

### Fixed

- Fixed nested scatter patterns ([a3c7f0b](https://github.com/alpha-unito/streamflow/commit/a3c7f0b))
- Fixed secondary files primary location ([aea75a9](https://github.com/alpha-unito/streamflow/commit/aea75a9))
- Fixed Docker and Travis build ([7d2a3a4](https://github.com/alpha-unito/streamflow/commit/7d2a3a4))

## [0.0.33] - 2021-08-12

### Fixed

- Fixes to improve CWL compatibility ([af4a0c2](https://github.com/alpha-unito/streamflow/commit/af4a0c2))

## [0.0.32] - 2021-07-05

### Added

- Added Local connector ([67ca093](https://github.com/alpha-unito/streamflow/commit/67ca093))
- Added Hardware requirements to jobs ([c0dd8a6](https://github.com/alpha-unito/streamflow/commit/c0dd8a6))
- Added CWL format checking support ([7602e52](https://github.com/alpha-unito/streamflow/commit/7602e52))

### Changed

- Updated SSH connector to handle multi-node models ([9438dc3](https://github.com/alpha-unito/streamflow/commit/9438dc3))
- Replaced `pipenv` toolchain ([e7474d1](https://github.com/alpha-unito/streamflow/commit/e7474d1))

### Fixed

- Fixed `CartesianProductInputCombinator` behaviour ([5a96a25](https://github.com/alpha-unito/streamflow/commit/5a96a25))

## [0.0.31] - 2021-05-29

### Fixed

- Fixed remote-to-remote copy for `QueueManagerConnector`

## [0.0.30] - 2021-05-27

### Fixed

- Fixed WebSocket API client in Helm connectors

## [0.0.29] - 2021-05-20

### Added

- Added token tag to avoid out-of-order errors ([dcddcb3](https://github.com/alpha-unito/streamflow/commit/dcddcb3))

### Changed

- Optimised Docker connector for bind mounts ([9b7e295](https://github.com/alpha-unito/streamflow/commit/9b7e295))

### Fixed

- Minor fixes to conditional behaviour ([5f73cf3](https://github.com/alpha-unito/streamflow/commit/5f73cf3))

## [0.0.28] - 2021-05-04

### Added

- Added support for CWL conditional steps ([deebe5a](https://github.com/alpha-unito/streamflow/commit/deebe5a))
- Added secondary files propagation ([9e31d58](https://github.com/alpha-unito/streamflow/commit/9e31d58))

### Changed

- Changed tar format to GNU ([ff6277d](https://github.com/alpha-unito/streamflow/commit/ff6277d))
- Removed compression in tar transfers ([6f4dd95](https://github.com/alpha-unito/streamflow/commit/6f4dd95))

## [0.0.27] - 2021-04-17

### Changed

- Improved data transfer logic ([313ba9e](https://github.com/alpha-unito/streamflow/commit/313ba9e))

## [0.0.26] - 2021-04-09

### Fixed

- Fixed SLURM job submission command
- Fixed PBS job status checking

## [0.0.25] - 2021-04-05

### Added

- Added PBS and SLURM connectors ([b75e6e2](https://github.com/alpha-unito/streamflow/commit/b75e6e2))
- Added caching for available resources ([2336e25](https://github.com/alpha-unito/streamflow/commit/2336e25))

### Changed

- Improved file transfer performances ([cd54aec](https://github.com/alpha-unito/streamflow/commit/cd54aec))
- Control number of sessions per SSH connection ([e160014](https://github.com/alpha-unito/streamflow/commit/e160014))
- Improved Singularity connector performances ([18c1915](https://github.com/alpha-unito/streamflow/commit/18c1915))
- Refactored connectors code ([808af6d](https://github.com/alpha-unito/streamflow/commit/808af6d))

### Fixed

- Fixed `pip search` command ([51ad998](https://github.com/alpha-unito/streamflow/commit/51ad998))

## [0.0.24] - 2021-03-22

### Added

- Added Singularity Connector ([b4735a2](https://github.com/alpha-unito/streamflow/commit/b4735a2))

### Changed

- Improved remote execution performances ([e39b303](https://github.com/alpha-unito/streamflow/commit/e39b303))

## [0.0.23] - 2021-01-03

### Added

- Added Fault Tolerance to StreamFlow ([0d1cbf8](https://github.com/alpha-unito/streamflow/commit/0d1cbf8))
- Added SSH and Slurm connectors ([fbd2c80](https://github.com/alpha-unito/streamflow/commit/fbd2c80))
- Added STMC support ([c199ac8](https://github.com/alpha-unito/streamflow/commit/c199ac8))
- Added support for stream redirection in connectors ([1e45a65](https://github.com/alpha-unito/streamflow/commit/1e45a65))

### Changed

- Created custom workflow management system ([6331926](https://github.com/alpha-unito/streamflow/commit/6331926))
- Split StreamFlow schema into multiple files ([6388746](https://github.com/alpha-unito/streamflow/commit/6388746))
- Improved termination of workflow ([60b7d92](https://github.com/alpha-unito/streamflow/commit/60b7d92))

## [0.0.21] - 2020-06-17

### Changed

- Updated README.md to include PYTHONPATH inclusion ([89e6958](https://github.com/alpha-unito/streamflow/commit/89e6958))

### Fixed

- Fixed wrong file creation in process.py ([f975dfd](https://github.com/alpha-unito/streamflow/commit/f975dfd))

## [0.0.20] - 2020-04-06

### Changed

- Updates to the Continuous Integration Pipeline ([300ae29](https://github.com/alpha-unito/streamflow/commit/300ae29))

## [0.0.13] - 2020-03-19

### Added

- Added Kubernetes inCluster execution for Helm templates ([6822935](https://github.com/alpha-unito/streamflow/commit/6822935))
- Added munipack workflow to StreamFlow examples ([325e8ed](https://github.com/alpha-unito/streamflow/commit/325e8ed))

### Changed

- Updated README.md to include Docker usage description ([caf261b](https://github.com/alpha-unito/streamflow/commit/caf261b))

## [0.0.12] - 2020-03-13

### Breaking Changes

- Added support for Helm 3 environments (breaking Helm 2 compatibility) ([922fde2](https://github.com/alpha-unito/streamflow/commit/922fde2))

### Added

- Initial StreamFlow implementation
- Added Travis CI integration ([f91d659](https://github.com/alpha-unito/streamflow/commit/f91d659))
- Added support for in-place dependency reading ([526ec7d](https://github.com/alpha-unito/streamflow/commit/526ec7d))
- Added support for remote files not present on local machine ([21a1d2f](https://github.com/alpha-unito/streamflow/commit/21a1d2f))
- Added support for directories generated by ExpressionTool ([7430d03](https://github.com/alpha-unito/streamflow/commit/7430d03))
- Added `flatten_list` function to avoid nested list outputs ([2357b7c](https://github.com/alpha-unito/streamflow/commit/2357b7c))

### Fixed

- Fixed `remote_fs_access` error for local jobs ([1954d83](https://github.com/alpha-unito/streamflow/commit/1954d83))
- Fixed bad behaviour with local job execution ([729b2d9](https://github.com/alpha-unito/streamflow/commit/729b2d9))
- Fixed remote-to-remote data movement on Helm connector ([0967668](https://github.com/alpha-unito/streamflow/commit/0967668))
- Patched WebSocket client `update()` function to handle binary format ([ba48f19](https://github.com/alpha-unito/streamflow/commit/ba48f19))

[Unreleased]: https://github.com/alpha-unito/streamflow/compare/0.2.0rc2...HEAD
[0.2.0rc2]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0rc2
[0.2.0rc1]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0rc1
[0.2.0.dev14]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev14
[0.2.0.dev13]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev13
[0.2.0.dev12]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev12
[0.2.0.dev11]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev11
[0.2.0.dev10]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev10
[0.2.0.dev9]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev9
[0.2.0.dev8]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev8
[0.2.0.dev7]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev7
[0.2.0.dev6]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev6
[0.2.0.dev5]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev5
[0.2.0.dev4]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev4
[0.1.6]: https://github.com/alpha-unito/streamflow/releases/tag/0.1.6
[0.2.0.dev3]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev3
[0.2.0.dev2]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev2
[0.2.0.dev1]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev1
[0.2.0.dev0]: https://github.com/alpha-unito/streamflow/releases/tag/0.2.0.dev0
[0.1.5]: https://github.com/alpha-unito/streamflow/releases/tag/0.1.5
[0.1.4]: https://github.com/alpha-unito/streamflow/releases/tag/0.1.4
[0.1.3]: https://github.com/alpha-unito/streamflow/releases/tag/0.1.3
[0.1.2]: https://github.com/alpha-unito/streamflow/releases/tag/0.1.2
[0.1.1]: https://github.com/alpha-unito/streamflow/releases/tag/0.1.1
[0.1.0]: https://github.com/alpha-unito/streamflow/releases/tag/0.1.0
[0.0.34]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.34
[0.0.33]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.33
[0.0.32]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.32
[0.0.31]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.31
[0.0.30]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.30
[0.0.29]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.29
[0.0.28]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.28
[0.0.27]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.27
[0.0.26]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.26
[0.0.25]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.25
[0.0.24]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.24
[0.0.23]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.23
[0.0.21]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.21
[0.0.20]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.20
[0.0.13]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.13
[0.0.12]: https://github.com/alpha-unito/streamflow/releases/tag/0.0.12
