# pg_trickle after v0.105.2: assessment and a bounded next phase

## Executive assessment

**The next phase should be a correctness and consolidation phase, not another expansion of SQL coverage, integrations, or scheduling machinery.** pg_trickle’s strongest product proposition is PostgreSQL-native, SQL-defined derived tables with automatic incremental maintenance. The engineering priority is to make that proposition dependable, understandable, and demonstrably economical for a clearly described set of workloads.

The assessment baseline is the v0.105.2 release, published on 11 September 2026, at commit `33df4cc91fda4fbadba79470347a714c8509703a`. As of 13 September 2026, v0.105.2 remains the latest published release, but the inspected `main` branch is ten commits ahead. Those commits include cleanup, dependency maintenance, and an important differential-maintenance correction. Released behavior and unreleased improvements must therefore be distinguished. [1–5]

The most immediate recommendation is a small maintenance release incorporating the fixes in PR #1011, with focused regression tests and package-level requalification. The next recommendation is to close the difference between the project’s release qualification ambitions and what its automated release gates actually establish. Only after those two tasks should performance optimization or further internal refactoring become the main workstream.

This conclusion is not that the implementation lacks engineering substance. The packaged Linux runtime qualification, differential-versus-full equivalence tests, explicit strategy checks, conservative capture defaults, and bounded refresh pipeline are substantial foundations. Rather, feature breadth has advanced further than the consistency of the support contract and the strength of the release evidence. [8–11, 17, 19–20]

A suitable product boundary is:

> Maintain SQL-defined PostgreSQL results correctly, with explainable freshness, observable maintenance strategy, and measured limits on the cost imposed on the database.

Everything recommended below fits that boundary. New query languages, distributed execution systems, lakehouse capabilities, additional capture backends, and more autonomous control actions do not belong in this next phase.

## 1. Release position and assurance

v0.105.2 is a meaningful package and runtime qualification milestone. The release publishes packages for PostgreSQL 18 on Linux amd64, Linux arm64, macOS arm64, and Windows amd64, alongside checksums and named runtime/performance evidence assets. Its workflow downloads the built Linux amd64 candidate and uses it in the light end-to-end qualification environment rather than merely recompiling unrelated source for all runtime checks. [1, 9]

That deserves credit. The release workflow includes public Graph V1 and Delta V1 conformance, DVM composition, differential/full equivalence, failure recovery, publication recovery, WAL admission, snapshot/security, and diagnostics test targets. The issue is not that all qualification is cosmetic. The issue is that several broader claims are not established by the particular gates used to publish this release. [8–9]

| Area | Assessment of the inspected release path |
|---|---|
| Packaged Linux runtime tests | A real and useful qualification path exists. |
| Exact-result and effective-strategy checks | Present in the test suite; these should remain central. |
| All-platform runtime qualification | Not established by the Linux-only smoke and qualification jobs. |
| Active-data upgrade qualification | Not established by the release’s SQL-completeness upgrade loop. |
| Foreground PostgreSQL write overhead | Not established by the Criterion benchmark smoke used here. |
| Provenance and results binding | Checksums and candidate metadata exist, but suite outcomes are not bound individually to artifact/workload measurements. |
| Long-running stability | Explicitly deferred, not a completed qualification claim. |

The 72-hour soak and seven-day longevity runs are explicitly deferred in the qualification contract and release materials. Their absence should not be treated as a hidden broken promise. Conversely, the release should not be interpreted as having supplied those assurances. The broader roadmap also defers the v1.0 qualification and release milestones. [8, 13, 25]

**Assessment boundary:** these conclusions are based on release metadata, source, tests, documentation, and workflow inspection. They are not an independently executed PostgreSQL qualification, benchmark, or security audit. The published release evidence and log asset contents were not independently verified; statements about their existence and size come from release metadata, while statements about their generation come from the workflow and writer implementation.

## 2. What is worth preserving

### The PostgreSQL-native execution model

The project’s design compiles maintenance work into SQL and executes it in PostgreSQL rather than requiring an external processing service for the core use case. The operator implementation, refresh pipeline, and normal table-based user interface all reinforce that architecture. It gives the project a coherent reason to exist: applications retain their database and SQL workflow while derived results become maintained objects. [6–7, 16, 20]

That architecture also creates a responsibility: maintenance competes with the foreground database workload. The success metric cannot stop at faster refreshes. It must include write latency, resource use, recovery behavior, and the effect of cleanup and scheduling on the same server. This is an architectural consequence, not evidence that the current system necessarily has unacceptable overhead.

### Correctness oracles and strategy visibility

`tests/e2e_diff_full_equivalence_tests.rs` checks stream-table results against the defining query across mutation cycles and separately asserts a differential-family effective mode. This is much stronger than checking whether a refresh call returns successfully. It also guards against an implementation becoming correct only by silently selecting whole-query recomputation. [19]

The next step should deepen that existing approach rather than introduce a second correctness framework. Particular attention should go to operator compositions, multiple sources changing between refreshes, duplicate multiplicities, and lifecycle boundaries.

### Conservative admission and operational mechanisms

The release code defaults to trigger capture and statement-level CDC triggers. The generated capability manifest declares the public Graph V1, Delta V1, and WAL capabilities, while the release contract includes admission and recovery checks. These are useful ingredients for a bounded support contract, even though the supporting documentation and manifest coverage need work. [8, 14, 17]

Similarly, the refresh pipeline already makes direct-versus-pipelined decisions, reports staging statistics, and keeps batches within the caller’s outer transaction. Existing status, explanation, backlog, drain, suspension, and repair procedures mean that the operator experience is not a blank slate. The next improvement is coherence and verification of these mechanisms, not adding more diagnostic endpoints indiscriminately. [20, 22]

### Subtractive maintenance is already underway

PR #1008 removes unused helpers, obsolete scripts, historical gates, and a development dependency. PR #1010 removes unused and duplicate code and shares end-to-end test helpers. Both were merged after the release. This is the right direction, and it should be acknowledged rather than proposed as though nothing has started. [23–24]

However, refactoring and dead-code removal should be kept separate from changes to relational semantics. Small diffs, preserved public behavior, and live regression evidence are more valuable than a target number of deleted lines.

## 3. Immediate priority: ship the post-release correctness correction

PR #1011, merged on 13 September 2026, modifies aggregate SQL generation and semi-/anti-join differentiation. Its changes are narrowly targeted: they replace a textual test for an outer `WHERE` clause with operator-tree information, and change the left-side snapshot used by the right-change branch of semi-/anti-join maintenance. [4–5]

Two points are important for the released version. First, the aggregate `has_outer_where` expression replaced by this PR is present in the v0.105.2 source. Second, the released semi-join implementation contains the old snapshot construction that re-adds deleted left rows. These are not merely defects introduced by the post-release cleanup. [6–7]

In the semi-join implementation, the left-change branch already handles deleted left rows. The right-change branch should deal with left rows that remain unchanged while their matching status changes. Reconstructing the entire old left relation for that second branch can bring deleted left rows back into consideration. The merged change instead derives an unchanged-left relation from the current relation with the inserted rows removed. This is an important distinction in a bag/multiset maintenance engine, particularly when the operator feeds another aggregate. It is a source-level explanation of the patch, not a claim that an independent runtime reproducer has been executed. [5–6]

**Recommended maintenance-release work:**

1. Incorporate PR #1011 and establish the affected release range with minimized reproductions.
2. Add small tests for simultaneous changes on both sides of `EXISTS` and `NOT EXISTS`, including deleted left rows, first/last right matches, duplicates, and an enclosing aggregate.
3. Add aggregate SQL-generation tests for a filter above a projected subquery or similar reconstructed `FROM` form.
4. Prove each regression fails on the affected implementation and passes on the candidate, while asserting the intended strategy and output-delta multiplicity where applicable.
5. Rerun the exact packaged candidate’s qualification and publish affected-query guidance.

The PR’s changed-file list contains three implementation files and no new test files. Existing tests may already catch these cases; the recommendation is to preserve compact, explicit regressions rather than depend only on a broader TPC-H workload. [5]

A code correction also does not establish that already-materialized results are repaired. The maintenance release should document how operators identify potentially affected stream tables, validate them against the defining query, and rebuild/resnapshot them using the supported lifecycle procedure where required. Avoid silently converting a targeted issue into a requirement to rebuild every stream table.

**Done means:** a released package contains the fix, a minimized regression demonstrates the defect and correction, effective strategy cannot hide the failure, and the upgrade notes explain recovery of affected existing state.

## 4. Release engineering findings

### 4.1 All distribution channels must wait for qualification

In `.github/workflows/release.yml`, `publish-release` depends on build, artifact smoke, and qualification. In contrast, `publish-docker-arch` depends only on build and artifact smoke; the Docker manifest publication follows that path. The workflow graph therefore permits container publication without a successful qualification job. [9]

This is a concrete release-gating defect. It does not establish that a bad image was actually published, and it is not a claim of a runtime vulnerability. Nevertheless, a failed qualification must block every official distribution channel, not only the GitHub Release.

**Bounded fix:** add the qualification dependency to the container publication path, or introduce a single shared release-approval job consumed by both publishing paths. Test the workflow dependency graph. Keep development images explicitly separate from release-qualified images.

**Done means:** an intentionally failed qualification prevents versioned release artifacts, container version tags, and mutable release aliases from being promoted.

### 4.2 Evidence should report results, not merely accept labels

The release workflow passes `--suite NAME=passed` arguments into `scripts/release_evidence.py`. The writer validates those result labels, verifies required suite names, hashes artifacts and logs, and writes candidate metadata. These are useful checks, but they do not connect each result to the exact artifact and measured workload that produced it. [9–10]

The writer also does not validate the qualification contract’s full required artifact set or compare measured values against its performance budgets. In the qualification path it requires at least one artifact and one retained log, rather than a complete artifact-by-suite result matrix. This is a weaker acceptance rule than a reader may infer from a release-level `passed` result. [8, 10]

**Bounded fix:** evolve the existing evidence writer, rather than add another framework. Test jobs should emit small structured result files containing candidate commit, artifact digest/platform, actual PostgreSQL version, suite identifier, effective workload, test counts, outcome, and log digest. Measured-budget records should contain the metric, baseline or denominator, measured value, unit, threshold, and verdict. The final writer should join and validate those records against the qualification contract.

Do not turn skipped, unavailable, historical, or empty test selections into green execution evidence. Preserve the current distinction among result states and make it operationally meaningful.

**Done means:** missing required artifacts, zero executed tests in a required suite, mismatched candidate hashes, empty required logs, and out-of-budget measurements each fail a negative-control test of the release gate.

### 4.3 Microbenchmarks do not qualify foreground database cost

The qualification contract declares a maximum 10% Criterion regression and a maximum 15% foreground-write overhead budget. The release’s performance command, however, is `BENCH_QUICK=1 ./scripts/run_benchmarks.sh`. The script compiles and runs pure-Rust Criterion benchmarks with PostgreSQL symbols stubbed; it does not measure source-table write latency in a live PostgreSQL server. [8, 11]

Microbenchmarks remain useful for code-generation and operator implementation changes. They should simply be named and interpreted as microbenchmark smoke or regression measurements. They cannot establish the foreground-write, WAL, refresh-tail-latency, output-log, and memory properties described in the package/field qualification plan. [11–12]

**Bounded fix:** add a small packaged PostgreSQL workload qualification, using the existing benchmark infrastructure where possible. Start with three maintained workload families: a keyed scan or small aggregate; a join-plus-aggregate with skew and updates/deletes; and a small dependency graph exercising the established external contracts. Include an output-consumer-off baseline and an active consumer case. Do not make the first iteration a benchmark for every SQL family.

Measure baseline source DML without maintenance, capture-enabled DML, and capture plus active refresh under the same workload and database conditions. Report write throughput and tail latency, refresh latency, end-to-end staleness, WAL, backlog, temp spills, and storage growth. A baseline that changes data, hardware, concurrency, or durability settings is not a valid overhead comparison.

The 15% figure is an existing contract threshold, not an independently validated universal performance property. Define exactly which statistic and workload it constrains. Where it is unattainable, fix the implementation or narrow the documented workload/assurance rather than removing the measurement.

**Done means:** the next package’s evidence contains actual database measurements, retained raw results, and explicit budget comparisons for a fixed workload set.

### 4.4 Platform support must match platform evidence

Four platforms are named in the qualification contract, but the release’s artifact smoke and runtime qualification jobs consume the Linux amd64 artifact. The Windows build also has `continue-on-error`. The existence of a released artifact is not equivalent to runtime qualification on that platform. [8–9]

This does not mean macOS, Windows, or arm64 packages fail. It means the inspected pipeline does not establish equal qualification across them.

**Bounded fix:** define support tiers. Either run the promised minimum install, active refresh, and lifecycle checks on each fully supported platform, or clearly label the narrower assurance attached to build-only or best-effort artifacts. Linux-first deep qualification is a sensible scope choice when presented honestly.

The release plan also calls for building every supported artifact twice in clean environments. The inspected workflow shows a single package build per matrix entry, not a second-build comparison. Treat reproducible-build qualification as a separate evidence requirement; checksums alone do not establish reproducibility. [9, 12]

**Done means:** every artifact’s published status states exactly whether it was built, installed, runtime-tested, upgrade-tested, and reproducibility-checked.

### 4.5 Upgrade completeness is not an active-data upgrade test

The release workflow’s upgrade loop runs `check_upgrade_completeness.sh` across SQL migration pairs. The qualification contract itself describes this suite as metadata-only upgrade-chain and catalog completeness validation. The packaged runtime target loop does not invoke `e2e_upgrade_tests`. [8–9]

The release gate verifies that an active-upgrade test function exists, but a source-level function-existence check does not show that the previous published binary was upgraded with pending source changes. This distinction matters because the implementation plan explicitly requires active-data upgrade and recreation paths. [12, 26]

**Bounded fix:** exercise the previous supported release artifact against the next candidate: create and maintain stream tables, accumulate pending changes, quiesce through the documented procedure, update the extension artifacts and SQL, restart/reload as required by the supported process, resume, and compare results and graph/delta state. Include logical restore/clone isolation as a separate lifecycle case. Retain static SQL checks as complementary coverage.

**Done means:** prior-version state, pending changes, the candidate artifact, the actual procedure, and post-upgrade parity are all present in an executable result record.

## 5. Product truth: one support contract that users can trust

The v0.105.2 README says that `cdc_mode=auto` aliases trigger capture in v0.100 and that WAL capture is unavailable until durable receipt is proven. The release’s configuration source instead defaults to `trigger`, describes `auto` as eligible to transition to receipt-backed WAL, and the capability manifest declares WAL capture stable and enabled. The README also describes row-level trigger capture, while the release code defaults to statement-level capture. [14, 16–17]

These are specific, user-facing inconsistencies, not stylistic objections. A DBA choosing a capture mechanism cannot be expected to discover which paragraph belongs to an older release.

There are broader SQL-support contradictions too. The README advertises recursive CTEs in differential and immediate modes, while the DVM support matrix describes recursive queries as whole-query FULL fallback. The right response is not to assume that the support matrix is always authoritative: reconcile both documents against runnable admission and execution tests on the candidate. [16, 18]

The existing generated capability manifest is a good starting point but is not yet a comprehensive query-strategy contract. It contains four capability declarations and three public-contract examples. Its generator checks that named Rust test functions and reason strings exist and that the listed documentation files exist. It does not prove that those documents agree with runtime behavior. [14–15]

**Recommended change:** extend this existing mechanism with a small set of executable query-family fixtures and generate the user-facing support summary from their observed admission and strategy results. Distinguish:

- Query accepted versus rejected.
- Declared refresh mode versus effective runtime strategy.
- Row-level incremental work versus affected-group or affected-partition recomputation.
- Whole-query FULL fallback versus a local implementation fallback.
- Stable, experimental, unavailable, and merely documented future capabilities.

This is not a request for more SQL coverage. It is a request to describe the current coverage accurately.

The marketing claim that a single inserted row means only one row is processed should also be replaced. The implementation already recognizes potential amplification in pipeline admission; joins, aggregates, and window partitions can require work beyond the input delta. A better statement is that maintenance aims to scale with the affected result and relevant state, subject to the documented strategy. [16, 20]

**Done means:** a user can determine what happens to their query without comparing contradictory documents or reading Rust source.

## 6. Core improvements with limited scope

### 6.1 Expand semantic depth, not operator breadth

The best correctness investment is composition and lifecycle coverage of the enabled feature set. The existing exact-result and strategy oracle is already the right foundation. Add targeted cases for multi-source insert/update/delete cycles, duplicate rows, nullable keys, changes to join/group keys, empty-to-nonempty transitions, and an operator feeding a downstream aggregate. [19]

For lifecycle correctness, combine the established operations with adversarial boundaries: cancellation during refresh, worker restart before and after publication, pending deltas during upgrade, clone/restore rebinding, and consumer acknowledgement boundaries. The point is not a Cartesian product of every option; it is a risk-selected matrix with minimized regressions for each actual defect class.

Use negative controls. A test suite intended to detect wrong results should demonstrably fail when a known faulty implementation or controlled fault is reintroduced. Likewise, strategy checks should fail when an unintended whole-query FULL path masks a differential defect.

The portfolio should contain both broad representative workloads and tiny explainable cases. Broad workloads discover interactions; minimized cases make the semantic contract maintainable.

### 6.2 Remove semantic decisions from string inspection where evidence justifies it

The aggregate correction in PR #1011 is a useful example of a small, high-value refactor: whether a reconstructed fragment has an outer predicate is decided using the operator tree rather than by searching for `WHERE` and inspecting the first character of a string. [5, 7]

Follow this pattern only at similarly fragile boundaries. Carry limited structural metadata alongside generated SQL: visible columns, aliases, outer-predicate presence, relation-state meaning, and identity guarantees. Let rendering consume those facts instead of rediscovering them from text.

Do not make this a complete compiler rewrite. Choose the boundaries implicated by real defects, add tests first, and improve them independently. A small typed SQL-fragment representation can be useful; a new general SQL framework is not automatically an improvement.

For join maintenance, make the distinction among current, pre-change, and unchanged relations explicit in local APIs and tests. Avoid introducing a general snapshot abstraction until the actual shared semantics are clear.

### 6.3 Optimize measured work amplification and database interference

Input change count is not a sufficient cost model. Work can be dominated by the size of affected groups, join fanout, partition recomputation, output rows, or retained state. The support matrix already distinguishes local recomputation strategies, and the pipeline admission code recognizes potential amplification. [18, 20]

The next performance improvement should be selected from a measured production-like workload. Appropriate candidates include excessive affected-key scans, unnecessary materialization, cleanup contention, repeated catalog queries, or poor fallback choices. The result should preserve exact-result and strategy checks and improve total database cost rather than an isolated microbenchmark.

Retain whole-query FULL as a legitimate operational choice. The goal is not to force a differential label everywhere; it is to make the selected strategy understandable and economical. A narrow local recomputation can be better than a complicated stateful optimization. The documented window work is an example: the evaluated stateful candidate lost to partition recomputation, so the runtime did not enable it. [18]

### 6.4 Bound housekeeping cost as history grows

`batch_update_cost_model_summary()` aggregates eligible refresh history since each stream table’s statistics reset and separately computes p95/p99 values using `percentile_cont`. This is a concrete profiling candidate because it revisits retained history rather than simply updating a bounded recent summary. [21]

This is not an observed performance defect. The next action should be to measure scheduler/housekeeping cost as retained history and stream-table count increase. If it becomes material, move to bounded recent windows or incremental summaries while preserving documented statistics/reset semantics. Do not introduce a new scheduling algorithm before establishing that the existing bookkeeping is or is not the bottleneck.

**Done means:** housekeeping cost remains within an explicit workload budget as the system ages, rather than being assessed only on a fresh small database.

### 6.5 Be precise about what “bounded” means

The pipeline uses a cursor and temporary batch relation within one outer transaction. This bounds a delivery/apply unit, but it should not be described as proving that all PostgreSQL executor memory, temporary-file use, transaction duration, or lock retention is bounded by the same setting. The transaction-finalization behavior is explicit in the implementation. [20]

Verify wide rows, amplification, spills, oversized batches, and cancellation at the live database level. Document the difference between an enforceable cap, a soft threshold, an alert, and a fallback trigger. Prefer clearer semantics for existing controls over another set of GUCs.

## 7. Improve adoption without adding another product surface

The project already offers a playground, persona-specific documentation paths, a quickstart, operational references, and a substantial SQL diagnostic surface. Adding another tutorial, dashboard, or command by default risks increasing the number of places that can drift. [16, 22]

The narrower opportunity is to make one existing route reliably lead through the whole lifecycle:

**Evaluate a query → inspect admission and expected strategy → create the stream table → mutate real data → verify parity and freshness → diagnose a stall → suspend/resume safely → upgrade → restore or rebuild.**

Every code block on that route should execute against the packaged candidate. The resulting SQL output should explain the selected strategy, why it was selected, how stale the result is, whether capture is progressing, whether maintenance is blocked, and the supported next action. Reuse the existing health, explanation, backlog, and lifecycle functions; consolidate overlapping guidance instead of adding an umbrella diagnostic framework without need. [22]

A particularly important documentation distinction is the difference between pausing refresh and pausing capture. A copy-pasted maintenance procedure must state whether source changes continue to be recorded, whether backlog accumulates, and whether a resnapshot is required afterwards. This is an operational contract, not a request for a new pause mode.

The release plan calls for independent operators to complete installation, diagnosis, alteration, upgrade, restore, and resnapshot procedures. Automated snapshot/security tests are valuable, but they are not the same as that human-operability exercise. Complete the exercise using the published package and documentation, retain the record, and fix points where an operator needs an undocumented query or maintainer intervention. [9, 12]

A few representative installations are more valuable at this stage than a broad integration launch. Choose users whose workloads remain inside the intended PostgreSQL-native scope. Treat feedback as input to the qualification workload and documentation, not automatic authorization to add a new backend or SQL family.

## 8. A finite execution plan

The stages below are proposed work packages, not commitments to release numbers or dates.

| Stage | Main outcome | Scope limit | Exit evidence |
|---|---|---|---|
| Next maintenance patch | Correct the known post-release DVM issues and close publication gating defects | No new SQL, public contracts, capture modes, or controller actions | Minimized regressions; affected-state guidance; exact candidate qualification; every release channel blocked on failure |
| Qualification/consolidation milestone | Make release claims correspond to executable results | Reuse existing suites, manifests, tools, and operator documentation | Live pending-data upgrade; explicit platform tiers; measured PostgreSQL budgets; runtime-backed support summary; independent operator record |
| Measured core improvement | Fix the most important observed source of cost or complexity | One or two evidence-selected internal changes, not an engine rewrite | Before/after workload results with parity and strategy checks; no foreground-workload regression |
| Later v1.0 decision | Establish the desired long-running and compatibility assurance | Resume only when the project chooses to resume v1.0 qualification | Candidate-bound soak/longevity evidence and a precise supported operating envelope |

A practical backlog can remain small:

| Priority | Work item | Why it is within scope |
|---|---|---|
| P0 | Release PR #1011 with compact regression coverage and state-repair guidance | Corrects existing maintenance semantics |
| P0 | Make all publishing depend on qualification | Fixes existing release enforcement |
| P1 | Bind suite results and measured budgets to individual artifacts | Strengthens the existing evidence contract |
| P1 | Run a live previous-package upgrade with pending changes | Verifies an already-promised lifecycle path |
| P1 | Add a small whole-database qualification workload set | Measures the existing product’s cost |
| P1 | Reconcile README, support matrix, capture defaults, and capability output | Removes contradictory product behavior descriptions |
| P2 | Complete one tested operator journey with independent validation | Makes existing functions usable |
| P2 | Select one SQL-generation or housekeeping improvement from defects/profiling | Reduces implementation risk without enlarging features |

Use outcome-based stop conditions. The phase is complete when claims and evidence align for the chosen support boundary, not when another collection of plans and release-gate scripts has been added.

## 9. Explicit non-goals

Do not expand SQL coverage merely to make a support table look complete. In particular, more sophisticated window maintenance, additional recursive or set-operation paths, and new optimizer branches should require a demonstrated workload need and a correctness/performance case. Existing local recomputation or explicit rejection is often preferable to a weakly qualified path. [18]

Do not turn the core into a distributed streaming or lakehouse platform. The README already presents substantial Citus and DuckLake ambitions. Keep current supported contracts correct and documented, but place new integration behavior in adapters or adjacent projects when possible. Preserve Graph V1 and Delta V1 rather than immediately defining new versions to accommodate every consumer request. [12–14, 16]

Do not add a new capture backend or more autonomous controller actions during this maintenance phase. Keep conservative defaults and qualify the currently admitted paths. Do not change an established default because a microbenchmark improves while foreground workload or recovery behavior remains unmeasured.

Do not make PostgreSQL-major-version expansion or a large internal rewrite a prerequisite to fixing release assurance. Separate compatibility work from the immediate corrective release, and let it proceed only under its own clear support and qualification contract.

Do not equate deleted lines, test counts, or a nearly empty issue tracker with operational maturity. They can be useful local indicators, but correctness under adversarial mutations, explainable fallback, recoverability, and measured database impact are the product outcomes.

## Conclusion

pg_trickle has enough capability to justify a focused consolidation phase. The strongest immediate improvement is to deliver the newly merged DVM correction as a well-tested maintenance release. The strongest structural improvement is to make every claim of support, performance, and package qualification traceable to an executed result on a named artifact.

The development direction should therefore be **fewer surprises, not more features**: fewer ambiguous support claims; fewer unverified release assumptions; fewer semantically fragile SQL-generation boundaries; fewer unexplained fallbacks; and fewer operational procedures that require maintainer knowledge.

A successful next phase leaves the product’s purpose essentially unchanged while making it much easier to trust.

## Sources

All repository material below is published by trickle-labs/pg-trickle. The release-source baseline is v0.105.2; post-release PRs and branch comparisons are identified separately. Sources were assessed as of 13 September 2026.

1. [Release v0.105.2](https://github.com/trickle-labs/pg-trickle/releases/tag/v0.105.2), published 11 September 2026; [latest-release metadata](https://api.github.com/repos/trickle-labs/pg-trickle/releases/latest).
2. [Annotated v0.105.2 tag object](https://api.github.com/repos/trickle-labs/pg-trickle/git/tags/0ca25d41bfa13cab089855636b5382dbb0bf6f75), 11 September 2026; resolves to commit `33df4cc91fda4fbadba79470347a714c8509703a`.
3. [Release-to-inspected-main comparison](https://github.com/trickle-labs/pg-trickle/compare/33df4cc91fda4fbadba79470347a714c8509703a...69ceca8b94f0951614065636880017f6a3538bfa), inspected 13 September 2026.
4. [PR #1011: Correct TPC-H differential refresh](https://github.com/trickle-labs/pg-trickle/pull/1011), merged 13 September 2026.
5. [PR #1011 changed files and patch](https://github.com/trickle-labs/pg-trickle/pull/1011/files), 13 September 2026.
6. [`src/dvm/operators/semi_join.rs`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/src/dvm/operators/semi_join.rs), v0.105.2.
7. [`src/dvm/operators/aggregate.rs`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/src/dvm/operators/aggregate.rs), v0.105.2.
8. [`tests/release/v0.105.2-qualification.json`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/tests/release/v0.105.2-qualification.json), v0.105.2.
9. [`.github/workflows/release.yml`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/.github/workflows/release.yml), v0.105.2; especially `qualification`, `publish-release`, `publish-docker-arch`, and `publish-docker`.
10. [`scripts/release_evidence.py`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/scripts/release_evidence.py), v0.105.2.
11. [`scripts/run_benchmarks.sh`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/scripts/run_benchmarks.sh), v0.105.2.
12. [`plans/PLAN_0_105_2.md`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/plans/PLAN_0_105_2.md), v0.105.2 implementation and acceptance plan.
13. [`roadmap/v0.105.2.md`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/roadmap/v0.105.2.md), released milestone and deferred qualification boundary.
14. [`docs/capability-manifest.json`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/docs/capability-manifest.json), v0.105.2.
15. [`scripts/generate_capability_manifest.py`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/scripts/generate_capability_manifest.py), v0.105.2.
16. [`README.md`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/README.md), v0.105.2.
17. [`src/config/cdc.rs`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/src/config/cdc.rs), v0.105.2 capture defaults and modes.
18. [`docs/DVM_SUPPORT_MATRIX.md`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/docs/DVM_SUPPORT_MATRIX.md), v0.105.2; support declarations require reconciliation where they conflict with other sources.
19. [`tests/e2e_diff_full_equivalence_tests.rs`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/tests/e2e_diff_full_equivalence_tests.rs), v0.105.2.
20. [`src/refresh/pipeline.rs`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/src/refresh/pipeline.rs), v0.105.2.
21. [`src/refresh/orchestrator.rs`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/src/refresh/orchestrator.rs), v0.105.2; `batch_update_cost_model_summary()`.
22. [`docs/OPS_CHEATSHEET.md`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/docs/OPS_CHEATSHEET.md), v0.105.2.
23. [PR #1008: Prune dead code and tooling](https://github.com/trickle-labs/pg-trickle/pull/1008), merged 12 September 2026.
24. [PR #1010: Remove unused and duplicate code](https://github.com/trickle-labs/pg-trickle/pull/1010), merged 12 September 2026.
25. [`ROADMAP.md`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/ROADMAP.md), v0.105.2; later v1.0 qualification and release deferral.
26. [`scripts/v0_105_2_release_gate.py`](https://github.com/trickle-labs/pg-trickle/blob/v0.105.2/scripts/v0_105_2_release_gate.py), v0.105.2; structural and function-existence checks.
