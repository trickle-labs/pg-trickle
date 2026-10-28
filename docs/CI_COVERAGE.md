# CI correctness allocation

The PR suite protects committed results through real PostgreSQL execution.
The inventory is checked by `python3 scripts/ci_pr_suite.py --list`: every
`tests/e2e_*.rs` target must be in the light allowlist, the explicit full
list, the database-free `pr_pure` list, or a documented scheduled tier. New targets fail the static contract
until allocated. The source of truth is
[ci-suites.json](../.github/ci-suites.json) plus
[the light allowlist](../scripts/run_light_e2e_tests.sh).

## What each layer proves

| Behavior | Pure Rust protection | PostgreSQL component protection | Installed-extension protection retained before merge |
|---|---|---|---|
| Operator planning, SQL construction, frontiers | `src/dvm/**`, `src/version.rs`, checked LSN proof | Executes generated delta SQL, not just SQL string assertions | Light parser/create/CTE/subquery/expression tests use the real PostgreSQL parser |
| INNER, LEFT, FULL and natural joins; simultaneous changes | Join operator unit tests | `dvm_join_tests`, `dvm_outer_join_tests`, `dvm_full_join_tests`, natural/nested join suites; EC-01 deletion retractions | Light join/full-join/EC-01/differential-gap tests; full corpus and sensitivity regressions |
| Semi/anti joins, NULL absence | Semi/anti operator unit tests | `dvm_semijoin_antijoin_tests` | Light subquery, IN, HAVING and lateral suites |
| Aggregate updates, extrema deletion, rescans | Aggregate decision and SQL tests | `dvm_aggregate_execution_tests`: COUNT/SUM/AVG, filters, extrema, ordered/JSON aggregates | Light aggregate/null-group/HAVING suites; full SUM(CASE), full-join aggregate and external correctness gate |
| Window frames and scalar subqueries | Window strategy/type checks | `dvm_window_scalar_subquery_tests`: partitions, moves, global/frame recompute, shared-source scalar changes | Light window/multi-window/scalar suites; full window recovery |
| Duplicate multiplicity and exact identities | Z-set and row-identity tests | Join fan-out and signed I/D result rows | Full keyless/duplicate/graph/row-ID regressions, Z-set property traces and exact-oracle negative controls |
| CDC source-transaction atomicity | Frontier and capture decision tests | Synthetic buffers only; cannot prove triggers or transactions | Light statement/row CDC and edge cases; full WAL CDC, receipts, publication crash recovery and WAL transition |
| Transaction visibility and committed-change durability | LSN proof and pure decision logic | Cannot prove snapshot acquisition or commit boundaries | Full long-transaction visibility, snapshot consistency, refresh atomicity, strategy transaction barrier and failure recovery |
| Scheduler, worker lifecycle, DAG visibility | Scheduler/DAG decision tests | No workers | Full bootstrap/watermark gating, background workers, circular/immediate/mixed-mode scheduling, DAG auto-refresh and property snapshots |
| Recovery, interrupted finalization, cancellation | Recovery classification tests | No crash/restart lifecycle | Full failpoint, cleanup chaos, error state, window recovery, publication crash and refresh-atomicity suites; relevant changes also repeat every witnessed schedule twice without retries; ten passes run nightly |
| Ownership, RLS, grants, permissions | Privilege-boundary/static SECURITY DEFINER contracts | Catalog DDL checks only | Light ownership/RLS; full ownership, safety, RLS/WAL, versioned security regressions, MAINTAIN grants, guard and replica boundaries; relevant unsafe paths keep ASan |
| Public SQL API, catalogs, DDL, dump, upgrade | Static API/catalog/release contracts | Existing `catalog_tests` etc. install generated DDL | Light create/alter/drop/schema/dump tests; full versioned conformance and upgrades; shipping-image upgrade stays |
| Release/candidate identity | Release evidence and LSN rejection controls | Cannot attest installed code | Existing release proof controls, light/full LSN runtime and candidate binding stay separate and unchanged |

The component layer constructs `OpTree` values and synthetic change buffers;
its identity helpers are SQL stand-ins. It does **not** load the extension,
parse user SQL, run capture triggers, acquire refresh snapshots, or start
workers. Catalog integration similarly installs generated DDL rather than
the packaged extension. Neither substitutes for the installed tests above.

## Ready PR budget and retained harness differences

For runtime or harness changes, Linux unit/catalog/component checks run even
on drafts. Ready PRs add the existing 91-target, three-shard light suite and
an explicit 100-target full suite across two duration-balanced runners and one database-free harness target. The old sensitive DVM gate selected all 161
E2E targets in addition to light E2E. The new full suite retains permanent
corpus, identity, versioned security and conformance regressions in both
harnesses. All targets with `cfg(not(feature = "light-e2e"))` stay in the full
suite, including watermark gating, IMMEDIATE mode, mixed DAGs, multi-cycle
refresh, failure recovery and checked LSN cases. The inventory validator
rejects an allocation that drops these full-only cases.

Full coverage also retains worker interactions without compile-time guards:
CDC/TRUNCATE and statement capture, concurrent and reconfigured DAGs,
scheduler suppression, shared-memory monitoring, GUC reload and lifecycle
operations. A full-harness replay observed an exact-row mismatch in the DAG
schedule-change case that passed on retry. That target stays pre-merge;
light execution alone does not establish equivalent worker protection.
The bounded full suite disables test retries so a correctness failure cannot
be hidden by a later successful attempt. Investigate a failed run before
rerunning it; ordinary infrastructure retries remain an explicit operator
decision rather than an automatic correctness verdict. The RLS/WAL owner-mode
case, seven WAL receipt-recovery cases that use advisory-lock witnesses, the
D+I insert oracle, the automatic WAL-transition case, two LSN scheduler cases,
the fixed-snapshot LSN case, the monotone circular fixpoint case, the
three-layer no-spurious-refresh witness, the STDDEV_SAMP update oracle, and
the revoked-source permission witness run one thread at a time after the
parallel full suite. This preserves their exact
assertions while isolating shared worker state and barrier timing from parallel
E2E load. The long-writer recovery wait is 120 seconds and WAL transition
witnesses wait up to 180 seconds under hosted-runner load; assertions and
zero-retry behavior are unchanged.
The paired full allocations also run one at a time so restart and cutover
assertions do not overlap.

The remaining 50 light targets use the installed extension for parser,
manual-refresh, DDL and operator checks. Their second full-harness invocation
is removed; their assertions remain in light E2E and nightly full E2E.
No installed-extension assertion is replaced solely by a component test.
Components add earlier, more focused operator feedback in the existing
integration job, without another setup/cache job.

All package consumers clear the cached package directory and download into
`target/release/pg_trickle-pg18`. Inspection of the existing Actions artifact
confirmed that its root is `usr/...`; downloading one directory higher could
trigger repackaging or leave consumers selecting a cached package instead.
The producer also clears that directory before packaging. Release controls
and candidate binding therefore consume the downloaded artifact at the same
path as the light shards.

The full suite keeps the metamorphic batching/multi-source history regression
and the strategy transaction-barrier/resource-boundary test. Only the named
exhaustive live rewrite and corpus-mode cross-product cases move to scheduled
tiers. The fixed seeded diamond topology combinations also move; deterministic
diamond regressions and DAG/snapshot/Z-set property traces remain on PRs.
Refresh-atomicity witnesses run serially without retries after shard 1's parallel
suite, preserving their isolation from its runner's container load. Shard 2 runs
on a separate runner. Selected failure-recovery paths additionally run two
isolated passes of all 13 witnessed schedules on shard 1. Ten passes run daily
and manually in `production-recovery-qualification`; all attempt logs are retained
and any failed attempt fails the job. This defers additional timing exploration,
not a unique failure scenario or assertion.

Drain-mode and long-transaction visibility binaries also run after the parallel
shard because both alter shared server settings and require scheduler progress.

Runtime selection uses [.github/ci-paths.yml](../.github/ci-paths.yml), with
unclassified inputs retaining runtime coverage. Ordinary docs changes avoid
extension builds. Mixed changes select the union. The bounded full suite now
runs for **every** ready runtime PR, including scheduler and API changes that
previously received only smoke coverage. Failure-recovery repetition selects
refresh, CDC, IMMEDIATE, SQL, scheduler, relevant tests and harness/allocation
changes. Unsafe-path selection and instrumented checks remain in place.

## Scheduled allocation and failure ownership

| Deferred work | Explicit owner workflow / cadence |
|---|---|
| Every E2E target, including removed full-harness duplicates, all ordinary property tests and both exhaustive cases | `ci.yml`, daily 03:00 UTC and manual full matrix |
| DVM live metamorphic combinations | `dvm-tiers.yml`, daily 02:30 UTC and manual nightly/weekly |
| DVM corpus × refresh/CDC strategy combinations | `dvm-tiers.yml`, Monday 04:00 UTC and manual weekly; also daily full CI |
| Seeded diamond topology combinations | Daily/manual full `ci.yml` |
| Ten isolated passes of every witnessed failure-recovery schedule (two remain on sensitive PRs) | `ci.yml` production recovery job, daily 03:00 UTC/manual; 14-day witness artifacts |
| Ignored Nexmark and TPC-H DAG workload combinations | `dvm-tiers.yml`, Monday 04:00 UTC and manual weekly (DAG SF-0.01) |
| TPC-H scale matrix | `tpch-nightly.yml`, daily/manual; `ci.yml` retains its ignored TPC-H step |
| Ignored refresh/CDC performance matrices | `e2e-benchmarks.yml`, Sunday 02:00 UTC/manual |
| DAG performance matrices | Daily/manual `ci.yml` DAG benchmark jobs |
| Soak, multi-database and Citus chaos | Daily/manual `stability-tests.yml` |
| Extended fuzz exploration | `fuzz-nightly.yml` daily and `dvm-tiers.yml` extended weekly; ready-PR corpus replay stays |

Repository maintainers own scheduled-failure triage. The author of the
introducing change owns the fix once identified. A correctness failure must
be reduced to a permanent regression in the relevant PR tier before merging
the repair. Preserve the failing SHA, seed and artifacts; rerun suspected
infrastructure failures against the same SHA. Do not make a correctness
failure green by deleting its check or adding retries. Existing release and
candidate-binding contracts remain mandatory for their respective workflows.

## Gate and workflow verification

`PR correctness` uses `always()` and inspects all pre-merge CI dependencies.
Every selected check must report `success`; failure, cancellation and
unexpected skip fail the aggregate. Draft/doc-only omissions are explicit.
Matrix failure/cancellation is propagated through `needs.light-e2e-tests` and
`needs.e2e-sensitive-gate`. The static contract job also runs the pinned proof;
its success is required before packaging and candidate binding.
`ready_for_review` and `converted_to_draft` remain trigger events; concurrency
cancels superseded runs, with event names separating pushes from schedules.

Tests in `scripts/test_ci_pr_suite.py` cover draft → ready → draft selection,
docs/shipping/monitoring inputs, every dependency's failure/cancellation/skip,
missing inventory entries, full-only cases and runner exit propagation.
Actionlint validates expressions and dependency wiring. These are local
contract checks; a real ready-PR run additionally verifies GitHub scheduling.

On 2026-10-18, REST inspection found no classic branch protection and the
active `Protect main` ruleset had no required status checks. This PR changes
no repository settings. Maintainers should make **`PR correctness`** a required
status check after its first successful run, retaining separately required
lint/security/release checks if configured later. A reported CI failure does
not itself configure GitHub to prevent merging.

## Investigation and focused improvement (2026-10-22)

### Baseline and limits

Read-only REST inspection of `branches/main/protection`, `rulesets/13289237`
and `rules/branches/main` still found **no required status checks**. The active
ruleset requires a PR and linear history and blocks deletion/force-push.
No settings were changed. Consequently, a literal required-check time to green
is undefined; the useful measure below is the terminal CI verdict, cross-checked
against the other workflows for the same PR update.

Inspected 100 completed PR CI run records, then job/step timestamps for 12
selected runs on October 19–22: 11 ready runs (four CI successes, seven failures)
and one draft. Readiness was checked against PR timeline events, not today's
draft flag. This is a small, purposive sample, with repeated updates to one PR,
not an unbiased repository-wide distribution. Timings start at workflow event
creation, the available proxy for the PR update; webhook delivery latency is
not available in these responses. Endpoints are job completion timestamps,
not the later workflow `updated_at` timestamp.

| Ready sample | CI verdict minutes | CI runner minutes | All triggered workflows green? / total runner minutes |
|---|---:|---:|---|
| [#1190 harness/allocation](https://github.com/trickle-labs/pg-trickle/actions/runs/39253819455) | 88.02 | 152.82 | Yes / 156.45 |
| [#1191 sensitive runtime, Oct 20 morning](https://github.com/trickle-labs/pg-trickle/actions/runs/39386589710) | 79.27 | 149.40 | **No: separate S1 qualification failed** |
| [#1191 sensitive runtime, Oct 20 evening](https://github.com/trickle-labs/pg-trickle/actions/runs/39472934464) | 68.17 | 150.30 | Yes / 164.22 |
| [#1191 sensitive runtime, Oct 22](https://github.com/trickle-labs/pg-trickle/actions/runs/39778269500) | 69.32 | 160.15 | Yes / 174.60 |

The two all-green sensitive-runtime updates have a median of 68.75 minutes
and a nearest-rank p90 of 69.32 minutes, but both are from **one PR**. Including
the harness sample gives median 69.32 and p90 88.02 (n=3). Neither estimates an
ordinary-runtime population reliably. The three latest inspected ordinary DVM
updates all failed: [FILTER](https://github.com/trickle-labs/pg-trickle/actions/runs/39776762344),
[COUNT](https://github.com/trickle-labs/pg-trickle/actions/runs/39773610952), and
[USING](https://github.com/trickle-labs/pg-trickle/actions/runs/39772678805).
Their first failed steps ended at 50.75, 53.65 and 65.77 minutes; terminal
verdicts arrived at 69.05, 84.27 and 88.53 minutes. These are failure latency,
not time to green. In the FILTER log the actionable assertion appeared about
five seconds before the failed step ended.

The other retained failures are runs
[39567538745](https://github.com/trickle-labs/pg-trickle/actions/runs/39567538745),
[39680531412](https://github.com/trickle-labs/pg-trickle/actions/runs/39680531412),
[39743634653](https://github.com/trickle-labs/pg-trickle/actions/runs/39743634653), and
[39744582143](https://github.com/trickle-labs/pg-trickle/actions/runs/39744582143).
Across all seven failures, the median first-failed-step latency was 55.28 minutes.
Two ASan jobs timed out after roughly 120 minutes. No failures were discarded
to obtain a faster passing comparison.

The apparently successful [#1197 run](https://github.com/trickle-labs/pg-trickle/actions/runs/39387305034)
was a **draft** (23.43 minutes, 11.82 CI runner minutes); its later ready event
failed. The inspected OpenWiki PRs also changed runtime source/tests, so they
are mixed changes, not docs-only samples. There is no docs-only hosted timing
sample in this set. Local selection checks cover docs-only and mixed paths.

### Where the time goes

In run 39778269500, the critical path was contract → sensitive-path detection →
full E2E → verdict. The full job started 15.18 minutes after the event and ran
53.97 minutes. Before it started, dependency execution/release consumed 4.10
elapsed minutes and its own runner wait consumed 11.08 minutes. Its steps were:

| Full job phase | Minutes |
|---|---:|
| Disk cleanup | 3.60 |
| pgrx setup, warm cache | 0.47 |
| Docker image build (including extension compilation) | 4.95 |
| Rust test compilation | 6.58 |
| Parallel full tests: 1,224 cases | 24.32 |
| Isolated atomicity: 12 cases | 0.60 |
| Ten witnessed recovery repetitions | 11.98 |

The mixed-scheduling target alone consumed 1,231.90 summed test-seconds;
individual diamond/chain cases took 186–187 seconds. Several waited 60 or 90
seconds for another `data_timestamp` advance after the relevant refresh had
already completed, then manually refreshed on timeout. Those waits both cost
time and could hide a broken scheduler. This is observed duration plus a
source-grounded explanation, tested by the local comparison below.

The other lane was contract → LSN proof → package → three light shards and
release controls; candidate binding additionally required the proof directly.
Proof execution was only 0.48 minutes, but its queue delayed package eligibility
until 12.20 minutes after the event. Packaging used 4.13 minutes. Light shard
jobs took 9.35/14.23/10.22 minutes, including cleanup of 2.23/5.92/1.30 minutes.
Shard 2 compiled for 3.23 minutes and executed 458 tests in 3.15 minutes. Count
alone does not explain the imbalance. The package artifact was 6.16 MiB and
individual upload/download steps were below six seconds. Full-image and host
package builds repeated extension compilation, but use different build/runtime
environments; substituting packages requires proving ABI and full-image parity.

The four inspected full/light/component logs restored exact cargo registry,
target, cargo-pgrx and pgrx-home cache keys. The successful full and light logs
showed no test retries; all sampled run attempts were attempt 1. Full execution
still compiled for 6.58 minutes despite the cache hit. Integration spent 4.02
minutes in catalog tests and 1.72 in DVM components, including ten separate
Cargo invocations. Setup was generally 0.4–0.8 minutes. Summed job runner wait
was 129.18 minutes in this run; it is **not** elapsed wall time or runner cost.

Lint, docs drift, unsafe inventory, secret scanning, Semgrep and fuzz replay
run independently of CI. Dependency/security workflows are selected for their
own inputs; Build and CodeQL do not currently run on PRs. The sampled S1 branch
also had a separate qualification workflow absent from main. In the three
all-green samples CI finished last. `PR correctness` aggregates its CI jobs,
not these independent workflows. Workflow creation-to-job-creation is reported
as release delay; creation-to-start is reported separately as a runner-wait
proxy. Neither proves the cause of runner queueing.

### Changes and coverage

| Rank / change | Expected benefit | Confidence, effort and coverage risk |
|---|---|---|
| 1. Observe exact scheduled results instead of waiting for a second timestamp and manually repairing | Remove long no-op waits from the dominant test target | High confidence in the race; small fixture change; stronger scheduler assertion |
| 2. Fold sensitive-path detection into the existing static selector | Remove one queued job before full E2E/ASan; observed contract-to-detection delays were 2.47, 12.65 and 17.45 minutes in the all-green samples | High confidence; move the same filters; hosted queue effects remain unmeasured |
| 3. Move structural scenario/shrink assertions to the existing Rust job | Earlier actionable harness failures, without waiting for Docker/image build | High confidence; small allocation change; negligible claimed total-run saving |

Earlier packaging was investigated and rejected: `check_lsn_verification.py`
explicitly requires proof before candidate package construction. That dependency
and the independent candidate-binding dependency remain intact, now pointing to
the consolidated contract/proof job described below. Starting packaging before
proof completion remains prohibited.

The mixed-scheduling helper now polls the existing exact schema/multiset oracle
against the original fixture SQL, in topological order. It never triggers manual
refresh in scheduled modes.
The original DML, transaction, view, serial/parallel-worker and final-result
assertions remain installed-extension tests. A regression check proves the
helper cannot repair stale rows with scheduling disabled and accepts rows that
were already refreshed. No timeout was shortened, concurrency raised, retry
added, or recovery/atomicity isolation changed.

`e2e_dvm_fuzz_tests` is explicitly allocated to `pr_pure`, executed by
`ci_pr_suite.py --pure` in the existing unit job, including on runtime drafts.
It protects scenario validation, generator/coverage/mutation logic and structural
shrinking that must preserve the injected defect while removing padding cycles
and mutations. These assertions use Rust values and fixture JSON only, so they
can detect the same regressions without PostgreSQL. The pure target's 17 tests
pass with `DOCKER_HOST` pointing at a nonexistent socket. The live original
negative-control replay and its exact `GeneratorInvalid` assertion remain in
`e2e_dvm_shrink_tests`; corpus and negative-control installed tests also remain.
Nothing here substitutes synthetic buffers or stand-ins for parser, trigger,
transaction, snapshot, worker or recovery coverage. No assertion moves later.

The allocation validator rejects overlap and obvious database tests in the pure
tier. Sensitive filters now live in `ci-paths.yml`, with outputs from the existing
contract job. Their old patterns are preserved; editing the selector additionally
selects both recovery and ASan qualification. Explicit draft/runtime conditions
replace the removed selector job's implicit skip propagation. Gate tests cover
these conditions while retaining proof before package construction and in candidate
binding and the terminal verdict. Missing dependency records now produce an
explicit failing verdict, alongside failed, cancelled and unexpectedly skipped
results. The metrics script adds wall time, successful-run time to green,
first-failed-step latency, release delay and step durations without confusing
these with summed runner minutes.

More shards, cross-job test archives, broad fixture/container sharing and further
suite exclusions were rejected for this change: observed runner waiting is large,
warm setup is small, light features differ from full features, and worker/cluster
isolation is part of the coverage. Component-runner batching and disk cleanup
are plausible follow-ups, but neither has a matched measurement here.

### First-pass validation and matched experiment

The local comparison used production source `00d21749bc20af8b19940b857fae461e0ccfe471`
and one freshly built PostgreSQL 18.6 image, `pg-ci-pyramid:baseline`
(`sha256:39690461fe55a958f8d195a63860500e683d5da711c1442e4bc0ef35ca6abded`).
The Rust clients ran on macOS ARM64; PostgreSQL ran as linux/amd64 under Docker
emulation. Both variants used the same 30 scenarios, one process per test, four
concurrent processes, warm compiled binaries and no retries. Compilation is
excluded. This is one sequential pair, not a hosted median/p90 experiment.

| Local mixed-scheduling comparison | Original | Corrected candidate |
|---|---:|---:|
| Passed scenarios | 30/30 | 30/30 |
| Wall seconds / single-host elapsed minutes | 286.81 / 4.78 | 56.33 / 0.94 |
| Summed test seconds (not elapsed time) | 1,012.26 | 211.32 |
| Individual-case nearest-rank p90 seconds | 97.46 | 8.26 |

The measured suite reduction is **80.4%**, or 230.47 seconds. This does not
establish PR median or p90 improvement. As an explicitly unmeasured illustration,
transferring that local delta and removing the observed 2.47-minute selector
delay from run 39778269500, while holding all other queues/costs fixed, would
put its roughly 69-minute verdict near 63 minutes and total runner cost near
171 rather than 175 minutes. That is nowhere near a proven 50% PR reduction.
Earlier eligibility can change contention; hosted p90 still needs measurement.

Failed attempts remain evidence: the first candidate finished in 55.84 seconds
with **28/30 passing**, and its normal E2E invocation stopped with two failures
and 20 unrun cases. Using the catalog's deparsed query lost the NUMERIC(10,2)
type modifier. Passing each fixture's original SQL fixed the comparison while
retaining the oracle's strict schema checks. Neither failed attempt is counted
as a passing speedup. The first image build reported an architecture mismatch
and was replaced by a consistently amd64 build before measurements. Three
socket-based unit tests initially failed under sandbox networking restrictions;
the unchanged suite passed with local socket access.

| Validation | Result |
|---|---|
| `just fmt`, then `just lint` | Passed, zero warnings |
| Pure Rust unit suite | 2,615 passed |
| Database-free harness with unavailable Docker socket | 17 passed |
| Python script tests, including allocation and verdict contracts | 35 passed |
| Normal full E2E runner, affected scheduling + live shrink targets, retries 0 | 48 passed, 0 skipped, 60.225 seconds test execution |
| All workflow YAML through actionlint 1.7.7 | Passed (embedded shellcheck disabled; no shell changes) |
| Actual picomatch selection checks | Docs, mixed, runtime, harness, allocation, recovery, unsafe and unknown inputs passed; original sensitive patterns preserved |
| Draft-to-ready, failed/cancelled/missing/unexpected-skip verdicts | Passed contract tests, including missing selection flags |

The package, light test runner, DVM operator code and full-image contents are
unchanged. In this first pass, full unmodified suites and ASan/recovery repetitions
were not rerun locally; follow-up validation is recorded below. Logs, saved REST responses, failed attempts
and per-case timing JSON are retained locally under `target/ci-pyramid-20261022/`.

The 50% median improvement remains a target, **not a measured result**. To assess
hosted median/p90, compare at least ten ready runtime updates per variant, split
ordinary and subsystem-sensitive inputs. Use the same production source, exact
package candidate, runner labels, feature sets, recovery flags and retry settings;
record cold/warm cache keys and keep failed/cancelled attempts. Collect jobs from
every workflow on the update, not only CI. Do not dispatch the broad manual
matrix as a substitute for the ready-PR graph. The proposed controlled experiment
is two temporary ready PRs from `00d21749`: a control with only a workflow comment
change (to select the harness gates) and this candidate, with no production-source
differences. Alternate ten comment-only workflow updates per branch, completing
each run before the next; record the exact SHA and every failed/cancelled attempt
without replacing failures with successful retries. Use the REST capture commands
below for CI and every companion workflow on each update. This costs roughly
3,400 runner minutes at the observed rate, before failures, and requires separate
authorization. Creating this PR exercises the candidate graph once; it does not
establish the requested hosted median/p90 result.

### Follow-up toward a 30-minute PR budget

The first candidate run [39825485460](https://github.com/trickle-labs/pg-trickle/actions/runs/39825485460)
was still queued when this follow-up began. It supplies no after measurement.
The earlier 80.4% local improvement applies only to mixed scheduling tests.

The next changes address execution and the package dependency chain together:

| Rank / change | Observed cost and expected effect | Coverage and tradeoff |
|---|---|---|
| 1. Run the unchanged pinned Verus baseline and semantic mutation control at the end of the existing static contract job | In run 39778269500 the 29-second proof job waited 10.10 minutes after the static job completed. Consolidation removes that queue boundary, while adding the proof execution to the selector job | Proof still runs on drafts and docs-only changes. Packaging and candidate binding depend on its successful job; artifact identity and rejection checks are unchanged. Mutation tests reject skipped/ignored proof steps and packaging that bypasses proof |
| 2. Keep two isolated recovery passes on sensitive PRs; run ten nightly | The ten-pass step took 11.98 minutes; eight fewer passes estimates 9.58 minutes less PR execution | Every one of the 13 schedules, SQLSTATE, cleanup assertion, lock witness and negative control remains. Every atomicity case also stays serial in the ordinary full suite. Additional opportunities to expose timing-dependent faults move to daily 03:00 UTC/manual production recovery qualification. This reduces pre-merge repetition confidence, not scenario coverage; maintainers own nightly failures |
| 3. Two full-suite runners, balanced by observed duration | After the scheduler fix, the old parallel tests represent about 20 minutes of four-slot execution. Two groups estimate 11.6 minutes each including shard 1's isolated work | Every target runs exactly once across the pair. The second runner repeats image/setup and common compilation work; no assertion, timeout or local concurrency limit changes. Matrix `fail-fast: false` preserves evidence from both runners |
| 4. Clean installed SDKs only when less than 60 GiB is available | Light shard 2 started with 87 GiB free, then spent 5.92 minutes deleting 22 GiB. The three light shards spent 9.45 minutes total in cleanup; full E2E spent 3.60 and release controls 3.67 | `ci_prepare_disk.sh` keeps the previous SDK cleanup on tighter Linux Actions runners and logs disk space. The 60 GiB trigger conservatively covers the documented 30+ GiB build cache plus Docker/test growth. Low-space and high-space branches are tested without deleting real files |

`pr_full_shard_2` in `ci-suites.json` is a static subset of `pr_full`; everything
else goes to shard 1. The validator rejects unknown/duplicate targets, empty
partitions and moving atomicity into the second shard. New explicitly allocated
targets conservatively enter shard 1. The allocation tests prove the union is
the entire full list, with no overlap, and preserve serial atomicity and both
shards' failure/cancellation propagation. `PR correctness` requires the matrix
aggregate to succeed, including its existing unexpected-skip rejection.

The partition uses per-target successful test durations from run 39778269500,
with the mixed-scheduling weight scaled by the measured local 56.33/286.81 ratio.
Shard 1 is reserved for atomicity, failure recovery and failpoints; the remaining
targets were assigned longest-first to the lighter lane. Atomicity (36 seconds)
and two recovery passes (719/5 seconds) were charged to lane 1 serially. The
result is 48/51 targets, approximately 2,784/2,782 four-slot-equivalent seconds.
The all-ignored WAL and feature-gated unsafe targets had no ordinary-run timing;
their allocation and separate qualification remain unchanged. These are initial
weights, not hosted after measurements; rebalance only after observing both lanes.

This does **not** establish a 30-minute gate. In the same baseline, independent
release-proof controls finished at minute 58.77 and light shard 1 at minute
54.22. They would become the longest path if only full E2E were accelerated.
Holding the observed queues fixed, proof consolidation advances the package
chain by roughly 10.10 minutes. Avoiding release-control cleanup would remove
another 3.67 minutes, giving an illustrative overall result near
**45 minutes**, not 30. The full branch estimates roughly 35–39 minutes,
depending on how much host compilation is shared versus target-specific. These
estimates assume both full runners are available with similar waits; an extra
runner can instead delay package consumers.

For runner cost, the extra full runner repeats roughly 6.9 minutes of setup,
image build and other overhead after avoiding cleanup, plus an uncertain amount of common compilation.
Removing eight recovery passes saves about 9.6 minutes per sensitive PR, and the
scheduler fix saves about four. If the new runners also have ample space, the
combined projection is roughly 151–158 runner minutes versus the 174.60-minute
baseline. Low-disk runners still pay the original cleanup cost. Nightly gains
about 12 minutes of repetition plus any additional test compilation in an existing
job, without an additional setup job. Measure
all-workflow cost as well as wall time; do not add a third shard without evidence.
Cold proof-image pulls now delay release of the other runtime jobs, so they
must be separated from the warm baseline when evaluating p90 and first-failure latency.

Binary archive reuse was considered but would add a producer dependency, artifact
transfer and compatibility checks; it is not needed for this small change.
Broadly deferring property/worker suites was rejected: their individual observed
costs are small beside setup/queueing, and they exercise real extension boundaries.
Production code and candidate package construction are unchanged. The failpoint
fixture correction below retains the failed-attempt witness and strengthens
the committed-mode check with a deterministic precondition.

The controlled hosted experiment above still applies, now including both full
lanes and nightly repetition evidence. Compare ready-runtime and sensitive
updates separately, retain failed attempts, and check median, p90 and total
runner minutes. The 30-minute goal and original 50% reduction remain unverified.

Follow-up validation uses the same unchanged production image and macOS ARM64 /
emulated linux/amd64 setup as the first pass. The two groups run sequentially on
one local host to preserve witness isolation; their maximum is not a measured
two-runner wall time. `cargo nextest list --message-format json` applied to the
previous selector and both new selectors proved exactly 1,213 selected cases
before and after, with zero overlap and the identical 31 existing skips. The
full/pure/scheduled target and case allocations were also compared with the
previous commit and are unchanged.

The pinned Verus baseline passed locally and its semantic mutation was rejected.
All 44 Python script checks passed, including proof bypass mutations, selection,
verdicts, both shard exits, witness log retention and guarded disk preparation.
All workflows passed actionlint; both changed shell scripts passed `bash -n`.
The ruleset was re-read before changing job names and still contained no required
status checks. No repository settings were changed and no workflows were manually dispatched.

| Follow-up local execution | Result |
|---|---|
| Full shard 1, before the failpoint fixture correction | 561/561 passed, 807.566 seconds test execution; then 12/12 atomicity checks passed serially in 57.066 seconds. Whole invocation: 912.79 seconds including compilation |
| Full shard 2 | 640/640 passed, 814.264 seconds test execution; whole invocation 856.54 seconds including compilation |
| Initial two-pass recovery probe | First pass 13/13 passed; second failed the failpoint case (`NO_DATA` versus `DIFFERENTIAL`) and left 12 cases unrun. This probe is not green |
| Controlled reproduction, old assertion | Failed deterministically after explicitly establishing a completed scheduler `NO_DATA` refresh |
| Corrected failpoint target | 14/14 passed without retries, 13.585 seconds |
| Corrected two-pass recovery probe | 13/13 passed in each isolated pass, 26 total; 201.24 seconds whole invocation, no retries |

The recovery probe found an incorrect fixture assumption: `effective_refresh_mode`
describes the last **completed** refresh, updated by `refresh::finalize_success`.
The scheduler's injected failure occurs before apply and records
`action=DIFFERENTIAL actual_path=NOT_REACHED`; it must not overwrite that committed
mode. A preceding successful no-data tick therefore legitimately leaves `NO_DATA`.
The fixture now waits for that explicit completed scheduler baseline, then asserts
the failed attempt is still DIFFERENTIAL and the committed mode remains exactly
NO_DATA. Requested/admitted mode, SQLSTATE, error marker, suspension, checkpoint
multiset preservation and recovery convergence assertions all remain. The old
assertion failed under this controlled precondition before being corrected; it was
not replaced by accepting either mode or by retrying until green.

Logs, both failing attempts, selection JSON and expected negative-control artifacts
are retained under `target/ci-pyramid-round2-20261022/`. The full replay predates
this final fixture correction; the affected target and isolated recovery passes
are revalidated separately. Light E2E, ASan and the ten-pass nightly job were not
rerun locally; their existing/new destinations remain selected as documented.
Both corrected passes succeeded; this does not erase the failed initial probe.
Including serial atomicity and the corrected two-pass probe, local shard 1 used
18.57 minutes versus shard 2's 14.28. This differs from the hosted duration weights;
do not retune the hosted allocation solely from an emulated local host.

## Historical measurements before this improvement

The issue's [observed baseline run](https://github.com/trickle-labs/pg-trickle/actions/runs/39047052638)
at `ceca8f6aa47a8738416e36072aae8e215134f194` is historical, before the
initial scheduling change in #1189. REST job timestamps show 19 non-skipped
jobs, 238.717 execution minutes and 1091.383 summed creation-to-start minutes.
Sensitive E2E used 63.8 execution minutes; the light shards used 42.3 combined.
Creation-to-start is a queue/wait proxy, not execution or a diagnosis of API
rate limiting; aggregate wait is not workflow wall time.

Do not attribute #1189's removal of PR benchmarks to this change. A controlled
comparison must use ready runtime PRs with the same production/test source,
runner class, selected subsystems, retry settings and similar cache state.
Record the SHA, active job count, execution and wait separately. This change
adds no component setup job; `PR correctness` adds one small aggregation job.
The 161 → 100 full-target reduction is an allocation count, not a measured
runtime speedup.

The completed [ready-PR run at `b3855f1a`](https://github.com/trickle-labs/pg-trickle/actions/runs/39214902371)
passed on 2026-10-19 with 18 active jobs, 146.333 execution minutes and
294.500 summed creation-to-start minutes. These are observations, not a
controlled speedup against the historical 19-job run above: that baseline
predates #1189 and uses different test fixtures. Issue #1188 remains open for
a matched before/after comparison using the same fixed sources and runner
class, with cache state and wait measured separately.

Capture a **completed** run, including all paginated matrix jobs:

```bash
gh api repos/trickle-labs/pg-trickle/actions/runs/RUN_ID > /tmp/ci-run.json
gh api --paginate --slurp repos/trickle-labs/pg-trickle/actions/runs/RUN_ID/jobs > /tmp/ci-jobs.json
python3 scripts/ci_run_metrics.py /tmp/ci-run.json /tmp/ci-jobs.json
```

The summarizer rejects incomplete runs and reports each job's result, wait
and execution minutes. Record cancelled/failed runs as such, never as passing
performance comparisons.

### Matched allocation replay

Dispatch `ci.yml` with `compare_pr_allocations=true` on the comparison branch
to run only the paired experiment. Normal PR, push, nightly and manual jobs
retain their existing behavior when this input is false.

Both allocations use the production/test trees at
`6de32e0` (the repaired frozen test tree), the `ubuntu-24.04` runner class,
cold workspace build directories and zero test retries. A single producer
freezes a Rust 1.99.1/nextest 0.9.148 toolchain container, all three PostgreSQL
images and one extension package for both sides. Both sides execute inside
that exact toolchain image, including its identical registry cache. The
producer smoke-tests Docker/socket/mount access in both full and light harnesses.
Its cost is reported separately. Each allocation runs five workload jobs:
the existing integration suites, the same three light shards, and full E2E.
The before allocation replays the broad full inventory from `b2ebaa8d`
(after #1189); the after allocation uses the checked bounded list and adds
the existing DVM components to integration. Fixed recovery witnesses and
isolated atomicity execution are held constant on both sides.
Production sources match merged PR #1190 (`0cbd84dc`); both sides also include
the fixture repairs described below.

This measures the affected tier workloads, including cold test compilation.
Unchanged unit, release, shipping, LSN and security jobs are excluded from
both sides. Replay job counts and minutes must not be labeled whole-PR totals;
the new `PR correctness` aggregation job is also outside this experiment.
Queue time is observed separately and is not a causal performance claim.
Source/tool/runner versions, image/package checksums and every step result
are retained as artifacts. A failed job or mismatched environment makes the
paired result inconclusive, rather than a passing speedup measurement.

The first [native replay attempt](https://github.com/trickle-labs/pg-trickle/actions/runs/39287515498)
was stopped as inconclusive after completed jobs exposed hosted image revisions
`20261018.339.1` and `20261012.334.1`. Its results are not a speedup claim.
The cancelled pilot used 11 jobs and 147.083 summed execution minutes, recorded
as experiment overhead rather than either allocation's measured cost.
The replacement fixes compilation and test userspace in one immutable container.
Hosted image revision, host kernel and Docker-server version remain disclosed
per job as residual host variability; GitHub's hosted image revision cannot be
selected by the `ubuntu-24.04` label. This is a single paired containerized replay,
not a population estimate or a reconstruction of historical queue conditions.

The first [containerized replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39292981857)
exposed another one-shot fixture assumption in the mixed view/stream-table
diamond. Its category update left `Cable` in the joined result (three rows
instead of two). A controlled source-writer lock reproduces that exact stale
row in both full and light harnesses after a completed manual attempt. The
fixture now uses the existing exact-convergence helper at each diamond branch
and the join, releasing its writer only after witnessing the held-back attempt.
All original final row assertions remain. Both 35-check targets passed after
repair; reverting only the convergence calls makes the controlled case fail
in both harnesses. The failed hosted replay is not a speedup measurement.
Its broad allocation also exposed a CTE insert fixture with the same one-shot
assumption (two rows instead of four), now using the convergence helper, and
a circular-dependency fixture whose session-only `SET` was lost across pooled
connections. The circular allow/reject cases now use the existing configuration
reload helper so every backend sees the intended setting. Dispatching its
replacement triggered the existing concurrency rule and cancelled the still
running broad job; this failed/cancelled run used 11 jobs and 184.467 summed
execution minutes, recorded only as experiment overhead. The premature
replacement was also stopped before building inputs so these failures could
be repaired before another paired replay.
Comparison dispatches now use distinct concurrency groups to retain earlier
attempts. The stopped replacement used one execution minute in its producer;
its unexpanded matrix placeholder consumed zero execution time.
The local circular follow-up exposed an additional terminal-state sampling
race: one peer was still `ACTIVE` after the first reached `ERROR`. The test
now requires both members to reach `ERROR` together before the deadline;
it also fails if that deadline expires, where the old conditional could pass.
The repaired nine-check circular target passed without retries. A separate
earlier local attempt failed during PostgreSQL template-lock connection setup;
that infrastructure failure remains part of the verification history.
Both 86-check CTE targets passed without retries after repair.

The [next paired replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39301975393)
passed both integration allocations and all six light shards, with matching
controls in their eight records. Its bounded full job exposed a stale
bit-aggregate fixture after an insert: the `eng` group retained count 3 and
permissions 7 instead of count 4 and permissions 23. That fixture now also
uses the existing convergence helper before its unchanged exact oracle.
The failed run is inconclusive. Comparison invocations now collect all selected
parallel failures rather than stopping at the first one; test failures and
cancellations still fail the run. Normal PR invocations keep their existing
early-failure behavior.
Both 36-check refresh targets passed without retries after the bit-aggregate
fixture repair; the exact wrong-row oracle remains unchanged.

The [following replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39312835865)
passed the bounded allocation but failed three broad-only fixture checks:
the cleanup chaos test suspended on a `NO_DATA` attempt with its error counter
pre-seeded to two; the guide report had not consumed its renamed upstream
window; and the extension drop/recreate fixture had not consumed its insert.
The chaos fixture now requires three real failed history rows before suspension
and retains its final DIFFERENTIAL/40001/failpoint and recovery assertions.
Guide data assertions and the upgrade roundtrip use exact convergence; guide
pipelines pause automatic scheduling so combined/sequential mutation windows
stay under manual control. The busy-refresh lock regression remains separate.
All 37 locally eligible checks in these three targets passed without retries
(17 upgrade checks remain ignored). This failed hosted attempt consumed 228.267
summed execution minutes, with 71.700 summed creation-to-start minutes; neither
is a passing speedup result. Earlier failed/cancelled attempts consumed another
546.917 execution minutes. All failed attempts remain experiment overhead.

The [next replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39373735709)
completed all 1,214 bounded checks with one failure: the RLS/WAL data oracle
passed but its final effective-mode assertion observed FULL. The fixture left
`refresh_strategy` at adaptive `auto` while requiring DIFFERENTIAL on a tiny,
high-churn source. It now explicitly selects the differential strategy for its
workers, retaining the RLS, WAL, exact-row and effective-mode assertions. The
local target passed all four checks without retries; two unchanged oracle
helper cases were marked leaky by nextest. The actual RLS/WAL check passed.
The failed hosted pair remains inconclusive.
Its broad allocation passed 2,181/2,182 checks but timed out before witnessing
the `before_receipt` WAL failpoint; no crash was performed in that failed case.
The phase helper now starts a fresh, armed scheduler before the target commit
for that boundary. Later durable-receipt phases retain their existing path.
All 14 local recovery checks then passed without retries (one cleanup case
was marked leaky). An earlier variant that restarted every phase failed two
later-phase witnesses and was narrowed before publication. The failed hosted
pair consumed 215.217 execution minutes and 0.550 creation-to-start minutes;
all failed/cancelled experiment attempts so far consumed 990.401 execution
minutes, recorded as experiment overhead rather than allocation savings.

The [following replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39382059821)
passed both integration allocations and all six light shards, whose eight
identity records matched. The bounded full allocation passed 1,211/1,214:
RLS/WAL still observed FULL, the auto-partition fixture retained count 3
instead of 4, and the FULL/view fixture retained two rows instead of three.
The latter two now use the existing exact-convergence helper before their
unchanged row assertions. The broad allocation passed 2,178/2,182: monitoring
teardown deadlocked, a fallback fixture counted one buffered row before replay,
the earliest WAL witness timed out again, and a watermark test missed its
initial auto-refresh. That watermark fixture now captures its timestamp before
the write, using the existing timestamp-aware wait helper.

The RLS mode, teardown and WAL failures did not reproduce in a 45-check local
run against this hosted attempt's exact archived extension image; they are not
claimed to have a confirmed causal diagnosis. Twenty independent RLS probes
against the earlier local image also passed without retries. Fixture setup
now loads the RLS strategy and the pre-receipt witness at retained-server
startup, rather than relying on a frontend reload acknowledgement or only
restarting a scheduler. WAL setup retains its database/volume identity check
and acquires the phase blocker before the target commit. The no-buffered-copy
witness now checks the target row ID 2 specifically: an older baseline row is
not a copy of that pending commit. Receipt, crash, exact-journal, recovery-mode
and final DIFFERENTIAL assertions remain. Failure diagnostics retain refresh
history, buffered rows, capture/slot and worker state, and full teardown errors.
The teardown assertion still runs in both full allocations without a retry.

A subsequent local post-restart baseline-guard run passed 13/14 but reproduced
the earliest fallback timeout. Its diagnostics showed both consumers back in
`TRANSITIONING` after baseline setup, despite the earlier WAL-mode check.
The fallback fixture now rechecks WAL readiness after all setup refreshes and restarts,
before committing the target row; the RLS fixture does the same after its
cutover drain. A six-target startup-adjusted run passed all 112 eligible checks
without retries before this additional guard; the failed 13/14 follow-up remains
part of the record. The two stale-row repairs also passed all 33 checks in the
light harness without retries.
The final RLS/WAL recovery follow-up passed all 18 checks without retries,
including the post-restart baseline guard and recovery negative controls.
Formatting and lint passed with zero warnings.

Both ten-repetition recovery steps and the broad serial atomicity phase passed.
This failed pair remains inconclusive: 11 jobs used 203.783 execution minutes
and 278.933 summed creation-to-start minutes. Failed/cancelled experiment
overhead was 1,194.184 execution minutes before the next attempt; none is
allocation savings.

The [2026-10-20 matched replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39394922140)
used the same frozen inputs in all ten replay records, and all six light shards
plus both integration jobs passed. The bounded full job also passed. The broad
full job failed 3 of 2,182 checks (2,179 passed, 135 skipped): two one-shot
fixture assertions now use `refresh_st_with_retry` or exact convergence, with
their assertions unchanged. The third was the RLS/WAL effective-mode check;
its history showed a FULL `SCHEMA_CHANGED` reinitialization after a transient
connection reset. The isolated RLS/WAL target passed without retries, so the
cause remains unconfirmed and its strict DIFFERENTIAL assertion remains.
Because the broad job failed, the validator rejects this pair and no before /
after cost difference is reported. The run used 11 jobs, 219.417 execution
minutes and 235.717 summed creation-to-start minutes. Failed/cancelled
experiment overhead is now 1,413.601 execution minutes. These costs are not
allocation savings; issue #1188 remains open pending a valid matched replay.

The next dispatch, [run `39408759659`](https://github.com/trickle-labs/pg-trickle/actions/runs/39408759659),
stopped at the producer's frozen-source guard before building or replaying
anything. It correctly rejected the two test-file changes against the old
`88c8` pin. The pin now advances to `91f3a039`, which contains those fixture
repairs while production sources still match merged #1190. One producer job
used 0.183 execution minutes and 9.633 queue-proxy minutes; no replay ran.
Including the producer check, failed/cancelled experiment overhead is now
1,413.784 execution minutes.

The next dispatch, [run `39410203313`](https://github.com/trickle-labs/pg-trickle/actions/runs/39410203313),
was also invalid: both full allocations failed. The bounded suite failed the
RLS/WAL effective-mode and before-receipt fallback cases; the broad suite
passed 2,179/2,182, with failures in disabled-worker resume, an auto-refresh
wait, and chained-stream row count.

The following [run `39420981755`](https://github.com/trickle-labs/pg-trickle/actions/runs/39420981755)
confirmed the RLS/WAL effective-mode case passes when isolated. Its
before-receipt fallback case still timed out on both sides, with two consumers
stuck in `TRANSITIONING`, the decoder at the cutover LSN, and autovacuum and
the scheduler both waiting on buffer content. The broad `before` suite passed
2,179/2,180; its only failure was the window delete fixture reading three rows
instead of two. That fixture now uses the existing convergence helper without
changing its oracle. The comparison jobs now run after/light work first and
serialize the two full allocations, preventing the sensitive tests from
overlapping the opposite full suite. The validator rejected the run; it used
11 jobs, 229.633 execution minutes and 131.650 summed creation-to-start
minutes. Failed/cancelled experiment overhead is now 1,862.300 execution
minutes. No cost difference is reported.

The [serialized replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39433887308)
still failed both full allocations. The broad side passed 2,178/2,180 checks;
the JSON aggregate and placeholder-cache fixtures now wait for a completed
manual refresh. The bounded side passed 1,211/1,212; its auto-refresh property
test timed out waiting for a scheduler cycle during a 30-second inner wait
within the existing 90-second overall deadline. That inner wait now uses the
full remaining deadline, preserving the exact invariant checks. The serial
before-receipt case failed again on the broad side: consumers stayed in
`TRANSITIONING` at the cutover LSN while autovacuum and the scheduler waited on
buffer content. The receipt-table autovacuum setting is now disabled in this
controlled fixture; this diagnosis still needs hosted verification. The two
full jobs ran sequentially, so cross-allocation overlap was not the cause. The
invalid run used 11 jobs, 228.967 execution minutes and 60.733 summed
creation-to-start minutes. Failed/cancelled experiment overhead is now
2,091.267 execution minutes. No before/after cost difference is reported.

The next [replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39456096246)
passed the broad inventory 2,180/2,180. Its serial before-receipt case still
timed out after restart: the scheduler remained in `BufferContent` wait after
receipt-table autovacuum was disabled, so that fixture setting did not resolve
the cutover stall and has been removed. The bounded side passed 1,211/1,212;
`test_scheduler_resumes_after_watermark_unstuck` sampled its auto-refresh
baseline after the triggering insert, so it now captures `data_timestamp`
before the write and waits with the existing timestamp-aware helper. Because
the bounded suite stopped after that failure, it did not run its serial WAL
cases. The pair is invalid; it used 11 jobs, 209.950 execution minutes and
107.550 summed creation-to-start minutes. Failed/cancelled experiment overhead
is now 2,301.217 execution minutes. No cost difference is reported.

The [next replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39548295282)
passed both integration allocations and all six light shards. The baseline
broad suite passed 2,178/2,180; its GUC aggregate fixture missed the completed
manual refresh, and the unchanged-upstream baseline asserted transaction
identity even though no upstream changes existed. The bounded allocation
passed 1,211/1,212; both allocations reproduced the before-receipt WAL stall
after an extra pre-witness restart, with both consumers stuck in
`TRANSITIONING` at the cutover LSN. The fixture now keeps WAL steady until the
actual crash witness, waits for every dependency to reach the requested CDC
mode, and the two broad-only assertions now match their intended invariants.
The recovery repetition passed 10/10 on both sides. This invalid pair used
11 jobs, 220.550 execution minutes and 107.250 summed creation-to-start
minutes. Failed/cancelled experiment overhead is now 2,521.767 execution
minutes. No cost difference is reported.

The [next paired replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39564224459)
passed integration and all six light allocations. The broad baseline full suite
passed 2,177/2,180: the backward-compatibility and CTE cascade count checks
failed, and the WAL semantic-control fixture timed out before its replay
barrier. The bounded full allocation passed 1,211/1,212; only
`test_scheduler_unchanged_upstream_baseline_preserves_counts` failed while
waiting for its scheduled sibling refresh. Its 60-second wait was too short
under parallel full-suite load and is now 120 seconds; the scheduler and exact
result assertions remain. Recovery repetition passed 10/10 on both sides.
The invalid pair used 11 jobs, 223.533 execution minutes and 257.817 summed
creation-to-start minutes. Its full allocation jobs took 80.417 minutes before
and 41.800 minutes after, but the unequal suites and failed correctness checks
make this unsuitable as a cost comparison. Failed/cancelled experiment
overhead is now 2,745.300 execution minutes.

The [next paired replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39592595933)
passed integration, all six light allocations and the 2,180-test broad baseline
suite, including isolated refresh atomicity and ten recovery repetitions. The
bounded full suite passed 1,211/1,212; its sole failure,
`test_wal_trigger_fallback_received_receipt_requires_rebaseline`, timed out
waiting for the `before_replay` barrier while the scheduler was in
`TRANSITIONING` at the cutover LSN and waiting on `BufferContent`. The
unchanged-upstream scheduler case now passed with its 120-second wait. All
seven WAL receipt-recovery tests that use advisory-lock witnesses are now
removed from the parallel invocation and retained in the serial one-thread
step on both allocations. This pair remains invalid. It used 11 jobs, 215.650
execution minutes and 224.867 summed creation-to-start minutes. The full jobs
took 62.450 minutes before and 43.817 minutes after; the failed, unequal runs
are not a cost comparison. Failed/cancelled experiment overhead is now
2,960.950 execution minutes.

The [next matched replay](https://github.com/trickle-labs/pg-trickle/actions/runs/39616553797)
passed both integration checks and all six light shards. Its broad baseline
full suite ran 2,174 tests: 2,172 passed and two failed. The D+I insert oracle
found one missing row, and `test_v107_auto_wal_transition_captures_changes`
timed out with capture still in `TRIGGER` instead of `WAL`. The bounded full
suite ran 1,206 tests: 1,205 passed and
`test_lsn_manual_refresh_fixed_snapshots_preserve_frontier` failed with a
serialization error during a concurrent refresh. The separately isolated
atomicity suite passed 12/12, the eight serial WAL/RLS cases passed 8/8, and
recovery repetition passed 10/10 on both allocations. All three newly exposed
parallel-load cases are retained in the one-thread step for the next replay.
This pair used 11 jobs, 219.600 execution minutes and 319.717 summed
creation-to-start minutes; its full jobs took 65.683 minutes before and
46.400 after. Failed suites make it an invalid cost comparison. Failed and
cancelled experiment overhead is now 3,180.550 execution minutes. Do not
report a speedup from it.

The repaired replay [run `39729948300`](https://github.com/trickle-labs/pg-trickle/actions/runs/39729948300)
passed the producer/source guard, both integration allocations and all six
light shards. The broad full suite passed 2,168/2,168 (151 skipped); the
bounded full suite passed 1,200/1,200 (47 skipped); isolated atomicity passed
12/12; recovery repetition passed 10/10 on both sides. The serial WAL/RLS
phase passed 15/16 on each side: after restart, the before-receipt witness
timed out with the dependency back in `TRANSITIONING`, the slot at cutover,
and scheduler/autovacuum waiting on buffer content. The catalog-only
post-restart `WAL` check did not prove the scheduler had resumed decoding. The
fixture now waits for a separate, acknowledged readiness receipt before
arming the target barrier. Both full allocations failed the serial witness,
so the pair is invalid and no cost difference is reported. Artifacts are
retained under `target/ci1188-comparison/run-39729948300/`.

The next repaired replay [run `39747899962`](https://github.com/trickle-labs/pg-trickle/actions/runs/39747899962)
passed the producer/source guard, both integration allocations and all six
light shards. Its before broad inventory passed 2,166/2,168; the two failures
were `test_lateral_multiple_refreshes_converge` (an exact row-count lag after
manual refresh) and `test_scheduler_clock_auto_cron_expires_without_writes`
(the 30-second due-time witness expired). Its after bounded inventory passed
1,200/1,200 (47 skipped), and isolated atomicity passed 12/12. The serial WAL
phase still failed 1/16 on both sides: the before-receipt witness timed out
with the dependency in `TRANSITIONING`, the slot at cutover, and the scheduler
and autovacuum waiting on buffer content. Recovery repetition failed on five
of ten before attempts and one of ten after attempts: the same failpoint test
observed `effective_refresh_mode = NO_DATA` where it expected `DIFFERENTIAL`.
The pair is invalid; do not report a cost difference. Source,
package and workload-image identities matched. Hosted runner image revisions
and kernels differed and are retained as host variability. Artifacts and
identity records are under `target/ci1188-comparison/run-39747899962/`.

The follow-up fixture repair makes the initial DVM refresh require a completed
manual history row, keeps autovacuum off the ephemeral receipt table while
testing its lock witness, makes the three lateral refreshes wait for exact
convergence, and allows 90 seconds for scheduler due-time witnesses. These
changes preserve the result assertions. The WAL change still needs hosted
verification; disabling autovacuum is a fixture-isolation hypothesis based
on both failure snapshots showing the scheduler and autovacuum blocked on
buffer content. The next paired run freezes these repairs at
`863eb42f0343c3f0610e7ca92036c840ccd8b586`; later documentation and workflow
guard commits must leave that source/test tree unchanged.

After saving the completed run/jobs JSON above, download only the replay
evidence (the shared image archive is not needed for the report):

```bash
gh run download RUN_ID --pattern 'allocation-before-*' --pattern 'allocation-after-*' --dir /tmp/ci-allocation-RUN_ID
python3 scripts/compare_ci_allocations.py /tmp/ci-run.json /tmp/ci-jobs.json /tmp/ci-allocation-RUN_ID
```

## Verification record (2026-10-18)

The local probes used a freshly rebuilt x86-64 PostgreSQL 18.6 image and the
same extension library in the light harness. Production Rust and SQL were
unchanged. The probes informed the final allocation; they are not hosted
runner performance measurements.

| Check | Observed result |
|---|---|
| Format, lint, actionlint, release/candidate static contract | Passed; lint had zero warnings |
| DVM components | 67 passed across ten targets |
| Script tests | 31 passed |
| Light inventory | 1493 passed; no failed attempts |
| Original 84-target full probe | 944 passed, one WAL recovery case failed |
| 67 candidate full-harness duplicates | 1205 passed with two retries; the replay led to retaining 16 worker-sensitive targets in the final 100-target full allocation |
| Isolated refresh-atomicity phase | All 12 passed serially with retries disabled |
| Path and artifact placement | 26 matching-library path cases and all three real-artifact consumer layouts passed; downloaded library digests matched and cached leftovers were removed |
| Hosted ready-PR execution | Passed at `b3855f1a` on 2026-10-19: full suite 1214/1214, isolated atomicity 12/12, ten recovery repetitions, all three light shards and `PR correctness` |
| Controlled before/after timings | Open; the historical baseline predates #1189 and is not a matched comparison |

The full-harness DAG schedule-change probe reported an extra stale `d` group
in `ops_l1`. A controlled source-writer lock now reproduces exactly that
extra `d = 99` row: the one-shot manual refresh completed a held-back window
before its pending delete was consumed. DAG operations and cascade fixtures
now share the bounded exact-convergence helper. The schedule-change regression
pauses scheduling and releases its writer only after witnessing the completed
held-back attempt and stale group; manual refresh must then remove the group
at all three layers. The final source-row assertions remain unchanged.
The other retry was Docker port discovery and passed an isolated no-retry run.

`test_wal_trigger_fallback_undecoded_commit_requires_rebaseline` failed in the
full probe and again in a separate no-retry run: the observed pending receipt
state after restart was `true`, while the witnessed phase required `false`.
That check remains pre-merge. The recovery fixture now uses an immediate
SIGKILL followed by a start of the same container: `docker restart --time 0`
first signals shutdown, which can release the barrier backend before the
decoder stops. Recovery checks also wait for the separately committed receipt
acknowledgement after data convergence. All 14 WAL recovery checks passed with
retries disabled after these fixture fixes.

The later 1,210-check full sweep passed 1,207 checks and exposed three failures:
the WAL crash boundary above, an RLS/WAL mode assertion, and Docker port
discovery before a refresh test could start. The RLS fixture now drains cutover
work and establishes its baseline before testing differential mutations; it
passed three independent runs without retries. The Docker setup case passed
separately. The hosted getting-started failure was traced to a refresh helper
that treated a NOTICE-only skip as completion; its lock regression and all 15
getting-started checks passed in both full and light harnesses after repair.
Production recovery behavior and data assertions are unchanged. The local
sweep with failed attempts remains **not green**. Subsequent hosted verification
at `b3855f1a` passed all selected checks without automatic full-suite retries;
it is separate evidence and does not erase the earlier failures.

Later hosted runs exposed cascade delete and multi-level DML assertions that
assumed every completed manual refresh had consumed the latest changes.
Controlled writer locks reproduced both stale results despite completed manual
attempts: the documented writer holdback can defer their source windows.
Cascade fixtures now manually refresh each layer until the existing exact
schema/multiset oracle matches its defining query, with a 90-second deadline;
schema and comparator errors fail immediately. The multi-level DML regression
pauses scheduling, witnesses a completed held-back attempt, then releases its
writer and requires L1 to converge while L2 remains stale before the next DML.
All original final assertions remain. All 19 cascade checks passed without
test retries after this correction.

The broader follow-up sweep also exposed a recovery negative control whose
one-second statement timeout could expire while it prepared the wrong-PID
cancellation. The control now uses witnessed lock barriers, verifies that the
wrong backend really was cancelled, and requires the untouched target to finish
successfully after releasing its blocker. Timed failure tests retain their
SQLSTATE and cleanup assertions, with ten-second faults occurring after the
five-second witness deadline. All 14 locally eligible recovery checks passed
without retries after these changes.

The LSN transition check also compared a live pending-receipt count before and
after rejected input while decoding continued. It now pins the initial receipt
ID boundary, waits for that cohort to be acknowledged, and compares the same
cohort after rejection. New bookkeeping receipts remain outside the protected
cohort. All 11 LSN contract checks passed without retries after this repair.

The next matched allocation replay, hosted run `39772191763` at comparison SHA
`58363734`, passed the producer, both integration jobs, and all six light
shards, but both full jobs failed, so the pair is invalid and has no usable
cost comparison. The before broad inventory passed 2,167/2,168; the nested
view test observed two rows instead of three. Its lateral convergence and
scheduler due-time tests passed. The before isolated WAL phase passed 15/16:
the pre-receipt crash-boundary test timed out after 196.6 seconds. The
dependency was `TRANSITIONING` with no slot or cutover recorded after the
readiness receipt had been acknowledged; disabling autovacuum on the ephemeral
receipt table did not remove the stall. Before recovery repetitions passed
9/10, with the same `NO_DATA` versus `DIFFERENTIAL` assertion as prior runs.
The after bounded inventory passed 1,199/1,200; the third refresh unexpectedly
succeeded in `test_consecutive_errors_tracked_and_reset`. After recovery
repetitions passed 10/10. Source, package, workload image, Rust, nextest, and
PostgreSQL identities matched; the hosted runner image revision and kernel
differed. The validator rejected the run because both full jobs failed. Full
logs, identity records, and validator inputs are retained under
`target/ci1188-comparison/run-39772191763/`.
