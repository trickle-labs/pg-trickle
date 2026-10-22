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
| Recovery, interrupted finalization, cancellation | Recovery classification tests | No crash/restart lifecycle | Full failpoint, cleanup chaos, error state, window recovery, publication crash and refresh-atomicity suites; relevant changes also repeat witnessed schedules ten times without retries |
| Ownership, RLS, grants, permissions | Privilege-boundary/static SECURITY DEFINER contracts | Catalog DDL checks only | Light ownership/RLS; full safety, RLS/WAL, versioned security regressions, MAINTAIN grants, guard and replica boundaries; relevant unsafe paths keep ASan |
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
an explicit 99-target full suite and one database-free harness target. The old sensitive DVM gate selected all 161
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
decision rather than an automatic correctness verdict.

The remaining 51 light targets use the installed extension for parser,
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
Refresh-atomicity witnesses run serially without retries after the parallel
full suite, preserving their isolation from its container load. Selected
failure-recovery paths additionally retain the ten witnessed repetitions.

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
Matrix failure/cancellation is propagated through `needs.light-e2e-tests`.
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
and the independent candidate-binding dependency remain intact. Removing it
would redefine an existing release control and is unnecessary for this change.

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

### Validation and matched experiment

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
unchanged. Full unmodified suites and ASan/recovery repetitions were not rerun
locally; the ready PR selects them. Logs, saved REST responses, failed attempts
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
