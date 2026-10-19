# CI correctness allocation

The PR suite protects committed results through real PostgreSQL execution.
The inventory is checked by `python3 scripts/ci_pr_suite.py --list`: every
`tests/e2e_*.rs` target must be in the light allowlist, the explicit full
list, or a documented scheduled tier. New targets fail the static contract
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
an explicit 100-target full suite. The old sensitive DVM gate selected all 161
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

## Measurements

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

Both allocations use the production/test trees at merged commit
`0cbd84dce4fede39fa36f2c8716e0faa0a9b1b14`, the `ubuntu-24.04` runner class,
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
