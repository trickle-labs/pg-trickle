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
light shards. Its broad full suite passed all 2,168 tests (151 skipped), the
isolated atomicity phase passed, and recovery repetition passed 10/10. The
serial WAL/RLS phase passed 15/16: after restart, the before-receipt witness
timed out with the dependency back in `TRANSITIONING`, the slot at cutover,
and both scheduler/autovacuum waiting on buffer content. The catalog-only
post-restart `WAL` check did not prove the scheduler had resumed decoding. The
fixture now waits for a separate, acknowledged readiness receipt before
arming the target barrier. The paired after full job was still running when
this note was written, so the pair is invalid and no cost difference is
reported. Artifacts are retained under `target/ci1188-comparison/run-39729948300/`.

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
