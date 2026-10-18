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
an explicit 84-target full suite. The old sensitive DVM gate selected all 161
E2E targets in addition to light E2E. The new full suite retains permanent
corpus, identity, versioned security and conformance regressions in both
harnesses. All targets with `cfg(not(feature = "light-e2e"))` stay in the full
suite, including watermark gating, IMMEDIATE mode, mixed DAGs, multi-cycle
refresh, failure recovery and checked LSN cases. The inventory validator
rejects an allocation that drops these full-only cases.

The remaining 67 light targets use the installed extension for parser,
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
The 161 → 84 full-target reduction is an allocation count, not a measured
runtime speedup.

Capture a **completed** run, including all paginated matrix jobs:

```bash
gh api repos/trickle-labs/pg-trickle/actions/runs/RUN_ID > /tmp/ci-run.json
gh api --paginate --slurp repos/trickle-labs/pg-trickle/actions/runs/RUN_ID/jobs > /tmp/ci-jobs.json
python3 scripts/ci_run_metrics.py /tmp/ci-run.json /tmp/ci-jobs.json
```

The summarizer rejects incomplete runs and reports each job's result, wait
and execution minutes. Record cancelled/failed runs as such, never as passing
performance comparisons.
