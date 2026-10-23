# Handoff for issue #1188

This handoff covers the follow-up work for GitHub issue [#1188](https://github.com/trickle-labs/pg-trickle/issues/1188), which remains open. The original PR [#1190](https://github.com/trickle-labs/pg-trickle/pull/1190) is already merged at `0cbd84dc`; the comparison and fixture follow-up is on `ci/1188-cost-comparison` and has not been merged. The required matched before/after CI comparison is still inconclusive, so do not record a speedup or close #1188 yet.

## Current repository state

- Repository: `/Users/grove/projects/pg-trickle2`
- Branch: `ci/1188-cost-comparison`
- The published comparison branch was at `58363734` when run `39772191763` was dispatched. The working branch now includes current `main` (merge commit `310c92f6`) so the PR will not revert later merged WAL and verification changes; the frozen replay source guard remains `863eb42f`.
- Replay evidence is retained under ignored `target/ci1188-comparison/run-39682504980/`, `run-39729948300/`, and `run-39747899962/`; the handoff itself remains untracked.
- The follow-up PR is separate from merged PR #1190. This branch is being prepared as a draft because the matched comparison remains invalid.

The remote branch contains the matched comparison workflow and accumulated test/harness fixes. The post-restart acknowledged WAL receipt is part of the frozen source tree at `6de32e0`. The workflow now guards the follow-up fixture repairs at `863eb42f0343c3f0610e7ca92036c840ccd8b586`.

## Latest comparison run

Run [39682504980](https://github.com/trickle-labs/pg-trickle/actions/runs/39682504980) completed with failure on both full replay jobs. The producer/source guard passed. Both integration jobs and all six light shards passed with matching prepared inputs. Both ten-attempt recovery repetitions passed; the before-side isolated atomicity phase passed. These successes do not make the pair valid because full runs failed.

Fresh repaired comparison [39729948300](https://github.com/trickle-labs/pg-trickle/actions/runs/39729948300) was dispatched from `aafc4c76` on 2026-10-22. The frozen source/test guard, candidate build, shared input generation, both harness smoke checks, both integration jobs, all six light shards, the 2,168-test broad inventory, the 1,200-test bounded inventory, both isolated atomicity phases, and both 10-attempt recovery repetitions passed. The serial WAL/RLS phase failed 1/16 on both sides: `test_wal_recovery_before_receipt_commit_preserves_committed_changes` timed out before witnessing `before_receipt`. Diagnostics showed the dependency back in `TRANSITIONING`, the slot at cutover, and scheduler/autovacuum waiting on buffer content. This pair is invalid. Artifacts are under `target/ci1188-comparison/run-39729948300/`.

The post-restart acknowledged-receipt repair was tested in comparison [run `39747899962`](https://github.com/trickle-labs/pg-trickle/actions/runs/39747899962), dispatched from `7a843c13` on 2026-10-22. Producer/source checks, harness smoke tests, both integration jobs and all six light shards passed. The before broad inventory passed 2,166/2,168; `test_lateral_multiple_refreshes_converge` missed one row after manual refresh, and the 30-second `test_scheduler_clock_auto_cron_expires_without_writes` due-time witness timed out. The after bounded inventory passed 1,200/1,200 (47 skipped), and isolated atomicity passed 12/12. The serial WAL phase failed on both sides: `test_wal_recovery_before_receipt_commit_preserves_committed_changes` timed out after about 200 seconds with the dependency `TRANSITIONING`, the slot at cutover, and the scheduler and autovacuum waiting on buffer content. Recovery repetition failed on 5/10 before attempts and 1/10 after attempts; each observed `NO_DATA` instead of `DIFFERENTIAL` in `test_dvm_failpoint_preserves_last_committed_result_and_recovers`. Source/package/workload-image identities matched; hosted runner image revisions/kernels differed. The comparison validator rejected the run because both full jobs failed. Artifacts are under `target/ci1188-comparison/run-39747899962/`.

The follow-up repair was pushed in `863eb42f`; workflow/docs pin commit `58363734` changes no frozen production/test source. Replacement [run `39772191763`](https://github.com/trickle-labs/pg-trickle/actions/runs/39772191763) completed on 2026-10-22. The producer, both integration jobs, and all six light shards passed. The before broad inventory failed 1/2,168 at `test_view_inline_nested_view` (observed 2 rows instead of 3); its three previously flaky witnesses passed, including the lateral convergence and scheduler clock cases. The after bounded suite failed 1/1,200 at `test_consecutive_errors_tracked_and_reset` because its third refresh unexpectedly succeeded. The isolated before WAL phase failed 1/16 at `test_wal_recovery_before_receipt_commit_preserves_committed_changes`: the committed sentinel did not reach the `before_receipt` barrier after 196.6 seconds. Diagnostics still showed the dependency in `TRANSITIONING`, `slot_name` and cutover fields null, and no active scheduler query; disabling receipt-table autovacuum did not remove the hang. The WAL test did observe its separately committed restart-readiness receipt first. Failure-recovery repetitions were 9/10 before and 10/10 after; the sole before failure again saw `NO_DATA` instead of `DIFFERENTIAL`. Source, package, workload image, Rust, nextest, and PostgreSQL identities matched; hosted runner image revisions and kernels differed. The validator returned `valid: false` because both full jobs failed. Full artifacts and run/job metadata are under `target/ci1188-comparison/run-39772191763/`.

Failures retained in `target/ci1188-comparison/run-39682504980/`:

- **Before, broad full inventory:** 2,164/2,166 passed. `test_recursive_cte_full_refresh_picks_up_changes` observed 3 members instead of 5; `test_having_row_group_migration` produced an exact-oracle mismatch (one extra and one missing row). See `before-full/step-1.log`.
- **Before, isolated serial checks:** 15/16 passed. `test_wal_recovery_before_receipt_commit_preserves_committed_changes` did not observe the `before_receipt` boundary before timeout (185.5s). Its log includes a connection reset and the retained database/slot/worker diagnostics. Do not assume a cause before reproducing it. See `before-full/step-3.log`.
- **After, bounded suite:** 1,197/1,198 passed. `test_p2_complexity_class_backfill` found `query_complexity_class` was not populated after first refresh. See `after-full/step-1.log`.

The comparison is **inconclusive**. Do not turn these durations into savings or update issue #1188 as complete. See the matched replay history in `docs/CI_COVERAGE.md` before repeating investigations.

## Repair progress (2026-10-22)

The three refresh-related failures now use existing E2E helpers: exact convergence for the recursive CTE and HAVING migrations, and a completed-refresh retry for the P-2 metadata backfill. The catalog-only post-restart wait for CDC mode `WAL` did not prove that the scheduler had resumed decoding. The test now commits a separate readiness row and waits for its WAL receipt acknowledgement before arming the pre-receipt barrier. This WAL fix still needs Testcontainers reproduction.

Local validation passed: `just fmt`, `just lint` (zero warnings), `python3 -m unittest discover -s scripts -p 'test_*ci*py'` (23 tests), and `git diff --check`. Docker access is denied in this environment, so the affected E2E tests were not run locally.

Run `39747899962` used the frozen source/test tree at `6de32e0`. The fixture repairs for the next run are committed locally at `863eb42f0343c3f0610e7ca92036c840ccd8b586`; the workflow source guard now points there. The user explicitly authorized committing and publishing repairs and dispatching comparisons on 2026-10-22. Validation passed: `just fmt`, `just lint` (zero warnings), and `git diff --check`. Docker access is denied locally, so Testcontainers reproduction is replaced by hosted comparison runs.

Current local repairs: the initial DVM refresh now waits for a completed manual attempt, lateral mutations wait for exact convergence, the scheduler due-time witnesses have 90-second windows, and the WAL receipt fixture disables autovacuum on its ephemeral receipt table while testing the lock boundary. The latest hosted run confirms the lateral and scheduler changes work, but shows the WAL lock hypothesis was insufficient and uncovered independent intermittent failures in nested-view convergence, error-count testing, and DVM recovery. `just fmt` and `just lint` passed with no warnings. Local Testcontainers remain unavailable.

## Next actions

1. Investigate the WAL decoder stall from the new diagnostic state: after readiness receipt acknowledgement, the dependency has returned to `TRANSITIONING` with no slot/cutover and the scheduler has no active query. Trace the transition/restart path and its lock ownership before changing the fixture again.
2. Reproduce and stabilize `test_view_inline_nested_view`, `test_consecutive_errors_tracked_and_reset`, and the intermittent DVM recovery assertion without relaxing their result or failure-state checks. The latest evidence and logs are in `target/ci1188-comparison/run-39772191763/`.
3. After fixes, run `just fmt`, then `just lint`, update the frozen source pin, and dispatch another matched hosted comparison. Do not report savings until `compare_ci_allocations.py` returns `valid: true`.
4. Once a valid pair exists, record execution and queue/wait data separately in `docs/CI_COVERAGE.md`, update the draft PR, and review remaining acceptance criteria. Keep #1188 open until the documented acceptance is complete.

## Useful commands

```bash
git status --short --branch
git log --oneline --decorate -12
gh issue view 1188
gh run view 39772191763
find target/ci1188-comparison/run-39772191763 -maxdepth 3 -type f
python3 scripts/compare_ci_allocations.py target/ci1188-comparison/run-39772191763/run.json target/ci1188-comparison/run-39772191763/jobs.json target/ci1188-comparison/run-39772191763/artifacts
python3 -m unittest discover -s scripts -p 'test_*ci*py'
just fmt
just lint
```

The repository workflow requires `just fmt` before `just lint`. Do not force-push. GitHub execution permission does not itself authorize unrelated tracker changes; the issue should remain open until the evidence and merge conditions above are satisfied.
