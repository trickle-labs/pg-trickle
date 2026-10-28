# Bounded recovery histories (#1219)

This first slice extends the existing Testcontainers WAL-recovery and refresh
atomicity targets. It changes tests and existing CI selections; production
recovery behavior is unchanged. It does not complete #1219 or #1225.

## Committed journal histories

`test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal` uses
the existing `TraceConfig` and deterministic RNG: four seeds/four cycles for
the PR set, ten seeds/twelve cycles in the existing scheduled DVM job. The PR
sample reaches all four WAL interruption phases across trigger-to-WAL handoff;
the scheduled job repeats those phases with more histories. Both row and
statement capture are selected and witnessed from actual triggers.
Initial states include empty/nonempty sources; snapshots use default and
caller-supplied targets.

Each generated committed INSERT/UPDATE/DELETE writes an independent journal in
the source transaction. Savepoint writes are rolled back before commit. A
separate transaction changes source rows, refreshes and then aborts; public
rows and frontier must stay at the preceding committed checkpoint. The fast
consumer advances with compaction enabled while the slow consumer stays at
its own original checkpoint. Each changed refresh requires a fresh completed
DIFFERENTIAL history row and must cover the captured committed source cut.

Restoring the fast consumer requires its original rows/frontier pair. Its
next differential refresh must exactly catch up from history protected by the
slow consumer. The history then transitions from trigger capture to WAL and
uses one of four witnessed interruption points: before receipt persistence,
before replay, before acknowledgement, and after slot advance. The last case
also commits a second source/journal row after the slot advances. PostgreSQL is
killed with SIGKILL and restarted in the same container/data volume. The test
checks system identifier, database OID, persisted sentinel, volume identity
and changed postmaster start time. After receipt recovery, the fast and slow
consumers must take the same differential, full, or reinitialization path, and
both must match the committed journal exactly. The post-slot-advance row must
have an acknowledged receipt or force a shared-consumer reinitialization.
`fsync`, `synchronous_commit` and `full_page_writes` are witnessed as on.

Boundary observations read rows and frontier together in one SQL statement;
bag comparisons also use EXCEPT ALL in both directions against the journal.
This is not a proof of arbitrary isolation schedules or every metadata field.

## Snapshot during cancellation

`test_snapshot_during_witnessed_cancel_keeps_committed_rows_and_frontier`
reuses the actual after-apply barrier. The observer verifies the executing
backend, lock wait and blocker before starting a snapshot. The snapshot may
complete against committed state or observably block behind the refresh.
The observer cancels the refresh, checks the intended cancellation error,
releases its own barrier and checks snapshot/committed rows and frontier.
After normal differential recovery, a caller-target snapshot checks the new
rows. Bounded waits prevent the observer from waiting indefinitely while
holding the lock needed by the operation it observes.

## Evidence and replay

Local baseline production package: `cbf98a884414d53880b43957e7bf6678e1ffb4ec`
(the existing COUNT/FILTER repairs integrated above the reviewed CDC slice).
The new tests do not require a production repair to pass. Earlier qualification
against the unchanged CDC package also passes.

- Formatting/lint: zero warnings.
- Default new histories plus cancellation: two test cases pass, no retries.
- Scheduled-size histories: ten seeds/twelve cycles plus cancellation pass,
  no retries. The expanded restore history also passes all four WAL
  interruption phases across ten seeds and twelve cycles.
- The parent semantic floor is independently 32/34 and 52/54; special SUM/AVG
  and mixed-snapshot division still fail. The original four COUNT/FILTER
  regressions pass. These are not recovery-history successes.

`PGT_LIFECYCLE_ARTIFACT_DIR` selects the JSON directory (default
`artifacts/lifecycle-histories`). Records include seed, runner revision,
declared server candidate, configuration, initial state, SQL mutations,
transaction boundaries, expected/actual rows, cuts and durable identity.
Only the executed prefix through the first failing assertion is retained.
This is ordered-prefix retention, not a general history reducer or arbitrary
SQL minimization. The ready-PR selection is:

```sh
PGS_PROP_CASES=4 PGS_PROP_CYCLES=4 \
  ./scripts/run_e2e_tests.sh --test e2e_wal_receipt_recovery_tests \
  -E 'test(=test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal)' --retries 0
```

For an exact single-seed replay, override the seed and reduce its bounded cycle
count while preserving the failure detector:

```sh
PGS_PROP_SEED=0x12190001 PGS_PROP_CYCLES=4 \
  ./scripts/run_e2e_tests.sh --test e2e_wal_receipt_recovery_tests \
  -E 'test(=test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal)' --retries 0
```

Existing CI jobs retain JSON histories and complete runner output on failure.
The candidate job runs the small set; the scheduled DVM job runs the larger
set. No remote CI success is claimed.

## Open boundaries

This does not establish recovery after required history has already been
removed. The snapshot metadata-read versus copy schedule is covered above.
PR #1266 adds separate witnessed cleanup-versus-registration and
pause-versus-dispatch schedules; those tests remain in that independent PR and
are not part of this branch. Existing premature-cleanup/oracle controls remain.
PR #1266 exercises the real cleanup and dispatch paths but adds no destructive
production-cleanup mutant. No general minimizer, whole recovery proof, or merge
approval is claimed.
