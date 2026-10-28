# Bounded recovery histories (#1219)

This first slice extends the existing Testcontainers WAL-recovery and refresh
atomicity targets. It changes tests and existing CI selections; production
recovery behavior is unchanged. It does not complete #1219 or #1225.

## Committed journal histories

`test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal` uses
the existing `TraceConfig` and deterministic RNG: three seeds/four cycles for
the PR set, ten seeds/twelve cycles in the existing scheduled DVM job. Both
row and statement capture are selected and witnessed from actual triggers.
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
slow consumer. PostgreSQL is then killed with SIGKILL and restarted in the
same container/data volume. The test checks system identifier, database OID,
persisted sentinel, volume identity and changed postmaster start time; the
fast result must survive and the lagging consumer must catch up exactly.
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
  no retries. Actual trigger/durability/restart witness additions also pass
  in the final default run.
- At that baseline the semantic floor was independently 32/34 and 52/54; special SUM/AVG
  and mixed-snapshot division still fail. The original four COUNT/FILTER
  regressions pass. These are not recovery-history successes.

`PGT_LIFECYCLE_ARTIFACT_DIR` selects the JSON directory (default
`artifacts/lifecycle-histories`). Records include seed, runner revision,
declared server candidate, configuration, initial state, SQL mutations,
transaction boundaries, expected/actual rows, cuts and durable identity.
Only the executed prefix through the first failing assertion is retained.
That trace is kept as the unminimized original. Recovery invariants carry
stable failure tags so unrelated setup errors cannot count as reproducing the
same failure. To reduce a failing seed to the shortest cycle prefix that
preserves its exact tagged invariant, run:

```sh
python3 scripts/shrink_recovery_history.py \
  artifacts/lifecycle-histories/history-0000000012190001.json
```

The reducer first verifies the original failure, then replays each shorter
cycle prefix in order with the same seed and setup. It keeps the unminimized
trace, candidate traces and logs, a minimized trace, and a report of every
attempted prefix. The replay requires Docker/Testcontainers and uses the
existing recovery E2E target. It preserves each cycle's SQL and transaction
boundaries; it does not simplify SQL statements.

Existing CI jobs retain JSON histories and complete runner output on failure.
The candidate job runs the small set; the scheduled DVM job runs the larger
set. No remote CI success is claimed.

## Open boundaries

This does not yet combine capture-mode transitions or WAL interruption with
the generated restore histories. It does not establish recovery after required
history has already been removed. Snapshot metadata-read versus copy,
cleanup versus new registration, and pause versus dispatch need their own
witnessed schedules. Existing premature-cleanup/oracle controls remain;
they are not an executable production cleanup mutant. The reducer minimizes
generated cycle prefixes, not individual operations or SQL. No whole recovery
proof or merge approval is claimed.

The subsequent [snapshot/restore repair](restore-history-safety.md) addresses
missing-history recovery, metadata-read versus copy, and cleanup during an
uncommitted restore. It also strengthens terminal cut/restart assertions and
fixes CI pipeline failure propagation. Its frozen backend `0ab22465` passes
the strengthened default and ten-seed/twelve-cycle histories. Capture-mode
transitions, cleanup versus registration, pause versus dispatch and complete
protocol proofs remain outside that follow-up.
