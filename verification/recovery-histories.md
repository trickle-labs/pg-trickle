# Bounded recovery histories (#1219)

This page specifies the #1219 generated-history test slice in the existing
Testcontainers WAL-recovery and refresh-atomicity targets. The history tests
change CI selections and do not require a WAL-recovery production fix. The
integrated Quality branch also includes separately scoped snapshot, identity,
join, and proof work; it does not complete #1219 or #1225.

## Committed journal histories

`test_lifecycle_seeded_restore_lag_and_crash_preserve_committed_journal` uses
the existing `TraceConfig` and deterministic RNG: four seeds/four cycles for
the PR selection and ten seeds/twelve cycles in the scheduled DVM job. Each
seed deterministically selects one WAL interruption phase, so the four-case
PR selection covers all phases and the scheduled run repeats them. Both row
and statement capture are selected and witnessed from actual triggers.
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
slow consumer and reach the recorded source cut. The history then transitions
from trigger capture to WAL and interrupts at one of four witnessed phases:
before receipt persistence, before replay, before acknowledgement, or after
slot advance. The last phase commits a second source/journal row after the slot
moves. PostgreSQL is killed with SIGKILL and restarted in the same
container/data volume. The test checks system identifier, database OID,
persisted sentinel, volume identity and changed postmaster start time. Both
consumers must follow the same differential, full, or reinitialization path and
match the committed journal; the post-slot-advance row must be acknowledged or
force shared-consumer reinitialization. `fsync`, `synchronous_commit` and
`full_page_writes` are witnessed as on.

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
  no retries. The expanded restore history covers all four WAL interruption
  phases across those seeds. Trigger, durability, restart, and source-cut
  witnesses pass in the final runs.
- At that baseline the semantic floor was independently 32/34 and 52/54; special SUM/AVG
  and mixed-snapshot division still fail. The original four COUNT/FILTER
  regressions pass. These are not recovery-history successes.

`PGT_LIFECYCLE_ARTIFACT_DIR` selects the JSON directory (default
`artifacts/lifecycle-histories`). Records include seed, runner revision,
declared server candidate, configuration, initial state, SQL mutations,
transaction boundaries, expected/actual rows, cuts and durable identity.
Only the executed prefix through the first failing assertion is retained.
Each trace records its WAL interruption phase. Recovery invariants carry
stable failure tags so unrelated setup errors cannot count as reproducing the
same failure. The shrinker replays the fixed seed and mutation plan; because
the phase is derived from that seed, shrinking preserves the original phase.
To reduce a failing seed while preserving its exact tagged invariant, run:

```sh
python3 scripts/shrink_recovery_history.py \
  artifacts/lifecycle-histories/history-0000000012190001.json
```

The reducer first verifies the recorded mutation schedule, then uses the
existing `scripts/dvm_shrink.py` cycle reducer to try removing any whole
mutation cycle while preserving order. It rejects candidates that would
duplicate an insert or update/delete a missing row, then tries replacing
remaining generated operations with simpler DELETE/UPDATE/INSERT forms and
replacing row IDs and values with zero. Every candidate replays with the same
seed and setup; source and journal always receive the same operation.
The unminimized trace, candidate plans, traces and logs, minimized trace, and
attempt report are retained. Old traces with SQL-only mutation records remain
readable. Replay requires Docker/Testcontainers and uses the existing recovery
E2E target. Operation type can change only among the generated forms.
Source/journal statements stay paired, transaction boundaries remain intact,
and arbitrary SQL syntax is not minimized.

Existing CI jobs retain JSON histories and complete runner output on failure.
The candidate job runs the small set; the scheduled DVM job runs the larger
set. No remote CI success is claimed.

## Open boundaries

The generated history covers a pending trigger-captured commit, a
lock-witnessed cancelled refresh with unchanged output/frontier, trigger-to-WAL
handoff, snapshot restore, and all four WAL receipt interruption phases. It
checks restart recovery and exact lagging-consumer catch-up, including the
post-slot-advance row. It does not establish recovery after required history
has already been removed or arbitrary isolation schedules.

The current quality-program base includes #1266's separate witnessed
cleanup-versus-registration and pause-versus-dispatch schedules; they are not
assertions in this seeded history. Existing premature-cleanup/oracle controls
remain, and there is no executable production-cleanup mutant. The reducer only
shrinks generated operations and operands, not arbitrary SQL syntax. No whole
recovery-protocol proof is claimed.

The separate [snapshot/restore repair](restore-history-safety.md) addresses
missing-history recovery, metadata-read versus copy, and cleanup during an
uncommitted restore. It strengthens terminal cut/restart assertions and fixes
CI failure propagation. Its frozen backend `0ab22465` passed the strengthened
default and ten-seed/twelve-cycle histories. Full cleanup/incarnation proofs,
remote Citus/dblink capture, and arbitrary SQL AST reduction remain outside
this slice.
