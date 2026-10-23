# Snapshot coherence and retained-history restore — #1219

This supplemental runtime repair addresses two reproduced failures on frozen
backend `1a717660`. It is not a formal cleanup/incarnation proof or completion
of all #1219/#1222 requirements.

1. A committed source update and insert were fully refreshed. Ordinary cleanup
   removed their consumed I/D history. Restoring an older archive and requesting
   differential catch-up then reported success while retaining the old single
   row, missing both current source rows. The regression retains this exact bag
   mismatch; no manual buffer deletion injects the failure.
2. An actual FULL refresh completed inside an uncommitted transaction. Snapshot
   creation read old metadata, observably waited behind that refresh's lock,
   then copied the new committed rows with the old frontier. The regression
   witnesses the snapshot PID's real blocker before committing the refresh and
   compares the entire archive rows/frontier pair to the refresh checkpoint.

## Ordinary implementation

Snapshot creation and restore lock the stream catalog row before reloading
metadata. The lock matches the row lock used by manual/scheduled refreshes and
precedes storage locks. Ownership is checked both before locking and after
reload; disappearance or rebinding fails before copying or truncation.

Restore preserves the archive rows/frontier pair. It retains differential reuse
only at READ COMMITTED, when current metadata needs no reinitialization, every
dependency is an ordinary TABLE, every source cut is present, and each cut has
an initialized, non-reinitializing consumer at or before it. The target itself
can protect only an equal cut; another consumer can protect an earlier one.
This avoids claiming that a previously compacted single-consumer window can
be split at an arbitrary intermediate archive boundary.

Before inspecting those protected cuts, restore acquires sorted source-OID
transaction advisory locks. Both deferred cleanup and batched persisted-frontier
cleanup try the same lock before reading consumer minima and deleting/detaching
buffer history. Contention or a missing lock result skips cleanup; errors cannot
grant cleanup authority. Once the rewind is published, a later cleanup computes
its minimum from the restored frontier. Existing single-consumer compaction
cannot run for a source shared with the independently protecting consumer.

If these conditions cannot establish retained history, restore marks
`needs_reinit` in the same subtransaction as restored rows/frontier. The next
refresh uses existing protected REINITIALIZE behavior even if differential
strategy is requested. A NULL archive frontier also requires reinitialization.
No new catalog watermark, source format, public argument or GUC is introduced.
The fallback is conservative: it does not assert history was actually removed.

## Qualification obligations

The two reproducing Testcontainers regressions must pass on the repaired exact
package, with unchanged baseline failure logs retained. Existing seeded restore
histories must still catch up DIFFERENTIALLY using a lagging consumer's retained
history. Their terminal phases now require restored and lagging frontier coverage
of the captured committed cut and exact rows/frontier persistence across SIGKILL.
The cancellation/snapshot witness and existing snapshot ownership/provenance
tests remain required regression coverage.

Both existing recovery CI steps explicitly select Bash, so a failing test piped
through tee fails its step. Candidate and scheduled selections include both new
regressions; artifact retention remains in the existing jobs.

The implementation relies on ordinary capture, dependency/catalog integrity,
existing source-reset recovery guards, and PostgreSQL locking/durability. It
does not prove arbitrary manual catalog/buffer tampering, source incarnation,
cross-cluster LSN identity, capture-mode transitions, all ST/foreign/MATVIEW
history grammars or all isolation schedules. Those unqualified source kinds
use reinitialization on restore. Build provenance remains runner input.
