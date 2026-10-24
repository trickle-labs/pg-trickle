# Snapshot coherence and retained-history restore — #1219

This supplemental runtime repair addresses three reproduced failures on frozen
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
3. An actual restore remained uncommitted while a sibling consumer refreshed
   FULL. The predecessor's cleanup deleted two history rows using the still
   visible pre-restore minimum. The repaired path must skip cleanup without
   blocking that refresh, honor the rewind after commit, and reclaim history
   only after both consumers catch up.

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

The three reproducing Testcontainers regressions must pass on the repaired exact
package, with unchanged baseline failure logs retained. Existing seeded restore
histories must still catch up DIFFERENTIALLY using a lagging consumer's retained
history. Their terminal phases now require restored and lagging frontier coverage
of the captured committed cut and exact rows/frontier persistence across SIGKILL.
The cancellation/snapshot witness and existing snapshot ownership/provenance
tests remain required regression coverage.

Both existing recovery CI steps explicitly select Bash, so a failing test piped
through tee fails its step. Candidate and scheduled selections include all three
regressions; artifact retention remains in the existing jobs.

The implementation relies on ordinary capture, dependency/catalog integrity,
existing source-reset recovery guards, and PostgreSQL locking/durability. It
does not prove arbitrary manual catalog/buffer tampering, source incarnation,
cross-cluster LSN identity, capture-mode transitions, all ST/foreign/MATVIEW
history grammars or all isolation schedules. Those unqualified source kinds
use reinitialization on restore. Build provenance remains runner input.

## Completed bounded qualification

Frozen backend: `0ab22465b99f5e4dde2cf595f5b5d50216b20209`; supplemental
test freeze: `efae4dccbddd43b357fee1fc3017a026febeffe8` (no backend, SQL,
Cargo or proof-source difference). Actual image
`pg_trickle_1219:restore-0ab2246`, extracted package
`target/restore-history-candidate`, library SHA-256
`4a1afb720c7d88d6efe27a2a24c832b0797fbf20db52314e233363d99bf38983`.

- `just fmt` and `just lint` pass with zero warnings; 2633 unit and 62 Python
  tests pass.
- Initial installed selection: 10/10, comprising four recovery cases, three
  snapshot security scenarios and three oracle helper tests. The added cleanup
  ordering case separately passes 1/1.
- Scheduled selection: 5/5, including ten seeded twelve-cycle histories, all
  three reproduced failures and snapshot cancellation. Existing lagging-consumer
  histories still require DIFFERENTIAL catch-up. Missing-history restore uses
  REINITIALIZE; cleanup ordering retains 2/2 rows before final reclamation.
- All fourteen runtime cases required by the six existing proof bindings pass.
  The six regenerated proof baselines/mutants and six bindings pass, retaining
  their original bounded claims.
- Both generated semantic floors pass on this package: 34/34 and 54/54.

No retries were used. The local artifact root
`.p2p/work/issue-1219-restore-history/` retains three predecessor failure logs,
successful histories and complete qualification logs. Its
`evidence/qualification-manifest.json` records seven frozen input hashes,
fourteen log hashes and all 40 installation observations matching the actual
three-file payload. Independent read-only review accepted this completed
bounded source/evidence after finding the original CI pipeline and terminal
frontier assertion gaps. The reviewer executed no project checks. No remote
CI success, authenticated compilation provenance or merge approval is claimed.
