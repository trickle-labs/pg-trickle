# Local WAL slot retirement (#1222)

## Observed predecessor failure

The frozen reproducer at `443bdee2` runs the installed predecessor backend
`49a8ed9e`. With a real inactive WAL slot and the scheduler disabled, it drops
the last consumer inside an explicit transaction and rolls that transaction
back. Consumer rows, frontier, population state and WAL dependency return to
their baseline. The slot is absent. PostgreSQL replication-slot removal is
not rolled back with SQL catalog changes.

The baseline log and JSON observation are retained under the task evidence
directory. Missing-slot health checks and reinitialization are counterevidence
against claiming immediate silent result corruption; the demonstrated failure
is loss of a restored consumer's retention boundary.

## Transaction boundary and ownership

API cleanup and forced trigger fallback now enqueue exact saved slot names in
the logged, private `pgt_wal_slot_retirements` catalog. Enqueue failure aborts
the catalog change. Caller rollback discards the intent along with the consumer
change. The source-drop hook records local provenance before deleting source
tracking metadata. The queue deliberately has no cascading source foreign key.
An existing reservation cannot change its source OID or capture owner: the
transactional upsert checks both identities atomically and rejects a conflict.
Reusing a source name can therefore require dropping its old suspended consumer,
letting the scheduler retire the old slot, and retrying registration. Stale
capture-owner reservations also require resolving provenance before cleanup.

A later scheduler transaction performs physical removal. It locks the current
ACTIVE capture owner `FOR SHARE`, checks database and system identity, takes a
source advisory transaction lock, and rereads each intent `FOR UPDATE`. It scans
WAL/TRANSITIONING references across all source OIDs by exact slot name, including
the persisted stable-name fallback for NULL dependency names before consulting
live relation names. A NULL name on
the queued source OID conservatively retains the intent even after source DROP
makes canonical-name resolution impossible. An absent slot
is distinct from an existing slot with a mismatched database or plugin; physical
slot NULL fields classify as mismatch. Optional SPI queries return one nullable
scalar row, so an empty lookup cannot masquerade as a successful classification.

The shared executable gate authorizes DROP only when the queued capture owner
matches, no active reference exists, and the actual slot belongs to this
database with `test_decoding`. A matching absent slot permits forgetting the
intent. Stale owners, active references and mismatches retain their intent.
The owner lock remains held through physical removal, serializing capture
recovery. A successful drop followed by worker abort leaves the previously
committed intent; a later drain observes absence and removes it idempotently.

The per-database scheduler holds a session advisory mutex across all local
pipeline transactions, acquired before registering scheduler state. This
excludes a second local coordinator during pristine creation and adoption.
Before pristine creation, Phase 1 commits a cleanup reservation. Adoption
requires the same current owner, exact reservation/source name, an extant
TRIGGER consumer, and an effective WAL/auto request. Committed adoption installs
WAL references and cancels the reservation together. Rejection or transaction
abort preserves the earlier reservation for cleanup. Restart therefore also
covers a physical slot created before adoption.

## Executable theorem and limits

`verification/receipt.rs::retired_slot_action` proves this finite decision table
on the supplied inputs. The pinned Verus runner also changes each ownership,
reference and slot-mismatch guard separately and requires the intended
postcondition failure. The existing acknowledgement and source-free predicates
retain their own obligations and mutation controls.

This is not a proof of SQL snapshots, PostgreSQL locking, catalog durability,
the whole receipt-prefix loop, plugin parsing, source classification or the
entire transition protocol. Those remain adapter assumptions checked by source
review and named installed tests. Package/install byte equality relies on a
trusted build and test runner; it does not authenticate compilation.

The admitted pipeline is local capture. Remote Citus/dblink slot creation is
excluded. Privileged external slot replacement, catalog tampering or releasing
the scheduler's session locks is outside the ownership assumption. Stale-owner
and mismatched-slot intents fail closed and can retain WAL until an operator
resolves their provenance. Disabling the scheduler also defers physical cleanup.

## Upgrade and qualification

The new catalog requires the `0.108.3` to `0.108.4` migration. Fresh installs and
upgrades revoke PUBLIC access. Physical restart retains intents; logical dumps
exclude live OID/capture-owner intents with an extension config-dump filter.
The release contract retains prior required suites and adds retirement runtime
cases plus an upgrade from the actual published `0.108.3` package. Passing the
contract validator alone does not mean those release suites have run.

Required installed observations cover last-consumer rollback, deferred
registration rollback, committed intent surviving restart, crash after physical
DROP, stale owner, active reference, logical/physical slot mismatches, owner
recovery blocking, source-drop cleanup, and rejected adoption of an orphaned
slot, and source-name replacement without reservation reassignment. The
replacement reproducer observes a changed intent and successful old-consumer
cleanup on the earlier candidate; it does not demonstrate cleanup failure or
output corruption. The new admission preserves the reservation's source/epoch
until retirement instead of attempting to prove cross-incarnation reuse here.
Qualification results are recorded only after those executions complete.
