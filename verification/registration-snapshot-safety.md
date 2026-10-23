# Initialization snapshot and published frontier — #1219 / #1222

An initialized consumer must not publish a capture cut beyond committed source
changes absent from its copied baseline. Source relation locks protect writers
while copying, but cannot refresh an already fixed transaction snapshot.

## Reproduced failure

The predecessor backend `1bcd6a8e` has one existing initialized consumer and no
pending data rows. A second connection begins REPEATABLE READ and reads the
one-row source, pinning its snapshot. Another connection commits a second
source row and the matching independent journal row. Creation in the first
connection then copies only the old row while publishing a frontier newer
than the writer's captured LSN. Forced DIFFERENTIAL refresh still misses the
second row: actual one versus expected two.

The frozen reproducer is `e0b6a2e`. Complete failure output and the snapshot,
writer cut, initialized/refreshed states and journal are retained under
`.p2p/work/issue-1222-registration-snapshot/baseline/` and
`/tmp/pg-trickle-registration-baseline-runtime.log`. This is an exact-row
failure after successful creation/refresh, not setup rejection or timeout.

## Repair and compatibility

The shared `create_stream_table_impl` path rejects REPEATABLE READ and
SERIALIZABLE when initial population is requested, before storage/CDC setup,
copying rows or publishing an initial frontier. The existing
`SafeFrontierUnavailable` error includes the table and READ COMMITTED retry
requirement; SQL-boundary error propagation rolls back creation's table,
dependency and catalog writes. An unavailable isolation observation fails
closed. READ UNCOMMITTED retains PostgreSQL's READ COMMITTED semantics.

Independent review moved admission ahead of CDC infrastructure: setup can
reconcile legacy WAL sources by dropping slots, which ordinary catalog rollback
cannot restore. A separate installed scenario must retain an existing WAL slot
and mode after both isolation rejections, then acknowledge a subsequent real
source change and match its journal.

READ COMMITTED initialization retains its existing source fencing, bounded
capture observation, row copy and transactional frontier publication. The
guard applies to initial population regardless of requested maintenance mode.
Creation without population (`initialize => false`, or external orchestration
that disables population) does not execute this path and publishes no populated
frontier. Its first refresh must establish the ordinary safe baseline.

The regression checks both fixed-snapshot isolation levels, actual committed
writer changes, the specific isolation rejection and absence of a partial
catalog/storage registration. READ COMMITTED retry must initialize exact rows
and consume a subsequent commit differentially. Deferred creation remains
allowed under a pinned snapshot, stays unpopulated/frontierless and catches up
through its first READ COMMITTED refresh.

## Evidence boundary

This is a runtime adapter repair, not a new formal theorem. Existing bounded
proofs retain their claims; any caller-input hash changed by the repair must
be regenerated and bound to the installed package. Installed qualification is
pending. It must exercise rejection/retry/deferral, existing creation modes,
recovery boundaries and current semantic floors.

PostgreSQL SPI snapshot behavior, relation locks, transactional DDL, source
identity/classification and capture atomicity remain trusted database/adapter
assumptions. This does not prove cleanup eligibility, incarnation, general
snapshot/restore protocols, remote build provenance or every concurrency
schedule. No merge approval is implied.
