# Source-free receipt bookkeeping and exact acknowledgement — #1222

This supplemental runtime repair removes recursive receipt capture without
weakening the requirement to durably apply source changes before acknowledging
them. It does not complete the incarnation, cleanup or recovery protocol proofs.

## Reproduced observation

The predecessor `0ab22465` decodes every table through test_decoding, including
its own receipt INSERTs and status UPDATEs. Their raw payloads contain earlier
receipt payloads. The bounded regression committed two real source changes and
observed 81 durable receipts with recursively nested receipt-table messages.
This is a semantic assertion after actual source acknowledgement, not a timeout
or resource-exhaustion control. The retained predecessor log and JSON record
live under `.p2p/work/issue-1222-receipt-feedback/`.

## Independent safety contract

Consider a validated decoded prefix partitioned into complete BEGIN/COMMIT
transactions, ordered by commit position. A transaction may be consumed only
when one of these two conditions holds:

1. Its complete `(lsn, xid, payload)` sequence equals the next durable APPLIED
   transaction. Source changes were written into their ordinary buffer in the
   same committed transaction that marked those receipts APPLIED.
2. Every body row is source-free ordinary INSERT/UPDATE/DELETE on exactly
   `pgtrickle.pgt_wal_receipts` or `pgtrickle.pgt_change_tracking`. A complete
   empty transaction also has no source effect. This is private bookkeeping
   under the existing catalog-integrity assumption.

Unknown relations, logical messages and TRUNCATE cannot satisfy condition 2.
Neither can any row classified as a source change, including a tracked private
table. A mixed transaction containing source/unknown work retains its complete
raw sequence; individual bookkeeping rows are not removed from it.

The consumed prefix stops before unrepresented non-bookkeeping work. A mismatch
against a pending APPLIED transaction fails before the consuming SQL runs.
Transactions already covered by the slot's confirmed commit position may finish
their local ACK status without being consumed or replayed again.

## Executable adapter

Poll reads owned decoded messages, validates all transaction framing, and
persists every complete non-bookkeeping transaction. It leaves source-free
bookkeeping for acknowledgement without inserting another receipt for it.
Only persisted transactions update the receipt high-water marker.

Acknowledgement takes the same source-OID transaction lock as polling, using
a nonblocking attempt so contention preserves the current slot. Polling orders
multi-source locks consistently with restore. A fresh peek and the committed
APPLIED ledger feed `represented_slot_prefix`: complete byte-matching applied
transactions take precedence, retaining compatibility with legacy bookkeeping
receipts. Otherwise only the conservative bookkeeping class can pass. Unknown
new work waits for a future receive/replay cycle.

The chosen raw prefix, including skipped bookkeeping, determines the checked
positive i32 request n. PostgreSQL consuming SQL must return exactly the same
raw sequence. Only covered APPLIED receipt IDs become ACKNOWLEDGED, and their
last COMMIT updates source acknowledgement progress. Skipping bookkeeping alone
writes no new receipt/status/watermark. WAL transition readiness continues to
use the actual slot confirmation, so an idle source can complete handoff.

Existing crash recovery after slot advancement uses complete commit positions.
After-slot test barriers target only newly consumed source receipts. Receipt
witness queries likewise require `source_change`, preventing nested bookkeeping
text from masquerading as acknowledgement of the intended source change.

## Formal and runtime boundaries

The shared Verus executable proves the exact n count and the unconditional
row-decision contract `!source_change && known_table && ordinary_dml`. The
ordinary caller computes these inputs from the canonical source-name set,
the two exact private names and parsed action. Four executable mutants alter
the bound, source guard, known-table guard or action guard; all must fail their
intended postconditions. This finite predicate proof does not establish parser
correctness, table classification, the prefix planner loop, transaction/FFI
semantics or durability. It is not a whole receipt-protocol proof.

Pure tests check all predicate inputs, conservative unknown/mixed/TRUNCATE
cases, exact prefix planning, legacy receipts, recovery of already-consumed
commits, mismatch-before-consume and rejection of partial frames. Installed
qualification must cover repeated source activity without recursive payloads,
the late-old-row-LSN commit schedule, transaction-size boundaries, and existing
receipt crash/fallback controls. No installed candidate success is asserted
until those checks complete. The binding contract requires all three named
receipt cases and all four intended semantic mutants.

Catalog integrity, current source/partition classification, existing source
eligibility/reset guards and PostgreSQL decoding order remain assumptions of
the ordinary adapter. Arbitrary private-catalog tampering, source incarnation,
cross-cluster LSN identity, every transition schedule and complete protected
consumer/cleanup proofs remain outside this slice. A source intentionally
observing its own receipt table is not declared source-free and cannot use the
bookkeeping skip to suppress real observations. Build provenance remains a
trusted runner input.
