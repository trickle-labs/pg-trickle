# Receipt acknowledgement limit: bounded production slice

This is the first slice of #1222, part of #1225. It does not complete the
cleanup, incarnation, reset/restore or recovery-transition proof work.

The original raw-stream adapter below describes frozen `c06b7125`. The
subsequent [feedback repair](receipt-feedback-safety.md) conservatively omits
source-free ordinary table transactions from persistence and validates their
place in a freshly peeked exact consuming prefix. Source and unrecognized
transactions retain complete durable receipts. The arithmetic contract still
uses the entire consumed raw prefix, including source-free work, as n.

## Independent contract

For a persisted, applied prefix containing `n` decoder messages and ending at a
complete source transaction, the acknowledgement request must stop at that
transaction. PostgreSQL checks its decoded row limit after each transaction
and stops when the decoded count reaches the requested limit. The PostgreSQL
18 [implementation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/logicalfuncs.c)
uses `upto_nchanges <= p->returned_rows`; the SQL documentation's prose says
"exceeds", so it must not be used to infer a strict inequality here.

Therefore the admitted arithmetic domain is `2 <= n <= i32::MAX`, and the
requested limit is `n`. The lower bound accounts for at least BEGIN and
COMMIT; zero would disable the useful row bound. Inputs outside that domain
return `None`, causing an explicit transition error rather than a truncating
cast or unbounded acknowledgement request.

`verification/receipt.rs::receipt_ack_row_limit` is the executable used both by
Verus and `src/wal_decoder.rs::acknowledge_receipt_batch`. Its postcondition
establishes the exact limit and its bounds. Changing the request to `n + 1`
fails that postcondition; it is not a compile-error mutation control.

## Original adapter obligations and separate runtime evidence

- `poll_wal_changes` persists the complete decoder output atomically before
  replay or acknowledgement. The decoder includes unrelated table messages;
  counting source changes alone cannot determine the acknowledgement limit.
- `load_wal_receipts` treats the configured row count as a soft budget and
  extends it through COMMIT in one SQL statement, using receipt insertion
  order. Individual change LSNs are not decoder order when transactions
  overlap. It rejects a pending batch without a COMMIT or without an initial
  BEGIN. Persisted partial statuses from an older executable take the existing
  trigger fallback/reinitialization path; they are not silently acknowledged.
- Replay and acknowledgement operate on separate committed stages. The
  consumed `(lsn, xid, payload)` sequence must equal the expected applied
  receipts before local acknowledgement status is advanced. A sufficiently
  advanced slot retains the existing recovery-after-slot-advance behavior.
- Transaction framing validates source XIDs, one slot per batch, increasing
  receipt IDs and increasing commit positions. Replay stamps buffered changes
  with their transaction's COMMIT position, preserving a late commit whose row
  LSN precedes an already-consumed transaction. Status updates use receipt IDs
  and acknowledgement skips only complete transactions already covered by the
  slot, rather than filtering individual rows by LSN.
- The runtime test emits 106 messages in one transaction: BEGIN, 52 source
  inserts, 52 independent journal inserts, COMMIT. Budgets 100, 106 and 107
  cover below/equal/above the transaction size. It requires all six kinds of
  evidence: complete message count, one COMMIT, all receipts acknowledged,
  exact journal result, WAL capture mode and absence of full reinitialization.
  A separate two-writer test commits the transaction with newer row LSNs first,
  waits until its receipt is acknowledged, then commits the older row and
  requires catch-up. The stream frontier may remain held by the writer fence
  while that older transaction is open.

The SQL prefix selection, PostgreSQL output ordering, source transaction
boundaries, SPI behavior, slot side effects, source ownership and crash
durability are **not** established by this arithmetic proof. Existing receipt
barriers and retained-cluster recovery tests remain necessary. In particular,
the proof does not turn a maximum observed LSN into a complete recoverable
prefix and does not authorize cleanup, reset, restore or source reuse.
An independent read-only agent review accepted the bounded arithmetic
specification at `c06b71252c86aa6e0b1da2e68927280cb9e5de65`. See
[the review record](specification-reviews.md). The arithmetic postcondition and
executable control are not a proof of the transaction-framing adapter.

## Reproduction and CI

Run `python3 scripts/check_receipt_verification.py` for the pinned Verus
baseline and executable mutations. The feedback follow-up adds the source-free
bookkeeping decision and three guard mutants to the original n+1 control.
It reuses the LSN proof runner, compiler and
solver identity collection, summary checks and source digests. Records are in
`.p2p/work/issue-1222-receipt-prefix/evidence/`. Ordinary Rust boundary tests
cover the minimum, maximum and rejected count domains. CI executes this proof
in the existing prerequisite proof job before candidate package builds and
uploads the receipt evidence separately.

Run the installed-extension test with:

```sh
just build-e2e-image
bash scripts/run_e2e_tests.sh --test e2e_wal_receipt_recovery_tests \
  -E 'test(test_wal_receipt_small_limits)' --retries 0
```

An arithmetic proof record alone is not source/package/installation binding.
The existing binding helper's `--proof-kind receipt` mode additionally requires
matching caller/build-input hashes, all three required receipt E2E cases and matching
packaged/installed library bytes. The existing packaged-candidate CI job runs
this mode after the receipt test. A configured workflow is not evidence that
its installed-candidate qualification has run successfully.

The binding helper trusts the surrounding build/test runner for build origin
and successful execution of the supplied test names. The historical local
observations had a NULL candidate commit; they establish package/install byte
equality, not independent frozen-build provenance. Current binding rejects
missing or mismatched runtime commit fields. The suite's duplicated-result
control tests the bag oracle, rather than a duplicate-replay executable mutant.
