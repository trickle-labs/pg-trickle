# Receipt transaction framing: production scan (#1222)

Production `receipt_transaction_ends` is called by durable replay,
fresh raw-prefix matching and applied-prefix acknowledgement. Its loop is
ordinary Rust, separate from the verified acknowledgement arithmetic. It checks
one slot, strict receipt-id order, non-NULL transaction XIDs, exact BEGIN/COMMIT
markers, unchanged XID within each transaction, increasing commit LSNs and
every transaction row LSN no greater than its commit. Empty input succeeds;
nonempty input must be covered by complete transactions. Individual row LSNs
may precede a prior transaction's COMMIT.

The actual scan and its existing end-index Vec are in
`verification/receipt.rs::receipt_frame_ends`, called directly by production.
No normalized per-message Vec or extra payload copy is introduced. The borrowed
`ReceiptFrameSource` trait
supplies length, slot equality to first row, receipt-id ordering, XID presence/
equality, marker classification and parsed LSN values. Ordinary Rust implements
the trait for the existing receipt slice using the current String comparisons
and the existing checked LSN parser. The proof sees an abstract immutable
fixed ghost sequence of per-message facts through the trait contract. Every
method corresponds to that same view; the borrowed receipt adapter has no SPI,
interior mutation or external reads. Marker classification preserves exact
matching BEGIN/COMMIT and rejects mismatched reserved `BEGIN `/`COMMIT `
prefixes rather than treating them as ordinary body rows. These adapters are
explicit, independently reviewed assumptions; the theorem does not establish
String classification, SPI selection, decoding or PostgreSQL durability.

Independent successful-output grammar:

- Returned exclusive end indices strictly increase from zero through the
  entire input, with at least two messages per transaction.
- Each span begins with exact BEGIN and ends with matching COMMIT; interior
  messages have no BEGIN/COMMIT marker. Every span row has the same present XID.
- Every row uses the first row's slot and receipt IDs strictly increase across
  the whole batch.
- Every row has a valid numeric LSN no greater than that span's commit. Commit
  LSNs strictly increase between spans; individual row LSNs need not do so.
- Successful empty input returns an empty end list. An unclosed last span is
  an explicit error and cannot yield progress. Indices/arithmetic remain safe.

The claim is soundness of successful framing and safe errors/indices, rather
than a total completeness theorem for every admitted input. Preserve ordinary
caller checks and first-offending receipt/LSN error ordering are preserved via
an indexed error enum. Incomplete suffixes return without parsing their LSNs;
COMMIT parses before the span's rows; a row beyond COMMIT still reports that
COMMIT receipt. The borrowed adapter compares exact marker suffixes with
`strip_prefix`, avoiding temporary formatted strings. Persisted receipt formats
and SQL are unchanged.

The independent output grammar is `framed_prefix`/`frame_span`, with explicit
row/XID/marker/LSN predicates. Open-frame induction covers unfinished work;
closing a span proves the complete span, then appends it to the prior prefix.
Opaque predicates and explicit transaction-index triggers bound solver
instantiation; no assumptions or external proof bodies replace the scan.
Only the existing end-index vector remains in executable Rust.

Ordinary adapter tests require successful empty and complete inputs, prior
valid multi-transaction cases with old row LSNs, reserved-marker rejection,
missing XIDs, receipt ordering, commit ordering, row bounds and lazy error
context. Successful-input assertions prevent a reject-all implementation from
qualifying this soundness-only theorem. Existing production batch-limit and
concurrent-commit installed cases remain required by candidate binding.

The pinned shared-loop baseline passes 10 verified/0 errors. An incomplete-
prefix executable mutant disables the final completion check and fails
the full-coverage postcondition. All eight receipt mutants report 9 verified/
1 error at their intended postconditions. The driver and binding contract require the
new shared scan and this mutant in addition to existing receipt gates. The
initial syntax/trigger, invariant and solver-resource failures are retained
under `.p2p/work/issue-1222-receipt-framing/prototype/`; the borrowed-trait probe
is feasibility evidence only. Frozen package/runtime and binding
qualification are pending; earlier package executions do not qualify this
changed backend.

Independent read-only review accepted the preimplementation design and
ordinary source adapter, including error order, absence of per-message copies
and the distinction between matching markers and mismatched reserved prefixes.
Final proof and evidence review remains required. This is not complete
recoverable-prefix, incarnation, cleanup, reset, replay idempotence or durability
verification. SPI selection, source identity, trusted immutable adapter facts,
PostgreSQL transaction/slot semantics and build/named-execution provenance
remain outside this framing theorem.
