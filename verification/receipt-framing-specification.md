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

## Frozen source and qualification

Backend `830c6a13a1025afe2f505d699ac6b699b5cf18ff` is built as
`pg_trickle_1222:framing-830c6a1`, with actual four-file package
`target/receipt-framing-candidate`. The extracted library SHA-256 is
`945ec0a0fe1500bc71ba952df45b2a4789c7e9dfddb84b275298063c144fd4f9`.
Final base/runtime images are linux/amd64 and the library has ELF machine 62.
The first arm64 runtime build used the existing amd64 compiler; it was retained
as an unqualified setup attempt and never extracted or run for qualification.

All six pinned suites were executed again at this backend. Baselines are LSN
44/0, receipt 10/0, lifecycle 6/0, CDC 4/0, join 6/0 and identity 6/0; all 28
actual executable mutant postconditions fail. The new incomplete-prefix mutant
disables the final completion check and directly fails successful-output full
coverage. All eight receipt mutants report 9 verified/1 error. The driver and
binding require the shared scan and this mutant in addition to existing gates.
Independent read-only proof review reconstructed all mutant source/output
hashes and checked current kernel/caller/test and pinned tool identities.

Units pass 2638/2638, including the new named framing adapter case; Python
checks pass 41/41. Formatting/lint has zero warnings, and release-contract and
upgrade-completeness checks pass. Corrected command/package-path setup failures
remain retained separately from these passing checks. The initial proof syntax,
trigger, induction and solver-resource failures remain under
`.p2p/work/issue-1222-receipt-framing/prototype/`; the borrowed-trait probe is
feasibility evidence only.

The first atomicity execution passed fifteen tests, then timed out before the
singleton's `input_boundary` witness after a logged NO_DATA refresh; three
cases were not run. A due-clock update could occur after the tick's fanout
change cache was built. No pause call or framing failure was observed in that
timed-out case. Qualification-only commit
`0d9f6824a4ac4e2d1b679a057b6873cb29123830` adds an acknowledged persistent
drain before both setup batches and resumes global dispatch before the actual
barrier/control witnesses. It changes no pause, row, frontier, error or exact-
result assertion and leaves per-node pause flags intact. This fixture is a
separately hashed qualification input; backend/proof/package source stays
frozen at `830c6a13`.

The fresh corrected atomicity run passes 19/19, WAL 35/35, additional core
13/13, scheduler/mixed-mode 28/28 and observed semantic floors 34/34 and 54/54.
All six bindings and 90 actual four-file package/install observations match.
The final validator checks five backend-frozen changed inputs, the separate
qualification-test input, 58 complete copied logs, native check markers and all
named bound runtime PASS records. Exact generated install SQL bytes are bound
in the payload; its noncomment SQL-line multiset matches the unchanged archive.
See `.p2p/work/issue-1222-receipt-framing/evidence/qualification-manifest.json`.
Final commands use
`--retries 0`; the earlier failed atomicity execution is preserved separately.

Independent read-only review accepts the design, ordinary adapter, frozen
proof evidence, qualification-fixture correction and final installed evidence.
See `specification-reviews.md` for the reviewed boundaries. This is not complete
recoverable-prefix, incarnation, cleanup, reset, replay idempotence or durability
verification. SPI selection, source identity, trusted immutable adapter facts,
PostgreSQL transaction/slot semantics and build/named-execution provenance
remain outside this framing theorem.
