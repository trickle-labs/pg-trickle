# Executable semantic oracle controls — #1218

This supplemental qualification completes the five requested SQL-control
categories for the existing bounded oracle. It changes tests, replay and CI;
it makes no additional backend or formal-proof claim. The original oracle
slice is in PR #1226. Its changed-data trigger/WAL × FULL/DIFFERENTIAL matrix
remains required: selected rows must change, captured cuts must be consumed,
and fresh completed history must witness the requested route without fallback.

## Observation contract

The Rust oracle checks the public relation signature and exact row bags at a
single comparison boundary. Ordinary comparison uses PostgreSQL equality.
An explicit `execution.observe_numeric_scale` replay field additionally
attaches `pg_catalog.scale(value)` to each complete row for exact built-in
NUMERIC columns (OID 1700). Duplicate multiplicity, NULL, NaN and infinities
remain part of that row comparison. This is a scale observation, not physical
storage identity, a floating-point representation claim, or a promise that
every numeric arithmetic result preserves input scale.

Exactly three generated histories opt in: `numeric_observations`, `all_first`
and `narrow_first`. The defining queries project raw values; the oracle adds
the observation. The latter two retain shared-source registration/refresh
order. Other cases retain ordinary semantics. Missing fields default to false
in format v1; nonboolean values are rejected by the Python loader. Shrinking
preserves the flag and typed transaction SQL. An explicit observation with
no admitted public NUMERIC column fails as an oracle error, never vacuously
passes. Private auxiliary numeric columns cannot satisfy admission.

Python replay attaches scale to the same complete-row projections on both
sides of EXCEPT ALL. Its admission metadata comes from the actual relation;
SQL errors remain replay errors. It does not add a new independent schema
oracle: Rust retains the full public signature check. Tests replay all three
generated histories through the real Python CLI against Testcontainers and
then inject an equal-value 1.0-to-1.00 stream-row corruption. Correct initial
comparison must pass, and only `MultisetMismatch` at
`scale_corruption exact comparison`, cycle 0, mutation null qualifies the
negative control. The Rust scale self-test separately checks ordinary equality
passes while scale observation detects two duplicate replacements.

## Four generated-SQL controls

`test_semantic_sql_controls_reject_predicate_count_payload_and_join_mutants`
runs SQL emitted by the production adapters in stock PostgreSQL. Each control
has an independently stated expected bag, an unmodified generated baseline,
and a changed executable SQL program. Both programs must execute successfully;
the baseline must equal the nonempty expected bag and the mutant must differ.
Compilation, SQL/setup errors, skipped cases and timeouts fail the test rather
than qualify a kill. Exactly four controls must run.

| Case | Executed mutation | Required phase / observation |
| --- | --- | --- |
| wrong-predicate-visibility | HAVING retraction IS NOT TRUE becomes IS FALSE | `having_retraction`: UNKNOWN must retract the old visible row |
| wrong-nonnull-count | COUNT(payload) becomes a composite IS NOT NULL test | `count_argument_nullability`: SQL NULL versus all/partly NULL composites changes the inserted/deleted argument counts |
| lost-old-payload | complete-image signed partition becomes key-only partition | `apply_same_key_replacement`: same-key replacement must install the new JSON payload |
| wrong-join-snapshot | new endpoint references become old endpoint references | `endpoint_actions`: changed value requires old retraction and new insertion |

The expected class is `MultisetMismatch` for each. Phases identify the specific
executed adapter comparison; they do not represent a separately instrumented
backend transaction phase. Assertions check mutation sites exist, so a source
change cannot silently turn a control into the baseline. Records include case,
class, phase, expected/baseline/mutant bags, compiled adapter/test source hashes,
and all three SQL hashes. An optional declared candidate identifies the native
test build; its origin remains a trusted runner input.

## Logical-slot SQL control

`test_receipt_prefix_sql_control_rejects_executable_off_by_one` creates two
logical test_decoding slots before two committed source transactions. It peeks
the first complete transaction and feeds its row count through the production
checked `receipt_ack_row_limit` helper. The baseline executes PostgreSQL
`pg_logical_slot_get_changes` with exactly n and must return the represented
prefix. The mutant executes the same prepared SQL with n+1. It must return
the same prefix plus unrepresented source changes from the later transaction.
Both SQL calls must succeed before the mismatch qualifies.

The expected class is `ReceiptPrefixMismatch`, phase
`acknowledge_complete_prefix`. The record retains both bounds and complete
expected/baseline/mutant message vectors, source/test/SQL hashes and declared
installed candidate. This qualifies the real SQL parameter boundary. It does
not prove decoder framing, durable receipt/catalog replay, consumer windows,
cleanup, reset/restore or source incarnation.

## Execution and evidence

The existing native DVM component CI job executes all four adapter controls
and uploads `semantic-sql-controls.json`. The existing installed receipt CI
selection includes the slot control and uploads `receipt-prefix-sql-control.json`.
The light E2E corpus and scheduled corpus jobs execute the numeric replay and
retain observations on success as well as failure. No new CI tier is added.

Local artifacts live under `.p2p/work/issue-1218-semantic-controls/`; generated
records are evidence, not checked-in claims of a future run. Runtime image and
package remain the frozen backend candidate
`1a717660030487b1d5e63f683c86e40f528b8fe6`, library SHA-256
`32b7d5f1cd53c5115581ddf68da3e043f1a1acb7252d7bf24db2312c02731259`.
The supplemental test source is frozen separately; installation attestations
must name the backend candidate rather than claiming it contains these tests.

These controls establish detection on the listed observations and histories.
They do not authenticate build provenance, establish SQL/FFI correctness,
prove all plans or recovery protocols, or authorize merge. The parent quality
program remains incomplete while its separately documented gaps remain.
