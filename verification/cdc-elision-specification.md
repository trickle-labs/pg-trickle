# Conservative CDC changed-column images

This #1221 slice changes UPDATE masks, retains deferred-key source histories,
classifies keyed image weights and applies the verified compaction eligibility
gate to both base and stream-table buffers. It does not yet prove complete scan
normalization or the reduction SQL's equivalence for every history. The
history model proves per-image deltas for valid unique-row transitions and,
for arbitrary finite event prefixes, equates each complete-image bucket's
signed sum with its initial-to-final bag multiplicity change. The latter
permits duplicate row identities. Consumer-window additivity and equal-LSN
transaction membership are also modeled. PostgreSQL SQL execution equivalence,
per-bucket rejection of unmatched deletes, and nonnegative bag counts are
modeled. Whole-batch validity for every production multi-row history, SQL
execution equivalence, and all protected consumer windows remain separate or
incomplete obligations.

## Independent observation contract

SQL equality does not imply equality of all admitted observations. In
particular, numeric 1.0 and 1.00 compare equal while their scale differs.
For an UPDATE, an unknown representation policy must retain a changed bit.
For an admitted image policy, only equal old/new images permit a zero bit.
Different images may cause extra work even when some SQL observations agree.

The mask is not a row identity and does not cancel signed row multiplicities.
INSERT and DELETE continue to capture their rows with an all-columns marker;
UPDATE still captures both complete D/I payloads in the source transaction.
The existing union of consumer columns and scan-side mask intersection remain
separate integration boundaries.

`verification/cdc_elision.rs` contains the exact production decisions checked
by Verus: `admitted_image_type`, `keep_change`, `image_event_weight`,
`image_action`, `image_action_priority`, `can_compact_history`, and
`compact_event_pruned`. They require no unchecked preconditions.
`keyed_weight_sql` obtains the `I` and `D`
coefficients from `image_event_weight` before lowering them into PostgreSQL's
per-image `SUM`; it obtains the insert-before-delete endpoint ordering from
`image_action_priority`.
`captured_image_bucket_delta_matches_bag_count` proves the generic accumulator
equation for an arbitrary finite event prefix and complete-image bucket. This
proves the signed count semantics, not PostgreSQL's aggregate execution or the
whole-batch validity of every production history; those remain runtime
conformance and caller-assumption boundaries. The bucket history predicate
rejects an unmatched delete and its lemma establishes nonnegative multiplicity
for every accepted prefix.
`can_compact_history` admits compaction only for an immediate unique identity,
one dependent stream table and a selected LSN range with no captured TRUNCATE
marker. Base sources supply primary-key metadata; stream-table sources supply
the immediate unique `__pgt_row_id` index. Both paths inspect the same selected
window and retain histories across consumers or reset barriers. This proves the
shared eligibility decision, not the full row-history reduction or every
consumer-window equivalence. `keep_change` retains every unknown policy and every unequal
image, and elides only the admitted/equal case. `admitted_image_type` admits
only the PostgreSQL built-in OIDs for fixed-size boolean, int2/int4/int8 and
uuid. Actual runtime `pg_typeof` OIDs enforce that
set; formatted type names, domains and user types cannot impersonate it.

The base and stream-table buffer compactors retain the minimal actions for a
valid unique-identity endpoint: no rows for I→D, both endpoints for D→I, only
the last INSERT for I→I, and only the first DELETE for D→D. The same predicate
is used by both SQL paths. An installed PostgreSQL 18 regression exercises
captured I/D/I histories in row and statement trigger modes, checks the
base-source history's transaction identity, observes both compactors remove
the superseded rows, and compares both resulting stream tables with their
defining queries. This qualifies those finite base/ST cases; the full SQL
reducer is not yet covered by the Verus proof, and other capture histories and
consumer cuts remain subject to the limits below.

The production compaction SQL now obtains its prune predicate from
`compaction_prune_sql`, which emits each SQL branch only when the checked
`compact_event_pruned` decision selects its representative endpoint/rank case.
Exhaustive Rust truth-table coverage checks both action endpoints and rank
pairs from 1 through 4; the pinned Verus job verifies the executable decision
and rejects mutations that retain an extra first INSERT or drop a D→I interior
event. The installed test still exercises PostgreSQL's actual ranked-window
execution for the I/D/I case. This binds the endpoint policy to both SQL
builders. `compaction_prune_preserves_complete_image_delta` proves that the
checked rank policy preserves each complete-image signed delta for every
ranked sequence. `valid_history_compaction_preserves_image_delta` composes
that result with the valid unique-row history model and proves equivalence to
the original bag delta within one source-incarnation consumer cut. Its input
is the SQL partition for one row identity and selected LSN window; projection,
transaction-cut membership, and PostgreSQL window execution remain explicit
adapters. The proof covers arbitrarily long projected histories under those
assumptions. An installed PostgreSQL test now enumerates all 90 valid paths of
up to four transitions through absent, `a` and `b` row states in both row and
statement trigger modes. It checks captured images and transaction grouping,
requires the production base compactor to prune rows, and compares the final
stream table with its defining query. The existing I/D/I test continues to
exercise actual ranked-window execution in both base and stream-table buffers.
The installed `test_cdc_compaction_immediate_key_changes_preserve_per_identity_endpoints`
regression also moves an immediate primary key through three identities in
both trigger modes. It checks all four captured D/I images, requires the
intermediate identity's I/D pair to be pruned, and compares the resulting
stream table with its defining query. The focused PostgreSQL 18 Testcontainers
case passed locally (1/1). This qualifies a finite key-movement history; the
proof still assumes the complete production history has already been
partitioned by the same exact identity.
The companion `test_cdc_immediate_key_move_preserves_protected_consumer_cut`
case advances a fast consumer after key 1→2, lets a lagging consumer process
the later 2→3 move, then catches the fast consumer up. It verifies that the
delete for key 2 remains available after the slow refresh and that both
consumers reach the final result in both trigger modes. Its focused PostgreSQL
18 Testcontainers run passed (1/1); this is a finite witness for the shared
consumer gate and does not prove all consumer schedules.

The PK scan reducer now uses the checked `scan_initial_row_present`,
`scan_final_row_present`, `scan_delete_candidate` and
`scan_insert_candidate` decisions when generating its SQL endpoint filters.
An old image candidate requires both an initially present row and a DELETE
event; a new image candidate requires both a finally present row and an INSERT
event. The merge-safe path uses the same checked initial/final presence guards.
Truth-table unit tests and two executable mutants protect these decisions.
This proves the finite action predicates and binds their SQL adapter to the
production scan source; it does not prove PostgreSQL's `FIRST_VALUE`,
`LAST_VALUE`, `DISTINCT ON` or ordering execution, nor arbitrary invalid
histories. The installed committed-history regression continues to check the
actual differential result with FULL fallback disabled.

Both trigger builders call the same `changed_column_sql` adapter. It lowers
the checked decisions into a CASE with a type-OID guard, followed by equality
of `pg_catalog.record_send(ROW(value))` bytea images. NULL fields have explicit
record framing. Variable-length and unknown types retain a bit without invoking an equality,
output or send function for that type. PostgreSQL resolves the generic record
function at SQL type checking; only the admitted branch evaluates its body.

## SQL and PostgreSQL boundaries

Verus proves the finite admission and keep decisions. It does not prove
PostgreSQL's binary send functions, datum representation, SQL lowering,
catalog OIDs, parser dependencies, expression determinism/totality, collation,
system columns, transaction visibility, FFI, or every possible consumer
observation. Fixed expression/environment and source-schema epochs remain
assumptions of any stronger consumer-equivalence claim. No such whole-query
proof is asserted here.

The independent native PostgreSQL conformance test enumerates 195 pairs across
every admitted type, plus variable-length types, json (without SQL equality),
a numeric domain and a domain over admitted `integer` values. The admitted-base
domain confirms that the runtime catalog OID guard does not widen admission to
domain types.
It checks actual catalog OIDs and compares mask results against independent
public text/NULL observations, including integer bounds, embedded zero bytes,
NULLs, numeric scale and special values. Unknown types always retain changes.
A native storage control has equal text wire images but different
`pg_column_size` values; variable-length types therefore retain changes even
when images agree. A value-equality control loses 1.0-to-1.00 while the policy retains it.
This is finite conformance evidence, not a proof of arbitrary scalar values.

The installed regression selects both row/statement trigger modes and both
consumer creation orders. It witnesses the actual update trigger mode, two
captured D/I rows and their numeric bit, then advances the all-column consumer
before the narrow scale consumer. Forward/reverse scale changes, NULL
transitions and same-image no-ops are compared against native defining queries.
Changed cycles require fresh completed DIFFERENTIAL history without fallback.
Numeric same-image no-ops remain captured conservatively.

The installed-trigger case `test_shared_domain_over_admitted_base_retains_changed_bit`
also exercises a domain over admitted `integer`. In both row and statement
trigger modes, a same-value update produces both D/I images and retains the
domain column's changed bit. This checks the production-generated type-OID
guard for that domain case; it does not generalize to every user type or
PostgreSQL catalog behavior.

The generated histories also exposed a separate concrete-window cache defect:
decision tracing bypassed the inner delta cache but the outer MERGE cache
stored literal LSN cuts. The deterministic traced-window regression observes
the stale second update before repair. Concrete tracing, per-leaf overrides,
bypass-table, graph and window-state plans now bypass both outer cache lookup
and storage. This is an ordinary Rust integration repair, not a Verus theorem.

## Deferred keys and complete captured images

A captured history is a signed bag of complete captured rows: INSERT contributes
+1, DELETE contributes -1. Its key locates a logical row; the key alone does not
identify its payload. A committed deferred-unique source can legally emit
I(new payload) before D(old payload) for the same key. This differs from a
genuine I(payload)/D(same payload) transient row. Rollback removes both source
and trigger-captured effects in the same transaction. Consumers select their
own `(previous cut, new cut]` before reduction; the ordinary transaction/window
and source-incarnation contract remains required.

Both production differentiation entry points inspect actual primary-key
`condeferrable` metadata. Deferred keys bypass first/last-key reduction and
emit their signed events with payloads and identities intact. Key-change
metadata is conservative. Immediate keys and keyless/ST-source reducers retain
their existing paths. New cache salts invalidate previously persisted templates
that used the old deferred-key reduction.

Statement UPDATE capture also avoids key pairing for deferred primary keys:
its old/new transition rows can contain duplicate keys before commit. Those
rows are captured separately with all-column masks. Creation and both rebuild
paths enforce the actual catalog constraint. Immediate-key pairing constructs
explicit payload records, so source fields named n/o cannot shadow row aliases.

The keyed MERGE adapter groups events by complete binary record image inside
each complete row identity. PostgreSQL's
[binary record operators](https://www.postgresql.org/docs/18/functions-comparisons.html#COMPOSITE-TYPE-COMPARISON)
distinguish representations that SQL equality merges and work without a
scalar equality/send/output function, including json. Dense ranks using binary
record ordering identify equal images; signed SQL sums cancel only within an
image. `image_event_weight` proves that each insert contributes +1 and each
delete contributes -1; `image_action` checks positive=insert, negative=delete
and zero=skip. The SQL adapter obtains its coefficients and action predicates
from those executables. The existing keyed-target rule prefers a surviving
insert over an old delete of the same identity, implementing an upsert. This
requires a valid keyed endpoint; it is not a general multiplicity-expanding
bag consumer. A repeated-image integration case checks that an intermediate
I/D pair cancels while the final payload survives.

Verus proves the weight classifier and the event-level bucket lemmas below. It
does **not** prove PostgreSQL's image comparisons, SQL sums/window functions,
field binding, complete committed cuts, catalog metadata, keyed endpoint
uniqueness or upsert equivalence. This is not yet the full R1/R2 bag/window theorem. Native SQL conformance checks
numeric scale, json and NULL cancellation; the installed history checks
same-key replacement, transient cancellation and savepoint rollback for both
trigger modes against independent source queries. Shared-buffer compaction,
epoch transitions and every protected consumer window remain outside the proof.

The verifier also proves `separate_consumer_cuts_retain_opposing_images`: for
any ordered insert/delete LSNs split by two consumers' cuts, the combined
signed weight is zero while the first consumer observes `+1` and the second
observes `-1`; the production-used compaction gate rejects the shared-consumer
case. This is a parameterized counterexample to cross-window cancellation,
not a proof that the PostgreSQL reducer preserves arbitrary histories or every
consumer window.

The event model also proves `captured_image_bucket_delta_splits_at_consumer_cut`
for any finite captured history prefix and complete-image bucket. The signed
delta over `(earlier_cut, later_cut]` equals the sum over adjacent
`(earlier_cut, middle_cut]` and `(middle_cut, later_cut]` windows. An event at
the shared cut belongs only to the first interval. This establishes partitioning
for the model; PostgreSQL capture, SQL grouping/aggregation, and runtime
compaction equivalence remain separate boundaries.

The complete-image event model adds six bounded event lemmas. Events carry a row
identity, full row-image bytes, source-incarnation identifier, transaction
identifier and an abstract transaction-visibility LSN. This model coordinate
is not the per-event `lsn` value stored by trigger capture: each trigger writes
`pg_current_wal_insert_lsn()`, so several rows in one transaction can have
different captured LSNs. The theorem that one transaction occupies one model
window therefore depends on the production writer fence: a safe frontier must
stay before every event of a still-open transaction and advance across its
events only after commit. The installed late-commit regression exercises this
mapping with an insert plus update in one open transaction, checks all three
complete images share one `source_xid`, and verifies the frontier stays before
the first event until commit then advances through the last event.

Under that visibility assumption, the model proves that events from one
transaction share a consumer-window membership; a same-key payload replacement
contributes `-1` and `+1` in distinct image buckets; an identical image
insert/delete pair cancels within one incarnation and window; and an event from
an earlier incarnation contributes nothing to the current incarnation's
bucket. Its recursive sequence delta also proves that appending any captured
event adds exactly that event's signed weight to its selected complete-image
bucket. Four executable Verus mutants reject key-only image grouping,
cross-incarnation grouping, a transaction cut that admits different model
visibility coordinates and a reversed history weight.

These lemmas specify the event semantics that the production adapter must
preserve. They assume captured events already represent committed transactions
with correct source-incarnation identifiers and full row images. The model
does not prove the production writer-fence algorithm or its PostgreSQL snapshot
behavior; the installed late-commit case is finite runtime evidence, not that
general proof. They also do not prove schema changes or every
source-recreation/concurrent-DDL behavior. The installed
`test_cdc_source_recreation_ignores_pending_old_incarnation_images` case covers
one source-recreation sequence in row and statement trigger modes: it leaves
old UPDATE/INSERT images pending, recreates the same qualified source name with
a new OID and stable buffer name, and checks the replacement's initial and
differential results against its live query. This finite runtime check does
not prove every incarnation transition, concurrent DDL/capture race or
PostgreSQL reduction history.

`valid_single_identity_history_preserves_image_delta` adds a valid-transition
model for one unique row identity. Walking backward from the final state, an
insert must match the active final image and a delete must remove the active
image. For every complete image, the sum of its signed events in the selected
incarnation and consumer window equals final multiplicity minus initial
multiplicity. This connects valid history transitions to bucket arithmetic;
it does not prove that production capture enforces these transitions or that
PostgreSQL's `record_send`, `dense_rank`, window `SUM`, or keyed upsert executes
this abstract reduction exactly.

`valid_history_has_unique_signed_image_endpoints` composes the per-image
transition theorem: for a valid history of one unique row identity, at most
one image has a positive delta and at most one has a negative delta. The
production keyed adapter prefers the positive insert image when both exist,
which is the endpoint for an update; the generated SQL integration test flips
that priority as a semantic mutant and observes the old delete image instead.
This proves the selection policy under the unique-row history model, not the
database implementation of record ordering, aggregation or MERGE.

The valid-history predicate projects an interleaved captured history onto one
row identity, source incarnation and consumer window. The
`interleaved_row_identity_histories_preserve_endpoints` obligation checks two
unique rows that exchange the same visible old/new images in one committed
transaction. Their global signed sum is zero for both images, while each
identity has its own delete/insert endpoint. The executable
`cross-row-identity-bucket` mutant removes identity filtering from the model;
the generated PostgreSQL bag-oracle case removes `__pgt_row_id` from the
per-image `SUM` partition and observes both endpoints disappear. This covers
identity separation for that interleaved key-swap history; it does not prove
PostgreSQL's general sort/aggregate implementation or every multi-row source
history.

The SQL bag-oracle test also enumerates nine initial/final image pairs over
absent, `a` and `b` states. Each path passes through intermediate updates and a
delete/insert cycle, interleaved with a second identity whose history cancels.
The generated SQL is compared with an independent endpoint oracle. These
finite cases broaden the runtime sample but do not establish equivalence for
arbitrary production histories.

`multi_identity_compaction_preserves_bag_delta` composes the checked rank
pruning theorem across any finite set of per-identity histories. Each history
is independently valid for one unique row identity, source incarnation and
consumer cut; summing the compacted complete-image deltas equals the sum of
final-minus-initial row-image multiplicities. The model takes histories after
identity partitioning, matching the production `ROW_NUMBER` partitions. The
production SQL must preserve that grouping; this proof does not establish
PostgreSQL's window execution, prove the partition adapter itself, or extend
the eligibility gate across multiple consumers or reset barriers.
The packaged `test_cdc_compaction_preserves_multi_identity_bag_delta` case
qualifies that adapter for six identities and three committed updates in both
trigger modes. It observes 36 captured signed events, requires compaction to
remove the 24 superseded interior events, and compares the remaining stream
table result against the live source query.

`multi_identity_history_preserves_bag_delta` now composes the per-identity
complete-image theorem across any finite sequence of distinct logical row
identities that covers every event in the selected source-incarnation consumer
window. Each identity may have an independently valid initial/final row state,
while all histories share the captured batch and can be arbitrarily
interleaved. The sum of the per-identity complete-image deltas equals the
batch's final-minus-initial row-image multiplicity. This closes the model's
two-identity cardinality limit under the explicit unique-row and valid-history
assumptions. The theorem does not establish those assumptions for every
production batch, nor PostgreSQL grouping, aggregation, MERGE execution, or
all protected consumer schedules; those remain acceptance gaps.

## Controls and candidate binding

The pinned Verus/Z3 runner checks the executable decisions and proof
obligations listed in `scripts/check_cdc_elision_verification.py`, including
the compaction rank and valid-history preservation results:
`separate_consumer_cuts_retain_opposing_images`,
`captured_image_bucket_delta_splits_at_consumer_cut`,
`transaction_window_membership_is_atomic`,
`payload_replacement_preserves_complete_image_delta`,
`append_captured_image_bucket_delta`,
`transient_same_image_cancels_within_incarnation` and
`source_incarnation_prevents_cross_epoch_cancellation`, and
`valid_single_identity_history_preserves_image_delta` and
`valid_history_has_unique_signed_image_endpoints`,
`compact_event_pruned_matches_endpoint_ranks`,
`compaction_prune_preserves_complete_image_delta`,
`valid_history_endpoint_states` and
`valid_history_compaction_preserves_image_delta`,
`different_identity_has_zero_image_weight` and
`interleaved_row_identity_histories_preserve_endpoints`, and
`multi_identity_compaction_preserves_bag_delta` and
`multi_identity_history_preserves_bag_delta`. Every semantic mutant declared
by the checker must be rejected by the intended proof obligation or executable
postcondition, including:
`lost-image-change`, `unknown-type-elision`, `lost-positive-image`,
`reversed-negative-image`, `wrong-insert-event-weight`,
`wrong-delete-event-weight`, `prefer-deleted-image`, `cross-truncate-barrier`,
`cross-shared-consumer`, `lost-intermediate-compaction`,
`keep-extra-first-insert`, `extra-initial-insert-spec`, `key-only-image-bucket`,
`cross-row-identity-bucket`, `cross-incarnation-bucket`,
`split-transaction-cut`, `reverse-history-weight`,
`overlapping-consumer-boundary`, `lost-scan-delete-initial-state`,
`lost-scan-insert-final-state`, `lost-identity-compaction-delta`, and
`lost-identity-bag-delta`;
compilation/setup failures do not count. The
candidate-binding helper requires these obligations and controls, hashes the
production callers and the installed test sources, requires all 26 named
installed cases in `CDC_ELISION_CASES`, and checks identical
packaged/installed library bytes. CI uses the existing proof and candidate
jobs, without introducing a tier.

Binding requires the runtime's declared candidate commit to match the requested
commit. It trusts the build/test runner for build origin and successful named
test execution; byte equality is not a source-to-binary compilation proof.

Independent read-only review accepted the bounded R3 specification at
`3184b55e0ee74d82f55203bf3bd744ab9a675f1e`, including a follow-up inspection of
the completed installed binding. See [the review record](specification-reviews.md).
This does not establish the other #1221 obligations or approve a merge.
The expanded deferred-history/image-weight additions were independently reviewed
at `87cef53357918e3abb267eda44f2f324308afe4c`. The reviewer found an ordinary SQL
alias collision at PostgreSQL's 63-byte identifier limit. A native regression
reproduced SQLSTATE 42702; short numbered aliases resolve it. Follow-up review
accepted `db909f1b0a97f06b999a68a478258aa21e2e7e73` for the bounded classifier
claim and inspected ordinary repairs. Full R1/R2/window/compaction correctness
remains excluded.
The earlier three-case/62-test report records the initial candidate binding;
it is superseded by the expanded selection. On 2026-11-03, the local PostgreSQL
18 E2E runner passed all 24 cases then in `CDC_ELISION_CASES` across four test
binaries. The gate now contains 26 cases; the immediate-key movement and
protected-cut cases each passed focused local runs, but the full 26-case
selection and hosted exact-candidate binding remain to be checked. Hosted
binding must verify the requested commit and packaged/installed bytes.
The generated semantic floor remains red at 16/34 passes; that separate floor
does not measure the installed CDC case selection.

## Existing installations and limits

There is no persisted identity or change-buffer format change. Already-created
trigger function bodies retain their old mask until rebuilt with the new
library. After installing/reloading the new library, use the existing
`pgtrickle.rebuild_cdc_triggers()` procedure with the intended
`pg_trickle.cdc_trigger_mode`; that procedure applies its selected mode to the
managed sources. Previously omitted updates cannot be reconstructed from a
zero changed bit after cleanup. Rebuild affected stored results with the
documented FULL recovery refresh and resnapshot any consumers that require it.
This slice does not silently rewrite or discard existing consumer progress.

Binary serialization adds capture CPU work; variable-length and unknown types retain more updates.
The ordinary same-image no-op remains elidable for admitted fixed-size built-ins. No
throughput improvement or benchmark result is claimed.
Binary image ranking adds sorting work to keyed weight reduction; no throughput
claim is made for that change either.

The focused PostgreSQL Testcontainers oracle also exercises the full `3^4`
endpoint state space for two distinct row identities, where each initial and
final state is absent, `a` or `b`. Both identities share the same visible key
and traverse repeated `a`/`b`/absent transitions before their final states.
The generated `keyed_weight_sql` result is compared with an independent
per-identity endpoint oracle. This checks simultaneous identity grouping and
image cancellation for these finite histories; it does not establish arbitrary
multi-row SQL equivalence or every protected consumer window.
