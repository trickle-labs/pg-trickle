# Conservative CDC changed-column images

This #1221 slice changes UPDATE masks, retains deferred-key source histories
and classifies keyed image weights. It does not yet prove complete scan
normalization, shared-buffer compaction,
source incarnation/epoch transitions, complete transaction histories, or all
protected consumer windows. Those obligations remain open.

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
by Verus: `admitted_image_type`, `keep_change` and `image_action`. They require no unchecked
preconditions. `keep_change` retains every unknown policy and every unequal
image, and elides only the admitted/equal case. `admitted_image_type` admits
only the PostgreSQL built-in OIDs for fixed-size boolean, int2/int4/int8 and
uuid. Actual runtime `pg_typeof` OIDs enforce that
set; formatted type names, domains and user types cannot impersonate it.

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

The independent native PostgreSQL conformance test enumerates 186 pairs across
every admitted type, plus variable-length types, json (without SQL equality)
and a numeric domain.
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
image. `image_action` checks positive=insert, negative=delete and zero=skip.
The SQL adapter obtains its action predicates from that executable. The
existing keyed-target rule prefers a surviving insert over an old delete of
the same identity, implementing an upsert. This requires a valid keyed endpoint;
it is not a general multiplicity-expanding bag consumer.

Verus proves the weight classifier only. It does **not** prove PostgreSQL's
image comparisons, SQL sums/window functions, field binding, complete committed
cuts, catalog metadata, keyed endpoint uniqueness or upsert equivalence. This
is not yet the full R1/R2 bag/window theorem. Native SQL conformance checks
numeric scale, json and NULL cancellation; the installed history checks
same-key replacement, transient cancellation and savepoint rollback for both
trigger modes against independent source queries. Shared-buffer compaction,
epoch transitions and every protected consumer window remain outside the proof.

## Controls and candidate binding

The existing pinned Verus/Z3 runner checks all three obligations. Executable
lost-image-change, unknown-type-elision, lost-positive-image and
reversed-negative-image mutants must fail a postcondition;
compilation/setup failures do not count. The existing candidate-binding helper
requires all four controls, source/caller/build hashes, all three named installed cases,
and identical packaged/installed library bytes. CI uses the existing proof and
candidate jobs, without introducing a tier.

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
The same candidate passes all three required installed CDC cases within a
62/62 runtime report, with zero skips/retries. Independent follow-up inspection
confirms its declared commit and package/installed byte equality, subject to
the trusted-runner boundary above. The generated semantic floor remains red
at 16/34 passes.

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
