# Restricted join endpoint checker

This #1223 slice selects an affected-key endpoint route for a computed root
projection over an inner join of two keyed base scans. It does not verify
legacy split-action or nested correction branches. Unsupported proof scope
keeps its existing product route; no new FULL fallback is used to hide a
differential failure. The complete issue remains open until its stated
qualification and any remaining selected obligations are satisfied.

## Independent semantics

A source state is a finite bag of complete logical payload images. For each
left/right payload-image pair, let its source multiplicities before the
window be `l0,r0` and after it be `l1,r1`. A fixed deterministic join predicate
contributes only when it is SQL TRUE, including SQL NULL as nonmatching.
The endpoint difference at that pair is `l1*r1 - l0*r0` when matching and zero
otherwise. This definition is independent of the generator's delta formulas.
Images that have equal SQL scalar values may remain distinct images.

`verification/join_plan.rs`, imported by production, describes two scan
occurrences and two endpoint terms. Scan occurrences identify roles, not
unique source table OIDs: a self join still has distinct left/right roles.
An insertion must bind the left/right roles to after/after, with positive
sign; a retraction must bind them to before/before, with negative sign.
`check_endpoint_plan` accepts exactly that shape, without preconditions, and
proves its signed sum equals the independently defined difference for all
integer multiplicities and both predicate outcomes. Wrong signs, swapped
roles, mixed boundaries and identical role IDs are rejected before SQL use.

`scope_pair` returns exactly left-changed OR right-changed. The separate
`unchanged_pair_has_zero_difference` lemma proves that omitting a pair with
unchanged multiplicities changes no delta. The executable SQL adapter calls
the checker with local occurrence IDs 0 and 1. The ordinary binding adapter
maps those roles to the actual child scans, their aliases and complete keys.
The proof does not parse SQL names or prove that mapping itself.

## Production admission and source boundaries

The ordinary route requires a root computed Project directly over two Scan
children of an InnerJoin. Both scans have complete primary-key metadata and
distinct aliases. Generated payload field names must be unique and avoid the
reserved action/identity columns. The predicate must be a single equality
between qualified columns on opposite sides, with the same exact built-in
bool, int2, int4, int8 or uuid OID. Domains and implicit cross-type comparisons
are excluded. The stream-owner namespace must resolve that same-type `=` to
the actual pg_catalog operator. Single-key USING retains a typed equality in
the parser; multi-key/raw predicates are outside this slice.

IMMUTABLE is a determinism restriction, not a totality theorem. Existing
analyzed-query admission rejects STABLE and VOLATILE differential queries.
The new route allows honest IMMUTABLE functions to throw on invalid inputs.
It applies projections only to old/old and new/new joined payloads; no mixed
pair is justified by later algebraic cancellation. A genuinely invalid new
endpoint still raises its PostgreSQL error and must publish neither partial
rows nor progress. Function definitions, schemas, expression environment,
correct previous results and source incarnation remain fixed assumptions.

Manual trigger-mode refresh takes existing OID-ordered source SHARE locks,
including partition descendants, before selecting its WAL bound. It rejects
REPEATABLE READ and SERIALIZABLE, whose older transaction snapshots cannot
be made current by a late lock. Locks prevent concurrent source writes until
transaction end. Concrete generation rechecks own-backend granted source
locks, READ COMMITTED-compatible isolation, actual TABLE/TRIGGER dependencies,
no captured action beyond the selected per-source cut, and no TRUNCATE in
the selected window. Each source's complete committed capture must be
retained from its previous cut through the new cut. WAL, fixed graph bounds
and scheduler paths receive no new automatic admission or source fencing.

The affected-key SQL takes keys from both complete captured deltas and
filters each endpoint source with built-in equality. Duplicate scope keys
do not multiply rows because this is membership testing. NULL keys cannot
match the admitted join predicate. A changed join key contributes its old
and new captured keys. Rewinding current rows with the inverse signed
captured images reconstructs old keyed endpoints, including deferred-key
replacement and transient rows. This relies on complete capture, exact
window selection and the ordinary keyed-image reducer; Verus does not prove
those SQL/transaction boundaries.

Old/old and new/new joins retain complete pair identities and payloads.
Unchanged pair images cancel through explicitly pg_catalog-qualified complete
bytea identity comparison and
`pg_catalog.record_image_eq`, preserving numeric scale, NULLs and types
without scalar payload equality. Direct record comparison is needed:
PostgreSQL expands a row-constructor binary operator into field operators.
The native SQL adapter test uses literal complete identities, independently
checks exact signed actions and duplicate payload multiplicities, and rejects
an invalid plan before executing SQL. This is conformance evidence, not a
proof of PostgreSQL record functions or row-identity encoding.
The same native case witnesses an owner search path that shadows bytea `=`
while leaving the admitted integer predicate equality built in, and requires
identical complete-identity actions under that path.

## Qualification and trust boundary

The pinned compatible Verus runner requires six verified obligations and
zero errors. Five executable mutants must fail their intended postcondition:
unknown equality type, lost right scope, mixed snapshot, swapped occurrence
and wrong sign. The separate native SQL control compiles an isolated copy
of the actual adapter with both terms inserting. It requires a passing
baseline and an exact signed-action mismatch, not a compiler/setup/SQL error.
It does not modify or qualify a mutant release library.

Installed tests cover both trigger modes, all five equality types, ON/USING,
duplicate join keys, NULLs, direct and IMMUTABLE throwing projections, both
sides changing, deferred replacement and transient rows. They compare exact
public result/schema bags, fresh DIFFERENTIAL history without fallback,
unchanged rows' physical xmin and failed-endpoint rows/frontier atomicity.
A separate witnessed schedule checks both source SHARE locks, an actually
blocked writer, the first endpoint cut and the next-cut catch-up. Runtime
results and frozen candidate hashes are recorded with the independent review.

Candidate binding checks proof/caller source hashes, a declared frozen commit,
and package/installed library bytes. Build origin and successful execution of
the named cases remain trusted runner inputs, not authenticated compilation
proof. CI regenerates each record for its exact combined candidate.

The checker does not prove arbitrary joins, nested compositions, legacy
correction terms, arbitrary predicate totality, source capture/window/epoch
protocols, key encoding, hash uniqueness, scalar canonicalization, SQL
lowering, planner behavior, FFI or transaction atomicity. Native regressions
qualify selected ordinary boundaries only. Locks can delay writers for the
refresh transaction; no throughput improvement is claimed. Persisted formats
are unchanged. Previously stale stream results require the existing protected
FULL recovery path before a stronger previous-state premise can hold.

PostgreSQL's [LOCK documentation](https://www.postgresql.org/docs/18/sql-lock.html)
defines the source lock, transaction duration and older-snapshot constraints.
Its [expression rules](https://www.postgresql.org/docs/18/sql-expressions.html)
explain why algebraic cancellation cannot establish safe evaluation order.
