# Proposal: Exact, Versioned Row Identity V2

**Status:** Proposed
**Target:** Pre-1.0 / v0.83.0 DVM semantic fidelity work
**Decision:** Replace hashed `BIGINT` row identities with canonical `BYTEA`
**Migration:** Rebuild every existing stream table; mixed V1/V2 operation is unsupported

## 1. Executive Decision

pg_trickle should replace `__pgt_row_id BIGINT` with an exact, versioned,
canonical `BYTEA` encoding of the logical identity fields.

V2 should not hash the canonical identity into 64 bits. The canonical bytes are
the identity. This removes session-dependent `::TEXT` formatting and hash
collisions. For types whose registry entries explicitly guarantee native-order
preservation, it also lets B-tree probes retain source-key locality instead of
deliberately randomizing it.

The storage contract becomes:

```text
__pgt_row_id     BYTEA NOT NULL   -- complete canonical identity
__pgt_row_probe  BYTEA GENERATED ALWAYS AS
                 (pgtrickle.row_probe_v1(__pgt_row_id)) STORED
```

Matching uses both columns:

```sql
st.__pgt_row_probe = delta.__pgt_row_probe
AND st.__pgt_row_id = delta.__pgt_row_id
```

The probe is an index accelerator, not an identity. For ordinary identities it
is exactly the full row ID. For unusually wide identities it is an ordered
prefix plus a 128-bit digest. The full row ID comparison remains authoritative,
so a probe collision can only add a candidate row to an index scan; it cannot
merge, overwrite, or delete the wrong row.

There should be one V2 encoding for the extension. No per-table encoding option,
no simultaneous `BIGINT` and `BYTEA` strategies, and no direct-integer side path
are proposed. A signed integer already has a compact, order-preserving V2 byte
encoding, so another strategy would add coordination and migration states
without adding a capability.

```text
typed logical fields
        |
        v
exact canonical encoding          --->  __pgt_row_id
        |
        v
bounded lookup probe              --->  __pgt_row_probe B-tree
```

## 2. Why Replace the Hash

The current row-ID path already frames composite inputs with an encoding version,
component count, NULL/value tags, and per-value lengths. Inputs such as `(ab, c)`
and `(a, bc)` therefore hash different framed byte sequences. V2 does not need to
fix delimiter ambiguity.

Four structural problems remain.

First, fields are converted to `String` values rather than encoded from typed
PostgreSQL datums. Text output can depend on PostgreSQL settings and type output
behavior.

Second, the final identity is only 64 bits. Different canonical inputs can hash
to the same `BIGINT`. Because MERGE treats the hash as proof of identity, a
collision can silently overwrite or delete the wrong logical row.

Third, a hash destroys potentially useful source-key ordering. MERGE probes
become random B-tree accesses once the storage table is larger than
`shared_buffers`, even when rows arrive in primary-key order and the source type
has a byte encoding that can preserve its native B-tree order.

Fourth, the current SQL shape pays conversion and allocation costs on every row:

```sql
pgtrickle.pg_trickle_hash_multi(
    ARRAY[(expr1)::TEXT, (expr2)::TEXT, ...]
)
```

Changing only the hash input would fix the first and fourth problems while
deliberately retaining the second and third. Since V2 already requires every
stream table and CDC identity buffer to be rebuilt, preserving `BIGINT` does not
avoid the expensive part of the migration. This is the correct point to adopt
the durable representation rather than schedule another identity migration.

## 3. Required Invariants

V2 is governed by these invariants:

1. **Equality agreement.** Values equal under the PostgreSQL equality semantics
   used by the maintained query encode identically.
2. **Injectivity modulo equality.** Values that are not equal encode differently.
   The final row identity has no hash-collision failure mode.
3. **Tuple framing.** Field count, order, type, NULL, and value boundaries are
   unambiguous. `(1, 23)` cannot encode like `(12, 3)`.
4. **Determinism.** Encoding is independent of `DateStyle`, `TimeZone`,
   `bytea_output`, locale formatting, process architecture, and database OIDs.
5. **Explicit ordering contract.** Every supported type declares whether
   lexicographic payload order matches its PostgreSQL B-tree comparator. Native
   ordering is a performance property, not a prerequisite for exact identity.
   No locality claim is made for types marked non-order-preserving.
6. **Prefix freedom.** No complete row identity is a byte prefix of another
   complete row identity.
7. **One implementation.** Trigger CDC, WAL CDC, full refresh, differential
   refresh, IMMEDIATE mode, joins, aggregates, and stream-table propagation use
   the same encoder.
8. **Exact matching.** The bounded probe may narrow candidates, but only equality
   of the complete `__pgt_row_id` establishes a match.
9. **Known version.** Persisted V1 and V2 identity state must never be consumed
   together.

Correctness takes precedence over accepting every PostgreSQL type. A type with
no proven encoder is rejected at stream-table creation instead of falling back
to text or a hash.

## 4. Normative Wire Format

The implementation must land a short normative wire-format specification and
golden vectors before it lands operator rewrites. The format must not be frozen
until the type contracts and probe sizing gates in sections 6, 8, and 13 pass.
Once V2 is released, its byte format is immutable. A semantic correction requires
V3 and another rebuild.

Conceptually, an identity is:

```text
VERSION | DOMAIN | FIELD_COUNT | FIELD... | TUPLE_END
```

Each field is:

```text
TYPE_TAG | NULL_TAG
```

or:

```text
TYPE_TAG | VALUE_TAG | PAYLOAD
```

The following rules are normative:

- `VERSION` is part of every identity, not only catalog metadata.
- `DOMAIN` separates scan keys, group keys, joins, set operations, windows,
  keyless rows, and synthetic identities.
- `TYPE_TAG` values are stable and never reused.
- fixed-width payloads have a fixed byte count implied by their type tag;
- `NULL_TAG` sorts before `VALUE_TAG`; all NULLs of one field type encode
   identically;
- variable-width payloads use an unsigned-byte-order-preserving
   escape-and-terminate scheme, not a length prefix;
- nested tuples are framed recursively;
- `TUPLE_END` makes complete identities prefix-free.

For arbitrary bytes, the base escaping is:

```text
00      -> 00 FF
end     -> 00 00
01..FF  -> unchanged
```

This preserves unsigned byte order while making the end of a value explicit.
Fixed identity schemas have the same header and type tags for every row, so
those bytes do not disturb ordering within an index.

The row ID is opaque to users. The format is specified so pg_trickle can test
and preserve it, not to create a user-facing serialization API.

## 5. Identity Domains and Composition

Domains make different identity meanings disjoint without relying on magic
numbers or hoping that a hash does not produce a reserved value.

The initial domain registry should include at least:

| Domain | Input |
|---|---|
| `SCAN_KEY` | source primary/unique-key fields |
| `KEYLESS_ROW` | all logical output fields |
| `GROUP_KEY` | GROUP BY fields |
| `JOIN_KEY` | ordered child identities |
| `SET_KEY` | set-operation identity components |
| `WINDOW_KEY` | partition/order identity components |
| `SYNTHETIC` | a stable internal discriminator |

Operators decide which logical values form an identity. The row-identity module
alone decides how those values are encoded.

Pass-through operators preserve the child identity unchanged. Derived operators
encode child identities as framed `BYTEA` fields. A join therefore has a stable
left-then-right ordering and cannot confuse `(left=A, right=BC)` with
`(left=AB, right=C)`.

Synthetic identities use the `SYNTHETIC` domain and a registered discriminator,
for example `scalar_aggregate_singleton` or `lateral_inner_dummy`. Hard-coded
`BIGINT` sentinel values disappear. SQL NULL is never a valid row identity.

## 6. Type Semantics

Type support is an explicit registry keyed by PostgreSQL type OID and resolved
to a stable V2 type tag. There is no generic `::TEXT`, output-function, or
`typsend` fallback. Every registry entry declares:

```text
equality_canonical:       required
default_nonnull_btree_order_preserving:  true | false
volatility:               immutable | stable
maximum_encoded_size:     fixed | typmod-bounded | unbounded
ddl_invalidations:        explicit list
```

`equality_canonical` means PostgreSQL equality and byte equality agree in both
directions. A type cannot ship without that proof.
`default_nonnull_btree_order_preserving` means that, for non-NULL values using
the default ascending B-tree operator class, the sign of lexicographic payload
comparison agrees with the type's PostgreSQL B-tree comparator. It does not
claim PostgreSQL's default NULL placement, descending-index order, non-default
operator classes, or expression-index order. Only entries marked `true` support
source-index locality claims. The registry stores these properties per concrete
type OID, even where the tables below group types with identical policies.

### 6.1 Initial scalar encoders

| Type family | Canonical ordered payload |
|---|---|
| `bool` | one byte, false before true |
| `int2`, `int4`, `int8` | sign bit flipped, then big-endian |
| `oid` | unsigned big-endian |
| `float4`, `float8` | sortable IEEE transform after canonicalizing signed zero and NaN |
| `numeric` | ordered class, normalized base-10000 exponent, normalized digits; negative magnitudes reversed |
| `text`, `varchar` | escaped database-encoding bytes |
| `bpchar` | trailing-space-normalized bytes, then escaped |
| `bytea` | escaped raw bytes |
| `uuid` | 16 network-order bytes |
| `date` | sign-flipped internal day count |
| `time` | unsigned internal microseconds |
| `timestamp`, `timestamptz` | sign-flipped internal microseconds; `timestamptz` is UTC |
| `timetz` | sign-flipped GMT-equivalent microseconds, then sign-flipped stored zone offset in seconds west of UTC |
| `interval` | sign-flipped `interval_cmp_value()` result as big-endian `int128` |
| `inet`, `cidr` | family, prefix length, and canonical address bytes; `cidr` host bits are zeroed |
| `macaddr`, `macaddr8` | network-order address bytes |
| `bit`, `varbit` | bit length followed by escaped packed bits |
| enum | escaped label bytes, never enum OIDs |
| domain | encoded as its base type |

The float transform must place PostgreSQL NaN after positive infinity and map
all NaN payloads to one quiet-NaN representation: `0x7FC00000` for `float4` and
`0x7FF8000000000000` for `float8`, before the sortable transform. Numeric must
encode `1.0` and `1.00` identically, and its class order is negative infinity,
finite values, positive infinity, then NaN, matching PostgreSQL. Interval uses
PostgreSQL's 128-bit `interval_cmp_value()`, which treats one month as 30 days
for comparison; therefore `1 month` and `30 days` encode identically. The
comparison value must remain 128 bits through the sign transform and big-endian
encoding because legal interval values can overflow `int64`. The physical
`(months, days, microseconds)` layout is not a valid identity encoding.

Enum labels are portable across dump/restore but mutable through `ALTER TYPE
... RENAME VALUE`. The existing ALTER TYPE DDL hook must continue to mark every
directly and transitively dependent stream table for reinitialization before it
can consume changes encoded with the new label. A DDL hook in one backend does
not clear another backend's `fn_extra`, so enum metadata caching also requires a
PostgreSQL shared invalidation callback, or an equivalent shared generation
mechanism observed independently by every backend. Before encoding an enum,
each backend compares its cached generation with the current generation and
discards stale label metadata before it can emit bytes. V2 tests both paths
explicitly, including a two-session rename after the encoder cache is warmed.

The initial registry records these ordering and catalog dependencies:

| Type family | Default non-NULL B-tree order preserving | Volatility | Maximum encoded size | DDL invalidations |
|---|---:|---|---|---|
| `bool`, integers, `oid`, floats, `numeric` | yes | immutable | fixed or typmod-bounded; unconstrained `numeric` is unbounded | none |
| `text`, `varchar`, `bpchar` | only under `C`/`POSIX` | immutable | typmod-bounded or unbounded | collation change |
| `bytea`, `uuid`, date/time types, `macaddr`, `macaddr8` | yes | immutable | fixed or unbounded as defined above | none |
| `interval` | yes, using the full 128-bit comparison value | immutable | fixed | none |
| `inet`, `cidr` | no | immutable | fixed | none |
| `bit`, `varbit` | no | immutable | typmod-bounded or unbounded | none |
| enum | no; PostgreSQL orders by `enumsortorder`, not label bytes | stable | bounded by PostgreSQL's enum-label limit | enum label/order DDL |
| domain | inherited from base type | inherited from base type | inherited from base type | domain or base-type DDL |

The listed `inet`/`cidr` and bit-string payloads are exact identities but do not
match PostgreSQL's native ordering. `inet`/`cidr` compare common network bits,
mask length, and full address in a different sequence. Bit strings compare
packed bits before using length as a tie-breaker. Enum label bytes likewise do
not reflect `enumsortorder`.

### 6.2 Structural encoders

Arrays, ranges, multiranges, `jsonb`, and composite values may be supported only
through explicit structural encoders:

- arrays include dimensions, lower bounds, element count, and framed elements;
- ranges include empty/infinite flags, inclusivity, and canonicalized bounds;
- multiranges encode canonical ordered ranges;
- `jsonb` is traversed structurally so object key order and duplicate-key input
  do not alter identity;
- composites include a stable field count and recurse into supported field
  types.

Nesting depth and total encoded size are bounded by explicit resource limits.
Exceeding a resource limit raises a contextual error; it never switches to a
different identity algorithm.

Structural encoders do not need to ship in the first implementation commit,
but any type without its encoder remains rejected. Adding a new type tag later
does not change existing V2 identities and therefore does not require V3.
Each structural entry also declares the five registry properties above.
Default non-NULL B-tree order preservation is false until comparison against
PostgreSQL's B-tree support function has been demonstrated. Volatility, maximum size, and DDL
invalidations propagate from nested types; composite alteration invalidates the
resolved field metadata and all dependent stream tables.

### 6.3 Collation policy

Text identity uses database-encoding bytes. For deterministic collations this
agrees with PostgreSQL equality, although byte order may differ from a locale's
sort order. The locality guarantee for text is therefore exact for `C`/`POSIX`
collations. Other deterministic collations preserve equality agreement but are
marked non-order-preserving and receive no source-index locality claim.

Non-deterministic collations are rejected in identity fields. Distinct byte
strings can compare equal under those collations, and ICU sort keys are not a
stable persisted identity across library upgrades. Rejection is preferable to
silently changing row identity after an operating-system update.

### 6.4 Validation

`CREATE STREAM TABLE` and `ALTER ... SET QUERY` validate every possible identity
field before creating storage or capture state. Errors name the expression,
resolved type, and unsupported property. Upgrade preflight reports every existing
stream table that cannot be represented by V2 before migration changes anything.

## 7. Typed SQL Entry Point

The encoder must receive PostgreSQL datums and their actual types. The existing
`TEXT[]` hash functions cannot be reused.

Generated SQL should call a new function shaped like:

```sql
pgtrickle.encode_row_id_v2(domain, ROW(expr1, expr2, ...)) RETURNS bytea
```

The C/Rust implementation obtains the record `TupleDesc`, deforms the tuple
directly, and dispatches through the type registry. It caches the resolved field
encoders in `fn_extra`, keyed by the anonymous record typmod. Every invocation
validates the current typmod and rebuilds the cache when it changes, following
PostgreSQL's record-function cache pattern. Catalog lookup, function lookup, and
type classification must not occur per row when the typmod is unchanged.

The implementation should write directly into one growing scratch buffer and
copy only the completed varlena result into PostgreSQL-owned memory. It must not
construct `Vec<String>`, call display output functions, or allocate once per
field.

Because the initial registry supports label-based enum encoding,
`encode_row_id_v2` is `STABLE` and `PARALLEL SAFE`. Enum output is catalog
dependent, so declaring the encoder `IMMUTABLE` would be false even with DDL
hooks and forced reinitialization. The generated probe expression calls only
`row_probe_v1(bytea)`, which remains genuinely `IMMUTABLE` and
`PARALLEL SAFE`; the row-ID encoder itself does not appear in that generated
expression. Existing V1 hash functions retain their old behavior during
migration and are removed only after no V1 trigger or stored expression can
reference them.

## 8. Bounded B-Tree Probe

A full canonical identity can be wider than PostgreSQL permits in one B-tree
index tuple, especially for keyless rows and composite text keys. Indexing the
full `BYTEA` directly would turn valid data growth into runtime index failures.
V2 avoids that ceiling without giving correctness back to a hash.

Probe version 1 includes a fixed prefix length `P`, but this proposal does not
freeze its value. Benchmarks must compare at least `P = 32`, `64`, `128`, and
`256` bytes before the probe format is frozen. For the selected value:

```text
if row_id.length <= P:
    probe = row_id
else:
    probe = row_id[0..P] || xxh3_128(row_id)
```

Properties:

- the probe is at most `P + 16` bytes;
- ordinary integer, UUID, and short composite identities are indexed in full;
- because complete identities are prefix-free, non-overflow probe ordering is
   exactly full-identity byte ordering, which matches native PostgreSQL ordering
   only for registry entries that guarantee it;
- overflow probes sort first by their first `P` canonical bytes; identities that
   share that complete prefix sort by digest, not by their remaining bytes;
- the digest normally distributes identities with a large common prefix across
   distinct probe keys; this is a performance property, not a correctness claim;
- digest collisions do not affect correctness because MERGE also compares the
  complete row ID.

`__pgt_row_probe` is internal implementation state. It is a stored generated
column computed by one `IMMUTABLE` helper, so callers cannot create a probe that
does not match its full identity. It is not part of the public contract unless a
sink deliberately publishes internal columns.

The implementation must validate the selected `P` against the running cluster's
actual `BTMaxItemSize`, including varlena and index-tuple overhead. Extension
installation or upgrade must reject a probe version whose worst-case index datum
cannot fit the cluster's configured B-tree page size. A prefix length that is
safe for the default `BLCKSZ` is not assumed safe for every supported build.

The identity index should not INCLUDE the full row ID or arbitrary user columns;
doing so would reintroduce the B-tree tuple-width failure. Covering indexes, if
useful, are separate optional indexes and must never be required for correctness.

Static boundedness is computed from identity field types and typmods: sum the
fixed header/framing cost and each field's worst-case escaped size in the database
encoding. Fixed-width types have known bounds; bounded `varchar(n)`, `bpchar(n)`,
`bit(n)`, `varbit(n)`, and `numeric(p,s)` use typmod-derived bounds; unconstrained
`numeric`, `text`, `bytea`, JSONB, array, range, unconstrained composite, or any
other unbounded field makes the whole schema unbounded. Conservative
classification is required: uncertainty means unbounded.

For identity schemas whose maximum encoded length is statically at most `P`, a
non-keyless stream table may keep a UNIQUE probe index because `probe == row_id`.
All keyless or unbounded schemas use a non-unique probe index and exact two-column
matching. The implementation must not place a UNIQUE constraint on a digest.

Probe format and identity format are versioned separately. Changing prefix length
or digest algorithm requires recomputing the probe column and index, but not
rebuilding logical row identities from source data.

## 9. Storage and DML Changes

Every storage, delta, temporary, CDC, and cache relation that currently carries
a `BIGINT` row identity must move to the V2 pair where indexed matching occurs.
Relations that only transport identity may carry the full row ID and derive the
probe at the consumer.

All UPDATE, DELETE, and MERGE predicates use probe equality plus full row-ID
equality. For schemas proven statically bounded and unique, the existing upsert
shape becomes `ON CONFLICT (__pgt_row_probe)`; in that case `probe == row_id`, so
the unique probe is also the exact identity. Unbounded identities use MERGE or
equivalent exact DML against the non-unique probe index. `ON CONFLICT
(__pgt_row_id)` is invalid because the full `BYTEA` is deliberately not indexed.

Keyless semantics do not change. Identical logical rows intentionally have the
same full identity and may occur more than once, so their probe index remains
non-unique and counted-delete logic remains authoritative.

For unique but unbounded identity schemas, the database index is intentionally
non-unique because no bounded B-tree key can prove uniqueness of arbitrary-length
values. Scheduled and manual refresh retain the existing transaction advisory
lock keyed by `pgt_id` and catalog-row serialization. Whole-table operations use
that lock as a stream-table gate in exclusive mode.

Per-identity locking supplements the existing query-shape lock analysis; it does
not replace it. The current `Exclusive` mode remains required for aggregates,
joins, `DISTINCT`, and other cross-row query shapes. `RowExclusive` remains the
concurrent mode for simple scan/filter/project chains. Per-identity locking
replaces only the additional table-wide serialization for an unbounded unique
identity in an otherwise concurrently maintainable simple query.

The lock hierarchy is:

1. Per-identity maintenance acquires the stream-table gate in shared mode.
2. The existing statement-level `AFTER` trigger uses transition tables to
   collect old and new identities, deduplicates them, and acquires all required
   per-identity locks in sorted order.
3. It performs probe-plus-full-ID matching and exact uniqueness checks before
   applying deltas.
4. Migration, reinitialization, manual/full refresh, mode changes, and other
   whole-table operations acquire the same stream-table gate in exclusive mode.

The statement-level `BEFORE` trigger cannot be the per-identity acquisition
point because it does not have the complete affected identity set. Sorted
acquisition prevents cycles within one collected lock set. Several statements
in one transaction can still deadlock with another transaction; PostgreSQL may
abort one participant with a retryable error, and that is normal behavior.

The per-identity key contract is fixed by `ROW_LOCK_VERSION = 1`. The input to
the key hash is the following exact byte sequence:

```text
ROW_LOCK_VERSION (u8 = 1) | ROW_LOCK_KIND (u8 = 1) |
pgt_id (u64, big-endian) | row_id_length (u64, big-endian) | row_id bytes
```

The hash is XXH3-64 with seed `0`. Its lower 63 bits are combined with the
reserved row-lock namespace bit (`0x8000000000000000`), then interpreted as a
two's-complement signed `int64` for `pg_advisory_xact_lock`. The stream-table
gate key is the non-negative `pgt_id`, so gate and per-identity namespaces are
disjoint. `pgt_id` must remain in the non-negative signed `int64` range. The
version, algorithm, seed, framing, byte order, and signed interpretation are
part of the compatibility contract. A different algorithm in a concurrent
backend is unsafe even though hash collisions only serialize unrelated rows.
The stored `row_lock_version` guard rejects a binary or catalog state with an
unknown version before it can maintain a V2 graph.

At `READ COMMITTED`, the exact check after advisory-lock acquisition runs in a
fresh command snapshot and can see a row committed while the backend waited for
the lock. The unbounded unique `IMMEDIATE` path is supported only at `READ
COMMITTED`. Under `REPEATABLE READ` or `SERIALIZABLE`, it raises
`serialization_failure` before capture or storage mutation; the caller must
retry in a new `READ COMMITTED` transaction. It must not use the old snapshot or
silently fall back to table-wide serialization. Other query shapes and bounded
unique paths retain their existing isolation and lock semantics.

An advisory-key collision may serialize unrelated identities but cannot affect
row identity or uniqueness. The lock hash is coordination metadata, never proof
of equality. Failing to acquire or hold every required lock aborts the statement.
Until this path has correctness tests, deadlock tests, and a concurrency
benchmark, `IMMEDIATE` mode must reject unique unbounded identity schemas during
planning. `RowExclusive` remains valid when a database UNIQUE probe index proves
identity uniqueness.

`RowIdStrategy` and `RowIdSchema` continue to decide which logical fields form an
identity. They do not select a storage encoding. V2 has one encoder.

## 10. Shared CDC State

A source change buffer may feed several stream tables. V2 is therefore
extension-wide, not a per-stream-table option. Every consumer of a source uses
the same source identity bytes and probe rules.

Trigger CDC and WAL CDC both emit full V2 identities. A consumer derives or
reads the V2 probe before indexed matching. No hot-path lookup of downstream
stream-table preferences is required because no such preference exists.

The V2 extension-upgrade DDL adds non-null `row_identity_version`,
`row_probe_version`, and `row_lock_version` columns to
`pgtrickle.pgt_change_buffers` and the corresponding version state for
stream-table storage. Buffer registry metadata records all three versions.
Runtime readers refuse mismatches before consuming a row, and the lock-version
guard prevents binaries with different advisory-key derivations from operating
concurrently.
The guard is enforced in code, not only by extension upgrade SQL, because a new
shared library can be installed before `ALTER EXTENSION ... UPDATE` runs.

## 11. Compatibility Contract

`__pgt_row_id` remains visible but changes type from `BIGINT` to `BYTEA`. Its
meaning is opaque implementation identity:

> Applications may compare or display `__pgt_row_id`, but must not parse it,
> generate it, or use it as a durable business identifier.

The V2 bytes are stable for the life of V2, but pg_trickle reserves the right to
introduce V3 through another explicit rebuild before 1.0 if correctness requires
it. Diagnostics should display the value in hexadecimal.

This is a breaking change for logical-replication subscribers, DuckLake sinks,
outbox consumers, dbt models, and user SQL that assumes `BIGINT`. That break is
accepted. Every stream table must be replaced anyway, so preserving the old type
would only retain its collision and locality limitations.

External consumers must update their schema and resnapshot the replaced stream
tables. There is no automatic cast from old numeric IDs to V2 bytes because the
numeric value does not contain the original logical identity.

After rebuild, bounded unique stream tables may use the UNIQUE probe index as
replica identity. Unbounded or keyless stream tables use `REPLICA IDENTITY FULL`.
Publication column lists should exclude `__pgt_row_probe` unless a sink has an
explicit operational reason to transport this non-contractual index helper.

## 12. Migration and Cutover

V1 and V2 state cannot coexist in one refresh graph. `ALTER EXTENSION UPDATE`
installs V2-capable code, catalog guards, and the migration command, but does not
rebuild a graph. Ordinary V1 capture and refresh continue in the `V1_ACTIVE`
state after the extension update. Migration is a separate, dry-runnable,
resumable administrative operation, not `ALTER COLUMN ... TYPE` and not a
rolling per-table conversion. The V2-capable binary understands V1 state for
preflight and for the controlled transition, but normal refresh never mixes
versions.

Migration uses this state model:

| State | Capture and refresh contract |
|---|---|
| `V1_ACTIVE` | V1 capture and refresh continue normally. V2 preflight and dry-run are available, but no V2 state is consumed. |
| `MIGRATING_TO_V2` | V1 refresh is disabled. V2 capture is armed at the recorded frontier, and rebuild plus catch-up are resumable. No mixed V1/V2 graph is refreshable. |
| `V2_ACTIVE` | Only V2 capture and refresh are accepted. |

The transition into `MIGRATING_TO_V2` is committed by the cutover transaction,
which records the frontier while replacing V1 capture with V2 capture. An
extension update alone therefore does not begin the operational outage.

### 12.1 Preflight

Before changing state, migration must:

1. validate all identity types and collations;
2. enumerate every source, shared buffer, stream table, and downstream edge;
3. verify sufficient disk space and required privileges;
4. report publications and external dependencies that require resnapshotting;
5. remove or replace any REPLICA IDENTITY configuration that depends on the V1
   `BIGINT` before the V2 rebuild snapshot is established;
6. stop before making changes if any stream table cannot be encoded by V2.

Dry-run executes the complete preflight and reports the planned graph, storage
replacement, estimated space, unsupported fields, and external resnapshot work
without changing catalog or capture state.

### 12.2 Cutover

The migration command should use the existing snapshot/frontier machinery:

1. pause scheduling, disable V1 refresh, and mark the graph
   `MIGRATING_TO_V2`;
2. acquire source locks in deterministic OID order for the short cutover
   transaction;
3. establish snapshot/frontier position `P`;
4. replace V1 capture definitions and buffers with empty V2 capture state in the
   same transaction;
5. release source locks; writes after `P` are captured as V2;
6. discard and recreate all stream-table storage in dependency order from the
   authoritative snapshot;
7. monitor V2 buffer growth during rebuild and pause/reject further rebuild work
   before a configurable disk watermark is crossed;
8. apply V2 changes captured after `P` in durable, restartable batches exactly
   once, checking available space before each batch;
9. mark the graph V2 and resume scheduling.

The invariant is:

> Every committed source change is represented either in the rebuild snapshot
> or in V2 CDC state after the snapshot frontier, exactly once.

Capture must never be unarmed between V1 and V2. V1 buffered rows are discarded
only after the same transaction establishes a snapshot known to include them.

### 12.3 Failure and rollback

Migration phase is durable catalog state. After cutover, a crash leaves V2
capture armed and the graph non-refreshable until rebuild resumes. Re-running the
migration continues from the recorded phase; it does not create another frontier.

Rollback is supported only before V2 cutover. After cutover, returning to V1
requires restoring a backup or rebuilding all stream tables and CDC state with a
V1 binary. Older binaries must reject V2 catalog state explicitly.

## 13. Performance Requirements

The encoder and probe run on a hot path. Correctness does not excuse avoidable
allocation or lookup overhead.

Implementation requirements:

- resolve `TupleDesc` and encoder dispatch once per call site;
- no per-field `String`, SQL text cast, catalog query, or fmgr lookup;
- one reusable scratch buffer per call site;
- one pass over input values to produce the full identity;
- compute overflow digest from the completed bytes only when length exceeds
  `P`;
- keep the common fixed-width path branch-light;
- preserve exact wire bytes across optimization changes.

Benchmarks must cover single integer, UUID, composite integers, short and long
text composites, arrays/JSONB when supported, and keyless wide rows. They must
measure encoding throughput, allocations, CDC overhead, index size, buffer hits,
WAL volume, encoded-ID storage, CDC-buffer growth, cached MERGE latency, and
MERGE latency with indexes larger than `shared_buffers`. Probe benchmarks must
compare 32, 64, 128, and 256-byte prefixes across short keys, ordered wide keys,
random wide keys, and long identities with common prefixes.

The comparison set is V1 hash, V2 full identity, and V2 overflow probe. A
material regression on cached common-key workloads must be investigated before
merge. The expected V2 benefit is removal of text conversion and substantially
better out-of-cache index locality where registry metadata supports that claim,
not merely a different microbenchmark score. V2 may regress cached integer keys,
randomly distributed deltas, wide keyless rows, and overflow identities that
require TOAST access; the release decision must measure rather than assume those
tradeoffs.

The release gates are index size, WAL volume, encoded-ID storage size,
CDC-buffer growth, cached integer-key latency, out-of-cache ordered-key latency,
overflow-key latency, and concurrent `IMMEDIATE` throughput for unconstrained
text identities. The probe prefix is frozen only after these results identify
the best acceptable tradeoff.

## 14. Testing

### 14.1 Encoder tests

- golden byte vectors for every supported type, domain, NULL state, and boundary;
- identical vectors on little- and big-endian targets where CI permits;
- setting-independence tests for `DateStyle`, `TimeZone`, `bytea_output`, and
  locale-sensitive output;
- equality-agreement property tests for every registry entry that prove
   `a IS NOT DISTINCT FROM b` if and only if `encode(a) = encode(b)`, including
   numeric scale, signed zero, NaN payloads, `bpchar` padding, timestamp zones,
   extreme and equivalent intervals, and JSONB key order;
- for entries marked `default_nonnull_btree_order_preserving`, property tests
   comparing the sign of `bytea` comparison with the sign of PostgreSQL's B-tree
   comparator for non-NULL values under the default ascending operator class;
- prefix-freedom and tuple-framing property tests;
- explicit rejection tests for unsupported types and non-deterministic collations.

### 14.2 Probe tests

- inline probes equal the full identity;
- overflow probes never exceed `P + 16` bytes and fit the running cluster's
   `BTMaxItemSize` with all tuple overhead included;
- differences in the first `P` bytes retain byte order;
- long common-prefix identities produce narrow candidate lookups;
- a test-only probe constructor can inject equal digests for distinct full IDs,
  proving that forced probe collisions still match only the exact full row ID;
- no UNIQUE index is created where overflow is possible.

### 14.3 Cross-path tests

The same logical identity must produce byte-identical output through trigger CDC,
WAL CDC, full refresh, differential refresh, IMMEDIATE mode, joins, aggregates,
set operations, windows, and downstream stream tables.

Tests must include primary-key updates, NULL group keys, keyless duplicates,
deep join composition, synthetic identities, and wide TOASTed values.

Concurrency tests must run multiple sessions against an unbounded unique
identity in each refresh mode and prove that exactly one logical row survives.
They must cover old/new identities on primary-key updates, deterministically
ordered multi-row lock acquisition, forced advisory-key collisions, and the
stream-table gate shared/exclusive hierarchy. At `READ COMMITTED`, a waiter
must see the committed row after obtaining the advisory lock. At `REPEATABLE
READ` and `SERIALIZABLE`, the unbounded unique `IMMEDIATE` path must raise
`serialization_failure` before mutation and require a retry in a new `READ
COMMITTED` transaction. A throughput benchmark must verify that unconstrained
text identities do not serialize all writers. Enum tests must use two sessions:
session A warms the encoder cache, session B renames an in-use label and
commits, and session A calls the cached expression again. Session A must either
resolve the new label or reject the affected stream state; it must never emit
stale bytes. The test must also prove that the existing DDL hook marks the full
downstream DAG for reinitialization before further refresh.

### 14.4 Migration tests

- non-empty V1 buffers at cutover;
- concurrent source writes throughout migration;
- several stream tables sharing one source buffer;
- multi-level DAG rebuild order;
- crash after each durable migration phase and successful resume;
- V1 binary/V2 catalog and V2 binary/V1 catalog rejection;
- unsupported-type preflight with no partial changes;
- logical-replication resnapshot procedure.

The final oracle is a from-scratch V2 rebuild: migrated output must be exactly
equal as a multiset after all captured changes are applied.

## 15. Implementation Plan

**Stage 1: Resolve and freeze the contracts.** Write the normative byte
specification, domain and type-tag registries, golden vectors, and type
validation. Prove the 128-bit interval encoding and per-type equality/order
contracts, specify volatility and DDL invalidation, validate per-identity lock
semantics, benchmark candidate probe prefixes, and check `BTMaxItemSize` before
freezing identity V2 and probe V1.

**Stage 2: Implement the encoder.** Add the typed record entry point, cached
dispatch, scalar encoders, structural encoders required by the existing test
surface, synthetic domains, and probe helper.

**Stage 3: Change storage and matching.** Create `BYTEA` row-ID/probe columns,
bounded probe indexes, and exact two-column DML. Remove assumptions that row IDs
are numeric or that every non-keyless identity has a unique full-width index.

**Stage 4: Centralize producers.** Move trigger CDC, WAL CDC, scan, refresh,
join, aggregate, set, window, and synthetic identities to the shared encoder.
Delete `::TEXT` row-ID construction and hard-coded numeric sentinels.

**Stage 5: Add version guards and migration.** Persist identity/probe versions,
add runtime mismatch rejection, install a dry-run mode and resumable
administrative cutover, and rebuild every stream table and shared buffer outside
`ALTER EXTENSION UPDATE`.

**Stage 6: Prove it.** Run the full correctness matrix, migration fault tests,
and performance benchmarks. Update SQL reference, architecture, upgrade,
replication, DuckLake, outbox, dbt, and release documentation.

Stages may be separate pull requests, but V2 must not be user-selectable until
all stages are complete. There is no supported partially migrated mode.

## 16. Alternatives Rejected

### Keep `BIGINT` and hash a better encoding

This fixes ambiguous input but retains collisions and random index locality. It
also requires the same rebuild of persisted state. It spends the migration cost
without reaching the durable design.

### Store a 128-bit or 256-bit hash in `BYTEA`

Using a digest alone destroys source-key ordering and still treats digest equality
as row equality. Wider probability is not needed when exact canonical bytes can
be stored. The overflow probe also contains a digest, but only as a bounded index
accelerator; it is never authoritative and exact matching is always preserved.

### Index the complete `BYTEA` directly

This is correct for short keys but fails for sufficiently wide B-tree entries.
The bounded probe retains prefix locality where the type contract supports it
and guarantees indexability without making a digest authoritative.

### Add a direct-integer strategy

The V2 integer payload is already fixed-width and order-preserving. A second
strategy would complicate shared buffers, derived identities, metadata, and
migration for little practical gain.

### Make BYTEA opt-in per stream table

Shared source buffers and downstream DAGs need one identity representation. Mixed
strategies expand the state machine and preserve V1 indefinitely. V2 is an
extension-wide format change.

### Fall back to text or hash for unsupported types

A silent fallback creates two identity contracts and reintroduces settings,
ambiguity, or collisions exactly where correctness is hardest to observe. Explicit
rejection is safer and allows support to grow one tested type tag at a time.

## 17. Acceptance Criteria

The proposal is complete when implementation can demonstrate all of the
following:

- no SQL-facing row-ID path casts identity fields to text;
- no final row identity is a hash or numeric sentinel;
- supported unequal logical identities have different full bytes;
- all indexed matching verifies the full identity;
- probe indexes remain bounded for arbitrary valid input size;
- the selected probe prefix fits the running cluster's `BTMaxItemSize` and is
   justified by the required benchmark matrix;
- each registry entry proves equality agreement and declares default non-NULL
   B-tree ordering, volatility, size, and DDL-invalidation metadata;
- interval identities encode the complete 128-bit PostgreSQL comparison value;
- enum label caching is invalidated by enum DDL and the encoder is not declared
   `IMMUTABLE` while label-based enum support is enabled;
- enum invalidation is observed across backends, not only in the backend that ran
   the DDL;
- unsupported types and collations fail before state is created;
- V1/V2 mismatch cannot consume or mutate persisted state;
- `V1_ACTIVE`, `MIGRATING_TO_V2`, and `V2_ACTIVE` enforce the stated capture and
   refresh contract;
- migration loses or duplicates no committed change under concurrent writes;
- interrupted migration resumes safely;
- external compatibility breaks and resnapshot steps are documented;
- unique unbounded `IMMEDIATE` identities use the versioned, deterministic
   per-identity lock protocol without table-wide writer serialization, while
   preserving query-shape `Exclusive` locks;
- the unbounded unique `IMMEDIATE` path rejects `REPEATABLE READ` and
   `SERIALIZABLE` before mutation and documents the `READ COMMITTED` contract;
- out-of-cache composite-key workloads show the intended locality benefit;
- cached common-key workloads have no unexplained material regression;
- all performance release gates in section 13 pass.

## 18. Recommendation

Adopt exact canonical `BYTEA` row identities plus a bounded, non-authoritative
probe as pg_trickle's V2 architecture before 1.0. Do not freeze or implement the
wire specification until interval encoding, enum volatility, per-type ordering,
unbounded-identity concurrency, and probe sizing meet the gates above.

This is intentionally a breaking, extension-wide rebuild. That cost buys a row
identity the project can keep: exact rather than probabilistic, typed rather than
formatted, capable of preserving native order where proven, bounded at the
B-tree boundary, and shared consistently across CDC and DVM.

The full identity is the source of truth. The bounded probe is only an index.
That separation avoids both failure modes that otherwise force another redesign:
unbounded `BYTEA` index entries and hashes treated as proof of equality.