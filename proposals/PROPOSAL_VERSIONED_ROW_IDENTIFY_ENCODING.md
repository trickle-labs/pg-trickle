# Proposal: Ordered, Collision-Free Row Identity V2

**Status:** Proposed
**Target:** Pre-1.0 / v0.83.0 DVM semantic fidelity work
**Decision:** Replace hashed `BIGINT` row identities with canonical `BYTEA`
**Migration:** Rebuild every existing stream table; mixed V1/V2 operation is unsupported

## 1. Executive Decision

pg_trickle should replace `__pgt_row_id BIGINT` with an exact, versioned,
memcomparable `BYTEA` encoding of the logical identity fields.

V2 should not hash the canonical identity into 64 bits. The canonical bytes are
the identity. This removes delimiter ambiguity, session-dependent `::TEXT`
formatting, and hash collisions in one change. It also gives B-tree indexes the
same stable ordering as the encoded key instead of deliberately randomizing it.

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
canonical memcomparable encoding  --->  __pgt_row_id
        |
        v
bounded ordered probe             --->  __pgt_row_probe B-tree
```

## 2. Why Replace the Hash

The current row-ID path has four structural problems.

First, composite fields are converted to text and separated before hashing.
Text output can depend on PostgreSQL settings, and a delimiter is not a rigorous
field framing protocol.

Second, the final identity is only 64 bits. Different canonical inputs can hash
to the same `BIGINT`. Because MERGE treats the hash as proof of identity, a
collision can silently overwrite or delete the wrong logical row.

Third, a hash destroys source-key ordering. MERGE probes become random B-tree
accesses once the storage table is larger than `shared_buffers`, even when rows
arrive in primary-key order.

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
5. **Ordering.** For a fixed identity schema and domain, lexicographic byte order
   follows the V2 comparator for its fields. Types with binary rather than
   locale ordering are identified explicitly in section 6.
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
golden vectors before it lands operator rewrites. Once V2 is released, its byte
format is immutable. A semantic correction requires V3 and another rebuild.

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
- variable-width payloads use an order-preserving escape-and-terminate scheme,
  not a length prefix;
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
`typsend` fallback.

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
| `interval` | sign-flipped `interval_cmp_value()` result as big-endian `int64` |
| `inet`, `cidr` | family, prefix length, and canonical address bytes; `cidr` host bits are zeroed |
| `macaddr`, `macaddr8` | network-order address bytes |
| `bit`, `varbit` | bit length followed by escaped packed bits |
| enum | escaped label bytes, never enum OIDs |
| domain | encoded as its base type |

The float transform must place PostgreSQL NaN after positive infinity and map
all NaN payloads to one quiet-NaN representation: `0x7FC00000` for `float4` and
`0x7FF8000000000000` for `float8`, before the sortable transform. Numeric must
encode `1.0` and `1.00` identically, and its class order is negative infinity,
finite values, positive infinity, then NaN, matching PostgreSQL. Interval uses PostgreSQL's
`interval_cmp_value()`, which treats one month as 30 days for comparison;
therefore `1 month` and `30 days` encode identically. The physical
`(months, days, microseconds)` layout is not a valid identity encoding.

Enum labels are portable across dump/restore but mutable through `ALTER TYPE
... RENAME VALUE`. The existing ALTER TYPE DDL hook must continue to mark every
directly and transitively dependent stream table for reinitialization before it
can consume changes encoded with the new label. V2 tests this path explicitly.

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

### 6.3 Collation policy

Text identity uses database-encoding bytes. For deterministic collations this
agrees with PostgreSQL equality, although byte order may differ from a locale's
sort order. The locality guarantee for text is therefore exact for `C`/`POSIX`
collations and deterministic but not necessarily source-index-correlated for
other deterministic collations.

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

The function is `IMMUTABLE` and `PARALLEL SAFE`; the type policy in section 6 is
part of making those declarations truthful. Existing V1 hash functions retain
their old behavior during migration and are removed only after no V1 trigger or
stored expression can reference them.

## 8. Bounded B-Tree Probe

A full canonical identity can be wider than PostgreSQL permits in one B-tree
index tuple, especially for keyless rows and composite text keys. Indexing the
full `BYTEA` directly would turn valid data growth into runtime index failures.
V2 avoids that ceiling without giving correctness back to a hash.

Let `P = 256` bytes for probe version 1:

```text
if row_id.length <= P:
    probe = row_id
else:
    probe = row_id[0..P] || xxh3_128(row_id)
```

Properties:

- the probe is at most 272 bytes;
- ordinary integer, UUID, and short composite identities are indexed in full;
- because complete identities are prefix-free, non-overflow probe ordering is
   exactly full-identity ordering;
- overflow probes sort first by their first 256 canonical bytes; identities that
   share that complete prefix sort by digest, not by their remaining bytes;
- the digest normally distributes identities with a large common prefix across
   distinct probe keys; this is a performance property, not a correctness claim;
- digest collisions do not affect correctness because MERGE also compares the
  complete row ID.

`__pgt_row_probe` is internal implementation state. It is a stored generated
column computed by one `IMMUTABLE` helper, so callers cannot create a probe that
does not match its full identity. It is not part of the public contract unless a
sink deliberately publishes internal columns.

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

For unique but unbounded identity schemas, exact uniqueness is maintained by
the existing single-writer refresh serialization plus exact MERGE matching; the
database index is intentionally non-unique because no bounded B-tree key can
prove uniqueness of arbitrary-length values. V2 must preserve that writer
serialization across scheduled, manual, and IMMEDIATE refresh paths.

Concretely, scheduled/manual refresh keeps the existing transaction advisory
lock keyed by `pgt_id` and catalog-row serialization. At plan time, any IMMEDIATE
stream table with a unique but unbounded identity is forced to
`IvmLockMode::Exclusive`, whose BEFORE trigger takes the blocking
`pg_advisory_xact_lock` on the stream-table OID. The lighter concurrent
`RowExclusive` mode is permitted only when a database UNIQUE probe index proves
identity uniqueness. Failing to acquire or hold the required lock aborts the
refresh; it must never continue without database-enforced uniqueness.

`RowIdStrategy` and `RowIdSchema` continue to decide which logical fields form an
identity. They do not select a storage encoding. V2 has one encoder.

## 10. Shared CDC State

A source change buffer may feed several stream tables. V2 is therefore
extension-wide, not a per-stream-table option. Every consumer of a source uses
the same source identity bytes and probe rules.

Trigger CDC and WAL CDC both emit full V2 identities. A consumer derives or
reads the V2 probe before indexed matching. No hot-path lookup of downstream
stream-table preferences is required because no such preference exists.

The V2 extension-upgrade DDL adds non-null `row_identity_version` and
`row_probe_version` columns to `pgtrickle.pgt_change_buffers` and the corresponding
version state for stream-table storage. Buffer registry metadata records both
versions. Runtime readers refuse mismatches before consuming a row.
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

V1 and V2 state cannot coexist in one refresh graph. Migration is an explicit
rebuild, not `ALTER COLUMN ... TYPE`, and not a rolling per-table conversion.

### 12.1 Preflight

Before changing state, migration must:

1. validate all identity types and collations;
2. enumerate every source, shared buffer, stream table, and downstream edge;
3. verify sufficient disk space and required privileges;
4. report publications and external dependencies that require resnapshotting;
5. remove or replace any REPLICA IDENTITY configuration that depends on the V1
   `BIGINT` before the V2 rebuild snapshot is established;
6. stop before making changes if any stream table cannot be encoded by V2.

### 12.2 Cutover

The migration command should use the existing snapshot/frontier machinery:

1. pause scheduling and mark the graph `MIGRATING_TO_V2`;
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
WAL volume, cached MERGE latency, and MERGE latency with indexes larger than
`shared_buffers`.

The comparison set is V1 hash, V2 full identity, and V2 overflow probe. A
material regression on cached common-key workloads must be investigated before
merge. The expected V2 benefit is removal of text conversion and substantially
better out-of-cache index locality, not merely a different microbenchmark score.

## 14. Testing

### 14.1 Encoder tests

- golden byte vectors for every supported type, domain, NULL state, and boundary;
- identical vectors on little- and big-endian targets where CI permits;
- setting-independence tests for `DateStyle`, `TimeZone`, `bytea_output`, and
  locale-sensitive output;
- equality-agreement tests such as numeric scale, signed zero, NaN payloads,
  `bpchar` padding, timestamp zones, interval equivalents, and JSONB key order;
- ordering property tests comparing byte order with the documented V2 comparator;
- prefix-freedom and tuple-framing property tests;
- explicit rejection tests for unsupported types and non-deterministic collations.

### 14.2 Probe tests

- inline probes equal the full identity;
- overflow probes never exceed 272 bytes;
- differences in the first 256 bytes retain order;
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

Concurrency tests must run two sessions against an unbounded unique identity in
each refresh mode and prove that exactly one logical row survives. Enum tests
must rename an in-use label and prove that the existing DDL hook marks the full
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

**Stage 1: Freeze the format.** Write the normative byte specification, domain
and type-tag registries, golden vectors, type validation, and probe specification.

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
add runtime mismatch rejection, implement preflight and resumable cutover, and
rebuild every stream table and shared buffer.

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
The bounded probe retains normal-key ordering and guarantees indexability without
making a digest authoritative.

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
- unsupported types and collations fail before state is created;
- V1/V2 mismatch cannot consume or mutate persisted state;
- migration loses or duplicates no committed change under concurrent writes;
- interrupted migration resumes safely;
- external compatibility breaks and resnapshot steps are documented;
- out-of-cache composite-key workloads show the intended locality benefit;
- cached common-key workloads have no unexplained material regression.

## 18. Recommendation

Adopt ordered, canonical `BYTEA` row identities as pg_trickle's V2 identity
format before 1.0.

This is intentionally a breaking, extension-wide rebuild. That cost buys a row
identity the project can keep: exact rather than probabilistic, typed rather than
formatted, ordered rather than randomized, bounded at the B-tree boundary, and
shared consistently across CDC and DVM.

The full identity is the source of truth. The bounded probe is only an index.
That separation avoids both failure modes that otherwise force another redesign:
unbounded `BYTEA` index entries and hashes treated as proof of equality.