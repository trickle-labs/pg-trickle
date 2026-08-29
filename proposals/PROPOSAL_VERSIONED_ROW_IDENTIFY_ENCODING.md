# Proposal: Exact, Versioned Row Identity V2

**Status:** Proposed
**Target:** Pre-1.0 / v0.83.0 DVM semantic fidelity work
**Decision:** Replace hashed `BIGINT` row identities with canonical `BYTEA`
**Upgrade:** Recreate every existing stream table; in-place V1/V2 migration is unsupported

## 1. Executive Decision

pg_trickle should replace `__pgt_row_id BIGINT` with an exact, versioned,
canonical `BYTEA` encoding of the logical identity fields.

V2 should not hash the canonical identity into 64 bits. The canonical bytes are
the identity. This removes session-dependent `::TEXT` formatting and hash
collisions. For types whose registry entries explicitly guarantee native-order
preservation, it also lets B-tree probes retain source-key locality instead of
deliberately randomizing it.

The heap storage contract becomes:

```text
__pgt_row_id  BYTEA NOT NULL  -- complete canonical identity
```

For an identity whose maximum encoded length is statically proven to fit a
B-tree tuple, pg_trickle indexes the complete identity directly:

```sql
CREATE [UNIQUE] INDEX ... USING btree (__pgt_row_id);
```

Only an unbounded identity uses a bounded expression index:

```sql
CREATE INDEX ... USING btree
   (pgtrickle.row_probe_v1(__pgt_row_id));
```

Matching through that expression index always rechecks the complete identity:

```sql
pgtrickle.row_probe_v1(st.__pgt_row_id) =
   pgtrickle.row_probe_v1(delta.__pgt_row_id)
AND st.__pgt_row_id = delta.__pgt_row_id
```

The probe is an index accelerator, not an identity. For ordinary identities it
is exactly the full row ID. For unusually wide identities it is an ordered
prefix plus a 128-bit digest. The full row ID comparison remains authoritative,
so a probe collision can only add a candidate row to an index scan; it cannot
merge, overwrite, or delete the wrong row. The probe is not stored in the heap
or exposed as a table column.

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
statically bounded? -- yes ------>  __pgt_row_id B-tree
   |
   no
   v
bounded lookup probe              --->  expression B-tree
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

The normative byte assignments, transforms, limits, support matrix, and
independent vectors are defined in [Row identity V2 wire format](../docs/ROW_IDENTITY_V2.md).
No encoder, stored expression, or operator rewrite may merge before those
vectors pass on every supported PostgreSQL major and CPU endianness.

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
| `SCAN_KEY` | fields from an eligible source identity index |
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

### 5.1 Source-key eligibility

"Primary or unique key" is not sufficient by itself. A `SCAN_KEY` may use only
an index that covers every source row and whose uniqueness semantics agree with
the encoder. The initial V2 release accepts:

- a primary key; or
- a non-partial, non-expression, immediate unique index whose key columns are
   all catalog-marked `NOT NULL`; or
- the same form of unique index declared `NULLS NOT DISTINCT`.

A default `NULLS DISTINCT` unique index with any nullable key column is not a
logical row identifier: PostgreSQL can store several rows whose encoded key is
identical. Partial and expression indexes are not eligible in V2. A deferrable
or otherwise non-immediate unique constraint is also ineligible because an
`IMMEDIATE` maintenance path can observe duplicates that are legal until source
transaction commit. If no eligible index exists, planning uses the existing
keyless/multiset identity rules or rejects a mode that requires unique identity.

The selected index's collation and equality operator family are part of
validation. V2 initially accepts default operator classes whose equality
contract is proven by the registry. A non-default operator class is rejected
unless a registry entry explicitly proves agreement between its equality
operator and canonical bytes. Catalog invalidation or DDL affecting any of these
properties invalidates the resolved identity schema and requires revalidation.

## 6. Type Semantics

Type support is an explicit registry. Runtime lookup begins with PostgreSQL type
OID, but encoder resolution is keyed by the complete semantic descriptor:

```text
concrete type | typmod | collation | equality operator family |
nested type metadata | PostgreSQL major version
```

There is no generic `::TEXT`, output-function, or `typsend` fallback. Every
resolved registry entry declares:

```text
semantic_family:          stable V2 type tag
equality_canonical:       required
default_nonnull_btree_order_preserving:  true | false
volatility:               immutable | stable
maximum_encoded_size:     fixed | typmod-bounded | unbounded
ddl_invalidations:        explicit list
supported_pg_majors:      explicit set
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

The stable wire `TYPE_TAG` identifies a canonical semantic family, not a
database object. Runtime OIDs and typmods never enter persisted bytes. Domains
intentionally use their base family's tag and erase domain identity. Enum values
use the enum-family tag and label bytes, so two concrete enum types with the same
label have the same field encoding; this is safe because row identities are
relation-local and the validated identity schema supplies the concrete type
contract. Set operations resolve a common PostgreSQL output type before
encoding. Altering a concrete type or replacing one type with another invalidates
that schema and requires rebuild even when the resulting bytes would coincide.
PostgreSQL major-version upgrades revalidate every registry contract before V2
state is consumed; an unverified major is rejected rather than assumed
byte-compatible.

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
can consume changes encoded with the new label. `fn_extra` caches enum dispatch
metadata but not enum labels. For each enum datum, the encoder resolves the value
OID through PostgreSQL's `ENUMOID` syscache and copies the current label while
holding the returned tuple. PostgreSQL's commit-time syscache invalidation, not
a custom pre-commit generation counter, governs cross-backend label visibility.
This is an in-memory syscache lookup rather than SPI or a catalog scan, and its
cost is part of the encoder benchmark. V2 tests both DDL-state invalidation and
syscache visibility explicitly, including a two-session rename after the
encoder call site is warmed.

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
but the initial support matrix must name which structural families ship; every
other type remains rejected. Adding a new type tag later does not change existing
V2 identities and therefore does not require V3. Each structural entry also
declares the registry properties above. Default non-NULL B-tree order
preservation is false until comparison against PostgreSQL's B-tree support
function has been demonstrated. Nested volatility is `stable` if any component
is stable and otherwise `immutable`; maximum size is unbounded if any component
is unbounded; DDL invalidations are the union of all component invalidations.
Composite alteration invalidates resolved field metadata and all dependent
stream tables.

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
hooks and forced reinitialization. The probe index expression calls only
`row_probe_v1(bytea)`, which remains genuinely `IMMUTABLE` and
`PARALLEL SAFE`; the row-ID encoder itself does not appear in that generated
index expression. Existing V1 hash functions retain their old behavior during
migration and are removed only after no V1 trigger or stored expression can
reference them.

## 8. Bounded B-Tree Indexing

A full canonical identity can be wider than PostgreSQL permits in one B-tree
index tuple, especially for keyless rows and composite text keys. Indexing the
full `BYTEA` without a static size proof would turn valid data growth into
runtime index failures. V2 indexes the complete `BYTEA` directly whenever the
identity schema is statically proven to fit. Only an unbounded schema uses the
bounded probe expression described below.

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

`row_probe_v1(bytea)` is an `IMMUTABLE`, `PARALLEL SAFE` index expression, not a
stored column or public value. The index always derives its key from the same
`__pgt_row_id` that the executor rechecks, so callers cannot supply a mismatched
probe.

The implementation must validate both the direct-index size proof and the
selected `P` against the running cluster's actual `BTMaxItemSize`, including
varlena and index-tuple overhead. The bridge binary computes V2 index readiness
for the running `BLCKSZ`; migration preflight and every V2 create/alter operation
reject V2 use if the probe's worst-case datum cannot fit, while existing V1 state
continues to operate. Direct-versus-expression classification is repeated for
each identity schema at create, alter, and migration preflight. Errors report
the configured page size, computed maximum datum size, and selected probe
version. A prefix length or direct bound safe for the default `BLCKSZ` is not
assumed safe for every supported build.

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

An identity schema whose maximum encoded index datum is statically within
`BTMaxItemSize` uses a direct B-tree index on `__pgt_row_id`. That index is
`UNIQUE` only when the identity semantics are unique; bounded keyless/multiset
storage uses a non-unique direct index. Every unbounded schema uses a non-unique
expression index on `row_probe_v1(__pgt_row_id)` plus an exact full-ID recheck.
The implementation must not place a `UNIQUE` constraint on a probe or digest.

Probe format and identity format are versioned separately. Changing prefix length
or digest algorithm requires rebuilding expression indexes, but not rewriting
heap rows or rebuilding logical row identities from source data.

## 9. Storage and DML Changes

Every storage, delta, temporary, CDC, and cache relation that currently carries
a `BIGINT` row identity moves to `__pgt_row_id BYTEA NOT NULL`. Transport
relations carry only the full identity. Consumers apply `row_probe_v1` when the
target schema is unbounded.

Bounded schemas match on full row-ID equality and use the direct B-tree. For a
bounded unique schema, the existing upsert shape becomes `ON CONFLICT
(__pgt_row_id)`. Bounded keyless storage uses the same full-ID predicate against
a non-unique direct index. Unbounded identities use MERGE or equivalent exact
DML against the non-unique expression index, with both probe-expression equality
and full row-ID equality in every UPDATE, DELETE, and MERGE predicate.

Keyless semantics do not change. Identical logical rows intentionally have the
same full identity and may occur more than once, so their direct or expression
index remains non-unique and counted-delete logic remains authoritative.

For unique but unbounded identity schemas, the database index is intentionally
non-unique because no bounded B-tree key can prove uniqueness of arbitrary-length
values. Scheduled and manual refresh retain the existing transaction advisory
lock keyed by `pgt_id` and catalog-row serialization.

Initial V2 does not add per-identity advisory locks. `IMMEDIATE` mode rejects a
unique unbounded identity during planning because the expression index cannot
enforce uniqueness. A later concurrency RFC may enable that path only after it
proves fresh-snapshot behavior, old/new-key acquisition, deadlock handling,
subtransaction ownership, and a cluster-wide lock-pool budget across concurrent
sessions. A per-transaction setting alone is not a sufficient capacity bound.
Bounded unique identities remain eligible for concurrent `IMMEDIATE` maintenance
because the direct `UNIQUE (__pgt_row_id)` index is authoritative.

`RowIdStrategy` and `RowIdSchema` continue to decide which logical fields form an
identity. They do not select a storage encoding. V2 has one encoder.

## 10. Shared CDC State

A source change buffer may feed several stream tables. V2 is therefore
extension-wide, not a per-stream-table option. Every consumer of a source uses
the same source identity bytes and probe rules.

Trigger CDC and WAL CDC both emit full V2 identities. A consumer derives the V2
probe only when matching an unbounded target. No hot-path lookup of downstream
stream-table preferences is required because no such preference exists.

The V2 extension-upgrade DDL adds non-null `row_identity_version` and
`row_probe_version` columns to `pgtrickle.pgt_change_buffers` and the
corresponding version state for stream-table storage. Buffer registry metadata
records both versions. Runtime readers refuse mismatches before consuming a row.
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

After rebuild, a bounded unique stream table may use its direct `UNIQUE
(__pgt_row_id)` index as replica identity. The column is catalog-marked `NOT
NULL`, satisfying PostgreSQL's replica-identity index requirements. A publication
that carries UPDATE or DELETE for that table must include `__pgt_row_id` in its
column list. Subscribers store it in a regular `BYTEA` column; no generated
publisher-to-generated-subscriber mapping is involved.

Unbounded or keyless stream tables use `REPLICA IDENTITY FULL`, and publication
column lists must include every column PostgreSQL requires for that identity
mode. `row_probe_v1` is only an index expression and is never part of a
publication schema. Operators must account for the resulting full old-row WAL,
network, and subscriber-apply cost on UPDATE and DELETE; it is included in the
replication benchmark matrix.

### 11.1 Security and privacy

"Opaque" is an API contract, not a confidentiality claim. V2 bytes contain
reversible encodings of source keys and, for keyless identities, may contain
every logical output field. Pass-through identity can retain an upstream key
that a later projection no longer displays. The ordered prefix in an overflow
probe also reveals the corresponding prefix of the canonical value. Hexadecimal
display does not anonymize any of this data.

The implementation and integration documentation must therefore enforce these
rules:

- default user-facing views and grants do not expose `__pgt_row_id`; reading the
   internal column requires an explicit privilege or ownership;
- logs, errors, traces, and monitoring report identity length and a short
   diagnostic fingerprint by default, not complete bytes or reversible prefixes;
- publication, outbox, DuckLake, and other sink configurations include the row
   ID only when their update/delete contract requires it or an administrator opts
   in after reviewing the data classification;
- operators treat identity fields containing personal data, credentials,
   tenant-private keys, or other secrets with the same controls as the source
   columns; and
- dump, support-bundle, and diagnostic documentation warns that internal
   identities can retain values omitted by a downstream projection.

## 12. Upgrade and Stream-Table Recreation

V1 and V2 state cannot coexist in one refresh graph. Because there are no
production V1 databases to preserve, V2 does not implement an in-place migration
or a V1 change replay protocol. This is an intentional pre-1.0 breaking change:
existing stream-table state is discarded and rebuilt from the unchanged source
tables.

The supported upgrade procedure is:

1. pause the scheduler and export each stream table's definition, query,
   schedule, refresh mode, and dependency order;
2. record external publication, outbox, dbt, DuckLake, and replication consumers
   that depend on `__pgt_row_id` or stream-table storage;
3. drop V1 stream tables in reverse dependency order using the supported SQL API;
   source tables and their data are not dropped;
4. remove the now-unused V1 change-buffer state through the extension's cleanup
   path, with no attempt to replay it into V2;
5. install the V2 extension build and restart PostgreSQL when the shared library
   has changed, so no backend retains V1 extension code;
6. recreate stream tables in dependency order with the recorded definitions;
   each table receives a fresh V2 initial refresh; and
7. recreate or resnapshot external consumers after updating them for the
   `BYTEA` row-ID contract, then resume the scheduler.

Source writes should be paused while the V1 graph is being removed and the V2
graph is being recreated. If writes cannot be paused, the operator must recreate
the graph using the normal source-capture setup before allowing writes and accept
that the initial refresh defines the new V2 state; V2 does not promise to replay
changes made during the recreation window.

The V2 preflight is deliberately small and fail-safe. It validates all identity
types, typmods, collations, source-key eligibility, operator classes, and
PostgreSQL-major registry contracts; detects any remaining V1 stream tables or
buffers; checks that external consumers are acknowledged for resnapshot; and
reports unsupported identities before changing state. If V1 state remains, the
operation fails with instructions to drop and recreate the affected stream
tables. It never partially converts a table, changes source data, or mixes V1
and V2 buffers in one graph.

There is no `V1_ACTIVE`, `MIGRATING_TO_V2`, or crash-resumable cutover state in
V2. A failed recreation is repaired by dropping the incomplete V2 stream-table
graph and running the documented recreation procedure again. The initial refresh
is the authoritative V2 baseline. Older binaries must reject V2 catalog state;
V2 binaries must reject leftover V1 state rather than guessing how to convert it.

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

For every applicable bounded and unbounded workload, the physical comparison
set includes:

- direct B-tree on the complete bounded `__pgt_row_id`;
- B-tree expression index on `row_probe_v1(__pgt_row_id)`;
- stored generated probe plus B-tree, including heap and WAL cost; and
- PostgreSQL's built-in hash index on the complete `__pgt_row_id`.

The built-in hash index is a particularly relevant unbounded, non-unique
baseline: it accepts wide input, stores a four-byte hash, and lets the executor
recheck complete `BYTEA` equality, but cannot enforce uniqueness or preserve
prefix locality. The matrix also retains V1 as an end-to-end baseline. A
material regression on cached common-key workloads must be investigated before
merge. The expected V2 benefit is removal of text conversion and substantially
better out-of-cache index locality where registry metadata supports that claim,
not merely a different microbenchmark score. V2 may regress cached integer keys,
randomly distributed deltas, wide keyless rows, and overflow identities that
require TOAST access; the release decision must measure rather than assume those
tradeoffs.

The release gates are index size, WAL volume, encoded-ID storage size,
CDC-buffer growth, cached integer-key latency, out-of-cache ordered-key latency,
overflow-key latency, and fresh initial-refresh throughput. The probe prefix and
physical index policy are frozen only after these results identify the best
acceptable tradeoff.

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
- direct full-ID indexes are selected whenever the proven datum bound fits the
   running cluster's `BTMaxItemSize`;
- unbounded identities select a non-unique expression index and no stored probe
   column; and
- no UNIQUE index is created where overflow is possible.

### 14.3 Cross-path tests

The same logical identity must produce byte-identical output through trigger CDC,
WAL CDC, full refresh, differential refresh, IMMEDIATE mode, joins, aggregates,
set operations, windows, and downstream stream tables.

Tests must include primary-key updates, NULL group keys, keyless duplicates,
deep join composition, synthetic identities, and wide TOASTed values.

Concurrency tests cover bounded unique identities under concurrent `IMMEDIATE`
writes and unbounded identities under scheduled/manual refresh. Every unbounded
unique `IMMEDIATE` path must raise `feature_not_supported` during planning,
before capture, storage mutation, or advisory-lock acquisition, at every
transaction isolation level. Enum tests use two sessions: session A warms the
encoder cache, session B renames an in-use label and commits, and session A calls
the cached expression again. Session A must either resolve the new label or
reject the affected stream state; it must never emit stale bytes. The test also
proves that the existing DDL hook marks the full downstream DAG for
reinitialization before further refresh.

Replication tests prove that bounded unique tables use the direct non-null row-ID
index as replica identity, required publication column lists include the row ID,
and subscribers use a regular `BYTEA` column. Unbounded and keyless tables test
`REPLICA IDENTITY FULL`. No schema or publication contains a generated probe
column.

### 14.4 Upgrade and recreation tests

- preflight detects V1 stream tables or buffers and makes no partial changes;
- source tables and source data remain untouched while the V1 graph is removed;
- V1 stream tables drop in reverse dependency order and V2 stream tables are
  recreated in dependency order;
- the recorded query, schedule, refresh mode, and dependency graph recreate
  successfully;
- source writes are paused during the recreation window, and the first V2
  refresh produces the exact expected multiset;
- an incomplete recreation is detected and can be repaired by dropping the
  partial V2 graph and starting again;
- replacing the shared library requires the documented PostgreSQL restart, and
  old binaries reject V2 catalog state;
- unsupported-type preflight fails before any stream-table state changes; and
- external publications, outbox consumers, dbt models, DuckLake sinks, and
  replication subscribers are identified and resnapshotted after the `BYTEA`
  row-ID change.

The final oracle is a fresh V2 build from unchanged source tables: recreated
output must be exactly equal as a multiset to an independent full computation.

## 15. Implementation Plan

**Stage 1: Resolve and freeze the contracts.** Write the normative byte
specification, domain and type-tag registries, golden vectors, and type
validation. Prove the 128-bit interval encoding and per-type equality/order
contracts, specify volatility and DDL invalidation, benchmark candidate probe
prefixes and all four physical index forms, and check `BTMaxItemSize` before
freezing identity V2 and probe V1.

**Stage 2: Implement the encoder.** Add the typed record entry point, cached
dispatch, scalar encoders, structural encoders required by the existing test
surface, synthetic domains, and probe helper.

**Stage 3: Change storage and matching.** Create one `BYTEA` row-ID column,
direct full-ID indexes for bounded schemas, expression-probe indexes for
unbounded schemas, and exact DML. Remove assumptions that row IDs are numeric or
that every non-keyless identity has a unique full-width index.

**Stage 4: Centralize producers.** Move trigger CDC, WAL CDC, scan, refresh,
join, aggregate, set, window, and synthetic identities to the shared encoder.
Delete `::TEXT` row-ID construction and hard-coded numeric sentinels.

**Stage 5: Add version guards and recreation tooling.** Persist identity/probe
versions, reject leftover V1 state, document the restart requirement, provide
preflight output and cleanup for V1 stream tables/buffers, and document the
reverse-drop/dependency-order recreation procedure outside `ALTER EXTENSION
UPDATE`.

**Stage 6: Prove it.** Run the full correctness matrix, recreation tests, and
performance benchmarks. Update SQL reference, architecture, upgrade,
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

### Index every complete `BYTEA` directly

This is the selected design for statically bounded identities. Applying it to an
unbounded identity would fail for sufficiently wide B-tree entries. The
expression probe is reserved for that unbounded case and guarantees indexability
without making a digest authoritative.

### Store a generated probe column

This duplicates ordinary short identities in the heap, increases WAL, complicates
replica identity and publication column lists, and cannot map generated-to-
generated in PostgreSQL 18 logical replication. An immutable expression index
provides the same unbounded lookup key without adding a public table column.

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
- bounded identities use direct full-ID B-trees, while unbounded expression-probe
   indexes remain bounded for arbitrary valid input size;
- the selected probe prefix fits the running cluster's `BTMaxItemSize` and is
   justified by the required benchmark matrix;
- the normative wire document defines every byte and structural limit and its
   golden identity/probe vectors pass before implementation merges;
- each registry entry proves equality agreement and declares default non-NULL
    B-tree ordering, volatility, size, DDL-invalidation metadata, semantic
    resolution context, and supported PostgreSQL majors;
- nullable `NULLS DISTINCT`, deferrable, partial, expression, and unregistered
   non-default-opclass indexes cannot become `SCAN_KEY` identities;
- interval identities encode the complete 128-bit PostgreSQL comparison value;
- enum label caching is invalidated by enum DDL and the encoder is not declared
   `IMMUTABLE` while label-based enum support is enabled;
- enum invalidation is observed across backends, not only in the backend that ran
   the DDL;
- unsupported types and collations fail before state is created;
- V1/V2 mismatch cannot consume or mutate persisted state;
- V1 stream tables and buffers are detected before any partial upgrade;
- source tables remain untouched while the old stream-table graph is removed;
- recreated V2 stream tables use reverse dependency order for removal and forward
   dependency order for creation;
- source writes are paused during recreation, and a fresh initial refresh is the
   correctness baseline;
- replacing the shared library requires a full PostgreSQL restart, and old
   binaries reject V2 catalog state;
- external compatibility breaks and resnapshot steps are documented and
   acknowledged before consumers resume;
- bounded unique replica identity uses the direct non-null row-ID index, and
   publication schemas include every required identity column;
- default views, grants, diagnostics, publications, and integration docs treat
   canonical row IDs as reversible source data rather than anonymized hashes;
- unique unbounded `IMMEDIATE` identities are rejected before mutation until a
   separate concurrency RFC proves correctness and cluster-wide lock capacity;
- out-of-cache composite-key workloads show the intended locality benefit;
- cached common-key workloads have no unexplained material regression;
- all performance release gates in section 13 pass.

## 18. Recommendation

Adopt exact canonical `BYTEA` row identities plus a bounded, non-authoritative
probe as pg_trickle's V2 architecture before 1.0. Do not freeze or implement the
wire specification until interval encoding, enum volatility, per-type ordering,
physical-index benchmarks, and durable migration meet the gates above. Keep
unbounded unique `IMMEDIATE` maintenance disabled until a separate concurrency
RFC is proven; it is not a V2 release dependency.

This is intentionally a breaking, extension-wide rebuild. That cost buys a row
identity the project can keep: exact rather than probabilistic, typed rather than
formatted, capable of preserving native order where proven, bounded at the
B-tree boundary, and shared consistently across CDC and DVM.

The full identity is the source of truth. The bounded probe is only an index.
That separation avoids both failure modes that otherwise force another redesign:
unbounded `BYTEA` index entries and hashes treated as proof of equality.
