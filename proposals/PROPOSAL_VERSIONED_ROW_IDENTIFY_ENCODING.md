# Proposal: Versioned Row Identity Encoding and Strategy Abstraction

**Status:** Proposed
**Target:** Pre-1.0 / v0.83.0 DVM semantic fidelity work
**Scope:** Row-identity correctness, migration safety, and architectural preparation for future optimizations

## 1. Summary

pg_trickle should replace its current composite row-identity encoding with a new, explicitly versioned encoding that is unambiguous, deterministic, and independent of PostgreSQL session formatting settings. The resulting row identity should continue to be stored as `__pgt_row_id BIGINT`, and the default implementation should continue to use a 64-bit hash. This keeps the public shape of stream tables unchanged while fixing an important weakness in how composite values are converted into hash input.

At the same time, pg_trickle should introduce a small internal abstraction around row-ID generation. The purpose of this abstraction is not to add multiple user-selectable strategies now. Instead, it should prevent the new encoding from becoming hard-coded throughout CDC, DVM, joins, aggregates, and refresh code. The first and only implemented strategy in this proposal should remain the hashed strategy. A direct integer-primary-key strategy may be added later as a separately benchmarked optimization without redesigning the row-ID system again.

The intended architecture is therefore:

```text
logical identity
      ↓
stable typed encoding
      ↓
versioned/domain-separated hash
      ↓
BIGINT
```

Future optimizations may replace the final step for narrowly defined cases, but they are outside the implementation scope of this proposal.

Four scope boundaries are stated explicitly because they are easy to leave implicit and expensive to get wrong: the supported type set is enumerated rather than open-ended (§6), the 64-bit hash is acknowledged as probabilistically rather than absolutely injective (§8), synthetic and sentinel identities are governed by the same module and versioning as user identities (§9), and the migration covers CDC change buffers and concurrent writers, not just stream-table rows (§15, §16).

One prerequisite is called out separately because it changes the shape of the work: row identity is currently computed inside generated SQL through functions that take `TEXT[]`, so a typed encoder cannot be introduced by replacing the hash behind those functions. A new typed entry point is required first (§10.1).

---

## 2. Problem

pg_trickle needs a stable identity for each maintained row so that INSERT, UPDATE, DELETE, MERGE, CDC, and DVM operations agree about which logical row they are referring to. For a single value this is relatively straightforward, but composite identities require several values to be converted into one byte sequence before hashing.

The current approach separates values using a delimiter. This is efficient, but a delimiter alone does not provide a rigorous guarantee that the original field boundaries are unambiguous. If field contents can contain the same byte sequence used as the delimiter, different logical tuples can theoretically result in the same pre-hash byte stream. A good row-identity system should not introduce this kind of ambiguity before the hash function is even involved.

There is a second, more subtle issue. Simply replacing the delimiter with length prefixes is not sufficient if the encoded value itself comes from PostgreSQL's textual output representation. Text representations can depend on type-specific formatting rules and, for some data types, session settings. The correct foundation is therefore not merely "better separators." pg_trickle needs a stable typed encoding whose meaning does not depend on how PostgreSQL happens to display a value in a particular session.

This proposal addresses both problems.

---

## 3. Design Principles

The new design should follow four principles. First, the same logical key must always produce the same encoded bytes regardless of where it is computed. CDC triggers, WAL decoding, IMMEDIATE mode, DIFFERENTIAL mode, joins, aggregates, and stream-table-to-stream-table propagation must not each invent their own representation.

Second, field boundaries and NULL values must be represented explicitly. The encoding of `(1, 23)` must be structurally different from `(12, 3)`, and NULL must be different from an empty string, zero, or any other valid value.

Third, encoding and hashing should be treated as separate concepts. Encoding answers "what is this logical identity in a stable byte representation?" Hashing answers "how do we turn that representation into the `BIGINT` used by pg_trickle?" Keeping those responsibilities separate makes the implementation easier to reason about and makes future optimizations possible without changing the definition of the logical identity.

Fourth, persisted row identities must always have a known encoding version. pg_trickle must never silently mix row IDs generated using two incompatible algorithms.

---

## 4. Proposed Architecture

Introduce an internal row-identity layer with two concepts:

```text
RowIdentity
├── canonical typed encoding
└── row-ID generation strategy
```

For this proposal, only one generation strategy is implemented:

```text
HashedCanonicalV2
```

The strategy receives a sequence of typed logical fields, encodes them using the V2 canonical encoding, applies explicit domain separation, hashes the resulting byte stream, and returns a `BIGINT`.

The abstraction should nevertheless be structured so that a future implementation could introduce something such as:

```text
DirectInteger
```

without requiring CDC, DVM, and refresh operators to be rewritten. That future strategy is deliberately not enabled by this proposal.

This distinction is important. We are designing for Option 2, but only implementing Option 1.

---

## 5. Canonical Typed Encoding V2

The new encoder must operate on typed PostgreSQL values rather than first converting every value to arbitrary SQL display text. Each supported type should have a deterministic binary representation defined by pg_trickle.

Every encoded identity begins with an encoding version marker. Every field then includes a NULL/value tag, a type identifier or type class where necessary, and an unambiguous payload length.

Conceptually:

```text
ENCODING_VERSION
FIELD
FIELD
FIELD
...
```

A NULL field can be represented as:

```text
NULL_TAG
```

A non-NULL field can be represented as:

```text
VALUE_TAG | TYPE_TAG | LENGTH | PAYLOAD
```

For example, two text fields containing `"ab"` and `"c"` could conceptually become:

```text
V2
TEXT 2 "ab"
TEXT 1 "c"
```

while `"a"` and `"bc"` become:

```text
V2
TEXT 1 "a"
TEXT 2 "bc"
```

Their boundaries are explicit, so the two tuples cannot produce the same canonical representation.

The exact wire format should be simple and documented in code. It does not need to be a public serialization format, but once persisted row IDs depend on it, changes to that format must require a new encoding version.

---

## 6. Supported Types and Type Encoding

The set of types V2 supports must be **explicitly enumerated**, not left to a generic fallback. A row identity is only correct if the encoding agrees with the equality semantics that the rest of the engine uses. The governing rule is:

> If two values compare equal under the operator used by the MERGE/join predicate for that type, they must produce identical V2 encodings. If they compare unequal, they must produce different encodings.

This rule is stronger than "deterministic bytes," and it is the reason several types need canonicalization rather than a raw memory image.

### 6.1 Tier 1 — supported with a defined canonical encoding

These types must be supported by the initial V2 implementation. Each entry states the canonical form and the canonicalization hazard it addresses.

| Type family | Canonical encoding | Notes / hazard addressed |
|---|---|---|
| `bool` | one byte, `0x00` / `0x01` | — |
| `int2`, `int4`, `int8`, `oid` | fixed-width, fixed byte order | widths are distinguished by the type tag, so `1::int2` and `1::int4` encode as different fields |
| `float4`, `float8` | IEEE-754 bits, normalized | `-0.0` must normalize to `+0.0` and all `NaN` payloads must normalize to one canonical `NaN`, because PostgreSQL btree equality treats them as equal |
| `numeric` | normalized decimal (sign, exponent, digit sequence with trailing zeros removed) | `1.0` and `1.00` are equal in PostgreSQL but differ in display scale and in the raw stored form |
| `text`, `varchar` | raw string bytes with explicit length framing | see §6.4 on collation |
| `bpchar` (`char(n)`) | string bytes with trailing spaces stripped | `bpchar` equality ignores trailing spaces |
| `bytea` | raw bytes with explicit length framing | avoids `bytea_output` (`hex` vs `escape`) session dependence |
| `uuid` | the underlying 16 bytes | avoids textual formatting |
| `date` | internal 32-bit day number | avoids `DateStyle` dependence |
| `time`, `timetz` | internal microsecond value, plus zone offset for `timetz` | — |
| `timestamp`, `timestamptz` | internal 64-bit microsecond value | `timestamptz` is stored in UTC internally, so this is immune to the session `TimeZone` GUC, unlike its text form |
| `interval` | the internal `(months, days, microseconds)` triple | must **not** be normalized across fields; PostgreSQL does not treat `1 month` and `30 days` as equal |
| enum types | the enum **label text** | enum OIDs differ between databases and after dump/restore, so OIDs must not be used |
| domain types | encoded as the underlying base type, tagged with that base type | — |
| `jsonb` | the binary form (keys sorted, duplicates removed) | `json` is **not** in this tier — see §6.3 |

### 6.2 Tier 2 — supported by structural recursion

Arrays, composite (row) types, and range types are encoded by recursing into their elements using the same field framing, with explicit encoding of element count, per-element NULL tags, and — for arrays — dimensionality and lower bounds. Empty ranges and NULL-bounded ranges must be tagged distinctly. Nesting depth must be bounded, and exceeding the bound must be a hard error rather than a silent fallback.

### 6.3 Tier 3 — rejected

Any type not in tier 1 or tier 2 must be **rejected at DDL time** with a clear error naming the column and its type, rather than accepted with a best-effort textual fallback. Silent fallback to `::text` is precisely the failure mode V2 exists to remove, and a wrong row identity is far more damaging than a rejected `CREATE STREAM TABLE`.

Known members of this tier at the time of writing include `json` (whitespace, key order, and duplicate keys are all preserved in the text form, and `json` has no equality operator at all), `xml`, `money` (output depends on `lc_monetary`), `point` and the other geometric types (float components with no exact equality operator), `tsvector` / `tsquery`, and any user-defined type with no registered encoder.

Promoting a type into tier 1 or tier 2 later is backwards-compatible only if it never appeared in a persisted identity — which rejection guarantees. That is a second reason to prefer rejection over fallback.

### 6.4 Text and collation

The goal of V2 is deterministic identity, not PostgreSQL sort-order preservation. Text is therefore encoded as its actual string bytes together with explicit framing, and the encoding must not claim to reproduce locale-sensitive collation order. That distinction becomes important if ordered `BYTEA` row IDs are ever considered later.

One consequence must be documented: under a **non-deterministic collation**, two distinct byte strings can compare equal in PostgreSQL while producing different V2 identities. V2 identity is byte identity for text. Columns with a non-deterministic collation should therefore either be rejected from identity roles as tier 3, or be documented as producing byte-level rather than collation-level identity. The implementation must pick one and test it, not leave it undefined.

### 6.5 Dispatch

Type dispatch must be centralized in the row-identity module and resolved from the type OID — never duplicated across call sites and never inferred from a value's text output.

---

## 7. Hashing and Domain Separation

After canonical encoding, the byte stream should be hashed into the existing `BIGINT` representation. xxh3 may continue to be used unless benchmarking or correctness work gives a separate reason to change it.

The hash input must include a domain identifier in addition to the encoding version. This prevents logically different kinds of identities from accidentally sharing the same hash namespace.

For example:

```text
PGT_ROW_ID_V2 | SCAN_KEY | encoded fields
PGT_ROW_ID_V2 | GROUP_KEY | encoded fields
PGT_ROW_ID_V2 | JOIN_KEY | encoded child identities
```

The exact set of domains should remain small. The important property is that a derived join identity should not be defined merely by concatenating two arbitrary `BIGINT` values without information about what those values mean.

This also prepares the architecture for a future direct-integer strategy. If a future operator combines a raw integer child identity with a hashed child identity, the derived encoding must encode both the value and its identity kind. A raw `42` and a hashed value whose numeric result happens to equal `42` must not become semantically indistinguishable when used as components of a derived identity.

The initial V2 implementation should establish this rule now, even though only hashed identities are produced initially.

---

## 8. Hash Width and Collision Semantics

V2 removes **encoding** ambiguity. It does not, and cannot, remove **hash** collisions. This proposal must state that plainly, because the two are easy to conflate and the existing code already leans on informal "minimise collision probability" language.

A 64-bit identity is subject to the birthday bound. For $n$ distinct logical identities within one matching namespace, the probability of at least one collision is approximately

$$p \approx \frac{n^2}{2^{65}}$$

which is roughly $3 \times 10^{-4}$ at $n = 10^{8}$ and roughly $2.7\%$ at $n = 10^{9}$. Those are not negligible numbers at the scale pg_trickle targets.

The blast radius matters as much as the probability. Because MERGE matches on `__pgt_row_id`, a collision does not raise an error — two logically distinct rows are treated as the same row, and one is silently overwritten or deleted. That is a silent-wrong-answer failure, the most expensive class of bug for this project.

The position taken by this proposal is:

1. V2 guarantees that **distinct logical identities produce distinct encoded byte streams**. That is the property that is proven and property-tested.
2. The mapping from encoded bytes to `BIGINT` is a 64-bit hash and is therefore **probabilistically**, not absolutely, injective. No document, doc comment, test name, or commit message may describe row IDs as collision-free.
3. The 64-bit width is retained for V2 because widening the identity is a separate change with a much larger compatibility surface — column type, index width, and replication format all change.
4. Two follow-ups are recorded but **not** implemented here: (a) verifying matches by additionally comparing identity columns in the MERGE predicate where those columns are physically present in the stream table, treating `__pgt_row_id` as an index-friendly probe rather than proof of equality; and (b) a wider 128-bit `BYTEA` identity. Option (a) is far cheaper and should be evaluated first.
5. The documentation should publish the scale guidance above, so the residual risk is a stated engineering property rather than an unstated assumption.

One interaction deserves separate mention. pg_trickle already supports stream tables whose `__pgt_row_id` index is **non-unique**, because uniqueness of the identity cannot always be proven from the query. In those tables a collision is indistinguishable from a legitimate duplicate: there is no constraint that would ever surface it, and no diagnostic that could distinguish the two. Conversely, where the index *is* unique, a collision surfaces as a unique-violation error — an outage rather than silent corruption, which is the better failure. This asymmetry should be stated in the documentation, because it means the practical risk from §8 is concentrated in exactly the stream tables that have the weakest guarantees to begin with.

---

## 9. Sentinel and Synthetic Identities

Not every `__pgt_row_id` comes from encoding user data. The DVM also manufactures identities, and today it does so with ad-hoc constants — the lateral inner-change dummy row uses the literal `i64::MIN + 1`, chosen because it is "unlikely" to be produced by the hash, and scalar aggregates use a singleton sentinel. These values sit outside the canonical encoding entirely, which means they sit outside its guarantees.

V2 must bring them inside. The rules should be:

**Synthetic identities are produced by the row-identity module, not by literals at call sites.** A dedicated `SYNTHETIC` hash domain should be used, with a stable string discriminator per purpose — for example `hash_identity(SYNTHETIC, ["lateral_inner_dummy"])` and `hash_identity(SYNTHETIC, ["scalar_agg_singleton"])`. This places synthetic identities in the same 64-bit space with the same, stated collision characteristics as everything else, instead of resting on an informal claim that a particular magic number is unlikely to occur.

**No reserved numeric band is introduced.** Carving out a range of `BIGINT` values for internal use is tempting, but every `INT8` value is a legitimate PostgreSQL key value, and a reserved band would conflict directly with the future direct-integer strategy in §12. If a band is ever adopted, it must be adopted together with that strategy, and values in the band must then be explicitly excluded from raw-integer eligibility.

**SQL NULL is never a valid row identity.** A NULL group key must encode through the NULL field tag and yield a real `BIGINT`; `__pgt_row_id` must not be NULL in any stream table row. The separate use of NULL as an *aggregate merge* marker inside generated delta SQL is a different mechanism and is out of scope here, but the implementation must not let the two meanings blur.

**The inventory must be explicit.** Stage 3 of the implementation plan should enumerate every producer of a synthetic or sentinel row identity — the lateral inner-change dummy, the scalar-aggregate singleton, keyless all-column identities, and anything else the audit finds — and route each through the module. When the stage is complete, a search for `i64::MIN`, hard-coded row-ID literals, and locally constructed hash expressions should return nothing outside the row-identity module.

**Sentinels are versioned with the encoding.** Because synthetic identities become hash outputs under a V2 domain, their values change in the V1-to-V2 migration like any other identity, and they fall under the same reinitialization requirement.

---

## 10. One Shared Implementation

The most important implementation requirement is that row-identity encoding must live in one shared module. CDC and DVM should not independently reconstruct equivalent SQL expressions such as arrays of `::TEXT` values.

The shared implementation should provide a small set of primitives conceptually similar to:

```text
encode_field(type, datum)
hash_identity(domain, fields)
hash_child_identities(domain, children)
```

The exact Rust API can differ, but the ownership boundary should be clear: operators decide **which logical fields constitute an identity**, while the row-identity module decides **how those fields are encoded and hashed**.

`RowIdStrategy` and `RowIdSchema` should continue to describe identity semantics such as primary-key identity, group-key identity, all-column identity, pass-through identity, and derived identity. They should not contain separate encoding implementations for each operator.

This gives pg_trickle one place to test and audit its row-ID invariant.

### 10.1 Delivery mechanism: typed arguments, not `TEXT[]`

There is a structural obstacle that the rest of this proposal assumes away and that must be resolved before Stage 2 can begin.

Row identity today is not computed in Rust over tuple datums. It is computed in **generated SQL**. `build_hash_expr` emits either

```text
pgtrickle.pg_trickle_hash(<expr>)
```

or, for composite identities,

```text
pgtrickle.pg_trickle_hash_multi(ARRAY[(<expr1>)::TEXT, (<expr2>)::TEXT, ...])
```

and that string is embedded in scan CTEs, delta queries, MERGE sources, and full-refresh statements. The SQL function's Rust signature is `Vec<Option<String>>`. Every value therefore passes through `::TEXT` at the SQL level before Rust ever sees it, and by that point the type information and any session-formatting exposure are already baked in.

This means a typed V2 encoder cannot simply be dropped in behind the existing functions. "Centralize call sites" (Stage 3) is not sufficient on its own — a centralized implementation that still receives `TEXT[]` would be tidier code with exactly the same defect. The proposal must therefore commit to a delivery mechanism:

**Use a `VARIADIC "any"` entry point.** A function such as

```text
pgtrickle.row_id_v2(domain text, VARIADIC "any") RETURNS bigint
```

receives the real datums along with their declared types, which the implementation recovers with `get_fn_expr_argtype` per argument position. This is the only way to get typed values into the encoder while keeping the generated-SQL architecture. The generated expression then loses its `::TEXT` casts entirely.

**Cache the resolved types in `fn_extra`.** Argument types are fixed for the lifetime of a given call site, so type resolution and encoder dispatch belong in `fn_extra` on the `FmgrInfo`, computed once per query execution rather than once per row. This is the concrete mechanism behind the general requirement in §11, and without it V2 will be measurably slower than V1 rather than comparable.

**Add new functions; do not redefine the old ones.** `pg_trickle_hash` and `pg_trickle_hash_multi` are `#[pg_extern]`, live in the `pgtrickle` schema, and are marked `IMMUTABLE, PARALLEL SAFE`. Redefining them in place would change the meaning of an `IMMUTABLE` function whose outputs are already persisted, which is exactly the situation `IMMUTABLE` exists to forbid. V2 should ship as new functions in the extension upgrade script, with the V1 functions retained through the transition and removed in a later release once no persisted state or generated SQL can reference them.

**`IMMUTABLE` must remain truthful.** Any encoding that consulted `DateStyle`, `TimeZone`, `bytea_output`, `lc_monetary`, or a collation would make the V2 function non-immutable in fact even if labelled immutable, with the planner free to fold and cache results across sessions. The tier-1 canonical forms in §6 were chosen so that the label stays honest; this is a correctness constraint on the type policy, not merely a preference.

---

## 11. Performance Requirements

Correctness is the reason for the change, but row-ID generation is a hot path and the new implementation should avoid unnecessary overhead. The encoder should preferably stream its output directly into the hash state rather than constructing a complete intermediate buffer for every row.

Tuple metadata and type dispatch should be resolved outside the per-row path wherever possible. If a record type is known for a generated plan, the encoder should cache the necessary type information rather than repeatedly performing catalog or function lookups for every row. For the `VARIADIC "any"` entry point described in §10.1, the concrete mechanism is `fn_extra` on the `FmgrInfo`, resolved on first call and reused for every subsequent row in the same execution.

The implementation should also avoid creating `Vec<String>` structures or repeatedly formatting values into SQL text when typed access is available. These changes are useful independently of future integer fast paths and may offset some of the additional framing work introduced by V2.

Performance optimizations should not change the canonical byte representation. Two implementations of V2 must generate exactly the same encoding.

---

## 12. Integer Primary-Key Fast Path

The architecture should make a future direct-integer strategy possible, but that strategy should not be implemented as part of this correctness migration.

A direct `INT4` or `INT8` primary key potentially offers substantial advantages: no hashing, no conversion, excellent B-tree locality for sequential keys, and — given §8 — no collision risk at all for the identities it covers. However, using the raw integer directly introduces additional correctness questions that deserve independent treatment. pg_trickle currently has DVM paths that use special `BIGINT` sentinel values, and every possible `INT8` value is also a legitimate PostgreSQL key value. Those sentinels must be moved onto the synthetic-domain rules in §9 before a raw `INT8` identity can be guaranteed safe.

Derived identities also need the domain-separation rules described above so that a direct integer and a hash result with the same numeric value remain semantically distinguishable when combined.

For these reasons, the correct sequence is:

```text
V2 correctness foundation
        ↓
remove sentinel assumptions
        ↓
benchmark direct integer identity
        ↓
implement only if worthwhile
```

This proposal completes only the first step while ensuring that the architecture does not block the later steps.

---

## 13. Persisted Encoding Version

pg_trickle should record the row-identity encoding version associated with persisted state. The exact catalog representation is an implementation detail, but the system needs enough information to answer:

> Were these stored row IDs generated using the same encoding that the current code will generate?

A simple internal version value such as:

```text
row_identity_version = 2
```

may be sufficient for the initial implementation.

Crucially, the version must be recorded for **both** kinds of persisted identity state: stream-table storage and the CDC change buffers in `pgtrickle_changes`. Buffered change rows carry V1 identities exactly as stream-table rows do, and a marker that covers only the stream table cannot detect a mixed-version buffer. Every consumer of identity state must refuse to proceed when the recorded version does not match the running code, and must fail loudly rather than fall back to a full refresh that quietly reuses stale identities.

The version check must be enforced at **runtime against catalog state**, not only inside the extension upgrade script. A PostgreSQL extension's shared library and its catalog contents version independently: an operator can install a new `.so` and restart the server without ever running `ALTER EXTENSION pg_trickle UPDATE`, and in a rolling or container-image upgrade this is the *normal* sequence rather than an exotic mistake. If the only guard lives in the upgrade SQL, a new binary will happily write V2 identities into a V1 catalog. The guard therefore belongs on the read/write paths themselves.

Downgrade is not supported. Once state has been migrated to V2, running an older binary against it must fail with a clear error rather than silently reinterpret V2 identities as V1. This should be stated in the release notes, since it constrains rollback plans.

Strategy metadata should only be added where it is actually needed. A single stream table may eventually contain scan identities, group identities, and derived join identities, so the proposal should not assume that one future `row_id_strategy` string on the stream table can describe an entire DVM plan.

Encoding version belongs to persisted compatibility state. Strategy selection belongs to the relevant plan nodes or generated code.

---

## 14. Public Compatibility Contract

`__pgt_row_id` should remain a `BIGINT`, but pg_trickle should explicitly document its value as **opaque implementation state**.

Users may observe the column, use it for diagnostics, or move it through replication, but applications should not assume that the same logical row will retain the same numeric `__pgt_row_id` forever across a reinitialization or an extension upgrade that changes the row-identity encoding version.

The pre-1.0 contract should therefore be:

> The presence and type of `__pgt_row_id` may remain stable, but its numeric value is not a durable business identifier.

This clarification substantially reduces the compatibility burden of future correctness fixes while preserving the practical usefulness of the column.

---

## 15. Migration

V2 will intentionally generate different hashes for many existing identities. Therefore old and new row IDs cannot safely coexist.

The upgrade must not simply install the new encoder and allow the next refresh to continue. Existing stream-table rows could contain V1 identities while new CDC events contain V2 identities, causing updates and deletes to miss their corresponding stored rows.

The migration must perform an explicit transition, and its scope is **all persisted identity state, not only stream-table rows**. Concretely it must account for:

1. **Stream-table storage** — every `__pgt_row_id` in every stream table.
2. **CDC change buffers** — pending rows in `pgtrickle_changes.changes_<oid>` that carry V1 identities. This is the most dangerous case: a buffer drained after the encoder is swapped would apply V1-keyed deltas against a V2-keyed table, producing missed deletes and duplicated inserts with no error raised.
3. **WAL-decoder state** — any decoded-but-unapplied position or identity state held by the WAL CDC path, which must not be replayed across the encoding boundary.
4. **Derived and cached identity state** — L0 caches, pre/post snapshot temp state, and any materialized helper structure keyed by row ID.
5. **Indexes and generated columns** — the `__pgt_row_id` index on each storage table, plus any expression index or generated column that references the row-ID function. A rebuilt table implies rebuilt indexes; an expression index over the *old* function would silently retain V1 semantics and must be dropped or redefined rather than reindexed in place.
6. **Downstream stream tables** — any stream table whose identity derives from an upstream stream table's identities, rebuilt in dependency order.

For existing installations, pg_trickle should mark affected stream tables as requiring reinitialization. Existing CDC identity state generated using V1 must not be consumed as V2 state — it must be discarded as part of a cutover that simultaneously re-establishes the source position (§16), never simply drained. Stream tables are then rebuilt from an authoritative source snapshot using the new encoding.

The simplest safe pre-1.0 policy is to treat the V1-to-V2 upgrade as requiring reinitialization of all existing stream tables and invalidation of all existing change-buffer contents, rather than attempting fine-grained detection of which specific identities happen to be unaffected.

---

## 16. Migration Concurrency and Cutover Safety

Reinitialization must be designed so that writes occurring during the migration are not lost. This requirement is what makes "discard the V1 buffers" safe rather than reckless: discarding buffered changes is acceptable only if the snapshot replacing them provably covers those same changes.

A safe implementation should use the same general principle as pg_trickle's existing snapshot/frontier machinery: establish a precise source position, build the new state relative to that position, and then process changes occurring after it. The migration must never depend on a sequence such as "clear the buffer, rebuild the table, then start capturing again," because concurrent source writes could fall into the gap.

The ordering constraint is therefore:

1. Capture must remain **continuously armed**. At no point may triggers be dropped, disabled, or recreated in a way that leaves an uncaptured window, however brief.
2. The snapshot position and the V1-buffer discard point must be established in the **same transaction**, so that "everything before position P is in the snapshot" and "everything after position P is in the buffer" partition the change stream exactly.
3. Rebuild from the snapshot using V2.
4. Resume application from position P, with the buffer now holding only V2-encoded state.

The exact implementation should reuse existing CDC transition and frontier mechanisms rather than inventing a separate migration protocol. The required invariant is:

> Every committed source change must be reflected either in the snapshot used for V2 initialization or in CDC state processed after that snapshot, exactly once.

If the current infrastructure cannot guarantee that invariant for an in-place encoding transition, the safer implementation is to quiesce the affected sources for the critical cutover — taking a lock strong enough to exclude concurrent writers on the source relations for the duration of the position-fixing transaction. Blocking writers briefly is an acceptable cost for an upgrade step; losing a committed write is not. This fallback must be an explicit, documented, tested code path, not an unstated hope that the window is small.

A crash or cancellation partway through must leave the affected stream tables marked as requiring reinitialization rather than in a silently mixed-version state. The migration must be safely resumable, and a half-migrated stream table must refuse incremental refresh until it completes.

Migration safety is part of the correctness work and must be tested as such, including a test that drives concurrent source writes throughout the entire migration and asserts that the post-migration stream table exactly equals a from-scratch full refresh.

---

## 17. Shared Change Buffers

A single source may feed multiple stream tables. Therefore the row-identity representation stored in source CDC state cannot depend on an arbitrary downstream stream-table preference.

V2 should be an extension-wide encoding version for source identity state. Every consumer of a particular source should interpret its CDC identity using the same encoding version.

This is another reason not to introduce a public `row_id_encoding` option now. A per-stream-table option would complicate shared source buffers and make it possible for different consumers to require incompatible upstream representations.

If future benchmarks justify multiple storage strategies, the design should keep the canonical source identity representation independent from the downstream storage strategy.

This also means the migration cannot be performed per stream table in isolation. Because one buffer feeds many consumers, the cutover unit is the **source together with all of its consumers**: the buffer's encoding version flips once, and every consumer of that buffer must have been reinitialized against the same snapshot position before incremental processing resumes.

---

## 18. Testing

The V2 encoder should have direct unit tests for every supported type and every framing invariant. NULL must differ from all non-NULL values. Field order must matter. Field count must matter. Values containing arbitrary text bytes must not affect field boundaries. Negative and boundary numeric values must round-trip deterministically.

Each tier-1 type in §6 needs its own equality-agreement test: pairs that PostgreSQL considers equal must encode identically (`1.0` vs `1.00` numeric, `-0.0` vs `0.0`, differing `NaN` payloads, `bpchar` with and without trailing spaces, the same `timestamptz` read under two different `TimeZone` settings, the same values read under two different `DateStyle` and `bytea_output` settings), and near-miss pairs must encode differently. Tier-3 types need a test asserting that DDL is **rejected** with a useful message rather than silently accepted.

Property-based tests should operate on the canonical encoding before hashing. For two different logical key tuples, their V2 encoded byte streams must differ. This is the meaningful injectivity property pg_trickle can guarantee — and it applies to the encoding, not to the hash.

The final 64-bit hash should be tested for determinism and consistency across all execution paths. No test, assertion message, or doc comment may claim or imply that the 64-bit hash is collision-free; §8 governs how that limitation is described.

Synthetic identities from §9 need their own tests: each must be stable across processes and plans, must be distinct from the others, and must not be produced by any hard-coded literal outside the row-identity module.

End-to-end tests should cover single and composite primary keys, UUIDs, NULL-containing group keys, keyless/all-column identities, joins, aggregates, PK-changing UPDATEs, trigger CDC, WAL CDC, IMMEDIATE mode, DIFFERENTIAL mode, stream-table DAGs, and V1-to-V2 upgrade/reinitialization.

Migration tests are first-class here, not an afterthought. At minimum: an upgrade with non-empty change buffers pending; an upgrade with a multi-level stream-table DAG; an upgrade with concurrent writers running for the whole migration; an upgrade interrupted mid-way and resumed; and a negative test proving that a V1 buffer cannot be drained by V2 code without an error.

A particularly important test should generate the same logical change through multiple execution paths and verify that every path computes the same V2 identity.

---

## 19. Benchmarking

Before merging, V2 should be benchmarked against the current encoder using representative identities: a single integer, a UUID, two-column composite keys, larger composite keys, short strings, and long strings.

The benchmark should measure raw encoding throughput, hash throughput, allocations per row, CDC overhead, and end-to-end differential refresh latency.

The acceptance criterion should not require V2 to outperform the existing implementation. It is a correctness fix. However, any significant regression should be investigated and reduced where practical, especially if it comes from avoidable allocations, repeated type lookup, or conversion through SQL text.

The same benchmark harness should later be reused to evaluate a direct integer-primary-key strategy.

---

## 20. Implementation Plan

The work should be implemented in small stages.

**Stage 1: Define the invariant.** Add the V2 encoding specification, version/domain constants, the explicit tier-1/tier-2/tier-3 type policy from §6, and tests for canonical field encoding.

**Stage 2: Build the shared encoder.** Implement typed field encoding and streaming hash generation in a dedicated row-identity module, including the equality-agreement canonicalizations (numeric scale, float zero/NaN, `bpchar` padding). Add the `VARIADIC "any"` entry point and `fn_extra` type caching from §10.1 as new SQL functions, leaving the V1 functions untouched.

**Stage 3: Centralize call sites.** Replace independent composite-hash construction in CDC, WAL decoding, IMMEDIATE processing, DVM scan generation, joins, aggregates, and other derived identities with calls through the shared abstraction. Remove the `::TEXT` casts from generated row-ID expressions. Enumerate and migrate every synthetic/sentinel identity producer per §9, and add tier-3 rejection at DDL time.

**Stage 4: Add version tracking.** Persist enough internal metadata to distinguish V1 and V2 state — for stream tables *and* change buffers — and hard-fail rather than proceed on mixed-version incremental processing, enforced at runtime against catalog state rather than only in the upgrade script.

**Stage 5: Implement migration/reinitialization.** Add the safe V1-to-V2 cutover path per §15 and §16: continuously armed capture, a single transaction fixing the snapshot position and discarding V1 buffer state, dependency-aware rebuilding, resumability after interruption, and the documented quiesce fallback. Concurrent-write tests gate this stage.

**Stage 6: Document the contract.** State clearly that `__pgt_row_id` remains `BIGINT` but its numeric value is opaque and may change after reinitialization or encoding-version migrations, and publish the collision guidance from §8.

**Stage 7: Benchmark.** Record V1 versus V2 performance and retain the harness for later integer-fast-path evaluation.

No direct-integer strategy should be introduced in these stages.

---

## 21. Alternatives Considered

Keeping the current delimiter-based representation would avoid migration work, but it leaves an unnecessary ambiguity in a correctness-critical identifier and makes the row-ID contract harder to defend before 1.0.

Changing directly to ordered `BYTEA` identities could potentially improve index locality for large tables, but it solves a different problem and introduces a substantially larger compatibility surface. The column type, index width, replication format, and downstream expectations would all change.

Introducing raw integer primary keys immediately is attractive from a performance perspective, but doing so during the same migration would combine a correctness change with an optimization and make failures harder to isolate. It also requires sentinel cleanup and additional derived-identity rules.

Widening the identity to 128 bits at the same time would address the collision exposure quantified in §8, but it changes the column type and every index, replication payload, and downstream expectation that depends on it. Doing that in the same change as the encoding fix would make an already-invasive migration substantially riskier, and the encoding fix is a prerequisite for a wider identity whenever it happens.

The recommended approach is therefore intentionally conservative: fix the correctness foundation first and make later optimizations easy rather than attempting to ship all possible strategies simultaneously.

---

## 22. Recommendation

Adopt **Versioned Row Identity Encoding V2** before 1.0.

The implementation should retain `__pgt_row_id BIGINT`, use a deterministic typed and length-framed canonical encoding over an explicitly enumerated set of supported types, centralize row-ID generation — including synthetic and sentinel identities — across CDC and DVM, introduce explicit encoding versioning and hash-domain separation for both stream tables and change buffers, and provide a reinitialization path that is safe under concurrent writes for existing installations.

The proposal claims injectivity for the **encoding** only. The 64-bit hash remains probabilistic, and that limit must be documented rather than glossed over.

The internal API should be designed around a row-ID strategy abstraction, but V2 hashing should be the only implemented strategy in this proposal.

After this foundation ships and the migration is proven safe, pg_trickle can separately evaluate a direct `INT4`/`INT8` primary-key fast path. At that point the decision can be based on benchmarks rather than architecture pressure, and the existing CDC/DVM code will already be structured to support it cleanly.

---

## 23. Review Record and Open Questions

This section records the review outcome and the questions that remain genuinely open. It should be resolved and removed before the proposal moves from Proposed to Accepted.

### 23.1 Review outcome

The direction is endorsed: versioned unambiguous encoding, hashed to the existing `BIGINT`, centralized generation, rebuild during migration, and integer identities deferred. Review raised five clarifications, all of which have been folded into the body of the proposal — migration scope beyond stream tables (§15), an explicit supported-type set (§6), concurrency safety during cutover (§16), rules for sentinel and synthetic identities (§9), and honest treatment of 64-bit collision probability (§8).

A sixth issue was identified during review of the existing code and is the largest change to the plan: the current row-ID pipeline runs through generated SQL with `::TEXT` casts, so a typed encoder requires a new `VARIADIC "any"` entry point rather than a drop-in replacement behind the existing functions (§10.1). Without that, Stage 3 could be completed in full while leaving the original defect intact.

### 23.2 Open questions

**Q1 — Non-deterministic collations.** §6.4 offers two options: reject such columns from identity roles, or accept them with documented byte-level identity. Rejection is safer but may break existing installations on upgrade, turning a correctness fix into a functional regression for those users. A decision is needed, along with a survey of how likely such columns are in practice.

**Q2 — Scope of the tier-3 rejection on upgrade.** An existing stream table whose identity includes, say, a `json` column is currently working, however unsoundly. V2 would reject it. Does the migration refuse to complete, or does it complete while marking that stream table permanently degraded and unmaintainable? The former is defensible pre-1.0; the latter needs its own design. Either way, the upgrade must detect and report affected stream tables *before* it starts making changes.

**Q3 — Cost of the quiesce fallback.** §16 permits taking a lock strong enough to exclude writers on source relations for the position-fixing transaction. On a large installation with many sources this could mean a meaningful write stall during upgrade. The expected duration should be measured on a realistic dataset before this is presented as the recommended path, and the documentation should give operators a way to estimate it.

**Q4 — Does V2 actually pay for itself on performance?** §19 sets no performance bar, which is correct for a correctness fix, but the honest expectation should be stated up front. Removing per-row `::TEXT` formatting and array construction is a real saving; adding per-field framing and type dispatch is a real cost. If the net turns out significantly negative on composite keys, that changes the release calculus and should be surfaced early rather than at Stage 7.

**Q5 — Priority of the collision follow-up.** §8 records identity-column verification in the MERGE predicate as the cheaper of two mitigations but does not schedule it. Given that the risk is concentrated in non-unique-index stream tables, it is worth deciding now whether that follow-up is a 1.0 blocker or genuinely post-1.0 work.

**Q6 — Interaction with logical replication.** `__pgt_row_id` values can be published downstream. A subscriber holding V1 identities while the publisher migrates to V2 is a scenario the migration sections do not currently cover. It may be adequately handled by the opacity contract in §14, but that should be confirmed rather than assumed.
