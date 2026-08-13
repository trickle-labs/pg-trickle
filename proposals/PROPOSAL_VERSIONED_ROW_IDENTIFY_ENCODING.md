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

## 6. Type Encoding

The implementation should start with a clear set of encoders for the PostgreSQL types currently important to row identity. Fixed-width numeric types should be encoded in a deterministic byte order. Boolean values should have fixed byte representations. UUIDs should use their underlying 16 bytes. Date and timestamp-family values should use a stable internal numeric representation rather than formatted strings where practical.

Text-like values require more care. The goal of V2 is deterministic identity, not PostgreSQL sort-order preservation. Text should therefore be encoded as its actual string bytes together with explicit framing. The encoding must not claim to reproduce locale-sensitive PostgreSQL collation order. That distinction becomes important if ordered `BYTEA` row IDs are ever considered later.

For less common PostgreSQL types, the implementation should define an explicit policy. If pg_trickle can obtain a stable binary representation that is guaranteed to be deterministic for the type, that representation may be used. Otherwise, a carefully defined fallback representation can be used, but the fallback must be documented and tested. The encoder must not silently rely on session-sensitive textual formatting.

Type dispatch should be centralized rather than duplicated across call sites.

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

## 8. One Shared Implementation

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

---

## 9. Performance Requirements

Correctness is the reason for the change, but row-ID generation is a hot path and the new implementation should avoid unnecessary overhead. The encoder should preferably stream its output directly into the hash state rather than constructing a complete intermediate buffer for every row.

Tuple metadata and type dispatch should be resolved outside the per-row path wherever possible. If a record type is known for a generated plan, the encoder should cache the necessary type information rather than repeatedly performing catalog or function lookups for every row.

The implementation should also avoid creating `Vec<String>` structures or repeatedly formatting values into SQL text when typed access is available. These changes are useful independently of future integer fast paths and may offset some of the additional framing work introduced by V2.

Performance optimizations should not change the canonical byte representation. Two implementations of V2 must generate exactly the same encoding.

---

## 10. Integer Primary-Key Fast Path

The architecture should make a future direct-integer strategy possible, but that strategy should not be implemented as part of this correctness migration.

A direct `INT4` or `INT8` primary key potentially offers substantial advantages: no hashing, no conversion, and excellent B-tree locality for sequential keys. However, using the raw integer directly introduces additional correctness questions that deserve independent treatment. pg_trickle currently has DVM paths that use special `BIGINT` sentinel values, and every possible `INT8` value is also a legitimate PostgreSQL key value. Those sentinels must be removed or represented differently before a raw `INT8` identity can be guaranteed safe.

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

## 11. Persisted Encoding Version

pg_trickle should record the row-identity encoding version associated with persisted state. The exact catalog representation is an implementation detail, but the system needs enough information to answer:

> Were these stored row IDs generated using the same encoding that the current code will generate?

A simple internal version value such as:

```text
row_identity_version = 2
```

may be sufficient for the initial implementation.

Strategy metadata should only be added where it is actually needed. A single stream table may eventually contain scan identities, group identities, and derived join identities, so the proposal should not assume that one future `row_id_strategy` string on the stream table can describe an entire DVM plan.

Encoding version belongs to persisted compatibility state. Strategy selection belongs to the relevant plan nodes or generated code.

---

## 12. Public Compatibility Contract

`__pgt_row_id` should remain a `BIGINT`, but pg_trickle should explicitly document its value as **opaque implementation state**.

Users may observe the column, use it for diagnostics, or move it through replication, but applications should not assume that the same logical row will retain the same numeric `__pgt_row_id` forever across a reinitialization or an extension upgrade that changes the row-identity encoding version.

The pre-1.0 contract should therefore be:

> The presence and type of `__pgt_row_id` may remain stable, but its numeric value is not a durable business identifier.

This clarification substantially reduces the compatibility burden of future correctness fixes while preserving the practical usefulness of the column.

---

## 13. Migration

V2 will intentionally generate different hashes for many existing identities. Therefore old and new row IDs cannot safely coexist.

The upgrade must not simply install the new encoder and allow the next refresh to continue. Existing stream-table rows could contain V1 identities while new CDC events contain V2 identities, causing updates and deletes to miss their corresponding stored rows.

The migration must perform an explicit transition.

For existing installations, pg_trickle should mark affected stream tables as requiring reinitialization. Existing CDC identity state generated using V1 must not be consumed as V2 state. Stream tables must be rebuilt from an authoritative source snapshot using the new encoding, and downstream stream tables whose identity depends on those tables must be rebuilt in dependency order.

The simplest safe pre-1.0 policy is to treat the V1-to-V2 upgrade as requiring reinitialization of all existing stream tables rather than attempting fine-grained detection of which specific identities happen to be unaffected.

---

## 14. Migration Concurrency and Cutover Safety

Reinitialization must be designed so that writes occurring during the migration are not lost.

A safe implementation should use the same general principle as pg_trickle's existing snapshot/frontier machinery: establish a precise source position, build the new state relative to that position, and then process changes occurring after it. The migration should never depend on a sequence such as "clear the buffer, rebuild the table, then start capturing again," because concurrent source writes could fall into the gap.

The exact implementation should reuse existing CDC transition and frontier mechanisms where possible rather than inventing a separate migration protocol. The required invariant is:

> Every committed source change must be reflected either in the snapshot used for V2 initialization or in CDC state processed after that snapshot, exactly once.

If the current infrastructure cannot guarantee that invariant for an in-place encoding transition, the safer implementation is to temporarily block or quiesce affected refresh/capture operations during the critical cutover transaction.

Migration safety is part of the correctness work and should be tested as such.

---

## 15. Shared Change Buffers

A single source may feed multiple stream tables. Therefore the row-identity representation stored in source CDC state cannot depend on an arbitrary downstream stream-table preference.

V2 should be an extension-wide encoding version for source identity state. Every consumer of a particular source should interpret its CDC identity using the same encoding version.

This is another reason not to introduce a public `row_id_encoding` option now. A per-stream-table option would complicate shared source buffers and make it possible for different consumers to require incompatible upstream representations.

If future benchmarks justify multiple storage strategies, the design should keep the canonical source identity representation independent from the downstream storage strategy.

---

## 16. Testing

The V2 encoder should have direct unit tests for every supported type and every framing invariant. NULL must differ from all non-NULL values. Field order must matter. Field count must matter. Values containing arbitrary text bytes must not affect field boundaries. Negative and boundary numeric values must round-trip deterministically.

Property-based tests should operate on the canonical encoding before hashing. For two different logical key tuples, their V2 encoded byte streams must differ. This is the meaningful collision-free property that pg_trickle can guarantee.

The final 64-bit hash should be tested for determinism and consistency across all execution paths, but the proposal must not claim that a 64-bit hash is mathematically collision-free.

End-to-end tests should cover single and composite primary keys, UUIDs, NULL-containing group keys, keyless/all-column identities, joins, aggregates, PK-changing UPDATEs, trigger CDC, WAL CDC, IMMEDIATE mode, DIFFERENTIAL mode, stream-table DAGs, and V1-to-V2 upgrade/reinitialization.

A particularly important test should generate the same logical change through multiple execution paths and verify that every path computes the same V2 identity.

---

## 17. Benchmarking

Before merging, V2 should be benchmarked against the current encoder using representative identities: a single integer, a UUID, two-column composite keys, larger composite keys, short strings, and long strings.

The benchmark should measure raw encoding throughput, hash throughput, allocations per row, CDC overhead, and end-to-end differential refresh latency.

The acceptance criterion should not require V2 to outperform the existing implementation. It is a correctness fix. However, any significant regression should be investigated and reduced where practical, especially if it comes from avoidable allocations, repeated type lookup, or conversion through SQL text.

The same benchmark harness should later be reused to evaluate a direct integer-primary-key strategy.

---

## 18. Implementation Plan

The work should be implemented in small stages.

**Stage 1: Define the invariant.** Add the V2 encoding specification, version/domain constants, supported type policy, and tests for canonical field encoding.

**Stage 2: Build the shared encoder.** Implement typed field encoding and streaming hash generation in a dedicated row-identity module.

**Stage 3: Centralize call sites.** Replace independent composite-hash construction in CDC, WAL decoding, IMMEDIATE processing, DVM scan generation, joins, aggregates, and other derived identities with calls through the shared abstraction.

**Stage 4: Add version tracking.** Persist enough internal metadata to distinguish V1 and V2 state and prevent mixed-version incremental processing.

**Stage 5: Implement migration/reinitialization.** Add the safe V1-to-V2 cutover path, including dependency-aware rebuilding and concurrent-write tests.

**Stage 6: Document the contract.** State clearly that `__pgt_row_id` remains `BIGINT` but its numeric value is opaque and may change after reinitialization or encoding-version migrations.

**Stage 7: Benchmark.** Record V1 versus V2 performance and retain the harness for later integer-fast-path evaluation.

No direct-integer strategy should be introduced in these stages.

---

## 19. Alternatives Considered

Keeping the current delimiter-based representation would avoid migration work, but it leaves an unnecessary ambiguity in a correctness-critical identifier and makes the row-ID contract harder to defend before 1.0.

Changing directly to ordered `BYTEA` identities could potentially improve index locality for large tables, but it solves a different problem and introduces a substantially larger compatibility surface. The column type, index width, replication format, and downstream expectations would all change.

Introducing raw integer primary keys immediately is attractive from a performance perspective, but doing so during the same migration would combine a correctness change with an optimization and make failures harder to isolate. It also requires sentinel cleanup and additional derived-identity rules.

The recommended approach is therefore intentionally conservative: fix the correctness foundation first and make later optimizations easy rather than attempting to ship all possible strategies simultaneously.

---

## 20. Recommendation

Adopt **Versioned Row Identity Encoding V2** before 1.0.

The implementation should retain `__pgt_row_id BIGINT`, use a deterministic typed and length-framed canonical encoding, centralize row-ID generation across CDC and DVM, introduce explicit encoding versioning and hash-domain separation, and provide a safe reinitialization path for existing installations.

The internal API should be designed around a row-ID strategy abstraction, but V2 hashing should be the only implemented strategy in this proposal.

After this foundation ships and the migration is proven safe, pg_trickle can separately evaluate a direct `INT4`/`INT8` primary-key fast path. At that point the decision can be based on benchmarks rather than architecture pressure, and the existing CDC/DVM code will already be structured to support it cleanly.
