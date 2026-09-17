# Verus adoption plan for pg-trickle

**Status:** Proposed implementation backlog; all tasks are open.  
**Date:** 2026-09-17  
**Repository baseline:** `trickle-labs/pg-trickle`, commit `e482dc4b1331b6de063a2f911a95cc4d9401b795` (`0.107.0`).  
**Suggested repository location:** `plans/quality/PLAN_VERUS_QUALITY_AND_SAFETY.md`  
**Accountable owner:** Maintainer designated at M0. Each milestone also requires an implementation owner and an independent specification reviewer.

> Build a small, production-used verified Rust core. Prove meaningful correctness and safety properties, connect them to the running PostgreSQL extension, and preserve the existing test and release-qualification suites. Do not describe the whole extension as formally verified.

This document is an implementation proposal based on source inspection, not a completed audit. No Verus proofs, builds, or PostgreSQL tests were executed when preparing it. New paths, commands, type names, and proof names below are proposed deliverables unless identified as existing. Recheck call sites against the implementation commit before starting each milestone.

## 1. Decision and intended outcomes

Adopt Verus incrementally, without first splitting the entire extension into separate PostgreSQL and engine crates. Add one small workspace member, `pg_trickle_verified`, and migrate narrowly bounded, safety-critical functions into it. The extension must call those same verified executable functions; a separate mathematical model alone does not qualify as a production improvement.

The first delivery should establish reproducible verification and improve LSN parsing/comparison. Follow with row-identity V2 encoding, graph-plan validation, refresh/frontier protocols, and CDC compaction. Introduce a typed delta-plan representation before attempting broad proofs of SQL differentiation.

The program should produce four measurable outcomes:

1. **Fewer silent wrong-result paths:** specified identity, delta, snapshot, and multiplicity invariants, with regression tests for counterexamples.
2. **Safer failure behavior:** invalid metadata, incomplete capture, overflow, and unsupported verified-path inputs return errors without advancing committed progress.
3. **Production-connected evidence:** every claimed proof names its executable implementation, actual callers, supported domain, and external assumptions.
4. **Maintainable assurance:** CI detects proof bypasses, stale evidence, missing verification coverage, and incompatible changes to persisted formats.

The approach follows Verus's specification-and-implementation model, but the task sequencing and acceptance criteria here are project-specific recommendations. [V1]

## 2. Baseline: what to preserve and where to work

| Existing surface | Observation from the inspected revision | Implementation consequence |
|---|---|---|
| `Cargo.toml` | One workspace member, Rust 2024 edition, pgrx `=0.18.0`. | Start with an isolated crate and prove toolchain compatibility before expanding scope. [R1] |
| `src/version.rs` | Frontier data uses string LSNs; comparison and parsing helpers are centralized. | A small executable proof target with immediate regression opportunities. [R2] |
| `src/dvm/row_id_v2.rs` | V2 defines canonical identity bytes, framing validation, scalar encoders, domain tags, bounded probes, and size limits; the file also contains PostgreSQL adapters. | Extract byte-level code, not the entire pgrx-dependent module. Preserve wire compatibility. [R3] |
| `src/dvm/row_id.rs` | A separate schema-inference and compatibility layer describes operator identities. | Verify semantic identity propagation as well as bytes; identify legacy and V2 call sites separately. [R4] |
| `src/dag.rs` | Graph algorithms and consistency-group types coexist with catalog integration. | Verify a small plan checker first; keep SPI outside the verified crate. [R5] |
| `src/refresh/mod.rs`, `pipeline.rs` | Refresh contexts carry boundaries and policy; `finalize_success` is the shared finalization entry point. Pipeline batches remain in the caller's outer transaction. | Attach protocol obligations to these existing boundaries rather than introducing a second executor. [R6] |
| `src/wal_decoder.rs` | The receipt path uses logical-slot peeking and durable receipt concepts. | Verify the receipt/acknowledgement protocol already being implemented; do not assume the historical consume-before-commit design is still current. [R7] |
| `src/cdc/mod.rs`, `compact.rs` | Compaction implementation lives in `mod.rs`; `compact.rs` contains the result type. | Do not mistake the result enum for the normalization algorithm. Audit generated SQL and its callers. [R8] |
| `src/dvm/operators/join.rs` | Join differentiation has pre/post snapshot branches and correction terms. | Prove actual admitted branches, not only a textbook three-term identity. [R9] |
| `tests/e2e_property_tests.rs` | The reference invariant uses symmetric `EXCEPT ALL` over user-visible results. | Preserve bag semantics and extend this independent SQL oracle. [R10] |
| `.github/workflows/ci.yml` | Existing push/PR path filters enumerate source, SQL, tests, scripts, and Cargo files; they do not currently include the proposed `crates/` and `verification/` trees. | Update existing filters as well as adding the Verus workflow. [R11] |

**Important scope correction:** the V2 identity foundation makes complete canonical bytes the identity, not a hash. This plan does not introduce a collision-freedom assumption for xxHash. Existence of the foundation also does not establish that every producer, consumer, or persisted table already uses V2; M2 must inventory that coverage. [R3]

## 3. What will be proved—and what will not

### 3.1 Main relational specification

Use finite-support signed bags, also called Z-sets:

```text
Bag<Row>   = Row -> mathematical integer, with finite support
Database   = SourceId -> Bag<Row>
D1         = D0 + delta_D
```

Committed source and visible-result bags must have nonnegative multiplicities. Intermediate deltas may contain arbitrary signed integer weights, even when physical change rows encode individual insert/delete actions.

For each admitted deterministic query and valid capture boundary, target:

```text
user_view(apply_delta(storage0, derive(query, D0, delta_D)))
    == eval(query, D1)

provided:
    user_view(storage0) == eval(query, D0)
    auxiliary_state_is_valid(storage0)
    captured_delta_is_exact(D0, D1, delta_D)
    query_and_types_are_admitted(query)
```

`user_view` is deliberate: hidden counts, identity columns, retained zero-count rows, and auxiliary state are not automatically the public SQL result. Prove that representation relationship rather than assuming storage is a plain bag.

This is equivalence at the **declared refresh boundary**, not a claim that a background stream table equals the latest concurrently changing database at every moment.

### 3.2 Separate SQL semantics explicitly

Define `SqlBool = True | False | Unknown`; filtering retains only `True`. Keep predicate equality separate from grouping/deduplication equality, including NULL handling. PostgreSQL distinguishes ordinary comparisons from `IS [NOT] DISTINCT FROM`. [P1]

Initially admit a small, explicit type/expression domain. Model mathematical counts separately from bounded executable counters, with checked conversions. Do not infer IEEE floating-point aggregate correctness from integer algebra. Treat collation, casts, PostgreSQL aggregate result types, volatile functions, and errors as distinct obligations—not convenient assumptions that all expressions are pure total functions.

### 3.3 Evidence labels

Every proof inventory entry must use one of these labels:

| Label | Meaning |
|---|---|
| `planned` | No accepted evidence yet. |
| `model-proved` | A specification-level property is checked; production correspondence is not established. |
| `exec-proved` | The checked executable implementation is used by production callers, under enumerated preconditions. |
| `integrated-qualified` | `exec-proved` plus required PostgreSQL conformance, failure, and release tests pass for the named configuration. This is not an end-to-end proof of PostgreSQL. |

Runtime support and proof coverage are separate. A currently supported query may remain tested but unproved. Discovering an actual correctness defect requires a fix, an explicit error, or a safe fallback; merely being outside the new proof subset does not by itself require disabling existing support.

### 3.4 Trusted boundary

Maintain a reviewed assumption registry covering the verifier/solver/compiler, applicable standard-library contracts, PostgreSQL/pgrx behavior, FFI conversions, SQL rendering, and transaction/capture adapters. For each entry record an ID, exact scope, justification, affected claims, owner, and validation evidence.

Verus offers mechanisms that introduce unchecked assumptions, including `assume`, `external_body`, and external function specifications. Their use must be visible and reviewed. [V3]

Do not introduce blanket axioms such as “the parser is correct,” “all generated SQL implements this plan,” or “the transaction committed successfully.” Document each remaining unproved correspondence separately and narrow it over time. Keep deliberate external contracts in designated modules; keep platform assumptions in the registry even when no Rust wrapper represents them.

Initial exclusions are full PostgreSQL/pgrx memory safety, all SQL features, distributed exactly-once delivery, scheduler liveness, and allocator/OS availability. Bounded arithmetic and bounds-check proofs do not establish that allocation can never fail.

## 4. Architecture and integration contract

### 4.1 Proposed layout

```text
crates/pg_trickle_verified/
  Cargo.toml
  src/
    lib.rs
    lsn.rs
    identity/{mod.rs,wire.rs,scalar.rs,shape.rs}
    graph.rs
    frontier.rs
    refresh_protocol.rs
    cdc.rs
    relational/{mod.rs,bag.rs,sql_value.rs,plan.rs,derive.rs}
    trusted.rs

verification/
  toolchain.toml
  proof-inventory.toml
  assumptions.toml
  fixtures/                  # persisted counterexamples and wire-format vectors
  negative/                  # expected proof failures, never production imports

scripts/verify.sh
scripts/check_verification_policy.py
.github/workflows/verus.yml
docs/verification/{README.md,SCOPE.md,TRUSTED_BOUNDARY.md}
```

Create modules only when their milestones start. Keep the existing extension package and public SQL API. Existing modules become adapters or re-export pure functions; do not create a second manually maintained executable implementation for Verus.

The verified crate must not depend on pgrx, SPI, PostgreSQL pointers, wall-clock access, or environment reads. Pass explicit inputs, validated types, and immutable context. Prefer simple collections and plain error enums where that reduces proof overhead; format database-facing errors in the extension.

### 4.2 Caller obligations are runtime obligations

Verus preconditions do not automatically protect calls from ordinary unverified Rust. Every entry point must either validate its inputs and return `Result`, or accept a type whose private constructor performs that validation. Do not expose a public unchecked constructor that lets the extension bypass a proof's preconditions.

For each migration, record:

```text
existing caller -> validated conversion -> verified exec function -> checked adapter
```

Prove the conversion where feasible; otherwise register and test it. Migration is incomplete while production still calls the old implementation or an optimizer bypasses the checked path.

### 4.3 Build approach

Verus supports per-crate Cargo opt-in with `[package.metadata.verus] verify = true` and a `vstd` dependency. Its documented `cargo verus verify` checks opted-in crates; normal Cargo can also build annotated code. [V2]

M0 must pin a compatible Verus release/commit, its Rust toolchain, solver, and matching dependencies. Do not choose an untested version merely because it is newest. Preserve the extension's build path and demonstrate that its normal compiler builds the same executable source after proof erasure. Do not mix compiled Rust libraries from incompatible toolchains.

## 5. Milestones and dependencies

No calendar promises are attached. Complete the acceptance gate before expanding the corresponding production claim.

| Milestone | Deliverable | Depends on |
|---|---|---|
| M0 | Reproducible toolchain, policy, and CI | None |
| M1 | Production-used checked LSN primitives | M0 |
| M2 | Verified identity V2 byte core and identity contracts | M0; staged by type family |
| M3 | Verified DAG execution-plan checker | M0 |
| M4 | Frontier, refresh, and WAL receipt safety protocol | M1; M3 for graph integration |
| M5 | Consumer-safe CDC normalization/compaction | M2, M4 |
| M6 | Typed delta plans and basic DVM correctness | M2; M4/M5 for integrated capture claims |
| M7 | Stateful SQL operators and snapshot-sensitive joins | M6; staged by operator |
| M8 | Operational safety boundaries and release qualification | M0 onward; gates each shipped slice |

**First production slice:** M0 + M1 + the applicable M8 release gate. Do not wait for the relational proof program to ship the first verified improvement.

## 6. M0 — Establish a trustworthy verification workflow

**Goal:** A clean checkout can verify and normally compile a small production-intended crate without a running PostgreSQL server.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-001 | Assign owners; inventory candidate functions, runtime callers, persisted formats, and security boundaries at the pinned baseline. Create `SCOPE.md` and the proof/assumption registries. | Every initial target has an owner, intended property, and current evidence label. |
| VFY-002 | Add `pg_trickle_verified` as a workspace member. Spike Rust 2024, annotated executable code, `Result`, slices, vectors, checked arithmetic, and required collection support. Keep unsupported convenience derives in adapters. | Clean Verus run and ordinary debug/release builds; compatibility decision recorded. |
| VFY-003 | Pin toolchain and dependency versions with checksums or immutable revisions; implement `scripts/verify.sh`. Record exact commands, features, target, source commit, and verification output. | Reproducible run from an empty verification cache. |
| VFY-004 | Add a required Verus workflow and update existing CI filters for `crates/**`, `verification/**`, proof scripts, and relevant workflow/toolchain files. Preserve existing pgrx and release gates. | A PR changing only verified-crate code triggers both proof and necessary integration checks. |
| VFY-005 | Add bypass-policy checks covering unchecked assumptions, excluded items, proof-skipping flags, verification-only implementation substitutions, and production imports from negative fixtures. Register approved exceptions. | Deliberately introduced bypasses fail CI; a zero-obligation run cannot pass as verified coverage. |
| VFY-006 | Add one expected-proof-failure fixture and one executable mutation that violates a real postcondition. Check the failure is the intended proof failure, not a syntax error or tool crash. | CI passes the original, rejects the mutation, and rejects a wrapper that ignores Verus's exit code. |

**Acceptance gate:** normal builds and Verus both use the intended executable implementation; a false postcondition fails; no production theorem is hidden behind unchecked assumptions; maintainers can reproduce the result. This gate establishes infrastructure, not any database correctness claim.

## 7. M1 — Verify LSN parsing, comparison, and bounded progress helpers

**Targets:** `src/version.rs`, then its frontier, CDC, and scheduler callers. [R2]

Source inspection gives a concrete regression seed: `lsn_gte` currently combines textual equality with strict numeric greater-than. Thus differently formatted equal numeric positions, such as `0/A` and `0/0000000A`, need an equality test. Parsing also maps malformed components to zero. These are helper-level observations; production reachability and impact must be assessed, not assumed.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-010 | Introduce `Lsn(u64)` with checked parsing, explicit accepted grammar, and canonical formatting. Reject malformed input and components wider than 32 bits; preserve valid legacy spellings through a compatibility adapter. | Table-driven compatibility tests plus invalid-input tests; no silent zero fallback in correctness-critical callers. |
| VFY-011 | Prove parse/format round-trip, numeric comparison laws, equality across admitted spellings, and absence of arithmetic/index overflow in parser and formatter. | Production-used executable proofs and boundary tests at `0` and `u64::MAX`. |
| VFY-012 | Replace string comparisons and unsafe defaulting in migrated callers. Distinguish missing/uninitialized frontier entries from a genuine zero LSN. | Caller inventory shows converted paths; malformed catalog metadata yields a clear error without frontier advancement. |
| VFY-013 | Audit `select_canonical_period_secs` and adjacent counters. Specify the below-minimum schedule policy; use checked arithmetic and establish loop progress for every admitted input. | Cases below 96 seconds, boundary powers, and `u64::MAX` terminate with the documented result or error. |
| VFY-014 | Test both legacy JSON compatibility and rejected malformed metadata in PostgreSQL. Preserve existing valid persisted frontiers. | Unit/property suites and targeted integration tests pass; rollout notes identify any deliberate behavior change. |

**Required seed cases:** equivalent casing/zero-padding; missing separator; empty component; invalid hex; extra separator; over-wide high/low halves; minimum/maximum values; absent source versus initialized zero. Mutating numeric `>=` back to textual equality plus `>` must be detected.

**Acceptance gate:** the extension uses the checked primitives in the claimed paths, with verified parser/comparator contracts and error handling. LSNs from unrelated sources or source incarnations are not made comparable merely because both fit in `u64`.

## 8. M2 — Verify canonical row identity, not hash uniqueness

**Targets:** pure functions in `src/dvm/row_id_v2.rs`, `src/dvm/row_id.rs`, and their actual DVM/storage consumers. [R3][R4]

Start with framing, boolean/integer encoding, and escaped byte payloads. Expand PostgreSQL-sensitive scalar canonicalization separately. The existing framer accepts `EncodedField` payloads: make the obligation that these payloads are already well formed explicit, or validate them at construction.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-020 | Inventory V1/V2 producers, merge/lookup consumers, probe-only uses, and persisted identities. Extract pure byte code with unchanged wire output. | Call-site map and golden vectors confirm which paths are actually covered. |
| VFY-021 | Define the V2 wire grammar independently of the encoder. Introduce validated field/identity types with private constructors, including domain/version/type/NULL framing and payload well-formedness. | Invalid variable payloads cannot satisfy a checked constructor merely by supplying a recognized type tag. |
| VFY-022 | Prove encoder/validator agreement, parser consumption of the entire input, domain separation, and unambiguous field boundaries for the admitted subset. Prove a decoding round-trip where a decoder is supplied. | Empty fields, embedded zero bytes, truncation, trailing bytes, unknown tags, and malformed lengths are covered. |
| VFY-023 | Prove output bounds, checked size calculations, and integer encoding injectivity/order preservation within a fixed admitted type. Review every intermediate addition/multiplication—not just the final checked addition. | Maximum field count/byte limit tests and integer-width extremes pass without wrapping or indexing panics. |
| VFY-024 | Specify identity equality relative to the admitted schema and SQL equivalence relation. Add boolean/integer/byte proofs first; queue numeric scale, NaN/signed zero, `bpchar`, collation, and temporal normalization as separate type-family work. | Coverage records distinguish framing validity from complete PostgreSQL equality canonicalization. |
| VFY-025 | Trace bounded-probe consumers; require complete-identity comparison before treating candidates as the same row. Inject deliberately colliding probes through a test seam. | Collisions may worsen performance but cannot merge/delete the wrong identity in the qualified path. |
| VFY-026 | Replace vague schema compatibility claims with identity transfer contracts over admitted operators. Resolve pass-through to the effective upstream identity; distinguish input acceptance from identity reconstruction. | Scan→filter→project and join/group compositions prove the advertised identity relation; mismatched producers are rejected. |

**Semantic target, not a claim about all current types:**

```text
same admitted schema/domain/context:
    encode(a) == encode(b)  <=>  a is identity-equivalent to b
```

For keyless duplicates, the same canonical row value may occur more than once. Identity equality does not prove multiplicity handling; require an explicit count/occurrence representation where the storage path needs it.

**Acceptance gate:** production calls the checked byte functions; the admitted subset has meaningful two-direction equality/round-trip properties; probe collisions are handled correctly; type families not yet proved remain explicitly unproved.

**Compatibility gate:** no silent changes to persisted V2 bytes. If a discovered defect requires different canonical bytes, produce a versioned migration/reinitialization design with dual-version handling where needed, rather than changing an “immutable” encoding in place.

## 9. M3 — Verify DAG execution-plan validation

**Target:** `src/dag.rs` and scheduler/graph-refresh consumers. [R5]

Prefer verifying a small checker for plans produced by existing graph algorithms before proving every optimization in the algorithms themselves. A sound checker can protect production even while the producer remains unverified.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-030 | Define a validated immutable graph input: complete node set, edge direction, duplicate-edge policy, selected refresh closure, and metadata generation. Reject missing required dependencies. | Constructor tests for isolated nodes, duplicates, self-loops, unknown nodes, and stale generations. |
| VFY-031 | Implement and prove a topological-order checker that accepts exactly those candidate orders that are a permutation of required nodes with every dependency before its dependent. | Producer output is checked before use; malformed orders and cyclic graphs cannot be accepted. |
| VFY-032 | Specify atomic consistency-group coverage from graph reachability and configured policy, independent of the producer's claimed diamond list. Validate partitioning, transitive overlap merging, and ordering between groups. | Adversarial omitted-diamond and overlapping-group certificates are rejected. |
| VFY-033 | Connect checker results to actual dispatch. A certificate is valid only for its immutable graph/policy generation; invalidate it on dependency or policy changes. | Tests cannot reuse a valid certificate after graph mutation. |
| VFY-034 | Benchmark checker cost and add explicit size/work limits. If validation cannot complete, return an error or use an independently safe conservative plan; never silently run the unchecked plan. | Recorded graph benchmark results and large/deep graph behavior. |

**Acceptance gate:** production cannot dispatch a claimed validated plan unless the checker accepts the complete graph/policy snapshot. A checker that always rejects is not useful: retain positive tests across the existing supported graph families and specify acceptance of valid orders.

Atomic grouping alone does not establish a common database snapshot. That separate requirement belongs to M4. Initial graph proofs exclude scheduler fairness and fixed-point convergence of cyclic query execution; optional SCC verification is a follow-on, not a prerequisite for this pilot.

## 10. M4 — Verify frontier, refresh, and durable-receipt protocols

**Targets:** `src/version.rs`, refresh context and `finalize_success` in `src/refresh/mod.rs`, `src/refresh/pipeline.rs`, `src/wal_decoder.rs`, and associated scheduler/manual/graph entry points. [R2][R6][R7]

The protocol must distinguish execution success, finalization inside an open transaction, and durable commit. Returning successfully from a refresh or releasing an inner savepoint is not evidence that the caller's outer transaction committed.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-040 | Define typed source identities, source incarnations/epochs, parsed positions, and a `ValidatedBoundary`. Specify missing-source behavior, complete transaction boundaries, and the correspondence between a captured delta and the query's snapshot. | A source/epoch mismatch or incomplete boundary cannot enter the verified refresh transition. |
| VFY-041 | Prove per-source monotone advancement and merge rules under explicit compatibility conditions. Do not treat componentwise maxima as proof of a globally consistent snapshot. Define cleanup eligibility relative to every relevant consumer and durable recovery requirement. | Mixed-boundary and slow-consumer counterexamples fail admission/cleanup checks. |
| VFY-042 | Extract a pure refresh transition kernel with prepared, applied, finalized-pending-commit, committed, and aborted states. Require appropriate evidence before each transition; do not treat unchecked caller booleans as durable-commit evidence. Audit adapter use. | Illegal transitions fail; accepted commit transitions preserve data/frontier/output-metadata correspondence in the model. |
| VFY-043 | Bind ordinary, manual, graph, no-data, FULL, reinitialize, and pipelined paths to the same finalization contract. Handle downstream CDC/output-delta metadata and transactional outbox effects where applicable. | No claimed path advances progress while bypassing required finalization; no-data still validates capture completeness. |
| VFY-044 | Model WAL peek, receipt persistence, committed contiguous receipt prefix, acknowledgement, replay, and deduplication. Map each transition to actual receipt/slot code and transaction boundaries. | Prove acknowledgement never exceeds the committed complete receipt prefix for that source incarnation; replay does not duplicate materialized effects. |
| VFY-045 | Define explicit reset/reinitialize transitions. Monotonicity holds within an epoch; resetting state must change epoch and invalidate stale work rather than being forbidden by an unrealistic universal monotonicity claim. | Restore, slot recreation, reinitialization, and stale-job tests preserve the new epoch's contract. |
| VFY-046 | Add protocol fault injection around every durable boundary and each batch/member. Test caller rollback after successful refresh, not only exceptions inside refresh. | After recovery, state is either the prior committed state or the fully committed new state, with recoverable capture coverage. |

### Required protocol invariants

```text
Within the same source incarnation and normal refresh epoch:
    committed_frontier does not regress

For each acknowledged source transaction:
    complete receipt is already durably represented

On a refresh abort before durable commit:
    no new committed frontier or partial visible result is published

Before capture data is deleted:
    every consumer/recovery obligation is satisfied or safely invalidated
```

Do not infer “no loss” from monotone numbers alone. Prove coverage of the full consumed interval, including completeness and replay identity. The maximum received LSN is not automatically a complete receipt prefix.

### Snapshot and transaction boundary obligations

PostgreSQL Read Committed can use a different snapshot for successive commands. A savepoint gives a rollback boundary, not a new guarantee of a common source snapshot. The adapter must supply the required stable snapshot/boundary relationship through a documented mechanism and tests. [P2]

Likewise, a buffer's `BIGSERIAL change_id` is not by itself a commit-order or gap-free progress certificate. Sequence increments are not rolled back like table writes. Keep physical row identifiers, source commit positions, and consumer cursors distinct. [P2]

PostgreSQL logical decoding can replay changes after a crash; the receipt protocol must therefore account for replay rather than assuming delivery is exactly once. [P3]

**Minimum fault matrix:** abort before receipt commit; crash after receipt commit but before acknowledgement; replay after acknowledgement/restart; failure after delta staging; failure after one pipeline batch; failure in output metadata/finalization; outer transaction rollback after a reported successful refresh; concurrent source commit between snapshot reads; missing required CDC state; graph member failure; source epoch change during queued work.

**Acceptance gate:** the transition kernel is production-used and proved; each relied-upon PostgreSQL/slot operation has a named adapter contract and fault tests. The result is a qualified protocol under those contracts—not a proof of PostgreSQL crash recovery itself. Mixed-source, distributed, and lakehouse paths remain excluded until separately mapped and tested.

## 11. M5 — Verify consumer-safe CDC normalization and compaction

**Targets:** compaction builders and execution in `src/cdc/mod.rs`, callers in refresh paths, and the `CompactionResult` interface. [R8]

A global equality of total deltas is insufficient when consumers have different frontiers. Cancellation must preserve each consumer's observable window, not just the final state after replaying the entire buffer.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-050 | Inventory compaction branches, SQL predicates, locking, source/transaction metadata, and reader windows. Protect reader registration, cuts, and deletion against stale plans. Specify valid traces and TRUNCATE/schema/epoch barriers. | Every compaction rule lists its applicability conditions and protected reader set. |
| VFY-051 | Define normalization over complete logical rows with signed multiplicities. Represent updates as delete-old plus insert-new; keep payload changes even when keys match. | Proofs preserve total bag delta for valid unsegmented batches; key-only cancellation is rejected. |
| VFY-052 | Strengthen the theorem to every active/admitted consumer window. Partition work at protected cuts or compact a private staged batch when shared-buffer rewriting cannot preserve all windows. | A fast-reader/slow-reader counterexample cannot be compacted unsafely; new readers cannot request discarded history without a new baseline. |
| VFY-053 | Implement verified normalization/planning decisions used by the production path. For SQL-executed compaction, map the plan to generated predicates and retain rendering/execution as explicit unproved adapters until justified. | No separate unused reference compactor is counted as `exec-proved` production coverage. |
| VFY-054 | Prove index/weight bounds and logical-row matching. Tie deletion to stable selected records and the validated compaction window, not a hash match alone. | Injected identity collisions and out-of-window records cannot cause extra deletions. |
| VFY-055 | Test skipped, contended, zero-effect, and successful outcomes; validate SQL results against an independently computed multiset/window oracle. | Concurrent refresh/compaction, abort, duplicate rows, and replay cases preserve every required observation. |

### Specifications to implement

```text
Single valid batch:
    delta(normalize(events)) == delta(events)

Shared buffer:
    for every protected consumer window W:
        delta(events_in_window(compacted, W))
            == delta(events_in_window(original, W))
```

The second expression may require a reconstruction/segment mapping rather than retained original event positions; specify that mapping explicitly if the representation changes.

**Required regression examples:** `I(x), D(x)` may cancel only in an admitted window; `D(old), I(new)` with the same key must retain a payload change; key-changing updates must retain both identities; duplicate equal rows preserve multiplicity; NULL values follow the declared row equivalence; TRUNCATE followed by INSERT is not normalized as an ordinary cancellable pair; no compaction crosses a reader's protected cut.

**Acceptance gate:** qualified compaction preserves each protected reader's result, and its physical SQL implementation has conformance evidence. If only the algebraic lemma is proved, label it `model-proved` and do not claim shared-buffer safety.

## 12. M6 — Introduce typed delta plans and prove basic DVM

**Targets:** `src/dvm/diff.rs`, admitted portions of `src/dvm/parser/types.rs`, `src/dvm/snapshot.rs`, basic operator modules, and join SQL generation. [R9][R12]

Do not begin by proving arbitrary SQL strings correct. Add a restricted typed delta-plan representation that production code actually uses. Keep the existing SQL renderer initially, but make its inputs structural enough to inspect and test.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-060 | Define finite-support bag operations and an independent SQL-subset denotation: scan, deterministic filter/project, `UNION ALL`, and inner join. Define valid source states, delta actions, visible results, and error behavior. | Model sanity cases include duplicates, NULL predicates, empty inputs, and simultaneous changes. |
| VFY-061 | Add typed column/alias IDs, schema checks, explicit pre/post snapshot references, source boundary IDs, and identity/multiplicity descriptors to a restricted `DeltaPlan`. Validate conversion from existing `OpTree`. | Unsupported or inconsistent conversions return a reason; aliased self-joins retain distinct scan occurrences. |
| VFY-062 | Prove executable derivation for scan/filter/project/`UNION ALL` over the admitted expression domain, with termination measures for recursive traversal. | Verified production planner outputs match the independent denotation. |
| VFY-063 | Prove inner-join delta identities with explicit snapshot versions. Audit and formalize the actual split insert/delete and correction-term branches; do not accept comments as proof. | Every claimed branch has a proof or a safe non-claimed route; simultaneous-change regressions pass. |
| VFY-064 | Route corresponding SQL generation through the typed plan. Add structural checks for column binding, action sign, complete row identity, and snapshot provenance. | Production cannot substitute an incompatible snapshot or omit a required correction term without rejection/proof failure. |
| VFY-065 | Establish executable-plan-to-SQL conformance tests against real PostgreSQL and the existing user-visible multiset oracle. Record renderer, parser, and database execution as remaining proof boundaries. | Randomized valid plans and adversarial mutations detect wrong signs, wrong aliases, missing columns, and wrong snapshots. |
| VFY-066 | Prove the basic composition theorem for admitted plan trees, including representation invariants and executable bounds. Integrate capture/compaction/boundary contracts only after their relevant gates pass. | The claim states precisely whether it is plan correctness or an integrated refresh result under adapters. |

### Join identities: fix the snapshot convention before proving code

For `L1 = L0 + dL` and `R1 = R0 + dR`, bag algebra gives:

```text
dJ = dL join R0 + L0 join dR + dL join dR
   = dL join R1 + L0 join dR
   = dL join R1 + L1 join dR - dL join dR
```

These equations are mathematical targets derived by bilinear expansion. They are **not interchangeable code templates without tracking snapshots**. In particular, the three-term all-old formula cannot be applied with two current snapshots and an additional positive cross term.

The current implementation has additional action splitting and snapshot/correction choices. Its mapping to these semantics must be proved branch by branch. When a branch needs a stronger input condition, enforce it at admission rather than burying it in an unchecked proof assumption. [R9]

**Required regression cases:** both-side insert; both-side delete; updates changing join keys; deleted old join partners; duplicate join keys; same-table aliases; a filter/project above nested joins; correlated sources sharing an underlying table; NULL join keys; empty/nonempty transitions; multiple changes to one row in the same batch.

**Acceptance gate:** a nontrivial admitted operator subset is planned by the same executable code Verus checks and passes SQL conformance tests. The SQL renderer remains explicitly unproved until a semantics-preserving lowering proof is delivered. String snapshot tests alone are insufficient.

## 13. M7 — Expand stateful operator coverage by family

Do not treat this as one all-or-nothing “prove SQL” task. Each row below is a separate sub-milestone with its own admission rules, production wiring, tests, and coverage label.

| Task | Operator family and proof obligation | Required edge cases |
|---|---|---|
| VFY-070 | `DISTINCT`: threshold changes from count zero to positive and back; auxiliary counts refine visible set membership. | Duplicate inserts/deletes, last-copy deletion, NULL grouping. |
| VFY-071 | `INTERSECT`/`EXCEPT`, with and without `ALL`: maintain both input counts and derive the correct visible multiplicity. | One-sided changes, zero crossings, asymmetric duplicates, retained hidden rows. |
| VFY-072 | `COUNT(*)`, `COUNT(expr)`, exact-domain `SUM`/`AVG`: prove accumulator and non-NULL-count invariants, result types, checked arithmetic, and empty-input behavior. | Group disappearance, all-NULL groups, global empty aggregate, overflow, delete-to-empty then reinsert. |
| VFY-073 | Semi/anti joins: prove existence-count transitions and left-row multiplicity. Treat `NOT IN` separately unless nullability/three-valued-logic conditions justify a rewrite. | Last matching right row removed; duplicate matches; NULL-sensitive predicates. |
| VFY-074 | LEFT/FULL OUTER joins: prove match-count transitions, null-extension/retraction, and identity stability using declared pre/post snapshots. | Simultaneous changes, unmatched→matched→unmatched, changed keys, deleted old partners. |
| VFY-075 | Scoped recomputation and `MIN`/`MAX` rescans: prove affected-scope completeness and exact replacement within scope, leaving other scopes unchanged. | Deleted extremum, moved group key, empty scope, ties, NULL values. |
| VFY-076 | Window/TopK: begin with admitted deterministic orderings and complete affected partitions; specify tie behavior and overflow-safe limit/offset arithmetic. | Ties, NULL ordering, partition moves, offset boundaries, empty partitions. |
| VFY-077 | Recursive/cyclic queries: define admitted monotonicity, finite-state or convergence assumptions, and guardrail behavior before claiming fixed-point correctness. | Deletions, nonconvergence, iteration limit, aborted partial iteration. |

PostgreSQL aggregate semantics distinguish `COUNT(*)`, counts of non-NULL inputs, and aggregates such as `SUM` that return NULL for no input rows. Preserve these distinctions in the model and runtime state. [P4]

For set operations, the mathematical multiplicity targets are:

```text
DISTINCT(x)       = 1 when count(x) > 0, otherwise 0
INTERSECT ALL(x)  = min(left_count(x), right_count(x))
EXCEPT ALL(x)     = max(left_count(x) - right_count(x), 0)
```

Treat ordinary `INTERSECT`/`EXCEPT` as separate Boolean-membership formulas. Test the actual user-facing relation, not merely internal rows filtered in a way that assumes the implementation's intended multiplicity behavior.

**Per-family acceptance gate:** independent denotation, proved executable transformation/state update, validated caller preconditions, production connection, regression/mutation tests, SQL conformance, and explicit exclusions. Float aggregates, custom collations, arbitrary user functions, and nondeterministic orderings are not included by implication.

## 14. M8 — Apply operational safety contracts and release each slice

This milestone runs alongside the functional work. It does not require completing every M7 family.

| Task | Concrete implementation | Completion evidence |
|---|---|---|
| VFY-080 | Extract/prove selected resource admission and checked-counter logic: batch size/byte bounds in `pipeline.rs`, DVM depth/CTE budgeting, and epoch progression. Specify oversized-single-row behavior. | Boundary tests; overflow is an explicit error, and proof failures cannot be dismissed by widening input assumptions silently. |
| VFY-081 | Audit the small FFI wrappers feeding verified inputs. Document pointer lifetime, tuple-descriptor compatibility, ownership, error/unwind behavior, and validation responsibilities. Keep unsafe inventory/review controls. | A reviewed adapter contract for each qualified entry point; no claim that the whole FFI is proved. |
| VFY-082 | Where role/security context participates in a qualified refresh, verify pure transition/admission decisions and test restoration on success, PostgreSQL error, nested execution, and cancellation. | Definition-derived SQL does not gain privileges from a proof refactor; RLS/search-path behavior is preserved in integration tests. |
| VFY-083 | Add proof evidence to release qualification alongside current tests. Bind claims to source revision, feature/target set, toolchain/dependencies, assumption registry, and SQL/conformance results. | Stale or manually asserted passing evidence is rejected by the release gate. |
| VFY-084 | Benchmark each migrated runtime path against baseline; publish measured allocation/time changes and agreed budgets. Limit costly validation through safe admission, not bypasses. | No unexplained regression; any budget exception is reviewed and documented. |
| VFY-085 | Publish a scope table by function/operator/type/capture mode. Train maintainers to review specifications and proof changes, including agent-generated patches. | Another maintainer reproduces a proof, diagnoses an intentional failure, and traces a production call to it. |

**Acceptance gate for every shipped slice:** the relevant proof and integration gates pass for the release source; unresolved limitations are public in the scope document; runtime behavior and migrations are reviewed; no release note inflates a subset proof into whole-extension verification.

## 15. CI, testing, and evidence contract

### 15.1 Required checks

Run these categories as complementary evidence, not substitutes:

| Check | Purpose | Required behavior |
|---|---|---|
| Clean pinned Verus verification | Establish declared proof obligations for claimed executable code. | Nonzero exit, timeout, skipped claimed module, or absent expected obligations fails. |
| Ordinary debug/release compilation and unit tests | Verify production build compatibility and executable behavior after proof erasure. | Use release-relevant features and targets; do not verify only `cfg(test)` substitutes. |
| Proof-policy and inventory validation | Detect bypasses, unreviewed assumptions, stale claim/caller mappings, and missing sources. | Fail closed; use syntax-aware inspection where feasible, not only grep. |
| Negative/mutation fixtures | Test that the proof/checker catches a deliberate semantic defect. | Confirm the intended rejection reason; a compiler error is not proof sensitivity evidence. |
| PostgreSQL conformance | Exercise admitted SQL, identity, and adapter behavior against the database. | Preserve the independent user-visible bag oracle and capture failure seeds. |
| Protocol fault/concurrency tests | Challenge runtime contracts outside Verus. | Inspect committed/recovered state and all consumer progress, not only return values. |
| Release/performance qualification | Bind evidence to the shipped artifact and measure costs. | Reject stale evidence; record budgets and reviewed exceptions. |

For the initial small crate, prefer full proof verification on every PR over complicated selective verification. Expand caching only after its source/configuration keys are trustworthy. A path-only workflow skip must not leave a required check hanging; use an always-reporting gate or run the inexpensive gate unconditionally.

### 15.2 Proposed contributor commands

The first three commands are the intended workflow **after M0 creates and configures the crate**. They are not asserted to run in today's unchanged repository.

```bash
./scripts/verify.sh
cargo verus verify -p pg_trickle_verified --locked
cargo test -p pg_trickle_verified --locked
cargo build -p pg_trickle_verified --locked --release

# Existing repository unit-test entry point; retain its pgrx prerequisites.
./scripts/run_unit_tests.sh pg18
```

`verify.sh` must run the policy/inventory checks and capture provenance, not simply return success when a tool is absent. Restrict proof-skipping developer conveniences to local workflows; release evidence requires the full selected proof set. The documented Cargo commands should be checked against the pinned toolchain during M0. [V2][R11]

### 15.3 Example proposed inventory entry

```toml
[[claims]]
id = "LSN-COMPARISON-001"
status = "planned"
module = "crates/pg_trickle_verified/src/lsn.rs"
property = "Checked LSN comparison agrees with the numeric value of every admitted spelling"
production_callers = ["src/version.rs::lsn_gt", "src/version.rs::lsn_gte"]
supported_domain = "Validated LSN values in the same source incarnation"
required_evidence = ["exec-proof", "ordinary-build", "unit-properties", "adapter-tests"]
```

Extend the schema with actual proof symbols, input validators, assumption IDs, evidence digests, type/operator coverage, and exclusions as implementation proceeds. Generate passing status from CI artifacts; do not turn `planned` into `integrated-qualified` by editing a checked-in Boolean.

### 15.4 Definition of done for an individual task

A task is complete when its implementation and independently reviewed specification are merged; expected positive and negative cases are covered; the proof applies to the production-used executable path; input preconditions are discharged or checked; all remaining assumptions are registered; required PostgreSQL/regression tests pass; persisted-format and performance effects are reviewed; and evidence matches the merged source/configuration.

For a model-only task, completion is allowed at `model-proved`, but dependent production milestones cannot treat that as executable coverage.

## 16. Rollout, stop conditions, and review discipline

### 16.1 Deliver small, reviewable changes

Use separate PRs for behavior-preserving extraction, specification/proof introduction, and behavior fixes where practical. Preserve minimized counterexamples as ordinary regression tests even after proving the corrected code. Keep deployed identity/frontier formats unchanged unless a reviewed migration explicitly changes them.

The first PR sequence should be:

| PR | Scope | Must not include |
|---|---|---|
| PR 1 | VFY-001–006: isolated crate, pins, CI, policy, negative fixture. | A broad engine rewrite or claims about unverified algorithms. |
| PR 2 | VFY-010–011: checked LSN implementation, proofs, compatibility tests. | Silent redefinition of malformed-input behavior across all callers. |
| PR 3 | VFY-012–014: selected production caller migration, bounded-helper hardening, integration qualification. | Proof preconditions enforced only by comments. |
| PR 4 | VFY-020–022: V2 extraction and first wire-subset proofs, with golden vectors. | Wire-format changes or assumptions that all scalar types are already proved. |
| PR 5 | VFY-030–031: topological-plan checker and production dispatch integration. | Replacing all graph algorithms before validating the checker approach. |

After these PRs, use evidence from the actual code/proof maintenance work to choose the next M2, M3, and M4 slices. This is an execution sequence, not a promise that each row fits a fixed effort estimate.

### 16.2 Stop or narrow a claim when necessary

| Condition | Required response |
|---|---|
| Proof succeeds only after assuming the desired conclusion. | Reject the proof; repair the specification/implementation or mark the boundary unproved. |
| A precondition excludes a reachable production case. | Add a runtime validator, broaden the proof, or use a documented safe error/fallback. |
| SQL renderer or database adapter is not proved. | Retain conformance/fault tests and explicitly conditional claims; do not label it proved by association. |
| New code is correct only when hashes do not collide. | Redesign identity comparison or narrow deployment; do not add a collision-freedom axiom. |
| Toolchain incompatibility blocks a larger migration. | Keep the verified crate small; resolve the build boundary without duplicating executable algorithms. |
| A checker is too expensive. | Optimize and re-prove, use a safe bounded policy, or return a clear error; never silently skip it. |
| A test exposes a real unsupported/incorrect delta path. | Fix, reject, or use an allowed protected FULL/reinitialize path. Respect existing FULL-forbidden graph policy. |
| A wire-format correction changes existing identities. | Require versioned migration/reinitialization and compatibility evidence before rollout. |

### 16.3 Specification review, including agent-generated work

Require an independent reviewer for the intended property and its assumptions, not just for proof syntax. Ask whether an always-error implementation satisfies a weak specification, whether valid examples are admitted, whether preconditions can be met by real callers, whether bag multiplicity and SQL NULL behavior survive the abstraction, and whether the code being proved is actually executed.

Coding agents may assist with lemmas, loop invariants, refactors, and counterexample tests. They must not autonomously weaken postconditions, strengthen caller preconditions, add unchecked assumptions, suppress verification, or redefine the expected SQL result merely to make CI pass.

## 17. Success criteria and permitted release wording

Track closed production invariants, production entry-point coverage, admitted operator/type families, remaining assumption scope, reproducible counterexamples, proof runtime, and measured runtime cost. Lines of proof and raw “functions verified” counts are diagnostic data, not the program's success metric.

A suitable early release statement is:

> The checked LSN parsing and comparison implementation used by the listed refresh/frontier paths is verified with Verus against numeric specifications. PostgreSQL adapter behavior is covered by the named integration tests. Other engine components are outside this proof scope.

A later statement may name identity framing, graph-plan validation, delta-plan families, or protocol invariants once their individual gates pass. Always distinguish proven Rust behavior from empirically qualified PostgreSQL integration.

**Program completion is incremental:** shipping M1 with truthful evidence is a real improvement. Larger relational guarantees become available only as the relevant identity, capture, snapshot, planner, renderer, and application boundaries are connected—not because the workspace has a green Verus badge.

## References

Repository references below are pinned to the inspected commit. They establish the baseline, not successful completion of this proposal. External documentation was consulted on 2026-09-17.

- **[R1]** [Repository Cargo manifest](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/Cargo.toml).
- **[R2]** [Frontier and LSN implementation](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/version.rs).
- **[R3]** [Typed row-identity V2 foundation](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/dvm/row_id_v2.rs).
- **[R4]** [Row-identity schema inference](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/dvm/row_id.rs).
- **[R5]** [Dependency graph implementation](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/dag.rs).
- **[R6]** [Refresh contexts and finalization](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/refresh/mod.rs); [bounded refresh pipeline](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/refresh/pipeline.rs).
- **[R7]** [WAL decoder and receipt path](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/wal_decoder.rs).
- **[R8]** [CDC implementation and compaction](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/cdc/mod.rs); [compaction result type](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/cdc/compact.rs).
- **[R9]** [Inner-join differentiation and snapshot branches](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/dvm/operators/join.rs).
- **[R10]** [E2E property tests and user-visible multiset invariant](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/tests/e2e_property_tests.rs).
- **[R11]** [Current CI workflow and filters](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/.github/workflows/ci.yml).
- **[R12]** [DVM differentiation entry points](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/dvm/diff.rs); [snapshot planning](https://github.com/trickle-labs/pg-trickle/blob/e482dc4b1331b6de063a2f911a95cc4d9401b795/src/dvm/snapshot.rs).
- **[V1]** Amazon Science, [Developing provably correct Rust code with Verus](https://www.amazon.science/blog/developing-provably-correct-rust-code-with-verus), 2026-08-31.
- **[V2]** Verus official guide, [Using Verus via Cargo](https://verus-lang.github.io/verus/guide/cargo_verus.html).
- **[V3]** Verus official guide, [Assumptions and trusted components](https://verus-lang.github.io/verus/guide/tcb.html).
- **[P1]** PostgreSQL 18, [Comparison Functions and Operators](https://www.postgresql.org/docs/18/functions-comparison.html).
- **[P2]** PostgreSQL 18, [Transaction Isolation](https://www.postgresql.org/docs/18/transaction-iso.html).
- **[P3]** PostgreSQL 18, [Logical Decoding Concepts](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html).
- **[P4]** PostgreSQL 18, [Aggregate Functions](https://www.postgresql.org/docs/18/functions-aggregate.html).
