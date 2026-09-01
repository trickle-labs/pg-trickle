# Proposal: V1 Composable Refresh and Durable Delta Contracts for `pg_trickle`

## Strict transactional graph refresh and durable typed output deltas for coordinating PostgreSQL extensions

**Status:** Proposed  
**Scope:** V1 only. Appendix A is non-normative and does not affect V1 acceptance.  
**Target:** Additive pre-1.0 integration contract  
**Decision:** Add a small, versioned SQL surface that lets another PostgreSQL extension coordinate a private stream-table graph without depending on `pg_trickle` internals  
**Compatibility:** Existing stream tables remain scheduler-managed by default, and existing public APIs retain their behavior  
**Motivating consumer:** [`pg_mdm`](https://github.com/grove/pg-mdm), beginning with its V1 entity-resolution design  
**Baseline reviewed:** `pg_trickle` 0.90.0 at commit [`a6a53e0`](https://github.com/trickle-labs/pg-trickle/commit/a6a53e0f6697eed4407a664b1af5172ca137fd9c)  
**Last updated:** 2026-09-01

---

## Contents

1. [Executive decision](#1-executive-decision)
2. [V1 release boundary](#2-v1-release-boundary)
3. [Motivation](#3-motivation)
4. [Goals and non-goals](#4-goals-and-non-goals)
5. [Relationship to `pg_mdm`](#5-relationship-to-pg_mdm)
6. [Existing `pg_trickle` foundations](#6-existing-pg_trickle-foundations)
7. [Terminology](#7-terminology)
8. [Required invariants](#8-required-invariants)
9. [Proposed V1 surface](#9-proposed-v1-surface)
10. [Capability discovery](#10-capability-discovery)
11. [Durable external orchestration](#11-durable-external-orchestration)
12. [Canonical stream-table and graph contracts](#12-canonical-stream-table-and-graph-contracts)
13. [Strict transactional graph refresh](#13-strict-transactional-graph-refresh)
14. [Source-boundary manifest](#14-source-boundary-manifest)
15. [Durable output-delta consumers](#15-durable-output-delta-consumers)
16. [V1 end-to-end protocol](#16-v1-end-to-end-protocol)
17. [Security and authorization](#17-security-and-authorization)
18. [Concurrency and transaction semantics](#18-concurrency-and-transaction-semantics)
19. [Failure and recovery behavior](#19-failure-and-recovery-behavior)
20. [Stable errors](#20-stable-errors)
21. [Suggested catalog and storage changes](#21-suggested-catalog-and-storage-changes)
22. [Integration with the refresh engine](#22-integration-with-the-refresh-engine)
23. [Lifecycle interactions](#23-lifecycle-interactions)
24. [Observability and administration](#24-observability-and-administration)
25. [Performance and resource behavior](#25-performance-and-resource-behavior)
26. [Upgrade and compatibility policy](#26-upgrade-and-compatibility-policy)
27. [Delivery plan](#27-delivery-plan)
28. [Test plan](#28-test-plan)
29. [Alternatives considered](#29-alternatives-considered)
30. [Risks and open implementation choices](#30-risks-and-open-implementation-choices)
31. [Acceptance criteria](#31-acceptance-criteria)
32. [Recommended disposition](#32-recommended-disposition)
33. [Appendix A: high-level V2 needs](#appendix-a-high-level-v2-needs)

---

## 1. Executive decision

`pg_trickle` should expose a generic composition contract for trusted PostgreSQL extensions that use stream tables as a relational computation layer but must publish a larger domain result under their own transaction and governance rules. The first consumer is `pg_mdm`, which wants `pg_trickle` to maintain normalized values, candidate blocks, candidate pairs, pair evidence, and golden-value candidates while `pg_mdm` remains responsible for clustering, stewardship, stable entity identifiers, reviews, provenance, and public MDM publication. The contract is intentionally domain-neutral and should also be usable by graph resolvers, policy engines, feature stores, search-index coordinators, and other extensions that need incrementally maintained relational facts without surrendering their own publication boundary.

The V1 contract contains six closely related capabilities. `pg_trickle` advertises integration capabilities and their versions; exposes canonical stream-table and graph contracts with stable digests; provides a durable `EXTERNAL` orchestration mode that excludes private graphs from the ordinary scheduler; refreshes a complete graph synchronously and strictly inside the caller's PostgreSQL transaction; returns one coherent source-boundary manifest for that refresh; and exposes durable, acknowledged, typed output deltas for selected terminal stream tables. Together, these capabilities let another extension refresh relational evidence, consume exactly what changed, update its own durable state, acknowledge the evidence, and commit or roll back all effects together.

This proposal does not add MDM concepts to `pg_trickle`, and it does not expose private `pg_trickle` implementation state. `pg_trickle` continues to own source capture, safe frontiers, differential view maintenance, full fallback, stream-table storage, row identity, dependency ordering, and refresh finalization. The coordinating extension continues to own domain-specific decisions and public outputs. No supported integration path reads or mutates private catalogs, scheduler jobs, change buffers, generated columns, internal frontier encodings, or storage naming conventions.

---

## 2. V1 release boundary

This document is a V1 proposal. Its acceptance boundary is a synchronous, single-database composition path in which the coordinating extension keeps one PostgreSQL transaction open while `pg_trickle` refreshes the private graph and the coordinator publishes its own result. V1 supports acyclic local stream-table graphs, complete source-boundary proofs for the source kinds accepted by the graph, exact output deltas when available, explicit full invalidation when an exact output delta is unavailable, owner-controlled durable cursors, and fail-closed behavior under concurrency, recovery, schema change, or retention loss.

The following capabilities are required for V1 and form one compatibility promise:

| Capability | V1 purpose |
|---|---|
| Versioned capability discovery | Refuse unsupported extension combinations before state is created or refreshed |
| Canonical stream-table and graph contracts | Prove that the graph being refreshed is the graph the coordinator compiled and approved |
| Durable `EXTERNAL` orchestration | Prevent independent scheduler advancement of a privately coordinated graph |
| Strict transactional graph refresh | Refresh the complete graph in dependency order with no notice-and-skip outcome |
| Source-boundary manifest | Name the exact, coherent source positions represented by the refreshed graph |
| Durable typed output deltas | Let a coordinator consume and acknowledge changed terminal rows without private-buffer access |

V1 does not include multi-transaction graph computation, resumable graph refresh, concurrent immutable graph states, background graph execution, cross-database graphs, distributed transaction coordination, arbitrary post-refresh callbacks, semantic business events, a stable Rust ABI, or external message delivery. Appendix A describes the V2 multi-transaction need at a high level so that V1 choices do not accidentally block it, but none of that appendix is normative and none of it may delay V1 acceptance.

---

## 3. Motivation

The current `pg_trickle` public API is designed primarily for users who create a stream table and allow `pg_trickle` to keep it fresh. That remains the correct ordinary product. The API is not quite sufficient when another extension creates several private stream tables as one internal graph and must coordinate that graph with additional domain state in the same transaction.

The existing [`refresh_stream_table()`](../src/api/refresh_ops.rs) function refreshes one stream table and returns `void`. A concurrent refresh becomes a `RefreshSkipped` condition that the human-facing wrapper turns into a notice and a successful no-op. That behavior is convenient for an operator issuing an opportunistic refresh, but it is unsafe for a coordinating extension. Such a caller must distinguish “the complete graph was refreshed at this exact boundary” from “another operation was busy and nothing happened,” and it needs a stable group identifier, actual graph digest, source-boundary manifest, per-node result, and terminal delta positions rather than reconstructing those facts from private tables.

The existing [`pause_scheduler()`](../src/api/scheduler_control.rs) function is also not a durable orchestration contract. It is an operational drain control whose state and purpose are separate from stream-table semantic ownership. A graph coordinated by another extension must remain outside scheduler dispatch after process restart, scheduler restart, failover, backup, restore, and extension upgrade. An accidental independent refresh is not merely a freshness difference: it can separate relational evidence from the higher-level membership, policy, or publication revision that is supposed to consume it.

The existing [`stream_table_spec()`](../src/api/spec.rs) function provides useful tooling metadata, but its present projection is not a complete semantic compatibility artifact. A coordinator must pin every result-affecting property, including defining-query semantics, output schema, owner execution identity, defining search path, dependency shape, source bindings, function dependencies, collations, row-identity and probe versions, and versioned semantic plans. It needs a canonical digest that changes when meaning changes but remains stable when only indexes, statistics, worker counts, or other physical choices change.

Finally, `pg_trickle` already computes logical output changes internally for stream-table-to-stream-table dependencies. The current architecture captures differential output and can compute a before-and-after logical difference for supported full paths so downstream stream tables remain incremental. Those internal buffers are deliberately private. A coordinating extension needs the same logical information through a supported contract: complete old and new rows grouped into immutable batches, a durable consumer cursor, transactional acknowledgement, and an explicit `FULL_INVALIDATION` marker when an exact delta is not available.

The missing feature is therefore not another dataflow engine. It is a narrow, stable composition boundary around machinery that already exists inside `pg_trickle`.

---

## 4. Goals and non-goals

### 4.1 Goals

The contract should let a trusted coordinating extension treat a `pg_trickle` graph as a versioned incremental component. A successful operation must answer four questions precisely: which graph was executed, which source state it represents, what each graph node did, and which terminal output changes remain for the coordinator to consume. The coordinator must then be able to write its own state and acknowledge those changes in the same PostgreSQL transaction.

The contract must preserve encapsulation. Public consumers should not know whether source changes came from statement triggers, row triggers, WAL decoding, snapshot comparison, or a later capture adapter. They should not know whether output deltas are stored in a dedicated heap, a partitioned relation, or a future log representation. They should observe versioned contracts, a strict graph result, an opaque but complete boundary manifest, a typed logical delta relation, and durable acknowledgements.

The contract must fail closed. A busy graph, stale contract digest, incomplete source boundary, missing output history, incompatible output schema, unknown row-identity version, authorization failure, or invalid acknowledgement must raise a stable error before the coordinator can mistake incomplete work for a valid refresh. Operational limits may block or reject work, but they may not silently skip evidence or advance a consumer cursor.

The contract should remain useful beyond `pg_mdm`. Its nouns are stream table, graph, contract, boundary, output delta, batch, and consumer. It does not mention entities, matches, golden records, clusters, or any other domain-specific concept.

### 4.2 Non-goals

V1 does not allow a caller to construct, edit, advance, or repair a source frontier. `pg_trickle` remains the sole authority for source visibility and completeness. The caller receives a source-boundary manifest as evidence of what was consumed, but cannot feed arbitrary positions back as refresh instructions.

V1 does not expose private change-buffer relations. The implementation may reuse them, replace them, partition them, compact them, or introduce a different representation. Only the logical batch, typed row relation, acknowledgement, and gap behavior are public.

V1 does not execute arbitrary extension callbacks from the refresh engine. The coordinator invokes `pg_trickle` through SQL and remains responsible for its subsequent domain work. This avoids an open-ended callback ABI, recursion policy, security boundary, and failure-isolation problem.

V1 does not publish domain events, deliver messages to external systems, replace PostgreSQL logical replication, or provide a Kafka, NATS, HTTP, or outbox transport. A coordinating extension may publish its completed domain result through another mechanism after its local transaction commits.

V1 does not support circular integration graphs, cross-database graphs, distributed transaction coordination, or multi-transaction graph execution. Those capabilities require separate semantics and proofs rather than hidden expansion of this contract.

---

## 5. Relationship to `pg_mdm`

The motivating architecture has a simple responsibility boundary:

> **`pg_trickle` maintains changing relational facts; `pg_mdm` decides identity.**

A `pg_mdm` definition compiles into private stream tables that project source records, normalize fields, create bounded candidate blocks, produce canonical candidate pairs, compare those pairs, maintain structured pair evidence, and maintain golden-value candidates. `pg_mdm` then consumes changes from the terminal evidence relations, computes the complete affected identity closure, applies manual decisions and component rules, reconciles durable `mdm_id` values, selects golden values, creates review items, and publishes its own output tables.

The V1 dependency maps directly to this proposal:

| `pg_mdm` requirement | `pg_trickle` contract |
|---|---|
| Verify that the installed substrate supports the required behavior | Capability discovery |
| Pin the private evidence graph to the compiled entity definition | Stream-table and graph contracts |
| Prevent evidence from advancing independently of MDM publication | `EXTERNAL` orchestration |
| Refresh the complete evidence graph inside `mdm.refresh()` | Strict transactional graph refresh |
| Explain which source state the publication represents | Source-boundary manifest |
| Recompute only affected identity components when safe | Durable typed output deltas |
| Broaden safely when exact incremental impact is unavailable | `FULL_INVALIDATION` |
| Roll everything back if MDM validation or publication fails | Shared outer transaction and transactional acknowledgement |

Nothing in the SQL surface assumes that `pg_mdm` is installed. `pg_mdm` is a conformance consumer and a useful end-to-end test, not a dependency of `pg_trickle`.

---

## 6. Existing `pg_trickle` foundations

This proposal is feasible because most of the difficult engine behavior already exists. `pg_trickle` maintains stream tables from declarative SQL, captures source changes through trigger and WAL paths, computes differential or full results, orders dependent stream tables through a graph, retains version frontiers, and finalizes output, frontiers, downstream change capture, cleanup, and refresh history transactionally. The current architecture also executes defining SQL under the stored stream-table owner and search-path contract, which is essential when a privileged coordinator invokes the engine.

Stream-table-to-stream-table propagation already creates logical delete and insert rows for downstream consumers. A differential upstream refresh can expose its computed delta, while a supported full upstream refresh can compare old and new contents before propagating a minimal downstream difference. The V1 output-delta contract should reuse that logical algebra, but it must not make the current private buffer schema or naming convention public.

The versioned row-identity work also provides a strong foundation. A public output delta needs an opaque identity that is exact, deterministic, and consistent across full and differential paths. The integration contract should expose the active row-identity version and complete identity bytes for comparison and ordering while leaving parsing optional and separately versioned.

The main implementation work is therefore orchestration and stabilization: expose canonical contracts, add durable external ownership, refactor graph refresh into a strict structured path, and create a supported durable cursor over logical output changes.

---

## 7. Terminology

A **coordinating extension** is a trusted PostgreSQL extension or database component that owns or has owner-equivalent authority over a set of stream tables and invokes this contract through SQL. PostgreSQL roles, object ownership, grants, and extension membership remain the security boundary; the contract does not attempt to authenticate a shared-library filename.

A **stream-table contract** is the canonical, versioned description of one stream table's result-affecting semantics and execution identity. It is distinct from operational status, recent timings, planner statistics, scheduler tier, physical indexes, and other tuning state.

A **refresh graph** is the transitive closure of stream-table dependencies reachable upstream from one or more named root stream tables. Base tables, materialized views, foreign or polled relations, and other non-stream sources appear in the graph contract as sources, but they are not refreshable graph members.

A **graph contract** is the canonical description and digest of the roots, member stream tables, dependency edges, deterministic topological order, output schemas, semantic versions, execution identities, and external source bindings that form one refresh graph.

A **source-boundary manifest** is an immutable, machine-readable statement of the lower and upper positions, snapshot tokens, upstream stream-table revisions, completeness classes, and source-contract digests consumed by one graph refresh. Position payloads are versioned and may remain opaque to callers.

A **strict graph refresh** is one synchronous execution of a refresh graph against one immutable source-boundary decision. It may use differential, full, scoped-recomputation, reinitialization, or no-data paths per member, but every member result belongs to the same graph refresh and outer transaction.

An **output-delta batch** is the logical change in one stream table's visible contents produced by one committed refresh. An exact batch contains complete deleted rows and inserted rows. An update is represented as deletion of the complete old row followed by insertion of the complete new row. A full-invalidation batch states that the consumer must read the complete current stream table because an exact logical delta is unavailable.

An **output-delta consumer** is a durable, role-owned cursor over the output-delta batches of one stream table. Reading is side-effect free. Acknowledgement is explicit, monotonic, validated, and transactional.

---

## 8. Required invariants

The V1 contract is governed by the following invariants. These are stronger than any suggested catalog layout, function spelling, or Rust module structure.

1. **One outer transaction.** `refresh_graph_strict()` performs no internal commit. Graph contents, node frontiers, refresh history, output-delta batches, the coordinator's own writes, and consumer acknowledgements commit or roll back with the caller's PostgreSQL transaction.

2. **Graph-wide validation before mutation.** The complete stream-table closure is resolved, authorized, locked, reloaded, and compared with the expected graph digest before any member is refreshed.

3. **One immutable source-boundary decision.** Every graph member receives positions derived from the same graph-refresh context. No node may independently reread and substitute a newer upper boundary.

4. **Strict concurrency.** A competing refresh, query alteration, storage recreation, repair, or drop operation on any graph member produces an error or waits according to ordinary PostgreSQL lock policy. It is never converted into a successful skipped refresh.

5. **Semantic contract pinning.** A refresh result names the graph digest and each member contract digest actually used. Unknown contract, semantic-plan, output-schema, row-identity, row-probe, or source-boundary versions fail closed.

6. **Exact delta or explicit invalidation.** A consumer receives either the complete logical output difference for a batch or an explicit `FULL_INVALIDATION` marker. Missing rows, truncated rows, expired rows, and fallback paths never masquerade as an empty exact batch.

7. **Transactional acknowledgement.** A delta cursor advances only when `ack_output_delta()` commits. If the coordinator later raises an error, both its domain publication and the acknowledgement roll back.

8. **Retention follows the slowest consumer.** Exact delta rows remain available until every active external consumer has acknowledged them or has been explicitly moved to a resynchronization-required state. Resource pressure cannot silently advance a cursor.

9. **External means externally orchestrated.** The ordinary scheduler does not queue, inline, fuse, or execute a stream table whose orchestration mode is `EXTERNAL`. This remains true across process restart and database recovery.

10. **Owner-equivalent execution.** Defining SQL continues to execute under the stored stream-table owner and defining search-path contract. The integration API grants no authority over source data, current output, or retained deleted rows beyond the authority explicitly checked for the caller.

11. **Public contract, private implementation.** A caller may use only the documented capability, contract, refresh, boundary, consumer, batch, and acknowledgement APIs, together with the generated read-only delta relation returned by registration. It may not derive or depend on private relation names, internal catalog identifiers, LSN placeholders, frontier JSON, or change-buffer schemas.

12. **No silent recovery.** If crash recovery, backup restore, PITR, manual DDL, storage loss, or an unsupported upgrade makes exact output history unverifiable, the consumer becomes `RESNAPSHOT_REQUIRED`. Repair may reconstruct infrastructure, but it may not invent a continuous exact history that no longer exists.

13. **Full and differential equivalence.** For the same starting output, defining contracts, source boundary, and row-identity versions, any exact output delta must transform the prior stream-table multiset into the same result as complete evaluation of the defining query at that boundary.

---

## 9. Proposed V1 surface

The exact SQL spelling may change during implementation review, but the following behavior is the proposal. Once capability major version 1 is declared stable, changing these semantics requires a new capability version rather than an undocumented function change.

| Extension point | Purpose |
|---|---|
| `pgtrickle.integration_capabilities()` | Discover independently versioned integration contracts |
| `pgtrickle.stream_table_contract()` | Return the canonical contract and digest for one stream table |
| `pgtrickle.graph_contract()` | Return the canonical closure and digest for one refresh graph |
| `pgtrickle.set_orchestration_mode()` | Persist `MANAGED` or `EXTERNAL` ownership of refresh scheduling |
| `pgtrickle.refresh_graph_strict()` | Refresh a complete external graph synchronously and transactionally |
| `pgtrickle.register_output_delta_consumer()` | Register a durable cursor over one stream table's logical output changes |
| `pgtrickle.output_delta_batches()` | Inspect the continuous batch sequence after the acknowledged cursor |
| Returned typed delta relation | Read complete old and new rows for exact batches |
| `pgtrickle.ack_output_delta()` | Acknowledge an exact or resynchronized range transactionally |
| `pgtrickle.output_delta_consumer_status()` | Inspect lag, retention, contract, and recovery state |
| Resnapshot helpers | Establish a new full baseline under a transaction-scoped output lock |

---

## 10. Capability discovery

A coordinating extension should negotiate behavior by capability rather than parsing the `pg_trickle` package version. Release versions remain useful for support, but one release may add several independent contracts, a distribution may backport one capability, and a later release may preserve an older contract alongside a newer one.

The proposed API is:

```sql
SELECT *
FROM pgtrickle.integration_capabilities();
```

An illustrative result is:

```text
capability                       major_version  minor_version  enabled  details
-------------------------------  -------------  -------------  -------  --------------------
stream_table_contract            1              0              true     {...}
graph_contract                   1              0              true     {...}
external_orchestration           1              0              true     {...}
transactional_graph_refresh      1              0              true     {...}
source_boundary_manifest         1              0              true     {...}
output_delta_consumer            1              0              true     {...}
```

The function returns one row per capability:

```sql
pgtrickle.integration_capabilities()
RETURNS TABLE (
    capability       text,
    major_version    smallint,
    minor_version    smallint,
    enabled          boolean,
    details          jsonb
)
```

A major version changes normative behavior or representation and requires explicit consumer support. A minor version is additive: it may add optional fields, supported source kinds, diagnostics, or admission limits without changing existing required fields or guarantees. A caller requires an exact supported major and may require a minimum minor. An absent or disabled capability is unsupported even when the package version appears recent enough.

The `details` object may report supported PostgreSQL majors, row-identity versions, maximum graph members, accepted source kinds, typed-delta encoding versions, and whether exact full-refresh deltas are available on particular execution paths. Normative behavior belongs in the capability version and documentation, not in free-form details. The function should be inexpensive, side-effect free, and executable by `PUBLIC` because it reveals product capabilities rather than protected relation definitions or data.

---

## 11. Durable external orchestration

### 11.1 Catalog-backed mode

Every stream table gains a durable orchestration mode:

```text
MANAGED   ordinary pg_trickle scheduler behavior
EXTERNAL  the ordinary scheduler never dispatches this stream table
```

`MANAGED` remains the default so existing installations behave exactly as before. `EXTERNAL` is orthogonal to lifecycle status. An external stream table may be active, suspended, initializing, or in error, but an active external table advances only through the strict graph API defined here. The ordinary human-oriented `refresh_stream_table()` function should reject an external table with a stable externally-managed error rather than bypass the coordinator.

An illustrative dedicated API is:

```sql
SELECT pgtrickle.set_orchestration_mode(
    stream_table => 'mdm_internal.customer_pair_evidence_v3'::regclass,
    mode         => 'EXTERNAL'
);
```

Creation and bulk creation should also accept the mode through their structured options so a private graph is never briefly scheduler-managed between creation and alteration. The exact parameter placement is not normative; the durable catalog value and scheduler semantics are.

### 11.2 Transition behavior

Changing from `MANAGED` to `EXTERNAL` acquires the normal lifecycle and refresh locks. It waits for or conflicts with in-flight work according to the caller's `lock_timeout`, and the scheduler rechecks orchestration mode under the same row-lock discipline used to claim a refresh. Once the mode change commits, no scheduler job accepted under stale managed state may remain able to publish afterward.

Changing from `EXTERNAL` to `MANAGED` is rejected while a strict graph refresh holds the node, while an output resnapshot lock is active, or while schedule configuration is invalid for managed execution. The operation does not acknowledge external consumers, drop retained history, or imply that an external coordinator has accepted the latest output.

### 11.3 Relationship to scheduler pause

`pause_scheduler()` remains a transient operational control. It is useful for draining or maintenance but is not a correctness mechanism for extension composition. An external stream table may still appear in global drain and health reporting, but it is already ineligible for scheduler dispatch because of durable catalog state.

### 11.4 Immediate mode

Capability version 1 rejects `EXTERNAL` orchestration for `IMMEDIATE` stream tables. Immediate maintenance is driven by source DML triggers inside source transactions and cannot be placed under a separate graph coordinator without a different consistency model. Coordinating extensions should use differential or full-capable modes and invoke strict graph refresh explicitly.

---

## 12. Canonical stream-table and graph contracts

### 12.1 Stream-table contract API

The proposed stream-table contract API is:

```sql
SELECT *
FROM pgtrickle.stream_table_contract(
    'mdm_internal.customer_pair_evidence_v3'::regclass
);
```

```sql
pgtrickle.stream_table_contract(stream_table regclass)
RETURNS TABLE (
    contract_version smallint,
    contract_digest  bytea,
    contract         jsonb
)
```

The contract contains everything that can change the logical contents of the stream table, the interpretation of its row identity, or the database identity under which defining SQL runs. At minimum it includes the schema-qualified stream-table identity and current relation binding; normalized defining-query and original-query digests; owner role, defining search path, and result-affecting row-security execution policy; ordered output schema with PostgreSQL type, typmod, collation, and nullability; row-identity and row-probe versions; versioned rewrite and DVM strategies that can affect logical output; tracked function and extension dependencies; source and stream-table dependencies; source schema or contract fingerprints; and the durable orchestration mode.

The contract excludes physical indexes, planner statistics, recent costs, worker counts, scheduler tier, batch sizes, cached plans, and other choices that may affect performance but are not allowed to affect meaning. A storage or plan optimization may therefore preserve the digest when it provably enumerates the same logical result.

An illustrative object is:

```json
{
  "contract_version": 1,
  "stream_table": "mdm_internal.customer_pair_evidence_v3",
  "relation_identity": {"oid": 48122, "database_lineage": "sha256:..."},
  "execution_identity": {
    "owner": "mdm_owner",
    "defining_search_path_digest": "sha256:...",
    "row_security_policy": "OWNER_EQUIVALENT"
  },
  "query": {
    "original_digest": "sha256:...",
    "normalized_digest": "sha256:...",
    "semantic_plan_digest": "sha256:..."
  },
  "output_schema": {
    "digest": "sha256:...",
    "columns": [
      {"name": "left_record_id", "type": "bytea", "nullable": false},
      {"name": "right_record_id", "type": "bytea", "nullable": false},
      {"name": "evidence", "type": "jsonb", "nullable": false}
    ]
  },
  "row_identity_version": 2,
  "row_probe_version": 1,
  "dependency_digest": "sha256:...",
  "function_dependency_digest": "sha256:...",
  "orchestration_mode": "EXTERNAL",
  "contract_digest": "sha256:..."
}
```

Every canonicalization rule and digest algorithm is part of the contract version. Human-readable optional fields may be added, but the document must identify which fields are normative digest inputs.

### 12.2 Graph contract API

The proposed graph API is:

```sql
SELECT *
FROM pgtrickle.graph_contract(
    roots => ARRAY[
        'mdm_internal.customer_pair_evidence_v3'::regclass,
        'mdm_internal.customer_golden_candidates_v3'::regclass
    ]
);
```

```sql
pgtrickle.graph_contract(roots regclass[])
RETURNS TABLE (
    contract_version smallint,
    graph_digest      bytea,
    contract          jsonb
)
```

The graph object contains the canonical root set, complete upstream stream-table closure, each member contract digest, dependency edges, deterministic topological order, external source descriptors, and one digest of the complete graph contract. Duplicate roots are rejected. Capability version 1 rejects cycles even when ordinary `pg_trickle` supports a separately governed monotone-cycle mode, because a strict external contract needs explicit fixed-point and delta semantics before cycles can be admitted.

The graph digest changes when a root or member is added, removed, replaced, rebound, or semantically altered. It does not change merely because an index is rebuilt, statistics change, or a different worker count would be used by ordinary managed scheduling. A graph contract is descriptive rather than a lock. `refresh_graph_strict()` re-resolves and revalidates the graph under the locks it acquires; a digest read earlier is an optimistic expectation, not permission to skip current validation.

---

## 13. Strict transactional graph refresh

### 13.1 Proposed API

The core refresh API is:

```sql
SELECT *
FROM pgtrickle.refresh_graph_strict(
    roots                       => ARRAY[
        'mdm_internal.customer_pair_evidence_v3'::regclass,
        'mdm_internal.customer_golden_candidates_v3'::regclass
    ],
    expected_graph_digest       => decode(:graph_digest_hex, 'hex'),
    require_complete_boundaries => true,
    full_policy                 => 'ALLOW'
);
```

An illustrative signature is:

```sql
pgtrickle.refresh_graph_strict(
    roots                       regclass[],
    expected_graph_digest       bytea,
    require_complete_boundaries boolean DEFAULT true,
    full_policy                 text DEFAULT 'ALLOW'
)
RETURNS pgtrickle.graph_refresh_result
```

The returned composite contains at least:

```text
contract_version        smallint
graph_refresh_id        bigint
graph_digest            bytea
source_boundary         jsonb
source_boundary_digest  bytea
node_results            jsonb
terminal_delta_tokens   jsonb
```

`full_policy` has two initial values. `ALLOW` permits any correct full or reinitialization fallback selected by the existing engine. `ERROR` rejects a member that cannot remain on an incremental or scoped-recomputation path and is intended for diagnostics or tightly controlled workloads. The policy never forces an unsafe incremental plan.

### 13.2 Resolution and locking

The implementation performs the following logical sequence:

1. Resolve the roots and compute the complete upstream stream-table closure.
2. Reject duplicate roots, unsupported cycles, temporary relations, cross-database references, `IMMEDIATE` members, and members not in `EXTERNAL` orchestration mode.
3. Verify that the caller is authorized to refresh every member.
4. Sort members by a canonical database-local lock key and acquire every advisory, lifecycle, and catalog-row lock before refreshing any member.
5. Reload every member under lock, recompute the graph contract, and compare it with `expected_graph_digest`.
6. Allocate one graph-refresh identifier and one immutable graph-refresh context.
7. Compute one source-boundary decision for every external source used by the graph.
8. Refresh members synchronously in deterministic topological order, passing the immutable context to each member.
9. Finalize node state and output-delta batches through the same transactional finalizer used by ordinary refresh paths.
10. Return the structured result while transaction-scoped locks remain held.

Any failure raises an error. The API never returns a partial list of successful members. The first implementation executes in the calling backend and does not dispatch work to independent background workers. It may reuse bounded internal batching and temporary relations as long as all effects remain within the caller's outer transaction.

### 13.3 Transaction semantics

The function performs no commit. Calling it in autocommit mode is valid, but a coordinating extension obtains composition semantics by invoking it inside its own function or explicit transaction and performing domain work before commit. Stream-table data changes, frontiers, history, output-delta batches, cleanup, group metadata, the coordinator's writes, and acknowledgements therefore commit or roll back together.

The function returns only after every member has completed and finalized or one member has caused the call to fail. Internal savepoints may be used for bounded cleanup, but they do not create progressive visibility or an independently committed successful subset. A no-data path remains a strict execution: it validates contracts, locks, source boundaries, and output-log health before returning `NO_DATA`.

### 13.4 Coherent source boundary

The graph-refresh context contains one immutable upper-bound decision for local CDC sources and one validated position or snapshot token for every other supported source. A source shared by several graph members appears once in the manifest, and every member consumes the same upper boundary. No member may call a current-position helper again and substitute a newer value.

For local trigger or WAL sources, the position is the safe durable boundary already governed by `pg_trickle` frontier invariants. For an upstream stream table inside the graph, the downstream member consumes the output state and delta produced earlier in the same topological refresh. For a materialized view or polling source, the manifest identifies the snapshot-comparison boundary and its completeness. A remote or distributed source is accepted only when its provider can prove that data and position belong to one complete boundary.

When `require_complete_boundaries` is true, an unknown, partial, unverifiable, or missing boundary aborts the graph refresh. A later minor contract may permit explicitly non-proven boundaries for generic consumers, but a consumer that requests complete boundaries must never receive a successful result with hidden uncertainty.

### 13.5 Result schema

An illustrative `node_results` entry is:

```json
{
  "mdm_internal.customer_pair_evidence_v3": {
    "refresh_id": 8124,
    "contract_digest": "sha256:...",
    "action": "DIFFERENTIAL",
    "result_class": "INCREMENTAL",
    "rows_inserted": 41,
    "rows_deleted": 7,
    "data_timestamp": "2026-09-01T18:02:11.531Z",
    "frontier": {"version": 1, "opaque": "..."},
    "row_identity_version": 2,
    "row_probe_version": 1,
    "output_delta": {
      "batch_token": 5502,
      "mode": "EXACT",
      "row_count": 48
    }
  }
}
```

`result_class` is a small stable normalization such as `NO_DATA`, `INCREMENTAL`, or `FULL`. `action` may expose a more specific versioned engine path such as differential, full, reinitialize, TopK, or partition recomputation. Consumers should use the stable class for control flow and retain the detailed action for diagnosis.

Reported row counts are exact or `NULL` with a machine-readable quality field. An estimate must never be presented as an exact inserted or deleted count. A no-data member may still advance a validated observation frontier, but its exact output-delta batch contains zero rows.

### 13.6 Busy behavior

The strict API never converts a lock conflict into a notice and successful return. If another refresh, alteration, repair, restore, or drop operation owns any member, the call waits according to PostgreSQL lock rules or raises a stable graph-busy error when the caller's `lock_timeout` or no-wait policy is reached. The ordinary `refresh_stream_table()` API may retain its existing notice-and-skip behavior for backward compatibility.

---

## 14. Source-boundary manifest

The source-boundary manifest is a first-class versioned result rather than an informal timestamp. A suggested shape is:

```json
{
  "manifest_version": 1,
  "graph_refresh_id": 1208,
  "captured_at": "2026-09-01T18:02:10.992Z",
  "database_identity": {
    "system_identifier_digest": "sha256:...",
    "database_oid": 16384
  },
  "completeness": "PROVEN",
  "sources": [
    {
      "relation": "crm.customer",
      "kind": "LOCAL_TABLE",
      "source_contract_digest": "sha256:...",
      "capture_mode": "TRIGGER",
      "position": {
        "version": 1,
        "kind": "WAL_LSN_RANGE",
        "lower": "0/16B6A90",
        "upper": "0/16D0200"
      },
      "completeness": "PROVEN"
    },
    {
      "relation": "registry.company_snapshot",
      "kind": "MATERIALIZED_VIEW",
      "source_contract_digest": "sha256:...",
      "capture_mode": "SNAPSHOT_COMPARE",
      "position": {
        "version": 1,
        "kind": "SNAPSHOT_TOKEN",
        "token": "opaque-provider-value"
      },
      "completeness": "PROVEN"
    }
  ]
}
```

The manifest has an authoritative digest returned separately. Callers store the object for explanation but treat individual source positions as opaque unless another capability explicitly documents them. A caller may not construct a replacement manifest, edit an upper position, or feed historical boundary JSON back into ordinary refresh as an instruction.

The database identity binds graph results and delta tokens to one database lineage so a token copied from a restored clone or another database cannot be mistaken for live state in the current database. The exact lineage identifier may be protected or hashed, but token validation must include it.

A boundary manifest states completeness, not merely recency. A timestamp saying when a refresh ran is not proof that every relevant source change was included. When a source cannot provide a complete boundary under the requested contract, the graph refresh fails before publication.

---

## 15. Durable output-delta consumers

### 15.1 Overview

A terminal stream table is useful to another extension only when the extension can identify its logical changes without querying private `changes_pgt_*` relations. The V1 consumer contract records one shared logical output log per opted-in stream table and one durable acknowledgement position per consumer. The log is created only when at least one external consumer exists, so ordinary stream tables pay no output-log storage or write cost.

The output delta describes changes to the stream table itself, not raw source events. It is independent of whether the producing refresh was differential, scoped recomputation, reinitialization, or full. An exact batch transforms the previous visible stream-table multiset into the new visible multiset. If row identity remains the same while non-identity values change, the batch contains a `DELETE` carrying the complete old row followed by an `INSERT` carrying the complete new row.

### 15.2 Registration

The proposed registration API is:

```sql
SELECT *
FROM pgtrickle.register_output_delta_consumer(
    stream_table             => 'mdm_internal.customer_pair_evidence_v3'::regclass,
    consumer_name            => 'pg_mdm/customer/v3/pair-evidence',
    expected_contract_digest => decode(:stream_contract_digest_hex, 'hex'),
    start_position           => 'CURRENT'
);
```

```sql
pgtrickle.register_output_delta_consumer(
    stream_table             regclass,
    consumer_name            text,
    expected_contract_digest bytea,
    start_position           text DEFAULT 'CURRENT'
)
RETURNS pgtrickle.output_delta_consumer_registration
```

The registration result contains:

```text
consumer_id                uuid
stream_table               regclass
consumer_name              text
delta_relation             regclass
acknowledged_batch_token   bigint
output_contract_digest     bytea
row_identity_version       smallint
state                      text
```

`CURRENT` creates the cursor at the latest completed batch and claims no earlier history. It is appropriate when the coordinator already has a matching baseline or when the stream table is newly created and unpopulated. A coordinator that does not already possess a matching baseline registers in `RESNAPSHOT_REQUIRED` mode and establishes one through the resnapshot protocol before claiming current state.

Registration is transactional and idempotent for `(stream_table, owner_role, consumer_name)` only when the expected digest and options are identical. A conflicting registration raises an error rather than silently altering an existing cursor. The function creates or references one shared typed output log for the stream table and returns a read-only relation through which the consumer can read exact rows. Callers use the returned `regclass`; they do not derive its physical name.

### 15.3 Typed delta relation

The generated relation has this logical schema:

```text
batch_token       bigint   NOT NULL
ordinal           bigint   NOT NULL
action            text     NOT NULL  -- DELETE | INSERT
row_identity      bytea    NOT NULL
<stream-table output columns, preserving PostgreSQL types>
```

The relation preserves the stream table's user-visible PostgreSQL types and contains complete old values for `DELETE` and complete new values for `INSERT`. Private generated columns are not exposed, except for the opaque canonical `row_identity` needed to pair and order changes. The relation is read only. Its physical name and implementation are not stable; the returned relation identity and logical schema remain valid for the lifetime of the consumer contract.

Rows are ordered by `(batch_token, ordinal)`. Ordinals are deterministic within a batch, and deletion precedes insertion for a same-identity update. Consumers may page within the relation, but acknowledgement remains batch-granular so a partially read batch cannot be discarded accidentally.

The recommended initial implementation uses one protected typed output-log table per stream table and one security-controlled read-only view per consumer. The output log is distinct from the private stream-table-to-stream-table buffer so this public contract does not freeze an existing internal schema. A later physical implementation may share storage when it preserves the same public behavior, types, retention, and upgrade boundary.

### 15.4 Batch metadata

Consumers inspect batches through:

```sql
SELECT *
FROM pgtrickle.output_delta_batches(
    consumer_id   => :consumer_id,
    through_token => :terminal_batch_token
);
```

```sql
pgtrickle.output_delta_batches(
    consumer_id   uuid,
    through_token bigint DEFAULT NULL
)
RETURNS TABLE (
    batch_token             bigint,
    producing_refresh_id    bigint,
    graph_refresh_id        bigint,
    mode                    text,
    row_count               bigint,
    rows_inserted           bigint,
    rows_deleted            bigint,
    output_contract_digest  bytea,
    row_identity_version    smallint,
    source_boundary_digest  bytea,
    created_at              timestamptz
)
```

The result begins after the consumer's acknowledged token and ends at `through_token`, or at the newest retained token when no upper token is supplied. It returns a continuous ordered sequence or raises a delta-gap error. It never silently omits an unavailable batch.

The initial modes are:

| Mode | Meaning |
|---|---|
| `EXACT` | The typed delta relation contains the complete logical delete and insert rows for the batch; an exact batch may contain zero rows |
| `FULL_INVALIDATION` | An exact logical row delta is unavailable; the consumer must read the complete current stream table at the transactionally stable graph result before acknowledging |

A differential refresh produces an exact batch when the output log is healthy. A full refresh produces an exact batch when the refresh path already has a complete before-and-after logical difference. Otherwise it produces `FULL_INVALIDATION`. V1 does not require `pg_trickle` to perform an additional unbounded full-table diff solely to satisfy an external consumer. Initial population may be represented as exact insertions when already materialized, but invalidation is always a permitted truthful result.

### 15.5 Reading and acknowledgement

A V1 coordinator reads the graph result, selects batch metadata through the terminal token returned for the relevant root, and follows one of two paths. If every batch in the range is exact, it reads the typed relation through that token and applies the logical changes. If the range contains a full invalidation, it reads the complete current terminal stream table while the strict graph transaction still holds a stable graph state and runs its full reference computation.

Acknowledgement is explicit:

```sql
SELECT pgtrickle.ack_output_delta(
    consumer_id   => :consumer_id,
    through_token => :terminal_batch_token,
    disposition   => 'APPLIED'
);
```

`disposition` is `APPLIED` when all acknowledged batches were exact and the consumer applied their rows. It is `RESYNCHRONIZED` when the range contained an invalidation and the consumer rebuilt from the complete terminal table. `APPLIED` is rejected for a range containing `FULL_INVALIDATION`. The acknowledgement is monotonic, validates the output contract and row-identity version, and updates the durable cursor in the caller's transaction.

Reading never acknowledges. Acknowledging an already acknowledged token is an idempotent no-op only when it belongs to the same consumer, output contract, and database lineage. Acknowledging beyond the newest visible token, across a gap, across an invalid contract, or out of order raises a stable error.

### 15.6 Resnapshot protocol

A consumer that lacks a matching baseline or loses exact history enters `RESNAPSHOT_REQUIRED`. It establishes a new baseline through a transaction-scoped protocol:

```sql
SELECT *
FROM pgtrickle.begin_output_delta_resnapshot(:consumer_id);

-- Read the complete stream table returned by the status record and rebuild
-- consumer-owned state while the transaction-scoped output lock is held.

SELECT pgtrickle.ack_output_delta_resnapshot(
    consumer_id    => :consumer_id,
    resnapshot_token => :resnapshot_token
);
```

The begin call returns the stream table, stable output batch token, contract digest, row-identity version, and one-use token. It locks the output against refresh or semantic change until transaction end. The acknowledgement advances the cursor to that stable token and moves the consumer to `ACTIVE`. A rollback leaves the cursor and consumer state unchanged.

### 15.7 Consumer lifecycle

A consumer has one of these states:

```text
ACTIVE
PAUSED
RESNAPSHOT_REQUIRED
INVALIDATED
DROPPED
```

`ACTIVE` means a continuous exact-or-invalidation sequence exists after the cursor. `PAUSED` retains history but rejects acknowledgement and is an operator control. `RESNAPSHOT_REQUIRED` means exact retained history was lost, deliberately expired, or made unverifiable by recovery. `INVALIDATED` means the stream-table semantic or output contract changed and the consumer must be recreated or explicitly rebound after a new baseline. `DROPPED` may remain only in audit history.

A semantic or output-changing `ALTER QUERY` is rejected while active consumers exist under capability version 1. `pg_mdm` avoids in-place mutation by creating a new private graph for each entity-definition version. Dropping a stream table requires dropping or explicitly cascading its consumers. Dropping a consumer is authorization-checked and may allow retained rows to be cleaned immediately.

### 15.8 Retention and backpressure

The output log is retained through the minimum acknowledged batch among active consumers. Internal downstream stream-table consumers continue to use their private frontier contract; an implementation may combine retention minima physically, but neither public cursor is represented as the other.

The default V1 retention behavior is fail closed. A slow consumer may retain substantial data, and `pg_trickle` must expose row, byte, batch, and age lag. If writing another exact batch would exceed a configured hard resource limit, the refresh fails before unacknowledged history is lost. An administrator may explicitly move a consumer to `RESNAPSHOT_REQUIRED`, record the gap boundary and reason, and then permit cleanup. Silent expiration is never allowed.

Output logs required by active consumers are logged and crash-safe. Loss of a log sentinel, batch metadata, typed relation, or continuity proof changes affected consumers to `RESNAPSHOT_REQUIRED`; it is never interpreted as no changes.

---

## 16. V1 end-to-end protocol

The complete composition pattern is:

```sql
BEGIN;

-- The coordinating extension stored this digest when it created or activated
-- its private graph definition.
SELECT *
INTO TEMP TABLE _graph_result
FROM pgtrickle.refresh_graph_strict(
    roots                       => ARRAY[
        'mdm_internal.customer_pair_evidence_v3'::regclass,
        'mdm_internal.customer_golden_candidates_v3'::regclass
    ],
    expected_graph_digest       => decode(:expected_graph_digest, 'hex'),
    require_complete_boundaries => true,
    full_policy                 => 'ALLOW'
);

-- Inspect the exact batch sequence through the terminal token returned above.
SELECT *
FROM pgtrickle.output_delta_batches(
    consumer_id   => :pair_consumer_id,
    through_token => :pair_terminal_token
)
ORDER BY batch_token;

-- Read exact typed rows from the consumer's returned delta relation, or scan
-- the complete terminal stream table when FULL_INVALIDATION is present.

-- Perform domain-specific work. pg_trickle does not know this schema.
SELECT mdm_internal.resolve_and_publish_customer(
    graph_refresh_id       => :graph_refresh_id,
    source_boundary        => :source_boundary,
    source_boundary_digest => :source_boundary_digest
);

-- Acknowledge only after the domain result has been validated and written.
SELECT pgtrickle.ack_output_delta(
    consumer_id   => :pair_consumer_id,
    through_token => :pair_terminal_token,
    disposition   => :pair_disposition
);

SELECT pgtrickle.ack_output_delta(
    consumer_id   => :golden_consumer_id,
    through_token => :golden_terminal_token,
    disposition   => :golden_disposition
);

COMMIT;
```

If domain resolution, clustering, validation, public-table DML, or acknowledgement raises an error, the transaction rolls back. Private stream tables return to their previous contents, node frontiers do not advance, graph and node refresh records do not commit, newly written output-delta batches disappear, and consumer cursors remain unchanged. The next invocation can repeat the complete operation without reconciling a half-published boundary.

The coordinator does not need to block new source writes for the duration of the transaction. `pg_trickle` captures a safe upper boundary and leaves later committed changes pending for the next refresh. The important property is not that the database stops changing; it is that every graph member and every domain output in this transaction refers to the same named boundary.

---

## 17. Security and authorization

The composition API is intended for trusted in-database coordinators, but it must not create a privilege-escalation path. Mutating functions should not be executable by `PUBLIC` by default. Installation may create a predefined integration role, or administrators may grant the relevant functions directly to the dedicated owner role used by the coordinating extension.

`refresh_graph_strict()` must verify ownership or a narrowly documented owner-equivalent maintenance privilege for every member in the graph closure. Capability version 1 should require one effective owner across the graph because mixed-owner execution complicates authorization, RLS, retained deleted rows, and lifecycle coordination. Defining queries continue to execute under each stream table's stored owner identity and defining search path rather than the invoker's accidental session context.

Output-delta registration, reading, resnapshot, acknowledgement, pause, and deletion should be restricted to the stream-table owner or superuser in V1. This avoids a subtle information leak from deleted rows retained in output history, because ordinary table RLS cannot reliably be reevaluated against a row that no longer exists. A later delegated-consumer design would require explicit row filters, masking rules, and retained-row policy rather than an informal grant.

Every `SECURITY DEFINER` entry point must use a fixed safe `search_path`, resolve objects through PostgreSQL catalogs, distinguish identifiers from values in generated SQL, and avoid exposing private relation names to unauthorized callers. Consumer and resnapshot tokens are references only after authorization; knowledge of a UUID is not authority.

---

## 18. Concurrency and transaction semantics

### 18.1 Lock ordering

Strict graph refresh resolves the complete closure and acquires all graph-member locks in one canonical order before executing the first member. The order must be independent of root-array order and shared with alter, repair, drop, resnapshot, and orchestration-mode transitions so that callers do not create lock-order cycles. Source relation locks required by a full baseline follow the existing source-lock protocol and are acquired in deterministic relation order.

### 18.2 Scheduler interaction

An `EXTERNAL` member is ineligible for scheduler dispatch. The scheduler must inspect the durable mode under the same lock discipline used to claim work so an already queued stale job cannot publish after a mode transition. Global drain or shutdown may observe external work, but it does not take ownership of the graph.

### 18.3 Source writes

Ordinary source writes continue while strict graph refresh runs unless an existing full-refresh path deliberately acquires source locks for snapshot alignment. The immutable source-boundary context determines which committed changes belong to the current refresh. Later changes remain in source capture for the next operation.

### 18.4 Caller transaction

The first implementation is synchronous in the caller's backend. It does not detach work into a background process and does not release graph locks before the caller either commits or rolls back. The coordinator must therefore size V1 entities for the documented single-transaction operating envelope and set `statement_timeout`, `lock_timeout`, memory, and temporary-space policy knowingly.

### 18.5 Isolation level

The API must work correctly under PostgreSQL's documented transaction isolation behavior and must not assume that a caller happened to start `REPEATABLE READ`. `pg_trickle` owns source-boundary construction and uses its required snapshots and locks internally. If a particular source adapter requires a stronger isolation or snapshot capability, admission fails unless the engine can establish it safely.

---

## 19. Failure and recovery behavior

A failed strict graph refresh leaves no committed member subset, frontier advancement, output batch, cleanup, or successful group record. A failure later in the coordinator's transaction also rolls those effects back. The ordinary PostgreSQL error becomes the synchronization mechanism: there is no second compensation protocol for a transaction that never committed.

A backend crash or server restart during an uncommitted strict refresh relies on PostgreSQL crash recovery. After recovery, graph storage and frontiers reflect the last committed state. Any stale operational `RUNNING` marker used for diagnosis may be marked failed by existing startup logic, but it must not be treated as evidence that output or frontiers committed.

Output-log integrity is durable control state. On startup, restore, or repair, `pg_trickle` validates consumer catalog rows, output-log relation identity, required columns and types, contract versions, sentinels, batch continuity, and acknowledged positions. Missing or unverifiable history moves the consumer to `RESNAPSHOT_REQUIRED`. The system does not synthesize an empty batch or silently jump the cursor.

A semantic contract change invalidates active consumers rather than reinterpreting retained rows. A physical-only change may preserve the contract and consumer continuity when validation proves the typed output relation and logical ordering remain unchanged. A dropped and recreated database clone has a different lineage identity, so copied tokens are rejected even when relation OIDs happen to match.

`repair_stream_table()` may rebuild triggers, buffers, or derived storage, but it cannot repair an exact external history that has been lost. Its result must state whether consumers remain active, require resnapshot, or were invalidated by the repair.

---

## 20. Stable errors

The integration API should use `pg_trickle`'s structured error machinery and expose a stable integration identifier in diagnostic detail. Callers must not parse free-form English messages. The precise SQLSTATE may follow the extension's established policy, but the following identifiers should be stable within capability major version 1:

| Error identifier | Meaning |
|---|---|
| `PGT_EXT_CONTRACT_UNSUPPORTED` | A required capability or contract version is unavailable |
| `PGT_EXT_GRAPH_INVALID` | Roots do not form a supported externally orchestrated graph |
| `PGT_EXT_CONTRACT_MISMATCH` | Current stream-table or graph contract differs from the expected digest |
| `PGT_EXT_REFRESH_BUSY` | Another operation owns a required graph member |
| `PGT_EXT_BOUNDARY_UNAVAILABLE` | A complete coherent source boundary cannot be proved |
| `PGT_EXT_NODE_FAILED` | One graph member could not produce a complete result |
| `PGT_EXT_CONSUMER_BLOCKED` | Consumer integrity, schema, authorization, or retention state prevents consumption |
| `PGT_EXT_DELTA_GAP` | The requested continuous batch range is not retained or cannot be proved |
| `PGT_EXT_TOKEN_INVALID` | Consumer, batch, or resnapshot token is unknown, altered, cross-database, or belongs to another object |
| `PGT_EXT_ACK_OUT_OF_ORDER` | Acknowledgement would move backward, skip an unapproved range, or use the wrong disposition |
| `PGT_EXT_AUTHORIZATION` | Caller is not authorized for the graph or consumer |
| `PGT_EXT_RESNAPSHOT_REQUIRED` | Exact history is unavailable and a full baseline is required |
| `PGT_EXT_ORCHESTRATION_MODE` | The requested operation conflicts with `MANAGED` or `EXTERNAL` ownership |

Each error names the affected graph, stream table, source, or consumer when disclosure is authorized, states the consequence, and includes one concrete next action. A strict refresh error is never downgraded to a notice. A retention or continuity error is never represented as an empty exact batch.

---

## 21. Suggested catalog and storage changes

This section is illustrative rather than a required physical schema. The public behavior above is normative; internal names and normalization may change.

### 21.1 Stream-table orchestration

A durable column on the stream-table catalog records the mode:

```sql
ALTER TABLE pgtrickle.pgt_stream_tables
ADD COLUMN orchestration_mode text NOT NULL DEFAULT 'MANAGED'
    CHECK (orchestration_mode IN ('MANAGED', 'EXTERNAL'));
```

The catalog should also retain the contract version or enough semantic metadata to build and validate the contract deterministically. A cached digest may be stored as an optimization, but it must be invalidated by every result-affecting DDL, function, owner, search-path, source-binding, and semantic-plan change.

### 21.2 Graph refresh records

A graph refresh needs one identity that links member refreshes and output batches:

```text
pgt_graph_refreshes
  graph_refresh_id
  owner_role
  graph_digest
  source_boundary
  source_boundary_digest
  roots
  started_at
  completed_at
  status
  error_code
```

Successful rows commit with the graph transaction. A failed transaction may leave no durable row because rollback is authoritative; failures remain visible to the caller and logs, and the coordinating extension may record them in its own operation history.

### 21.3 Output consumers and batches

A small logged catalog records consumer state:

```text
pgt_output_delta_consumers
  consumer_id
  stream_table_id
  owner_role
  consumer_name
  output_contract_digest
  row_identity_version
  acknowledged_batch_token
  state
  resnapshot_reason
  created_at
  acknowledged_at
```

Batch metadata is also logged:

```text
pgt_output_delta_batches
  stream_table_id
  batch_token
  producing_refresh_id
  graph_refresh_id
  mode
  row_count
  rows_inserted
  rows_deleted
  output_contract_digest
  row_identity_version
  source_boundary_digest
  created_at
```

The payload is stored once per opted-in stream table in a protected typed relation. Consumer-specific views or access predicates may expose the shared rows without duplicating payload. Batch tokens are monotonically increasing within one stream-table output log and remain opaque outside the documented ordering and acknowledgement contract.

### 21.4 Resnapshot state

A resnapshot token may be represented as a signed or catalog-backed one-use record bound to consumer, database lineage, stream-table identity, output contract, stable batch token, owner, and transaction. Its implementation must prevent replay against another consumer or later incompatible contract. No long-lived application secret is required; PostgreSQL authorization remains primary.

---

## 22. Integration with the refresh engine

The strict graph API should call the same validated execution and common-finalizer code used by scheduler and manual refresh paths. The main refactoring is to separate the engine result from the current human-facing wrapper so a strict caller receives `Result<GraphRefreshResult, PgTrickleError>` rather than a notice on `RefreshSkipped`.

The scheduler already computes dependency-aware execution units and database-local source bounds. The strict path can create one execution unit for the requested closure, capture one immutable boundary context, acquire deterministic locks, and invoke existing member refresh machinery in topological order. It should not synthesize sequential calls to the public `refresh_stream_table()` wrapper because that would repeat boundary decisions, lose group identity, and retain notice-and-skip semantics.

The contract builder can reuse stream-table metadata, dependency catalogs, defining-query hashes, owner execution state, row-identity and row-probe versions, function fingerprints, collation validation, source schema fingerprints, and versioned strategy fields already present in the system. A dedicated canonicalization module should define digest inputs, with independent fixtures so an incidental JSON serialization refactor cannot change a contract silently.

Output-delta batch creation belongs in the common finalizer. A member frontier must not commit without the corresponding promised batch metadata and exact payload or `FULL_INVALIDATION` marker. Differential output can reuse the logical D+I relation already produced by the engine. Supported full paths can reuse their existing before-and-after diff. Other full paths write invalidation metadata rather than an incomplete payload.

The external output log should remain distinct from private downstream buffers in capability version 1. This slightly increases write amplification for opted-in stream tables but preserves a clean public boundary and permits private buffer evolution. Shared physical storage may be considered later only after the public cursor, type, retention, and upgrade semantics are fixed.

---

## 23. Lifecycle interactions

### 23.1 Create and bulk create

A coordinating extension normally creates an immutable private graph with `orchestration_mode = 'EXTERNAL'` and `initialize = false`, computes and stores its graph digest, registers consumers on terminal nodes, and performs first population through `refresh_graph_strict()`. Bulk creation should validate all names, ownership, modes, and graph constraints before making any member visible.

### 23.2 Alter query and storage recreation

A stream table with active output consumers cannot undergo a semantic or output-contract change in place under capability version 1. The caller creates a new stream table or graph version, registers new consumers, establishes a new baseline, activates the corresponding higher-level definition, and later removes the old graph after its audit or explanation horizon is satisfied.

Operational alterations that do not change the stream-table contract, such as supported index maintenance or statistics updates, may proceed when they do not conflict with an active refresh. The contract digest remains unchanged only when semantic equivalence is proven.

### 23.3 Suspend, resume, and repair

Suspending an external stream table blocks strict graph refresh but does not transfer it to the scheduler. Resuming makes it explicitly refreshable again. Repair validates contracts and output-log continuity and reports whether consumers remain active or require resnapshot. It never acknowledges on behalf of a coordinator.

### 23.4 Drop

Dropping a stream table fails while output consumers exist unless the caller uses a documented cascade operation. A privileged cascade explicitly invalidates or drops consumers, records the consequence, removes generated views and payload storage, and then follows ordinary dependency cleanup. It may not leave another extension believing that its acknowledged cursor has a continuous future stream.

### 23.5 Orchestration-mode changes

Returning a table to `MANAGED` mode does not consume or delete pending external output history. Administrators must explicitly decide whether consumers are retained, resnapshotted, or dropped. A mode change is a lifecycle operation, not an acknowledgement shortcut.

---

## 24. Observability and administration

The new contract should add concise supported status functions rather than require operators to join private catalogs:

```sql
SELECT * FROM pgtrickle.external_graph_status(roots => ARRAY[...]::regclass[]);
SELECT * FROM pgtrickle.output_delta_consumer_status();
```

Graph status should report graph digest, member count, orchestration eligibility, current busy state, most recent graph refresh, source-boundary completeness, and terminal batch positions. Consumer status should report owner, stream table, state, acknowledged and newest batch token, retained rows and bytes, batch lag, age lag, contract digest, row-identity version, and resnapshot reason.

`health_check()` should warn when an external consumer needs resnapshot, is invalidated, retains more than configured advisory thresholds, references missing storage, or blocks refresh because a hard limit is near; when an external graph contains a managed or unsupported member; or when a graph contract can no longer be computed. Warnings are operational and do not change truth or advance state.

Refresh history should identify `initiated_by = 'EXTERNAL_GRAPH'`, the graph refresh identifier, and the graph digest. Existing timeline and metrics surfaces may aggregate graph duration and output-log overhead while preserving per-member history. Useful counters include strict refresh totals and failures, busy conflicts, exact and invalidation batches, output rows and retained bytes, consumer gaps, resnapshots, and acknowledgement latency.

---

## 25. Performance and resource behavior

The integration contract should add no output-delta overhead to a stream table that remains `MANAGED` and has no registered external consumer. Capability discovery is constant-size metadata. Contract digests may be cached by a complete semantic cache key and invalidated by the same catalog and function-dependency mechanisms that invalidate query plans.

A strict graph refresh may be cheaper than repeated single-node public refreshes because it computes one closure, one lock plan, one source-boundary context, and one topological traversal. It should not repeatedly probe the same source, parse the same contract, or rebuild identical dependency state. The locked database catalog remains authoritative even when a cached DAG is reused.

Output-delta capture adds one shared write per logical changed row, regardless of the number of external consumers. Per-consumer cost is a cursor row and access view rather than duplicate payload. Batch metadata adds one row per opted-in stream-table refresh, including no-data batches where a stable token is useful.

A slow consumer can retain substantial output history. Advisory limits must be observable. Hard limits may block a refresh before history is lost or may require an administrator to move the consumer explicitly to `RESNAPSHOT_REQUIRED`; they may not truncate the current exact batch. If a complete exact batch cannot be written, the finalizer either writes an allowed invalidation marker or aborts.

The initial contract should publish conservative admission bounds for root count, graph members, dependency edges, consumers per stream table, generated views, contract size, and batch pagination. Exceeding an admission bound fails before refresh and does not process an arbitrary graph prefix. V1 does not promise resumability; callers must operate within documented transaction, WAL, lock, memory, and temporary-space limits.

---

## 26. Upgrade and compatibility policy

The proposal is additive. Existing stream tables receive `orchestration_mode = 'MANAGED'`. Existing APIs, including the human-friendly notice-and-skip behavior of `refresh_stream_table()`, remain unchanged. No external output log exists until a consumer registers.

Capability major versions are independent of the extension package version. A storage-only or physical-plan upgrade may preserve capability majors and contract digests. A change to canonical contract encoding, graph closure, source-boundary meaning, output-delta row semantics, acknowledgement rules, token validation, or resnapshot behavior requires a new capability major and either side-by-side support or an explicit integrator migration.

A mandatory correctness repair that changes logical semantics must change the relevant semantic-plan version and therefore the contract digest. A coordinating extension then refuses to refresh the old graph until it creates or explicitly adopts a new definition. A repair must not claim compatibility merely because the output column list is unchanged.

Upgrade scripts preserve durable orchestration mode, consumer registrations and cursors, batch metadata, output-log payload, contract versions, and graph refresh identities. `pg_dump`, physical backup, PITR, and failover treat those objects as durable state. Generated consumer views may be recreated deterministically from the consumer catalog, but missing payload history moves the consumer to `RESNAPSHOT_REQUIRED`.

The V1 contract is SQL-facing. Coordinating extensions must not link to private Rust modules or assume a stable Rust ABI. Runtime capability negotiation is authoritative even when package dependencies pin a supported release line.

---

## 27. Delivery plan

The work should be delivered in three phases while keeping capability versions experimental until transaction, crash, and upgrade proofs pass.

### Phase 1: contracts and external ownership

Add capability discovery, canonical stream-table contracts, graph contracts, graph digests, and durable orchestration mode. Extend create, bulk-create, status, scheduler admission, lifecycle locks, dump, restore, and upgrade handling. This phase has no external output log and can be tested independently.

### Phase 2: strict graph refresh

Extract or reuse one structured internal graph-refresh context, acquire all external-member locks before execution, compute one immutable source boundary, execute members synchronously in topological order, and return a typed graph result. Wire every member through the common transactional finalizer and add stable busy, contract, completeness, and mode errors. At the end of this phase, a consumer may safely coordinate a graph but may still scan terminal relations completely after every refresh.

### Phase 3: durable typed output deltas

Add consumer registration, shared typed output logs, batch metadata, generated read-only consumer relations, exact differential capture, full invalidation, transactional acknowledgement, resnapshot, retention, status, and repair behavior. Integrate batch creation into the common finalizer so frontier advancement cannot commit without the corresponding promised batch. This phase completes the V1 dependency.

Operational documentation, metrics, capacity guidance, backup and restore verification, upgrade matrix tests, and an example coordinating test extension are required before capability major version 1 is advertised as stable. During implementation, discovery should report an experimental version or `enabled = false` until each phase's complete contract is available.

---

## 28. Test plan

### 28.1 Unit and property tests

Canonical contract tests must prove deterministic ordering, exclusion of operational fields, inclusion of every result-affecting field, digest changes for semantic mutations, and digest stability for physical tuning. Graph tests should generate DAGs with duplicate roots, shared upstream members, diamonds, unsupported cycles, renames, object recreation, owner changes, and dependency changes and verify one canonical closure and lock order.

Output-delta algebra tests must compare the old and new stream-table multisets with every exact batch after inserts, deletes, same-identity updates, key-changing updates, duplicates, aggregates, joins, set operations, TopK paths, no-op changes, and full fallback. Consumer state-machine tests cover registration idempotency, monotonic acknowledgement, invalidation disposition, resnapshot, gap handling, pause and resume, multiple consumers, drop, and contract change.

### 28.2 PostgreSQL integration tests

At minimum, integration tests must cover:

1. A one-node external graph refreshed inside a transaction that later commits.
2. The same refresh followed by a coordinator error, proving stream output, frontier, output batch, and acknowledgement all roll back.
3. A chain of stream tables refreshed in dependency order against one boundary.
4. A diamond graph in which both branches observe one shared upstream result and the terminal sees a coherent fan-in.
5. A concurrent strict refresh producing `PGT_EXT_REFRESH_BUSY` or bounded lock wait, never successful skip.
6. A concurrent `ALTER QUERY`, repair, mode change, and drop against a strict refresh, proving compatible lock behavior.
7. A graph-contract change between initial inspection and locked refresh, producing contract mismatch before committed mutation.
8. Concurrent source writers before and after the safe boundary, proving no lost or double-consumed changes.
9. Exact differential output with inserts, deletes, and updates represented as delete plus insert.
10. Full refresh with an exact before-and-after delta.
11. Full refresh without exact delta support, producing one `FULL_INVALIDATION` and no partial payload.
12. Multiple external consumers at different cursors, proving cleanup follows the slowest consumer.
13. Acknowledgement followed by transaction rollback, proving the batch remains pending.
14. A hard retention limit, proving refresh blocks before exact history is lost.
15. Explicit resnapshot, proving full-table read and cursor reset commit together.
16. Output-schema or semantic change with active consumers, proving rejection or explicit invalidation rather than silent reinterpretation.
17. External orchestration across scheduler restart and PostgreSQL restart.
18. Missing or corrupted output-log state after restore, proving `RESNAPSHOT_REQUIRED` rather than an empty delta.
19. Ownership and RLS cases proving owner-equivalent defining-query execution and no deleted-row leak.
20. Cross-database or restored-clone token reuse, proving database-lineage validation.

### 28.3 Full and differential equivalence

For every supported query family used by the contract, a reference harness should begin from one committed stream-table state, apply a generated source-change sequence, run differential refresh with exact output logging, and compare the resulting multiset with full query evaluation at the same source boundary. It should also verify that applying the exposed delete and insert rows to the old multiset produces the new multiset exactly. This is the central semantic proof for the public delta contract.

### 28.4 Security tests

Security tests cover function grants, unauthorized graph members, mixed owners, hostile `search_path`, RLS-enabled sources, guessed consumer tokens, guessed resnapshot tokens, private relation disclosure, direct DML attempts against generated delta relations, and deleted-row access. SQL-reachable Rust paths must not panic on malformed input or unexpected catalog state.

### 28.5 Recovery and upgrade tests

Physical restart, crash during refresh, PITR-style restore, logical dump and restore, extension upgrade, relation recreation, missing payload relations, and damaged sentinels must be tested. Machine-readable status must distinguish active, pending, invalidated, and resnapshot-required consumers. Compatibility fixtures should pin capability results, contract canonicalization, graph digest behavior, batch ordering, and token validation across every supported upgrade path.

### 28.6 Performance tests

Benchmarks should measure strict graph overhead against repeated single-node refresh, contract-digest cache cost, exact output-log write amplification, typed consumer-view scan throughput, batch metadata cost, cleanup with several consumers, and zero-cost behavior when no external consumer exists. Performance gates must never weaken exactness or failure behavior.

---

## 29. Alternatives considered

### 29.1 Call `refresh_stream_table()` repeatedly

A coordinator could sort its own graph and invoke the current public refresh once per node. This is rejected because each call may select a separate boundary, concurrency may become a notice-and-skip success, graph membership can change between calls, and the coordinator receives no graph contract or structured group result. Reimplementing `pg_trickle`'s DAG and lock logic in another extension would create two authorities for the same graph.

### 29.2 Use `pause_scheduler()` as durable ownership

Temporary pause is not a catalog-backed semantic declaration and is not intended to survive every restart and lifecycle transition. Correctness should not depend on shared-memory timing or an operator remembering to reapply pause state.

### 29.3 Read private change buffers

A coordinator could query `pgtrickle_changes.*` and maintain its own frontier. This would freeze private relation names, schemas, cleanup rules, catalog identifiers, and position semantics as an accidental public API. It would also allow the caller to bypass the validation that protects completeness.

### 29.4 Install row triggers on terminal stream tables

Triggers can copy differential changes into a consumer-owned relation in the same transaction, but they do not provide a supported graph cutoff, durable consumer cursor, minimum-retention coordination, contract verification, or reliable baseline semantics across every full and schema-changing path. They remain a useful user-space technique, not the extension-to-extension compatibility boundary.

### 29.5 Use logical replication

Logical replication is appropriate for an external downstream system. It is not an adequate in-database composition boundary because subscriber acknowledgement is not committed with the coordinator's local publication transaction, and WAL events do not directly provide the graph contract and source-boundary result required here.

### 29.6 Use `pg_tide`

`pg_tide` is an event outbox and delivery boundary. It does not provide a typed relational before-and-after delta whose cursor participates in `pg_trickle` output retention. A coordinator may publish its final domain events through `pg_tide`, but that occurs after it consumes local evidence and decides domain meaning.

### 29.7 Always scan the terminal stream table

A full scan is correct and can be the first milestone after strict graph refresh. It discards the principal performance benefit of incremental evidence maintenance for large terminal relations. The completed V1 contract therefore includes durable deltas, while full scan remains the explicit invalidation and resnapshot fallback.

### 29.8 Add an arbitrary post-refresh callback

A callback that invokes another extension after each refresh creates difficult transaction, authorization, recursion, error-isolation, resource, and versioning questions. It also makes the `pg_trickle` scheduler the owner of the higher-level workflow. The explicit SQL coordinator model is easier to test and leaves domain semantics with the domain extension.

### 29.9 Link to or fork private Rust modules

PostgreSQL extensions do not have a stable Rust ABI, and a fork would duplicate the most difficult CDC, DVM, frontier, and upgrade work. A small SQL compatibility surface is a cleaner long-term boundary.

---

## 30. Risks and open implementation choices

The largest risk is that a public output-delta contract constrains internal cleanup and storage. The mitigation is to stabilize logical batch tokens, ordering, typed relations, acknowledgement, and gap behavior rather than the private buffer layout. The output log may be a separate relation initially and can evolve physically behind the contract.

A second risk is unbounded retention caused by a failed coordinator. The mitigation is explicit lag status, alerts, conservative hard bounds, blocking before data loss, and an administrator-controlled transition to `RESNAPSHOT_REQUIRED`. Availability must not be obtained by silently discarding exact history.

A third risk is API growth in an already broad extension. The mitigation is a focused integration namespace with independently versioned capabilities and a small number of functions. Ordinary users never need these functions to create and query a normal stream table.

A fourth risk is overclaiming source coherence. The mitigation is a typed source-boundary manifest and failure when the graph cannot obtain one complete proof. Several incomparable source positions must not be reduced to a reassuring but meaningless timestamp.

A fifth risk is security leakage from retained deleted rows. The initial owner-only consumer policy avoids delegated access until a complete filtering and masking design exists.

Several implementation choices remain open without changing the proposal's semantics. Capability discovery may return rows or one versioned JSON document. Graph and refresh results may use named composite types or versioned JSON subdocuments. The typed delta surface may be a protected table, view, or set-returning function backed by one shared log. Lock behavior may rely on ordinary `lock_timeout` rather than a separate API option. Batch tokens may be per stream table or globally allocated, provided ordering, continuity, and validation remain exactly defined. These choices should be resolved in implementation review and recorded before capability major version 1 is frozen.

---

## 31. Acceptance criteria

The V1 proposal is complete when a small conformance extension can perform the following operations using only supported SQL APIs:

1. Discover and require the six V1 integration capabilities by major and minimum minor version.
2. Create an acyclic private stream-table graph durably in `EXTERNAL` orchestration mode.
3. Obtain canonical stream-table and graph contracts whose digests change for semantic or binding changes and remain stable for physical-only changes.
4. Register durable typed output consumers on terminal stream tables without learning private buffer names.
5. Refresh the complete graph synchronously against one coherent, complete source-boundary decision and receive one structured graph result.
6. Observe a busy, altered, unsupported, incomplete, unauthorized, or failed graph as a stable error with no committed member subset.
7. Read an immutable continuous exact batch, apply it to coordinator-owned state, acknowledge it, and commit all effects together.
8. Roll back after graph refresh or acknowledgement and observe graph contents, frontiers, output batches, coordinator state, and cursor all unchanged.
9. Receive `FULL_INVALIDATION` rather than a partial row set when exact output change cannot be supplied, then resynchronize from the complete stable terminal relation.
10. Retain independent cursors for multiple consumers and clean payload only through the slowest acknowledged position or an explicit resnapshot transition.
11. Survive PostgreSQL restart, backup and restore, failover, and supported extension upgrade without losing external orchestration or silently skipping a delta gap.
12. Observe owner checks, safe search paths, row-identity and contract validation, bounded inputs, stable errors, lag status, and health diagnostics.
13. Prove through differential/full tests that every exact batch transforms the prior output multiset into the complete query result at the named source boundary.
14. Implement the complete `pg_mdm` V1 refresh and publication transaction without reading or writing a private `pg_trickle` catalog, buffer, frontier, scheduler table, or generated implementation column.

Nothing in Appendix A is part of these acceptance criteria.

---

## 32. Recommended disposition

Adopt the V1 composition contract as a focused pre-1.0 extension boundary. Deliver durable external orchestration and canonical contracts first, strict transactional graph refresh second, and durable typed output deltas third. Keep the surface experimental until transaction, concurrency, crash, security, and upgrade proofs pass, then advertise capability major version 1.

The proposed boundary preserves the responsibilities of both projects:

> **`pg_trickle` owns complete incremental relational facts and their source boundaries. A coordinating extension owns the higher-level decisions made from those facts. PostgreSQL owns the transaction that commits them together.**

---

# Appendix A: high-level V2 needs

**This appendix is non-normative. It summarizes the `pg_mdm` V2 need only. It is not part of the V1 decision, delivery plan, implementation estimate, or acceptance criteria, and every capability described here requires a separate proposal before implementation.**

The V1 contract keeps graph refresh, domain computation, domain publication, and delta acknowledgement inside one PostgreSQL transaction. That is the simplest and strongest model for small and medium workloads, but a higher-level resolver may eventually need minutes or hours of component analysis, stable-identity reconciliation, validation, or preview work. Holding one database transaction and one set of graph locks for that entire period may become operationally unacceptable.

A later capability would therefore need an immutable prepared graph result at a named source boundary. `pg_trickle` would refresh the graph, commit the private relational state, and seal the exact member contracts, source-boundary manifest, terminal output state, and delta positions. The coordinating extension could then perform deterministic checkpointed work across several transactions while source changes beyond the sealed boundary continued to accumulate for a later refresh.

Final publication would still need one short atomic transaction. In that transaction, the coordinator would verify that the prepared graph remains valid, publish its domain result, acknowledge the prepared terminal deltas, and promote or consume the prepared state. If the transaction rolled back, the prepared state and unacknowledged deltas would remain available. An explicitly abandoned preparation would release its graph lease without acknowledging that the higher-level domain accepted it.

That capability introduces concerns that are deliberately absent from V1: immutable generation identity, graph-member leases, prepared-state recovery after restart, promotion and abandonment, source-buffer retention while a graph is frozen, output-delta continuity across several transactions, storage cleanup, and the choice between freezing one physical graph or maintaining multiple physical generations. It may also require progress reporting, resumable work allocation, and stronger resource governance.

The V1 design is intended to leave room for that later work. Graph contracts already name a complete semantic closure. Graph refreshes already have stable identifiers and source-boundary manifests. Output consumers already use durable cursors rather than assuming that the current private frontier equals the last accepted domain publication. Capability discovery can add a separately versioned future feature without changing any V1 major contract.

The later design should not be inferred from this appendix. In particular, V1 does not promise a `prepare_graph()` function, generation-specific storage, concurrent preparations, asynchronous workers, checkpoint formats, or promotion semantics. Those decisions require a dedicated proposal and a full crash, upgrade, retention, and transaction proof matrix.
