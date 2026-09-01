# Proposal: V1 Composable Refresh and Durable Delta Contracts for `pg_trickle`

## Strict transactional graph refresh and durable typed output deltas for coordinating PostgreSQL extensions

**Status:** Proposed  
**Scope:** Two independently versioned V1 capability contracts. Appendix A is non-normative and does not affect either V1 acceptance gate.
**Target:** v0.93.0 for Graph V1 contracts, v0.94.0 for strict graph refresh, and v0.95.0 for separately gated Delta V1, all before v1.0
**Decision:** Add a small, versioned SQL API that lets another PostgreSQL extension coordinate a private stream-table graph without depending on `pg_trickle` internals. Stabilize durable output consumption only after its separate retention, recovery, and upgrade gates pass.
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

`pg_trickle` should expose a generic composition contract for trusted PostgreSQL extensions that use stream tables as a relational computation layer but must publish a larger domain result under their own transaction and governance rules. The first consumer is `pg_mdm`, which wants `pg_trickle` to maintain normalized values, candidate blocks, candidate pairs, pair evidence, and golden-value candidates while `pg_mdm` remains responsible for clustering, stewardship, stable entity identifiers, reviews, provenance, and public MDM publication. The contract is domain-neutral and should also work for graph resolvers, policy engines, feature stores, search-index coordinators, and other extensions that need incrementally maintained relational facts without surrendering their own publication boundary.

The proposal defines two independent capability contracts. `external_graph_refresh` advertises integration support, exposes versioned stream-table and graph execution contracts, persists `EXTERNAL` orchestration, refreshes a complete graph strictly inside the caller's PostgreSQL transaction, and returns one coherent source-boundary manifest. This contract is sufficient for a coordinator to scan terminal stream tables, publish its own result, and commit or roll back everything together.

`output_delta_consumer` is a separate optimization contract for coordinators that have proven that complete terminal scans are too expensive. It adds durable acknowledged typed output deltas, retention, resnapshot, recovery, and backpressure. Delta V1 requires a compatible enabled Graph V1, but Graph V1 does not require Delta V1. The contracts do not share a capability version or acceptance gate. A failure to stabilize output deltas must not delay or weaken the Graph V1 release, but the pre-1.0 roadmap now blocks v1.0 until Delta V1 passes its independent v0.95.0 gate.

This proposal does not add MDM concepts to `pg_trickle`, and it does not expose private `pg_trickle` implementation state. `pg_trickle` continues to own source capture, safe frontiers, differential view maintenance, full fallback, stream-table storage, row identity, dependency ordering, and refresh finalization. The coordinating extension continues to own domain-specific decisions and public outputs. No supported integration path reads or mutates private catalogs, scheduler jobs, change buffers, generated columns, internal frontier encodings, or storage naming conventions.

---

## 2. V1 release boundary

This document contains two V1 proposals with separate acceptance boundaries.

Graph V1 is a synchronous, single-database composition path in which the coordinating extension keeps one PostgreSQL transaction open while `pg_trickle` refreshes a private graph and the coordinator publishes its own result. It supports acyclic local stream-table graphs, complete source-boundary proofs for explicitly admitted source kinds, stable execution-contract digests, complete terminal-table reads, and fail-closed behavior under concurrency or schema change.

Delta V1 adds exact output deltas when available, explicit full invalidation when an exact output delta is unavailable, owner-controlled durable cursors, and fail-closed behavior under retention loss, recovery, restore, and upgrade. v0.94.0 may advertise Graph V1 while reporting Delta V1 as absent, disabled, or experimental; v1.0 may not ship until Delta V1 is stable.

| Capability contract | Required components | Purpose |
|---|---|---|
| `external_graph_refresh` V1 | Capability discovery, execution contracts, durable `EXTERNAL` orchestration, strict graph refresh, source-boundary manifest | Prove and refresh the graph the coordinator compiled, then let it read terminal state in the same transaction |
| `output_delta_consumer` V1 | Registration, typed output batches, acknowledgement, retention, resnapshot, lineage validation | Let a coordinator consume changed terminal rows without private-buffer access or a complete terminal scan |

Neither V1 contract includes multi-transaction graph computation, resumable graph refresh, concurrent immutable graph states, background graph execution, cross-database graphs, distributed transaction coordination, arbitrary post-refresh callbacks, semantic business events, a stable Rust ABI, or external message delivery. Appendix A describes the later multi-transaction need at a high level so that V1 choices do not accidentally block it, but none of that appendix is normative and none of it may delay either V1 acceptance gate.

---

## 3. Motivation

The current `pg_trickle` public API is designed primarily for users who create a stream table and allow `pg_trickle` to keep it fresh. That remains the correct ordinary product. The API is not quite sufficient when another extension creates several private stream tables as one internal graph and must coordinate that graph with additional domain state in the same transaction.

The existing [`refresh_stream_table()`](../src/api/refresh_ops.rs) function refreshes one stream table and returns `void`. A concurrent refresh becomes a `RefreshSkipped` condition that the human-facing wrapper turns into a notice and a successful no-op. That behavior is convenient for an operator issuing an opportunistic refresh, but it is unsafe for a coordinating extension. Such a caller must distinguish "the complete graph was refreshed at this exact boundary" from "another operation was busy and nothing happened," and it needs a stable group identifier, actual graph digest, source-boundary manifest, and per-node result rather than reconstructing those facts from private tables. Delta V1 separately provides terminal output positions.

The existing [`pause_scheduler()`](../src/api/scheduler_control.rs) function is also not a durable orchestration contract. It is an operational drain control whose state and purpose are separate from stream-table scheduling ownership. A graph coordinated by another extension must remain outside scheduler dispatch after process restart, scheduler restart, failover, backup, restore, and extension upgrade. An accidental independent refresh is not merely a freshness difference: it can separate relational evidence from the higher-level membership, policy, or publication revision that is supposed to consume it.

The existing [`stream_table_spec()`](../src/api/spec.rs) function provides useful tooling metadata, but its present projection is not a complete execution-compatibility artifact. A coordinator must pin every tracked result-affecting property, including defining SQL, output schema, owner execution identity, defining search path, dependency shape, source bindings, function dependencies, collations, row-identity and probe versions, and versioned rewrite and DVM strategies. It needs a canonical digest that changes when a tracked input changes but remains stable for changes classified as contract-neutral.

Finally, `pg_trickle` already computes logical output changes internally for stream-table-to-stream-table dependencies. The current architecture captures differential output and can compute a before-and-after logical difference for supported full paths so downstream stream tables remain incremental. Those internal buffers are deliberately private. A coordinating extension needs the same logical information through a supported contract: complete old and new rows grouped into immutable batches, a durable consumer cursor, transactional acknowledgement, and an explicit `FULL_INVALIDATION` marker when an exact delta is not available.

The first missing feature is a narrow, stable graph-coordination boundary around machinery that already exists inside `pg_trickle`. Durable public output consumption is a second feature with its own storage and operational contract.

---

## 4. Goals and non-goals

### 4.1 Goals

Graph V1 should let a trusted coordinating extension treat a `pg_trickle` graph as a versioned incremental component. A successful operation must answer three questions precisely: which graph was executed, which source state it represents, and what each graph node did. The coordinator must then be able to read terminal state and write its own state in the same PostgreSQL transaction. Delta V1 additionally answers which terminal output changes remain and lets the coordinator acknowledge them in that transaction.

Both contracts preserve encapsulation. Graph V1 exposes execution contracts, a strict graph result, and an opaque but complete boundary manifest without exposing capture internals. Delta V1 exposes a typed logical delta relation and durable acknowledgements without stabilizing private buffer or payload storage.

Both contracts fail closed. Graph V1 rejects a busy graph, stale digest, incomplete boundary, unsupported contract version, or authorization failure before the coordinator can mistake incomplete work for a valid refresh. Delta V1 rejects missing output history, an incompatible output contract, an unknown row-identity version, or invalid acknowledgement. Operational limits may block work, but they may not silently skip evidence or advance a cursor.

Both contracts should remain useful beyond `pg_mdm`. Their nouns are stream table, graph, contract, boundary, output delta, batch, and consumer. They do not include domain-specific entity-resolution concepts.

### 4.2 Non-goals

Graph V1 does not allow a caller to construct, edit, advance, or repair a source frontier. `pg_trickle` remains the sole authority for source visibility and completeness. The caller receives a source-boundary manifest as evidence of what was consumed, but cannot feed arbitrary positions back as refresh instructions.

Neither V1 contract exposes private change-buffer relations. Graph V1 exposes no buffer API. Delta V1 stabilizes only logical batches, the typed row relation, acknowledgement, and gap behavior.

Graph V1 does not execute arbitrary extension callbacks from the refresh engine. The coordinator invokes `pg_trickle` through SQL and remains responsible for its subsequent domain work. This avoids an open-ended callback ABI, recursion policy, security boundary, and failure-isolation problem.

Neither V1 contract publishes domain events, delivers messages to external systems, replaces PostgreSQL logical replication, or provides a Kafka, NATS, HTTP, or outbox transport. A coordinating extension may publish its completed domain result through another mechanism after its local transaction commits.

Graph V1 does not support circular integration graphs, cross-database graphs, distributed transaction coordination, or multi-transaction graph execution. Those capabilities require separate semantics and proofs rather than hidden expansion of this contract.

---

## 5. Relationship to `pg_mdm`

The motivating architecture has a simple responsibility boundary:

> **`pg_trickle` maintains changing relational facts; `pg_mdm` decides identity.**

A `pg_mdm` definition compiles into private stream tables that project source records, normalize fields, create bounded candidate blocks, produce canonical candidate pairs, compare those pairs, maintain structured pair evidence, and maintain golden-value candidates. `pg_mdm` then consumes changes from the terminal evidence relations, computes the complete affected identity closure, applies manual decisions and component rules, reconciles durable `mdm_id` values, selects golden values, creates review items, and publishes its own output tables.

The dependency maps directly to the two capability contracts:

| `pg_mdm` requirement | `pg_trickle` contract |
|---|---|
| Verify that the installed substrate supports the required behavior | Capability discovery |
| Pin the private evidence graph to the compiled entity definition | Stream-table and graph contracts |
| Prevent evidence from advancing independently of MDM publication | `EXTERNAL` orchestration |
| Refresh the complete evidence graph inside `mdm.refresh()` | Strict transactional graph refresh |
| Explain which source state the publication represents | Source-boundary manifest |
| Publish correctly before Delta V1 is available | Complete terminal-table scan inside the strict graph transaction |
| Recompute only affected identity components when measured scale requires it | Delta V1 typed output deltas |
| Broaden safely when exact incremental impact is unavailable | Delta V1 `FULL_INVALIDATION` |
| Roll everything back if MDM validation or publication fails | Shared outer transaction; Delta V1 acknowledgement joins it when enabled |

Nothing in the SQL API assumes that `pg_mdm` is installed. `pg_mdm` is a conformance consumer and a useful end-to-end test, not a dependency of `pg_trickle`.

---

## 6. Existing `pg_trickle` foundations

This proposal is feasible because most of the difficult engine behavior already exists. `pg_trickle` maintains stream tables from declarative SQL, captures source changes through trigger and WAL paths, computes differential or full results, orders dependent stream tables through a graph, retains version frontiers, and finalizes output, frontiers, downstream change capture, cleanup, and refresh history transactionally. The current architecture also executes defining SQL under the stored stream-table owner and search-path contract, which is essential when a privileged coordinator invokes the engine.

Stream-table-to-stream-table propagation already creates logical delete and insert rows for downstream consumers. A differential upstream refresh can expose its computed delta, while a supported full upstream refresh can compare old and new contents before propagating a minimal downstream difference. Delta V1 should reuse that logical algebra, but it must not make the current private buffer schema or naming convention public.

The versioned row-identity work also provides a strong foundation. A public output delta needs an opaque identity that is exact, deterministic, and consistent across full and differential paths. The integration contract should expose the active row-identity version and complete identity bytes for comparison and ordering while leaving parsing optional and separately versioned.

Graph V1 needs canonical contracts, durable external ownership, and a strict structured graph-refresh path. Delta V1 separately needs durable typed logs and cursors over logical output changes.

---

## 7. Terminology

A **coordinating extension** is a trusted PostgreSQL extension or database component that owns or has owner-equivalent authority over a set of stream tables and invokes this contract through SQL. PostgreSQL roles, object ownership, grants, and extension membership remain the security boundary; the contract does not attempt to authenticate a shared-library filename.

A **stream-table execution contract** is the canonical, versioned description of one stream table's tracked definition, object binding, output schema, dependency versions, and execution identity. It is a syntactic and catalog contract, not a claim that `pg_trickle` can prove semantic equivalence between arbitrary SQL expressions. It is distinct from operational status, recent timings, planner statistics, scheduler tier, physical indexes, and other tuning state.

A **refresh graph** is the transitive closure of stream-table dependencies reachable upstream from one or more named root stream tables. Non-stream relations appear in the graph contract as sources, not refreshable graph members. Graph V1 admits only the local source classes listed in Section 10.

A **graph execution contract** is the canonical description and digest of the roots, member stream tables, dependency edges, deterministic topological order, output schemas, contract generations, execution identities, and external source bindings that form one refresh graph.

A **source-boundary manifest** is an immutable, machine-readable statement of the lower and upper positions, snapshot tokens, upstream stream-table revisions, completeness classes, and source-contract digests consumed by one graph refresh. Position payloads are versioned and may remain opaque to callers.

A **strict graph refresh** is one synchronous execution of a refresh graph against one immutable source-boundary decision. It may use differential, full, scoped-recomputation, reinitialization, or no-data paths per member, but every member result belongs to the same graph refresh and outer transaction.

An **output-delta batch** is the logical change in one stream table's visible contents produced by one committed refresh. An exact batch contains complete deleted rows and inserted rows. An update is represented as deletion of the complete old row followed by insertion of the complete new row. A full-invalidation batch states that the consumer must read the complete current stream table because an exact logical delta is unavailable.

An **output-delta consumer** is a durable, role-owned cursor over the output-delta batches of one stream table. Reading is side-effect free. Acknowledgement is explicit, monotonic, validated, and transactional.

---

## 8. Required invariants

The two V1 contracts are governed by the following invariants. Invariants 1 through 5 and 9 through 11 govern Graph V1. Invariants 6 through 8, 12, and 13 additionally govern Delta V1. These are stronger than any suggested catalog layout, function spelling, or Rust module structure.

1. **One outer transaction.** `refresh_graph_strict()` performs no internal commit. Graph contents, node frontiers, refresh history, and the coordinator's own writes commit or roll back with the caller's PostgreSQL transaction. When Delta V1 applies, output batches and consumer acknowledgements join that transaction.

2. **Graph-wide validation before mutation.** The complete stream-table closure is resolved, authorized, locked, reloaded, and compared with the expected graph digest before any member is refreshed.

3. **One immutable source-boundary decision.** Every graph member receives positions derived from the same graph-refresh context. No node may independently reread and substitute a newer upper boundary.

4. **Strict concurrency.** A competing refresh, query alteration, storage recreation, repair, or drop operation on any graph member produces an error or waits according to ordinary PostgreSQL lock policy. It is never converted into a successful skipped refresh.

5. **Execution contract pinning.** A refresh result names the graph digest, each member contract generation, and each member contract digest actually used. Unknown contract, rewrite, DVM, output-schema, row-identity, row-probe, or source-boundary versions fail closed.

6. **Exact delta or explicit invalidation.** A consumer receives either the complete logical output difference for a batch or an explicit `FULL_INVALIDATION` marker. Missing rows, truncated rows, expired rows, and fallback paths never masquerade as an empty exact batch.

7. **Transactional acknowledgement.** A delta cursor advances only when `ack_output_delta()` commits. If the coordinator later raises an error, both its domain publication and the acknowledgement roll back.

8. **Retention follows the slowest consumer.** Exact delta rows remain available until every active external consumer has acknowledged them or has been explicitly moved to a resynchronization-required state. Resource pressure cannot silently advance a cursor.

9. **External means externally orchestrated.** The ordinary scheduler does not queue, inline, fuse, or execute a stream table whose orchestration mode is `EXTERNAL`. This remains true across process restart and database recovery.

10. **Owner-equivalent execution.** Defining SQL continues to execute under the stored stream-table owner and defining search-path contract. The integration API grants no authority over source data, current output, or retained deleted rows beyond the authority explicitly checked for the caller.

11. **Public contract, private implementation.** A Graph V1 caller uses only the documented capability, execution-contract, refresh, and boundary APIs. A Delta V1 caller additionally uses the consumer, batch, acknowledgement, resnapshot, and returned typed-relation APIs. Neither may derive or depend on private relation names, internal catalog identifiers, LSN placeholders, frontier JSON, or change-buffer schemas.

12. **No silent recovery.** If crash recovery, backup restore, PITR, manual DDL, storage loss, or an unsupported upgrade makes exact output history unverifiable, the consumer becomes `RESNAPSHOT_REQUIRED`. Repair may reconstruct infrastructure, but it may not invent a continuous exact history that no longer exists.

13. **Full and differential equivalence.** For the same starting output, defining contracts, source boundary, and row-identity versions, any exact output delta must transform the prior stream-table multiset into the same result as complete evaluation of the defining query at that boundary.

---

## 9. Proposed V1 surface

The exact SQL spelling may change during implementation review, but the following behavior is the proposal. Once capability major version 1 is declared stable, changing these semantics requires a new capability version rather than an undocumented function change.

| Capability | Extension point | Purpose |
|---|---|---|
| Both | `pgtrickle.integration_capabilities()` | Discover independently versioned integration contracts |
| Graph V1 | `pgtrickle.stream_table_contract()` | Return the execution contract, generation, and digest for one stream table |
| Graph V1 | `pgtrickle.graph_contract()` | Return the canonical closure and digest for one refresh graph |
| Graph V1 | `pgtrickle.set_orchestration_mode()` | Persist `MANAGED` or `EXTERNAL` ownership of refresh scheduling |
| Graph V1 | `pgtrickle.refresh_graph_strict()` | Refresh a complete external graph synchronously and transactionally |
| Delta V1 | `pgtrickle.register_output_delta_consumer()` | Register a durable cursor over one external stream table's logical output changes |
| Delta V1 | `pgtrickle.output_delta_batches()` | Inspect the continuous batch sequence after the acknowledged cursor |
| Delta V1 | Returned typed delta relation | Read complete old and new rows for exact batches |
| Delta V1 | `pgtrickle.ack_output_delta()` | Acknowledge an exact or resynchronized range transactionally |
| Delta V1 | `pgtrickle.output_delta_consumer_status()` | Inspect lag, retention, contract, and recovery state |
| Delta V1 | Resnapshot helpers | Establish a new full baseline under a transaction-scoped output lock |

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
capability             major_version  minor_version  enabled  details
---------------------  -------------  -------------  -------  --------------------
external_graph_refresh  1              0              true     {...}
output_delta_consumer   1              0              false    {"status":"experimental"}
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

The `details` object may report supported PostgreSQL majors, row-identity versions, maximum graph members, admitted source-boundary classes, typed-delta encoding versions, and whether exact full-refresh deltas are available on particular execution paths. Graph V1 admits local regular and partitioned tables captured by trigger or WAL CDC, plus upstream stream tables that belong to the same graph. It rejects foreign tables, materialized-view polling sources, remote or distributed sources, and cross-database sources. A later minor version may add a source class only when it can produce one complete boundary under the same Graph V1 rules. Normative behavior belongs in the capability version and documentation, not in free-form details. The function should be inexpensive, side-effect free, and executable by `PUBLIC` because it reveals product capabilities rather than protected relation definitions or data.

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

Changing from `EXTERNAL` to `MANAGED` is rejected while a strict graph refresh holds the node, while an output resnapshot lock is active, while output consumers remain registered, or while schedule configuration is invalid for managed execution. The administrator must drop consumers explicitly before transferring scheduling ownership. The mode change does not acknowledge them or imply that an external coordinator accepted the latest output.

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
  contract_version    smallint,
  contract_generation bigint,
  contract_digest     bytea,
  contract            jsonb
)
```

The contract records every tracked input that can change the stream table's logical contents, row-identity interpretation, or execution identity. At minimum it includes an opaque stream-table object identity, schema-qualified name, and current relation binding; original and normalized defining-query digests; owner role, defining search path, and result-affecting row-security execution policy; ordered output schema with PostgreSQL type, typmod, collation, and nullability; row-identity and row-probe versions; versioned rewrite and DVM strategies; tracked function and extension dependencies; source and stream-table dependencies; source schema or execution-contract fingerprints; the explicit database-instance identity defined by v0.92 clone isolation; and the durable orchestration mode.

`contract_generation` is a per-object, monotonically increasing catalog value. The same transaction that changes any normative contract input increments it. Rollback restores the prior generation. Recreating a dropped stream table assigns a new opaque object identity; rebinding an existing object increments its generation. The generation provides a cheap invalidation check; the digest proves the complete tracked contract.

The contract excludes physical indexes, planner statistics, recent costs, worker counts, scheduler tier, batch sizes, cached plans, and other choices that may affect performance but are not tracked execution inputs. A storage or plan optimization may preserve the generation and digest only when the implementation classifies it as contract-neutral. Matching digests prove equality of the tracked representation, not semantic equivalence between different SQL expressions.

An illustrative object is:

```json
{
  "contract_version": 1,
  "contract_generation": 7,
  "stream_table": "mdm_internal.customer_pair_evidence_v3",
  "relation_identity": {
    "object_identity": "opaque-stream-table-id",
    "oid": 48122,
    "database_instance_id": "opaque-instance-id"
  },
  "execution_identity": {
    "owner": "mdm_owner",
    "defining_search_path_digest": "sha256:...",
    "row_security_policy": "OWNER_EQUIVALENT"
  },
  "query": {
    "original_digest": "sha256:...",
    "normalized_digest": "sha256:...",
    "rewrite_contract_version": 1,
    "dvm_contract_version": 1
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

Contract version 1 computes SHA-256 over an ordered typed byte encoding, not over JSON text. Each value is encoded as a fixed field tag, a type tag, a big-endian payload length, and payload bytes. Integers use fixed-width big-endian encoding, strings use UTF-8 bytes without Unicode rewriting, byte strings remain raw, null has its own type tag, and arrays start with a count followed by encoded elements in their specified canonical order. Normative fields have fixed tags and order. Sets are sorted by their encoded bytes before hashing. The JSON object is a diagnostic projection of those same values and is not itself a digest input. A contract-version change is required to alter this encoding or the normative field set.

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

The graph object contains the canonical root set, complete upstream stream-table closure, each member's contract generation and digest, dependency edges, deterministic topological order, admitted external source descriptors, and one digest of the complete graph execution contract. Duplicate roots are rejected. Capability version 1 rejects cycles even when ordinary `pg_trickle` supports a separately governed monotone-cycle mode, because a strict external contract needs explicit fixed-point semantics before cycles can be admitted.

The graph digest uses the same versioned typed encoding. Roots and members are sorted by database-local relation identity, edges by encoded endpoint pair, and the topological order uses that same relation identity as its tie-breaker. The digest changes when a root or member is added, removed, replaced, rebound, or has a new execution-contract digest. It does not change merely because an index is rebuilt, statistics change, or a different worker count would be used by ordinary managed scheduling. A graph contract is descriptive rather than a lock. `refresh_graph_strict()` re-resolves and revalidates the graph under the locks it acquires; a digest read earlier is an optimistic expectation, not permission to skip current validation.

---

## 13. Strict transactional graph refresh

### 13.1 Proposed API

The core refresh API is:

```sql
SELECT *
FROM pgtrickle.refresh_graph_strict(
  roots                 => ARRAY[
        'mdm_internal.customer_pair_evidence_v3'::regclass,
        'mdm_internal.customer_golden_candidates_v3'::regclass
    ],
  expected_graph_digest => decode(:graph_digest_hex, 'hex'),
  full_policy           => 'ALLOW'
);
```

An illustrative signature is:

```sql
pgtrickle.refresh_graph_strict(
  roots                 regclass[],
  expected_graph_digest bytea,
  full_policy           text DEFAULT 'ALLOW'
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
```

Graph V1 does not return or require output-delta tokens. When Delta V1 is enabled and a node has registered consumers, its `node_results` entry may include an `output_delta` object as an additive diagnostic. Consumers discover authoritative pending batches through the Delta V1 APIs.

`full_policy` has two initial values. `ALLOW` permits any correct full or reinitialization fallback selected by the existing engine. `ERROR` rejects a member that cannot remain on an incremental or scoped-recomputation path and is intended for diagnostics or tightly controlled workloads. The policy never forces an unsafe incremental plan.

### 13.2 Resolution and locking

The implementation performs the following logical sequence:

1. Resolve the roots and compute the complete upstream stream-table closure.
2. Reject duplicate roots, unsupported cycles, temporary relations, cross-database references, `IMMEDIATE` members, members not in `EXTERNAL` orchestration mode, and source classes not admitted by Graph V1.
3. Verify that the caller is authorized to refresh every member.
4. Sort members by a canonical database-local relation key and acquire each member's existing refresh, lifecycle, and catalog-row locks in that order. Do not use one hash of the member set as the graph lock.
5. Reload every member under lock, recompute the graph contract, and compare it with `expected_graph_digest`.
6. Lock admitted external source relations in canonical OID order, then compute one complete source-boundary decision for every external source used by the graph.
7. Allocate one graph-refresh identifier and one immutable graph-refresh context.
8. Refresh members synchronously in deterministic topological order, passing the immutable context to each member.
9. Finalize each member through the same transactional finalizer used by ordinary refresh paths. When Delta V1 applies, that finalizer also writes the member's output batch.
10. Return the structured result while transaction-scoped locks remain held.

All admission, authorization, contract, and boundary checks complete before the first member refresh begins. Any failure raises an error. The API never returns a partial list of successful members and never uses `SKIP LOCKED` or converts a busy member into a successful skip. The first implementation executes in the calling backend and does not dispatch work to independent background workers. It may reuse bounded internal batching and temporary relations as long as all effects remain within the caller's outer transaction.

### 13.3 Transaction semantics

The function performs no commit. Calling it in autocommit mode is valid, but a coordinating extension obtains composition semantics by invoking it inside its own function or explicit transaction and performing domain work before commit. Stream-table data changes, frontiers, history, cleanup, group metadata, and the coordinator's writes therefore commit or roll back together. When Delta V1 applies, output batches and acknowledgements join that same transaction.

The function returns only after every member has completed and finalized or one member has caused the call to fail. Per-member finalization remains inside the caller's outer transaction; there is no deferred graph-wide finalizer and no internal commit. Internal savepoints may be used for bounded cleanup, but they do not create progressive visibility or an independently committed successful subset. A no-data path remains a strict execution: it validates contracts, locks, and source boundaries before returning `NO_DATA`. When Delta V1 applies, it also validates output-log health and records an exact zero-row batch.

### 13.4 Coherent source boundary

The graph-refresh context contains one immutable upper-bound decision for local CDC sources and one validated position or snapshot token for every other supported source. A source shared by several graph members appears once in the manifest, and every member consumes the same upper boundary. No member may call a current-position helper again and substitute a newer value.

For local trigger or WAL sources, the position is the safe durable boundary already governed by `pg_trickle` frontier invariants. For an upstream stream table inside the graph, the downstream member consumes the output state produced earlier in the same topological refresh. Graph V1 rejects materialized-view polling, foreign, remote, distributed, and cross-database sources. Admitting any of those sources requires a later minor version with a concrete lock and boundary-proof mechanism.

An unknown, partial, unverifiable, or missing boundary always aborts Graph V1 refresh. Completeness is not optional and cannot be weakened in a minor version.

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
    "row_probe_version": 1
  }
}
```

When Delta V1 applies, the member object may also contain `"output_delta": {"batch_token": "...", "mode": "EXACT", "row_count": 48}`.

`result_class` is a small stable normalization such as `NO_DATA`, `INCREMENTAL`, or `FULL`. `action` may expose a more specific versioned engine path such as differential, full, reinitialize, TopK, or partition recomputation. Consumers should use the stable class for control flow and retain the detailed action for diagnosis.

Reported row counts are exact or `NULL` with a machine-readable quality field. An estimate must never be presented as an exact inserted or deleted count. A no-data member may still advance a validated observation frontier. When Delta V1 applies, it produces an exact zero-row batch.

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
    "database_instance_id": "opaque-instance-id"
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
        "kind": "OPAQUE_CDC_BOUND",
        "token": "opaque-local-bound"
      },
      "completeness": "PROVEN"
    }
  ]
}
```

The manifest has an authoritative digest returned separately. Callers store the object for explanation but treat individual source positions as opaque unless another capability explicitly documents them. A caller may not construct a replacement manifest, edit an upper position, or feed historical boundary JSON back into ordinary refresh as an instruction.

`database_instance_id` is the explicit capture-ownership identity required by [roadmap/v0.92.0.md](../roadmap/v0.92.0.md). PostgreSQL's system identifier and database OID are insufficient because a physical clone can preserve both. Graph V1 cannot be enabled until clone isolation creates and validates this identity before capture resumes. Graph results, refresh identifiers, Delta V1 batch tokens, and acknowledgements bind to it. A clone with a new identity cannot reuse state issued by the source database.

A boundary manifest states completeness, not merely recency. A timestamp saying when a refresh ran is not proof that every relevant source change was included. When a source cannot provide a complete boundary under the requested contract, the graph refresh fails before publication.

---

## 15. Durable output-delta consumers

### 15.1 Overview

Delta V1 is optional. A Graph V1 coordinator can always read the complete terminal stream table after strict refresh. Delta V1 avoids that scan without exposing private `changes_pgt_*` relations.

The consumer contract creates exactly one shared logical output log and one shared typed read-only relation per opted-in stream table, plus one durable acknowledgement position per consumer. It never duplicates payload by consumer. The log exists only while the stream table has external consumers, so ordinary stream tables pay no output-log storage or write cost. Delta consumers may register only on stream tables in `EXTERNAL` orchestration mode.

The output delta describes changes to the stream table itself, not raw source events. It is independent of whether the producing refresh was differential, scoped recomputation, reinitialization, or full. An exact batch transforms the previous visible stream-table multiset into the new visible multiset. If row identity remains the same while non-identity values change, the batch contains a `DELETE` carrying the complete old row followed by an `INSERT` carrying the complete new row.

The output-contract digest combines the stream-table contract generation and digest, ordered public output schema, row-identity version, and Delta V1 encoding version. It changes whenever an existing consumer could no longer interpret its baseline and later batches under the same rules.

### 15.2 Registration

The proposed registration API is:

```sql
SELECT *
FROM pgtrickle.register_output_delta_consumer(
    stream_table             => 'mdm_internal.customer_pair_evidence_v3'::regclass,
    consumer_name            => 'pg_mdm/customer/v3/pair-evidence',
    expected_contract_digest => decode(:stream_contract_digest_hex, 'hex'),
  start_position           => 'RESNAPSHOT_REQUIRED'
);
```

```sql
pgtrickle.register_output_delta_consumer(
    stream_table             regclass,
    consumer_name            text,
    expected_contract_digest bytea,
    start_position           text DEFAULT 'RESNAPSHOT_REQUIRED'
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
state_reason               text
```

Registration requires the stream table to be in `EXTERNAL` orchestration mode and compares `expected_contract_digest` with its locked stream-table contract. A managed table, stale digest, unsupported row-identity version, or unauthorized caller fails before creating consumer state.

The default creates the consumer in `RESNAPSHOT_REQUIRED` with reason `INITIAL_BASELINE_REQUIRED`. The coordinator must establish a baseline through the resnapshot protocol. `CURRENT` is accepted only when `pg_trickle` can prove that the stream table is newly created, has never completed a refresh, and is empty. That case creates an `ACTIVE` consumer at token zero. Delta V1 does not accept an unverifiable caller assertion that an existing external baseline matches current state.

Registration is transactional and idempotent for `(stream_table, owner_role, consumer_name)` only when the expected digest and options are identical. A conflicting registration raises an error rather than silently altering an existing cursor. The function creates or references the stream table's shared typed output log and read-only relation. Every consumer of that stream table receives the same `delta_relation` identity. Callers use the returned `regclass`; they do not derive its physical name.

### 15.3 Typed delta relation

The generated relation has this logical schema:

```text
batch_token       bigint   NOT NULL
ordinal           bigint   NOT NULL
action            text     NOT NULL  -- DELETE | INSERT
row_identity      bytea    NOT NULL
<stream-table output columns, preserving PostgreSQL types>
```

The relation preserves the stream table's user-visible PostgreSQL types and contains complete old values for `DELETE` and complete new values for `INSERT`. Private generated columns are not exposed, except for the opaque canonical `row_identity` needed to pair and order changes. The relation is read only. Its physical name, relation OID, and implementation are not stable. The logical schema and lookup contract remain stable for the lifetime of the consumer contract, but a returned `regclass` is only the current binding. `output_delta_consumer_status()` returns that binding, and callers must rediscover it after extension upgrade, logical restore, repair, or any reported storage recreation rather than persisting the OID as durable identity.

`ordinal` is unique within a batch, starts at zero, and is immutable once stored. A `DELETE` precedes the matching `INSERT` for a same-identity update. Order between distinct row identities has no public meaning. Consumers apply every row as a multiset operation and must not infer source transaction order from the ordinal. Consumers may page within the relation, but acknowledgement remains batch-granular so a partially read batch cannot be discarded accidentally.

The initial implementation uses one protected typed output-log table and one security-controlled read-only view per stream table. The output log is distinct from the private stream-table-to-stream-table buffer so this public contract does not freeze an existing internal schema. Physical names are not stable. A later implementation may change physical storage when it preserves the same single-copy payload, public relation behavior, types, retention, and upgrade boundary.

### 15.4 Batch metadata

Consumers inspect batches through:

```sql
SELECT *
FROM pgtrickle.output_delta_batches(
    consumer_id   => :consumer_id,
  through_token => :through_token
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

Batch tokens are positive, monotonically increasing integers scoped to one stream table's output log and database-instance identity. The log head is a catalog counter locked and incremented in the same transaction that inserts batch metadata and payload. PostgreSQL sequences must not allocate these tokens because sequence increments do not roll back. Every committed refresh of an opted-in stream table records exactly one metadata row, including exact zero-row and invalidation batches. Token zero denotes the pristine empty origin and never has payload.

The result begins after the consumer's acknowledged token and ends at `through_token`, or at the current log head when no upper token is supplied. It must return every metadata token in that closed range in order. A missing token, payload relation, required payload row, or continuity sentinel is treated immediately as `RESNAPSHOT_REQUIRED` with reason `LOG_INCOMPLETE` and raises a delta-gap error. The failing request does not claim to persist that state transition because PostgreSQL rolls back statement changes when it raises the error. Startup recovery, repair, or an explicit non-throwing validation operation must acquire the consumer lock and commit the transition in a separate transaction. Until that succeeds, every read continues to fail closed on the same integrity check. Missing history is never treated as no changes.

The initial modes are:

| Mode | Meaning |
|---|---|
| `EXACT` | The typed delta relation contains the complete logical delete and insert rows for the batch; an exact batch may contain zero rows |
| `FULL_INVALIDATION` | An exact logical row delta is unavailable; the consumer must read the complete current stream table at the transactionally stable graph result before acknowledging |

A differential refresh produces an exact batch when the output log is healthy. A full refresh produces an exact batch when the refresh path already has a complete before-and-after logical difference. Otherwise it produces `FULL_INVALIDATION`. Delta V1 does not require `pg_trickle` to perform an additional unbounded full-table diff solely to satisfy an external consumer. Initial population may be represented as exact insertions when already materialized, but invalidation is always a permitted truthful result.

### 15.5 Reading and acknowledgement

An `ACTIVE` Delta V1 coordinator refreshes the graph, then calls `output_delta_batches()` for each terminal consumer while the strict graph locks remain held by the caller's transaction. The newest returned batch token is the acknowledgement target. If every batch in the range is exact, the coordinator reads the typed relation through that token and applies the logical changes. If the range contains a full invalidation, it reads the complete current terminal stream table and runs its full reference computation.

Acknowledgement is explicit:

```sql
SELECT pgtrickle.ack_output_delta(
    consumer_id   => :consumer_id,
  through_token => :through_token,
    disposition   => 'APPLIED'
);
```

`disposition` is `APPLIED` when all acknowledged batches were exact and the consumer applied their rows. It is `RESYNCHRONIZED` when the range contained an invalidation and the consumer rebuilt from the complete terminal table. `APPLIED` is rejected for a range containing `FULL_INVALIDATION`. `RESYNCHRONIZED` requires the output lock held by the strict graph refresh or resnapshot protocol. The acknowledgement is monotonic, validates the output contract and row-identity version, and updates the durable cursor in the caller's transaction.

Reading never acknowledges. `ack_output_delta()` is valid only in `ACTIVE`; a consumer in `RESNAPSHOT_REQUIRED` must use the resnapshot protocol. Acknowledging an already acknowledged token is an idempotent no-op only when it belongs to the same consumer, output contract, and database-instance identity. Acknowledging beyond the current log head, across a gap, across an invalid contract, or out of order raises a stable error.

### 15.6 Resnapshot protocol

A consumer that lacks a matching baseline or loses exact history enters `RESNAPSHOT_REQUIRED`. It establishes a new baseline through a transaction-scoped protocol:

```sql
SELECT *
FROM pgtrickle.begin_output_delta_resnapshot(:consumer_id);

-- Read the complete stream table returned by the status record and rebuild
-- consumer-owned state while the transaction-scoped output lock is held.

SELECT pgtrickle.ack_output_delta_resnapshot(
    consumer_id      => :consumer_id,
    resnapshot_token => :resnapshot_token
);
```

The begin call is valid only in `RESNAPSHOT_REQUIRED`. It returns the stream table, current output-log head, output-contract digest, row-identity version, and a one-use token bound to the consumer and database-instance identity. It locks the stream table against refresh or contract change until transaction end. The acknowledgement advances the cursor to that head, clears the reason, and moves the consumer to `ACTIVE`. Both calls and the complete terminal-table read must occur in one transaction. A rollback leaves the cursor and consumer state unchanged.

### 15.7 Consumer lifecycle

A consumer has one of these states:

```text
ACTIVE
PAUSED
RESNAPSHOT_REQUIRED
INVALIDATED
DROPPED
```

`ACTIVE` means a continuous exact-or-invalidation sequence exists after the cursor. `PAUSED` retains history but rejects acknowledgement and is an operator control. `RESNAPSHOT_REQUIRED` means the consumer has no proven baseline or retained history became unavailable. `INVALIDATED` means the stream-table or output contract changed and the consumer must be recreated or explicitly rebound after a new baseline. `DROPPED` may remain only in audit history.

State transitions record one stable reason code:

| Reason | State | Meaning |
|---|---|---|
| `INITIAL_BASELINE_REQUIRED` | `RESNAPSHOT_REQUIRED` | A new consumer has no proven baseline |
| `HISTORY_EXPIRED` | `RESNAPSHOT_REQUIRED` | An administrator explicitly discarded history behind the cursor |
| `LOG_INCOMPLETE` | `RESNAPSHOT_REQUIRED` | Metadata, payload, relation, or continuity validation failed |
| `RECOVERY_INCOMPLETE` | `RESNAPSHOT_REQUIRED` | Restored consumer state is ahead of recoverable output history |
| `DATABASE_INSTANCE_CHANGED` | `RESNAPSHOT_REQUIRED` | Clone activation assigned a new database-instance identity |
| `CONTRACT_CHANGED` | `INVALIDATED` | The stream-table or output contract no longer matches registration |
| `ADMIN_RESET` | `RESNAPSHOT_REQUIRED` | An administrator explicitly requested a new baseline |

An execution-contract or output-contract-changing `ALTER QUERY` is rejected while any nonterminal consumer remains registered under Delta V1, including `ACTIVE`, `PAUSED`, `RESNAPSHOT_REQUIRED`, and `INVALIDATED`. `pg_mdm` avoids in-place mutation by creating a new private graph for each entity-definition version. Dropping a stream table requires dropping or explicitly cascading its consumers. Dropping a consumer is authorization-checked and may allow retained rows to be cleaned immediately.

### 15.8 Retention and backpressure

The output log is retained through the minimum acknowledged batch among every consumer whose state still claims continuity. `ACTIVE` and `PAUSED` consumers both pin history. `RESNAPSHOT_REQUIRED`, `INVALIDATED`, and `DROPPED` consumers stop pinning discarded history only after the transaction that records their discarded-through token or terminal state commits. A future prepared binding also participates in this minimum while active. Internal downstream stream-table consumers continue to use their private frontier contract; an implementation may combine retention minima physically, but neither public cursor is represented as the other.

The default V1 retention behavior is fail closed. A slow consumer may retain substantial data, and `pg_trickle` must expose row, byte, batch, and age lag. Soft limits warn but do not delete history. If finalizing a new exact batch would cross an enabled hard limit, the refresh raises `PGT_EXT_CONSUMER_BLOCKED` before inserting batch metadata, payload, or advancing the transactional log head. The outer refresh transaction then rolls back.

An administrator may explicitly move one or more blocking consumers to `RESNAPSHOT_REQUIRED` with reason `HISTORY_EXPIRED` or `ADMIN_RESET`. That transaction records the discarded-through token before cleanup becomes eligible. The administrator then retries the refresh. Delta V1 never expires history, truncates the current batch, or changes consumer state automatically merely because a size or age threshold elapsed.

Output logs required by continuity-pinning consumers are logged and crash-safe. Loss of a log sentinel, batch metadata, typed relation, or continuity proof is treated as `RESNAPSHOT_REQUIRED`; it is never interpreted as no changes. Request paths raise without claiming a durable state change, while startup recovery, repair, or explicit validation records the transition in its own committing transaction.

---

## 16. V1 end-to-end protocols

### 16.1 Graph V1 with complete terminal reads

Graph V1 is complete without Delta V1:

```sql
BEGIN;

-- The coordinating extension stored this digest when it created or activated
-- its private graph definition.
SELECT *
INTO TEMP TABLE _graph_result
FROM pgtrickle.refresh_graph_strict(
  roots                 => ARRAY[
        'mdm_internal.customer_pair_evidence_v3'::regclass,
        'mdm_internal.customer_golden_candidates_v3'::regclass
    ],
  expected_graph_digest => decode(:expected_graph_digest, 'hex'),
  full_policy           => 'ALLOW'
);

-- Read the complete terminal stream tables while graph locks remain held.
SELECT * FROM mdm_internal.customer_pair_evidence_v3;
SELECT * FROM mdm_internal.customer_golden_candidates_v3;

-- Perform domain-specific work. pg_trickle does not know this schema.
SELECT mdm_internal.resolve_and_publish_customer(
    graph_refresh_id       => :graph_refresh_id,
    source_boundary        => :source_boundary,
    source_boundary_digest => :source_boundary_digest
);

  COMMIT;
  ```

  ### 16.2 Delta V1 optimization

  When Delta V1 is enabled, an `ACTIVE` consumer replaces each complete terminal read with this sequence inside the same transaction:

  ```sql
  -- Read every pending batch through the head visible after strict refresh.
  SELECT *
  FROM pgtrickle.output_delta_batches(
    consumer_id => :consumer_id
  )
  ORDER BY batch_token;

  -- Apply all exact rows from delta_relation. If any batch is
  -- FULL_INVALIDATION, rebuild from the complete terminal stream table instead.

  -- Acknowledge only after the coordinator has validated and written its state.
  SELECT pgtrickle.ack_output_delta(
    consumer_id   => :consumer_id,
    through_token => :newest_batch_token,
    disposition   => :disposition
  );
```

  If domain work raises an error, the transaction rolls back. Private stream tables return to their previous contents, node frontiers do not advance, and graph and node refresh records do not commit. Under Delta V1, newly written output batches also disappear and consumer cursors remain unchanged. The next invocation can repeat the complete operation without reconciling a half-published boundary.

The coordinator does not need to block new source writes for the duration of the transaction. `pg_trickle` captures a safe upper boundary and leaves later committed changes pending for the next refresh. The important property is not that the database stops changing; it is that every graph member and every domain output in this transaction refers to the same named boundary.

---

## 17. Security and authorization

The composition API is intended for trusted in-database coordinators, but it must not create a privilege-escalation path. Mutating functions should not be executable by `PUBLIC` by default. Installation may create a predefined integration role, or administrators may grant the relevant functions directly to the dedicated owner role used by the coordinating extension.

`refresh_graph_strict()` must verify ownership or a narrowly documented owner-equivalent maintenance privilege for every member in the graph closure. Capability version 1 should require one effective owner across the graph because mixed-owner execution complicates authorization, RLS, retained deleted rows, and lifecycle coordination. Defining queries continue to execute under each stream table's stored owner identity and defining search path rather than the invoker's accidental session context.

Output-delta registration, reading, resnapshot, acknowledgement, pause, and deletion should be restricted to the stream-table owner or superuser in V1. This avoids a subtle information leak from deleted rows retained in output history, because ordinary table RLS cannot reliably be reevaluated against a row that no longer exists. A later delegated-consumer design would require explicit row filters, masking rules, and retained-row policy rather than an informal grant.

Any durable consumer row that stores a PostgreSQL role OID must register a PostgreSQL shared dependency on that role, or use an equivalent mechanism that prevents the role from being dropped while the consumer exists. A dangling owner OID is invalid state, and later OID reuse must never transfer consumer authority to an unrelated role. Reassignment or removal therefore uses an explicit authorization-checked consumer lifecycle operation rather than raw catalog mutation.

Every `SECURITY DEFINER` entry point must use a fixed safe `search_path`, resolve objects through PostgreSQL catalogs, distinguish identifiers from values in generated SQL, and avoid exposing private relation names to unauthorized callers. Consumer and resnapshot tokens are references only after authorization; knowledge of a UUID is not authority.

---

## 18. Concurrency and transaction semantics

### 18.1 Lock ordering

Strict graph refresh resolves the complete closure and acquires all graph-member locks in one canonical order before executing the first member. The order must be independent of root-array order and shared with alter, repair, drop, resnapshot, and orchestration-mode transitions so that callers do not create lock-order cycles. One shared lock-plan helper must define the canonical member key and acquire, for each complete member set, refresh or lifecycle locks before catalog-row locks, followed by required member storage locks. A path may omit a lock class it does not need, but it may not invert the remaining order. Multi-member lifecycle operations lock the complete set before mutating any prefix. Source relation locks required by a full baseline follow the existing source-lock protocol and are then acquired in ascending relation OID. Delta V1 consumer locks follow member and source locks in ascending UUID byte order, followed by batch and payload relations. The strict API uses blocking locks governed by `lock_timeout`, or an explicit no-wait variant, and never uses `SKIP LOCKED`.

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

An execution-contract change discovered through recovery, unsupported mutation, or mandatory correctness repair invalidates affected consumers rather than reinterpreting retained rows. Supported contract-changing lifecycle operations are rejected while any nonterminal consumer remains registered. A contract-neutral physical change may preserve the contract and consumer continuity. Clone activation assigns and validates a different database-instance identity, so copied tokens are rejected even when relation OIDs happen to match.

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

The catalog also retains `contract_generation`, the contract version, and enough tracked metadata to build and validate the contract deterministically. A cached digest may be stored as an optimization, but the same transaction that changes a normative input increments the generation and invalidates the digest.

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
  database_instance_id
  owner_role
  consumer_name
  output_contract_digest
  row_identity_version
  acknowledged_batch_token
  discarded_through_token
  state
  state_reason
  created_at
  acknowledged_at
```

Batch metadata is also logged:

```text
pgt_output_delta_batches
  stream_table_id
  database_instance_id
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

Each opted-in stream table also has one locked `output_log_head` counter, one protected typed payload relation, and one shared read-only view. The finalizer increments the counter and writes batch metadata and payload in one transaction. Rollback restores the previous head. Consumer authorization is enforced before returning or reading the shared view; consumer-specific payload copies and views are not created. Batch tokens have meaning only with their stream-table identity, output contract, and database-instance identity.

### 21.4 Resnapshot state

A resnapshot token may be represented as a signed or catalog-backed one-use record bound to consumer, database-instance identity, stream-table identity, output contract, stable batch token, owner, and transaction. Its implementation must prevent replay against another consumer or later incompatible contract. No long-lived application secret is required; PostgreSQL authorization remains primary.

---

## 22. Integration with the refresh engine

The strict graph API should call the same validated execution and common-finalizer code used by scheduler and manual refresh paths. The main refactoring is to separate the engine result from the current human-facing wrapper so a strict caller receives `Result<GraphRefreshResult, PgTrickleError>` rather than a notice on `RefreshSkipped`.

The scheduler already computes dependency-aware execution units and database-local source bounds. The strict path can create one execution unit for the requested closure, capture one immutable boundary context, acquire deterministic locks, and invoke existing member refresh machinery in topological order. It should not synthesize sequential calls to the public `refresh_stream_table()` wrapper because that would repeat boundary decisions, lose group identity, and retain notice-and-skip semantics.

The contract builder can reuse stream-table metadata, dependency catalogs, defining-query hashes, owner execution state, row-identity and row-probe versions, function fingerprints, collation validation, source schema fingerprints, and versioned strategy fields already present in the system. A dedicated canonicalization module should define digest inputs, with independent fixtures so an incidental JSON serialization refactor cannot change a contract silently.

When Delta V1 is enabled for a member, output-batch creation belongs in the common finalizer. That member's frontier cannot commit without the corresponding batch metadata and exact payload or `FULL_INVALIDATION` marker. Differential output can reuse the logical D+I relation already produced by the engine. Supported full paths can reuse their existing before-and-after diff. Other full paths write invalidation metadata rather than an incomplete payload. A Graph V1-only member finalizes without an external output batch.

Under Delta V1, the external output log remains distinct from private downstream buffers. This increases write amplification for opted-in stream tables but permits private buffer evolution. Shared physical storage may be considered in a later Delta capability major only if it preserves the public cursor, type, retention, and upgrade semantics.

---

## 23. Lifecycle interactions

### 23.1 Create and bulk create

A coordinating extension normally creates an immutable private graph with `orchestration_mode = 'EXTERNAL'` and `initialize = false`, computes and stores its graph digest, and performs first population through `refresh_graph_strict()`. A Delta V1 coordinator may register terminal consumers at the pristine `CURRENT` origin before first refresh or use the default resnapshot protocol after population. Bulk creation should validate all names, ownership, modes, and graph constraints before making any member visible.

### 23.2 Alter query and storage recreation

A stream table with active output consumers cannot undergo an execution-contract or output-contract change in place under Delta V1. The caller creates a new stream table or graph version, registers new consumers, establishes a new baseline, activates the corresponding higher-level definition, and later removes the old graph after its audit or explanation horizon is satisfied.

Operational alterations classified as contract-neutral, such as supported index maintenance or statistics updates, may proceed when they do not conflict with an active refresh. Other alterations increment the stream-table contract generation and change the graph digest.

### 23.3 Suspend, resume, and repair

Suspending an external stream table blocks strict graph refresh but does not transfer it to the scheduler. Resuming makes it explicitly refreshable again. Repair always validates execution contracts. When Delta V1 applies, it also validates output-log continuity and reports whether consumers remain active or require resnapshot. It never acknowledges on behalf of a coordinator.

### 23.4 Drop

Under Delta V1, dropping a stream table fails while output consumers exist unless the caller uses a documented cascade operation. A privileged cascade explicitly invalidates or drops consumers, records the consequence, removes the shared view and payload storage, and then follows ordinary dependency cleanup. It may not leave another extension believing that its acknowledged cursor has a continuous future stream.

### 23.5 Orchestration-mode changes

Returning a table to `MANAGED` mode is rejected while Delta V1 consumers exist. Administrators must drop them explicitly first; the drop records the final cursor state and makes retained history eligible for cleanup. A mode change is a lifecycle operation, not an acknowledgement shortcut.

---

## 24. Observability and administration

The capability contracts should add concise supported status functions rather than require operators to join private catalogs:

```sql
SELECT * FROM pgtrickle.external_graph_status(roots => ARRAY[...]::regclass[]);
SELECT * FROM pgtrickle.output_delta_consumer_status();
```

Graph V1 status reports graph digest, member count, orchestration eligibility, current busy state, most recent graph refresh, and source-boundary completeness. Delta V1 consumer status reports owner, stream table, state, acknowledged and newest batch token, retained rows and bytes, batch lag, age lag, output-contract digest, row-identity version, and state reason.

Graph V1 health checks warn when an external graph contains a managed or unsupported member or when its execution contract can no longer be computed. Delta V1 adds warnings for consumers that need resnapshot, are invalidated, exceed advisory thresholds, reference missing storage, or approach a hard retention limit. Warnings are operational and do not change state.

External graph refresh history identifies `initiated_by = 'EXTERNAL_GRAPH'`, the graph refresh identifier, and the graph digest while preserving per-member history. Graph V1 counters include strict refresh totals and failures, busy conflicts, and boundary failures. Delta V1 adds exact and invalidation batches, output rows and retained bytes, consumer gaps, resnapshots, and acknowledgement latency.

---

## 25. Performance and resource behavior

Graph V1 adds no output-log overhead. Delta V1 adds none to a stream table without a registered consumer. Capability discovery is constant-size metadata. Contract digests may be cached by a complete execution-contract key and invalidated by the same catalog and function-dependency mechanisms that invalidate query plans.

A strict graph refresh may be cheaper than repeated single-node public refreshes because it computes one closure, one lock plan, one source-boundary context, and one topological traversal. It should not repeatedly probe the same source, parse the same contract, or rebuild identical dependency state. The locked database catalog remains authoritative even when a cached DAG is reused.

Under Delta V1, output capture adds one shared write per logical changed row regardless of consumer count. Per-consumer cost is one cursor row; all consumers use the stream table's shared typed view. Batch metadata adds one row per opted-in stream-table refresh, including no-data batches.

A slow Delta V1 consumer can retain substantial output history. Advisory limits must be observable. A hard limit blocks refresh before the finalizer changes the output-log head or writes a batch. Only an explicit administrator transition to `RESNAPSHOT_REQUIRED` makes unacknowledged history eligible for cleanup. If a complete exact batch cannot be written, the finalizer writes a truthful allowed invalidation marker or aborts.

Graph V1 publishes conservative admission bounds for root count, graph members, dependency edges, and contract size. Delta V1 separately bounds consumers per stream table and batch pagination. Exceeding a bound fails before mutation and does not process an arbitrary prefix. Neither V1 contract promises resumability; callers must operate within documented transaction, WAL, lock, memory, and temporary-space limits.

---

## 26. Upgrade and compatibility policy

The proposal is additive. Existing stream tables receive `orchestration_mode = 'MANAGED'`. Existing APIs, including the human-friendly notice-and-skip behavior of `refresh_stream_table()`, remain unchanged. No external output log exists until a Delta V1 consumer registers.

Capability major versions are independent of each other and of the extension package version. A change to canonical execution-contract encoding, graph closure, strict-refresh semantics, or source-boundary meaning requires a new `external_graph_refresh` major. A change to output-row semantics, acknowledgement rules, token validation, retention, or resnapshot behavior requires a new `output_delta_consumer` major. Either change needs side-by-side support or an explicit integrator migration.

A mandatory correctness repair that changes tracked logical behavior increments the relevant rewrite or DVM contract version and the stream-table contract generation. A coordinating extension then refuses to refresh the old graph until it creates or explicitly adopts a new definition. A repair must not claim compatibility merely because the output column list is unchanged.

Graph V1 upgrade scripts preserve durable orchestration mode, contract generations, contract versions, and graph refresh identities. Delta V1 scripts additionally preserve consumer registrations and cursors, the transactional log head, batch metadata, and output-log payload. `pg_dump`, physical backup, PITR, and failover treat enabled capability state as durable. The shared typed view may be recreated deterministically from catalog state, and status returns its current `regclass` binding after recreation. Callers must not treat a prior relation OID as durable. Missing payload history requires resnapshot under the separate-transition rule in Section 15.4.

Both V1 contracts are SQL-facing. Coordinating extensions must not link to private Rust modules or assume a stable Rust ABI. Runtime capability negotiation is authoritative even when package dependencies pin a supported release line.

---

## 27. Delivery plan

The work should be delivered in three phases with separate stability gates.

### Phase 1: contracts and external ownership

Add capability discovery, canonical stream-table execution contracts, graph contracts, graph digests, contract generations, and durable orchestration mode. Extend create, bulk-create, status, scheduler admission, lifecycle locks, dump, restore, and upgrade handling. This phase has no external output log and can be tested independently.

### Phase 2: strict graph refresh

Extract or reuse one structured internal graph-refresh context, acquire all external-member locks before execution, compute one immutable source boundary, execute members synchronously in topological order, and return a typed graph result. Wire every member through the common transactional finalizer and add stable busy, contract, completeness, and mode errors. At the end of this phase, Graph V1 is complete. It may advertise stable version 1 after its transaction, concurrency, crash, clone-isolation, security, and upgrade tests pass.

### Phase 3: durable typed output deltas

Add consumer registration, shared typed output logs and read-only relations, transactional batch metadata, exact differential capture, full invalidation, acknowledgement, resnapshot, retention, status, and repair behavior. Integrate batch creation into the common finalizer for opted-in stream tables. Delta V1 remains absent, disabled, or experimental until its separate algebra, retention, recovery, clone-isolation, security, and upgrade tests pass.

Each capability needs its own operational documentation, metrics, capacity guidance, backup and restore verification, upgrade matrix, and conformance tests before discovery advertises stable major version 1. During implementation, discovery reports that capability as experimental or `enabled = false`; the status of one capability does not affect the other.

---

## 28. Test plan

### 28.1 Graph V1 unit and property tests

Canonical contract fixtures pin the typed byte encoding and prove deterministic ordering, exclusion of operational fields, inclusion of every normative field, generation changes for tracked mutations, and stability for contract-neutral tuning. Graph generators cover duplicate roots, shared upstream members, diamonds, unsupported cycles, renames, object recreation, owner changes, and dependency changes. They verify one canonical closure and member lock order.

### 28.2 Graph V1 integration tests

At minimum, Graph V1 tests cover:

1. One-node, chain, and diamond graphs committing against one complete source boundary.
2. A coordinator error after refresh, proving stream output, frontiers, graph history, and coordinator writes all roll back.
3. Concurrent refresh, alter, repair, mode change, and drop operations, proving canonical lock behavior and no successful skip.
4. A contract change between inspection and locked refresh, proving mismatch before the first member executes.
5. Source writers on both sides of the safe boundary, proving no lost or double-consumed changes.
6. Rejection of every unadmitted source class before member execution.
7. Scheduler and PostgreSQL restart with durable `EXTERNAL` ownership.
8. Crash, physical restore, logical restore, clone activation, and extension upgrade with contract and database-instance validation.

### 28.3 Delta V1 unit and property tests

Output-delta algebra tests compare old and new stream-table multisets with every exact batch after inserts, deletes, same-identity updates, key-changing updates, duplicates, aggregates, joins, set operations, TopK paths, no-op changes, and full fallback. Applying all exposed deletes and inserts must produce the full query result at the same source boundary. State-machine tests cover default registration, pristine `CURRENT`, idempotency, transactional token allocation, acknowledgement, invalidation disposition, resnapshot, gaps, pause and resume, multiple consumers, drop, and contract change. They also prove that an error cannot claim a committed state transition and that non-throwing validation can persist the same transition.

### 28.4 Delta V1 integration tests

At minimum, Delta V1 tests cover:

1. Exact differential output and exact full output, including delete-plus-insert updates and zero-row batches.
2. Full refresh without an exact diff, producing one `FULL_INVALIDATION` and no partial payload.
3. Several consumers sharing one typed relation while cleanup follows the slowest `ACTIVE` or `PAUSED` cursor, plus any active prepared binding.
4. Refresh and acknowledgement rollback, proving the log head, payload, and cursor all return to their prior state without a token gap.
5. Hard retention backpressure before metadata, payload, or log-head mutation.
6. Explicit resnapshot, proving the complete read and cursor activation commit together.
7. Contract change, missing storage, damaged continuity, recovery rollback, role drop, relation rebinding, and clone activation, proving the documented state and reason code.
8. Registration rejection for managed stream tables and non-pristine `CURRENT` requests.

### 28.5 Security and compatibility tests

Graph V1 tests cover function grants, unauthorized graph members, mixed owners, hostile `search_path`, RLS-enabled sources, private relation disclosure, and malformed catalog state. Delta V1 adds guessed consumer and resnapshot tokens, direct DML against the shared typed relation, and retained deleted-row access. Compatibility fixtures pin each capability result independently. Graph fixtures also pin contract encoding and graph digests; Delta fixtures pin batch ordering, token validation, state reasons, and resnapshot behavior across supported upgrades.

### 28.6 Performance tests

Graph V1 benchmarks measure strict graph overhead against repeated single-node refresh and contract-digest cache cost. Delta V1 separately measures output-log write amplification, shared typed-view scan throughput, batch metadata cost, cleanup with several consumers, and zero overhead when no consumer exists. Performance gates must never weaken exactness or failure behavior.

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

A full scan is the complete Graph V1 consumption path and the Delta V1 invalidation and resnapshot fallback. It may be too expensive for large terminal relations, which is the measured reason to enable the separate Delta V1 contract.

### 29.8 Add an arbitrary post-refresh callback

A callback that invokes another extension after each refresh creates difficult transaction, authorization, recursion, error-isolation, resource, and versioning questions. It also makes the `pg_trickle` scheduler the owner of the higher-level workflow. The explicit SQL coordinator model is easier to test and leaves domain semantics with the domain extension.

### 29.9 Link to or fork private Rust modules

PostgreSQL extensions do not have a stable Rust ABI, and a fork would duplicate the most difficult CDC, DVM, frontier, and upgrade work. A small SQL compatibility surface is a cleaner long-term boundary.

---

## 30. Risks and open implementation choices

Graph V1's main risk is claiming source coherence that the engine cannot prove. Its closed source admission list, typed boundary manifest, and fail-closed refresh rule address that risk. Several incomparable source positions must not be reduced to a timestamp.

Delta V1 has the larger implementation risk. A public log constrains cleanup and upgrades, a failed coordinator can retain unbounded history, and retained deleted rows can leak data. The contract therefore fixes transactional per-stream-table tokens, single-copy typed payload, owner-only access, explicit acknowledgement, observable lag, refresh backpressure, and administrator-controlled resnapshot. It does not stabilize private downstream buffers or physical output-log names.

Both contracts add API and long-term compatibility cost. Capability discovery may return rows or one versioned JSON document, graph and refresh results may use named composites or versioned JSON subdocuments, and busy behavior may rely on ordinary `lock_timeout` rather than a separate option. These representation choices must be settled before the relevant capability advertises stable major version 1. Decisions already fixed by this proposal, including source admission, typed digest encoding, per-stream-table output logs and tokens, shared typed views, and state transitions, are not implementation options.

---

## 31. Acceptance criteria

### 31.1 Graph V1

Graph V1 is complete when a small conformance extension can use supported SQL APIs to:

1. Discover and require `external_graph_refresh` by major and minimum minor version.
2. Create an acyclic private graph durably in `EXTERNAL` mode and obtain canonical member generations, execution-contract digests, and one graph digest.
3. Refresh the complete graph synchronously against one coherent, proven source boundary and receive one structured result.
4. Read complete terminal tables and publish coordinator-owned state before committing the same transaction.
5. Receive stable errors for busy, changed, unsupported, incomplete, unauthorized, or failed graphs with no committed member subset.
6. Roll back after refresh and observe graph contents, frontiers, graph history, and coordinator state unchanged.
7. Survive restart, backup and restore, failover, clone activation, and supported upgrade without losing external ownership or accepting stale identity.
8. Enforce owner checks, safe search paths, bounded inputs, contract validation, status, and health diagnostics.
9. Implement the `pg_mdm` V1 refresh and publication transaction without reading or writing private `pg_trickle` catalogs, buffers, frontiers, scheduler tables, or generated columns.

### 31.2 Delta V1

Delta V1 is complete when the conformance extension can independently:

1. Discover `output_delta_consumer`, register only on an external stream table, and establish a proven baseline through pristine `CURRENT` or resnapshot.
2. Read every immutable batch after its cursor from the stream table's shared typed relation and prove that each exact batch transforms the prior multiset into the full result at the named boundary.
3. Acknowledge exact batches or resynchronize after `FULL_INVALIDATION`, committing output batches, coordinator state, and cursors together.
4. Roll back refresh or acknowledgement without advancing the transactional log head or leaving a token gap.
5. Retain independent cursors without duplicating payload and clean only through the slowest cursor or an explicit resnapshot transition.
6. Fail closed with the specified state and reason when history, storage, contract, or database-instance validation fails.
7. Enforce owner-only deleted-row access, hard-limit backpressure, stable errors, lag status, and health diagnostics.

Nothing in Appendix A is part of either acceptance gate.

---

## 32. Recommended disposition

Adopt Graph V1 as a focused pre-1.0 extension boundary. Deliver durable external orchestration and execution contracts first, then strict transactional graph refresh. Advertise `external_graph_refresh` major version 1 when its acceptance gate passes.

Pursue Delta V1 as a separate optimization. Keep `output_delta_consumer` absent, disabled, or experimental until its own algebra, transaction, retention, crash, security, clone, and upgrade proofs pass. Delta V1 must not delay Graph V1 or weaken its complete terminal-read path, but its independent v0.95.0 gate must pass before v1.0.

The proposed boundary preserves the responsibilities of both projects:

> **`pg_trickle` owns complete incremental relational facts and their source boundaries. A coordinating extension owns the higher-level decisions made from those facts. PostgreSQL owns the transaction that commits them together.**

---

# Appendix A: high-level V2 needs

**This appendix is non-normative. It summarizes the `pg_mdm` V2 need only. It is not part of the V1 decision, delivery plan, implementation estimate, or acceptance criteria, and every capability described here requires a separate proposal before implementation.**

Graph V1 keeps graph refresh, domain computation, and domain publication inside one PostgreSQL transaction. Delta V1 adds acknowledgement to that transaction. This model is simple and strong for small and medium workloads, but a higher-level resolver may eventually need minutes or hours of component analysis, stable-identity reconciliation, validation, or preview work. Holding one database transaction and one set of graph locks for that entire period may become operationally unacceptable.

A later capability would therefore need an immutable prepared graph result at a named source boundary. `pg_trickle` would refresh the graph, commit the private relational state, and seal the exact member contracts, source-boundary manifest, terminal output state, and delta positions. The coordinating extension could then perform deterministic checkpointed work across several transactions while source changes beyond the sealed boundary continued to accumulate for a later refresh.

Final publication would still need one short atomic transaction. In that transaction, the coordinator would verify that the prepared graph remains valid, publish its domain result, acknowledge the prepared terminal deltas, and promote or consume the prepared state. If the transaction rolled back, the prepared state and unacknowledged deltas would remain available. An explicitly abandoned preparation would release its graph lease without acknowledging that the higher-level domain accepted it.

That capability introduces concerns that are deliberately absent from V1: immutable generation identity, graph-member leases, prepared-state recovery after restart, promotion and abandonment, source-buffer retention while a graph is frozen, output-delta continuity across several transactions, storage cleanup, and the choice between freezing one physical graph or maintaining multiple physical generations. It may also require progress reporting, resumable work allocation, and stronger resource governance.

The V1 design is intended to leave room for that later work. Graph V1 contracts already name a complete execution closure, and graph refreshes already have stable identifiers and source-boundary manifests. Delta V1 consumers use durable cursors rather than assuming that the current private frontier equals the last accepted domain publication. Capability discovery can add a separately versioned future feature without changing either V1 major contract.

The later design should not be inferred from this appendix. In particular, V1 does not promise a `prepare_graph()` function, generation-specific storage, concurrent preparations, asynchronous workers, checkpoint formats, or promotion semantics. Those decisions require a dedicated proposal and a full crash, upgrade, retention, and transaction proof matrix.
