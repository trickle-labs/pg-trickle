# Proposal: V2 Prepared Graph Generations for `pg_trickle`

## Immutable cross-transaction graph state and atomic acceptance for coordinating PostgreSQL extensions

**Status:** Proposed  
**Scope:** V2-only integration capability. This proposal does not alter or enlarge the V1 acceptance boundary.  
**Target:** Additive capability after the V1 composable refresh and durable delta contract  
**Decision:** Add a versioned prepared-generation lifecycle that lets an authorized extension refresh an externally orchestrated stream-table graph, commit and freeze that exact result, process it across several later transactions, and finally promote or abandon it without losing source or output-delta continuity  
**Compatibility:** Existing stream tables and the V1 integration APIs retain their behavior. Prepared generations are opt-in and apply only to eligible `EXTERNAL` graphs.  
**Motivating consumer:** `pg_mdm` V2, particularly large entity-resolution runs that cannot safely or economically remain inside one PostgreSQL transaction  
**Prerequisite:** [V1 Composable Refresh and Durable Delta Contracts](PROPOSAL_V1_COMPOSABLE_REFRESH_AND_DELTA_CONTRACTS.md)  
**Baseline reviewed:** `pg_trickle` 0.90.0 at commit [`a6a53e0`](https://github.com/trickle-labs/pg-trickle/commit/a6a53e0f6697eed4407a664b1af5172ca137fd9c)  
**Last updated:** 2026-09-01

---

> **Terminology note:** “V2” in this document means the second-stage integration needed by `pg_mdm` V2. It does not mean `pg_trickle` 2.0, PostgreSQL row-identity V2, or a replacement for the V1 composition contract.

## Contents

1. [Executive decision](#1-executive-decision)
2. [V2 release boundary](#2-v2-release-boundary)
3. [Relationship to the V1 contract](#3-relationship-to-the-v1-contract)
4. [Motivation](#4-motivation)
5. [Goals and non-goals](#5-goals-and-non-goals)
6. [Relationship to `pg_mdm` V2](#6-relationship-to-pg_mdm-v2)
7. [Existing foundations](#7-existing-foundations)
8. [Terminology](#8-terminology)
9. [Required invariants](#9-required-invariants)
10. [Proposed V2 surface](#10-proposed-v2-surface)
11. [Capability discovery and versioning](#11-capability-discovery-and-versioning)
12. [Prepared-generation identity](#12-prepared-generation-identity)
13. [`prepare_graph()`](#13-prepare_graph)
14. [Prepared-generation state machine](#14-prepared-generation-state-machine)
15. [Durable member leases and graph isolation](#15-durable-member-leases-and-graph-isolation)
16. [Cross-transaction read protocol](#16-cross-transaction-read-protocol)
17. [Prepared output-delta ranges](#17-prepared-output-delta-ranges)
18. [`promote_prepared_graph()`](#18-promote_prepared_graph)
19. [`abandon_prepared_graph()`](#19-abandon_prepared_graph)
20. [Source changes while a generation is prepared](#20-source-changes-while-a-generation-is-prepared)
21. [Validation, invalidation, and repair](#21-validation-invalidation-and-repair)
22. [Concurrency and lock ordering](#22-concurrency-and-lock-ordering)
23. [Security and authorization](#23-security-and-authorization)
24. [Failure, crash, and recovery behavior](#24-failure-crash-and-recovery-behavior)
25. [Stable errors](#25-stable-errors)
26. [Suggested catalog and storage changes](#26-suggested-catalog-and-storage-changes)
27. [Integration with the refresh engine](#27-integration-with-the-refresh-engine)
28. [Lifecycle interactions](#28-lifecycle-interactions)
29. [Observability and administration](#29-observability-and-administration)
30. [Performance and resource behavior](#30-performance-and-resource-behavior)
31. [Upgrade and compatibility policy](#31-upgrade-and-compatibility-policy)
32. [End-to-end protocols](#32-end-to-end-protocols)
33. [Delivery plan](#33-delivery-plan)
34. [Test plan](#34-test-plan)
35. [Alternatives considered](#35-alternatives-considered)
36. [Risks and open implementation choices](#36-risks-and-open-implementation-choices)
37. [Acceptance criteria](#37-acceptance-criteria)
38. [Recommended disposition](#38-recommended-disposition)
39. [Appendix A: possible later generations](#appendix-a-possible-later-generations)

---

## 1. Executive decision

`pg_trickle` should add one separately versioned integration capability called **prepared graph generations**. The capability is for trusted PostgreSQL extensions that already use the V1 composition contract but need to perform expensive domain work after relational evidence has been refreshed. Instead of keeping one PostgreSQL transaction open while that work runs, the caller can ask `pg_trickle` to refresh an eligible private graph to one complete source boundary, commit the resulting stream-table state, and seal it as an immutable prepared generation. The caller may then inspect that exact generation and maintain its own checkpoints over several later transactions. When its complete domain result is ready, it promotes the prepared generation and acknowledges the bound output deltas inside the same short transaction that publishes its own result.

The first implementation should deliberately use a simple physical model. Preparing a graph freezes the graph's existing stream-table storage in place. It does not clone a second writable copy, and it does not permit another refresh over any overlapping member while the generation remains prepared. Source tables may continue to accept writes, and `pg_trickle` continues to capture those later changes in its ordinary CDC state. The next graph refresh processes them only after the prepared generation has been promoted or abandoned. This model supports one active prepared generation per stream-table member, avoids creating a second data-storage architecture, and is sufficient for private graphs whose higher-level resolver needs time rather than simultaneous graph versions.

The capability does not make `pg_trickle` responsible for another extension's business computation. `pg_trickle` owns the immutable evidence generation, its source boundary, member contracts, output-delta positions, and lifecycle lease. The coordinating extension owns its checkpoint format, worker allocation, domain validation, compare-and-swap conditions, public tables, and final business publication. Promotion is intentionally called by the coordinating extension rather than triggered through a callback. PostgreSQL remains the mechanism that commits the final domain publication, delta acknowledgement, and prepared-generation transition together.

---

## 2. V2 release boundary

This proposal is only for the cross-transaction lifecycle that begins after the V1 composition contract exists. It assumes that `pg_trickle` can already discover integration capabilities, compute canonical stream-table and graph contracts, keep a graph in durable `EXTERNAL` orchestration mode, refresh a complete graph strictly inside the caller's transaction, return one coherent source-boundary manifest, expose durable typed output deltas, and acknowledge those deltas transactionally. Those capabilities remain governed by their V1 contract and are not redefined here.

The V2 capability adds the following behavior:

| V2 capability | Purpose |
|---|---|
| Prepare an eligible graph | Refresh the graph once and commit a named immutable result |
| Durable generation identity | Let later transactions prove that they are reading the same evidence |
| Durable member leases | Prevent refresh or semantic mutation of every prepared member |
| Transaction-scoped read opening | Prevent promotion or abandonment while one processing transaction is reading |
| Prepared consumer ranges | Pin the exact output-delta interval or resnapshot requirement associated with the generation |
| Atomic promotion | Acknowledge prepared deltas and release the graph in the caller's final publication transaction |
| Explicit abandonment | Release a generation without claiming that its evidence was accepted |
| Recovery and verification | Preserve or fail closed across restart, failover, restore, upgrade, or detected corruption |

This proposal does not implement several broader `pg_mdm` V2 ideas. It does not add valid-time source processing, custom MDM functions, entity-level stewardship, semantic business events, MDM worker coordination, or domain checkpoint tables to `pg_trickle`. It also does not make the graph refresh itself resumable across committed transactions. `prepare_graph()` still performs one ordinary strict graph refresh in one PostgreSQL transaction; the cross-transaction benefit begins after that refresh has committed. A graph whose relational refresh cannot fit within the existing transaction and resource envelope needs a later, separate design.

---

## 3. Relationship to the V1 contract

The V1 composition protocol is intentionally the default. A coordinating extension begins a transaction, calls strict graph refresh, consumes exact output deltas or performs a full resynchronization, updates its own durable state, acknowledges the deltas, and commits. That is the simplest and strongest execution model because every layer shares one snapshot and one transaction from beginning to end.

Prepared generations are an optional escape hatch for workloads that exceed the practical duration of that transaction. They are not a replacement for V1, and implementing them must not change V1 semantics. An ordinary strict graph refresh still rolls back completely when the caller's domain publication fails. An output-delta consumer still advances only through explicit acknowledgement. A graph in `MANAGED` mode still belongs to the normal scheduler. A graph in `EXTERNAL` mode remains refreshable through the V1 strict API whenever no prepared lease is active.

The V2 capability depends on exact V1 capability majors rather than only on the package version. Capability discovery should report `prepared_graph_generation` as disabled unless all prerequisite contracts are available and compatible. At minimum, capability major 1 depends on compatible majors of `graph_contract`, `external_orchestration`, `transactional_graph_refresh`, `source_boundary_manifest`, and `output_delta_consumer`.

A coordinating extension may support both paths for the same logical product. Small entities can continue through V1's one-transaction path. Large entities can opt into the prepared path after admission checks show that source-buffer retention, output-delta retention, and graph-freeze duration are acceptable. Both paths must produce the same higher-level result when given the same graph contract, source boundary, domain inputs, and policy versions.

---

## 4. Motivation

A higher-level resolver can require much more time than the relational refresh that creates its input. In `pg_mdm`, for example, `pg_trickle` may incrementally maintain normalized source values, bounded candidate pairs, pair evidence, authoritative conflicts, and golden-value candidates. Once that evidence is ready, `pg_mdm` may still need to discover a large affected closure, recompute conservative components, reconcile stable identifiers across merges and splits, validate manual constraints, calculate golden records, create review items, and compare the result with the prior publication. On a large graph, that work may need to be partitioned, checkpointed, retried, or reviewed over many transactions.

Keeping one transaction open for the whole operation retains an old snapshot, holds graph and catalog locks longer than necessary, delays vacuum cleanup, increases the impact of process failure, and makes operational progress difficult to expose. Committing the graph refresh and then simply trusting that the private stream tables will not change is not safe. The scheduler, a manual refresh, a definition alteration, a repair operation, or another coordinator could advance or recreate the graph before the higher-level result is published. A source-boundary digest alone cannot protect the data if the physical stream-table contents are mutable.

Exporting the terminal rows into coordinator-owned staging tables is possible, but it duplicates potentially large evidence relations and transfers responsibility for row identity, output-delta continuity, schema changes, cleanup, and source-boundary proof to every integrating extension. It also weakens the clean boundary established by the V1 proposal. `pg_trickle` already owns the stream-table contents and the metadata that proves how they were produced. The missing primitive is a durable lease that turns one completed graph refresh into a stable, named generation until the coordinator explicitly accepts or rejects it.

---

## 5. Goals and non-goals

### 5.1 Goals

The prepared-generation capability has ten goals. It must let an authorized coordinator prepare a complete `EXTERNAL` graph under the same source-boundary and strict-refresh rules as V1; assign an immutable generation identity and digest; preserve the prepared state across backend disconnect, restart, physical failover, backup, and supported upgrades; block every graph mutation that could change the evidence; permit short read transactions to reopen and verify the same generation; pin the relevant output-delta intervals; allow source writes after the prepared boundary to remain pending; atomically promote the generation with the coordinator's publication and delta acknowledgements; explicitly abandon without acknowledgement; and fail closed whenever the generation can no longer be proved.

The design should remain useful beyond MDM. Any PostgreSQL extension that compiles a domain model into a private `pg_trickle` graph and then performs expensive deterministic work can use the same lifecycle. The API therefore speaks about graphs, generations, contracts, source boundaries, output consumers, leases, and transitions. It does not mention entities, matches, goldens, features, policies, or any other consuming domain.

### 5.2 Non-goals

Capability major 1 does not clone several physical generations, permit simultaneous preparations over overlapping graph members, refresh a leased graph in the background, or let source changes be incorporated into the prepared state after preparation. It does not checkpoint or resume `pg_trickle`'s own graph refresh. It does not create a worker protocol for the coordinating extension, store the coordinator's partial business results, or decide whether a changed domain definition or stewardship decision supersedes a run.

The proposal does not use PostgreSQL prepared transactions or two-phase commit to keep the original graph-refresh transaction open. It does not export a PostgreSQL snapshot for later sessions, and it does not rely on session-level advisory locks as durable ownership. It does not add arbitrary callbacks from the `pg_trickle` finalizer into another extension. It does not provide cross-database or cross-cluster generations, and it does not promise that an indefinitely prepared graph can retain unlimited CDC or output-delta history without operational consequences.

The first capability also does not hide prepared stream-table contents from roles that already have ordinary `SELECT` privileges. The motivating design uses private schemas and restricted grants, but `pg_trickle` remains a PostgreSQL extension rather than a separate confidentiality boundary. It guarantees immutability and authorized lifecycle transitions, not invisibility from database owners or superusers.

---

## 6. Relationship to `pg_mdm` V2

The motivating `pg_mdm` V2 flow separates evidence preparation from identity publication. `pg_trickle` refreshes the private evidence graph and seals one prepared generation. `pg_mdm` stores the generation identifier and digest in its own run manifest, processes deterministic component ranges in logged checkpoint tables, and may use several workers or several transactions to finish the domain result. Every processing transaction opens the same prepared generation before reading its terminal relations. The number of workers, checkpoint boundaries, and retry order remain `pg_mdm` concerns and are not semantic inputs to `pg_trickle`.

When `pg_mdm` is ready to publish, it begins a short transaction and verifies its own compare-and-swap conditions, such as the expected active MDM publication revision, desired definition version, and stewardship decision epoch. It writes the new memberships, golden rows, reviews, provenance, identity history, and semantic events, then calls `promote_prepared_graph()` with the exact output-delta acknowledgements recorded by the generation. If any MDM validation or DML fails, PostgreSQL rolls back the promotion and the generation remains prepared. If a newer MDM definition or decision makes the run obsolete before publication, `pg_mdm` calls `abandon_prepared_graph()` and does not acknowledge the evidence.

The critical responsibility boundary is therefore:

> **`pg_trickle` proves and freezes relational evidence. `pg_mdm` decides whether that evidence may become an identity publication.**

`pg_trickle` does not inspect the MDM decision epoch, stable-ID ledger, checkpoint tables, or public MDM outputs. `pg_mdm` does not inspect private `pg_trickle` leases, change buffers, frontier encodings, or storage-generation catalogs. The generation identifier, generation digest, supported status APIs, and final promotion call are the complete supported bridge.

---

## 7. Existing foundations

The proposal is intentionally built on existing `pg_trickle` architecture and the companion V1 integration proposal. `pg_trickle` already maintains stream tables through full and differential refresh paths, tracks per-source frontiers, captures local changes through trigger or WAL mechanisms, maintains stream-table-to-stream-table deltas, serializes refreshes, records durable refresh history, uses exact versioned row identity, and performs owner-equivalent execution. The V1 proposal adds the graph-wide contract, strict synchronous refresh, external orchestration, source-boundary manifest, and durable output-delta cursor required before a prepared lifecycle can be safe.

The most important implementation discipline is reuse. `prepare_graph()` must not become a fourth independent refresh engine beside manual, scheduled, and V1 strict graph refresh. It should invoke the same graph resolver, contract validator, source-boundary planner, member executor, output-delta finalizer, and transaction semantics as `refresh_graph_strict()`. Its additional work is performed after the graph result has been successfully finalized but before the transaction commits: construct the generation manifest, bind terminal consumer ranges, insert durable member leases, and return the prepared identity.

The initial freeze-in-place design also relies on existing stream-table write protection. Every supported mutation of a stream-table storage relation must flow through `pg_trickle` and advance a durable storage-generation marker. Superuser tampering remains outside the ordinary trust model, but supported lifecycle paths, repair paths, DDL hooks, and refresh paths must all participate in generation validation. A prepared generation cannot be considered immutable if one supported code path can recreate or rewrite a member without checking its lease.

---

## 8. Terminology

A **prepared graph generation** is one completed graph refresh whose stream-table contents, graph contract, member contracts, source-boundary manifest, storage-generation markers, and bound output-delta positions have been sealed against further graph mutation. The generation is durable and identified by a UUID and an immutable digest.

A **generation member** is a stream table in the canonical dependency closure of the prepared roots. Capability major 1 requires every member to use durable `EXTERNAL` orchestration and permits at most one active prepared lease for a member.

A **member lease** is a logged catalog record that prohibits refresh, defining-query change, ownership transfer, orchestration-mode change, storage recreation, drop, and other result-affecting lifecycle operations while a generation is `PREPARED`. The lease is durable state rather than a session lock. Ordinary transaction locks are still used to serialize acquisition, reading, promotion, and abandonment.

A **prepared read** is a transaction that calls `open_prepared_graph()` and then reads one or more generation members or bound output-delta relations. Opening validates the generation and holds a transaction-scoped shared transition lock so that promotion or abandonment cannot release the member leases until the reader's transaction ends.

A **prepared consumer range** is the interval from one output-delta consumer's acknowledged batch token, exclusive, through the terminal token produced or observed by the prepared graph, inclusive. The range records whether exact rows are continuously available or whether the coordinator must resynchronize from the complete prepared terminal relation.

**Promotion** is the transactionally atomic acceptance transition from `PREPARED` to `PROMOTED`. It validates the immutable generation, applies every required output-delta acknowledgement, records completion, and releases member leases in the caller's transaction. Capability major 1 does not perform a physical table swap because the prepared result already occupies the private graph storage.

**Abandonment** is the explicit rejection transition from `PREPARED` or `INVALID` to `ABANDONED`. It releases member leases without acknowledging any prepared consumer range and without attempting to reverse the graph refresh.

**Future continuity** describes whether `pg_trickle` can safely refresh the graph after its lease is released. Capability major 1 reports `CONTINUOUS`, `REINITIALIZE_REQUIRED`, or `UNKNOWN`. A prepared generation may remain valid evidence even if later source CDC state becomes damaged; in that case promotion can still be valid while the graph's next refresh requires a complete reinitialization. Prepared validity and future continuity are therefore reported separately.

---

## 9. Required invariants

The capability is governed by the following invariants. They define the contract more strongly than any proposed catalog layout.

1. **One immutable generation.** Every successful `prepare_graph()` identifies exactly one graph contract, member set, source-boundary manifest, member content generation, and prepared consumer range. Those inputs cannot change while the generation remains prepared.

2. **No invisible graph mutation.** A refresh, query alteration, storage recreation, ownership transfer, orchestration change, drop, restore, or repair that could alter a prepared member is rejected before mutation.

3. **One active lease per member.** Two active prepared generations cannot overlap on any stream-table member in capability major 1.

4. **Source writes remain independent.** Committed source changes after the prepared boundary may accumulate for a later graph refresh, but they do not modify or invalidate the prepared member contents merely because they exist.

5. **Prepared reads are repeatable by contract.** Every transaction that successfully opens a prepared generation reads the same member contents and bound output ranges as every other successful prepared read of that generation.

6. **Promotion shares the caller's commit.** Generation promotion, bound output acknowledgements, and the coordinating extension's publication commit or roll back together.

7. **Abandonment is not acceptance.** Abandoning a generation never advances an output consumer cursor and never claims that a higher-level publication consumed its evidence.

8. **No rollback reconstruction.** Abandonment does not reverse the private stream tables. The durable consumer cursor and later cumulative delta sequence record what the coordinator has or has not accepted.

9. **Exact history or explicit resynchronization.** A prepared consumer range is either a continuous exact sequence or an explicit requirement to read the complete prepared terminal relation. Missing history is never represented as an empty exact range.

10. **Transition serialization.** A prepared read blocks promotion or abandonment for the duration of its transaction, and promotion or abandonment blocks new prepared reads while the transition is validated and committed.

11. **Owner-equivalent authority.** Preparation, opening, promotion, abandonment, and inspection grant no authority beyond the permissions explicitly checked for every member and output consumer.

12. **Durable recovery.** Restart, failover, backup, restore, and supported extension upgrade preserve prepared state and leases or report a stable invalid condition. They never silently release, promote, or acknowledge a generation.

13. **Semantic contract pinning.** A semantic mutation to the graph, a member, a result-affecting function, row-identity behavior, collation dependency, or output contract prevents promotion under the old generation digest.

14. **Physical tuning is not semantic.** A supported statistics update, index maintenance operation, worker-count change, or other proven physical-only change does not alter the generation digest or domain meaning.

15. **Future damage is distinguished.** Loss of pending CDC needed only for a later refresh does not rewrite the already prepared evidence. It is reported as degraded future continuity and requires reinitialization after release.

16. **No private dependency.** Coordinating extensions use only documented capability, contract, generation, output-delta, and status APIs. They do not infer private lease tables, relation names, internal tokens, or frontier encodings.

---

## 10. Proposed V2 surface

Capability major 1 adds the following public integration operations. The exact PostgreSQL composite syntax may be refined during implementation, but the behaviors and transaction boundaries are normative.

| Operation | Purpose |
|---|---|
| `integration_capabilities()` | Advertise `prepared_graph_generation` and its prerequisite majors |
| `prepare_graph()` | Strictly refresh an eligible graph and commit it as `PREPARED` |
| `prepared_graph_status()` | Inspect state, validity, boundaries, retention, and blockers without opening it for data reads |
| `prepared_graph_members()` | Return the public identities and pinned contracts of generation members |
| `prepared_graph_consumers()` | Return bound consumer intervals and required acknowledgement dispositions |
| `open_prepared_graph()` | Validate and transactionally open the immutable generation for reading |
| `verify_prepared_graph()` | Perform fast or deep integrity verification and report validity separately from future continuity |
| `promote_prepared_graph()` | Atomically acknowledge bound deltas, record acceptance, and release leases |
| `abandon_prepared_graph()` | Release leases without acknowledgement |

The primary lifecycle can be summarized as:

```text
strict graph refresh
        |
        v
PREPARED  ---- promote in caller publication transaction ---->  PROMOTED
    |
    +------ abandon without acknowledgement ------------------>  ABANDONED
    |
    +------ integrity loss ------------------------------------>  INVALID
                                                                  |
                                                                  +--> ABANDONED
```

A failed preparation transaction creates no durable generation. `PREPARING` may appear in an operation record or future asynchronous implementation, but it is not a committed generation state in capability major 1.

---

## 11. Capability discovery and versioning

The V1 capability-discovery function should add one row when the feature is installed and usable:

```text
capability                    major  minor  enabled  details
prepared_graph_generation       1      0      true   {...}
```

The details object is advisory and may include:

```json
{
  "storage_mode": "LEASE_CURRENT",
  "max_active_generations_per_member": 1,
  "asynchronous_prepare": false,
  "multi_generation_storage": false,
  "prepared_graph_refresh_resumable": false,
  "required_capabilities": {
    "graph_contract": 1,
    "external_orchestration": 1,
    "transactional_graph_refresh": 1,
    "source_boundary_manifest": 1,
    "output_delta_consumer": 1
  }
}
```

A capability major changes when the prepared-generation state model, member immutability rules, read-opening semantics, generation digest, promotion atomicity, abandonment behavior, or prepared consumer-range contract changes incompatibly. A minor version may add optional diagnostics, support more source kinds, permit more physical maintenance operations, or add non-required result fields without weakening the major contract.

The capability is disabled when a prerequisite capability is absent, an upgrade has not completed the durable catalog migration, recovery has not validated the generation registry, or the server configuration cannot preserve the documented semantics. A new package version must not be treated as sufficient evidence that preparation is safe; runtime capability discovery remains authoritative.

---

## 12. Prepared-generation identity

Every generation has both a durable identifier and an immutable digest. The identifier is an opaque UUID suitable for catalog lookup. The digest is computed from a canonical, versioned encoding of the generation's immutable contract and protects callers against stale references, cross-object mistakes, and accidental catalog drift.

The generation digest includes at least:

```text
generation-contract major and minor
prepared_generation_id
preparation request_id
database lineage identifier
owner role identity
canonical roots and member order
graph contract digest
source-boundary digest
for every member:
  stable stream-table identity
  stream-table contract digest
  storage relation identity
  storage generation
  producing refresh identifier
  output schema digest
  row-identity and probe versions
for every bound consumer:
  consumer identity
  output contract digest
  row-identity version
  acknowledged start token
  prepared terminal token
  required disposition class
```

Mutable operational fields such as state, age, warning counters, active readers, retained-byte estimates, and completion timestamps do not affect the digest. The canonical representation must be specified and tested with independent golden vectors. A refactor of JSON rendering, catalog row order, or internal relation naming must not change the digest.

The caller stores both the UUID and digest in its own run manifest. Every later open, promotion, or abandonment supplies the expected digest. An identifier that exists with a different digest is a hard error rather than an invitation to use the current row.

Preparation also accepts a caller-provided `request_id`. The pair `(owner_role, request_id)` is unique. Repeating a request after an uncertain network outcome returns the existing generation only when the roots, graph digest, consumers, and options are identical. Reusing the request identifier with different inputs raises a stable idempotency error. This prevents a lost response after commit from leaving the coordinator unable to rediscover which prepared generation owns the graph.

---

## 13. `prepare_graph()`

### 13.1 Proposed API

An illustrative call is:

```sql
SELECT *
FROM pgtrickle.prepare_graph(
    request_id                   => :mdm_run_id,
    roots                        => ARRAY[
        'mdm_internal.customer_pair_evidence_v7'::regclass,
        'mdm_internal.customer_golden_candidates_v7'::regclass
    ],
    expected_graph_digest        => decode(:graph_digest_hex, 'hex'),
    output_consumers             => ARRAY[
        :pair_evidence_consumer_id,
        :golden_candidate_consumer_id
    ]::uuid[],
    require_complete_boundaries  => true,
    full_policy                  => 'ALLOW'
);
```

The initial signature may be represented as:

```sql
pgtrickle.prepare_graph(
    request_id                   uuid,
    roots                        regclass[],
    expected_graph_digest        bytea,
    output_consumers             uuid[] DEFAULT ARRAY[]::uuid[],
    require_complete_boundaries  boolean DEFAULT true,
    full_policy                  text DEFAULT 'ALLOW'
)
RETURNS pgtrickle.prepared_graph_result
```

The result contains at least:

```text
prepared_generation_id   uuid
generation_digest        bytea
graph_refresh_id         bigint
graph_digest             bytea
source_boundary          jsonb
source_boundary_digest   bytea
node_results             jsonb
state                    text        -- PREPARED
prepared_at              timestamptz
future_continuity         text
```

Detailed member and consumer rows are returned by separate set-returning status functions so that the primary result does not grow into an unstable nested document.

### 13.2 Admission checks

Before any graph member is mutated, `prepare_graph()` resolves the complete canonical closure and verifies that every member is in `EXTERNAL` orchestration mode, every expected graph contract matches, the graph is acyclic under the supported V1 rules, no member is suspended or already leased, no conflicting refresh or lifecycle operation is active, and every requested output consumer belongs to an eligible terminal or explicitly named member with a compatible output contract.

The initial capability also rejects a graph that has a scheduler-managed downstream stream table outside the prepared closure. Such a dependent could consume and publish the newly prepared private state before the coordinating extension accepts it. Direct SQL readers remain governed by PostgreSQL grants, but automatic downstream propagation must stay within the same externally controlled boundary. A later capability may define explicit multi-coordinator dependencies; capability major 1 fails closed instead.

If complete boundaries are required, every accepted source kind must provide the same proof required by V1 strict graph refresh. A partial remote snapshot, an unverifiable polling source, missing CDC state, or any source-boundary uncertainty aborts preparation. `full_policy` has the same meaning as in the V1 strict API: a caller may allow a correct full refresh or require differential execution, but no policy may publish incomplete relational state.

### 13.3 Execution and commit

After admission, `prepare_graph()` invokes the ordinary strict graph-refresh engine using one immutable boundary context and the standard deterministic lock order. Every member refresh, output-delta batch, frontier update, and refresh-history record is finalized through the normal common path. If any node fails, the entire preparation transaction rolls back and no generation or lease exists.

After the graph result is complete, but before returning, the function captures each member's contract and storage-generation markers, binds the requested output-consumer ranges, constructs the canonical generation digest, inserts the durable generation and member rows, and acquires one persistent lease record per member. The stream-table rows and lease records commit together. There is no interval in which the refreshed contents are committed but the graph is not protected.

The call itself does not commit. The caller controls its transaction as usual. The recommended pattern is a small standalone preparation transaction that stores the returned generation identity in the coordinating extension's run record and commits both records together. If that transaction rolls back, the graph refresh, output batches, generation, leases, and coordinator run record all disappear.

### 13.4 Idempotent recovery after an uncertain result

If the client loses its connection after the server may have committed, it queries by its durable request identifier rather than issuing a different preparation. A helper may be provided:

```sql
SELECT *
FROM pgtrickle.prepared_graph_by_request(
    owner_role => current_user::regrole,
    request_id => :mdm_run_id
);
```

When the request did not commit, no row exists and the caller may retry with the same inputs. When it committed, the existing generation and digest are returned. When the identifier exists for different inputs, the function reports a request conflict. This behavior is important because a committed but undiscovered prepared generation intentionally blocks later graph refreshes.

---

## 14. Prepared-generation state machine

The durable state machine is deliberately small:

```text
                 +------------+
                 |  PREPARED  |
                 +-----+------+
                       |
             +---------+----------+
             |                    |
             v                    v
       +-----------+        +-----------+
       | PROMOTED  |        | ABANDONED |
       +-----------+        +-----------+

PREPARED may become INVALID when its immutable proof is lost.
INVALID may only become ABANDONED.
```

A successful synchronous preparation commits directly to `PREPARED`. A failed call leaves no generation record. `PROMOTED` and `ABANDONED` are terminal audit states. `INVALID` means that `pg_trickle` can no longer prove the generation's immutable evidence, contract, or bound delta range. An invalid generation cannot be repaired in place or promoted; an authorized caller must abandon it, repair or recreate the graph, and prepare a new generation.

There is no automatic expiry transition. A maximum age may be an advisory policy that produces warnings, but time alone does not prove that a generation was accepted or rejected. An operator, scheduler, or recovery worker must never silently convert an old `PREPARED` generation into `ABANDONED` or `PROMOTED`.

Every state change records the actor, transaction identifier where practical, timestamp, reason or external request reference, and prior state. The generation digest remains the digest of the prepared immutable inputs and does not change when the state changes.

---

## 15. Durable member leases and graph isolation

A prepared member lease is a logged catalog fact. It must not be implemented solely with session advisory locks because sessions end, backends crash, and failover starts new processes. The recommended representation is one active lease row keyed by stable stream-table identity and referencing the prepared generation. A uniqueness constraint on the member identity prevents overlapping preparations even when two callers race.

Preparation acquires ordinary transaction locks on every member in canonical order, verifies that no active lease exists, writes all leases, and commits them with the graph refresh. Every supported code path that can mutate a member must acquire the same member catalog lock and call one central lease guard after locking. That includes scheduled and manual refresh, V1 strict graph refresh, another preparation, `ALTER QUERY`, storage migration, restore, recreation, owner transfer, orchestration-mode change, suspend/resume operations that alter refresh eligibility, and drop. A check performed before acquiring the mutation lock is insufficient because preparation could commit between the check and mutation.

While the lease is active, ordinary reads, `VACUUM`, `ANALYZE`, and other explicitly classified operations that do not change logical contents or semantic contracts may proceed. Physical maintenance that replaces storage, changes relation identity, or cannot prove preservation is blocked in capability major 1. A later minor version may allow more maintenance after tests demonstrate that the pinned storage-generation and contract checks remain sufficient.

The graph remains in `EXTERNAL` orchestration mode after promotion or abandonment. Releasing a prepared lease makes it eligible for a later explicit V1 strict refresh or another preparation; it never transfers the graph to the normal scheduler.

---

## 16. Cross-transaction read protocol

A durable member lease prevents refresh while the generation is prepared, but a processing transaction also needs protection against a concurrent promotion or abandonment that would release those leases before the transaction finishes reading. The proposal therefore adds an explicit open operation:

```sql
BEGIN;

SELECT *
FROM pgtrickle.open_prepared_graph(
    prepared_generation_id => :generation_id,
    expected_generation_digest => decode(:generation_digest_hex, 'hex')
);

-- Read member stream tables and bound output-delta relations here.
-- Store or validate coordinator-owned checkpoint work.

COMMIT;
```

`open_prepared_graph()` validates state, digest, ownership, database lineage, member leases, member contracts, member storage generations, and bound output state. It then holds a transaction-scoped shared transition lock on the generation. Several read transactions may open the same generation concurrently. Promotion and abandonment require an exclusive transition lock and therefore wait or return a stable busy error until all prepared readers finish.

The open result includes the generation identifier and digest, graph digest, source-boundary digest, state, member count, and public member relation identities. It does not grant `SELECT`; ordinary PostgreSQL privileges still apply. It also does not create a new snapshot token. The persistent member leases ensure that the stream tables do not change, and the transaction-scoped transition lock ensures that the leases cannot be released during the read transaction. This makes the contents stable even under ordinary `READ COMMITTED` statement snapshots.

Status inspection without data reads does not need to open the generation and does not block promotion. A coordinating extension that reads generation data without calling `open_prepared_graph()` is outside the supported cross-transaction protocol, even though the tables may happen to remain unchanged.

---

## 17. Prepared output-delta ranges

At preparation, each explicitly bound V1 output consumer receives an immutable range associated with the generation. The range starts immediately after the consumer's acknowledged token and ends at the terminal token for the prepared stream-table state. The generation records the output contract digest, row-identity version, source-boundary digest, and whether the range can be applied incrementally.

A status function exposes the bindings:

```sql
SELECT *
FROM pgtrickle.prepared_graph_consumers(:generation_id);
```

It returns at least:

```text
consumer_id
stream_table
from_token_exclusive
through_token_inclusive
range_mode                 -- EXACT | RESYNCHRONIZE
required_disposition       -- APPLIED | RESYNCHRONIZED
output_contract_digest
row_identity_version
retained_rows
retained_bytes
```

`EXACT` means the V1 batch sequence is continuous and every batch is exact. `RESYNCHRONIZE` means the consumer already required a baseline, the range contains `FULL_INVALIDATION`, or another truthful V1 condition requires the complete terminal relation. The prepared generation itself provides the stable relation needed for a cross-transaction resynchronization, so a coordinator can page through a very large terminal table over several short prepared reads and checkpoint its progress.

Every batch and payload row needed by a prepared binding is pinned against cleanup. A direct call to the V1 acknowledgement API that would advance a bound consumer through or beyond the prepared terminal token is rejected while the generation is `PREPARED`. Only promotion may apply the bound acknowledgement. Consumer pause, drop, rebind, output-contract mutation, or resnapshot outside the prepared protocol is also blocked for bound consumers.

The range is determined at preparation and does not grow when source changes arrive later. Later graph refreshes cannot occur while the member lease is active, so no new output batches for those members are produced until promotion or abandonment releases the graph.

---

## 18. `promote_prepared_graph()`

### 18.1 Proposed API

Promotion is intended to be called inside the coordinating extension's final publication transaction:

```sql
SELECT *
FROM pgtrickle.promote_prepared_graph(
    prepared_generation_id      => :generation_id,
    expected_generation_digest  => decode(:generation_digest_hex, 'hex'),
    acknowledgements            => jsonb_build_array(
        jsonb_build_object(
            'consumer_id',  :pair_consumer_id,
            'through_token', :pair_through_token,
            'disposition',   'APPLIED'
        ),
        jsonb_build_object(
            'consumer_id',  :golden_consumer_id,
            'through_token', :golden_through_token,
            'disposition',   'RESYNCHRONIZED'
        )
    )
);
```

The final implementation should preferably use a typed array such as `pgtrickle.prepared_consumer_ack[]`; JSONB above is illustrative. Promotion returns the generation identifier, prior and new state, completion timestamp, released member count, and acknowledged consumer count.

### 18.2 Validation

Promotion takes the generation's exclusive transition lock and then reacquires member locks in the same canonical order used by preparation. It verifies the expected digest, state, owner, database lineage, graph digest, member set, member contracts, member storage generations, producing refresh identifiers, source-boundary digest, active leases, output-consumer contracts, retained ranges, and required acknowledgement disposition. Every bound consumer must be acknowledged exactly through its prepared terminal token. Extra acknowledgements and omitted required acknowledgements are rejected.

Source rows may have changed after preparation and do not prevent promotion. Those changes are intentionally pending. A semantic source or graph change that alters a pinned contract, a recreated member relation, a changed row-identity version, missing delta history, or a lost lease does prevent promotion. A separate future-continuity warning caused only by damage to post-boundary pending CDC may be returned, but it does not rewrite the already prepared evidence.

### 18.3 Transactional effect

Promotion applies the bound V1 output-delta acknowledgements, moves any resynchronized consumer back to `ACTIVE`, marks the generation `PROMOTED`, records completion, and releases all member leases in the caller's transaction. It does not call arbitrary external code and it does not publish the coordinating extension's tables itself.

The caller may perform its own compare-and-swap checks and domain DML before or after the promotion call in the same transaction. If any later statement fails or the transaction is rolled back, the generation remains `PREPARED`, every lease remains active, and every consumer cursor remains unchanged. This property is the reason promotion must be an ordinary transactional SQL function rather than an asynchronous scheduler action.

### 18.4 Meaning of promotion in the first physical model

Because capability major 1 freezes the current private graph storage, the prepared rows are already present in the stream tables before promotion. Promotion does not swap a hidden table into place. It records that the authorized coordinating extension accepted this exact generation, consumes the bound delta ranges, and releases the graph for its next refresh. The API uses generational terminology so a later storage implementation can preserve the same logical lifecycle, but callers must not infer a physical table swap from the word “promote.”

---

## 19. `abandon_prepared_graph()`

An authorized caller may reject a prepared or invalid generation explicitly:

```sql
SELECT *
FROM pgtrickle.abandon_prepared_graph(
    prepared_generation_id      => :generation_id,
    expected_generation_digest  => decode(:generation_digest_hex, 'hex'),
    reason                       => 'superseded by a newer MDM decision epoch'
);
```

Abandonment takes the exclusive transition lock, validates the generation identity and authority, marks the generation `ABANDONED`, records the required reason, and releases every member lease. It does not acknowledge any output consumer, does not delete the prepared output batches, and does not attempt to restore the stream tables to their previous contents.

Leaving the private graph at the abandoned boundary is safe because the durable output-consumer cursor still records what the higher-level publication accepted. Suppose a consumer was acknowledged through batch 40, generation A prepared batches 41 and 42, and A was abandoned. A later preparation may advance the graph again and produce batches 43 and 44. The consumer still begins after 40 and therefore sees the cumulative range 41 through 44, or an explicit resynchronization if any batch invalidated exact application. Reversing the graph would require a second retained physical generation or a guaranteed inverse delta and would add complexity without improving the acceptance contract.

An invalid generation can only be abandoned. It cannot be promoted after a repair because repair would create different evidence from the immutable generation that the coordinator processed. If invalidation involved lost or unverifiable output history, the affected V1 consumer remains or becomes `RESNAPSHOT_REQUIRED` after abandonment. The audit row remains available according to normal retention policy even after its active leases are removed.

---

## 20. Source changes while a generation is prepared

Preparing a graph freezes derived stream-table members, not their source systems. Local source transactions continue to insert, update, delete, and truncate rows under the ordinary `pg_trickle` CDC contract. WAL decoders, trigger buffers, upstream stream tables outside the prepared graph, and supported snapshot providers may continue advancing. Those later changes remain beyond the prepared source boundary and are processed by a future graph refresh after the lease is released.

This behavior is essential for operational usefulness. A large resolver should not need to stop OLTP writes while it analyzes prepared evidence. The generation manifest records the exact boundary it represents, and the coordinating extension publishes that boundary explicitly. Freshness may lag during a long preparation, but correctness is not weakened.

The cost is retention. Because the prepared graph cannot advance its member frontiers, source change buffers and upstream stream-table output buffers may retain all changes after the prepared boundary. `pg_trickle` must expose the resulting row, byte, age, and WAL-risk estimates. It may refuse a new preparation when current capacity cannot support the requested graph or configured advisory horizon, but it must not discard pending source changes or silently abandon an active generation to relieve pressure.

A semantic source change is different from ordinary row changes. A mapped column type change, source relation replacement, result-affecting view or function change, collation change, source identity change, or other mutation that changes the pinned graph contract invalidates the generation or is blocked by existing DDL guards. An unrelated source-table change that provably does not affect the graph contract need not invalidate it.

---

## 21. Validation, invalidation, and repair

The status model separates **prepared validity** from **future refresh continuity**. Prepared validity answers whether the member contents, contracts, source-boundary proof, and output ranges still match the immutable generation. Future continuity answers whether the graph can perform another safe incremental refresh after the lease is released.

A prepared generation becomes invalid when a member relation is missing or recreated, a member storage-generation marker changes, an active lease is missing or points elsewhere, a graph or stream-table contract no longer matches, a bound output log or batch range is unavailable, a row-identity or output contract changes, database lineage is wrong, or any other required proof cannot be reconstructed. It is never automatically refreshed to a newer boundary and still called the same generation.

Pending CDC damage after the prepared boundary may instead set future continuity to `REINITIALIZE_REQUIRED` while leaving prepared validity as `VALID`. The coordinator may still publish the proved prepared state. After promotion or abandonment, the next graph refresh must follow the V1 full-reinitialization and explicit invalidation rules rather than pretending incremental continuity survived.

The proposal adds:

```sql
SELECT *
FROM pgtrickle.verify_prepared_graph(
    prepared_generation_id => :generation_id,
    verification_level     => 'FAST'  -- FAST | DEEP
);
```

A fast verification checks catalogs, contracts, leases, storage generations, relation identities, output ranges, sentinels, and database lineage. A deep verification may perform expensive row-identity, schema, or content checks where supported. The function returns a structured validity result rather than raising for every discovered defect, allowing a recovery worker or administrator to commit an `INVALID` state. Promotion still fails on any invalid result.

`repair_stream_table()` and storage recreation are blocked while a valid prepared lease exists because changing a member would invalidate the coordinator's work. When a generation is already invalid, the operator first abandons it, then repairs or recreates the graph, and finally prepares a new generation. Repair does not mutate an old generation into valid state.

---

## 22. Concurrency and lock ordering

Preparation, opening, promotion, abandonment, graph refresh, and lifecycle operations must share one documented lock hierarchy. A recommended order is:

```text
database-local integration registry
  -> canonical graph transition key
  -> generation row, when one exists
  -> member stream-table catalog rows in ascending stable identity
  -> member storage relations in ascending OID where required
  -> output consumers in ascending UUID byte order
  -> output batch and payload relations
```

`prepare_graph()` locks the complete member set before refreshing any member and inserts the durable leases while those locks remain held. Concurrent preparations with overlapping members serialize at the first common member and one fails with a stable lease conflict. A V1 strict graph refresh, manual refresh, scheduler dispatch, or lifecycle mutation that encounters an active lease fails rather than waiting indefinitely and later mutating evidence whose coordinator assumptions may have changed.

`open_prepared_graph()` takes a shared transition lock that lasts until transaction end. Promotion and abandonment take the exclusive form. The implementation may use row locks, transaction advisory locks derived from stable generation identity, or both, but the durable member lease remains the source of truth after transaction end. Session locks are not state.

Deadlock tests must cover graphs with overlapping roots, consumer UUID order differing from member order, promotion racing with readers, abandonment racing with source DDL, and restore or repair paths attempting to lock members in another sequence. No public operation may discover members incrementally and mutate the prefix before learning that a later member is unavailable.

---

## 23. Security and authorization

Preparation and transition APIs preserve the V1 owner-equivalent execution model. The caller must own every member and every bound output consumer, be a member of their owner roles under the documented policy, or hold a future explicit maintenance privilege that is checked per object. `USAGE` on the `pgtrickle` schema is not sufficient. A `SECURITY DEFINER` function in another extension cannot borrow `pg_trickle`'s installation authority to prepare or release unrelated graphs.

Defining SQL still executes as each stored stream-table owner under the stored defining search path and row-security contract. The generation owner is the authorized coordinating role, not necessarily the role used to evaluate every defining query. Ownership and role identities are pinned in the generation contract and cannot be changed while prepared.

Prepared output ranges may contain complete values from deleted or superseded rows. Opening a generation does not bypass the V1 consumer authorization rules, and the generated typed delta relation remains accessible only to roles trusted to see the complete output history. Application RLS is not applied as an incomplete filter over a logical delta. A coordinator that cannot see complete terminal evidence cannot own a prepared consumer binding.

Status functions reveal only information allowed by the existing stream-table contract-inspection policy. Public capability discovery remains safe. Full graph definitions, source relation names, consumer lag, and source-boundary details may be sensitive and require ownership or an explicit inspection privilege. Logs and errors use identifiers, counts, state, and digests rather than raw row values.

An administrator or superuser can always destroy extension state at the PostgreSQL level. The contract prevents accidental or ordinary authorized lifecycle mutation and detects supported-path inconsistencies; it does not claim protection from a malicious database superuser.

---

## 24. Failure, crash, and recovery behavior

A failure before the preparation transaction commits is simple: PostgreSQL rolls back the graph refresh, frontiers, output batches, generation rows, and member leases. The prior graph state remains usable. A network failure after an uncertain commit is handled through the caller-provided request identifier and `prepared_graph_by_request()` lookup.

A backend disconnect after a generation is prepared does nothing to its state. The logged generation and lease rows remain. On server restart or physical failover, the scheduler and every mutation path load the durable leases before dispatching work. A `PREPARED` generation is not marked abandoned merely because no coordinator session is connected.

A crash during a prepared read rolls back only that read transaction and releases its shared transition lock. The durable generation remains prepared. A crash during promotion or abandonment follows ordinary PostgreSQL atomicity. If the transition transaction did not commit, state, leases, and consumer cursors remain exactly as before. If it committed, all effects are visible together.

Startup recovery performs a fast verification of every active generation and lease. It checks catalog referential integrity, member identities, storage generations, output-consumer bindings, output-log continuity, and capability versions. A provable mismatch is recorded as `INVALID` in a separate recovery transaction and surfaced through health diagnostics. Recovery never guesses a replacement generation or advances a consumer cursor.

Physical backup, PITR, and streaming replication include generation catalogs, member storage, leases, output logs, and consumer cursors as ordinary durable PostgreSQL state. A restored writable clone must have a documented database-lineage policy. If the V1 integration contract rekeys lineage for a clone, active prepared generations from the original lineage become invalid until explicitly abandoned; a clone must not accidentally promote a generation intended for another database history.

Logical dump and restore support must either preserve every required durable object and dependency in a verified order or reject active prepared generations before dump with a clear instruction. The implementation must not restore the member tables while omitting their leases or bound delta history and then report the graph as freely refreshable.

---

## 25. Stable errors

The V2 API should use the structured error framework established by the V1 integration proposal. Callers must not parse English messages. Capability major 1 adds stable identifiers such as:

| Error identifier | Meaning |
|---|---|
| `PGT_PREP_CAPABILITY_UNAVAILABLE` | The prepared-generation capability or one prerequisite major is unavailable |
| `PGT_PREP_REQUEST_CONFLICT` | A preparation request identifier already exists with different inputs |
| `PGT_PREP_GRAPH_INELIGIBLE` | The graph is not a complete supported external preparation boundary |
| `PGT_PREP_LEASE_CONFLICT` | One or more members already belong to another prepared generation |
| `PGT_PREP_GENERATION_NOT_FOUND` | The generation identifier is unknown in the current database lineage |
| `PGT_PREP_DIGEST_MISMATCH` | The supplied generation digest does not match the stored immutable generation |
| `PGT_PREP_STATE_CONFLICT` | The requested action is invalid for the generation's current state |
| `PGT_PREP_READ_BUSY` | A transition cannot proceed because prepared readers are active, or a reader cannot open during transition |
| `PGT_PREP_MEMBER_CHANGED` | A member contract, relation, storage generation, or producing refresh changed |
| `PGT_PREP_DELTA_UNAVAILABLE` | A bound output range or its proof is missing or discontinuous |
| `PGT_PREP_ACK_MISMATCH` | Promotion acknowledgements are missing, extra, out of range, or use the wrong disposition |
| `PGT_PREP_AUTHORIZATION` | The caller lacks authority over the generation, one member, or one bound consumer |
| `PGT_PREP_INVALID` | Verification has determined that the generation cannot be promoted |
| `PGT_PREP_LIFECYCLE_BLOCKED` | Refresh, DDL, repair, drop, or another lifecycle operation is blocked by a prepared lease |
| `PGT_PREP_LINEAGE_MISMATCH` | The token or generation belongs to another database lineage |
| `PGT_PREP_RESOURCE_RISK` | Admission refused because the requested preparation exceeds a configured hard safety bound |

Each error names the affected generation and object when disclosure is authorized, states whether the generation remains prepared, identifies the concrete consequence, and provides one next action. A lease conflict is never downgraded to a notice or successful no-op. Missing delta history is never described as an empty exact range.

---

## 26. Suggested catalog and storage changes

This section is illustrative. Public behavior and invariants are normative; private table names may change.

### 26.1 Prepared generations

A logged generation catalog may contain:

```text
pgt_prepared_graphs
  prepared_generation_id uuid primary key
  request_id              uuid not null
  owner_role              oid not null
  database_lineage_id     uuid not null
  state                   text not null
  capability_major        smallint not null
  capability_minor        smallint not null
  graph_refresh_id        bigint not null
  graph_digest            bytea not null
  source_boundary         jsonb not null
  source_boundary_digest  bytea not null
  generation_digest       bytea not null
  roots                    oid[] not null
  prepared_at              timestamptz not null
  completed_at             timestamptz
  completion_reason        text
  invalid_reason           text
  future_continuity        text not null
  unique (owner_role, request_id)
```

The state check permits `PREPARED`, `PROMOTED`, `ABANDONED`, and `INVALID`. Mutable status fields are excluded from the immutable generation digest.

### 26.2 Member manifests and leases

The immutable member manifest records:

```text
pgt_prepared_graph_members
  prepared_generation_id
  pgt_id
  storage_relid
  stream_contract_digest
  output_schema_digest
  storage_generation
  producing_refresh_id
  row_identity_version
  row_probe_version
  is_root
  primary key (prepared_generation_id, pgt_id)
```

Active leases are normalized separately so the database can enforce one active generation per member:

```text
pgt_prepared_graph_leases
  pgt_id primary key
  prepared_generation_id references pgt_prepared_graphs
  acquired_at
```

Promotion or abandonment deletes active lease rows but retains the immutable member manifest for audit.

### 26.3 Prepared consumer bindings

The consumer manifest records:

```text
pgt_prepared_graph_consumers
  prepared_generation_id
  consumer_id
  stream_table_id
  from_token_exclusive
  through_token_inclusive
  range_mode
  required_disposition
  output_contract_digest
  row_identity_version
  source_boundary_digest
  primary key (prepared_generation_id, consumer_id)
```

A batch or payload row referenced by an active prepared consumer binding participates in the output-log retention minimum even if another consumer has advanced farther.

### 26.4 Storage-generation marker

If no equivalent durable marker already exists, each stream table should have a monotonically increasing `storage_generation` or content epoch. Every supported operation that can change logical rows, replace storage, restore a snapshot, reinitialize, or recreate generated columns increments it in the same transaction. Preparation pins it, and opening or promotion validates it. A no-data refresh may leave it unchanged when the logical contents and storage identity are unchanged.

The marker is not a content hash and does not protect against malicious superuser writes. It is a complete supported-path invalidation token. A deep verification may add expensive checks when an operator suspects out-of-band tampering.

---

## 27. Integration with the refresh engine

`prepare_graph()` should be a thin coordination layer over the V1 strict graph-refresh implementation. It creates the same canonical graph plan, computes the same source-boundary context, takes the same locks, invokes the same member refresh operations, and uses the same common finalizer. The refresh engine returns a structured graph result; the prepared layer then validates output consumers, writes the immutable manifest and leases, and returns the prepared result.

This ordering matters. Leases cannot commit before the graph has successfully refreshed, but the refreshed graph cannot commit without leases. Both are therefore finalized in one transaction. Output-delta batches are created by the ordinary V1 finalizer and are merely bound by the generation; the prepared layer does not create another delta format.

Every existing member-mutation entry point should call one central `ensure_member_not_prepared()` guard after acquiring the member's catalog lock. Scattering direct queries against a private lease table through the codebase would make it easy for a later lifecycle feature to omit the check. The central guard should return a structured error containing the owning generation and permitted next actions.

Promotion and abandonment should similarly share one internal transition routine that acquires the generation and member locks, validates immutable state, applies the requested transition-specific action, and releases leases. Promotion supplies output acknowledgements; abandonment supplies a required reason and no acknowledgements. Neither path should call the graph-refresh engine.

The scheduler must treat an active member lease as a hard exclusion even though eligible graphs are already `EXTERNAL`. This redundant check protects against catalog corruption, orchestration-mode migration, and future scheduler features. A leased member is never refreshed because it happens to appear stale.

---

## 28. Lifecycle interactions

### 28.1 Create and initial preparation

A coordinating extension creates its graph and output consumers through the V1 contract. The graph normally has no prepared state until a large run begins. Initial population may itself be prepared. If the bound consumer has no baseline or the initial batches require full invalidation, the prepared consumer range requires `RESYNCHRONIZED`, and the coordinator builds its first domain result from the frozen terminal relations.

### 28.2 Strict refresh and repeated preparation

A V1 strict graph refresh is rejected while any member is leased. After promotion or abandonment, the graph may be refreshed normally or prepared again. The next preparation starts from the graph's current private state and the output consumer's last acknowledged position, not from the most recent abandoned generation's acceptance state.

### 28.3 Query and schema changes

Any semantic or output-changing alteration of a prepared member is blocked. The coordinating extension first abandons the generation, then creates or alters the desired graph according to the V1 contract, establishes new consumers or baselines where required, and prepares a new generation. A physical-only operation may proceed only when it is explicitly classified as safe and does not alter pinned storage generation or relation identity.

### 28.4 Ownership and orchestration changes

Owner transfer, role dependency changes, and returning a member to `MANAGED` orchestration are blocked while prepared. The generation's authorization and execution identities are immutable inputs. Promotion and abandonment do not change orchestration mode.

### 28.5 Suspend, resume, repair, snapshot, and restore

Operations that only report state may run. Operations that alter refresh eligibility, storage contents, frontier, output history, or relation identity are blocked until the generation is released. An invalid generation must be abandoned before repair. A stream-table snapshot API must not restore over a prepared member, and a snapshot taken from a prepared member does not become a second supported prepared generation.

### 28.6 Drop

Dropping a member or a bound output consumer fails while a prepared lease or binding exists. An administrator who intends to destroy the graph must explicitly abandon active generations first, preserving an audit reason. `DROP EXTENSION ... CASCADE` remains a superuser-level destruction operation and cannot preserve integration guarantees.

---

## 29. Observability and administration

The primary inspection API is:

```sql
SELECT *
FROM pgtrickle.prepared_graph_status(:generation_id);
```

It reports state, owner, request identifier, age, roots, member count, graph and generation digests, source-boundary summary, prepared consumer count, active reader count, future-continuity state, invalid reason, pinned output rows and bytes, estimated pending source rows and bytes, and every current lifecycle blocker. A separate list function returns one concise row per active or recently completed generation.

`prepared_graph_members()` reports member relation, contract digest, storage generation, producing refresh identifier, root status, and current validation result. `prepared_graph_consumers()` reports the immutable prepared ranges and required dispositions. Neither function exposes private change-buffer relation names or internal lease keys.

`health_check()` should warn about an invalid generation, an unexpectedly old prepared generation, missing or inconsistent leases, output history approaching a hard retention bound, source-buffer or WAL growth beyond advisory thresholds, a missing owner role, degraded future continuity, or a graph whose members no longer form a valid external closure. Alerts do not change state.

Useful metrics include preparation count and duration, prepared age, active generation count, lease conflicts, open-reader count, promotion and abandonment counts, promotion rollback retries, exact versus resynchronization ranges, pinned output bytes, pending source bytes, invalid generations, and future-reinitialization requirements. The coordinating extension remains responsible for reporting its domain-processing progress; `pg_trickle` may expose only the opaque request identifier that links the two systems.

An administrator may need a supported way to abandon a generation whose owner extension has been removed or whose role no longer exists. The ordinary abandonment function may permit a superuser or designated `pg_trickle` administrator after the same validation and audit requirements. There is no administrative “force promote,” because only the coordinating extension can prove that its domain publication was completed.

---

## 30. Performance and resource behavior

The freeze-in-place model avoids duplicating all graph rows. The direct storage overhead is limited to generation manifests, member and consumer rows, active leases, audit history, and the V1 output-delta and source-change data that must remain retained. Opening and status checks should be small catalog operations and should not scan member contents in fast mode.

The principal cost is deferred cleanup. Source CDC, upstream stream-table buffers, and external output logs can grow while the graph is frozen. Before preparation, `pg_trickle` should report or optionally enforce conservative admission bounds based on current pending bytes, recent source change rate, configured maximum prepared age, available storage, WAL retention risk, number of members, and number of bound consumers. Such estimates are advisory unless a hard resource policy is explicitly configured; an estimate must never be presented as a proof that indefinite retention is safe.

Once a generation is prepared, resource pressure cannot silently alter its meaning. `pg_trickle` may warn, pause unrelated work, or require an operator to abandon the generation, but it may not discard needed source changes, delete pinned output batches, release leases, or switch the generation to promoted state. If the database is approaching an unavoidable hard limit, preserving correctness may reduce availability.

Prepared reads should support keyset pagination through the terminal stream tables and batch/ordinal pagination through output deltas. The generation does not require one read transaction to scan the full relation. Repeated short reads are the purpose of the feature.

The initial capability should publish admission maxima for graph members, roots, bound consumers, generation-manifest size, concurrent prepared readers, and status result size. Exceeding a maximum fails before preparation rather than producing a partially leased graph.

---

## 31. Upgrade and compatibility policy

The proposal is additive. Existing databases have no prepared generations, and existing V1 APIs continue unchanged. The new capability is available only after the required catalogs, lease guards, storage-generation markers, recovery checks, and SQL functions have been installed and validated.

A compatible extension upgrade preserves active generation identifiers, digests, member manifests, leases, output bindings, and states. It may change physical indexes or internal query plans while preserving capability major 1. An upgrade that changes generation digest encoding, state transitions, prepared-read locking, member immutability, output-range semantics, or promotion behavior requires a new capability major.

An upgrade that cannot preserve an active prepared generation must fail before mutating extension state and instruct the operator to promote or abandon the generation. It may not auto-abandon or mark it promoted. A mandatory correctness repair that proves an existing generation invalid may mark it `INVALID` with an explicit reason, but it cannot silently reinterpret or rebuild that generation under new semantics.

Upgrade scripts and dump support must preserve the lease-before-scheduler invariant. There must be no startup window after an upgrade or restore in which the scheduler can refresh a member before active leases have been loaded and validated. Recovery and upgrade tests should deliberately crash or restart at each migration boundary.

The public contract is SQL-facing. A coordinating extension pins capability majors and generation digests and does not link to private Rust symbols. Package dependency ranges may narrow installation combinations, but runtime negotiation remains authoritative.

---

## 32. End-to-end protocols

### 32.1 Prepare and process

A higher-level coordinator first creates a durable run identifier in its own catalog. It then prepares the graph and commits the returned generation with that run record:

```sql
BEGIN;

INSERT INTO mdm_internal.runs(run_id, state)
VALUES (:run_id, 'preparing');

SELECT *
INTO TEMP TABLE _prepared
FROM pgtrickle.prepare_graph(
    request_id                  => :run_id,
    roots                       => :roots,
    expected_graph_digest       => :graph_digest,
    output_consumers            => :consumer_ids,
    require_complete_boundaries => true,
    full_policy                 => 'ALLOW'
);

UPDATE mdm_internal.runs
SET state             = 'evidence_ready',
    generation_id     = (SELECT prepared_generation_id FROM _prepared),
    generation_digest = (SELECT generation_digest FROM _prepared),
    source_boundary   = (SELECT source_boundary FROM _prepared)
WHERE run_id = :run_id;

COMMIT;
```

Each later work transaction opens the generation before reading:

```sql
BEGIN;

SELECT pgtrickle.open_prepared_graph(
    :generation_id,
    :generation_digest
);

-- Claim and process one coordinator-owned deterministic work range.
-- Read prepared stream tables or prepared output-delta rows.
-- Store an idempotent checkpoint in coordinator-owned tables.

COMMIT;
```

The graph remains immutable between these transactions. The coordinator can crash and resume by loading its run manifest and opening the same generation again.

### 32.2 Final publication and promotion

When all work is complete, the coordinator performs one final transaction:

```sql
BEGIN;

-- Coordinator-owned compare-and-swap checks.
SELECT mdm_internal.assert_run_publishable(
    run_id                    => :run_id,
    expected_publication      => :base_publication,
    expected_definition       => :definition_version,
    expected_decision_epoch   => :decision_epoch
);

-- Publish the complete domain result.
SELECT mdm_internal.publish_run(:run_id);

-- Accept exactly the prepared relational evidence.
SELECT pgtrickle.promote_prepared_graph(
    prepared_generation_id     => :generation_id,
    expected_generation_digest => :generation_digest,
    acknowledgements           => :prepared_acknowledgements
);

UPDATE mdm_internal.runs
SET state = 'published'
WHERE run_id = :run_id;

COMMIT;
```

If any assertion, domain validation, output DML, acknowledgement, or promotion check fails, the complete transaction rolls back. The old domain publication remains visible and the generation remains prepared for inspection or retry.

### 32.3 Supersession and abandonment

When a newer definition or decision makes the run obsolete, the coordinator records the reason and abandons in one transaction:

```sql
BEGIN;

UPDATE mdm_internal.runs
SET state = 'superseded',
    failure_reason = :reason
WHERE run_id = :run_id;

SELECT pgtrickle.abandon_prepared_graph(
    prepared_generation_id     => :generation_id,
    expected_generation_digest => :generation_digest,
    reason                      => :reason
);

COMMIT;
```

No output cursor advances. A later run sees the complete cumulative delta sequence from the last accepted publication or performs a prepared resynchronization.

---

## 33. Delivery plan

The capability should be delivered in four dependency-ordered phases. The feature remains disabled in capability discovery until each normative path is wired through the common mutation guards and the complete recovery matrix passes.

### Phase 1: generation contract and durable leases

Add the capability row in disabled form, generation and member catalogs, canonical generation digest, storage-generation markers, request idempotency, active lease storage, centralized mutation guards, lifecycle blocking, and basic status. This phase proves that a committed lease survives restart and prevents every supported member mutation, but it does not yet permit prepared output consumers or promotion.

### Phase 2: preparation and prepared reads

Implement `prepare_graph()` over the V1 strict refresh engine, atomic generation-and-lease finalization, `open_prepared_graph()`, shared versus exclusive transition locking, member inspection, source-write continuation, and basic abandonment. At the end of this phase a test extension can freeze a graph and read identical contents over several transactions, but it must still establish its own full baseline and cannot yet atomically consume V1 output cursors.

### Phase 3: prepared consumer ranges and promotion

Bind V1 output-delta consumers to generations, pin exact or resynchronization ranges, block direct acknowledgement and consumer lifecycle conflicts, implement typed promotion acknowledgements, atomically transition to `PROMOTED`, and prove rollback behavior with coordinator-owned publication tables. This phase completes the functional `pg_mdm` V2 dependency.

### Phase 4: recovery, upgrade, and operational hardening

Add fast and deep verification, startup recovery, invalid-state handling, future-continuity reporting, backup and restore tests, physical failover tests, upgrade gates, clone-lineage behavior, capacity forecasting, health checks, metrics, administrator abandonment, and a full conformance test extension. Only after this phase should the capability advertise `enabled = true` for production use.

---

## 34. Test plan

### 34.1 Unit and property tests

Pure tests should cover canonical generation encoding, digest golden vectors, state-transition legality, request idempotency, member and consumer ordering, required acknowledgement computation, exact versus resynchronization classification, lock-key derivation, and future-validity classification. Property tests should generate valid and invalid state sequences and prove that no path reaches `PROMOTED` without complete matching acknowledgements or releases a member lease while the generation remains prepared.

### 34.2 PostgreSQL integration tests

Integration tests should prepare one-node and multi-node external graphs, verify identical member contents across several transactions, allow source writes while prepared, prove that later source changes remain pending, and demonstrate that a later preparation catches up after promotion or abandonment. Tests must cover exact output ranges, ranges containing full invalidation, initial resynchronization, several bound consumers, and a generation without output consumers.

Every member mutation path must have a prepared-lease test: manual refresh, scheduler dispatch, V1 strict graph refresh, another preparation, alter query, storage recreation, owner transfer, orchestration-mode change, suspend/resume, repair, restore, rename, and drop. Physical-only operations classified as safe should have positive tests proving that generation digest and contents remain valid.

Concurrency tests should race overlapping preparations, prepared readers, promotion, abandonment, source DML, source DDL, output acknowledgement, consumer drop, and extension lifecycle operations. They should use condition-based synchronization rather than sleeps and run under PostgreSQL's supported isolation levels. Deadlock detection and deterministic lock order are release gates.

### 34.3 Atomic publication tests

A small independent test extension should maintain its own publication table and run manifest. It prepares a graph, performs checkpointed work over multiple transactions, writes a final publication, promotes, and commits. A forced error after `promote_prepared_graph()` must leave the generation prepared, leases active, publication unchanged, and consumer cursors unadvanced. A retry must then succeed without re-preparation.

The same test extension should abandon a generation and prove that no cursor advances. A later preparation must expose the cumulative exact range or require a full resynchronization. This is the key proof that abandonment does not lose evidence even though private stream-table contents are not reversed.

### 34.4 Crash, recovery, and upgrade tests

Tests should restart PostgreSQL after preparation, during an open read, during promotion before commit, and after promotion commit. Physical replica promotion should preserve the lease and state. Backup, PITR, and supported logical dump paths should either restore a valid generation or produce an explicit invalid result; no path may silently free the graph.

Upgrade tests should cover no active generation, one valid prepared generation, one invalid generation, terminal audit generations, and a deliberately incompatible future migration. The incompatible path must fail before catalog or storage mutation. Clone-lineage tests must prove that tokens from one writable history cannot be promoted in another without an explicit supported lineage policy.

### 34.5 Corruption and fail-closed tests

Tests should remove or alter a lease, member relation, storage-generation marker, output batch, payload relation, consumer contract, source-boundary record, and row-identity metadata. Fast verification and promotion must detect each case. Pending source-buffer loss after the prepared boundary should produce degraded future continuity without necessarily invalidating intact prepared member evidence, while loss of a bound prepared output range must invalidate promotion.

### 34.6 Security tests

Cross-role tests should verify mixed member ownership, revoked role membership, `SECURITY DEFINER` wrappers, unauthorized status inspection, deleted-row access through output deltas, generation ownership transfer attempts, and administrator abandonment. Search-path shadowing and dynamic SQL identifier tests remain mandatory.

### 34.7 Performance and longevity tests

Benchmarks should measure preparation overhead relative to V1 strict refresh, open and status latency, promotion latency with several consumers, lease-guard overhead on ordinary unprepared refreshes, output and source retention growth, and repeated paginated prepared reads. A longevity test should hold a generation while sustained source writes continue, report buffer and WAL growth accurately, promote or abandon, and then prove complete catch-up.

---

## 35. Alternatives considered

### 35.1 Keep the V1 transaction open

This remains the preferred path for small and medium workloads. It becomes operationally expensive when domain processing is long, checkpointed, or manually reviewed. The V2 capability exists only for cases where that measured cost is unacceptable.

### 35.2 Export a PostgreSQL snapshot

An exported snapshot can be imported only while the exporting transaction remains open, so it does not remove the long-lived transaction. It also does not by itself prevent stream-table lifecycle mutation after the exporter ends. A durable generation lease addresses the actual problem.

### 35.3 Use `PREPARE TRANSACTION`

Two-phase commit would retain a prepared database transaction and its locks rather than produce an ordinary committed private graph. Long-lived prepared transactions complicate vacuum, lock management, recovery, and operator safety and are disabled in many deployments. They are not an appropriate routine work queue for domain processing.

### 35.4 Copy terminal relations into coordinator-owned staging

This can work as an application pattern, but it duplicates large data, requires every extension to invent its own exact copy boundary and schema evolution, and separates the copy from `pg_trickle`'s output-delta and storage-generation proofs. The proposed lease reuses the authoritative private graph in place.

### 35.5 Clone every graph generation

Multiple physical copies would permit continued graph refresh and simultaneous preparations. They require a new storage-generation manager, generation-specific relation routing, delta fan-out, source-buffer retention across several frontiers, query and index lifecycle, garbage collection, and much larger storage. Capability major 1 intentionally avoids that scope.

### 35.6 Trust `EXTERNAL` mode without a lease

External orchestration prevents scheduler dispatch, but it does not stop another authorized manual refresh, strict graph refresh, repair, alteration, or coordinator. A persistent prepared lease is the explicit proof that the graph must not change.

### 35.7 Hold a session advisory lock

A session lock disappears on disconnect and does not survive failover. It is useful for transaction serialization but cannot represent durable prepared state. The proposal uses catalog leases as truth and ordinary locks only for transitions.

### 35.8 Let the coordinator acknowledge deltas before publication

Early acknowledgement would permit cleanup of evidence that the domain publication has not accepted. A later failure could leave no exact path from the accepted publication to the next result. Promotion therefore owns the prepared acknowledgements and shares the final publication transaction.

### 35.9 Reverse the graph on abandonment

Restoring the prior graph would require retaining a second copy or a complete invertible delta for every member and execution path. The output consumer's unadvanced cursor already preserves the logical difference safely. Reversal adds risk and is unnecessary.

### 35.10 Add an automatic timeout

Age is an operational signal, not proof of acceptance. Automatic promotion is unsafe and automatic abandonment may discard a coordinator's recoverable work or release evidence while it is still being read. The first contract alerts but requires an explicit transition.

---

## 36. Risks and open implementation choices

The largest correctness risk is an incomplete mutation guard. One forgotten refresh, repair, restore, or DDL path could change prepared evidence. The mitigation is a centralized guard called after canonical member locking, generated tests that enumerate all SQL mutation entry points, and a storage-generation marker checked by every open and promotion.

The largest operational risk is retention growth while a graph is prepared. The mitigation is the single-generation freeze model, admission forecasts, exact lag metrics, advisory age policies, and explicit abandonment. Correctness still takes priority over automatic cleanup, so deployments must size the prepared horizon conservatively.

The largest transactional risk is releasing leases while a processing transaction is still reading. The mitigation is the explicit shared `open_prepared_graph()` lock and exclusive transition lock. Documentation and the conformance test must treat opening as mandatory, not an optional status call.

The largest semantic risk is confusing private graph advancement with higher-level acceptance. The mitigation is the immutable prepared consumer range. Only promotion advances the external cursor; abandonment never does. The coordinating extension's own publication remains outside `pg_trickle` and must share the promotion transaction.

The proposal leaves several implementation choices for review:

1. Whether acknowledgement parameters use a typed composite array or a compact JSONB document. The validation and all-or-nothing behavior are unchanged.
2. Whether generation identifiers use PostgreSQL UUIDv7 support, ordinary UUID generation, or another opaque UUID. Ordering is not semantic.
3. Whether transaction transition locks use catalog row locks, advisory transaction locks, or both. Durable leases remain authoritative.
4. Whether `storage_generation` is a new catalog field or an existing complete mutation epoch can satisfy the same invariant.
5. Which physical maintenance operations are safe during preparation. Capability major 1 may conservatively block uncertain operations.
6. Whether fast verification marks `INVALID` directly or reports a finding that a startup or administrator transaction then persists. Promotion must fail either way.
7. How long terminal audit rows remain after promotion or abandonment. Retention may be configurable, but active generation proof is never pruned.

None of these choices justify multiple active generations, automatic expiry, private-catalog coupling, or weaker promotion atomicity in capability major 1.

---

## 37. Acceptance criteria

The prepared-generation capability is ready when an independently developed test extension can:

- discover and pin `prepared_graph_generation` major 1 and all prerequisite V1 capability majors;
- prepare a multi-node `EXTERNAL` graph under one complete source boundary and receive a durable UUID and generation digest;
- lose the preparation response, rediscover the committed generation by request identifier, and avoid creating a second generation;
- read exactly the same member contents and prepared output range over several later transactions by calling `open_prepared_graph()`;
- continue writing to source tables while the graph remains frozen and later prove that those changes were not included in the prepared boundary;
- observe hard errors for every refresh, DDL, repair, ownership, orchestration, consumer, and storage operation that would invalidate a prepared member;
- observe a hard overlap conflict when another preparation includes any leased member;
- process an exact prepared output range and a prepared full-resynchronization range without reading private `pg_trickle` objects;
- publish coordinator-owned state and promote in one transaction, then force rollback and prove that generation state, leases, and output cursors all remain unchanged;
- retry and commit promotion, then refresh the released graph through later pending source changes;
- abandon without acknowledgement and later consume the complete cumulative range from the last accepted cursor;
- survive backend disconnect, PostgreSQL restart, physical failover, backup and restore, and a supported extension upgrade with state and leases intact;
- detect missing member storage, changed contracts, lost output history, lineage mismatch, and other invalid conditions without silently re-preparing;
- distinguish an invalid prepared generation from degraded future refresh continuity;
- expose clear ownership checks, stable errors, prepared age, active readers, retained bytes, pending source growth, and next actions.

The feature is not ready merely because `prepare_graph()` returns a row. Every supported mutation path, rollback boundary, consumer transition, restart path, and upgrade path must preserve the invariants above. `pg_mdm` V2 should not claim resumable publication until this complete conformance matrix passes against packaged `pg_trickle` builds.

---

## 38. Recommended disposition

Accept the prepared-generation capability as a separate V2 proposal after the V1 composition contract is stable. Implement the first version as a single durable freeze over current external graph storage, one active generation per member, explicit prepared reads, bound output-delta ranges, transactional promotion, and explicit abandonment. Do not expand the first release into multi-generation storage, asynchronous graph construction, or coordinator worker management.

This boundary preserves the responsibilities of both projects:

> **`pg_trickle` owns the immutable relational generation and its continuity proofs. A coordinating extension owns the long-running domain computation and the decision to publish. PostgreSQL owns the final atomic commit.**

---

# Appendix A: possible later generations

**This appendix is non-normative and does not affect capability major 1 acceptance. Each item requires a separate proposal.**

A later implementation might store several physical graph generations simultaneously. That would let the graph continue refreshing while an earlier generation is under analysis, support concurrent previews, and reduce source-buffer retention. It would also require generation-specific member relations, query routing, index lifecycle, output-delta fan-out, source-frontier accounting, promotion or garbage-collection rules, and much larger storage. The capability should be added only after measured workloads show that freeze-in-place is inadequate.

Another later capability might prepare the graph asynchronously or checkpoint `pg_trickle`'s own graph refresh across transactions. That is distinct from this proposal. The current design makes the completed evidence immutable so a higher-level computation can be resumable; it does not make relational evidence construction resumable. An asynchronous design would need durable graph-refresh jobs, source-boundary leases, deterministic batch manifests, restart-safe work claiming, and a final graph-generation assembly proof.

A future capability could permit carefully declared coordinator dependencies, such as one prepared graph consuming another prepared graph or several coordinating extensions sharing a generation. That requires explicit ownership, acknowledgement, retention, and promotion precedence. Capability major 1 instead requires one externally controlled graph boundary and rejects scheduler-managed downstream dependents outside it.

Finally, object-storage snapshots or remote prepared generations may eventually reduce primary-database retention pressure. Such a design would need exact typed serialization, relation and collation compatibility, encryption and access control, restoration proof, and a transactionally safe promotion link back to PostgreSQL. None of those mechanisms should be inferred from the V2 capability proposed here.
