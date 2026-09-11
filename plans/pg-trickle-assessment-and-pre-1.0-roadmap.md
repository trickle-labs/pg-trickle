# pg-trickle: deep assessment and proposed pre-1.0 roadmap

**Assessment date:** 7 September 2026\
**Repository:** [trickle-labs/pg-trickle](https://github.com/trickle-labs/pg-trickle)\
**Reviewed revision:** [`ef6d75a60a431b407bccc623b6dbea4390589902`](https://github.com/trickle-labs/pg-trickle/commit/ef6d75a60a431b407bccc623b6dbea4390589902), corresponding to v0.94.0\
**Requested direction:** An exceptional PostgreSQL incremental view maintenance engine, with carefully bounded extension points.\
**Roadmap proposal:** v0.99.0 through v0.105.2. v1.0 qualification and
release candidates are deferred indefinitely.

## 1. Executive assessment

pg-trickle has the foundations of a compelling IVM product: PostgreSQL-native execution, broad relational differentiation, multiple refresh modes, durable relational state, explicit dependency management, exact row identities, and substantial correctness infrastructure. The project has also made several good decisions that should be preserved: moving messaging into pg_tide, removing heavy lakehouse dependencies, retaining conservative fallbacks, and declining to enable a window optimization that lost its benchmark. These are useful precedents for a focused path to 1.0. [Sources: refresh architecture][architecture], [dependency decision][adr009], [window admission][window-bench], [roadmap][roadmap].

My assessment is that **the main remaining challenge is making the implementation, advertised guarantees, and release evidence agree**. More feature breadth alone will not make this a world-class IVM engine. Predictable semantics, efficient composition, low interference with application writes, and demonstrable recovery behavior will.

The review found specific reasons to prioritize that work:

1. **A serious WAL receipt durability concern.** The active polling path calls a consuming logical-decoding function before the local buffer-writing transaction commits. This creates a plausible lost-change window on a subsequent error or rollback. Treat it as a release-blocking investigation, with a targeted fault-injection test and a durable-before-acknowledge design.
2. **Manual and graph refresh differ materially from scheduled refresh.** Manual differential refresh explicitly performs FULL refresh for any stream-table source. Strict graph refresh uses this path, undermining incremental composition. Its `full_policy = 'ERROR'` check does not cover every downstream FULL path.
3. **Graph V1 is advertised as stable without sufficient checked-in conformance evidence.** Several named tests exercise simple containers or mode parsing rather than the graph or authorization behavior their names imply. The v0.94 offline gate checks markers and test names, not transactional graph execution.
4. **Documentation contradicts runtime admission.** Set operations, volatility, recursion, aggregate strategies, identity, and integration claims need reconciliation.
5. **Performance evidence needs a stronger product contract.** Useful benchmarks exist, but foreground tail latency, source locks, output amplification, retention, and large shared graphs need to determine success alongside refresh throughput.

These are not reasons to replace the architecture. They are reasons to consolidate it around a small set of enforceable invariants. The recommended product promise is:

> Keep expensive PostgreSQL query results correct and fresh, with the least practical recomputation and a predictable impact on the database that serves the application.

**Recommended order:** close correctness and contract hazards immediately; finish the already planned operational work through 0.98; then execute the bounded v0.99.0–v0.105.2 IVM improvement programme. New external platforms and broad integrations should not enter that critical path.

## 2. Scope, evidence, and limitations

This is a source and release assessment, not a production certification or an independently reproduced benchmark study.

The review covered the pinned repository, release metadata, selected recent CI jobs, the current roadmap, SQL admission, row identity, snapshot planning, aggregates, refresh execution, graph integration, WAL capture, security context, recovery documentation, benchmark artifacts, and release workflows. Source searches also inspected test coverage and cross-path call sites.

The latest release observed was **v0.94.0**. Versions **0.95, 0.96, 0.97, and 0.98.x are planned**, so this report does not present their intended capabilities as shipped. The existing 0.98 plan explicitly prohibits new features after the 0.97 freeze. The proposed releases beyond 0.98 therefore require an explicit revision of that policy. [v0.94 release][release094], [0.95 plan][r095], [0.96 plan][r096], [0.97 plan][r097], [0.98 plan][r098].

Evidence labels used below:

| Label | Meaning |
|---|---|
| **Observed** | Directly visible in the reviewed source, configuration, artifact, or workflow. |
| **Executed** | A check run during this assessment. |
| **Code-path finding** | A behavior or risk derived by tracing implementation; no live PostgreSQL reproduction was performed here. |
| **Recommendation** | Proposed future behavior, design, or acceptance criterion. |

Available verification:

| Check | Result | What it establishes |
|---|---|---|
| Local revision check | Matched the pinned commit | All local source findings refer to one baseline. |
| `python3 scripts/v0_94_release_gate.py` | Passed | Required v0.94 artifacts, source markers, and named tests exist. It does not prove graph correctness. |
| `python3 scripts/v0_89_release_gate.py` | Passed | The checked-in window admission artifacts satisfy the offline contract. Timings were not rerun. |
| `python3 scripts/check_docs_truth.py` | Failed with 9 findings | The documentation/catalog checking path currently reports unresolved drift. Some findings are missing catalog entries for implemented APIs. |
| GitHub CI for the pinned commit | Overall success | Unit, integration, light E2E, smoke, and other listed jobs succeeded; several broader suites were skipped for this trigger. |
| Repository working tree | Clean | No project code was changed. |

Docker, PostgreSQL, Cargo, Rust, and `just` were unavailable in this assessment environment. Consequently, **no extension build, database E2E test, crash test, or performance benchmark was run locally**. Potential defects below retain this limitation. The checked-in tests and successful CI are useful evidence, but neither establishes that every roadmap exit criterion has passed. [CI run][ci-run], [CI definition][ci], [v0.94 gate][gate094].

## 3. What should remain central

### 3.1 Preserve the PostgreSQL execution model

The core design differentiates relational operators into delta SQL and uses PostgreSQL for query execution and persisted state. This is a strong fit for the product: users retain SQL, transactions, indexes, familiar operations, and one database to administer. Keep improving the delta plan and the surrounding state protocol before considering a replacement query runtime. [Differentiation context][diff], [refresh implementation][merge].

A useful internal boundary is:

| Layer | Core responsibility |
|---|---|
| Analysis and admission | Resolve PostgreSQL semantics and establish which maintenance strategies are safe. |
| Delta planning | Choose legal differentiation, affected regions, state access, and execution strategy. |
| Capture and frontiers | Preserve committed changes and identify the exact input boundary. |
| Owner execution | Evaluate user-authored semantics under the intended role and stored search path. |
| Transactional finalization | Commit output, private state, frontiers, history, and opted-in output batches consistently. |
| Scheduling and operations | Admit work, track freshness, manage pressure, recover, and explain behavior. |
| Extension contracts | Let other tools coordinate or consume the result without depending on private state. |

### 3.2 Preserve conservative admission

The runtime explicitly distinguishes proven incremental forms from forms requiring FULL. For example, `INTERSECT` and `EXCEPT` are blocked from explicit differential/immediate execution, while AUTO can choose FULL; non-immutable expressions are also conservatively handled. That is preferable to shipping an optimistic SQL coverage claim with hidden semantic holes. Improve the supported surface by proving additional cases and remove incorrect claims immediately. [Admission rules][admission].

### 3.3 Preserve exact identity and multiset comparison

The V2 identity design uses exact canonical `BYTEA`, with a probe used as an accelerator rather than a substitute for equality. The test oracle compares both directions of `EXCEPT ALL`, checks schema, and can detect silent fallback. These are important foundations for duplicates, NULLs, joins, and replay. Strengthen them; do not simplify back to hash-only equality or row-count validation. [V2 contract][rowid-doc], [V2 encoder][rowid-code], [exact oracle][oracle].

### 3.4 Preserve evidence-based optimization admission

The v0.89 window candidate was rejected after losing to partition recomputation in every measured cell. The v0.88 aggregate work uses narrow, dependency-free typed pages. The accompanying decisions are more valuable than a large list of nominal optimizations: they connect an implementation choice to measured end-to-end benefit. Apply the same standard to every new join rule, aggregate state, controller decision, and storage optimization. [Window evidence][window-bench], [aggregate evidence][aggregate-bench], [ADR-009][adr009].

## 4. Priority findings

Priorities describe the recommended response. They do not imply that every code-path concern has been reproduced in a database.

| ID | Priority | Finding | Evidence status | Recommended response |
|---|---|---|---|---|
| F01 | P0 investigation | WAL consumption precedes durable receiving-transaction commit | High-confidence code-path risk | Fault-inject immediately; correct receipt/acknowledgement order before relying on WAL capture. |
| F02 | P1 | Graph/manual composition forces FULL and can bypass the FULL policy | Code-path finding | Enforce actual strategy policy in the executor; unify refresh paths. |
| F03 | P1 | Graph stability claim exceeds checked-in conformance evidence | Observed | Require live graph, authorization, rollback, and recovery conformance before stable admission. |
| F04 | P1 | Manual/strict graph source locks can block foreground writes through the caller transaction | Observed mechanism; performance impact unmeasured here | Publish lock behavior, measure it, and design narrower proof-preserving boundaries. |
| F05 | P1 | Graph boundary/context failure handling needs explicit proof | Code-path risks | Test WAL lag, mixed paths, and exception cleanup; use scoped context guards. |
| F06 | P1 | Documentation and feature claims disagree with implementation | Observed and executed | Generate authoritative capability examples and reconcile active documentation. |
| F07 | P1 | Release gates do not yet demonstrate all promises on the packaged candidate | Observed | Bind runtime evidence to the exact candidate and test real maintained data in packages. |
| F08 | P1 | Numeric and structural-type semantics need deeper qualification | Observed mechanisms; correctness risk requires targeted tests | Establish numerical contracts and expand exact identity support only with proof. |
| F09 | P2 | Performance accounting lacks sufficient locality and amplification evidence | Observed partial instrumentation | Measure work touched, emitted, retained, and written per strategy. |
| F10 | P2 | SQL generation and execution orchestration remain costly to reason about | Observed structure | Consolidate around typed plans, explicit state contracts, and one finalizer. |
| F11 | P2 | Operational interfaces and extension scope remain broader than necessary | Observed/planned | Stabilize the small public contract; isolate optional ecosystems. |

### F01. Make WAL receipt durable before acknowledging consumption

**Observed:** `poll_wal_changes()` issues `pg_logical_slot_get_changes()`, obtains its result through SPI, and only then loops over decoded rows to insert changes into local buffers. `poll_source_changes()` subsequently updates the catalog position. The scheduler invokes this work inside a transaction that also processes other sources and transition work. [WAL polling][wal], [scheduler transaction][scheduler-loop].

PostgreSQL distinguishes consuming `get_changes` from non-consuming `peek_changes`. Its SQL decoding implementation confirms the slot position before returning the materialized result. The receiving application transaction is a separate durability boundary. [PostgreSQL replication functions][pg-functions], [PostgreSQL decoding implementation][pg-logical-source].

**Code-path concern:** if buffer decoding/insertion, a later source, or the surrounding transaction fails after consumption, local buffer rows and catalog progress can roll back while the slot has already moved forward. Setting synchronous commit for buffer writes does not make an acknowledgement issued before that commit transactional. The visible error counter and eventual fallback are not equivalent to recovering the consumed batch.

This is the most urgent investigation in the report. It is a concrete ordering concern, not a claim that data loss was reproduced here.

**Recommended design:**

1. Read without acknowledging, or use a decoding receiver whose feedback is controlled explicitly.
2. Persist complete source transactions, a replay identity, and the receiving high-water mark in one local transaction.
3. Acknowledge only the durably committed boundary.
4. On restart, replay safely using durable receipt state. Do not deduplicate legitimate duplicate rows merely because their payloads are equal.
5. If retained history cannot cover the committed receipt boundary, suspend with a precise reason and require a proven rebuild.

The identity must distinguish individual events or complete transaction batches; a row key or transaction ID alone is insufficient for several changes to the same row in one transaction. The local and Citus remote polling paths should share the same receipt invariant, even if Citus remains an optional integration. [Remote poll implementation][citus].

**Required validation:** inject failure immediately after slot consumption/read, during buffer insertion, after durable receipt but before acknowledgement, and while another source in the same polling cycle fails. Compare subsequent results and receipt history after retries and restarts. A correct test must account for both missing events and replayed events. PostgreSQL explicitly permits logical-decoding replay after a crash, so replay handling belongs in the contract. [Logical decoding concepts][pg-logical].

**Timing:** address now, before adding output-delta consumers. This must not wait for 0.99.

### F02. Make incremental graph composition real and enforce actual FULL policy

**Observed:** `execute_manual_differential_refresh()` contains an unconditional FULL fallback when any dependency is a stream table. Its comment explains that the manual FULL path does not populate the downstream change buffer used by the scheduled path. `refresh_graph_strict()` delegates member execution to this manual machinery. [Manual refresh][manual], [strict graph implementation][graph].

This means an otherwise differential chain can refresh downstream members fully through the external graph path. That preserves results in the stated guard scenario, but it is a substantial limitation for a product whose strength should be efficient composition.

There is also a policy mismatch. Strict graph refresh checks metadata for `full_policy = 'ERROR'`, then calls the executor without passing that policy. The downstream-stream-table guard and other runtime FULL fallbacks occur later. The implementation can therefore accept a member at the policy check and subsequently execute FULL. Conversely, the precheck rejects TopK based on its presence even though the proposal permits scoped recomputation under this policy. [Graph implementation][graph], [manual fallback][manual], [runtime fallbacks][merge], [Graph V1 policy][proposal].

**Recommended change:** pass a structured refresh context containing the graph boundary, ownership, allowed strategies, and finalization obligations. Enforce the policy at every strategy-selection boundary, including runtime fallback. A policy error should roll back the whole graph. Correctness must remain superior to an incremental-only preference.

Then remove the manual/scheduler discrepancy: all successful refresh paths must produce the downstream evidence needed by later members, with explicitly qualified invalidation for paths that cannot cheaply emit exact deltas.

**Acceptance:** the same chain and diamond workloads produce identical multisets and equivalent effective strategies under scheduled, manual, and external coordination where their contracts overlap. Inject a downstream FULL requirement and verify that `ERROR` aborts rather than silently recomputing.

### F03. Replace graph test names with graph conformance

**Observed:** in `src/api/integration.rs`, the test named for graph closure/order checks a `BTreeSet`; the test named for authorization checks that an unknown mode string is rejected; the scheduler-exclusion test checks mode compatibility. Those checks can be useful unit tests, but they do not verify the named integration properties. The v0.94 gate looks for source markers and the presence of two such names. It passed during this assessment. [Integration tests][graph], [offline gate][gate094].

Meanwhile, capability discovery reports Graph V1 enabled and stable. The checked-in v0.94 roadmap still lists its substantive conformance exit criteria as unchecked. A search of `src/` and `tests/` found the strict-refresh implementation but no database test invocation of `refresh_graph_strict()`; the v0.93 smoke test inspects contracts rather than performing a strict graph refresh. [Capability discovery][graph], [release criteria][r094], [smoke tests][smoke].

**Recommendation:** stable capability admission should depend on a runnable, independent conformance suite. It should create actual graphs, invoke the public SQL surface as distinct roles, verify every maintained multiset, and exercise coordinator writes and rollback in the same transaction.

Require one-node, chain, diamond, shared-upstream, stale-digest, busy-lock, drop/recreate, changed-owner, restore/clone, and deliberate coordinator-failure cases. Assert contents, frontiers, history, ownership, and capability state. Keep marker checks as packaging checks; do not count them as behavioral evidence.

If a capability must be withdrawn until this is proven, communicate the compatibility impact explicitly. The recommendation is to qualify the existing boundary, not to silently change what integrators believe is stable.

### F04. Treat source-lock duration as a first-class performance cost

**Observed:** `lock_source_relations()` locks managed source relations and partition descendants in `SHARE` mode. Manual differential refresh calls it; strict graph refresh locks its admitted sources before executing members. [Source locking][cdc], [manual path][manual], [graph path][graph].

PostgreSQL `SHARE` conflicts with the table lock taken by inserts, updates, deletes, and merges. Locks normally last until transaction end. For strict graph refresh, that can include additional coordinator work after the function returns. This is a correctness mechanism with a potentially large foreground-write cost. [PostgreSQL lock semantics][pg-locks].

**Recommendation:** immediately document the modes that take these locks and report acquisition wait and hold duration. Benchmark application writes during complete graph transactions, including slow coordinator work. Do not describe these paths as nearly invisible to OLTP based only on isolated refresh timings.

Longer term, investigate a snapshot and capture-boundary protocol that avoids holding source write-conflicting locks throughout ordinary delta computation. A lock-free claim requires a proof that late commits, old snapshots, schema changes, and WAL lag cannot invalidate the boundary. Reducing lock strength without that proof would be a regression.

### F05. Strengthen graph boundary proof and exception cleanup

Two narrow issues deserve explicit tests:

- **Boundary completeness:** `build_source_boundary()` labels source entries `PROVEN` after obtaining their positions. WAL positions come from committed decoder progress, which can lag the live source state. The graph also permits FULL execution, whose source reads can be newer than that decoder boundary. Establish exactly what consistency point a mixed FULL/differential graph promises, and show that every member observes that point. Returning a vector of tokens is useful; it does not itself prove coherent evaluation. [Boundary construction][graph], [frontier construction][cdc].
- **Ambient context cleanup:** `with_graph_safe_bound()` replaces a thread-local value, calls a closure, and restores the value afterward. There is no visible scoped guard around that restoration. An unwind before the restoration could leave a stale bound in the backend. Test a caught PostgreSQL error followed by a normal refresh in the same session, and use a guard or PostgreSQL transaction/subtransaction cleanup mechanism appropriate to the error model. [Graph-bound context][manual].

These are code-path risks, not demonstrated wrong-result cases. Their importance is that graph composition expands the correctness boundary beyond one table. A strict API needs proof for every permitted combination, including no-data execution and failures.

### F06. Establish one authoritative SQL and operational support contract

The contradictions are substantial enough to affect adoption and configuration decisions:

| Subject | Conflicting published material | Stronger evidence at the reviewed revision |
|---|---|---|
| `INTERSECT` / `EXCEPT` | README presents full differential support. | Runtime admission requires FULL/AUTO and rejects explicit differential/immediate use. |
| STABLE expressions | README describes warnings and lists current-time expressions as supported. | Admission rejects non-immutable expressions for explicit incremental modes. |
| Windows | Limitations page directs users to FULL. | Current support matrix and window code use partition recomputation; the new state algorithm is disabled. |
| Recursion | Support matrix says recursive CTEs always use FULL. | Recursive operator code contains semi-naive, DRed, and recomputation strategies with restrictions. |
| DISTINCT aggregates | Limitations page describes algebraic `COUNT(DISTINCT)`. | `classify_agg_strategy()` classifies DISTINCT aggregates as group rescan. |
| Row identity | Older prose still describes hashed identities. | V2 uses exact canonical `BYTEA`, with restricted identity type support. |
| DuckLake sink | README presents native sink functionality. | Dependency documentation records its removal in v0.76; current dependencies do not include the old sink stack. |
| CDC mechanism | README emphasizes row triggers; other docs describe a decoder worker. | Current architecture states statement triggers by default; active WAL polling is driven by the scheduler. |

[README][readme], [limitations][limitations], [support matrix][support], [admission][admission], [aggregate classification][types], [recursive implementation][recursive], [dependency policy][dependencies], [WAL implementation][wal].

The executed docs check found nine issues. Four current graph APIs are missing from the generated SQL catalog, producing several findings; other examples include a removed batch-size name, DuckLake sink references, and a freshness setting described as a GUC. These are mixed documentation and generator problems, not nine missing runtime implementations. The checker also has broad exemptions for stale and anticipated interfaces. [Docs checker][docs-check], [exemptions][docs-allow], [generated SQL catalog][api-catalog].

**Recommendation:** generate a capability manifest from executable admission examples and stable strategy/reason identifiers. Each documented query shape should state:

- Whether it is accepted, and in which refresh modes.
- The actual strategy: delta, affected-group/partition recomputation, whole-query recomputation, or rejection.
- Identity/type/collation and ordering restrictions.
- Why the strategy was selected and how to make it cheaper.
- The version in which the behavior was verified.

Use that manifest to generate the README summary and support matrix. Run runnable examples against the packaged build. Retain historical plans as history, clearly separated from current product documentation. Add README changes to semantic documentation checks, and replace unbounded exemptions with a specific owner, scope, and expiry condition.

### F07. Qualify the exact release artifact

There is meaningful CI infrastructure already: live DVM smoke suites, composition and metamorphic tests, negative controls, path-triggered full E2E, security checks, and upgrade checks. The improvement is to establish what each gate actually proves. [CI][ci], [DVM tiers][dvm-tiers], [oracle][oracle].

Several concrete gaps remain:

1. The release preflight invokes gates through v0.90 but does not invoke the checked-in v0.91–v0.94 scripts. Those newer scripts are exposed through `justfile`. Even adding them would provide only the proof those scripts actually perform.
2. The Linux release smoke test installs the extension, checks its version/configuration, and calls status. It does not create a source, maintain a stream table through mutations, or perform an upgrade with pending changes.
3. The path filter for the dedicated DVM full-E2E job covers `src/dvm/**`, `src/refresh/**`, and `src/cdc/**`. It omits correctness-sensitive peers such as `src/wal_decoder.rs`, graph APIs, version/frontier logic, and scheduler files. Other jobs provide coverage, but this filter is not a complete map of the correctness boundary.
4. The successful main CI run for this revision skipped broader full E2E, binary-upgrade, and pgbench jobs for that trigger. Release readiness must not infer their success from the overall green check.
5. The pgbench step retries the whole benchmark up to three times and accepts any successful attempt. This reduces sensitivity to noisy infrastructure but also makes acceptance depend on a favorable run.

[Release workflow][release-workflow], [CI workflow][ci], [observed CI run][ci-run].

**Recommendation:** introduce one release-evidence manifest binding source SHA, artifact digest, PostgreSQL/platform, suite version, workload, result, and retained logs. Reuse prior runs only if they match the candidate and relevant artifact. A finalizer or migration change should invalidate the corresponding evidence.

Retain all performance attempts and evaluate a prespecified statistical rule over them. Infrastructure failure should be distinguishable from a workload regression. Supported platforms need executable package tests; a platform can be explicitly experimental if that is the realistic support level.

### F08. Qualify numeric fidelity and extend identity types deliberately

**Numerical risk:** statistical aggregates use sum, sum-of-squares, count, and cross-product auxiliaries. The variance expression subtracts two potentially large quantities, and clamps a negative result to zero. Accumulator resolution allows `double precision` for appropriate overloads. [Aggregate arithmetic][aggregate], [accumulator types][types].

An isolated Python binary64 arithmetic check during this assessment illustrates the risk: for values `100000001`, `100000002`, `100000003`, the displayed sum-of-squares formula produced variance `0.0`; the exact mathematical variance is `2/3`. This is **not a PostgreSQL or pg-trickle execution result**, but it gives a concrete adversarial input for the database suite. Clamping prevents a negative result from surfacing; it does not recover lost precision.

**Recommendation:** define numerical behavior per PostgreSQL aggregate overload. Test high-offset/low-variance inputs, cancellation, extreme magnitudes, NULL transitions, NaN/infinities, repeated retract/reinsert cycles, and batch-order variation. Compare to PostgreSQL under controlled execution conditions. Exact integer/numeric semantics must remain exact; floating-point tolerances must be explicit and must not hide large semantic errors. A stable retractable algorithm, bounded periodic recomputation, or a conservative fallback can each be appropriate if the policy is visible.

**Identity coverage:** V2 deliberately rejects structural types and nondeterministic collations. That is a sound conservative boundary, but it constrains JSONB, arrays, composites, ranges, and custom types when they participate in identities. Distinguish this from merely carrying a payload column behind a supported key: “JSONB is supported” is too broad to describe both cases. [Identity registry][rowid-code], [wire contract][rowid-doc].

Prioritize exact JSONB/array identity support only if supported workloads need it. Every admitted family needs a canonical equality contract, resource limits, collation/type-change invalidation, and migration behavior. Hashes may accelerate equality checks; they must not become identity truth again.

## 5. The most valuable IVM improvements

### 5.1 Use an honest definition of incremental work

A one-row source change does not always imply one-row maintenance. One changed dimension row can alter millions of join outputs. Inserting at the beginning of a ranked partition can change every rank. Deleting an extremum can require additional work to discover the next value.

The useful objective is **work proportional to the changed inputs, relevant state, and changed outputs wherever the supported query permits it**. Track those components separately. Avoid universal O(Δ) or “one row means one row” claims.

For each strategy, measure:

| Metric | What it reveals |
|---|---|
| Input delta rows and bytes | Capture/batching cost. |
| Rows and bytes examined | Whether a small delta causes a broad scan. |
| Affected groups, keys, partitions, and join fanout | The region that logically needs work. |
| Output inserts, deletes, and updates | Unavoidable result amplification. |
| Private state reads/writes and size | Cost of maintaining the optimization itself. |
| WAL, temporary bytes, index writes, and cleanup work | Database cost beyond executor time. |
| Planning, execution, apply, lock wait, and commit time | Where latency is actually spent. |

The current planner already records estimates and observations, but its `actual_intermediate_rows` field explicitly describes the final delta relation, not per-operator cardinalities. Extend it with bounded, opt-in operator-level evidence where useful. [Planner observations][planner].

### 5.2 Close the exact relational-state gaps

The most compelling SQL expansion is to make the existing relational surface more consistently incremental.

**Set operations:** implement durable private left/right multiplicity state and exact boundary crossings for `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, and `EXCEPT ALL`. Test duplicates and NULLs, both-side changes in one batch, empty/nonempty transitions, and nested composition. Do not simply remove the admission gate because differentiator modules exist. [Current set admission][admission].

**DISTINCT aggregates:** evaluate private per-group value counts for common cases such as `COUNT(DISTINCT)`. The state must correctly retract the last occurrence and preserve SQL NULL behavior. Compare its storage/write amplification with group rescanning; enable only the cases that win. [Aggregate strategies][types].

**MIN/MAX and other aggregates:** preserve batched affected-group rescans as a sound baseline. For workloads dominated by repeated extremum deletion, test an ordered per-group support structure. For ordered/holistic aggregates, scoped recomputation may remain the best product choice. A large state index that costs more to maintain is not progress.

### 5.3 Improve delta locality before building another optimizer

ADR-010 correctly assigns physical join planning to PostgreSQL. pg-trickle should supply semantically safe relational alternatives and the evidence needed to choose among them. [Planning ownership][adr010].

High-value work includes:

- Extend existing unchanged-source pruning and compacted-delta paths where equivalence is proven.
- Preserve selective predicates and narrow projections through generated CTEs.
- Batch affected-key lookups rather than repeatedly rescanning a large source.
- Improve statistics visibility for temporary/staged delta relations without running expensive analysis on every tiny refresh.
- Distinguish join fanout and affected-region size from source change percentage in fallback decisions.
- Keep materialization and rewrite decisions bounded; stale evidence should preserve a known safe plan.
- Recommend useful source/join/group indexes with their write cost visible. Automatic index creation needs a clear user policy.

Start with representative production-shaped joins and aggregates, then add rules individually. The repository already contains pruning, per-leaf fallback, and adaptive strategy work; the proposed improvement is to quantify and safely extend those mechanisms, not rebuild them. [Refresh strategy implementation][merge], [planner][planner].

### 5.4 Treat windows and TopK as workload-specific problems

Retain partition recomputation as the default until a genuine incremental algorithm wins. The rejected v0.89 candidate still recomputed the affected partition and added state overhead; that result does not prove every possible window algorithm is unhelpful. It does prove this candidate should remain disabled. [Window benchmark][window-bench].

Useful bounded candidates include monotone append-at-tail ordering, small offset windows, and narrowly defined sliding aggregates. Each needs explicit handling of peers, NULL ordering, late rows, deletions, collation, frame bounds, and deterministic tie-breaking. Rank updates with large output amplification cannot be optimized away.

For TopK, prioritize deterministic ordering and cheap access to the next eligible rows after deletes. Do not widen SQL coverage simply to tick off function names. A documented scoped-recompute strategy with stable performance is a valid 1.0 outcome.

### 5.5 Make capture economical across many sources

The current WAL implementation uses `test_decoding` and filters its database-wide text output per source slot. Repeating database decoding for many source slots is a scaling concern even when each individual source has a small delta. This should be measured with realistic numbers of tracked tables and unrelated database writes. [WAL code][wal].

After the durability issue is closed, consider a database-level receiver that decodes once and routes typed events to source buffers. Prefer a documented production decoding protocol or a narrowly scoped decoder implementation. Preserve independent consumer frontiers and cleanup safety. This remains internal IVM infrastructure; it does not require turning pg-trickle into a general CDC service.

### 5.6 Optimize state lifetime as carefully as state computation

The pipeline already stages bounded batches inside one outer transaction. Those batches do not provide early visibility or early release of outer transaction locks. Its admission also includes compatibility exclusions and oversized-batch accounting. [Pipeline][pipeline], [architecture][architecture].

The next improvements should account for all live state: source buffers, intermediate deltas, auxiliary tables, output logs, temporary spill, indexes, and obsolete schema generations. Specify ownership and reclamation conditions for each state family.

For every retained item, ask: which frontier, active consumer, prepared binding, or recovery operation still needs it? Cleanup should follow that evidence. Workloads with a slow consumer must reveal their storage liability before a disk crisis.

## 6. Operations, security, and maintainability

### 6.1 Finish existing operational plans before adding another layer

Versions 0.90–0.92 already establish freshness evidence, schema evolution, recovery identity, and upgrade safeguards. Planned 0.96–0.97 cover resource-derived defaults, enforcement classes, progress, stable errors, roles, monitoring, soak, and packaging. These are appropriate priorities and should not be renamed as discoveries of this report. [Freshness controller][controller], [0.91 plan][r091], [0.92 plan][r092], [0.96 plan][r096], [0.97 plan][r097].

The improvement beyond them is integrated validation: run realistic write traffic while a graph rebuild, slow consumer, slot lag, and resource pressure overlap. A resource limit that works in isolation is insufficient if another executor path bypasses it.

### 6.2 Freshness needs evidence, feasibility, and controlled authority

Keep exact commit-to-visible observations separate from time since the last refresh or a schedule target. The current controller has bounded samples and advisory decisions; that is a good starting point. [Controller][controller], [freshness diagnostics][diagnostics].

Before authorizing a new automatic action, require evidence that it improves the declared workload objective. Interval, batching, refresh strategy, and concurrency are separate decisions with different risks. Each should have independent admission, limits, hysteresis, and an explanation.

Expose at least: target, measurement availability, observation window/sample count, recent achieved freshness, backlog age, cause of infeasibility, and current action. A system should be able to say that a target is infeasible without repeatedly escalating work and harming the application.

A source commit timestamp alone is not an ordering or completeness proof. Retain the frontier protocol as the authority for what was consumed.

### 6.3 Recovery must preserve evidence, not just restart work

The current restore/clone identity and quarantine direction is sound. Build a state-transition contract across ordinary refresh, initial population, ALTER shadow rebuild, upgrade, restore, and capture repair. [Recovery plan][r092], [recovery API][recovery], [upgrade guidance][upgrading].

For each operation, document which old result remains readable, what happens to pending changes, how interrupted work is identified, whether a resnapshot is required, and how the operator can verify completion.

Capacity planning for a rebuild must include old storage, shadow storage, both sets of indexes, accumulated CDC, and WAL. “Online” should describe reader availability and measured lock behavior, not imply that a large rebuild is free or instantaneous.

The proposed post-0.98 releases also require updating the upgrade support manifest, which currently bounds the path at 0.98.x. Preserve the distinction between an in-place upgrade and the earlier V1-to-V2 recreation/resnapshot boundary. Do not promise automatic preservation of state that the existing contract deliberately recreates. [Support manifest][upgrade-manifest], [V2 upgrade contract][rowid-doc].

### 6.4 Keep security tied to owner execution and retained data

The owner execution wrapper and deny-first ACL model are strong decisions. Centralized identity switching is especially important when SQL executes in a backend capable of privileged catalog work. [Security context][security-context], [security model][security].

Extend adversarial tests across every executor entry point, including graph refresh and future output consumers. Cover role changes, `SECURITY DEFINER` nesting, stored search paths, dropped/recreated functions and relations, changed RLS, direct private-table access, and cleanup after exceptions.

Treat retained deleted rows and reversible identity bytes as user data. Output log permissions and resnapshot permissions must be as deliberate as current-table permissions. A diagnostic bundle should redact these by default and remain useful through lengths, counts, and safe fingerprints.

RLS semantics also affect performance: the runtime can fall back to FULL when source RLS is enabled because old owner-visible rows cannot safely be reconstructed. Publish this as part of the capability contract rather than a surprising runtime behavior. [RLS fallback][merge].

### 6.5 Refactor around invariants, not file size alone

The reviewed `src/` contains **125 Rust files and 166,912 physical lines**, including embedded tests and comments. Several modules exceed 4,000 lines; the largest inspected modules include sublink parsing, rewrites, aggregates, scheduler, catalog, and lifecycle code. These counts indicate review surface, not defect density or executed coverage.

The most valuable refactors are those that make a correctness obligation local:

- One structured refresh context and finalization protocol across entry points.
- One persisted state lifecycle abstraction for generation, ownership, readiness, rebuild, and cleanup.
- Typed SQL relation/schema metadata carried through rewrites, including collation and identity where required.
- Explicit unknown metadata rather than silently treating an unresolved type or schema as equivalent.
- SQL construction that separates identifiers, expressions, bindings, and relation references.
- One capability/strategy registry from which tests and documentation can be generated.
- Small wrappers around PostgreSQL memory, resource-owner, and exception boundaries.

There are already useful pieces: typed schemas, a centralized SQL builder, a snapshot plan, separated optimization/cache state, and the owner context. Extend these incrementally. Do not embark on a broad parser rewrite while correctness-sensitive refresh consolidation is unfinished. [Schema contracts][schema], [snapshot plans][snapshot], [SQL builder][sql-builder], [differentiation context][diff].

## 7. Extension points that strengthen the IVM product

The project should have a small, reliable integration boundary. The Graph V1 and planned Delta V1 separation is the right direction: coordination and change consumption have different obligations and should earn stability independently. [Integration proposal][proposal], [0.95 delta plan][r095].

| Extension point | What belongs in pg-trickle | What should remain outside |
|---|---|---|
| Graph coordination | Capability/version discovery, deterministic contracts, explicit ownership, one transactional refresh, exact boundary/outcome reporting. | A second job scheduler, workflow language, or external orchestration service. |
| Output deltas | Typed committed batches, sequence/cursor identity, exact changes or explicit invalidation, retention, acknowledgement, and resnapshot. | Kafka/NATS/SQS clients, retries to remote destinations, message routing, and broker administration. |
| Change-source abstraction | Internally: typed source positions, transaction completeness, snapshot establishment, replay, schema-change and recovery obligations. | A universal connector marketplace or arbitrary callback execution inside the refresh transaction. |
| Aggregate support | PostgreSQL-resolved signatures, proven algebraic state or safe group recomputation, explicit type/NULL/overflow semantics. | Treating every user aggregate as incrementally invertible because it has transition/final functions. |
| Observability | Stable SQL views, reason identifiers, metric/span schemas, bounded diagnostics. | A mandatory dashboard service or control plane. |
| Lifecycle integrations | Read-only planning/preflight, deterministic apply behavior, supported grants and upgrade contracts. | Provider-specific deployment logic and policy embedded in the IVM executor. |

### 7.1 Finish Delta V1 without making ordinary refresh pay for it

The 0.95 plan already contains unusually important details: shared payload storage, immutable batches, zero-row batches, FULL invalidation, explicit resnapshot, retaining cursors, hard-pressure behavior, and owner-only access to deleted rows. Preserve that scope. [0.95 plan][r095].

Add a public conformance harness and a minimal reference consumer. The consumer must demonstrate rollback, replay, invalidation, retention pressure, and resnapshot through supported APIs. It should never need a private change buffer, physical table name, or interpretation of identity bytes.

Keep disabled overhead measurable. A deployment with no output consumers should not incur output-log writes, consumer scans, or new retention obligations.

Transactional acknowledgement can compose atomically with writes in the same PostgreSQL transaction. **It does not by itself create exactly-once delivery to a remote system.** Remote consumers still need an idempotent destination or a protocol suited to their own durability boundary. `NOTIFY` can wake a consumer; the durable log remains the source of truth.

### 7.2 Keep adapter interfaces narrow before stabilizing an SDK

Define the source adapter contract internally first. It should express what the core needs: snapshot-plus-change continuity, transaction completeness, typed position comparison, replay identity, schema generations, and recovery. An adapter must explain how it proves those properties; it cannot return a timestamp and label the source current.

For pre-1.0, prefer documented SQL contracts and an independent conformance fixture over a stable in-process Rust ABI. PostgreSQL and pgrx internals evolve, and an ABI freeze would create a much larger maintenance commitment than most users need.

Require two independent uses of the public contract before calling it an SDK: a minimal test coordinator and a real integration such as pg_tide or dbt where appropriate. “Independent” means the consumer uses the public boundary, not internal tables through a test-only shortcut.

### 7.3 Keep these ambitions off the pre-1.0 critical path

- Native lakehouse sinks and object-storage state.
- Distributed delta execution, cross-cluster graphs, and a Kubernetes operator.
- Automatic rewriting of arbitrary application queries to use stream tables.
- Embedding generation, model orchestration, vector-index management, and generic search workflows.
- General-purpose event delivery and workflow orchestration.
- A new dataframe/runtime stack without a demonstrated core workload benefit.
- Broad PostgreSQL major-version expansion before PostgreSQL 18 behavior is qualified.

These can be useful ecosystem projects. The current roadmap already defers major distributed and automatic-query-acceleration work, and pg_tide extraction is a good precedent. Correct existing integration bugs, preserve explicit compatibility, and avoid increasing the core release burden. [Roadmap][roadmap], [dependency policy][dependencies].

## 8. A world-class performance and correctness scorecard

“World class” needs a workload and a reproducible result. The scorecard should report supported strategies and their resource cost, rather than one headline speedup.

### 8.1 Retain current evidence, strengthen its interpretation

The checked-in v0.88 comparison reports approximately **9.23× throughput** relative to v0.87.17 on its frozen aggregate workload. That is evidence for that specific improvement, not a universal speedup. The v0.89 negative window result is similarly specific. [Aggregate comparison artifact][aggregate-comparison], [benchmark guide][benchmark].

The v0.87 product budgets require at least 99% installed-without-stream-table TPS, but allow up to 2× active p99 latency and 50% refresh CPU share. Those are explicit useful floors; they are too permissive to support a broad claim that active IVM is invisible to OLTP. [Current budgets][budgets].

**Proposed goals**, to be calibrated on designated reference hardware and published workloads:

| Dimension | Proposed qualification rule |
|---|---|
| Exact results | Zero unexplained multiset or schema deviations on the declared exact support surface. |
| Capture/recovery | Every committed source transaction is accounted for across the documented fault matrix; replay does not change the final multiset. |
| No-stream overhead | Preserve the existing ≥99% TPS floor, with a prespecified paired-run decision rule. |
| Active OLTP coexistence | Target ≤10–15% p99 latency increase for a named low-delta workload at a fixed offered load; publish the throughput/freshness tradeoff. This is a proposed goal, not a current result. |
| New optimization admission | Require a material measured end-to-end win, for example ≥20% on its target workload, with no unexplained >10% regression on the declared neighboring workload set. |
| Freshness | Meet declared achievable targets under the reference load; report infeasibility and measurement gaps truthfully. |
| State growth | After the workload reaches steady state, retained-state growth follows a documented bound or retention obligation. |
| Graph overhead | Report both refresh work and coordinator-transaction lock duration; tiny upstream changes must not silently turn the graph into full recomputation. |
| Packaging and recovery | Maintained data, pending deltas, permissions, and supported upgrade/recreation paths work in every supported artifact. |

Do not impose one latency ratio on all queries: an expensive high-fanout view has a different economic profile from a small projection. Publish categories so users can choose an appropriate tradeoff.

### 8.2 Workload matrix

| Workload family | Important dimensions |
|---|---|
| Projection/filter | Wide rows, TOAST, irrelevant-column updates, key changes, and zero-change refresh. |
| Aggregation | Low/high group cardinality, skew, NULL groups, DISTINCT, extremum deletion, numeric adversaries. |
| Joins | One-to-one, one-to-many, many-to-many, outer/semi/anti, both sides changed, hot dimension updates. |
| Windows/TopK | Tail/front insert, late rows, peers, deletes, partition size, deterministic ordering. |
| Graphs | Chain, diamond, shared upstream, mixed FULL/scoped/differential, 10/100/1,000 members where supported. |
| CDC | Statement/row trigger and WAL paths, many tracked sources, large source transactions, unrelated WAL traffic. |
| Concurrency | Multiple writers, long transactions, lock contention, DDL, refresh cancellation, graph coordinators. |
| Operations | Slow consumers, rebuild, upgrade, restart, restore, resource pressure, and recovery overlap. |

Use small, medium, and large data scales appropriate to the hardware; include approximately 0.1%, 1%, 10%, and burst/high-change cases. Report actual changed output size so high amplification is not mistaken for algorithmic failure.

TPC-H and Nexmark are useful parts of the suite. Add representative application schemas and workload traces; neither analytical query coverage nor a small synthetic aggregate alone describes an application database.

### 8.3 Measurement discipline

Record the exact commit and artifact, PostgreSQL version, hardware, CPU/memory limits, durability settings, indexes, data distributions, offered write load, achieved throughput, warm-up, and all repetitions. Compare identical result semantics and freshness settings. Separate compilation/planning from warm execution while reporting both.

Use enough latency observations to interpret p99; a handful of refresh repetitions cannot characterize foreground tail behavior. Report uncertainty and all trials. Keep correctness checks active, assert effective strategy, and distinguish unsupported cases from infrastructure failures and expected fallback. A benchmark that becomes fast by doing no work or silently switching semantics must fail.

## 9. Relationship to the existing 0.95–0.98 roadmap

The next roadmap should build on the current one:

| Existing release | Keep | Clarify or strengthen |
|---|---|---|
| **0.95.0 — typed output deltas** | Exact batches/invalidation, transactional acknowledgement, retention, resnapshot, independent capability gate. | Close Graph V1 and WAL hazards first; prove disabled overhead and consumer conformance. |
| **0.96.0 — defaults, bounds, diagnosis** | Resource-derived choices, hard/throttled/forecast classes, progress, errors, role mapping. | Ensure manual, scheduled, graph, and rebuild paths obey the same policy; include caller-transaction lock costs. |
| **0.97.0 — assurance and packaging** | Monitoring, 72-hour soak, real binary upgrades, reproducible artifacts. | Make the packaged candidate and retained evidence authoritative; test actual stream maintenance in packages. |
| **0.98.x — stabilization** | Fixes, narrowed unsafe optimizations, compatibility, documented limitations. | Treat it as an interim qualified baseline if the extended IVM programme is adopted. |

[0.95][r095], [0.96][r096], [0.97][r097], [0.98][r098].

The policy change should be explicit: **0.97 ceases to be the final feature release; 0.98 remains a stabilization series; the final feature freeze moves to 0.104, followed by the v0.105.0–v0.105.2 qualification series. v1.0 qualification is deferred indefinitely.** Update roadmap prose, release automation, feature-freeze checks, and upgrade manifests together. Do not leave incompatible freeze statements in different documents.

Urgent correctness, security, and misleading-contract fixes should ship at the earliest appropriate release. Their inclusion in later acceptance criteria is a regression requirement, not permission to defer them.

## 10. Proposed releases beyond 0.98

Version numbers are semantic-version components: `0.100.0` follows `0.99.0`; they are not decimal fractions. The sequence below is deliberately finite. Sizes are relative engineering scope, not calendar commitments; team capacity and measured defect depth are unknown.

| Release | Theme and user outcome | Size | Principal dependency |
|---|---|---|---|
| **0.99.0** | A verified support contract: users can trust what the product says it supports. | Medium | Qualified 0.98 baseline; urgent hazards resolved or the affected feature explicitly unavailable. |
| **0.100.0** | One transactional refresh implementation: composition behaves consistently across entry points. | Large | 0.99 capability and behavioral baseline. |
| **0.101.0** | Exact relational state: common set and distinct workloads become safely incremental. | Large | Unified state/finalization protocol. |
| **0.102.0** | Efficient delta plans: small changes touch less irrelevant data. | Large | Exact oracle, measured state costs, stable executor boundary. |
| **0.103.0** | Predictable OLTP coexistence: freshness and throughput fit the application’s resource budget. | Large | Reliable capture and execution instrumentation. |
| **0.104.0** | Proven extension contracts and final feature freeze. | Medium | Graph/Delta conformance and preceding core behavior settled. |
| **0.105.0** | Qualification contract and release evidence. | Medium | Frozen 0.104 feature surface. |
| **0.105.1** | Runtime conformance and recovery qualification. | Large | v0.105.0 qualification contract. |
| **0.105.2** | Package, upgrade, performance, and field validation. | Large | v0.105.1 runtime evidence. |

### 0.99.0 — Verified capabilities and product truth

**User promise:** “I can determine whether my query will work, how it will be maintained, and which limitations apply.”

Deliver a machine-readable capability/strategy manifest backed by executable examples. Reconcile README, limitations, SQL reference, generated catalogs, configuration, and integration documentation. Explain requested versus actual mode, scoped versus whole-query recomputation, and stable reason identifiers through the existing diagnostic surface wherever possible.

Replace nominal graph tests with actual conformance and establish a candidate evidence manifest. Carry regression tests for F01–F05; if any affected feature remains unproven, make its admission explicitly unavailable or experimental with a documented compatibility decision.

**Exit criteria:**

- Every public support claim maps to a runnable example and expected strategy/outcome.
- No known P0 correctness investigation remains unresolved for an enabled stable feature.
- Graph policy and rollback tests execute the public API against PostgreSQL.
- Documentation/catalog checks pass without broad exemptions for active unsupported interfaces.
- The release evidence manifest distinguishes executed, skipped, unavailable, and historical results.

**Scope control:** no new SQL coverage or integrations. This release establishes the baseline against which later improvements are judged.

### 0.100.0 — Unified transactional IVM execution

**User promise:** “A graph remains incremental when I refresh it manually or through a coordinator.”

Consolidate prepare, owner-execute, and finalize behavior across scheduled, manual, external graph, and lifecycle paths, while preserving the different consistency contract of IMMEDIATE mode. Introduce a scoped refresh context rather than adding more ambient flags. It should carry permitted strategies, proven boundaries, cancellation/resource policy, and downstream/output obligations.

Remove the blanket manual FULL fallback for stream-table sources by producing the required downstream evidence in the common path. Enforce actual FULL policy at strategy execution. Make exceptional exits restore backend-local state and leave no partial committed graph.

**Exit criteria:**

- Chain and diamond workloads with small changes stay differential/scoped where admitted.
- Entry-point parity covers result multisets, effective strategy, frontier, downstream change propagation, and rollback.
- `full_policy = 'ERROR'` rejects every whole-query FULL transition, including runtime fallback, without forcing unsafe incremental execution.
- No-data, initial population, reinitialization, and function/schema change paths finalize consistently.
- Lock acquisition/hold costs and caller-transaction obligations are documented and measured.

**Scope control:** preserve PostgreSQL as the executor. This is consolidation of the existing engine, not a second scheduler or dataflow runtime.

### 0.101.0 — Exact relational state and semantic depth

**User promise:** “More ordinary SQL workloads remain incremental through duplicates, deletions, and repeated changes.”

Implement durable private multiplicity state for the four `INTERSECT`/`EXCEPT` forms and a narrowly admitted DISTINCT-aggregate state path. Provide a reusable private-state lifecycle covering schema/identity version, ownership, readiness, rebuild, cleanup, and restore. Qualify statistical aggregate arithmetic with the adversarial cases in F08.

Choose at most a small number of additional identity families needed by real workloads. JSONB and arrays are candidates; neither is required merely to expand the registry. Their inclusion depends on a complete equality and resource contract.

**Exit criteria:**

- Mutation-by-mutation multiset parity for all newly admitted forms, including both branches changing in one transaction.
- NULL, duplicate, last-occurrence, empty-group, overflow, and identity-generation transitions covered.
- Restart, clone, upgrade, and failed-refresh tests preserve or safely invalidate private state.
- Numeric behavior is documented per overload and adversarial tests meet that contract.
- State paths beat or justify their cost relative to rescanning on their declared workload; expensive cases retain explicit alternatives.

**Scope control:** no arbitrary user-defined incremental operator API. Do not unblock a form merely because the SQL differentiator compiles.

### 0.102.0 — Output-sensitive delta performance

**User promise:** “Small relevant changes avoid unnecessary scans, and I can see why a refresh is expensive.”

Extend the v0.88 planning work with bounded per-operator measurements, affected-key/group locality, better delta statistics, and individually admitted rewrite rules. Improve join/aggregate compositions and evaluate narrow extensions to typed aggregate pages, such as proven filters or projections that currently disqualify a workload.

Cost selection should consider affected regions, fanout, emitted rows, state writes, planning overhead, and PostgreSQL apply cost. Preserve safe current plans when statistics are missing. Publish SQL and the PostgreSQL plan used to execute it.

**Exit criteria:**

- Each enabled optimization has exact-result, schema, and snapshot parity tests plus retained benchmark evidence.
- Target improvements meet a prespecified material-win threshold and neighboring workloads meet the regression budget.
- Plan/cache invalidation covers relevant schema, identity, function, and statistical changes.
- High-fanout workloads expose unavoidable output cost rather than an unexplained O(Δ) promise.
- Optional window/TopK candidates remain disabled unless their complete lifecycle and end-to-end cost win.

**Scope control:** no independent physical join optimizer; no required Arrow/SIMD runtime. A losing optimization is removed or left experimental.

### 0.103.0 — Low-interference capture and workload control

**User promise:** “IVM stays within a visible database budget while maintaining an achievable freshness target.”

Improve many-source capture economics after F01 is closed, evaluate shared typed decoding, and reduce source write-conflicting lock duration only where a replacement boundary proof is complete. Apply resource admission and cancellation consistently across graph, manual, scheduled, and rebuild work.

Graduate selected freshness-controller actions from advisory behavior independently. Add workload fairness and backlog-age protections so a hot view, rebuild, or stalled consumer cannot silently consume the whole refresh budget. Reuse the 0.96 resource contract rather than create a competing profile system.

**Exit criteria:**

- Paired foreground-write benchmarks meet the named workload budget with all attempts retained.
- Many-source WAL workloads show bounded decoding amplification relative to the declared design.
- Late commits, giant transactions, decoder lag, and concurrent source DDL preserve capture completeness.
- Pressure yields a documented action: throttling, reduced admission, freshness breach, or suspension; committed changes remain accounted for.
- Every automatic controller action has evidence, limits, hysteresis, override behavior, and a reason.

**Scope control:** no universal low-latency promise under overload. If a nonblocking capture/boundary optimization cannot be proven, retain the conservative path and its published limits.

### 0.104.0 — Extension conformance and final feature freeze

**User promise:** “Other PostgreSQL tools can build on pg-trickle through a small, stable contract.”

Package the Graph V1 and Delta V1 conformance suites and a minimal reference coordinator/consumer. Freeze capability versions, schema/identity interpretation rules, errors, retention, acknowledgement, invalidation, and recovery behavior. Ensure consumers survive upgrades by negotiating supported versions and resnapshotting when required.

Clarify the supported boundary with pg_tide, dbt, and optional source integrations. Keep infrastructure-specific protocols outside the core executor. Complete compatibility documentation and remove obsolete public knobs where the pre-1.0 migration policy permits it.

**Exit criteria:**

- Two independent consumers/coordinators use public contracts without private catalog/buffer dependencies.
- Rollback, replay, invalidation, slow-consumer pressure, schema change, restore, and clone conformance pass.
- Output-consumer-disabled overhead remains within its explicit budget.
- Owner permissions include retained deleted data and lifecycle transitions.
- API, GUC, metrics, errors, supported packages, and upgrade policy are frozen for the final qualification series.

**Scope control:** no connector marketplace, stable Rust ABI, or remote delivery service. Finish the boundary already designed in 0.93–0.95.

### 0.105.0–0.105.2 — Qualification and maintenance

**User promise:** “The documented build has passed the documented release conditions.”

Allow correctness and security fixes, compatibility repair, documentation,
package fixes, missing tests, and removal or narrowing of unstable
optimizations. Add no SQL features or capabilities.

Use [v0.105.0](../roadmap/v0.105.0.md) for the qualification contract and
candidate-bound release evidence. Use [v0.105.1](../roadmap/v0.105.1.md) for
runtime conformance, the exact oracle, the capture fault matrix, recovery, and
upgrade behavior. Use [v0.105.2](../roadmap/v0.105.2.md) for package,
performance, and field validation.

Do not run the 72-hour mixed-workload soak or the longer-running longevity
environment in this series. Keep both as future v1.0 qualification gates.
Have independent operators exercise the install, diagnose, alter, upgrade,
restore, and resnapshot runbooks during v0.105.2.

**Exit criteria:**

- No known correctness or security blocker in the supported surface.
- All required results map to the candidate SHA and shipped artifact digests.
- Upgrade support manifest includes the accepted post-0.98 path and explicitly classifies recreation boundaries.
- No unexplained growth appears in the measured qualification workloads.
- Supported-platform package tests maintain real data and exercise the advertised upgrade procedure.
- All remaining limitations have a stable diagnostic and a documented supported response.

Do not publish `1.0.0-rc.1` or `1.0.0` from this series. Both remain
indefinitely deferred. A material change to capture, finalization, or recovery
invalidates the corresponding qualification evidence and requires rerunning it.

## 11. Programme priorities and scope discipline

The sequence must not become another open-ended “final hardening” arc. Use these rules:

1. **Fix supported-surface correctness hazards immediately.** Do not reserve them for a future milestone while expanding capabilities.
2. **Separate required reliability from optional speed.** A correct recomputation path can ship; an unsafe optimization cannot.
3. **Require a concrete workload for every performance feature.** State its current cost, proposed mechanism, acceptance threshold, and removal condition.
4. **Give every new state family a recovery and cleanup owner.** No persistent state is complete until it can be restored, invalidated, rebuilt, and reclaimed.
5. **Allocate most engineering effort to the core.** A reasonable planning target is roughly 80% for IVM/correctness/performance/operations and at most 20% for public integration boundaries and examples. This is a proposed allocation, not a claim about current staffing.
6. **Do not add releases to justify existing plans.** If a proposed milestone is already satisfied, record the evidence and move on. If an optional feature misses its gate, defer it beyond 1.0.

The first concrete work packages are v0.105.0's qualification contract,
v0.105.1's live conformance and recovery evidence, and v0.105.2's package and
field validation. The 72-hour soak and longevity environment are deliberately
deferred until v1.0 qualification resumes.

## 12. Suggested investigation and acceptance scenarios

These are proposed tests, not claims of completed reproduction.

| Scenario | Required observation |
|---|---|
| WAL receipt fails after decoded rows are returned | No committed source event disappears; retry or explicit suspension accounts for the batch. |
| Receipt commits, acknowledgement is interrupted | Replay is harmless and duplicate logical changes are not applied twice. |
| One WAL source succeeds, a later source fails in the polling transaction | Receipt and frontier state remain coherent for every source. |
| Strict chain, all members differential, tiny base change | Downstream work remains incremental/scoped; actual strategy is reported. |
| Strict graph with `full_policy = 'ERROR'`, runtime fallback triggered | Whole graph aborts; no FULL is silently accepted. |
| Graph error caught by an outer savepoint, then ordinary refresh | No graph-bound state leaks into the subsequent refresh. |
| Mixed FULL/differential graph with one lagging WAL source | Every member matches the declared coherent boundary, or the graph refuses to run. |
| Source writer waits while coordinator retains the graph transaction | Lock behavior is measured and documented; timeout/cancellation behavior is deterministic. |
| DISTINCT/set result changes from two copies to one, then zero | Correct multiplicity transitions and cleanup across all modes. |
| Statistical aggregate with large common offset and small variance | Matches the documented numerical contract, including repeated retractions. |
| Output consumer stalls, resumes, or requires resnapshot | Retention and hard-pressure behavior preserve all required history or record explicit invalidation. |
| Restore/clone with existing consumer and capture metadata | New instance identity prevents stale cursor/boundary reuse. |
| Upgrade with pending CDC and maintained output | Actual old binary and new package follow the declared in-place or recreation path. |

## 13. Definition of readiness for 1.0

pg-trickle is ready for 1.0 when a database owner can answer these questions accurately from the product:

- Is this query supported, and under which semantic and type restrictions?
- What will actually be recomputed after my writes?
- Which committed source changes are represented in this result?
- What freshness can this workload achieve at its current resource budget?
- How much do capture and maintenance affect application write latency?
- What happens when maintenance fails, history is unavailable, or disk pressure rises?
- Can I alter, upgrade, restore, and recover the state using a proven procedure?
- Can another tool coordinate or consume it without interpreting private implementation details?

The architecture can support that product. The strongest path is to make these answers provable and consistent, then improve the amount of useful incremental work performed per unit of database cost.

## 14. Source index

Repository links below are pinned to the reviewed commit. External PostgreSQL documentation is version 18. Benchmark numbers are repository-reported unless explicitly described as an assessment check.

| Evidence area | Principal references |
|---|---|
| Current baseline | [v0.94.0 release][release094], [roadmap][roadmap], [CI run][ci-run] |
| Capture and durability | [WAL implementation][wal], [source frontiers][cdc], [PostgreSQL 18 decoding source][pg-logical-source] |
| Refresh and composition | [manual refresh][manual], [strict graph implementation][graph], [common apply paths][merge] |
| SQL semantics | [admission][admission], [aggregate strategies][types], [exact identity][rowid-doc], [snapshot plans][snapshot] |
| Behavioral evidence | [exact oracle][oracle], [graph gate][gate094], [smoke tests][smoke], [DVM CI tiers][dvm-tiers] |
| Performance | [benchmark guide][benchmark], [aggregate result][aggregate-comparison], [window result][window-bench], [OLTP budgets][budgets] |
| Product/release truth | [docs checker][docs-check], [support matrix][support], [release workflow][release-workflow], [upgrade manifest][upgrade-manifest] |
| Extension direction | [Graph/Delta proposal][proposal], [0.95 output-delta plan][r095], [dependency policy][dependencies] |

[architecture]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/ARCHITECTURE.md
[adr009]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/plans/adrs/ADR-009.md
[adr010]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/plans/adrs/ADR-010.md
[window-bench]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/benchmarks/window-v0.89/README.md
[roadmap]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/ROADMAP.md
[r091]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/roadmap/v0.91.0.md
[r092]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/roadmap/v0.92.0.md
[r094]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/roadmap/v0.94.0.md
[r095]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/roadmap/v0.95.0.md
[r096]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/roadmap/v0.96.0.md
[r097]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/roadmap/v0.97.0.md
[r098]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/roadmap/v0.98.x.md
[ci]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/.github/workflows/ci.yml
[gate094]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/scripts/v0_94_release_gate.py
[diff]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/diff.rs
[merge]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/refresh/merge/mod.rs
[admission]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/parser/validation.rs
[rowid-doc]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/ROW_IDENTITY_V2.md
[rowid-code]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/row_id_v2.rs
[oracle]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/tests/e2e/oracle.rs
[aggregate-bench]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/benchmarks/vector-aggregate-v0.88/README.md
[wal]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/wal_decoder.rs
[scheduler-loop]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/scheduler/scheduler_loop.rs
[citus]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/citus.rs
[manual]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/api/refresh_ops.rs
[graph]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/api/integration.rs
[proposal]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/plans/PROPOSAL_V1_COMPOSABLE_REFRESH_AND_DELTA_CONTRACTS.md
[smoke]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/tests/e2e_smoke_tests.rs
[cdc]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/cdc/mod.rs
[readme]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/README.md
[limitations]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/LIMITATIONS.md
[support]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/DVM_SUPPORT_MATRIX.md
[types]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/parser/types.rs
[recursive]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/operators/recursive_cte.rs
[dependencies]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/DEPENDENCIES.md
[docs-check]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/scripts/check_docs_truth.py
[docs-allow]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/scripts/check_docs_truth_allowlist.yml
[api-catalog]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/SQL_API_CATALOG.md
[dvm-tiers]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/.github/workflows/dvm-tiers.yml
[release-workflow]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/.github/workflows/release.yml
[aggregate]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/operators/aggregate.rs
[planner]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/planner.rs
[pipeline]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/refresh/pipeline.rs
[controller]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/scheduler/controller.rs
[diagnostics]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/api/diagnostics.rs
[recovery]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/api/recovery.rs
[upgrading]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/UPGRADING.md
[upgrade-manifest]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/upgrade-support-manifest.json
[security-context]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/api/security_context.rs
[security]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/SECURITY_MODEL.md
[schema]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/schema.rs
[snapshot]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/dvm/snapshot.rs
[sql-builder]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/src/sql_builder.rs
[aggregate-comparison]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/benchmarks/vector-aggregate-v0.88/comparison.json
[benchmark]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/docs/BENCHMARK.md
[budgets]: https://github.com/trickle-labs/pg-trickle/blob/ef6d75a60a431b407bccc623b6dbea4390589902/benchmarks/pgbench-v0.87/budgets.json
[release094]: https://github.com/trickle-labs/pg-trickle/releases/tag/v0.94.0
[ci-run]: https://github.com/trickle-labs/pg-trickle/actions/runs/34129028946
[pg-functions]: https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-REPLICATION
[pg-logical-source]: https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/logicalfuncs.c
[pg-logical]: https://www.postgresql.org/docs/18/logicaldecoding-explanation.html
[pg-locks]: https://www.postgresql.org/docs/18/explicit-locking.html
