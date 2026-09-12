# PLAN: Additional Algorithms for pg_trickle Optimization

> **Date:** 2026-04-19
> **Status:** Research
> **Related:** [PLAN_ALGORITHMS_LEAPFROG_TRIEJOIN.md](PLAN_ALGORITHMS_LEAPFROG_TRIEJOIN.md) (WCOJ algorithms),
> [PLAN_DVM_IMPROVEMENTS.md](../performance/PLAN_DVM_IMPROVEMENTS.md),
> [PLAN_TPCH_DVM_PERF.md](../performance/PLAN_TPCH_DVM_PERF.md),
> [PLAN_NEW_STUFF.md](../performance/PLAN_NEW_STUFF.md),
> [GAP_ANALYSIS_FELDERA.md](../ecosystem/GAP_ANALYSIS_FELDERA.md)
> **Scope:** Comprehensive survey of algorithms beyond WCOJ that can improve
> pg_trickle's performance, scalability, and efficiency — covering
> higher-order IVM, aggregate-aware processing, multi-query optimization,
> scheduling, data structures, and parallelism.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Higher-Order & Ring-Based IVM](#2-higher-order--ring-based-ivm)
3. [Aggregate-Aware Join Processing](#3-aggregate-aware-join-processing)
4. [Demand-Driven Query Rewriting](#4-demand-driven-query-rewriting)
5. [Dynamic Query Evaluation](#5-dynamic-query-evaluation)
6. [Multi-Query Optimization](#6-multi-query-optimization)
7. [Scheduling & Cascade Optimization](#7-scheduling--cascade-optimization)
8. [Data Structure Optimizations](#8-data-structure-optimizations)
9. [Parallelism & Distribution](#9-parallelism--distribution)
10. [Summary & Priority Matrix](#10-summary--priority-matrix)
11. [References](#11-references)

---

## 1. Overview

The companion document
[PLAN_ALGORITHMS_LEAPFROG_TRIEJOIN.md](PLAN_ALGORITHMS_LEAPFROG_TRIEJOIN.md)
covers worst-case optimal join (WCOJ) algorithms: Leapfrog Triejoin, NPRR,
Generic Join, Free Join, Yannakakis, Minesweeper, Tetris, Factorized
Representations, and Delta-WCOJ. Those algorithms target the multi-way join
bottleneck specifically.

This document surveys **all other** algorithmic areas that can contribute to
pg_trickle's goals of maximum performance, low latency, and high throughput.
The algorithms are organized by category and evaluated for applicability to
pg_trickle's architecture: a PostgreSQL extension that **generates delta SQL**
for PostgreSQL to execute — it does not implement its own query executor.

### Key Constraints

Any algorithm considered must satisfy at least one of:

1. **Expressible in generated SQL** — can be encoded as CTEs, subqueries,
   or query rewrites that PostgreSQL's planner/executor handles.
2. **Implementable in Rust-side logic** — affects how `OpTree` is
   constructed, how delta SQL is generated, or how refreshes are scheduled.
3. **Applicable to PostgreSQL-level tuning** — controls GUCs, index
   creation, or planner hints that improve delta SQL execution.

Algorithms requiring a custom executor or custom data structures internal to
the query engine are noted as "theoretical interest" and deprioritized.

---

## 2. Higher-Order & Ring-Based IVM

### 2.1 Higher-Order Delta Processing (DBToaster)

**Koch, C., Ahmad, Y., Kennedy, O., et al. (2014).** "DBToaster:
Higher-order Delta Processing for Dynamic, Frequently Fresh Views."
*VLDB Journal*, 23(2).

**Core idea:** Instead of computing first-order deltas
$\Delta Q = Q(D \cup \Delta D) - Q(D)$ directly, recursively differentiate
the query to produce **higher-order delta triggers** that are progressively
simpler. For a query $Q$ over $k$ tables, the $k$-th order delta is a
constant (depends only on $\Delta D$, not on $D$ itself). Intermediate
auxiliary views (materialized sub-deltas) are maintained to make each
order's computation efficient.

**Complexity:** For a join of $k$ tables, first-order IVM may require
$O(|D|^{k-1})$ work per update. Higher-order IVM reduces this to
$O(|D|^{k-2})$ per update at the cost of maintaining $O(k)$ auxiliary
views. Fully recursive (order-$k$) IVM achieves $O(1)$ per update for
many queries but requires $O(k^2)$ auxiliary views.

**Relevance to pg_trickle:** **High.** pg_trickle currently implements
first-order IVM — each refresh computes the delta by joining $\Delta R$
against the full current state of all other tables. For deep joins
(TPC-H Q05, Q07, Q08, Q09 with 6 tables), this produces $O(|D|^5)$
intermediate work per delta source.

Second-order IVM would maintain auxiliary views for frequently-accessed
sub-joins. When table $A$ changes, instead of recomputing
$\Delta A \bowtie B \bowtie C \bowtie D \bowtie E \bowtie F$ from
scratch, a pre-materialized view $V_{BCDEF} = B \bowtie C \bowtie D
\bowtie E \bowtie F$ would reduce the delta to $\Delta A \bowtie
V_{BCDEF}$.

**SQL encoding:** Each auxiliary view can be a **stream table itself** —
pg_trickle already supports ST-to-ST dependencies. The challenge is
**automatically decomposing** a complex query into a DAG of simpler
stream tables that form the higher-order delta hierarchy.

**Implementation approach:**

1. In `OpTree` analysis, detect joins with $k \geq 4$ tables.
2. Identify high-selectivity sub-joins that benefit from materialization.
3. Automatically create intermediate stream tables representing the
   auxiliary views.
4. Wire the original stream table to depend on the intermediates instead
   of the base tables directly.

**Trade-offs:**
- **Pro:** Dramatic reduction in per-update work for deep joins.
- **Con:** More storage for auxiliary views; more stream tables to manage;
  cascade latency increases (more DAG levels).
- **Mitigation:** Only decompose when $k \geq 4$ and the cost model
  estimates benefit. Provide a `pgtrickle.auto_decompose` GUC to control.

### 2.2 F-IVM (Factorized IVM)

**Nikolic, M. & Olteanu, D. (2018).** "Incremental View Maintenance with
Triple Lock Factorisation Benefits." *SIGMOD 2018.*

**Nikolic, M., Zhang, H., Kara, A., & Olteanu, D. (2020).** "F-IVM:
Learning over Fast Evolving Relational Data." *SIGMOD Demo 2020.*

**Core idea:** F-IVM unifies IVM under an algebraic **ring** abstraction.
The query result is a function from keys to **payloads**, where payloads
are elements of a commutative ring $(R, +, \times, 0, 1)$. Different
choices of ring yield different applications:

| Ring | + | × | Application |
|------|---|---|-------------|
| $(\mathbb{Z}, +, \times)$ | addition | multiplication | Count/Sum aggregates |
| $(2^T, \cup, \bowtie)$ | union | join | Factorized query results |
| $(\mathbb{R}^{d \times d}, +, \times)$ | matrix add | matrix multiply | Gradient computation |

F-IVM combines higher-order IVM with factorized computation:
1. **Higher-order:** Reduces maintenance to a tree of simpler views.
2. **Factorized updates:** Processes bulk updates as low-rank
   decompositions.
3. **Factorized results:** Maintains compressed query results.

**Performance:** Outperforms DBToaster by up to 4 orders of magnitude
and classical first-order IVM by up to 6 orders of magnitude on
join-aggregate queries.

**Relevance to pg_trickle:** **Medium-High.** The ring abstraction is
elegant but pg_trickle operates over PostgreSQL's type system, not
custom algebraic structures. However, two specific ideas are directly
applicable:

1. **Aggregate pushdown past joins:** F-IVM's key optimization is
   computing aggregates *during* the join rather than *after*. For
   queries like `SELECT region, SUM(amount) FROM orders JOIN lineitem
   ... GROUP BY region`, the current approach joins first (producing
   a large intermediate) and then aggregates. F-IVM computes partial
   aggregates at each join step.

2. **Factorized bulk updates:** When multiple rows change in the same
   group, F-IVM processes them as a single aggregated delta rather than
   row-by-row. This directly applies to pg_trickle's change buffer
   processing.

**SQL encoding:**

```sql
-- Current: join first, aggregate later
WITH delta_join AS (
  SELECT ... FROM delta_orders o JOIN lineitem l ON ...
)
SELECT region, SUM(amount) FROM delta_join GROUP BY region;

-- F-IVM style: aggregate during join
WITH delta_agg AS (
  SELECT o.region, SUM(o.amount * l.quantity) AS partial_sum
  FROM delta_orders o
  JOIN lineitem l ON o.o_orderkey = l.l_orderkey
  GROUP BY o.region, o.o_orderkey  -- partial group-by
)
SELECT region, SUM(partial_sum) FROM delta_agg GROUP BY region;
```

### 2.3 IVM_ε (Adaptive IVM)

**Kara, A., Ngo, H.Q., Nikolic, M., Olteanu, D., & Zhang, H. (2019).**
"Counting Triangles under Updates in Worst-Case Optimal Time." *ICDT 2019.*
(Best Paper Award)

**Kara, A., Nikolic, M., Olteanu, D., & Zhang, H. (2020).** "Trade-offs
in Static and Dynamic Evaluation of Hierarchical Queries." *PODS 2020.*

**Core idea:** IVM_ε defines a **continuum** of maintenance strategies
parameterized by $\epsilon \in [0,1]$ that trades off space vs. time:

- Space: $O(|D|^{1 + \min(\epsilon, 1-\epsilon)})$
- Amortized update time: $O(|D|^{\max(\epsilon, 1-\epsilon)})$

For the triangle count query at $\epsilon = 0.5$: $O(\sqrt{|D|})$
amortized update time, which is provably worst-case optimal under the
OMv conjecture.

The key technique is **degree-based partitioning**: data values are
classified as "heavy" (high degree/frequency) or "light" (low degree)
with a threshold $\tau = |D|^\epsilon$. Heavy values use pre-computed
indexes; light values use on-the-fly computation.

**Relevance to pg_trickle:** **Medium.** The degree-partitioning idea
can be applied to pg_trickle's delta SQL generation:

- **Heavy keys** (high fan-out join keys) should use pre-filtered CTEs
  with existence checks before the full join.
- **Light keys** (low fan-out) can use direct hash joins without
  pre-filtering.

This is a refinement of the Yannakakis semi-join reduction planned in
Phase 1 of the WCOJ plan. Instead of uniformly applying semi-join
reduction to all keys, partition the approach by estimated key frequency.

**SQL encoding:**

```sql
-- Partition delta keys by estimated degree
WITH delta_keys AS (
  SELECT join_key, COUNT(*) AS degree
  FROM delta_table GROUP BY join_key
),
-- Heavy keys: use semi-join reduction (EXISTS chain)
heavy_delta AS (
  SELECT d.* FROM delta_table d
  JOIN delta_keys dk ON d.join_key = dk.join_key
  WHERE dk.degree > :threshold
),
-- Light keys: direct hash join (PG optimizer handles well)
light_delta AS (
  SELECT d.* FROM delta_table d
  JOIN delta_keys dk ON d.join_key = dk.join_key
  WHERE dk.degree <= :threshold
)
-- Process each partition with its optimal strategy
...
```

**Implementation approach:** Add a cost-based decision in delta SQL
generation that checks `pg_stats.n_distinct` and `most_common_freqs`
for join key columns. Route high-skew keys through semi-join reduction
and low-skew keys through direct joins.

---

## 3. Aggregate-Aware Join Processing

### 3.1 FAQ (Functional Aggregate Queries)

**Abo Khamis, M., Ngo, H.Q., & Rudra, A. (2016).** "FAQ: Questions
Asked Frequently." *PODS 2016.*

**Core idea:** FAQ provides a unified framework encompassing conjunctive
queries, aggregation, constraint satisfaction, probabilistic inference,
and matrix operations. A FAQ expression is:

$$\varphi(x_f) = \bigoplus_{x_1} \bigoplus_{x_2} \cdots \bigoplus_{x_n} \bigotimes_{S \in \mathcal{E}} \psi_S(\mathbf{x}_S)$$

where $\bigoplus$ is a commutative semiring "sum" (e.g., $+$, $\max$,
$\cup$, OR) and $\bigotimes$ is the semiring "product" (e.g., $\times$,
$+$, $\cap$, AND). The $\psi_S$ are input "factors" (relations,
functions, constraints).

The key insight is that **variable elimination order** determines
computational complexity, and the optimal order depends on the query
hypergraph structure — identical to the tree decomposition used for
join optimization.

**Relevance to pg_trickle:** **High.** Many pg_trickle stream tables
are join-aggregate queries (GROUP BY + SUM/COUNT/AVG over multi-table
joins). The FAQ framework shows that these should be processed by
**interleaving aggregation with joins** rather than the current
join-then-aggregate approach.

For a star schema query:
```sql
SELECT d1.region, d2.category, SUM(f.amount)
FROM fact f
JOIN dim1 d1 ON f.d1_key = d1.key
JOIN dim2 d2 ON f.d2_key = d2.key
GROUP BY d1.region, d2.category
```

The FAQ approach eliminates the fact table's non-group-by columns
**during** the join rather than after, reducing intermediate sizes from
$O(|fact| \times |dim1| \times |dim2|)$ to $O(|groups|)$.

**SQL encoding — eager aggregation:**

```sql
-- Instead of: full join → GROUP BY
-- FAQ-style: partial aggregate → join → final aggregate

WITH fact_partial AS (
  -- Aggregate fact table down to unique (d1_key, d2_key) combos
  SELECT d1_key, d2_key, SUM(amount) AS partial_amount
  FROM delta_fact
  GROUP BY d1_key, d2_key
)
SELECT d1.region, d2.category, SUM(fp.partial_amount)
FROM fact_partial fp
JOIN dim1 d1 ON fp.d1_key = d1.key
JOIN dim2 d2 ON fp.d2_key = d2.key
GROUP BY d1.region, d2.category;
```

This is the **eager aggregation** optimization described in the database
literature (Yan & Larson, 1995). The FAQ framework provides the
theoretical foundation for when this optimization is safe and beneficial.

### 3.2 InsideOut Algorithm

**InsideOut** is the evaluation algorithm for FAQ expressions. It is a
variable-elimination procedure that:

1. Chooses a variable ordering based on the query hypergraph.
2. For each variable, "pushes" the aggregate through all factors that
   mention only eliminated variables.
3. Uses fractional edge covers to bound intermediate sizes.

**Relevance to pg_trickle:** The InsideOut variable ordering can inform
the **CTE generation order** in delta SQL. Currently, pg_trickle
generates CTEs in the order determined by the `OpTree` structure (which
follows the SQL parse tree). InsideOut suggests an alternative ordering
that minimizes intermediate cardinality by aggregating early.

**Implementation approach:** During `OpTree` construction for
join-aggregate queries, compute the FAQ-optimal variable ordering. If
it differs from the parse-tree order and the estimated cost reduction
exceeds a threshold, rewrite the CTE chain accordingly.

### 3.3 LMFAO (Layered Multiple FAQ Optimization)

**Schleich, M., Olteanu, D., & Ciucanu, R. (2019).** "Layered Multiple
Functional Aggregate Optimization." *SIGMOD 2019.*

**Core idea:** LMFAO evaluates **multiple** aggregate queries over the
same join simultaneously by:

1. Computing a single tree decomposition for the join.
2. Layering multiple aggregate functions as separate "layers" over the
   same decomposition.
3. Evaluating all aggregates in a single pass over the data.

**Performance:** On the Favorita retail dataset (5 tables, 125M rows),
LMFAO computes all covariance matrix entries in 3.5 seconds — 200×
faster than PostgreSQL's equivalent SQL.

**Relevance to pg_trickle:** **Medium.** The layered evaluation applies
when a single stream table's defining query computes multiple aggregates
over the same join. More importantly, it applies to the **multi-ST**
case: when several stream tables share the same base tables and join
pattern, their delta computations could share a single join evaluation
with different aggregate layers.

This connects to Multi-Query Optimization (§6) — LMFAO provides the
theoretical basis for shared computation across stream tables.

### 3.4 Eager Aggregation / Aggregate Pushdown

**Yan, W.P. & Larson, P.-Å. (1995).** "Eager Aggregation and Lazy
Aggregation." *VLDB 1995.*

**Core idea:** In a join-aggregate query, aggregation can often be pushed
below the join:

- **Eager aggregation:** Aggregate a table before joining it, reducing
  the number of rows entering the join.
- **Lazy aggregation:** Delay aggregation past a join when the join
  doesn't increase cardinality.

The key condition for safety: the group-by columns must be a superset of
the join keys, or the aggregate function must be decomposable
(SUM, COUNT, MIN, MAX — but not AVG, MEDIAN directly).

**Relevance to pg_trickle:** **Very High.** This is the single most
impactful optimization for aggregate-heavy TPC-H queries (Q01, Q03, Q10,
Q11, Q13, Q14, Q17, Q18). pg_trickle's current delta SQL joins first
and aggregates last, producing large intermediates.

**Implementation approach:**

1. In `OpTree`, detect `Aggregate(Join(...))` patterns.
2. Check the decomposability condition: are all aggregate functions
   decomposable? Are the group-by columns compatible with the join?
3. If safe, rewrite to `Join(Aggregate(Scan), ...)` — push the
   aggregate into the leaf CTEs before the join.

**SQL encoding example (TPC-H Q03 pattern):**

```sql
-- Current: JOIN → aggregate
WITH delta_join AS (
  SELECT c.c_mktsegment, o.o_orderdate, l.l_extendedprice, l.l_discount
  FROM delta_lineitem l
  JOIN orders o ON l.l_orderkey = o.o_orderkey
  JOIN customer c ON o.o_custkey = c.c_custkey
)
SELECT c_mktsegment, o_orderdate,
       SUM(l_extendedprice * (1 - l_discount))
FROM delta_join GROUP BY 1, 2;

-- Eager aggregation: pre-aggregate lineitem per orderkey
WITH delta_by_order AS (
  SELECT l_orderkey,
         SUM(l_extendedprice * (1 - l_discount)) AS revenue
  FROM delta_lineitem
  GROUP BY l_orderkey
),
-- Now join aggregated delta (much fewer rows) against orders/customer
delta_join AS (
  SELECT c.c_mktsegment, o.o_orderdate, d.revenue
  FROM delta_by_order d
  JOIN orders o ON d.l_orderkey = o.o_orderkey
  JOIN customer c ON o.o_custkey = c.c_custkey
)
SELECT c_mktsegment, o_orderdate, SUM(revenue)
FROM delta_join GROUP BY 1, 2;
```

**Expected impact:** For TPC-H Q03 at SF=1.0, lineitem has 6M rows.
Pre-aggregating by `l_orderkey` reduces the delta input from potentially
millions of rows to at most `|Δlineitem distinct orderkeys|` rows (often
100–1000× smaller).

---

## 4. Demand-Driven Query Rewriting

### 4.1 Magic Sets

**Bancilhon, F., Maier, D., Sagiv, Y., & Ullman, J.D. (1986).** "Magic
Sets and Other Strange Ways to Implement Logic Programs." *PODS 1986.*

**Core idea:** Given a query with a binding pattern (e.g., a specific
value is provided for one attribute), the Magic Sets transformation
rewrites the program to propagate demand information top-down through
the query plan. This restricts the computation to only produce tuples
that could contribute to the answer.

For a query like:
```
ancestor(X, 'alice') :- parent(X, 'alice').
ancestor(X, 'alice') :- parent(X, Z), ancestor(Z, 'alice').
```

Magic Sets would add a "magic" predicate `magic_ancestor(Y)` that
propagates the binding `Y = 'alice'` into the recursive computation,
ensuring only ancestors of 'alice' are computed — not all ancestor pairs.

**Relevance to pg_trickle:** **High for recursive CTEs.** pg_trickle's
`WITH RECURSIVE` support uses semi-naive evaluation. When the recursive
CTE is used with a specific filter (e.g., `SELECT * FROM reachable_nodes
WHERE source = 42`), the current approach computes the full transitive
closure and then filters. Magic Sets would push the filter into the
recursion, computing only nodes reachable from 42.

**SQL encoding:**

```sql
-- Current recursive CTE (full computation)
WITH RECURSIVE reach AS (
  SELECT target FROM edges WHERE source = 42
  UNION
  SELECT e.target FROM edges e JOIN reach r ON e.source = r.target
)
SELECT * FROM reach;

-- Magic-Sets-style restricted recursion
-- (restrict the 'edges' scan to only edges reachable from known nodes)
WITH RECURSIVE reach AS (
  SELECT target FROM edges WHERE source = 42
  UNION
  SELECT e.target FROM edges e
  WHERE e.source IN (SELECT target FROM reach)  -- demand restriction
)
SELECT * FROM reach;
```

PostgreSQL already generates similar plans for some recursive CTEs. The
opportunity for pg_trickle is to apply the Magic Sets idea to **delta
computation for recursive stream tables**: when changes arrive, only
recompute the portion of the recursion affected by the changed tuples.

**Implementation approach:**

1. In `diff_recursive_cte()`, analyze whether the recursive CTE has a
   "seed" restriction (WHERE clause on the base case).
2. If so, propagate the restriction into the delta computation, ensuring
   only the affected sub-graph is recomputed.
3. For non-seeded recursive CTEs, use the **delta keys** as the demand
   set: changed edges define which reachable sets need updating.

### 4.2 Sideways Information Passing (SIP)

**Beeri, C. & Ramakrishnan, R. (1991).** "On the Power of Magic."
*JCSS*, 43(3).

**Core idea:** SIP is the generalization of Magic Sets. In a multi-way
join $R_1 \bowtie R_2 \bowtie \cdots \bowtie R_k$, binding information
from early relations can be "passed sideways" to restrict later relations
before they participate in the join.

**Relevance to pg_trickle:** **High — identical to Yannakakis semi-join
reduction** but applied more broadly. SIP can pass bindings through
aggregates, subqueries, and LATERAL joins — not just equi-joins.

For pg_trickle's delta SQL generation, SIP means: when processing
$\Delta R_i$, propagate the delta's key values into **all** downstream
relations in the CTE chain, not just the immediate join partner. This
is what the Yannakakis Phase 1 plan describes, but SIP provides the
theoretical framework for extending it to outer joins, semi-joins, and
aggregates.

### 4.3 Bloom Filter Pre-Filtering

**Bloom, B.H. (1970).** "Space/time trade-offs in hash coding with
allowable errors." *CACM*, 13(7).

**Applied to joins:** Use Bloom filters to pre-filter rows that cannot
participate in a join before the actual join execution.

**Relevance to pg_trickle:** **Medium.** PostgreSQL does not natively
support Bloom filters in join planning (though some extensions exist).
However, the **concept** can be expressed in generated SQL:

```sql
-- Bloom-filter-like pre-filtering using IN/EXISTS
WITH delta_keys AS (
  SELECT DISTINCT join_key FROM delta_table
),
-- Pre-filter the large table to only matching keys
filtered_big_table AS (
  SELECT b.* FROM big_table b
  WHERE b.key IN (SELECT join_key FROM delta_keys)
)
-- Now join against filtered_big_table instead of big_table
SELECT ... FROM delta_table d JOIN filtered_big_table f ON ...
```

This is exactly the semi-join reduction from the WCOJ plan (Yannakakis
Phase 1), but explicitly modeled as a Bloom-filter-like restriction.
The optimization is valuable when the delta is small relative to the
probed table and an index exists on the join key.

**Implementation note:** PostgreSQL 18's planner already performs some
semi-join pushdown optimization. pg_trickle can help by structuring
the generated SQL to make this optimization more accessible to the
planner.

---

## 5. Dynamic Query Evaluation

### 5.1 Insert-Only vs. Insert-Delete in Dynamic Evaluation

**Abo Khamis, M., Kara, A., Olteanu, D., & Suciu, D. (2024).** "Insert-Only
versus Insert-Delete in Dynamic Query Evaluation." *PODS 2024.*

**Core idea:** The paper shows a fundamental separation between the
complexity of maintaining query results under insert-only updates vs.
insert-delete updates:

- **Insert-only:** A sequence of $N$ inserts can be processed in total
  time $O(N^{w(Q)})$ where $w(Q)$ is the fractional hypertree width.
  For acyclic queries, this means $O(1)$ amortized per insert.

- **Insert-delete:** Requires $O(N^{w(Q')})$ where $Q'$ extends $Q$
  with "lifespan" variables. The overhead reflects the need to track
  which tuples are "alive" at any point.

**The lifespan technique:** Each tuple is annotated with its insertion
and deletion timestamps. The join is extended with constraints ensuring
lifespans overlap. This transforms the dynamic problem into a static
problem on a larger query.

**Relevance to pg_trickle:** **High — directly applicable.** pg_trickle
already tracks insert/delete operations via the change buffer's `op`
column. The lifespan insight suggests that for **append-heavy workloads**
(audit logs, event tables, time-series), pg_trickle should use a
specialized insert-only delta path that avoids the overhead of tracking
deletions.

**Implementation approach:**

1. In `catalog.rs`, add an `is_append_only` flag per source dependency.
2. When all sources of a stream table are append-only, use a simplified
   delta SQL path that:
   - Skips `EXCEPT ALL` / pre-change snapshot reconstruction.
   - Skips the `WHERE op = '-'` branches entirely.
   - Uses `INSERT`-only delta application (no `DELETE` phase in MERGE).
3. Detect append-only tables via:
   - Explicit user annotation: `CREATE STREAM TABLE ... WITH (append_only_sources = 'orders')`
   - Automatic detection: if no DELETE/UPDATE triggers have fired for
     $N$ consecutive refreshes, mark as append-only (with fallback).

**Expected impact:** For append-heavy workloads, this eliminates ~40%
of the delta SQL complexity and the entire DELETE phase of MERGE
application. Particularly impactful for log/event analytics.

### 5.2 Batch-Dynamic Processing

**Core idea:** Instead of processing changes one tuple at a time
(as in DBToaster / F-IVM) or one batch at a time (as in pg_trickle),
optimize the batch size to balance latency vs. throughput.

**Relevance to pg_trickle:** pg_trickle already operates in batch mode
(each refresh processes all accumulated changes). The optimization
opportunity is in **batch sizing**: when a source table has a high change
rate, it may be beneficial to process changes in smaller, more frequent
batches rather than one large batch per refresh cycle.

**Why this matters:** At high change rates ($> 10\%$ of table rows
changed), the delta computation approaches the cost of a full refresh.
The per-row cost of delta processing is approximately:

$$C_{\Delta} \approx C_{setup} + |\Delta| \times C_{per\_row} + C_{merge}$$

where $C_{setup}$ is fixed overhead (CTE compilation, snapshot read),
$C_{per\_row}$ includes the join fan-out for each changed row, and
$C_{merge}$ is the MERGE application cost. When $|\Delta|$ is large,
the $|\Delta| \times C_{per\_row}$ term dominates and may exceed
$C_{full}$ (the full refresh cost).

**Implementation approach:**

1. In the scheduler, implement an adaptive batch-sizing algorithm:
   - Track the ratio $C_{\Delta} / |\Delta|$ (amortized per-row cost)
     across recent refreshes.
   - If the ratio exceeds $C_{full} / |D|$ (the per-row cost of full
     refresh), reduce the batch size or switch to FULL mode.
2. This is related to the existing `auto_threshold` mechanism but
   operates at a finer granularity — adjusting refresh frequency
   rather than just switching modes.

### 5.3 Count-Based Maintenance (Z-Set Weights)

**Core idea:** Instead of tracking individual INSERT/DELETE operations,
maintain a **weight** (multiplicity) for each tuple. A tuple with weight
$+1$ exists; weight $0$ means deleted; negative weights represent
pending deletions. This is the Z-set model from DBSP.

**Relevance to pg_trickle:** **Already partially implemented** in the
multi-table delta batching plan (PLAN_MULTI_TABLE_DELTA_BATCHING.md).
The extension opportunities are:

1. **Net-effect computation in change buffers:** Currently, if a row is
   inserted and then updated before a refresh, the change buffer has
   two entries. Z-set consolidation would collapse these to a single
   net change.
2. **Aggregate maintenance with weights:** For `COUNT(*)` and `SUM()`,
   maintaining weights directly avoids the INSERT/DELETE decomposition
   entirely: `new_count = old_count + SUM(delta_weights)`.

**Implementation note:** The change buffer already has an `op` column
('+' / '-'). Converting to numeric weights and using `SUM(weight)
GROUP BY key HAVING SUM(weight) <> 0` for net-effect computation is
the natural extension. The `PLAN_MULTI_TABLE_DELTA_BATCHING.md` plan
covers the intra-query case; the opportunity here is to extend this to
the **change buffer consolidation** phase before delta SQL generation.

---

## 6. Multi-Query Optimization

### 6.1 Shared Sub-Expression Elimination

**Sellis, T.K. (1988).** "Multiple-Query Optimization." *ACM TODS*,
13(1).

**Core idea:** When multiple queries share common sub-expressions
(e.g., the same join or scan), evaluate the common part once and reuse
the result.

**Relevance to pg_trickle:** **High.** In a typical analytics deployment,
multiple stream tables share the same source tables and often the same
join patterns:

```sql
-- ST1: Revenue by region
SELECT r.name, SUM(l.revenue) FROM lineitem l
JOIN orders o ON ... JOIN customer c ON ... JOIN region r ON ...
GROUP BY r.name;

-- ST2: Revenue by product category
SELECT p.category, SUM(l.revenue) FROM lineitem l
JOIN orders o ON ... JOIN part p ON ...
GROUP BY p.category;

-- ST3: Revenue by region AND product (superset join)
SELECT r.name, p.category, SUM(l.revenue) FROM lineitem l
JOIN orders o ON ... JOIN customer c ON ... JOIN region r ON ...
JOIN part p ON ...
GROUP BY r.name, p.category;
```

When `lineitem` changes, pg_trickle currently generates **independent**
delta SQL for each stream table. The `delta_lineitem ⋈ orders` sub-join
is computed three times.

**Implementation approach:**

1. **Co-scheduled refresh groups:** Identify stream tables that share
   the same source change epoch and have overlapping sub-expressions.
2. **Shared CTE generation:** For co-scheduled groups, generate a
   single SQL statement with shared CTEs:

```sql
-- Shared delta computation for ST1, ST2, ST3
WITH delta_lineitem AS (...),
-- Shared: delta_lineitem ⋈ orders (used by all three)
delta_lo AS (
  SELECT d.*, o.o_custkey, o.o_orderdate
  FROM delta_lineitem d JOIN orders o ON ...
),
-- ST1 branch: ⋈ customer ⋈ region → aggregate
st1_delta AS (
  SELECT r.name, SUM(dlo.revenue)
  FROM delta_lo dlo JOIN customer c ON ... JOIN region r ON ...
  GROUP BY r.name
),
-- ST2 branch: ⋈ part → aggregate
st2_delta AS (
  SELECT p.category, SUM(dlo.revenue)
  FROM delta_lo dlo JOIN part p ON ...
  GROUP BY p.category
),
...
```

3. **Applicability analysis:** Build a "sub-expression graph" across
   all stream tables during `CREATE STREAM TABLE`. Detect common
   sub-trees in their `OpTree` representations.

**Expected impact:** For $k$ stream tables sharing $m$ common
sub-expressions, reduces total delta computation from $O(k)$ to
$O(k - m + 1)$ join evaluations. In the TPC-H example above, the
shared `lineitem ⋈ orders` join (the most expensive sub-expression)
is computed once instead of three times.

### 6.2 Materialized Sub-Expression Caching

A lighter-weight alternative to full multi-query optimization:
**cache the results of frequently-needed sub-expressions** across
refresh cycles.

**Implementation approach:**

1. Use PostgreSQL unlogged tables or temp tables to cache intermediate
   join results that are needed by multiple stream tables.
2. Maintain a TTL or epoch-based invalidation: the cache is valid as
   long as the source tables haven't changed since the cache was built.
3. The scheduler would build caches before refreshing dependent STs
   and invalidate them after.

**Trade-off:** Additional storage and write cost for cache maintenance,
but amortized over multiple ST refreshes.

### 6.3 Global Query Graph (GQG)

**Chen, J., DeWitt, D.J., Tian, F., & Wang, Y. (2000).** "NiagaraCQ:
A Scalable Continuous Query System for Internet Databases." *SIGMOD 2000.*

**Core idea:** Represent all registered continuous queries as a single
directed acyclic graph where shared operators are merged. Updates flow
through the graph, triggering only affected operators.

**Relevance to pg_trickle:** **Medium.** pg_trickle's DAG already
captures stream table dependencies. The GQG extends this to
**intra-query** operator sharing (shared scans, shared joins). This is
the implementation-level version of §6.1's shared sub-expression
elimination.

---

## 7. Scheduling & Cascade Optimization

### 7.1 Adaptive Processing / Eddies

**Avnur, R. & Hellerstein, J.M. (2000).** "Eddies: Continuously
Adaptive Query Processing." *SIGMOD 2000.*

**Core idea:** Instead of fixing the join order at query compile time,
route tuples adaptively through operators based on real-time selectivity
and cost observations.

**Relevance to pg_trickle:** **Low for delta SQL** (PostgreSQL's planner
controls join order). However, the adaptive principle applies to
**refresh scheduling**: when multiple stream tables need refreshing,
the order should adapt based on observed refresh costs and downstream
impact.

**Implementation approach:** The scheduler already uses EDF (earliest
deadline first). An enhancement would be **cost-aware EDF**: among
stream tables with equal deadlines, prioritize those whose refresh
unblocks the most downstream dependents (critical path scheduling).

### 7.2 Micro-Batch Sizing Optimization

**Related to §5.2 but focused on scheduling.**

For a stream table with schedule interval $I$ and average change rate
$\lambda$, the expected batch size is $B = \lambda \times I$. The
refresh cost function $C(B)$ is typically:

$$C(B) = C_0 + \alpha B + \beta B \log B$$

where $C_0$ is fixed overhead, $\alpha$ is per-row processing, and
$\beta B \log B$ accounts for sort/hash costs in the planner.

The optimal batch size minimizes total cost per unit time:

$$\min_{I} \frac{C_0 + \alpha \lambda I + \beta \lambda I \log(\lambda I)}{I}$$

This gives $I^* \approx \sqrt{C_0 / (\beta \lambda)}$ — small tables
with low change rates should refresh less frequently; large tables with
high change rates need more frequent, smaller batches.

**Implementation approach:** The scheduler's `CALCULATED` mode already
inherits intervals from downstream consumers. Adding a **cost feedback
loop** that adjusts intervals based on recent refresh cost observations
would implement this optimization automatically.

### 7.3 Cascade-Aware Refresh Ordering

**Problem:** In a deep DAG (A → B → C → D), refreshing A's delta
propagates through B, C, D sequentially. If A and B both have changes,
the optimal ordering is to refresh A first (so B gets both its own
changes and A's propagated changes in a single refresh).

**Current behavior:** The scheduler uses topological order, which
already handles this correctly for DAGs. The optimization opportunity
is for **diamonds**: when B and C both depend on A, and D depends on
both B and C, the scheduler should refresh B and C in parallel (if
possible) and then D once.

**Implementation:** Already addressed by the parallel refresh plan
(PLAN_PARALLELISM.md). The additional algorithmic contribution is a
**cascade cost model** that estimates the total DAG refresh cost under
different orderings and parallelism strategies.

---

## 8. Data Structure Optimizations

### 8.1 Differential Arrangements (McSherry)

**McSherry, F., Murray, D.G., Isaacs, R., & Isard, M. (2013).**
"Differential Dataflow." *CIDR 2013.*

**McSherry, F. (2020).** "Shared Arrangements: practical inter-query
sharing for streaming dataflows." *PVLDB*, 13(10).

**Core idea:** An "arrangement" is a persistent, indexed, compacted
representation of a differential collection. Key properties:

1. **Indexed:** Supports point lookups by key in $O(\log n)$.
2. **Shared:** Multiple operators can read from the same arrangement.
3. **Compacted:** Old diffs are consolidated — if a key was inserted at
   time 1 and deleted at time 3, the arrangement only stores the net
   effect at the current frontier.

**Relevance to pg_trickle:** **Medium — architectural mismatch.**
pg_trickle delegates storage to PostgreSQL tables and indexes. It
doesn't maintain custom in-memory data structures. However, the
**concepts** are applicable:

1. **Shared indexes:** When multiple stream tables probe the same base
   table on the same join key, a single covering index serves all of
   them. pg_trickle could **recommend indexes** (or create them
   automatically) based on the union of all delta SQL access patterns.

2. **Compaction of change buffers:** The change buffer tables accumulate
   raw INSERT/DELETE rows. If a row is inserted and then quickly
   updated, the buffer has both the delete of the old version and the
   insert of the new version. Compacting these to a single "UPDATE"
   (or net change) before delta processing reduces I/O.

**Implementation approach:**

1. **Index recommendation:** During `CREATE STREAM TABLE`, analyze the
   generated delta SQL's access patterns (join keys, WHERE conditions).
   Emit `NOTICE` recommendations for missing indexes, or create them
   automatically if `pgtrickle.auto_create_indexes = on`.

2. **Change buffer compaction:** Before delta SQL execution, add a
   compaction query:

```sql
-- Compact change buffer: net effect per row
DELETE FROM pgtrickle_changes.changes_<oid>
WHERE __pgt_change_id IN (
  SELECT __pgt_change_id
  FROM (
    SELECT __pgt_change_id,
           SUM(CASE WHEN op = '+' THEN 1 ELSE -1 END)
             OVER (PARTITION BY __pgt_row_id) AS net
    FROM pgtrickle_changes.changes_<oid>
    WHERE __pgt_lsn <= :upper_bound
  ) t
  WHERE t.net = 0  -- insert + delete = no net change
);
```

### 8.2 Persistent / Functional Data Structures for Snapshots

**Core idea:** Use immutable, copy-on-write data structures to
efficiently represent pre-change snapshots without full materialization.

**Relevance to pg_trickle:** **Low — already solved differently.**
pg_trickle's pre-change snapshot (L₀) reconstruction currently uses
`EXCEPT ALL` against the change buffer. The WCOJ plan's Opportunity C
(L₀ elimination) addresses this more effectively by computing only the
affected portion of L₀. Functional data structures would require
replacing PostgreSQL's heap storage, which is not feasible.

### 8.3 Compact Differential Groups / Consolidation

**Core idea:** In differential dataflow, "consolidation" merges
multiple diffs at the same logical time into a single diff. This is
critical for keeping memory bounded in long-running computations.

**Relevance to pg_trickle:** **Already implemented** as change buffer
cleanup in the refresh pipeline. The additional optimization is
**pre-refresh consolidation**: before executing delta SQL, consolidate
the change buffer to remove redundant changes (e.g., INSERT followed
by DELETE of the same row). This reduces the delta size and hence the
delta SQL execution cost.

This is the same as §8.1's "change buffer compaction" — the differential
dataflow literature provides the theoretical foundation for why this is
always safe and beneficial.

---

## 9. Parallelism & Distribution

### 9.1 HyperCube Shuffle Algorithm

**Afrati, F.N. & Ullman, J.D. (2010).** "Optimizing Joins in a
Map-Reduce Environment." *EDBT 2010.*

**Beame, P., Koutris, P., & Suciu, D. (2014).** "Skew in Parallel
Query Processing." *PODS 2014.*

**Core idea:** For a multi-way join distributed across $p$ processors,
the HyperCube algorithm assigns each processor a "cell" in a
multi-dimensional space defined by the join attributes. Each tuple is
sent to all cells matching its attribute values. The total communication
is minimized by choosing the dimension allocation to match the query
hypergraph structure.

**Optimal communication:** For a query with fractional edge cover number
$\rho^*$, the minimum communication per processor is
$O(|D| / p^{1/\rho^*})$.

**Relevance to pg_trickle:** **Low for current architecture** (single
PostgreSQL instance). However, if pg_trickle ever supports distributed
execution (e.g., Citus, pg_trickle on multiple nodes), HyperCube
provides the optimal data distribution strategy for multi-way delta
joins.

**Near-term applicability:** PostgreSQL's parallel query execution
partitions work across parallel workers. When the delta SQL triggers
parallel execution, the planner's choice of partition strategy affects
performance. Understanding HyperCube helps inform **partitioning
recommendations** for source tables.

### 9.2 Shares Algorithm (Skew-Resilient Parallel Joins)

**Beame, P., Koutris, P., & Suciu, D. (2017).** "Communication Steps
for Parallel Query Processing." *JACM*, 64(6).

**Core idea:** The Shares algorithm handles **skewed data** in parallel
join processing. When some join key values have disproportionately many
tuples (heavy hitters), the algorithm replicates those heavy-hitter
tuples across more processors while keeping light-hitter tuples local.

**Relevance to pg_trickle:** **Medium.** Skew in join keys is a common
cause of poor parallel performance in PostgreSQL. When pg_trickle's
delta SQL triggers parallel execution and one worker gets stuck
processing a heavy-hitter key, the entire refresh is delayed.

**Implementation approach:** When generating delta SQL for stream tables
that are known to have skewed join keys (detectable via `pg_stats`),
add explicit **key-range partitioning** in the CTE:

```sql
-- Split delta processing by key range for parallel-friendly execution
WITH delta_light AS (
  SELECT * FROM delta_table
  WHERE join_key NOT IN (SELECT key FROM known_heavy_hitters)
),
delta_heavy AS (
  SELECT * FROM delta_table
  WHERE join_key IN (SELECT key FROM known_heavy_hitters)
)
-- Process separately: light keys use hash join, heavy keys use nested loop
...
```

### 9.3 Intra-Query Parallelism for Delta SQL

**Core idea:** Structure the generated delta SQL to maximize PostgreSQL's
parallel query execution capabilities.

**Relevant PostgreSQL features:**

- Parallel sequential scans (for large change buffers)
- Parallel hash joins (for joining delta against base tables)
- Parallel aggregation (for GROUP BY in delta SQL)

**Implementation approach:**

1. **Table statistics hints:** After initial population, ensure source
   tables have up-to-date statistics (`ANALYZE`) so the planner
   correctly estimates parallel scan benefits.
2. **CTE inlining vs. materialization:** PostgreSQL 12+ can inline
   non-recursive CTEs. pg_trickle should structure delta SQL to
   allow CTE inlining where the CTE is referenced once (avoiding
   unnecessary materialization barriers).
3. **GUC tuning:** Automatically set `parallel_tuple_cost` and
   `parallel_setup_cost` appropriately for the expected delta SQL
   workload via `SET LOCAL` in the refresh transaction.

---

## 10. Summary & Priority Matrix

### 10.1 Algorithms Not Covered in WCOJ Plan

| # | Algorithm | Category | Applicability | Priority | SQL-Expressible? | Effort |
|---|-----------|----------|--------------|----------|-----------------|--------|
| 1 | **Eager Aggregation** | Aggregate-aware | Join-aggregate queries | **Critical** | Yes | 3–5 days |
| 2 | **Higher-Order Delta** (DBToaster-style auto-decomposition) | Higher-order IVM | Deep joins (≥4 tables) | **High** | Yes (via ST DAGs) | 5–8 days |
| 3 | **Magic Sets** (for recursive CTE delta) | Demand-driven | Recursive stream tables | **High** | Yes (CTE rewrite) | 3–5 days |
| 4 | **Insert-Only Fast Path** | Dynamic evaluation | Append-heavy workloads | **High** | Yes (simplified SQL) | 2–3 days |
| 5 | **Shared Sub-Expression Elimination** | Multi-query opt. | Multiple STs, shared sources | **High** | Yes (shared CTEs) | 5–7 days |
| 6 | **Change Buffer Compaction** | Data structures | High churn sources | **Medium** | Yes (consolidation SQL) | 2–3 days |
| 7 | **FAQ / InsideOut Variable Ordering** | Aggregate-aware | CTE generation order | **Medium** | Yes (CTE reorder) | 3–5 days |
| 8 | **F-IVM Ring Abstraction** | Higher-order IVM | Aggregate maintenance | **Medium** | Partially | 5–8 days |
| 9 | **IVM_ε Degree Partitioning** | Adaptive IVM | Skewed join keys | **Medium** | Yes (partitioned CTEs) | 3–4 days |
| 10 | **Adaptive Batch Sizing** | Scheduling | High change rate sources | **Medium** | N/A (scheduler logic) | 2–3 days |
| 11 | **LMFAO Layered Aggregation** | Multi-query opt. | Multi-ST shared aggregates | **Low** | Partially | 5–8 days |
| 12 | **Index Recommendation** | Data structures | Delta SQL access patterns | **Low** | N/A (DDL advice) | 2–3 days |
| 13 | **Cascade Cost Model** | Scheduling | Deep DAGs | **Low** | N/A (scheduler logic) | 2–3 days |
| 14 | **Skew-Resilient Parallel Splits** (Shares) | Parallelism | Skewed parallel execution | **Low** | Partially | 3–5 days |
| 15 | **HyperCube Shuffle** | Parallelism | Distributed execution | **Low** | No (needs Citus) | N/A |

### 10.2 Recommended Implementation Roadmap

**Phase A — Quick Wins (5–8 days, high confidence):**

1. **Eager Aggregation** (#1) — Most impactful single optimization for
   aggregate-heavy queries. Can be implemented independently of the
   WCOJ plan.
2. **Insert-Only Fast Path** (#4) — Low effort, high impact for
   append-heavy workloads (event sourcing, audit logs, time-series).
3. **Change Buffer Compaction** (#6) — Reduces delta size for
   high-churn sources. Simple SQL-level optimization.

**Phase B — Core Algorithmic Improvements (8–15 days):**

4. **Higher-Order Delta Auto-Decomposition** (#2) — Complements the
   WCOJ plan. For deep joins, automatically create intermediate stream
   tables. Requires careful design of the auto-decomposition heuristic.
5. **Magic Sets for Recursive Deltas** (#3) — Critical for recursive
   CTE performance. Pushes demand restriction into the semi-naive
   iteration.

**Phase C — Multi-ST Optimization (5–7 days):**

6. **Shared Sub-Expression Elimination** (#5) — Requires a global
   view of all registered stream tables. Most impactful when there are
   10+ stream tables sharing base tables.

**Phase D — Fine-Tuning (as needed):**

7–15. Remaining items based on observed bottlenecks in production
deployments. The FAQ variable ordering (#7) and IVM_ε degree
partitioning (#9) are refinements of the WCOJ plan's Yannakakis
phase.

### 10.3 Interaction with WCOJ Plan

These algorithms are **complementary** to the WCOJ plan, not competing:

| WCOJ Plan Phase | This Plan's Complement |
|-----------------|----------------------|
| Phase 1: Yannakakis semi-join reduction | IVM_ε degree partitioning (#9) refines the selectivity threshold |
| Phase 2: Delta-WCOJ for multi-way joins | Higher-order delta (#2) reduces the number of relations per join |
| Phase 3: Free Join hybrid strategy | FAQ variable ordering (#7) extends the cost model to include aggregates |
| Phase 4: Factorized intermediate CTEs | Eager aggregation (#1) reduces CTE sizes from a different angle |
| (Not covered) | Magic Sets (#3), Insert-Only (#4), Multi-ST opt. (#5) are orthogonal |

---

## 11. References

### Higher-Order & Ring-Based IVM

1. **Koch, C., Ahmad, Y., Kennedy, O., et al. (2014).** "DBToaster:
   Higher-order Delta Processing for Dynamic, Frequently Fresh Views."
   *VLDB Journal*, 23(2).

2. **Nikolic, M. & Olteanu, D. (2018).** "Incremental View Maintenance
   with Triple Lock Factorisation Benefits." *SIGMOD 2018.*
   [arXiv:1703.07484](https://arxiv.org/abs/1703.07484)

3. **Nikolic, M., Zhang, H., Kara, A., & Olteanu, D. (2020).** "F-IVM:
   Learning over Fast Evolving Relational Data." *SIGMOD Demo 2020.*
   [arXiv:2006.00694](https://arxiv.org/abs/2006.00694)

4. **Kara, A., Ngo, H.Q., Nikolic, M., Olteanu, D., & Zhang, H. (2019).**
   "Counting Triangles under Updates in Worst-Case Optimal Time."
   *ICDT 2019.* (Best Paper Award)
   [arXiv:1804.02780](https://arxiv.org/abs/1804.02780)

5. **Kara, A., Nikolic, M., Olteanu, D., & Zhang, H. (2020).**
   "Trade-offs in Static and Dynamic Evaluation of Hierarchical Queries."
   *PODS 2020.* [arXiv:1907.01988](https://arxiv.org/abs/1907.01988)

### Aggregate-Aware Processing

6. **Abo Khamis, M., Ngo, H.Q., & Rudra, A. (2016).** "FAQ: Questions
   Asked Frequently." *PODS 2016.*
   [arXiv:1504.04044](https://arxiv.org/abs/1504.04044)

7. **Schleich, M., Olteanu, D., & Ciucanu, R. (2019).** "Layered
   Multiple Functional Aggregate Optimization (LMFAO)." *SIGMOD 2019.*
   [arXiv:1906.08687](https://arxiv.org/abs/1906.08687)

8. **Yan, W.P. & Larson, P.-Å. (1995).** "Eager Aggregation and Lazy
   Aggregation." *VLDB 1995.*

### Demand-Driven Evaluation

9. **Bancilhon, F., Maier, D., Sagiv, Y., & Ullman, J.D. (1986).**
   "Magic Sets and Other Strange Ways to Implement Logic Programs."
   *PODS 1986.*

10. **Beeri, C. & Ramakrishnan, R. (1991).** "On the Power of Magic."
    *JCSS*, 43(3).

11. **Ullman, J.D. (1989).** "Bottom-up Beats Top-down for Datalog."
    *PODS 1989.*

### Dynamic Query Evaluation

12. **Abo Khamis, M., Kara, A., Olteanu, D., & Suciu, D. (2024).**
    "Insert-Only versus Insert-Delete in Dynamic Query Evaluation."
    *PODS 2024.* [arXiv:2312.09331](https://arxiv.org/abs/2312.09331)

13. **Kara, A., Ngo, H.Q., Nikolic, M., Olteanu, D., & Zhang, H. (2020).**
    "Maintaining Triangle Queries under Updates." *ACM TODS*, 48(3).
    [arXiv:2004.03716](https://arxiv.org/abs/2004.03716)

### Multi-Query Optimization

14. **Sellis, T.K. (1988).** "Multiple-Query Optimization." *ACM TODS*,
    13(1).

15. **Chen, J., DeWitt, D.J., Tian, F., & Wang, Y. (2000).** "NiagaraCQ:
    A Scalable Continuous Query System for Internet Databases."
    *SIGMOD 2000.*

### Scheduling & Adaptive Processing

16. **Avnur, R. & Hellerstein, J.M. (2000).** "Eddies: Continuously
    Adaptive Query Processing." *SIGMOD 2000.*

### Data Structures & Differential Dataflow

17. **McSherry, F., Murray, D.G., Isaacs, R., & Isard, M. (2013).**
    "Differential Dataflow." *CIDR 2013.*

18. **McSherry, F. (2020).** "Shared Arrangements: practical inter-query
    sharing for streaming dataflows." *PVLDB*, 13(10).

### Parallelism & Distribution

19. **Afrati, F.N. & Ullman, J.D. (2010).** "Optimizing Joins in a
    Map-Reduce Environment." *EDBT 2010.*

20. **Beame, P., Koutris, P., & Suciu, D. (2014).** "Skew in Parallel
    Query Processing." *PODS 2014.*

21. **Beame, P., Koutris, P., & Suciu, D. (2017).** "Communication Steps
    for Parallel Query Processing." *JACM*, 64(6).

### IVM Foundations (already referenced by pg_trickle)

22. **Budiu, M., Ryzhyk, L., McSherry, F., & Tannen, V. (2023).** "DBSP:
    Automatic Incremental View Maintenance for Rich Query Languages."
    *PVLDB*, 16(7). [arXiv:2203.16684](https://arxiv.org/abs/2203.16684)

23. **Gupta, A. & Mumick, I.S. (1995).** "Maintenance of Materialized
    Views: Problems, Techniques, and Applications." *IEEE Data
    Engineering Bulletin*, 18(2).

24. **Gupta, A., Mumick, I.S., & Subrahmanian, V.S. (1993).** "Maintaining
    Views Incrementally." *SIGMOD 1993.* (DRed algorithm)
