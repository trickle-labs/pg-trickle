# Tutorial: Circular Dependencies

pg_trickle supports circular (cyclic) stream table dependencies
for queries that use only **monotone** operators. The scheduler groups
circular dependencies into Strongly Connected Components (SCCs) and
iterates them to a fixed point.

## When to Use It

- **Transitive closure** — computing all reachable nodes in a graph
- **Graph reachability** — finding all paths between nodes
- **Iterative convergence** — mutual dependencies that stabilize after
  a few iterations

## Prerequisites

Circular dependencies are disabled by default. Enable them:

```sql
SET pg_trickle.allow_circular = true;
```

## Monotone Operator Requirement

Only **monotone** operators are allowed in circular dependency chains.
Monotone operators guarantee convergence — the result set grows (or stays
the same) with each iteration until a fixed point is reached.

| Allowed (proven monotone) | Blocked or unproven |
|---------------------------|---------------------|
| INNER JOIN | LEFT/FULL JOIN |
| Filters (WHERE) | Aggregates (SUM, COUNT, etc.) |
| Projections (SELECT) | INTERSECT / EXCEPT |
| UNION ALL | Window functions |
| EXISTS / IN | NOT EXISTS / NOT IN |
| DISTINCT | Scalar subqueries and LATERAL |

Creating a circular dependency with blocked or unproven operators is rejected
with a clear error message, regardless of the `allow_circular` setting.

## Step-by-Step Example: Transitive Closure

Suppose you have a graph of relationships:

```sql
CREATE TABLE edges (src INT, dst INT);
INSERT INTO edges VALUES
    (1, 2), (2, 3), (3, 4), (4, 5),
    (1, 3), (2, 5);
```

### 1. Create the base reachability table

```sql
-- Direct edges: all nodes directly connected
SELECT pgtrickle.create_stream_table(
    name     => 'reachable_direct',
    query    => 'SELECT src, dst FROM edges',
    schedule => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

### 2. Create the transitive closure with a self-reference

```sql
-- Transitive closure: if A→B and B→C, then A→C
-- This creates a circular dependency (reachable depends on itself via the join)
SELECT pgtrickle.create_stream_table(
    name     => 'reachable',
    query    => 'SELECT DISTINCT r1.src, r2.dst
                 FROM pgtrickle.reachable_direct r1
                 JOIN pgtrickle.reachable_direct r2 ON r1.dst = r2.src
                 UNION ALL
                 SELECT src, dst FROM edges',
    schedule => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

> **Note:** This example uses the `reachable_direct` table for the
> join rather than self-referencing `reachable` directly. For a true
> self-referencing cycle, pg_trickle detects the SCC and iterates.

### 3. Observe the fixed-point iteration

When the scheduler processes an SCC, it iterates until no new rows are
produced (the fixed point):

```sql
-- Check SCC status
SELECT * FROM pgtrickle.pgt_scc_status();
```

Output:

```
 scc_id | members                          | iteration | converged
--------+----------------------------------+-----------+-----------
      1 | {reachable_direct,reachable}     |         3 | true
```

### 4. Add new edges and watch convergence

```sql
INSERT INTO edges VALUES (5, 1);  -- creates a cycle in the graph
```

On the next refresh cycle, the scheduler re-iterates the SCC until the
transitive closure stabilizes with the new edge.

## Monitoring SCCs

```sql
-- View all SCCs and their convergence status
SELECT * FROM pgtrickle.pgt_scc_status();

-- Check which stream tables belong to which SCC
SELECT pgt_name, scc_id
FROM pgtrickle.pgt_stream_tables
WHERE scc_id IS NOT NULL;
```

## Controlling Iteration Limits

The `pg_trickle.max_fixpoint_iterations` GUC limits how many iterations
the scheduler attempts before declaring non-convergence:

```sql
-- Default: 100 (generous headroom)
SHOW pg_trickle.max_fixpoint_iterations;

-- Lower it for fast-converging workloads
SET pg_trickle.max_fixpoint_iterations = 20;
```

If convergence is not reached within the limit, all SCC members are marked
as `ERROR`. This prevents runaway infinite loops.

## Limitations

- **Blocked or unproven operators are always rejected** — outer joins,
  aggregates, set differences, window functions, anti-joins, scalar
  subqueries, and LATERAL cannot appear in circular chains because they can
  remove or replace output, or lack a proof of monotonicity.
- **Performance scales with iteration count** — each iteration runs a full
  differential refresh cycle for all SCC members. Keep cycles small.
- **All SCC members must use DIFFERENTIAL mode** — FULL and IMMEDIATE modes
  are not supported for circular dependencies.

## Further Reading

- [Configuration — pg_trickle.allow_circular](../CONFIGURATION.md#pg_trickleallow_circular)
- [Configuration — pg_trickle.max_fixpoint_iterations](../CONFIGURATION.md#pg_tricklemax_fixpoint_iterations)
- [SQL Reference — pgt_scc_status()](../SQL_REFERENCE.md#pgtricklepgt_scc_status)
- [FAQ — Cycle Detection](../FAQ.md#i-get-cycle-detected-when-creating-a-stream-table)
