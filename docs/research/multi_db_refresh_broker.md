# Multi-Database Refresh Broker — Design Document

> **Implementation Status:** Design only — not yet implemented.
> This feature is planned for a future release after v1.0.
> Track progress at [ROADMAP.md](../../ROADMAP.md).
>
> The design below is a stable reference for contributors and reviewers.
> The API described here is aspirational and subject to change before
> implementation begins.

**Status:** Design only (v0.31.0 SCAL-2). Implementation planned for a future release.

---

## Problem

When pg_trickle is installed in multiple databases on the same PostgreSQL cluster, each
per-database scheduler independently scans its change buffers. For workloads where two
databases reference the same upstream source — commonly via `postgres_fdw` foreign tables
or logical replication — each scheduler pays the full scan cost independently:

```
DB A scheduler: SELECT * FROM pgtrickle_changes.changes_12345  (full scan)
DB B scheduler: SELECT * FROM pgtrickle_changes.changes_12345  (same scan, again)
```

At 100 stream tables across 10 databases with 5 shared sources, this is 10× the
necessary I/O.

---

## Goal

Introduce a "refresh broker" — a singleton background worker that:

1. De-duplicates change buffer scans across databases in the same cluster.
2. Distributes scan results to per-database schedulers via shared memory.
3. Reduces total change buffer I/O proportionally to the number of databases
   sharing a source.

---

## Design

### Components

```
┌─────────────────────────────────────────────────────────────┐
│ PostgreSQL Cluster                                           │
│                                                             │
│  ┌────────────────┐     shared memory      ┌─────────────┐ │
│  │ Refresh Broker │ ──── scan results ────► │ DB-A sched  │ │
│  │  (singleton)   │                         │ DB-B sched  │ │
│  │                │ ◄─── scan requests ──── │ DB-C sched  │ │
│  └────────────────┘                         └─────────────┘ │
│           │                                                  │
│           │ single scan per source OID per tick              │
│           ▼                                                  │
│  pgtrickle_changes.changes_{oid}                             │
└─────────────────────────────────────────────────────────────┘
```

### Broker Protocol

1. **Registration**: Each per-database scheduler registers its interest in a set of
   source OIDs with the broker via shared memory (`PgLwLock<BrokerRegistry>`).

2. **Tick coordination**: At the start of each tick, the broker scans each registered
   source OID once. Results (row counts + LSN watermarks) are written to a shared
   memory segment indexed by (database_oid, source_oid).

3. **Result consumption**: Per-database schedulers read the broker's results instead
   of issuing their own SPI queries. The broker's scan is authoritative for the tick.

4. **Fallback**: If the broker is not running (e.g. `max_worker_processes` exhausted),
   per-database schedulers fall back to their current direct-scan behaviour.

### Shared Memory Layout

```rust
/// BRK-1: Broker registry entry for one (database, source) pair.
struct BrokerEntry {
    db_oid: pg_sys::Oid,
    source_oid: pg_sys::Oid,
    /// Last scan result: row count in the change buffer.
    pending_rows: i64,
    /// Last scan result: maximum LSN seen in the change buffer.
    max_lsn_u64: u64,
    /// Monotone tick counter when this entry was last updated.
    last_updated_tick: u64,
}

/// Maximum number of (db, source) pairs the broker tracks.
const BROKER_CAPACITY: usize = 4096;
```

### Broker Worker Loop

```
loop:
  1. Sleep until next tick (shared scheduler_interval_ms).
  2. Lock BrokerRegistry (read) to collect unique source OIDs.
  3. For each unique source OID, run:
       SELECT COUNT(*), MAX(lsn) FROM pgtrickle_changes.changes_{oid}
  4. Write results to BrokerScanResults (lock-free CAS update).
  5. Advance broker tick counter.
  6. Per-DB schedulers wake and consume results.
```

### Integration with Per-DB Schedulers

In `src/scheduler.rs`, `has_table_source_changes` would gain a fast path:

```rust
fn has_table_source_changes(st: &StreamTableMeta) -> bool {
    // Fast path: try broker results first.
    if config::pg_trickle_adaptive_batch_coalescing() {
        if let Some(result) = broker::get_scan_result(source_oid) {
            return result.pending_rows > 0;
        }
    }
    // Fallback: direct SPI query (current behaviour).
    // ...
}
```

---

## Open Questions

1. **Transaction isolation**: The broker scans in its own transaction. Per-DB schedulers
   that read its results are using data from a different snapshot. Is this acceptable?
   (Short answer: yes — the existing behaviour already has a tick-window delay between
   when changes are written and when they are consumed.)

2. **Cross-database connectivity**: The broker must connect to each database to read its
   change buffers. PostgreSQL background workers connect to a specific database. We may
   need a pool of broker workers, one per database, coordinated by a shared-memory
   rendezvous point.

3. **Authorization**: The broker needs read access to `pgtrickle_changes.*` in each
   database. This is satisfied by `shared_preload_libraries` + `SECURITY DEFINER`
   wrapper functions.

4. **Failure isolation**: If the broker crashes, per-DB schedulers must detect the
   absence of fresh results and fall back to direct scans within one tick.

---

## Next Steps (v0.32.0+)

- [ ] Implement `BrokerRegistry` shared memory struct + `init_shared_memory` hook
- [ ] Implement broker background worker registration
- [ ] Add `broker::get_scan_result` fast path in `has_table_source_changes`
- [ ] Add `pg_trickle.enable_refresh_broker` GUC (default `false` until stable)
- [ ] Add E2E test: two databases sharing a source, broker reduces SPI queries to 1×
- [ ] Benchmark: 10 databases × 5 shared sources — measure scan reduction

---

*Filed under v0.31.0 (SCAL-2). ADR reference: see plans/adrs/ for architectural
rationale on trigger-based vs broker-based CDC.*
