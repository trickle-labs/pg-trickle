# Storage Backends

> **Status per backend** — Heap: **Stable** · Unlogged: **Stable** · columnar/Hydra: **Beta** · DuckDB: **Experimental** · TimescaleDB: **Beta** · pgvector: **Stable**

pg_trickle supports multiple storage backends for stream table output. Each
backend has different prerequisites, semantics, and performance characteristics.

---

## Available Backends

### Heap (default)

Standard PostgreSQL row-store tables. No prerequisites beyond a working
PostgreSQL installation. All pg_trickle features are supported.

- **Required extensions:** None
- **Recommended for:** General-purpose stream tables, OLTP workloads
- **Refresh mode support:** FULL, DIFFERENTIAL (AUTO)
- **Partitioning support:** Yes (via `PARTITION BY`)

### Unlogged (UNLOGGED)

Heap tables declared `UNLOGGED`. Faster write performance because WAL is not
generated for table data. Tables are truncated on crash recovery.

- **Required extensions:** None
- **Recommended for:** Caches and derived tables where data loss on crash is
  acceptable and fast refresh is critical
- **Refresh mode support:** FULL, DIFFERENTIAL (AUTO)
- **Partitioning support:** Yes
- **Note:** Change buffers (`pgtrickle_changes.*`) are always UNLOGGED for
  performance. Stream table outputs can optionally be UNLOGGED via the
  `pg_trickle.change_buffer_durability` GUC.

### Citus Columnar (`citus`)

Append-only columnar storage via the Citus extension. Provides high compression
and fast analytical query performance for time-series and immutable data.

- **Required extensions:** `citus` (v11+) with columnar storage enabled
- **Recommended for:** Append-only analytics, audit logs, time-series data
- **Refresh mode support:** FULL only (columnar is append-only; differential
  requires UPDATE/DELETE which columnar does not support)
- **Partitioning support:** Yes (range/list partitioning on columnar tables)
- **Limitations:** No UPDATE/DELETE, no UNIQUE constraints, no FK constraints

### pg_mooncake Columnstore (`pg_mooncake`)

Columnstore tables via the `pg_mooncake` extension. Similar semantics to Citus
columnar — append-only with high compression.

- **Required extensions:** `pg_mooncake`
- **Recommended for:** Analytical workloads requiring fast scans over large data
- **Refresh mode support:** FULL only
- **Partitioning support:** Limited (depends on pg_mooncake version)

---

## Choosing a Backend

```
Is data loss on crash acceptable and write speed critical?
  └─ Yes → UNLOGGED heap
  └─ No  → Continue ↓

Is the output append-only (no deletes/updates in the stream)?
  └─ Yes → Citus columnar or pg_mooncake (high compression, fast scans)
  └─ No  → Heap (default) — supports DIFFERENTIAL refresh

Is high-speed analytical querying over large history the priority?
  └─ Yes → Citus columnar or pg_mooncake
  └─ No  → Heap (simpler, fully supported)
```

---

## Migration Between Backends

pg_trickle does not automatically migrate data between backends. To change a
stream table's backend:

1. Drop the stream table: `SELECT pgtrickle.drop_stream_table('my_st');`
2. Re-create with the desired backend using the `storage_backend` option
3. Run a full refresh: `SELECT pgtrickle.refresh_stream_table('my_st');`

No data in the underlying source tables is lost during this process — only
the derived stream table output is rebuilt.

---

## Failure Modes and Fallback Semantics

| Backend      | Failure mode                        | Fallback behavior                              |
|-------------|--------------------------------------|------------------------------------------------|
| Heap         | Disk full, I/O error                | Transaction rollback; stream table unchanged   |
| Unlogged     | Server crash                        | Table truncated on recovery; full rebuild needed |
| Citus columnar | Citus extension not loaded        | Error at CREATE STREAM TABLE time              |
| Citus columnar | Full disk                         | Transaction rollback; partial segments may remain |
| pg_mooncake  | Extension not loaded                | Error at CREATE STREAM TABLE time              |

For UNLOGGED stream tables, pg_trickle detects the truncation during the next
refresh cycle and performs a full rebuild automatically.

---

## Configuration Reference

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.columnar_backend` | `none` | Columnar backend: `none`, `citus`, `pg_mooncake` |
| `pg_trickle.change_buffer_durability` | `unlogged` | Durability of CDC change buffers: `logged` or `unlogged` |

See [docs/CONFIGURATION.md](CONFIGURATION.md) for the full GUC reference.

---

## Extension Dependencies

| Backend      | Extension       | Minimum version | Install command           |
|-------------|-----------------|-----------------|---------------------------|
| Citus columnar | `citus`       | 11.0            | `CREATE EXTENSION citus;` |
| pg_mooncake  | `pg_mooncake`   | 0.1             | `CREATE EXTENSION pg_mooncake;` |

All extensions must be installed in the same database as pg_trickle before
creating stream tables with that backend.
