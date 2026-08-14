# Storage Backends

> **Status per backend** — Heap: **Stable** · Unlogged: **Stable** · columnar/Hydra: **Beta** · DuckDB: **Experimental** · TimescaleDB: **Beta** · pgvector: **Stable**

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
- **Note:** Stream-table outputs and CDC buffers are separate. CDC buffers use
  `pg_trickle.change_buffer_durability`; choose `unlogged` only when the
  sentinel-based FULL-reinitialization trade-off is acceptable.

### Citus Columnar (`citus`)

Append-only columnar storage via the Citus extension. Provides high compression
and fast analytical query performance for time-series and immutable data.

- **Required extensions:** `citus` (v11+) with columnar storage enabled
- **Recommended for:** Append-only analytics, audit logs, time-series data
- **Refresh mode support:** FULL only (columnar is append-only; differential
  requires UPDATE/DELETE which columnar does not support)
- **Partitioning support:** Yes (range/list partitioning on columnar tables)
- **Limitations:** No UPDATE/DELETE, no UNIQUE constraints, no FK constraints

---

## Choosing a Backend

```
Is data loss on crash acceptable and write speed critical?
  └─ Yes → UNLOGGED heap
  └─ No  → Continue ↓

Is the output append-only (no deletes/updates in the stream)?
  └─ Yes → Citus columnar (high compression, fast scans)
  └─ No  → Heap (default) — supports DIFFERENTIAL refresh

Is high-speed analytical querying over large history the priority?
  └─ Yes → Citus columnar
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

For UNLOGGED CDC buffers, loss of the registered sentinel blocks differential
refresh and marks consumers for protected FULL reinitialization. A clean
restart that preserves the sentinel does not trigger a rebuild.

---

## Configuration Reference

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.columnar_backend` | `none` | Columnar backend: `none` or `citus` |
| `pg_trickle.change_buffer_durability` | `logged` | Durability of CDC change buffers: `logged`, `sync`, or `unlogged` |

See [docs/CONFIGURATION.md](CONFIGURATION.md) for the full GUC reference.

---

## Extension Dependencies

| Backend      | Extension       | Minimum version | Install command           |
|-------------|-----------------|-----------------|---------------------------|
| Citus columnar | `citus`       | 11.0            | `CREATE EXTENSION citus;` |

All extensions must be installed in the same database as pg_trickle before
creating stream tables with that backend.
