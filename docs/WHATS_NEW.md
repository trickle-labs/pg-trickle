# What's New

A curated, plain-language summary of recent pg_trickle releases —
the bits a human reader actually wants to see. For the full
exhaustive list of changes per release, see the
[Changelog](changelog.md).

## v0.82.0 — Frontier and CDC durability gate

- Safe frontiers now account for active XID/XMIN and prepared-transaction
  writers, with fail-closed probe errors and immutable scheduler tick bounds.
- CDC buffers have a logged registry/sentinel, strict durability modes, and
  missing or crash-cleared state forces reinitialization.
- FULL refreshes protect source relations, and WAL cutover uses exact LSN
  proofs without blind slot advancement. Fused refresh is opt-in.

---

## v0.76 — Complete DuckLake Integration Removal (May 2026)

All DuckLake-specific code has been removed. The `DUCKLAKE_CHANGE_FEED` CDC
mode, the Parquet/S3 sink, all `ducklake_*` GUCs, catalog columns, and tables
are gone. pg_ducklake uses native table AM (not FDW), making the detection
heuristic obsolete; pg_duckpipe covers the outbound direction. Users who need
DuckLake egress should evaluate `pg_duckpipe` or `pg_tide`.

---

## v0.70 — Scheduler, validator & security hardening (May 2026)

The 0.70.0 sprint targets correctness, performance, and observability
without adding new SQL surface.

- **LATERAL validator fixed**: volatile expressions inside LATERAL SRFs
  and subqueries are now correctly caught before they silently corrupt
  differential maintenance
- **Batched health checks**: `check_slot_health_and_alert()` now issues
  one query for all change buffers instead of one per buffer (O(1) SPI)
- **Fused-chain eligibility batched**: dependency lookups for the fused
  refresh path are now a single query
- **History prune interval is now GUC-controlled**:
  `pg_trickle.history_prune_interval_seconds` (default 60 s); set `0` to
  disable
- **`delta_work_mem_cap_mb` default raised to 256 MiB**: the previous
  default of `0` (disabled) allowed unbounded memory during large
  differential refreshes
- **Publication name parser unified**: `create_publication` and
  `alter_publication` share one parser, closing a potential divergence
- **New function**: `pgtrickle.history_prune_status()` — exposes prune
  error count and last timing so operators can detect a stalled cleanup
  loop

## v0.69 — DuckLake sink reliability & security

- **Sink delivery state machine**: every delivery attempt is now tracked
  (`PENDING → WRITING → DELIVERED / FAILED_RETRYABLE / FAILED_PERMANENT`)
- Two new GUCs: `pg_trickle.ducklake_sink_max_retries` and
  `pg_trickle.ducklake_sink_failure_mode`
- View registration updated on `ALTER QUERY` so DuckLake clients always
  see the current view definition

## v0.68 — Correctness & durability sprint (Assessment 13)

Four real data-loss or audit-integrity bugs fixed:

- **Fused refresh audit trail restored**: `SCHEDULER_FUSED` was silently
  rejected by the `initiated_by` CHECK constraint since v0.63; every fused
  refresh audit record was missing from `pgt_refresh_history`
- **`change_buffer_durability` GUC is now wired**: the new GUC was
  registered in v0.36 but never read; `pg_trickle.unlogged_buffers` was
  the only path that actually worked. Both now function correctly;
  `unlogged_buffers` emits a deprecation `WARNING`
- **DuckLake timestamp NULL fix**: `timestamptz` / `timestamp` columns no
  longer produce `NULL` in exported Parquet files
- **Worker-pool executor removed**: the pool pre-dated fused chains and
  was dead code; eliminated 297 lines of stale code. Dynamic per-tick
  workers remain the only scheduling path

## v0.67 — DuckLake Phase 3b: view registration, provenance & ecosystem

- Stream tables with a DuckLake sink are now auto-registered as native
  catalog objects in DuckLake — every DuckLake client can discover them
  without extra wiring
- Every Parquet delta is traceable to the exact refresh cycle that
  produced it

## v0.65–0.66 — DuckLake Phase 2 & 3a: change-feed adapter + Parquet sink

- WAL-based change-feed adapter feeds incremental deltas directly into the
  DuckLake sink
- Parquet sink infrastructure: S3/MinIO upload, compression, encryption
  key prefix; new S3 credentials GUCs

## v0.64 — DuckLake Ecosystem Phase 1

- First-class DuckLake integration: stream tables can push Parquet deltas
  to a DuckLake catalog

## v0.63 — CTE-fused multi-node refresh

The scheduler now composes the delta SQL for an entire topological batch
into a **single** `WITH … MERGE; MERGE; …` statement, giving the PostgreSQL
planner visibility across the whole batch and eliminating per-node SPI
round-trips. For DAGs with N DIFFERENTIAL nodes, change-buffer I/O goes
from O(N) to O(1) (combined with the v0.62 fan-out optimisation).

- New GUCs: `pg_trickle.enable_fused_refresh`,
  `pg_trickle.fused_refresh_max_delta_rows`

## v0.62 — Change-buffer fan-out + `pg_aqueduct` prerequisites

- **Change-buffer fan-out**: each source's change buffer is now scanned
  once per tick and the delta routed to every dependent stream table,
  replacing per-consumer rescans
- New `pgtrickle.pause_scheduler` / `pgtrickle.resume_scheduler` SQL
  functions for migration tooling
- New `pgtrickle.stream_table_spec(name)` — stable JSON projection of a
  stream table's specification

## v0.59–0.61 — Performance, DX & pre-1.0 polish

- Seven hot-path performance improvements including batched CDC
  buffer-growth monitoring
- `pgtrickle.explain_stream_table(name)` — shows defining query, cached
  refresh metadata, and current state flags
- Extensive documentation additions; LATERAL joins, inline aggregates,
  and circular dependency tutorials completed

## v0.58 — Security & correctness hardening

- Ownership checks for outbox and publication APIs (HIGH-severity fix)
- `NOT IN` + NULL row constructor handled correctly — no more phantom
  deletes when one side of the predicate contains NULL
- Recursive-CTE depth guard applied consistently to DIFFERENTIAL mode
- WAL decoder TOCTOU advisory lock closes a race in concurrent WAL polling

## v0.50–0.57 — Embedding programme, pg_tide extraction & operational hardening

- **pgvector incremental aggregates**: `vector_avg` and `halfvec_avg`
  maintained differentially; ANN index rebuild after refresh
- **Hybrid search**: `pgtrickle.vector_status()` and per-tenant ANN
  indexing patterns
- **`pg_tide` extracted**: the outbox, inbox, and relay are now a
  standalone companion extension
- `pgtrickle.reliability_counters()` and operational health counters
  exposed via Prometheus
- Extensive CI coverage (light E2E path using `cargo pgrx package` +
  stock postgres container; dbt integration tests)

## v0.46–0.49 — pg_tide & embedding pipeline infrastructure

- Transactional outbox and inbox extracted into the `pg_tide` companion
  extension
- Embedding pipeline infrastructure: `attach_embedding_outbox()`,
  vector column tracking
- Repository migrated to `trickle-labs/pg-trickle`

## v0.43–0.45 — D+I change-buffer schema, GUC tuning & scalability

- Change buffer tables moved to dedicated `pgtrickle_changes` schema
- New `pg_trickle.cdc_trigger_mode = 'statement'` default — bulk DML
  now uses one trigger invocation per statement, not per row
- `worker_allocation_status()` and per-database worker quota GUC

## v0.41–0.42 — DVM correctness & repair API

- Structural cache keys fix: DAG-position is now part of the cache key,
  preventing cross-node plan sharing
- New `pgtrickle.repair_stream_table(name)` — detects and heals schema
  drift between the catalog entry and the actual storage table

## v0.38–0.40 — EC-01 join correctness + observability

- EC-01 join correctness sprint: phantom-row cleanup, cross-node row-id
  validation
- `pgtrickle.explain_delta()` — returns the actual query plan for the
  next differential refresh cycle

## v0.37 — pgVector incremental aggregates + distributed trace propagation

- `vector_avg` / `halfvec_avg` aggregate functions maintained
  incrementally
- OpenTelemetry trace propagation: set `pg_trickle.trace_id` in a
  session and every refresh span is linked to your application trace

## v0.36 — Temporal IVM, columnar backends & drain mode

- **Temporal IVM**: `temporal := true` on `create_stream_table` adds
  `__pgt_valid_from` / `__pgt_valid_to` columns for SCD Type-2 patterns
- **Columnar storage backends**: `storage_backend` parameter accepts
  `'citus'` (Citus columnar)
- **Drain mode**: `pgtrickle.drain()` gracefully quiesces the scheduler
  before maintenance windows or `pg_upgrade`
- **L0 process-local template cache**: eliminates the ~45 ms cold-start
  penalty per new backend connection in pooled deployments
- **Online schema evolution**: compatible `ALTER QUERY` (column additions
  only) no longer requires a full reinit

## v0.35 — Reactive subscriptions & relay resilience

- `pgtrickle.subscribe(stream_table, channel)` — NOTIFY-based reactive
  delivery after every non-empty refresh cycle
- `pgtrickle.sla_summary()` — p50/p99 freshness latency and error-budget
  remaining over a configurable window
- `pg_trickle.cdc_paused` GUC — pause all CDC capture without dropping
  triggers (useful during maintenance)
- Relay: `${ENV:VAR_NAME}` secret interpolation and exponential reconnect
  backoff

## v0.34 — Citus self-driving (April 2026)

The Citus integration grew up. The per-worker WAL slot lifecycle —
creation, polling, lease management, recovery from rebalances — now
runs automatically. There is no manual wiring left for distributed
sources.

- Per-worker slot lifecycle fully automated
  ([CITUS](CITUS.md))
- Shard-rebalance auto-recovery
- Worker failure isolation with retry budget

## v0.33 — DAG observability + worker-pool quotas

- Per-database worker quotas keep one busy database from starving
  the rest ([SCALING](SCALING.md))
- New cluster-wide health view
  ([MULTI_DATABASE](MULTI_DATABASE.md))

## v0.32 — Citus distributed sources & outputs

- Stream tables can read from Citus-distributed source tables
- `output_distribution_column` produces co-located distributed
  stream tables

## v0.28 — Transactional Outbox & Inbox

- First-class outbox and inbox patterns built on stream tables
  ([OUTBOX](OUTBOX.md) · [INBOX](INBOX.md))
- Consumer groups, lag tracking, and dead-letter queues out of
  the box

## v0.27 — Snapshots & SLA-based scheduling

- [Snapshots](SNAPSHOTS.md) of stream-table contents — point-in-time
  copies for backup, replica bootstrap, and rollback
- `recommend_schedule` and predicted-SLA-breach alerts
- PITR alignment guidance for replica bootstrap

## v0.22 — Downstream Publications

- Any stream table can be exposed as a PostgreSQL logical
  publication. Debezium, Kafka Connect, Spark Structured
  Streaming, a downstream PostgreSQL replica — all subscribe to
  pg_trickle's incrementally-computed diffs without extra
  pipelines ([PUBLICATIONS](PUBLICATIONS.md))
- `set_stream_table_sla` introduces freshness deadlines

## v0.14 — AUTO mode by default + ergonomic warnings

- `refresh_mode = 'AUTO'` is the new default
- `create_stream_table` warns on common anti-patterns
  (low-cardinality aggregates, non-deterministic queries)

## v0.13 — Delta SQL profiling

- `pgtrickle.explain_delta`, `dedup_stats`,
  `shared_buffer_stats` — visibility into what the engine is
  actually doing per refresh

## v0.12 — Tiered scheduling on by default

- Hot/Warm/Cold/Frozen tiers, enabled by default, dramatically
  reduce scheduler overhead at scale
  ([Tiered Scheduling tutorial](tutorials/TIERED_SCHEDULING.md))

## v0.10 — Production-readiness floor

- Crash recovery, fuse circuit breaker, monitoring views, structured
  errors with SQLSTATE codes
  ([ERRORS](ERRORS.md) · [TROUBLESHOOTING](TROUBLESHOOTING.md))

## v0.9 — Algebraic aggregate maintenance

- `AVG`, `STDDEV`, and `COUNT(DISTINCT)` maintained from auxiliary
  state — no group-rescan needed in the common case

## v0.7 — Watermarks and circular DAGs

- Watermark gating for ETL pipelines
- Monotone cycles supported with explicit `pg_trickle.allow_circular`
  ([Circular Dependencies](tutorials/CIRCULAR_DEPENDENCIES.md))
- Prometheus / Grafana observability
  ([integrations/prometheus](integrations/prometheus.md))

## v0.6 — Partitioned source tables and idempotent DDL

- Stream tables can now read from partitioned source tables; all partitions
  are tracked automatically without extra configuration
- `create_stream_table` and `drop_stream_table` are idempotent — safe to
  call from migration scripts and `IF NOT EXISTS` guards
- Circular dependency detection with a hard gate: cycles in the DAG raise
  a clear error with the offending chain listed

## v0.5 — Row-level security and ETL bootstrap gating

- RLS policies on source tables are respected during the defining query's
  first FULL refresh; differential refreshes maintain the same visibility
  contract
- ETL bootstrap gate: a stream table can be held in SUSPENDED state until
  an external ETL load completes, then released atomically
- `pgtrickle.pgt_status()` view expanded with per-table health indicators

## v0.4 — Parallel refresh

- `parallel_refresh_mode = 'on'` dispatches independent stream
  tables across a worker pool ([SCALING](SCALING.md))

## v0.3 — HAVING, FULL OUTER JOIN, and correlated subqueries

- `HAVING` clauses are now maintained differentially — no more falling
  back to FULL refresh when a GROUP BY result is post-filtered
- `FULL OUTER JOIN` supported in DIFFERENTIAL mode using an 8-part
  UNION ALL delta strategy
- Correlated subqueries in the SELECT-list maintained with a pre/post
  snapshot EXCEPT ALL diff

## v0.2 — IMMEDIATE mode + TopK

- `IMMEDIATE` refresh mode: maintain stream tables inside the
  source DML's transaction
- TopK stream tables: `ORDER BY x LIMIT N`
- `ALTER QUERY` — change the defining query online

## v0.1 — Differential foundation

- Trigger-based CDC captures every INSERT, UPDATE, and DELETE into
  per-table change buffers within the source DML transaction — zero
  committed-change loss
- Differential (incremental) and full refresh, with automatic fallback
  when a query is not IVM-eligible
- Background scheduler with per-database workers
- Initial monitoring views: `pgt_stream_tables`, `pgt_refresh_history`

---

**See also:** [Changelog (full detail)](changelog.md) ·
[Roadmap (what's coming)](roadmap.md)
