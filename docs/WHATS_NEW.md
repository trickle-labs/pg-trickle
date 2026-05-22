# What's New

A curated, plain-language summary of recent pg_trickle releases —
the bits a human reader actually wants to see. For the full
exhaustive list of changes per release, see the
[Changelog](changelog.md).

---

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
