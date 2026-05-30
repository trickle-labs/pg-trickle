<!-- AUTO-GENERATED — do not edit by hand.
     Run `python3 scripts/gen_catalogs.py` to regenerate.
     CI fails if this file is out of date with source code. -->

# SQL API Reference — pg_trickle

**127 SQL-callable functions** discovered via `#[pg_extern]` in `src/`.

See [docs/SQL_REFERENCE.md](SQL_REFERENCE.md) for full signatures and examples.


| Function | Schema | Returns | Description |
|----------|--------|---------|-------------|
| `pgtrickle._on_ddl_end()` | `pgtrickle` | `` | Registered via `extension_sql!()` in lib.rs as: ```sql CREATE FUNCTION pgtrickle._on_ddl_end() RETURNS event_trigger . |
| `pgtrickle._on_sql_drop()` | `pgtrickle` | `` | Detects when upstream source tables or ST storage tables themselves are dropped and reacts accordingly. |
| `pgtrickle._signal_launcher_rescan()` | `pgtrickle` | `` | Also safe to call manually if the launcher needs a nudge. |
| `pgtrickle.advance_watermark()` | `pgtrickle` | `void` | - **Monotonic:** rejects watermarks that go backward. |
| `pgtrickle.alter_stream_table()` | `pgtrickle` | `` | Alter properties of an existing stream table. |
| `pgtrickle.attach_embedding_outbox()` | `pgtrickle` | `` | The `vector_column` parameter documents which column carries the embedding — it is stored in the outbox headers so consumers can identify the embedding field without inspecting the payload. |
| `pgtrickle.attach_outbox()` | `pgtrickle` | `` | Requires `pg_tide` to be installed. |
| `pgtrickle.bootstrap_gate_status()` | `pgtrickle` | `SetOf row` | BOOT-F3: Designed for debugging "why isn't my stream table refreshing?" situations by showing the full gate lifecycle at a glance. |
| `pgtrickle.build_init_decision()` | `pgtrickle` | `(internal)` |  |
| `pgtrickle.bulk_alter_stream_tables()` | `pgtrickle` | `integer` | # Example ```sql SELECT pgtrickle.bulk_alter_stream_tables(     ARRAY['public.orders_summary', 'public.daily_revenue'],     '{"schedule": "5m", "tier": "warm"}'::jsonb ); ```. |
| `pgtrickle.bulk_create()` | `pgtrickle` | `jsonb` | On any error, the entire transaction is rolled back (standard PostgreSQL transactional semantics). |
| `pgtrickle.bulk_drop_stream_tables()` | `pgtrickle` | `integer` | # Example ```sql SELECT pgtrickle.bulk_drop_stream_tables(     ARRAY['public.orders_summary', 'public.stale_view'] ); ```. |
| `pgtrickle.cache_stats()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.cache_stats()`. |
| `pgtrickle.cdc_pause_status()` | `pgtrickle` | `SetOf row` | Returns a table with one row containing: - `paused` — `true` when `cdc_paused = on` - `capture_mode` — `'discard'` or `'hold'` - `note` — human-readable explanation of the current state. |
| `pgtrickle.change_buffer_sizes()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.change_buffer_sizes()`. |
| `pgtrickle.check_cdc_health()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.check_cdc_health()`. |
| `pgtrickle.clear_caches()` | `pgtrickle` | `bigint` | Use during debugging, emergency migration rollback, or after a query definition change that was not captured by the normal DDL invalidation path. |
| `pgtrickle.cluster_worker_summary()` | `pgtrickle` | `SetOf row` | Reads from `pg_stat_activity` (shared catalog) so the calling role needs `pg_monitor` or superuser privilege. |
| `pgtrickle.convert_buffers_to_unlogged()` | `pgtrickle` | `bigint` | **Warning:** After conversion, buffer contents will be lost on crash recovery. |
| `pgtrickle.create_or_replace_stream_table()` | `pgtrickle` | `` | This is the declarative API for idempotent deployments (dbt, migrations, GitOps). |
| `pgtrickle.create_refresh_group()` | `pgtrickle` | `` | # Arguments - `group_name`: Unique human-readable name for the group. |
| `pgtrickle.create_stream_table()` | `pgtrickle` | `` | # Arguments - `name`: Schema-qualified name (`'schema.table'`) or unqualified (`'table'`). |
| `pgtrickle.create_stream_table_fast_append_only()` | `pgtrickle` | `` | # Example ```sql SELECT pgtrickle.create_stream_table_fast_append_only(     'my_schema.event_counts',     'SELECT user_id, count(*) AS n FROM events GROUP BY user_id' ); ```. |
| `pgtrickle.create_stream_table_if_not_exists()` | `pgtrickle` | `` | This is useful for migration scripts that should be safe to re-run. |
| `pgtrickle.create_watermark_group()` | `pgtrickle` | `` | - `group_name`: unique name for this group. |
| `pgtrickle.dedup_stats()` | `pgtrickle` | `SetOf row` | Example: ```sql SELECT * FROM pgtrickle.dedup_stats(); ```. |
| `pgtrickle.dependency_tree()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.dependency_tree()`. |
| `pgtrickle.detach_outbox()` | `pgtrickle` | `` | Removes the entry from `pgtrickle.pgt_outbox_config`. |
| `pgtrickle.diagnose_errors()` | `pgtrickle` | `SetOf row` | # SQL usage ```sql SELECT * FROM pgtrickle.diagnose_errors('my_stream_table'); ```. |
| `pgtrickle.diamond_groups()` | `pgtrickle` | `SetOf row` | Returns one row per group member, indicating which group it belongs to, whether it is a convergence (fan-in) node, the group's current epoch, and the effective schedule policy. |
| `pgtrickle.drain()` | `pgtrickle` | `` | # Example ```sql -- Quiesce before pg_upgrade or rolling restart: SELECT pgtrickle.drain(); -- Confirm drained: SELECT pgtrickle.is_drained(); -- Resume normal operation after maintenance: UPDATE pgtrickle.pgt_stream_tables SET status = status; -- noop, scheduler picks up ```. |
| `pgtrickle.drop_refresh_group()` | `pgtrickle` | `void` | Drop a refresh group by name. |
| `pgtrickle.drop_snapshot()` | `pgtrickle` | `` | Removes the snapshot table and its catalog row from `pgtrickle.pgt_snapshots`. |
| `pgtrickle.drop_stream_table()` | `pgtrickle` | `` | Changed in v0.19.0 (UX-6): default flipped from `true` to `false` to prevent accidental cascading drops. |
| `pgtrickle.drop_stream_table_publication()` | `pgtrickle` | `` | CDC-PUB-2: Drop the logical replication publication for a stream table. |
| `pgtrickle.drop_watermark_group()` | `pgtrickle` | `void` | Drop a watermark group by name. |
| `pgtrickle.embedding_stream_table()` | `pgtrickle` | `` | # Returns A single-column table with one row per action taken (or SQL line for dry_run). |
| `pgtrickle.exec_stream_ddl()` | `pgtrickle` | `boolean` | # Example ```sql SELECT pgtrickle.exec_stream_ddl(   'CREATE STREAM TABLE revenue AS SELECT SUM(amount) FROM orders' ); ```. |
| `pgtrickle.explain_dag()` | `pgtrickle` | `` | Node colours: user STs = blue, self-monitoring STs = green, suspended = red, fused = orange. |
| `pgtrickle.explain_delta()` | `pgtrickle` | `` | Example: ```sql SELECT line FROM pgtrickle.explain_delta('public.orders_summary'); SELECT line FROM pgtrickle.explain_delta('public.orders_summary', 'json'); ```. |
| `pgtrickle.explain_diff_sql()` | `pgtrickle` | `text (nullable)` | Exposed as `pgtrickle.explain_diff_sql(name)`. |
| `pgtrickle.explain_query_rewrite()` | `pgtrickle` | `SetOf row` | # SQL usage ```sql SELECT * FROM pgtrickle.explain_query_rewrite(   'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id' ); ```. |
| `pgtrickle.explain_refresh_mode()` | `pgtrickle` | `SetOf row` | Example: ```sql SELECT * FROM pgtrickle.explain_refresh_mode('public.orders_summary'); ```. |
| `pgtrickle.explain_st()` | `pgtrickle` | `` | PERF-3: When `with_analyze` is true, the defining query is EXPLAINed with ANALYZE to show actual row counts, timings, and buffer usage. |
| `pgtrickle.explain_stream_table()` | `pgtrickle` | `text` | v0.39.0 extends the output to include: - Explicit DIFF/FULL fallback reason from the stream table catalog - Whether `force_full_refresh` GUC is overriding the mode - The effective refresh mode from the last completed refresh cycle - Whether the backpressure or CDC-pause state is active. |
| `pgtrickle.export_definition()` | `pgtrickle` | `text` | Returns a `DROP STREAM TABLE IF EXISTS` + `CREATE STREAM TABLE . |
| `pgtrickle.fuse_status()` | `pgtrickle` | `SetOf row` | Returns one row per stream table with fuse configuration and state. |
| `pgtrickle.gate_source()` | `pgtrickle` | `void` | `source` is the source table name, optionally schema-qualified. |
| `pgtrickle.get_refresh_history()` | `pgtrickle` | `` | Exposed as `pgtrickle.get_refresh_history(name, limit)`. |
| `pgtrickle.get_staleness()` | `pgtrickle` | `double precision (nullable)` |  |
| `pgtrickle.handle_vp_promoted()` | `pgtrickle` | `boolean` | Returns `true` if the payload was valid and a matching source was found; `false` if the payload was invalid or no source matched. |
| `pgtrickle.health_check()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.health_check()`. v0.80.0 (O-1/O-2): Two new check rows — `dvm_fallbacks` (WARN if DVM fallback refreshes in last hour) and `ring_overflow_trend` (WARN if DDL invalidation ring has overflowed). |
| `pgtrickle.health_summary()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.health_summary()`. |
| `pgtrickle.history_prune_status()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.history_prune_status()`. |
| `pgtrickle.is_drained()` | `pgtrickle` | `boolean` | A scheduler is considered drained when `DRAIN_COMPLETED >= DRAIN_REQUESTED` in shared memory. |
| `pgtrickle.list_auxiliary_columns()` | `pgtrickle` | `SetOf row` | # SQL usage ```sql SELECT * FROM pgtrickle.list_auxiliary_columns('my_stream_table'); ```. |
| `pgtrickle.list_distance_subscriptions()` | `pgtrickle` | `` | When `p_stream_table` is provided (e.g. |
| `pgtrickle.list_snapshots()` | `pgtrickle` | `SetOf row` | Returns one row per snapshot ordered by creation time descending. |
| `pgtrickle.list_sources()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.list_sources(name)`. |
| `pgtrickle.list_subscriptions()` | `pgtrickle` | `SetOf row` | Returns a table with columns (stream_table TEXT, channel TEXT, created_at TIMESTAMPTZ). |
| `pgtrickle.metrics_summary()` | `pgtrickle` | `SetOf row` | v0.31.0 (PERF-3): Added `ivm_lock_parse_error_count` — cumulative count of IMMEDIATE-mode lock-mode downgrades due to query parse failures. v0.80.0 (O-3): Two new columns — `cleanup_backlog_count` (total deferred cleanup entries) and `cleanup_blocked_count` (blocked cleanup entries). |
| `pgtrickle.migrate()` | `pgtrickle` | `text` | This is a convenience function for users who upgrade the extension without using `ALTER EXTENSION pg_trickle UPDATE` — it ensures the catalog schema matches the library expectations. |
| `pgtrickle.parallel_job_status()` | `pgtrickle` | `` | Exposed as `pgtrickle.parallel_job_status(max_age_seconds)`. |
| `pgtrickle.parse_duration_seconds()` | `pgtrickle` | `bigint (nullable)` | Used by SQL views to compare schedule. |
| `pgtrickle.pause_scheduler()` | `pgtrickle` | `text` | Example: ```sql SELECT pgtrickle.pause_scheduler(ARRAY['public.my_view', 'analytics.summary']); ```. |
| `pgtrickle.pause_stream_table()` | `pgtrickle` | `` | # Example ```sql SELECT pgtrickle.pause_stream_table('my_schema.my_st'); SELECT pgtrickle.resume_stream_table('my_schema.my_st'); ```. |
| `pgtrickle.pg_trickle_hash()` | `pgtrickle` | `bigint` | NULL input is mapped to a deterministic sentinel (`\x00NULL\x00`) — the same encoding used by [`pg_trickle_hash_multi`] — so that rows with NULL-valued group keys receive a non-NULL `__pgt_row_id`. |
| `pgtrickle.pg_trickle_hash_multi()` | `pgtrickle` | `bigint` | The hash output is identical to the previous xxh64-based implementation **except** that it now uses xxh3 which produces different numeric values. |
| `pgtrickle.pgt_ivm_apply_delta()` | `pgtrickle` | `void` | Delta SQL templates are cached per (pgt_id, source_oid, has_new, has_old) to avoid re-parsing the defining query on every trigger invocation. |
| `pgtrickle.pgt_ivm_apply_delta_enr()` | `pgtrickle` | `void` | Requires PostgreSQL 18+ which propagates ENRs to nested SPI calls within trigger execution contexts. |
| `pgtrickle.pgt_ivm_handle_truncate()` | `pgtrickle` | `void` | Truncates the stream table (equivalent to a full refresh with empty base table for simple views). |
| `pgtrickle.pgt_scc_status()` | `pgtrickle` | `SetOf row` | Returns one row per SCC, summarising its members, most recent fixpoint iteration count, and last convergence time. |
| `pgtrickle.pgt_status()` | `pgtrickle` | `SetOf row` | Returns a summary row per stream table including schedule configuration, data timestamp, and computed staleness interval. |
| `pgtrickle.pgtrickle_refresh_stats()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.pgtrickle_refresh_stats()`. |
| `pgtrickle.preflight()` | `pgtrickle` | `text` | Returns a JSON string with one entry per check: `pass` (bool), `check` (name), `detail` (human-readable message). |
| `pgtrickle.rebuild_cdc_triggers()` | `pgtrickle` | `text` | Returns `'done'` on success. |
| `pgtrickle.recommend_refresh_mode()` | `pgtrickle` | `` | Read-only — no side effects. |
| `pgtrickle.recommend_schedule()` | `pgtrickle` | `jsonb` | PLAN-1 (v0.27.0): Return a schedule recommendation for the given stream table as a JSONB object with keys: `recommended_interval_seconds`, `peak_window_cron`, `confidence` (0–1), `reasoning`. |
| `pgtrickle.refresh_efficiency()` | `pgtrickle` | `SetOf row (failable)` | Returns operational metrics for each stream table: FULL vs DIFFERENTIAL timing, change ratios, speedup factor, and refresh counts. |
| `pgtrickle.refresh_groups()` | `pgtrickle` | `SetOf row` | Return all user-declared refresh groups with member details. |
| `pgtrickle.refresh_stream_table()` | `pgtrickle` | `` | Manually trigger a synchronous refresh of a stream table. |
| `pgtrickle.refresh_timeline()` | `pgtrickle` | `` | Exposed as `pgtrickle.refresh_timeline(limit)`. |
| `pgtrickle.reliability_counters()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.reliability_counters()`. |
| `pgtrickle.repair_stream_table()` | `pgtrickle` | `text` | Steps performed (actions taken are summarized in the return text): 1. |
| `pgtrickle.reset_fuse()` | `pgtrickle` | `` | Returns nothing on success; raises an ERROR if the stream table does not exist or the fuse is not blown. |
| `pgtrickle.restore_from_snapshot()` | `pgtrickle` | `` | The stream table must already be registered. |
| `pgtrickle.restore_stream_tables()` | `pgtrickle` | `void` | During a `pg_restore`, `pg_dump` will restore the base storage tables and the `pgtrickle.pgt_stream_tables` catalog, but the necessary CDC triggers and internal wiring will be missing. |
| `pgtrickle.resume_scheduler()` | `pgtrickle` | `text` | Example: ```sql SELECT pgtrickle.resume_scheduler(ARRAY['public.my_view']); ```. |
| `pgtrickle.resume_stream_table()` | `pgtrickle` | `` | Resume a suspended stream table, clearing its consecutive error count and re-enabling automated and manual refreshes. |
| `pgtrickle.schedule_recommendations()` | `pgtrickle` | `SetOf row` | PLAN-2 (v0.27.0): Return one schedule recommendation row per registered stream table, sortable by `delta_pct DESC`. |
| `pgtrickle.scheduler_overhead()` | `pgtrickle` | `SetOf row` | Computes busy-time ratio, queue depth, avg dispatch latency, and the fraction of CPU spent on self-monitoring STs vs user STs from refresh history. |
| `pgtrickle.self_monitoring_status()` | `pgtrickle` | `SetOf row` | For each of the five expected DF stream tables, reports whether it exists, its current status, refresh mode, and last refresh time. |
| `pgtrickle.set_stream_table_refresh_policy()` | `pgtrickle` | `` | # Example ```sql SELECT pgtrickle.set_stream_table_refresh_policy('my_schema.my_st', 'DIFFERENTIAL'); ```. |
| `pgtrickle.set_stream_table_sla()` | `pgtrickle` | `` | Accepts an interval and stores it as `freshness_deadline_ms`. |
| `pgtrickle.set_stream_table_storage_policy()` | `pgtrickle` | `` | # Example ```sql SELECT pgtrickle.set_stream_table_storage_policy('my_schema.my_st', true, 'hot'); ```. |
| `pgtrickle.setup_self_monitoring()` | `pgtrickle` | `` | UX-2: Emits a warm-up hint if `pgt_refresh_history` has fewer than 50 rows. |
| `pgtrickle.shared_buffer_stats()` | `pgtrickle` | `SetOf row` | Example: ```sql SELECT * FROM pgtrickle.shared_buffer_stats(); ```. |
| `pgtrickle.sla_summary()` | `pgtrickle` | `SetOf row` | Returns per-stream-table statistics: p50/p99 refresh latency, freshness lag, error rate, and remaining error budget. |
| `pgtrickle.slot_health()` | `pgtrickle` | `SetOf row` | Returns trigger/slot name, source table, active status, retained WAL bytes, and the CDC mode (`trigger`, `wal`, or `transitioning`). |
| `pgtrickle.snapshot_stream_table()` | `pgtrickle` | `` | The snapshot table is created in the `pgtrickle` schema with the naming convention `snapshot_<name>_<epoch_ms>` unless `p_target` is given. |
| `pgtrickle.source_gates()` | `pgtrickle` | `SetOf row` | Only rows that have ever been gated appear in this view (one row per source_relid in `pgt_source_gates`). |
| `pgtrickle.source_stable_name()` | `pgtrickle` | `text (nullable)` | Returns `NULL` when the relation no longer exists (e.g. |
| `pgtrickle.st_auto_threshold()` | `pgtrickle` | `double precision (nullable)` | Returns the per-ST `auto_threshold` if set, otherwise the global `pg_trickle.differential_max_change_ratio` GUC. |
| `pgtrickle.st_refresh_stats()` | `pgtrickle` | `SetOf row` | This is the primary monitoring function, exposed as `pgtrickle.st_refresh_stats()`. |
| `pgtrickle.stream_table_lineage()` | `pgtrickle` | `SetOf row` | # Example ```sql SELECT * FROM pgtrickle.stream_table_lineage('public.revenue_summary'); ```. |
| `pgtrickle.stream_table_spec()` | `pgtrickle` | `jsonb (nullable)` | Example: ```sql SELECT pgtrickle.stream_table_spec('public.my_view'::regclass); ```. |
| `pgtrickle.stream_table_spec()` | `pgtrickle` | `jsonb (nullable)` | Example: ```sql SELECT pgtrickle.stream_table_spec('public.my_view'); ```. |
| `pgtrickle.stream_table_to_publication()` | `pgtrickle` | `` | Creates a PostgreSQL publication exposing the named stream table so that Kafka Connect, Debezium, and other logical replication subscribers can receive change events without a separate replication slot. |
| `pgtrickle.subscribe()` | `pgtrickle` | `void` | The subscription is stored in `pgtrickle.pgt_subscriptions` and survives restarts. |
| `pgtrickle.subscribe_distance()` | `pgtrickle` | `void` | The subscription is stored in `pgtrickle.pgt_distance_subscriptions` and survives restarts. |
| `pgtrickle.teardown_self_monitoring()` | `pgtrickle` | `` | Safe with partial setups: each table is dropped individually, and missing tables are silently skipped (STAB-5). |
| `pgtrickle.trigger_inventory()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.trigger_inventory()`. |
| `pgtrickle.ungate_source()` | `pgtrickle` | `void` | `source` is the source table name, optionally schema-qualified. |
| `pgtrickle.unsubscribe()` | `pgtrickle` | `void` | UX-SUB: Remove a NOTIFY subscription for a stream table / channel pair. |
| `pgtrickle.unsubscribe_distance()` | `pgtrickle` | `void` | VH-2 (v0.48.0): Remove a distance-predicate subscription. |
| `pgtrickle.validate_query()` | `pgtrickle` | `SetOf row` | # SQL usage ```sql SELECT * FROM pgtrickle.validate_query(   'SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id' ); ```. |
| `pgtrickle.vector_status()` | `pgtrickle` | `SetOf row` | Returns one row per stream table that has a `post_refresh_action` other than 'none', or that has any ANN-relevant index on its storage table. |
| `pgtrickle.version()` | `pgtrickle` | `text` |  |
| `pgtrickle.version_check()` | `pgtrickle` | `text` | Returns a JSON string with library_version, extension_version, pg_version, and a boolean `version_match`. |
| `pgtrickle.view_evolution_status()` | `pgtrickle` | `SetOf row` | During a zero-downtime schema evolution (ALTER STREAM TABLE), pg_trickle builds the new definition in a shadow table. |
| `pgtrickle.wal_source_status()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.wal_source_status()`. |
| `pgtrickle.watermark_groups()` | `pgtrickle` | `SetOf row` | Return all watermark group definitions. |
| `pgtrickle.watermark_status()` | `pgtrickle` | `SetOf row` | Shows per-group lag, whether the group is currently aligned, and the effective minimum watermark. |
| `pgtrickle.watermarks()` | `pgtrickle` | `SetOf row` | Return the current watermark state for all registered sources. |
| `pgtrickle.worker_allocation_status()` | `pgtrickle` | `SetOf row` | Columns: - `db_name`: The current database name. |
| `pgtrickle.worker_pool_status()` | `pgtrickle` | `SetOf row` | Exposed as `pgtrickle.worker_pool_status()`. |
| `pgtrickle.write_and_refresh()` | `pgtrickle` | `` | Calling `pgtrickle.write_and_refresh(sql, name)` guarantees the refresh sees the writes from `sql` because both run in the same transaction. |
