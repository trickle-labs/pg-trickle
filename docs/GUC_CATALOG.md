<!-- AUTO-GENERATED — do not edit by hand.
     Run `python3 scripts/gen_catalogs.py` to regenerate.
     CI fails if this file is out of date with source code. -->

# GUC Reference — pg_trickle

**122 configuration parameters** extracted from `src/config.rs`.

See [docs/CONFIGURATION.md](CONFIGURATION.md) for full descriptions and usage examples.


| GUC name | Type | Default | Description |
|----------|------|---------|-------------|
| `pg_trickle.adaptive_batch_coalescing` | `bool` | `true` | Disable if the batched query plan is unexpectedly slow (rare). |
| `pg_trickle.adaptive_merge_strategy` | `bool` | `false` | Default `false` — the fixed `merge_strategy` GUC governs. |
| `pg_trickle.agg_diff_cardinality_threshold` | `int4` | `1000` | Set to 0 to disable the cardinality warning. |
| `pg_trickle.aggregate_fast_path` | `bool` | `true` | B-1: Aggregate fast-path — use explicit DML instead of MERGE for GROUP BY queries where all aggregates are algebraically invertible (COUNT, SUM, AVG, etc.). |
| `pg_trickle.algebraic_drift_reset_cycles` | `int4` | `0` | Set to 0 to disable periodic drift reset (default). |
| `pg_trickle.allow_circular` | `bool` | `false` | When `false` (default), cycle detection rejects any stream table creation that would introduce a cycle in the dependency graph. |
| `pg_trickle.analyze_before_delta` | `bool` | `true` | When enabled, `ANALYZE pgtrickle_changes.changes_<oid>` is run before the delta SQL is executed. |
| `pg_trickle.auto_backoff` | `bool` | `true` | This prevents CPU runaway when a stream table's refresh cost exceeds its schedule budget and an operator is not available to respond manually. |
| `pg_trickle.auto_index` | `bool` | `true` | When enabled, `create_stream_table()` automatically creates indexes on GROUP BY keys, DISTINCT columns, and adds INCLUDE clauses to the `__pgt_row_id` index for stream tables with ≤ 8 output columns. |
| `pg_trickle.backpressure_consecutive_limit` | `int4` | `3` | Default: 3 cycles. |
| `pg_trickle.block_source_ddl` | `bool` | `true` | Default is `true` (enabled) as of v0.11.0 — set to `false` to restore the previous permissive behavior (DDL triggers reinitialization instead of blocking). |
| `pg_trickle.buffer_alert_threshold` | `int4` | `1000000` | When any source table's change buffer exceeds this number of rows, a `BufferGrowthWarning` alert is emitted. |
| `pg_trickle.buffer_partitioning` | `text` | `"off"` | Controls whether change buffer tables use `PARTITION BY RANGE (lsn)`: - `"off"` (default): Unpartitioned heap tables (current behaviour). |
| `pg_trickle.cdc_capture_mode` | `text` | `"discard"` | Use `pgtrickle.cdc_capture_mode()` to inspect the active mode at runtime. |
| `pg_trickle.cdc_mode` | `text` | `"auto"` | - `"auto"` (default): Use triggers for creation, transition to WAL if   `wal_level = logical` is available. |
| `pg_trickle.cdc_paused` | `bool` | `false` | Default: `false` (CDC writes are enabled). |
| `pg_trickle.cdc_trigger_mode` | `text` | `"statement"` | Changing this GUC takes effect for newly created stream tables. |
| `pg_trickle.change_buffer_durability` | `text` | `"logged"` | This GUC supersedes `pg_trickle.unlogged_buffers` (which is now a compatibility alias: `true` maps to `"unlogged"`, `false` to `"logged"`). |
| `pg_trickle.change_buffer_schema` | `text` | `"pgtrickle_changes"` | Schema name for change buffer tables. |
| `pg_trickle.citus_st_lock_lease_ms` | `int4` | `60000` | Default: 60 000 ms (60 seconds). |
| `pg_trickle.citus_worker_retry_ticks` | `int4` | `5` | Default: 5 ticks. |
| `pg_trickle.cleanup_use_truncate` | `bool` | `true` | Set to false if the TRUNCATE AccessExclusiveLock on the change buffer is problematic for concurrent DML on the source table. |
| `pg_trickle.columnar_backend` | `text` | `"none"` | When set, `create_stream_table()` uses the specified columnar backend and routes differential refresh to the `delete_insert` strategy (columnar backends are append-only). |
| `pg_trickle.compact_threshold` | `int4` | `100000` | Set to 0 to disable compaction. |
| `pg_trickle.connection_pooler_mode` | `text` | `"off"` | Overrides the per-ST `pooler_compatibility_mode` for all stream tables. |
| `pg_trickle.cost_cache_capacity` | `int4` | `256` | Default: 256. |
| `pg_trickle.cost_model_safety_margin` | `float8` | `0.8` | Default 0.8 — DIFFERENTIAL is chosen unless it's estimated to cost more than 80% of FULL. |
| `pg_trickle.deep_join_l0_scan_threshold` | `int4` | `4` | Default: 4 (matches the previously hardcoded `DEEP_JOIN_L0_SCAN_THRESHOLD`). |
| `pg_trickle.default_schedule_seconds` | `int4` | `1` | Default: 1 s. |
| `pg_trickle.delta_amplification_threshold` | `float8` | `100.0` | Set to 0.0 to disable amplification detection. |
| `pg_trickle.delta_enable_nestloop` | `bool` | `true` | When enabled, `SET LOCAL enable_nestloop = off` is applied inside `execute_delta_sql` before running the generated delta SQL. |
| `pg_trickle.delta_work_mem` | `int4` | `0` | Set to 0 (default) to inherit the session `work_mem`. |
| `pg_trickle.delta_work_mem_cap_mb` | `int4` | `256` | PERF-004 (v0.70.0): Default changed from 0 (disabled) to 256 MB. |
| `pg_trickle.diff_output_format` | `text` | `"split"` | Controls how the DI-2 aggregate UPDATE-split surfaces changes: - `"split"` (default): Emit DELETE+INSERT pairs for aggregate UPDATEs. |
| `pg_trickle.differential_max_change_ratio` | `float8` | `0.15` | Set to 0.0 to disable adaptive fallback (always use DIFFERENTIAL). |
| `pg_trickle.drain_timeout` | `int4` | `60` | Default: 60 seconds. |
| `pg_trickle.enable_change_buffer_fanout` | `bool` | `true` | Disable only if the shared cache is producing incorrect change-detection results (should not occur in practice). |
| `pg_trickle.enable_fused_refresh` | `bool` | `true` | Disable if a specific DAG shape causes unexpected planner behaviour. |
| `pg_trickle.enable_trace_propagation` | `bool` | `false` | F10 (v0.37.0): Enable W3C Trace Context propagation through the refresh pipeline. |
| `pg_trickle.enable_vector_agg` | `bool` | `false` | F4 (v0.37.0): Enable pgVectorMV — incremental vector aggregate operators. |
| `pg_trickle.enabled` | `bool` | `true` | Master enable/disable switch for the extension. |
| `pg_trickle.enforce_backpressure` | `bool` | `false` | Default: `false` (alerts only, no throttling). |
| `pg_trickle.force_full_refresh` | `bool` | `false` | Useful for SRE diagnosis when a cluster-wide `refresh_strategy = 'full'` still has DIFFERENTIAL STs due to explicit per-ST row values. |
| `pg_trickle.foreign_table_polling` | `bool` | `false` | When enabled, foreign tables used in DIFFERENTIAL / IMMEDIATE mode defining queries will be supported via a snapshot-comparison approach: before each refresh cycle the scheduler materializes a snapshot of the foreign table into a local shadow table, then computes EXCEPT ALL deltas against the previous snapshot. |
| `pg_trickle.frontier_holdback_mode` | `text` | `"xmin"` | \| Value \| Meaning \| \|-------\|---------\| \| `"xmin"` (default) \| Probe `pg_stat_activity` + `pg_prepared_xacts` once per tick and cap the frontier to the safe upper bound. |
| `pg_trickle.frontier_holdback_probe_cache_ms` | `int4` | `250` | Set to 0 to disable caching and probe on every scheduler tick. |
| `pg_trickle.frontier_holdback_warn_seconds` | `int4` | `60` | Set to 0 to disable the warning (not recommended for production). |
| `pg_trickle.fuse_default_ceiling` | `int4` | `0` | Set to 0 to disable the global default ceiling (per-ST ceiling only). |
| `pg_trickle.fused_refresh_max_delta_rows` | `int4` | `500000` | Default: 500 000. |
| `pg_trickle.history_prune_interval_seconds` | `int4` | `60` | Default: 60 seconds. |
| `pg_trickle.history_retention_days` | `int4` | `90` | The scheduler runs a daily cleanup that deletes rows from `pgtrickle.pgt_refresh_history` older than this many days. |
| `pg_trickle.invalidation_ring_capacity` | `int4` | `1024` | Default: 1024. |
| `pg_trickle.ivm_recursive_max_depth` | `int4` | `100` | Set to 0 to disable the depth guard (allow unlimited recursion). |
| `pg_trickle.ivm_topk_max_limit` | `int4` | `1000` | TopK queries with `LIMIT > threshold` are rejected in IMMEDIATE mode because inline recomputation of large result sets adds unacceptable latency to the trigger path. |
| `pg_trickle.ivm_use_enr` | `bool` | `false` | When false, the legacy temp-table copy behaviour is used. |
| `pg_trickle.lag_aware_scheduling` | `bool` | `false` | Off by default — use static quotas. |
| `pg_trickle.log_delta_sql` | `bool` | `false` | **Do not enable in production** — every refresh will emit potentially large SQL strings to the server log. |
| `pg_trickle.log_format` | `text` | `"text"` | - `"text"` (default): Unstructured human-readable messages via `pgrx::log!()`. |
| `pg_trickle.log_merge_sql` | `bool` | `false` | Intended for debugging MERGE query generation only. |
| `pg_trickle.matview_polling` | `bool` | `false` | When `true`, materialized views referenced in DIFFERENTIAL/IMMEDIATE defining queries will be supported via a snapshot-comparison approach (same mechanism as foreign table polling). |
| `pg_trickle.max_buffer_rows` | `int4` | `1000000` | Set to 0 to disable the limit. |
| `pg_trickle.max_change_buffer_alert_rows` | `int4` | `0` | Set to 0 to disable (default). |
| `pg_trickle.max_concurrent_refreshes` | `int4` | `4` | Default: 4. |
| `pg_trickle.max_consecutive_errors` | `int4` | `3` | Default: 3. |
| `pg_trickle.max_delta_estimate_rows` | `int4` | `0` | Set to 0 to disable the estimation check (default). |
| `pg_trickle.max_diff_ctes` | `int4` | `1000` | Complex queries with many operators, joins, and set operations can produce hundreds of CTEs. |
| `pg_trickle.max_dynamic_refresh_workers` | `int4` | `4` | This is distinct from `pg_trickle.max_concurrent_refreshes`, which is the per-database dispatch cap. |
| `pg_trickle.max_fixpoint_iterations` | `int4` | `100` | When stream tables form a cyclic dependency (circular reference), the scheduler iterates to a fixed point. |
| `pg_trickle.max_grouping_set_branches` | `int4` | `64` | Maximum allowed grouping set branches for CUBE/ROLLUP expansion (EC-02). |
| `pg_trickle.max_parallel_workers` | `int4` | `0` | Default 0 = serial mode (existing behavior preserved). |
| `pg_trickle.max_parse_depth` | `int4` | `64` | Prevents stack-overflow crashes on pathological queries with deeply nested subqueries, CTEs, or set operations. |
| `pg_trickle.max_parse_nodes` | `int4` | `0` | Queries that exceed this limit are rejected with `QueryTooComplex` to prevent unbounded memory allocation in the parse advisory warnings cache and CTE registry. |
| `pg_trickle.merge_join_strategy` | `text` | `"auto"` | Controls the join strategy hint applied via `SET LOCAL` during MERGE: - `"auto"` (default): delta-size heuristics choose the strategy. |
| `pg_trickle.merge_planner_hints` | `bool` | `true` | Deprecated — use `pg_trickle.planner_aggressive` instead. |
| `pg_trickle.merge_seqscan_threshold` | `float8` | `0.001` | Set to 0.0 to disable this optimization. |
| `pg_trickle.merge_strategy` | `text` | `"auto"` | The former `"delete_insert"` value was removed in v0.19.0 (CORR-1). |
| `pg_trickle.merge_strategy_threshold` | `float8` | `0.01` | Default: 0.01 (1%). |
| `pg_trickle.merge_work_mem_mb` | `int4` | `64` | A higher value lets PostgreSQL use larger hash tables for the MERGE join, avoiding disk-spilling sort/merge strategies on large deltas. |
| `pg_trickle.metrics_port` | `int4` | `0` | Example: ```sql ALTER SYSTEM SET pg_trickle.metrics_port = 9188; SELECT pg_reload_conf(); ```. |
| `pg_trickle.metrics_request_timeout_ms` | `int4` | `5000` | Protects the scheduler from a slow client stalling the tick loop. |
| `pg_trickle.min_schedule_seconds` | `int4` | `1` | Default: 1 s. |
| `pg_trickle.notify_coalesce_ms` | `int4` | `250` | Default: 250 ms. |
| `pg_trickle.online_schema_evolution` | `bool` | `false` | Default: `false` (standard ALTER QUERY reinit behaviour). |
| `pg_trickle.otel_endpoint` | `text` | `None` | F10 (v0.37.0): OTLP/gRPC endpoint for OpenTelemetry span export. |
| `pg_trickle.parallel_refresh_mode` | `text` | `"on"` | - `"on"` (default as of v0.11.0): Enable true parallel refresh via   dynamic workers. |
| `pg_trickle.part3_max_scan_count` | `int4` | `5` | Default: 5 (matches the previously hardcoded `PART3_MAX_SCAN_COUNT`). |
| `pg_trickle.per_database_worker_quota` | `int4` | `0` | Set to 0 (default) to disable per-database quotas — all databases share `max_dynamic_refresh_workers` on a first-come-first-served basis, bounded per coordinator by `max_concurrent_refreshes`. |
| `pg_trickle.planner_aggressive` | `bool` | `true` | Replaces the old `merge_planner_hints` and `merge_work_mem_mb` GUCs (both still accepted but emit deprecation warnings). |
| `pg_trickle.prediction_min_samples` | `int4` | `5` | When fewer than this many data points exist, the predictor falls back to the existing fixed-threshold logic. |
| `pg_trickle.prediction_ratio` | `float8` | `1.5` | When `predicted_diff_ms > last_full_ms × prediction_ratio`, the scheduler overrides the strategy to FULL refresh. |
| `pg_trickle.prediction_window` | `int4` | `60` | The forecaster fits `duration_ms ~ delta_rows` over this many minutes of `pgt_refresh_history` data per stream table. |
| `pg_trickle.publication_lag_warn_bytes` | `int4` | `0` | Set to 0 to disable subscriber lag tracking (default). |
| `pg_trickle.refresh_strategy` | `text` | `"auto"` | This GUC is a cluster-wide override. |
| `pg_trickle.reindex_drift_threshold` | `float8` | `0.20` | Default: 0.20. |
| `pg_trickle.schedule_alert_cooldown_seconds` | `int4` | `300` | Prevents alert spam when the cost model consistently predicts SLA breach. |
| `pg_trickle.schedule_recommendation_min_samples` | `int4` | `20` | When fewer samples are available, `confidence` is returned as 0.0 and the recommendation fields are NULL or conservative defaults. |
| `pg_trickle.scheduler_drain_timeout` | `int4` | `30` | Default: 30 seconds. |
| `pg_trickle.scheduler_interval_ms` | `int4` | `1000` | Default: 1,000 ms (1 s). |
| `pg_trickle.self_monitoring_auto_apply` | `text` | `"off"` | Automatic application mode for self-monitoring stream tables. |
| `pg_trickle.sla_window_hours` | `int4` | `24` | Default: 24 hours. |
| `pg_trickle.slot_lag_critical_threshold_mb` | `int4` | `1024` | When a WAL-mode source retains more than this amount of WAL, `pgtrickle.check_cdc_health()` reports a `slot_lag_exceeds_threshold` alert for the source. |
| `pg_trickle.slot_lag_warning_threshold_mb` | `int4` | `100` | When a WAL-mode source retains more than this amount of WAL, pg_trickle: - emits a `slot_lag_warning` NOTIFY event from the scheduler, and - reports a WARN row in `pgtrickle.health_check()`. |
| `pg_trickle.spill_consecutive_limit` | `int4` | `3` | When a stream table accumulates this many consecutive differential refreshes where `temp_blks_written > spill_threshold_blocks`, the scheduler marks the ST for reinitialization (FULL refresh) on the next cycle. |
| `pg_trickle.spill_threshold_blocks` | `int4` | `0` | Set to 0 to disable spill detection (default). |
| `pg_trickle.template_cache` | `bool` | `true` | In transaction-pooling mode, rely on L2 rather than L0 warm-up for cross-connection performance. |
| `pg_trickle.template_cache_max_age_hours` | `int4` | `168` | Prevents stale entries accumulating after ALTER QUERY without DROP or source-OID renumbering. |
| `pg_trickle.template_cache_max_bytes` | `int4` | `0` | Set to 0 to disable byte-based eviction and rely only on `template_cache_max_entries`. |
| `pg_trickle.template_cache_max_entries` | `int4` | `0` | When the cache reaches this size, the least-recently-used entry is evicted. |
| `pg_trickle.temporal_stream_tables` | `bool` | `false` | Default: `false` (standard non-temporal storage). |
| `pg_trickle.tick_watermark_enabled` | `bool` | `true` | Disable only if you need stream tables to always advance to the very latest available LSN regardless of cross-source consistency. |
| `pg_trickle.tiered_scheduling` | `bool` | `true` | Default changed to `true` in v0.12.0 (PERF-3) — prevents large deployments from wasting CPU refreshing cold STs at full speed. |
| `pg_trickle.trace_id` | `text` | `None` | F10 (v0.37.0): Session-level W3C traceparent header for trace context propagation. |
| `pg_trickle.unlogged_buffers` | `bool` | `false` | **Deprecated (COR-003/ARCH-001, v0.68.0):** Use `pg_trickle.change_buffer_durability` instead. |
| `pg_trickle.use_prepared_statements` | `bool` | `true` | Disable if prepared-statement parameter sniffing produces poor plans (e.g., highly skewed LSN distributions). |
| `pg_trickle.use_sqlstate_classification` | `bool` | `true` | The SQLSTATE-based classification is locale-safe: it works correctly regardless of `lc_messages`. |
| `pg_trickle.user_triggers` | `text` | `"auto"` | - `"auto"` (default): Detect user-defined row-level triggers on the   stream table and automatically use explicit DML (DELETE + UPDATE +   INSERT) so triggers fire with correct `TG_OP`, `OLD`, and `NEW`. |
| `pg_trickle.volatile_function_policy` | `text` | `"reject"` | Controls how volatile functions in defining queries are handled: - `"reject"` (default): Error — volatile functions are rejected. |
| `pg_trickle.wal_max_changes_per_poll` | `int4` | `10000` | Default: 10 000. |
| `pg_trickle.wal_max_lag_bytes` | `int4` | `65536` | Default: 65 536 (64 KiB). |
| `pg_trickle.wal_transition_timeout` | `int4` | `300` | Maximum time (seconds) to wait for the WAL decoder to catch up during transition from triggers to WAL-based CDC before falling back to triggers. |
| `pg_trickle.watermark_holdback_timeout` | `int4` | `0` | Set to 0 to disable stuck-watermark detection (default). |
| `pg_trickle.worker_pool_size` | `int4` | `0` | Set to 0 (default) to use the existing spawn-per-task model. |
