# pg_trickle Blog

> **Note:** This blog directory is an experiment. All posts were generated with
> AI assistance (GitHub Copilot / Claude) as a way to explore how well
> LLM-generated technical writing holds up for a niche systems engineering
> topic. The technical content has been reviewed for accuracy, but treat the
> posts as drafts — not as officially reviewed documentation. The blog is meant
> for informative purposes — to learn about interesting topics in the context of
> pg_trickle. It is a showcase for use-cases rather than a definitive
> reference; for authoritative documentation see the
> [pg_trickle documentation](https://trickle-labs.github.io/pg-trickle/).

---

## Posts

### Core Concepts & Theory

| Post | Summary |
|------|---------|
| [Why Your Materialized Views Are Always Stale](stale-materialized-views.md) | Explains why `REFRESH MATERIALIZED VIEW` fails at scale — locking, cost, and the full-scan ceiling — and how switching to a stream table with `DIFFERENTIAL` mode fixes staleness in 5 lines of SQL. |
| [Differential Dataflow for the Rest of Us](differential-dataflow-explained.md) | A plain-language walkthrough of the mathematics behind incremental view maintenance: delta rules for filters, joins, aggregates, the MERGE application step, and why some aggregates (MEDIAN, RANK) can't be made incremental. |
| [Incremental Aggregates in PostgreSQL: No ETL Required](incremental-aggregates-no-etl.md) | How `SUM`, `COUNT`, `AVG`, and (in v0.37) `vector_avg` are maintained as running algebraic state rather than full scans. Covers multi-table aggregates, conditional aggregates, and the non-differentiable cases. |
| [The Z-Set: The Data Structure That Makes IVM Correct](z-set-data-structure.md) | A concrete tour of the integer-weighted multiset that underlies pg_trickle's differential engine — how inserts are +1, deletes are -1, updates are both, and why commutativity eliminates an entire class of ordering bugs. |
| [The Cost Model: How pg_trickle Decides Whether to Refresh Differentially](cost-model-auto-mode.md) | Inside AUTO mode: the decision inputs (delta ratio, query complexity, historical timings), the learned cost model, and when the engine switches between DIFFERENTIAL and FULL refresh mid-flight. |

### SQL Operator Deep Dives

| Post | Summary |
|------|---------|
| [Recursive CTEs That Update Themselves](recursive-ctes-that-update-themselves.md) | Semi-naive evaluation for insert-only tables and Delete-and-Rederive for mixed DML — how pg_trickle maintains `WITH RECURSIVE` queries incrementally for org charts, BOMs, and graph reachability. |
| [Window Functions Without the Full Recompute](window-functions-without-full-recompute.md) | Partition-scoped recomputation for `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, and all standard window functions. Change one partition, leave the rest untouched. |
| [GROUPING SETS, ROLLUP, and CUBE — Incrementally](grouping-sets-rollup-cube.md) | Multi-dimensional aggregation decomposed into UNION ALL branches, each maintained with algebraic delta rules. Drill-down dashboards that refresh in milliseconds. |
| [EXISTS and NOT EXISTS: The Delta Rules Nobody Talks About](exists-not-exists-delta-rules.md) | Semi-joins and anti-joins maintained via reference counting on the join key. Delta-key pre-filtering, inverted semantics for NOT EXISTS, SubLink extraction from WHERE clauses. |
| [DISTINCT That Doesn't Recount](distinct-reference-counting.md) | Reference counting (`__pgt_dup_count`) for incremental deduplication. Insert increments, delete decrements, row removed when count hits zero. DISTINCT ON with tie-breaking. |
| [Scalar Subqueries in the SELECT List — Incrementally](scalar-subqueries.md) | Pre/post snapshot diff for correlated subqueries. Only groups affected by the delta are re-evaluated — O(affected groups), not O(all rows). |
| [LATERAL Joins in a Stream Table](lateral-joins.md) | Row-scoped re-execution for `JSON_TABLE`, `unnest()`, `generate_series()`, and correlated set-returning functions. Cost proportional to changed left-side rows. |
| [Set Operations Done Right: UNION, INTERSECT, EXCEPT](set-operations.md) | Dual-count multiplicity tracking for all set operations. UNION uses reference counting, INTERSECT requires both-side presence, EXCEPT removes when the right side gains a match. |

### Refresh Modes & Scheduling

| Post | Summary |
|------|---------|
| [IMMEDIATE Mode: When "Good Enough Freshness" Isn't Good Enough](immediate-mode-zero-lag.md) | Synchronous IVM inside the source transaction — zero lag, no background worker. Account balances, inventory tracking, and the trade-offs vs. DIFFERENTIAL mode. |
| [How pg_trickle Handles Diamond Dependencies](diamond-dependencies.md) | When two branches of a DAG share a source and converge downstream, naively refreshing can cause double-counting. How the frontier tracker and diamond-group scheduling ensure correctness. |
| [Temporal Stream Tables: Time-Windowed Views That Update Themselves](temporal-stream-tables.md) | The "last 7 days" problem — results that change because time passes, not because data changed. Sliding-window eviction, the `temporal_mode` parameter, and when fixed windows don't need it. |
| [Declare Freshness Once: CALCULATED Scheduling](calculated-scheduling.md) | Upstream tables derive their refresh cadence from downstream consumers. Set the SLA on the dashboard; the pipeline adjusts automatically. |
| [Cycles in Your Dependency Graph? That's Fine.](circular-dependencies.md) | Fixed-point iteration for monotone queries. `allow_circular = on`, SCC detection, convergence guarantees, the iteration limit, and when cycles are a legitimate design choice. |
| [Hot, Warm, Cold, Frozen: Tiered Scheduling at Scale](tiered-scheduling.md) | Automatic tier classification by change frequency. The scheduler checks hot tables every cycle, frozen tables every ~60 cycles — 80%+ overhead reduction at 500+ stream tables. |

### CDC & Change Tracking

| Post | Summary |
|------|---------|
| [The CDC Mode You Never Have to Choose](hybrid-cdc-mode.md) | Hybrid CDC starts with triggers, silently graduates to WAL. Three-step transition orchestration, automatic fallback on failure, WAL backpressure, and why AUTO is the right default. |
| [IVM Without Primary Keys](ivm-without-primary-keys.md) | Content-based hashing (`xxHash64`) generates synthetic row identity for keyless tables. Multiplicity counting for duplicates, collision probability, and when to add a PK anyway. |
| [Foreign Tables as Stream Table Sources](foreign-table-sources.md) | IVM over `postgres_fdw`, `file_fdw`, and `parquet_fdw` sources using polling-based change detection. Mixed local/foreign source queries, performance trade-offs, and the materialize-first optimization. |

### Architecture & Data Patterns

| Post | Summary |
|------|---------|
| [The Medallion Architecture Lives Inside PostgreSQL](medallion-architecture-postgresql.md) | Bronze/Silver/Gold without Spark or Airflow. Chained stream tables propagate from raw ingest to business aggregates in under 5 seconds, with DAG-aware scheduling and transactional consistency. |
| [CQRS Without a Second Database](cqrs-without-second-database.md) | Command Query Responsibility Segregation using stream tables as the read model — same PostgreSQL instance, no CDC pipeline, read-your-writes with IMMEDIATE mode. |
| [Slowly Changing Dimensions in Real Time](slowly-changing-dimensions.md) | SCD Type 2 (historical attribute tracking with `valid_from`/`valid_to`) maintained continuously by a stream table — no nightly ETL, no Airflow DAG. |
| [The Append-Only Fast Path](append-only-fast-path.md) | Why insert-only tables (event logs, sensor data, clickstreams) get a 2–3× faster refresh: no delete-side delta, no inverse computation, no before-image lookups. |

### Use Cases & Migration

| Post | Summary |
|------|---------|
| [Real-Time Leaderboards That Don't Lie](real-time-leaderboards.md) | Top-N stream tables for games, sales dashboards, and coding challenges — tied scores, multi-category boards, the pagination problem, and why you might not need Redis. |
| [The Hidden Cost of Trigger-Based Denormalization](trigger-denormalization-cost.md) | Four failure modes of hand-rolled trigger sync — blind UPDATE divergence, statement vs. row trigger semantics, invisible deletes, and multi-row races — and how declarative IVM avoids all of them. |
| [How We Replaced a Celery Pipeline with 3 SQL Statements](replaced-celery-with-sql.md) | A before/after case study of a Celery + Elasticsearch product search pipeline across three generations of growing complexity, and the pg_trickle stream table that replaced it. Includes benchmark numbers. |
| [Migrating from pg_ivm to pg_trickle](migrating-from-pg-ivm.md) | Feature gap table, SQL syntax differences, step-by-step migration procedure, and when staying on pg_ivm is the right call. |

### Integrations & Ecosystem

| Post | Summary |
|------|---------|
| [dbt + pg_trickle: The Analytics Engineer's Stack](dbt-analytics-stack.md) | The `pgtrickle` dbt materialization: continuously-fresh models that are also version-controlled, tested, and documented. DAG alignment, freshness checks, and mixing materializations. |
| [Distributed IVM with Citus](distributed-ivm-citus.md) | Incremental view maintenance across sharded PostgreSQL: per-worker CDC, shard-aware delta routing, co-located join push-down, and automatic recovery after shard rebalances. |
| [pg_trickle on CloudNativePG](pg-trickle-cloudnativepg-kubernetes.md) | Production Kubernetes deployment using the CloudNativePG operator: Dockerfile, Cluster manifest, GUC configuration, HA failover behaviour, Prometheus metrics ConfigMap, alerting rules, upgrade procedure, and sizing guidance. |
| [Making pg_trickle Work Through PgBouncer](pgbouncer-compatibility.md) | Connection pooling modes, the background-worker bypass, LISTEN/NOTIFY caveats in transaction mode, and a configuration checklist for PgBouncer + pg_trickle. |
| [Publishing Stream Tables via Logical Replication](logical-replication-publishing.md) | Stream tables as standard publication sources for downstream PostgreSQL instances. Replication identity, multi-region distribution, and feeding Debezium/Kafka with clean aggregated events. |
| [One PostgreSQL, Five Databases, One Worker Pool](multi-database.md) | Multi-database architecture: one launcher per server, one scheduler per database, shared worker pool with per-database quotas. Failure isolation and the database-per-tenant SaaS pattern. |

### DuckLake Integration

| Post | Summary |
|------|---------|
| [Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](ducklake-ivm-missing-piece.md) | DuckLake's catalog-in-SQL architecture makes pg_trickle a natural fit for lakehouse IVM — filling the gap DuckLake's own roadmap acknowledges. How incremental aggregations over Iceberg-formatted data work without recomputing from scratch. |
| [The Modern Data Stack in One Box](ducklake-modern-data-stack.md) | Replace Debezium + Kafka + Flink + Iceberg + Polaris + an orchestrator with one PostgreSQL instance and an S3 bucket — pg_trickle for incremental aggregations, DuckLake for lake storage. No JVM, no separate orchestration. |
| [Real-Time Dashboards on Your Data Lake](ducklake-real-time-dashboards.md) | End-to-end Grafana dashboard tutorial: DuckLake stores events in PostgreSQL, pg_trickle maintains per-minute revenue and purchase counts incrementally inside the same database, Grafana visualises live with no Kafka or Flink. |
| [Monitoring Your DuckLake with pg_trickle](ducklake-monitoring.md) | DuckLake's ~28 metadata tables record every snapshot, file, schema change, and compaction event. Build live alerts and pre-computed monitoring views over them with stream tables — turning expensive multi-table scans into instant reads. |
| [DuckLake's `table_changes()` Meets pg_trickle's DVM Engine](ducklake-table-changes-dvm.md) | Wire-level mapping of DuckLake's change-feed format (`insert`/`delete`/`update_preimage`/`update_postimage`) to pg_trickle's internal change-buffer model — and the Phase 2 DuckLake adapter design. |

### pgvector Integration

| Post | Summary |
|------|---------|
| [Your pgvector Index Is Lying to You](incremental-pgvector.md) | Four silent failure modes of unmanaged pgvector deployments: stale embedding corpora, drifting aggregates, IVFFlat recall loss, and over-fetching. How pg_trickle's differential IVM and drift-aware reindexing closes each gap. |
| [Incremental Vector Aggregates: Building Recommendation Engines in Pure SQL](incremental-vector-aggregates.md) | How `vector_avg` (v0.37) turns user taste vectors, category centroids, and cluster representatives into live algebraic aggregates — O(new interactions) cost, not O(history). Comparison with batch recomputation, feature stores, and application-level updates. |
| [Deploying RAG at Scale: pg_trickle as Your Embedding Infrastructure](deploying-rag-at-scale.md) | Production operations for pgvector + pg_trickle: drift-aware HNSW reindexing (`reindex_if_drift`), `vector_status()` monitoring, multi-tenant tiered indexing patterns, sparse/half-precision aggregates, reactive distance subscriptions, and the `embedding_stream_table()` ergonomic API. |
| [HNSW Recall Is a Lie: Distribution Drift Explained](hnsw-recall-distribution-drift.md) | Deep dive on IVFFlat centroid staleness and HNSW tombstone accumulation — how to measure drift, what the right threshold is, and how `post_refresh_action => 'reindex_if_drift'` (v0.38) automates the fix. |
| [The pgvector Tooling Landscape in 2026](pgvector-tooling-landscape.md) | Honest comparison of pg_trickle against pgai (archived Feb 2026), pg_vectorize, DIY batch pipelines, and Debezium. Introduces the two-layer model: Layer 1 = embedding generation, Layer 2 = derived-state maintenance. |
| [Multi-Tenant Vector Search with Row-Level Security](multi-tenant-vector-search-rls.md) | Zero cross-tenant data leakage using RLS policies on stream tables, tiered tenancy (large / medium / small tenant strategies), per-tenant partial HNSW indexes, and drift-aware reindexing per partition. |

### Operations & Observability

| Post | Summary |
|------|---------|
| [Stop Rebuilding Your Search Index at 3am](stop-rebuilding-search-index.md) | How pg_trickle's scheduler, SLA tiers (`critical` / `standard` / `background`), backpressure, and parallel workers let you tune refresh behaviour per workload — and why the 3am maintenance window disappears with continuous incremental refresh. |
| [pg_trickle Monitors Itself](self-monitoring.md) | Since v0.20, the extension's own health metrics are maintained as stream tables. How self-monitoring works, what it tracks, and the recursion question ("who monitors the monitor?"). |
| [How to Change a Stream Table Query Without Taking It Offline](online-schema-evolution.md) | `ALTER STREAM TABLE ... QUERY` performs online schema evolution — the stream table stays queryable during migration, with atomic swap and cascade-safe dependency checking. |
| [Backup and Restore for Stream Tables](backup-and-restore.md) | pg_dump, PITR, selective restore, and the `repair_stream_table` procedure. What to do (and what breaks) when you restore a database with active stream tables. |
| [Testing Stream Tables: Shadow Mode and Correctness Fuzzing](shadow-mode-correctness-fuzzing.md) | Shadow mode runs DIFFERENTIAL and FULL refresh in parallel and compares. SQLancer fuzzing generates random schemas and DML to find delta engine bugs. The multiset invariant and what it caught. |
| [Snapshots: Time Travel for Stream Tables](snapshots-time-travel.md) | `snapshot_stream_table()` captures point-in-time copies for pre-migration safety, replica bootstrap, forensic comparison, and test fixtures. Restore, list, and clean up with one function call each. |
| [Drain Mode: Zero-Downtime Upgrades for Stream Tables](drain-mode.md) | `pgtrickle.drain()` quiesces in-flight refreshes before maintenance. Safe upgrade workflow, CloudNativePG integration, HA failover, and the resume path. |
| [Column-Level Lineage in One Function Call](column-level-lineage.md) | `stream_table_lineage()` maps output columns to source columns. Impact analysis before ALTER TABLE, GDPR column-deletion audit, documentation generation, and recursive DAG tracing. |
| [Error Budgets for Stream Tables](error-budgets.md) | SRE-style freshness monitoring: `sla_summary()` with p50/p99 latency, staleness tracking, error budget consumption, alerting thresholds, and Prometheus integration. |
| [Structured Logging and OpenTelemetry for Stream Tables](structured-logging.md) | `log_format = json` emits structured events with `cycle_id` correlation. Event taxonomy, log aggregator integration (Loki, Datadog, Elasticsearch), and OpenTelemetry compatibility. |

### Analytics & Feature Engineering

| Post | Summary |
|------|---------|
| [Funnel Analysis and Cohort Retention at Scale](funnel-analysis-cohort-retention.md) | Computing conversion funnels, retention matrices, and session aggregates incrementally — keeping product analytics live without billion-row scans. |
| [Incremental ML Feature Engineering in PostgreSQL](incremental-ml-feature-engineering.md) | Replace nightly feature store batch jobs with continuously fresh features: rolling windows, lag features, cross-entity comparisons, all maintained as stream tables. |
| [Time-Series Downsampling Without TimescaleDB](time-series-downsampling.md) | Hourly, daily, and monthly rollups maintained incrementally from raw sensor data — cascading stream tables as a lightweight alternative to a dedicated TSDB. |
| [Incremental Statistical Aggregates: stddev, Percentiles, and Histograms](incremental-statistical-aggregates.md) | Which higher-order statistics (variance, correlation, histograms) can be maintained exactly, which need approximations, and the space-accuracy trade-offs. |

### Data Patterns & Domain Applications

| Post | Summary |
|------|---------|
| [Event Sourcing Read Models Without Replay](event-sourcing-read-models.md) | Project live read-optimized views from an append-only event store without replaying history — order status, revenue analytics, and inventory projections as stream tables. |
| [Soft Deletes and Tombstone Management in Differential IVM](soft-deletes-tombstone-management.md) | How `deleted_at` patterns interact with delta propagation, ghost row pitfalls, cascading visibility, and best practices for correct stream tables over soft-deletable data. |
| [Compliance and Audit Trails with Append-Only Stream Tables](compliance-audit-trails.md) | GDPR-compliant, tamper-evident audit logs: right-to-erasure reconciliation, hash chains, access pattern monitoring, and retention policies — all incrementally maintained. |
| [Incremental Full-Text Search with tsvector](incremental-fulltext-search.md) | Maintain ranked search results incrementally as documents change — tracked queries, faceted counts, and top-K ranking without re-indexing the corpus. |
| [Incremental PageRank and Graph Analytics in SQL](incremental-pagerank-graph-analytics.md) | Live PageRank, connected components, and shortest-path metrics maintained inside PostgreSQL as stream tables — no graph database required. |
| [PostGIS + pg_trickle: Incremental Geospatial Aggregates](postgis-incremental-geospatial.md) | Heatmaps, geofencing, spatial clustering, and distance-based aggregation that update in milliseconds as new points arrive. |

### Deployment & Multi-Tenancy

| Post | Summary |
|------|---------|
| [High Availability Failover with pg_trickle and Patroni](ha-failover-patroni.md) | How stream table state survives primary switchover, WAL replay semantics for change buffers, split-brain prevention, and zero-data-loss configuration. |
| [Parameterized Stream Tables: Building a SQL View Library](parameterized-stream-tables.md) | Patterns for reusable, tenant-scoped, and versionable stream table definitions: single-table multi-tenant, template functions, schema isolation, and composable building blocks. |

### Performance Internals

| Post | Summary |
|------|---------|
| [The 45ms Cold-Start Tax and How L0 Cache Eliminates It](l0-cache-cold-start.md) | Connection poolers recycle backends, paying a template-parse penalty. The L0 process-local `RwLock<HashMap>` cache keyed by `(pgt_id, cache_generation)` drops p99 from 48ms to 6ms. |
| [Spill-to-Disk and the Auto-Fallback Safety Net](spill-to-disk-fallback.md) | When delta queries exceed `work_mem`, pg_trickle detects consecutive spills and auto-switches to FULL refresh. Tuning `merge_work_mem_mb`, `spill_threshold_blocks`, and the self-healing recovery path. |

### Benchmarks & Advanced Patterns

| Post | Summary |
|------|---------|
| [TPC-H at 1GB in 40ms](tpch-benchmarking-ivm.md) | Reproducible benchmark of differential vs. full refresh across five TPC-H queries (Q1, Q3, Q5, Q6, Q12). Results: 13–22× faster per refresh cycle, with differential lag under 2.5 seconds vs. 186 seconds at 5,000 rows/second sustained write load. |
| [From Nexmark to Production: Benchmarking Stream Processing in PostgreSQL](nexmark-benchmark.md) | pg_trickle on the Nexmark streaming benchmark: per-query throughput, latency percentiles, and how the numbers compare to Flink, Materialize, and a cron job. |
| [Reactive Alerts Without Polling](reactive-alerts-without-polling.md) | How pg_trickle's reactive subscriptions (v0.39) replace polling loops: SLA breach detection, inventory alerts, fraud velocity checks, and vector distance subscriptions. Covers `OLD.*`/`NEW.*` transition semantics and PostgreSQL `LISTEN`. |
| [The Outbox Pattern, Turbocharged](outbox-pattern-turbocharged.md) | Using stream tables as transactionally consistent event sources for the outbox pattern — derived aggregate events, fat payloads, transition-based routing, and why stream tables naturally debounce high-frequency changes into fewer events. |

---

## Contributing

These posts are deliberately rough-edged — they're drafts exploring how the extension works, not polished marketing copy. If you spot a technical inaccuracy, open an issue or PR. If you want to write a post, open a discussion first to avoid duplication.
