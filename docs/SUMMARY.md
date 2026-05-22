# Summary

[pg_trickle](introduction.md)
[What is pg_trickle?](ESSENCE.md)

---

# Discover

- [Use Cases](USE_CASES.md)
- [Comparisons](COMPARISONS.md)
- [Glossary](GLOSSARY.md)
- [Playground (Docker sandbox)](PLAYGROUND.md)
- [Real-time Demo](DEMO.md)

---

# Get Started

- [5-Minute Quickstart](QUICKSTART_5MIN.md)
- [Getting Started Tutorial](GETTING_STARTED.md)
- [Installation](installation.md)

---

# Build with Stream Tables

- [Best-Practice Patterns](PATTERNS.md)
- [Mental Model: How It Works](MENTAL_MODEL.md)
- [Limitations](LIMITATIONS.md)
- [Performance Cookbook](PERFORMANCE_COOKBOOK.md)
- [Performance Cheat Sheet](PERFORMANCE_CHEATSHEET.md)
- [SQL Reference](SQL_REFERENCE.md)
- [SQL API Reference](SQL_API_CATALOG.md)
- [GUC Reference](GUC_CATALOG.md)
- [Configuration](CONFIGURATION.md)
- [Storage Backends](STORAGE_BACKENDS.md)
- [Predictive Cost Model](COST_MODEL.md)

---

# Operate

- [Pre-Deployment Checklist](PRE_DEPLOYMENT.md)- [Operator Cheat Sheet](OPS_CHEATSHEET.md)- [Scaling Guide](SCALING.md)
- [Capacity Planning](CAPACITY_PLANNING.md)
- [Multi-Database Deployments](MULTI_DATABASE.md)
- [Backup and Restore](BACKUP_AND_RESTORE.md)
- [Snapshots](SNAPSHOTS.md)
- [High Availability and Replication](HA_AND_REPLICATION.md)
- [Upgrading](UPGRADING.md)
- [Security Guide](SECURITY_GUIDE.md)
- [Security Model](SECURITY_MODEL.md)
- [Troubleshooting & Runbook](TROUBLESHOOTING.md)
- [Drain-Mode Runbook](RUNBOOK_DRAIN.md)
- [Error Reference](ERRORS.md)

---

# Distributed & Streaming

- [Citus Distributed Tables](CITUS.md)
- [CDC Modes](CDC_MODES.md)
- [SLA-based Smart Scheduling](SLA_SCHEDULING.md)
- [Downstream Publications](PUBLICATIONS.md)
- [Transactional Outbox](OUTBOX.md)
- [Transactional Inbox](INBOX.md)

---

# Tutorials: Fundamentals

- [What Happens on INSERT](tutorials/WHAT_HAPPENS_ON_INSERT.md)
- [What Happens on UPDATE](tutorials/WHAT_HAPPENS_ON_UPDATE.md)
- [What Happens on DELETE](tutorials/WHAT_HAPPENS_ON_DELETE.md)
- [What Happens on TRUNCATE](tutorials/WHAT_HAPPENS_ON_TRUNCATE.md)

# Tutorials: Data Lake & Analytics

- [Real-Time Analytics Dashboard](tutorials/FIRST_DASHBOARD.md)
- [The Modern Data Stack in One Box](tutorial-modern-data-stack-one-box.md)
- [Streaming PostgreSQL to a Data Lake](tutorial-streaming-postgres-to-data-lake.md)
- [IVM for DuckLake Before v2.0](tutorial-ivm-ducklake-before-v2.md)
- [pg-tide DuckLake Pipeline](tutorial-pg-tide-ducklake-pipeline.md)

# Tutorials: Event-Driven Architecture

- [Event Sourcing / CQRS Read Models](tutorials/EVENT_SOURCING.md)
- [ETL & Bulk Load Patterns](tutorials/ETL_BULK_LOAD.md)

# Tutorials: CDC & Data Integration

- [Sub-Millisecond Inlined-Data CDC](tutorial-sub-millisecond-inlined-cdc.md)
- [Foreign Table Sources](tutorials/FOREIGN_TABLE_SOURCES.md)
- [Migrating from Materialized Views](tutorials/MIGRATING_FROM_MATERIALIZED_VIEWS.md)
- [Migrating from pg_ivm](tutorials/MIGRATING_FROM_PG_IVM.md)

# Tutorials: Operations & Performance

- [Tiered Scheduling](tutorials/TIERED_SCHEDULING.md)
- [Tuning Refresh Mode](tutorials/tuning-refresh-mode.md)
- [Circular Dependencies](tutorials/CIRCULAR_DEPENDENCIES.md)
- [Fuse Circuit Breaker](tutorials/FUSE_CIRCUIT_BREAKER.md)
- [Monitoring & Alerting](tutorials/MONITORING_AND_ALERTING.md)
- [Backfill and Migration from Materialized Views](tutorials/BACKFILL_AND_MIGRATION.md)

# Tutorials: Security & Multi-Tenancy

- [Security Hardening](tutorials/SECURITY_HARDENING.md)
- [Row-Level Security](tutorials/ROW_LEVEL_SECURITY.md)
- [Per-Tenant ANN Indexing Patterns](tutorials/PER_TENANT_ANN_PATTERNS.md)

# Tutorials: Vector Search & AI/ML

- [Hybrid Search Patterns](tutorials/HYBRID_SEARCH_PATTERNS.md)
- [pgvector RAG Cookbook](tutorials/PGVECTOR_RAG_COOKBOOK.md)
- [pgVector Embedding Pipelines](tutorials/PGVECTOR_EMBEDDING_PIPELINES.md)
- [Vector RAG Starter](tutorials/VECTOR_RAG_STARTER.md)

# Tutorials: Advanced Patterns

- [Partitioned Tables](tutorials/PARTITIONED_TABLES.md)

---

# Integrations

- [dbt-pgtrickle](integrations/dbt.md)
- [CloudNativePG / Kubernetes](integrations/cloudnativepg.md)
- [Citus (long-form reference)](integrations/citus.md)
- [Prometheus & Grafana](integrations/prometheus.md)
- [PgBouncer & Connection Poolers](integrations/pgbouncer.md)
- [Flyway & Liquibase](integrations/flyway-liquibase.md)
- [ORM Integration](integrations/orm.md)
- [Multi-Tenant](integrations/multi-tenant.md)
- [dbt Hub Submission](integrations/dbt-hub-submission.md)
- [OpenTelemetry](OPENTELEMETRY.md)

---

# Reference

- [Blog](../blog/README.md)
- [FAQ](FAQ.md)
- [What's New](WHATS_NEW.md)
- [Changelog](changelog.md)
- [Roadmap](roadmap.md)
- [Release Process](RELEASE.md)
- [Project History](PROJECT_HISTORY.md)
- [Contributing](contributing.md)
- [Security Policy](security.md)

---

# Internals (for contributors)

- [Architecture Overview](ARCHITECTURE.md)
- [DVM Operators](DVM_OPERATORS.md)
- [DVM Rewrite Rules](DVM_REWRITE_RULES.md)
- [Benchmarks](BENCHMARK.md)
- [Dependency Policy](DEPENDENCIES.md)
- [Documentation Ownership](DOCS_OWNERSHIP.md)

# Research

- [DBSP Comparison](research/DBSP_COMPARISON.md)
- [pg_ivm Comparison](research/PG_IVM_COMPARISON.md)
- [Custom SQL Syntax](research/CUSTOM_SQL_SYNTAX.md)
- [Triggers vs Replication](research/TRIGGERS_VS_REPLICATION.md)
- [Prior Art](research/PRIOR_ART.md)
- [Multi-DB Refresh Broker](research/multi_db_refresh_broker.md)
- [Materialised k-NN Graph Trade-offs](research/KNN_GRAPH_TRADEOFFS.md)
