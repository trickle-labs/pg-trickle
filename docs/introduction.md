# pg_trickle

**pg_trickle** is a PostgreSQL 18 extension that adds *self-maintaining*
materialized views — **stream tables** — and keeps them up to date
incrementally as the underlying data changes. No external streaming
engine, no sidecars, no bespoke refresh pipeline. Just install the
extension and write SQL.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'active_orders',
    query    => 'SELECT * FROM orders WHERE status = ''active''',
    schedule => '30s'
);

INSERT INTO orders (id, status) VALUES (42, 'active');
SELECT count(*) FROM active_orders;  -- 1, automatically
```

> **New here?** Read **[What is pg_trickle?](ESSENCE.md)** for the
> plain-language overview, or jump to the
> **[5-Minute Quickstart](QUICKSTART_5MIN.md)** to try it.
> First time installing? See the **[Installation Guide](../INSTALL.md)**.

---

## How it works

pg_trickle keeps stream tables current by tracking every change to the source
tables — inserts, updates, and deletes — and recomputing only the parts of the
view that are affected by those changes. This is called **differential** (or
incremental) view maintenance. Instead of re-running the full query on every
refresh cycle, pg_trickle applies a *delta* computation proportional to the
number of changed rows, not the total table size. A stream table over a
billion-row orders table refreshes in milliseconds when only a few rows changed.

Change capture works through **trigger-based CDC** (statement-level triggers by
default, row-level triggers available for compatibility) or **WAL-based logical
decoding** (`cdc_mode = 'wal'` or the automatic `'auto'` mode). Trigger-based
capture writes changed rows into a per-source change-buffer table within the
same transaction, providing full atomicity with no possibility of a committed
change being missed. The background scheduler reads from the
change buffer, computes the delta SQL, and applies the result to the stream
table using `MERGE` in a separate transaction.

For queries that cannot be maintained incrementally (non-monotonic functions,
`LATERAL` with volatile sub-expressions, etc.), pg_trickle automatically falls
back to a **full refresh** — replacing the entire stream table contents in a
single transaction. You can also force full mode explicitly or let the
cost-based `AUTO` strategy choose per-refresh based on the change-to-table-size
ratio.

---

## Choose your path

| Persona | Start here |
|---|---|
| **Curious / evaluator** | [What is pg_trickle?](ESSENCE.md) → [Use Cases](USE_CASES.md) → [Comparisons](COMPARISONS.md) → [Playground](PLAYGROUND.md) |
| **Application developer** | [5-Minute Quickstart](QUICKSTART_5MIN.md) → [Getting Started tutorial](GETTING_STARTED.md) → [Patterns](PATTERNS.md) → [SQL Reference](SQL_REFERENCE.md) |
| **DBA / SRE** | [Pre-Deployment Checklist](PRE_DEPLOYMENT.md) → [Configuration](CONFIGURATION.md) → [Troubleshooting](TROUBLESHOOTING.md) → [Capacity Planning](CAPACITY_PLANNING.md) |
| **Data / analytics engineer** | [Use Cases](USE_CASES.md) → [dbt integration](integrations/dbt.md) → [Migrating from materialized views](tutorials/MIGRATING_FROM_MATERIALIZED_VIEWS.md) |
| **Building a dashboard backend** | [Real-Time Analytics Dashboard tutorial](tutorials/FIRST_DASHBOARD.md) |
| **Event-sourced architecture** | [Event Sourcing / CQRS tutorial](tutorials/EVENT_SOURCING.md) |
| **Migrating from REFRESH MATERIALIZED VIEW** | [Backfill and Migration tutorial](tutorials/BACKFILL_AND_MIGRATION.md) |
| **Hardening a production deployment** | [Security Hardening tutorial](tutorials/SECURITY_HARDENING.md) → [Security Guide](SECURITY_GUIDE.md) |
| **Confused by jargon** | [Glossary](GLOSSARY.md) |

---

## What's new

See [What's New](WHATS_NEW.md) for a curated summary of recent
releases, or the [Changelog](changelog.md) for the full history.

---

## Project & licensing

- Written in Rust using [pgrx](https://github.com/pgcentralfoundation/pgrx).
- Targets PostgreSQL 18.
- Apache 2.0 licensed.
- Repository: <https://github.com/trickle-labs/pg-trickle>
- [Project history](PROJECT_HISTORY.md) ·
  [Roadmap](roadmap.md) ·
  [Contributing](contributing.md) ·
  [Security policy](security.md)
