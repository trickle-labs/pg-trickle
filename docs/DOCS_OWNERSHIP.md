# Documentation Ownership and Canonical Sources

This file defines the **canonical source** for each type of fact in the
pg_trickle documentation. When facts are duplicated across files, the canonical
source is the one to update first; other files should cross-link rather than
reproduce the content.

Use this as a guide when:
- Writing new documentation
- Reviewing a PR that changes docs
- Deciding where to add a new fact

---

## GUC (Configuration) Parameters

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| GUC **name, type, default** | [GUC_CATALOG.md](GUC_CATALOG.md) | Auto-generated from `src/config.rs`. Never edit by hand. |
| GUC **narrative description, examples, interactions** | [CONFIGURATION.md](CONFIGURATION.md) | Covers ~50 highest-impact GUCs with examples. |
| GUC **coverage policy** | [CONFIGURATION.md](CONFIGURATION.md) `## Coverage policy` | Explains what is documented here vs. GUC_CATALOG.md. |

**Rule:** If you add or rename a GUC in `src/config.rs`, run `python3 scripts/gen_catalogs.py` to regenerate GUC_CATALOG.md. Then add a narrative entry in CONFIGURATION.md if it is operator-visible.

---

## SQL Functions

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| Function **name, schema, return type** | [SQL_API_CATALOG.md](SQL_API_CATALOG.md) | Auto-generated from `#[pg_extern]` attributes. Never edit by hand. |
| Function **full signature, parameters, examples** | [SQL_REFERENCE.md](SQL_REFERENCE.md) | Written by hand. Organized by use case. |
| Advanced/diagnostic function summary table | [SQL_REFERENCE.md](SQL_REFERENCE.md) `## Advanced and Diagnostic Functions` | Quick-reference table for less-common functions. |

**Rule:** When a function is renamed in Rust, use `name = "..."` in the `#[pg_extern]` attribute so the SQL name is preserved. Regenerate SQL_API_CATALOG.md and update any doc references.

---

## Stream Table Lifecycle

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| Status values (INITIALIZING, ACTIVE, SUSPENDED, ERROR) | [SQL_REFERENCE.md](SQL_REFERENCE.md) `## Stream Table Lifecycle` | Primary definition. |
| State transitions | [SQL_REFERENCE.md](SQL_REFERENCE.md) `## Stream Table Lifecycle` | ASCII state machine diagram. |
| `consecutive_errors` auto-suspend threshold | [SQL_REFERENCE.md](SQL_REFERENCE.md) and [CONFIGURATION.md](CONFIGURATION.md) `pg_trickle.max_consecutive_errors` | Do not reproduce the threshold value in tutorials — link to these. |
| Reinitialisation trigger (`needs_reinit`) | [SQL_REFERENCE.md](SQL_REFERENCE.md) `## Stream Table Lifecycle` | |

---

## CDC Architecture

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| Trigger type defaults (`statement` vs `row`) | [ARCHITECTURE.md](ARCHITECTURE.md) and source `src/cdc/mod.rs` comments | Default is **statement-level** (`FOR EACH STATEMENT`). Do not say "row-level" is the default. |
| Trigger-based CDC detailed walkthrough | [docs/tutorials/WHAT_HAPPENS_ON_INSERT.md](tutorials/WHAT_HAPPENS_ON_INSERT.md) | Step-by-step INSERT lifecycle. |
| WAL CDC detailed walkthrough | [ARCHITECTURE.md](ARCHITECTURE.md) `## WAL-based CDC` | |
| CDC mode GUC options | [CONFIGURATION.md](CONFIGURATION.md) `### pg_trickle.cdc_mode` | |
| Foreign table CDC constraints | [docs/tutorials/FOREIGN_TABLE_SOURCES.md](tutorials/FOREIGN_TABLE_SOURCES.md) | Foreign tables support **no triggers** at all. |

---

## Scheduler Architecture

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| Worker model (launcher + per-DB schedulers) | [ARCHITECTURE.md](ARCHITECTURE.md) `## Background Workers` | There is one launcher (`pg_trickle launcher`) and one per-database scheduler worker. Do not describe it as "a single background worker." |
| Refresh group / parallel refresh | [SQL_REFERENCE.md](SQL_REFERENCE.md) `## Refresh Groups` | |
| Scheduler GUCs | [CONFIGURATION.md](CONFIGURATION.md) `## Essential` | |

---

## Partitioned Tables

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| How CDC triggers work on partitioned tables | [docs/tutorials/PARTITIONED_TABLES.md](tutorials/PARTITIONED_TABLES.md) | Statement-level triggers on root; transition tables capture all partitions. |
| PostgreSQL partition inheritance rules for triggers | [docs/tutorials/PARTITIONED_TABLES.md](tutorials/PARTITIONED_TABLES.md) | Row-level triggers are auto-cloned; statement-level triggers are NOT. pg_trickle installs on root. |

---

## pg_tide Integration (formerly pg_trickle outbox/inbox)

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| Which APIs moved to pg_tide | [OUTBOX.md](OUTBOX.md) and [INBOX.md](INBOX.md) | APIs moved: `enable_outbox`, `disable_outbox`, `poll_outbox`, `commit_offset`, `create_consumer_group`, `create_inbox`, etc. |
| APIs remaining in pg_trickle | [SQL_REFERENCE.md](SQL_REFERENCE.md) `## Transactional Outbox` | `attach_outbox`, `detach_outbox`, `attach_embedding_outbox`, `outbox_status` remain. |

---

## Feature Readiness

| Fact type | Canonical source | Notes |
|-----------|-----------------|-------|
| Per-feature maturity labels | The feature's primary doc file (e.g., CITUS.md, STORAGE_BACKENDS.md) | Use the badge format: `> **Status: Beta**`. See [DOCS_OWNERSHIP.md](DOCS_OWNERSHIP.md) label definitions below. |

### Label definitions

| Label | Meaning |
|-------|---------|
| **Stable** | Feature is production-ready, API is stable, tested in CI. |
| **Beta** | Feature works but the API may change in a minor version. Suitable for staging. |
| **Experimental** | Early-access feature. Breaking changes expected. Do not use in production. |
| **Design only** | Described in plans/blog but not yet implemented in the codebase. |
| **Moved to pg_tide** | Feature was extracted to the [pg_tide](https://github.com/trickle-labs/pg-tide) extension. |
| **Internal** | Not user-facing; used by pg_trickle itself. Do not reference in user documentation. |
| **Deprecated** | Still works but scheduled for removal. Use the replacement instead. |

---

## Drift Guards

Run these scripts before merging any documentation PR:

```bash
python3 scripts/gen_catalogs.py --check    # catalogs are up to date
python3 scripts/check_docs_truth.py        # no stale API/GUC references
python3 check_links.py                     # no broken links
just fmt                                   # code formatted
just lint                                  # clippy passes
```

The `check_docs_truth.py` script compares all `pg_trickle.XXX` and
`pgtrickle.XXX()` references in the docs against the generated catalogs.
Unknown references must either be fixed or added to the allowlist in
`scripts/check_docs_truth_allowlist.yml` with a justification comment.

---

*This file is maintained by hand. If you change the doc structure, update the
canonical source table here.*
