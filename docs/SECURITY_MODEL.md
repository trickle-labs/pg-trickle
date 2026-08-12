# Security Model — pg_trickle

> **Audience:** Database administrators, security engineers, and operators
> deploying pg_trickle in production environments.

---

## Overview

pg_trickle is a PostgreSQL extension that runs inside the PostgreSQL server
process. Its security surface spans:

1. **SQL-callable functions** — the `pgtrickle` schema
2. **CDC trigger bodies** — fire as `SECURITY DEFINER` to write to the change
   buffer, which lives in the `pgtrickle_changes` schema
3. **Background worker** — a scheduler process that runs as the PostgreSQL
   superuser
4. **Secret handling** — credentials in configuration files, environment
   variables, and shell history

---

## Superuser Requirement (A45-6)

### Why pg_trickle requires a superuser at install time

pg_trickle uses `superuser = true` and `trusted = false` in its
`pg_trickle.control` file. This means:

- The extension can **only be installed by a superuser** (`CREATE EXTENSION`
  requires `pg_catalog.pg_extension_config_dump()` access and the right to
  create background workers).
- The extension is **not trusted**, so it cannot be installed by a database
  owner in a database where the installer is not also a superuser.

**Exact privileges needed at install time:**

| Privilege | Reason |
|-----------|--------|
| `SUPERUSER` | Register dynamic background workers; set GUCs at function creation |
| `CREATE` on extension schemas | Create `pgtrickle` and `pgtrickle_changes` schemas |
| `CREATE TRIGGER` on source tables | Install CDC triggers when creating stream tables |
| Read access to `pg_catalog.*` | Resolve OIDs, inspect RLS flags, check relkind |

**Runtime privileges (background worker):**

The scheduler background worker runs as the PostgreSQL superuser. It uses
this privilege only for:

- Reading the `pgtrickle.*` catalog tables.
- Writing delta results via MERGE/INSERT/DELETE/UPDATE on stream tables.
- Managing logical replication slots for WAL-based CDC.
- Calling `pg_cancel_backend()` when a stale worker must be interrupted.

### Why trusted install is not currently supported

PostgreSQL's trusted extension model allows a database owner (non-superuser)
to install an extension if it is marked `trusted = true`. pg_trickle cannot
use this model because:

1. **Background worker registration** (`BackgroundWorkerBuilder`) requires
   superuser privilege at worker startup. A trusted install context does not
   provide this.
2. **Event triggers** (`CREATE EVENT TRIGGER`) require superuser privilege.
3. **Schema-level REVOKE on `pgtrickle_changes`** requires ownership, which
   means the extension owner must be the superuser at install time.

### Guidance for managed / hosted environments

In cloud environments (RDS, AlloyDB, Cloud SQL, Neon, Supabase, etc.) where
you do not have full superuser access:

1. Verify that your provider supports extensions with `trusted = false`
   (most managed providers have an approved extension allowlist).
2. Request or verify that `pg_trickle` is on the allowlist.
3. Use the provider's superuser-equivalent role (e.g., `rds_superuser` on RDS)
   to install the extension: `CREATE EXTENSION pg_trickle;`
4. After installation, non-superuser roles can use the explicitly granted
   `pgtrickle.*` functions. Creation APIs use `SECURITY DEFINER` only for
   private infrastructure; defining queries and output-table DDL keep the
   caller's privileges.

See [INSTALL.md](../INSTALL.md) for distribution-specific instructions.

---

## SECURITY DEFINER Usage

### CDC trigger functions

All CDC trigger functions created by `create_stream_table()` are
`SECURITY DEFINER` and owned by the superuser. This is necessary because:

- The change buffer tables (`pgtrickle_changes.changes_<oid>`) are owned by
  the superuser.
- DML sessions on source tables must be able to write to the change buffer
  without being granted direct access to `pgtrickle_changes`.

**Implication:** Any user with `INSERT`, `UPDATE`, or `DELETE` access to a
source table will indirectly write to the change buffer. This is by design —
the trigger captures every committed change regardless of who made it.

### `search_path` hardening

All pg_trickle `SECURITY DEFINER` functions and trigger procedures set
`search_path = pgtrickle, pgtrickle_changes, pg_catalog, pg_temp` at creation
time to prevent search-path injection attacks. This follows PostgreSQL best
practice (see [CWE-89](https://cwe.mitre.org/data/definitions/89.html) and the
PostgreSQL docs on
[writing SECURITY DEFINER functions](https://www.postgresql.org/docs/current/sql-createfunction.html#SQL-CREATEFUNCTION-SECURITY)).

To verify:

```sql
SELECT proname, prosecdef, proconfig
FROM pg_proc
WHERE pronamespace = 'pgtrickle'::regnamespace
  AND prosecdef
ORDER BY proname;
```

---

## Row-Level Security (RLS)

pg_trickle **does not enforce RLS** on stream tables by default. Stream tables
are ordinary PostgreSQL tables — RLS can be applied to them with `ALTER TABLE
... ENABLE ROW LEVEL SECURITY` as with any table.

**Important caveats:**

- The background worker refreshes stream tables as the superuser. RLS policies
  do **not** apply to the superuser by default.
- To enforce RLS during refresh, use `FORCE ROW LEVEL SECURITY` on the stream
  table and ensure the superuser is explicitly covered by a permissive policy.
- The defining query for a stream table runs as the superuser regardless of who
  created the stream table. This means RLS on **source tables** is bypassed
  during refresh unless those tables also use `FORCE ROW LEVEL SECURITY`.

---

## CDC Buffer Access

The `pgtrickle_changes` schema contains one unlogged table per source table
OID. These tables are only meant for internal pg_trickle use:

- **Do not grant** `SELECT`, `INSERT`, `UPDATE`, or `DELETE` on
  `pgtrickle_changes.*` to application users.
- **Do not include** `pgtrickle_changes.*` in logical replication publications
  (they are UNLOGGED by default and thus not replicatable).
- The scheduler reads and truncates change buffer tables during each refresh.
  External reads during active refresh may observe partial or inconsistent
  intermediate state.

---

## TRUNCATE Semantics

When a FULL refresh completes, pg_trickle uses `TRUNCATE pgtrickle_changes.changes_<oid>`
(or `DELETE`, depending on the `pg_trickle.cleanup_use_truncate` GUC) to clear
the change buffer after consuming all pending changes.

**TRUNCATE behaviour:**

- Acquires `ACCESS EXCLUSIVE` lock on the change buffer table for the duration
  of the TRUNCATE. This briefly blocks concurrent DML on the **change buffer**
  (not the source table). Source table DML is unaffected.
- Is WAL-logged if the change buffer table is `LOGGED`, or simply resets the
  relation's fork if `UNLOGGED` (the default).
- When `pg_trickle.cdc_paused = on`, CDC trigger bodies return `NULL`
  regardless of this setting — the change buffer is not written, so there
  is nothing to TRUNCATE.

### `cdc_paused` vs `drain()` semantics

| Mechanism | Effect | Change buffer | Stream table |
|-----------|--------|---------------|--------------|
| `pg_trickle.cdc_paused = on` | New changes are discarded (triggers return NULL) | Not written | Stale |
| `pgtrickle.drain(timeout)` | Wait for in-flight refreshes to finish; stop scheduling new ones | Unchanged | Consistent after drain |
| `pg_trickle.enabled = off` | Disable the entire scheduler | Accumulates | Stale |

When resuming from `cdc_paused`, call
`SELECT pgtrickle.reinitialize('schema.stream_table')` to restore consistency,
since changes that arrived during the pause were discarded.

---

## Background Worker Privilege

The scheduler background worker runs with full superuser privilege because
PostgreSQL requires it for dynamic background worker registration. pg_trickle
uses this privilege only to:

- Read `pgtrickle.*` catalog tables
- Write to `pgtrickle_changes.*` change buffers
- Execute MERGE/INSERT/UPDATE/DELETE on stream tables
- Register and manage dynamic refresh workers

The worker does **not**:

- Write to user application tables (except stream tables owned by the extension)
- Execute arbitrary SQL from untrusted input
- Access credentials or secrets at runtime

---

## Incident Response: TRUNCATE Semantics Under Pause

When `cdc_paused` was active during an incident:

1. `SELECT pgtrickle.cdc_pause_status()` — confirm pause mode and scope.
2. Set `cdc_paused = off` to re-enable captures.
3. For each affected stream table, call
   `SELECT pgtrickle.reinitialize('schema.table_name')` to trigger a full
   resync from source. In-flight refresh will overwrite any stale data.
4. Monitor `pgtrickle.health_check()` until all tables report `status = 'ok'`.

---

## v1.0 Supply-Chain Preparation

The following supply-chain controls are staged for v1.0 (tracked by O40-9
in [ROADMAP.md](../ROADMAP.md)):

- **SBOM generation** (`cargo sbom` / `cyclonedx-rust-cargo`): Planned for v1.0.
  Will be generated in CI and attached to each GitHub release as
  `sbom.cdx.json`.
- **Artifact signing** (sigstore/cosign for Docker images and PGXN archives):
  Planned for v1.0.  Docker images will be signed with `cosign sign` using
  keyless OIDC signing; signatures will be verifiable via
  `cosign verify ghcr.io/trickle-labs/pg_trickle:<tag>`.
- **Provenance attestation** (`actions/attest-build-provenance`): Planned for
  v1.0.  Build provenance (builder, repository, ref SHA) will be attached to
  every release artifact.
- **Reproducible builds** (`cargo auditable`): Planned for v1.0.  Binaries
  will embed dependency version information auditable via `cargo auditable info`.

---

## Related Documentation

- [docs/CONFIGURATION.md](CONFIGURATION.md) — GUC reference
- [docs/RUNBOOK_DRAIN.md](RUNBOOK_DRAIN.md) — drain-mode operational guide
- [docs/GUC_CATALOG.md](GUC_CATALOG.md) — generated GUC catalog
- [docs/SQL_API_CATALOG.md](SQL_API_CATALOG.md) — generated SQL API catalog
