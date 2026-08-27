# pg_trickle Error Reference

This document lists all `PgTrickleError` variants with descriptions, common
causes, and suggested fixes. If you encounter an error not listed here, please
[open an issue](https://github.com/trickle-labs/pg-trickle/issues).

> **Tip:** Most errors include context (table name, OID, or query fragment) in
> the message text. Use that context to narrow down the root cause.

---

## SQLSTATE Code Reference

Every pg_trickle error includes a PostgreSQL SQLSTATE code for programmatic
error handling. Use `SQLSTATE` in PL/pgSQL `EXCEPTION WHEN` blocks or check
the error code in your client library.

| Error Variant | SQLSTATE | Code Name |
|---------------|----------|-----------|
| QueryParseError | `42000` | SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION |
| TypeMismatch | `42804` | DATATYPE_MISMATCH |
| UnsupportedOperator | `0A000` | FEATURE_NOT_SUPPORTED |
| CycleDetected | `3F000` | INVALID_SCHEMA_DEFINITION |
| NotFound | `42P01` | UNDEFINED_TABLE |
| AlreadyExists | `42P07` | DUPLICATE_TABLE |
| InvalidArgument | `22023` | INVALID_PARAMETER_VALUE |
| QueryTooComplex | `54000` | PROGRAM_LIMIT_EXCEEDED |
| PermissionDenied | `42501` | INSUFFICIENT_PRIVILEGE |
| UpstreamTableDropped | `42P01` | UNDEFINED_TABLE |
| UpstreamSchemaChanged | `42P17` | INVALID_TABLE_DEFINITION |
| LockTimeout | `55P03` | LOCK_NOT_AVAILABLE |
| ReplicationSlotError | `55000` | OBJECT_NOT_IN_PREREQUISITE_STATE |
| WalTransitionError | `55000` | OBJECT_NOT_IN_PREREQUISITE_STATE |
| SpiError | `XX000` | INTERNAL_ERROR |
| SpiErrorCode | `XX000` | INTERNAL_ERROR (SQLSTATE preserved from original) |
| SpiPermissionError | `42501` | INSUFFICIENT_PRIVILEGE |
| WatermarkBackwardMovement | `22000` | DATA_EXCEPTION |
| WatermarkGroupNotFound | `42704` | UNDEFINED_OBJECT |
| WatermarkGroupAlreadyExists | `42710` | DUPLICATE_OBJECT |
| RefreshSkipped | `55000` | OBJECT_NOT_IN_PREREQUISITE_STATE |
| PublicationAlreadyExists | `42710` | DUPLICATE_OBJECT |
| PublicationNotFound | `42704` | UNDEFINED_OBJECT |
| SlaTooSmall | `22023` | INVALID_PARAMETER_VALUE |
| ChangedColsBitmaskFailed | `XX000` | INTERNAL_ERROR |
| PublicationRebuildFailed | `XX000` | INTERNAL_ERROR |
| DiagnosticError | `XX000` | INTERNAL_ERROR |
| SnapshotAlreadyExists | `42710` | DUPLICATE_OBJECT |
| SnapshotSourceNotFound | `42P01` | UNDEFINED_TABLE |
| SnapshotSchemaVersionMismatch | `42P17` | INVALID_TABLE_DEFINITION |
| OutboxAlreadyEnabled | `42710` | DUPLICATE_OBJECT |
| OutboxNotEnabled | `42704` | UNDEFINED_OBJECT |
| PgTideMissing | `0A000` | FEATURE_NOT_SUPPORTED |
| PgTideUnsupportedVersion | `XX000` | INTERNAL_ERROR |
| PgTideUpgradeInProgress | `XX000` | INTERNAL_ERROR |
| PgTideOperationDenied | `XX000` | INTERNAL_ERROR |
| PgTideBindingMismatch | `XX000` | INTERNAL_ERROR |
| UnresolvedPlaceholder | `XX000` | INTERNAL_ERROR |
| DiffDepthExceeded | `54000` | PROGRAM_LIMIT_EXCEEDED |
| DiffCteCountExceeded | `54000` | PROGRAM_LIMIT_EXCEEDED |
| StSourceFrontierMissing | `42P01` | UNDEFINED_TABLE |
| InternalError | `XX000` | INTERNAL_ERROR |

---

## Error Categories

pg_trickle classifies errors into four categories that determine retry behavior:

| Category | Retried by scheduler? | Description |
|----------|-----------------------|-------------|
| **User** | No | Invalid queries, type mismatches, DAG cycles. Fix the input. |
| **Schema** | No (triggers reinitialize) | Upstream DDL changes. The stream table is reinitialized automatically. |
| **System** | Yes (with backoff) | Lock timeouts, replication slot problems, transient SPI failures. |
| **Internal** | No | Unexpected bugs. Please report these. |

---

## User Errors

### QueryParseError

**Message:** `query parse error: <details>`

**Description:** The defining query could not be parsed or validated by the
pg_trickle query analyzer.

**Common causes:**
- Syntax error in the defining query
- Use of PostgreSQL syntax not yet supported by pgrx's query parser
- A CTE or subquery that cannot be analyzed

**Suggested fix:** Simplify the query. Check that it runs as a standalone
`SELECT` statement. Review [SQL Reference — Expression Support](SQL_REFERENCE.md#expression-support)
for supported syntax.

---

### TypeMismatch

**Message:** `type mismatch: <details>`

**Description:** A type incompatibility was detected between the defining query
output and the stream table schema, or between source columns and expected types.

**Common causes:**
- Column type changed on a source table after stream table creation
- Explicit cast to an incompatible type in the defining query
- `UNION` branches with mismatched column types

**Suggested fix:** Ensure column types match. Use explicit `CAST()` to align
types if needed. If the source table changed, use
`pgtrickle.repair_stream_table()` to reinitialize.

---

### UnsupportedOperator

**Message:** `unsupported operator for DIFFERENTIAL mode: <operator>`

**Description:** The defining query uses an SQL operator or construct that
pg_trickle cannot maintain incrementally.

**Common causes:**
- `TABLESAMPLE`, `GROUPING SETS` beyond the branch limit, recursive CTEs with
  unsupported patterns, certain window function combinations
- Non-monotonic or volatile functions in positions that prevent differential
  maintenance

**Suggested fix:** Use `refresh_mode => 'FULL'` to fall back to full
recomputation:
```sql
SELECT pgtrickle.alter_stream_table('my_stream_table',
    refresh_mode => 'FULL');
```
Or restructure the query to avoid the unsupported construct. See
[SQL Reference — Expression Support](SQL_REFERENCE.md#expression-support).

---

### CycleDetected

**Message:** `cycle detected in dependency graph: A -> B -> C -> A`

**Description:** Adding or altering this stream table would create a circular
dependency in the refresh DAG.

**Common causes:**
- Stream table A depends on stream table B, which depends on A
- Indirect cycles through chains of stream tables

**Suggested fix:** Restructure the stream table definitions to break the cycle.
Use `pgtrickle.get_dependency_graph()` to visualize the current DAG. If
circular dependencies are intentional, enable `pg_trickle.allow_circular = true`
(see [Configuration](CONFIGURATION.md)).

---

### NotFound

**Message:** `stream table not found: <name>`

**Description:** The specified stream table does not exist in the
`pgtrickle.pgt_stream_tables` catalog.

**Common causes:**
- Typo in the stream table name
- The stream table was already dropped
- Schema-qualified name required but not provided (e.g., `myschema.my_st`)

**Suggested fix:** Check the name with `pgtrickle.list_stream_tables()`. Use
the fully qualified name: `schema.table_name`.

---

### AlreadyExists

**Message:** `stream table already exists: <name>`

**Description:** A `create_stream_table()` call was made for a stream table
name that is already registered.

**Common causes:**
- Re-running a migration or DDL script without `IF NOT EXISTS`

**Suggested fix:** Use `pgtrickle.create_stream_table_if_not_exists()` or
`pgtrickle.create_or_replace_stream_table()` for idempotent creation.

---

### InvalidArgument

**Message:** `invalid argument: <details>`

**Description:** An invalid value was passed to a pg_trickle API function.

**Common causes:**
- Invalid `refresh_mode` value (must be `'DIFFERENTIAL'`, `'FULL'`, or `'AUTO'`)
- Calling `resume_stream_table()` on a table that is not suspended
- Invalid schedule interval or threshold value
- Empty or malformed table name

**Suggested fix:** Check the function signature in the
[SQL Reference](SQL_REFERENCE.md) and correct the argument.

---

### QueryTooComplex

**Message:** `query too complex: <details>`

**Description:** The defining query exceeds the maximum parse depth, which
protects against stack overflow during query analysis.

**Common causes:**
- Deeply nested subqueries (> 64 levels by default)
- Large `UNION ALL` chains
- Complex CTE hierarchies

**Suggested fix:** Simplify the query. If the depth limit is too restrictive,
increase `pg_trickle.max_parse_depth` (default: 64). See
[Configuration](CONFIGURATION.md).

---

### PermissionDenied

**Message:** `permission denied: <details>`

**Description:** The current role does not own the stream table's storage table
or lacks the necessary PostgreSQL privileges (SEC-1).

**Common causes:**
- Calling `alter_stream_table()` or `drop_stream_table()` as a role that does
  not own the underlying storage table
- Using `SECURITY DEFINER` functions that change the effective role

**Suggested fix:** Run the operation as the table owner, or grant ownership
with `ALTER TABLE ... OWNER TO <role>`.

---

### UpstreamTableDropped

**Message:** `upstream table dropped: OID <oid>`

**Description:** A source table referenced by the stream table's defining query
was dropped.

**Common causes:**
- `DROP TABLE` on a source table
- Table replaced via `DROP` + `CREATE` (new OID)

**Suggested fix:** Either recreate the source table with the same schema or
drop the stream table and recreate it. If `pg_trickle.block_source_ddl = true`
(default), the DROP would have been blocked in the first place.

---

### UpstreamSchemaChanged

**Message:** `upstream table schema changed: OID <oid>`

**Description:** A source table's schema was altered (e.g., column added,
dropped, or type changed) in a way that affects the defining query.

**Common causes:**
- `ALTER TABLE ... ADD/DROP/ALTER COLUMN` on a source table
- Type change on a column used in the defining query

**Suggested fix:** The stream table will be automatically reinitialized on the
next scheduler tick. If `pg_trickle.block_source_ddl = true` (default), most
schema changes are blocked proactively. Use
`pgtrickle.alter_stream_table(..., query => '...')` to update the defining
query if needed.

---

## System Errors

### LockTimeout

**Message:** `lock timeout: <details>`

**Description:** A lock required for refresh could not be acquired within the
configured timeout.

**Common causes:**
- Long-running transactions holding locks on the stream table or source tables
- Concurrent `ALTER TABLE` or `VACUUM FULL` operations
- High contention on the change buffer tables

**Suggested fix:** This error is automatically retried with exponential backoff.
If persistent, investigate long-running transactions with `pg_stat_activity`.
Consider increasing `lock_timeout` or reducing refresh frequency.

---

### ReplicationSlotError

**Message:** `replication slot error: <details>`

**Description:** An error occurred with the logical replication slot used for
WAL-based CDC.

**Common causes:**
- Replication slot dropped externally
- `wal_level` changed from `logical` to a lower level
- Slot lag exceeded `max_slot_wal_keep_size`

**Suggested fix:** Check replication slot status with
`SELECT * FROM pg_replication_slots`. Ensure `wal_level = logical`. If the slot
was dropped, pg_trickle will recreate it automatically. See
[Configuration — WAL CDC](CONFIGURATION.md).

---

### WalTransitionError

**Message:** `WAL transition error: <details>`

**Description:** An error occurred during the transition from trigger-based CDC
to WAL-based CDC.

**Common causes:**
- `wal_level` is not `logical` when `cdc_mode = 'auto'`
- Transient connection issues during the transition

**Suggested fix:** Ensure `wal_level = logical` in `postgresql.conf` if you
want WAL-based CDC. Otherwise set `pg_trickle.cdc_mode = 'trigger'` to stay
on trigger-based CDC. This error is retried automatically.

---

### SpiError

**Message:** `SPI error: <details>`

**Description:** A PostgreSQL Server Programming Interface (SPI) error occurred
during an internal query.

**Common causes:**
- Transient serialization failures under high concurrency
- Deadlocks between refresh and concurrent DML
- Connection issues in background workers
- Permanent errors: missing columns, syntax errors in generated SQL

**Suggested fix:** Transient SPI errors (deadlocks, serialization failures) are
retried automatically. Permanent errors (permission denied, missing objects)
will suspend the stream table after `max_consecutive_errors` failures. Check
`pgtrickle.check_health()` for details.

---

### SpiErrorCode

**Message:** `SPI error [<sqlstate_code>]: <details>`

**Description:** A PostgreSQL SPI error where the original SQLSTATE code has
been preserved for programmatic classification (SCAL-1). Used when
`pg_trickle.use_sqlstate_classification = true`, which is the default.

**Common causes:** Same as `SpiError` above. The difference is that this
variant carries the 5-character SQLSTATE code for locale-safe retry decisions
rather than relying on English message pattern matching.

**Suggested fix:** Inspect the SQLSTATE code. `40001` (serialization failure)
and `40P01` (deadlock) are retried automatically. `42xxx` (privilege/schema
errors) will suspend the stream table.

---

### SpiPermissionError

**Message:** `SPI permission error: <details>`

**Description:** The background worker's role lacks required permissions.

**Common causes:**
- Missing `SELECT` privilege on a source table
- Missing `INSERT`/`UPDATE`/`DELETE` privilege on the stream table
- Role used by the background worker is not the table owner

**Suggested fix:** Grant the necessary privileges to the role running
pg_trickle's background workers:
```sql
GRANT SELECT ON source_table TO pgtrickle_role;
GRANT ALL ON pgtrickle.my_stream_table TO pgtrickle_role;
```
This error does **not** count toward the consecutive error suspension limit.

---

## Watermark Errors

### WatermarkBackwardMovement

**Message:** `watermark moved backward: <details>`

**Description:** A watermark advancement was rejected because the new value is
older than the current watermark, violating monotonicity.

**Common causes:**
- Clock skew in distributed systems
- Manual watermark manipulation with an incorrect value
- Bug in watermark tracking logic

**Suggested fix:** Ensure watermark values are monotonically increasing. Check
the current watermark with `pgtrickle.get_watermark_groups()`.

---

### WatermarkGroupNotFound

**Message:** `watermark group not found: <details>`

**Description:** The specified watermark group does not exist.

**Common causes:**
- Typo in the watermark group name
- The group was deleted or never created

**Suggested fix:** List existing groups with
`pgtrickle.get_watermark_groups()`.

---

### WatermarkGroupAlreadyExists

**Message:** `watermark group already exists: <details>`

**Description:** A watermark group with this name already exists.

**Common causes:**
- Re-running a setup script without idempotent guards

**Suggested fix:** Use a different name or delete the existing group first.

---

## Transient Errors

### RefreshSkipped

**Message:** `refresh skipped: <details>`

**Description:** A refresh was skipped because a previous refresh for the same
stream table is still running.

**Common causes:**
- Slow refresh (large delta or complex query) overlapping with the next
  scheduled cycle
- Multiple manual `refresh_stream_table()` calls in parallel

**Suggested fix:** No action needed — the scheduler will retry on the next
cycle. If this happens frequently, increase the schedule interval or
investigate why refreshes are slow using `pgtrickle.explain_st()`.

This error does **not** count toward the consecutive error suspension limit.

---

## Publication Errors

### PublicationPrivilegeDenied

**Message:** PostgreSQL's publication privilege error for `<name>`

**Description:** The caller does not own the stream table or does not have
`CREATE` on the current database.

**Suggested fix:** Run the call as the stream-table owner and grant the caller
`CREATE` on the database. Grant `EXECUTE` only on the exact publication API
overload.

### PublicationBindingMismatch

**Message:** `publication binding '<publication_name>' is stale (<reason>): <details>`

**Description:** The recorded publication identity, owner, stream relation, or
relation set differs from the live object. The API rejects the operation before
changing the private catalog.

**Suggested fix:** Inspect `pg_publication` and
`pgtrickle.pgt_stream_tables`. Run `pgtrickle.lifecycle_preflight()` and do not
adopt a same-name replacement without confirming the object identity.

### PublicationAlreadyExists

**Message:** `publication already exists for stream table: <name>`

**Description:** `pgtrickle.create_publication()` was called for a stream table
that already has a downstream publication registered.

**Common causes:**
- Re-running publication setup without `IF NOT EXISTS`
- Concurrent setup in multi-process deployments

**Suggested fix:** Use `pgtrickle.drop_publication()` first if you want to
recreate it, or check the existing publication with
`SELECT * FROM pgtrickle.pgt_stream_tables WHERE outbox_enabled = true`.

---

### PublicationNotFound

**Message:** `no publication found for stream table: <name>`

**Description:** A publication management call (e.g., `drop_publication()`)
was made for a stream table that does not have a downstream publication.

**Common causes:**
- Calling `drop_publication()` on a stream table that never had one
- The publication was already dropped

**Suggested fix:** Check if the stream table has a publication before dropping
it. Use `pgtrickle.list_publications()` to see active publications.

---

## SLA Errors

### SlaTooSmall

**Message:** `SLA interval too small for available tiers: <details>`

**Description:** The requested SLA interval is smaller than the fastest
available scheduling tier.

**Common causes:**
- Specifying a sub-second SLA (e.g., `sla_seconds => 0.1`) when the
  minimum schedule is 1 second (`pg_trickle.min_schedule_seconds`)
- No available tier can satisfy the requested latency budget

**Suggested fix:** Lower `pg_trickle.min_schedule_seconds` if the cluster
supports faster scheduling, or set a larger SLA interval. Check available
tiers with `pgtrickle.list_sla_tiers()`.

---

## CDC Errors

### ChangedColsBitmaskFailed

**Message:** `failed to build changed-columns bitmask: <details>`

**Description:** CDC-1: The columnar change tracking system could not
build the bitmask expression for changed-column detection. This indicates a
table structure that prevents column-level CDC tracking.

**Common causes:**
- All columns of the source table are part of the primary key (no non-key
  columns to track changes for)
- Very wide tables exceeding the bitmask width limit
- Schema edge cases with generated or system columns

**Suggested fix:** Switch to whole-row change tracking with
`pg_trickle.columnar_cdc = false`, or restructure the source table to have
at least one non-primary-key column.

---

### PublicationRebuildFailed

**Message:** `publication rebuild failed: <details>`

**Description:** CDC-2: The logical replication publication could not
be rebuilt for a partitioned source table after a partition attach/detach or
schema change.

**Common causes:**
- Insufficient privileges to manage publications
- The publication slot was already dropped
- Partition schema changed concurrently during the rebuild

**Suggested fix:** Ensure the pg_trickle background worker role has
`CREATE PUBLICATION` privilege. Check publication status with
`SELECT * FROM pg_publication`. If the issue persists, call
`pgtrickle.reinitialize_stream_table()` to force a clean rebuild.

---

## Diagnostic Errors

### DiagnosticError

**Message:** `diagnostic error: <details>`

**Description:** ERR-1: An error occurred inside a diagnostic or
monitoring function such as `explain_refresh_mode()`, `source_gates()`, or
`watermarks()`. These surface as user-visible PostgreSQL errors with context.

**Common causes:**
- The stream table was dropped between the diagnostic call and its execution
- Missing privileges on the internal catalog tables
- An internal consistency check found unexpected state

**Suggested fix:** Verify the stream table still exists. Check that the
calling role has access to `pgtrickle.*` catalog tables. If the error
message says "stream table not found", the table may have been dropped or
its catalog entry is corrupted — use `pgtrickle.repair_stream_table()`.

---

## Snapshot Errors

### SnapshotAlreadyExists

**Message:** `snapshot already exists: <name>`

**Description:** SNAP-1: A snapshot with the given target name
already exists in the snapshot catalog.

**Common causes:**
- Re-running snapshot creation without checking for existing snapshots
- Concurrent snapshot creation with the same name

**Suggested fix:** Use a unique name for each snapshot, or drop the existing
snapshot with `pgtrickle.drop_snapshot()` before creating a new one.

---

### SnapshotSourceNotFound

**Message:** `snapshot source not found: <name>`

**Description:** SNAP-2: The stream table specified as the snapshot
source was not found in the catalog.

**Common causes:**
- Typo in the stream table name
- The stream table was dropped before the snapshot was taken

**Suggested fix:** Verify the stream table name with
`pgtrickle.list_stream_tables()`.

---

### SnapshotSchemaVersionMismatch

**Message:** `snapshot schema version mismatch: <details>`

**Description:** SNAP-3: The snapshot's schema version does not
match the current extension version, indicating the snapshot was taken with
a different version of pg_trickle.

**Common causes:**
- Upgrading pg_trickle after taking a snapshot
- Restoring a snapshot from a different version of the extension

**Suggested fix:** Re-create the snapshot after the upgrade. Old snapshots
cannot be used across major version boundaries. See
[Backup & Restore](../blog/backup-and-restore.md) for migration guidance.

---

## DuckLake / Sink Errors

These errors are raised by the DuckLake CDC source and Parquet sink subsystem
introduced in v0.65.0–v0.66.0.

### DuckLakeChangeFeedError

**Message:** `DuckLake change-feed error: <details>`

**Description:** An error occurred in the DuckLake change-feed pipeline while
reading or processing incremental changes from the DuckLake catalog.

**Common causes:**
- DuckLake catalog table is unavailable or has an unexpected schema
- Object-store connectivity issue during Parquet file fetch
- Change-feed cursor has advanced past available snapshot history

**Suggested fix:** Check object-store connectivity and DuckLake catalog
health. Review the PostgreSQL log for the underlying I/O error. If the
cursor is corrupt, reinitialize:
```sql
SELECT pgtrickle.reinitialize_stream_table('<name>');
```

---

### DucklakeParquetError

**Message:** `DuckLake sink Parquet error: <details>`

**Description:** Parquet serialisation of the differential output failed when
writing to the DuckLake sink.

**Common causes:**
- Type mismatch between the stream table column types and the Parquet schema
- Unsupported PostgreSQL type in the output (e.g. composite types, arrays of
  arrays)
- Out-of-memory during Parquet row-group encoding

**Suggested fix:** Check that all output columns have types supported by the
Parquet/Arrow type mapping. Inspect the error detail for the offending column.

---

### DucklakeUploadError

**Message:** `DuckLake sink upload error: <details>`

**Description:** The Parquet file upload to the object store failed.

**Common causes:**
- Object-store credentials expired or are missing
- Network connectivity issue between the PostgreSQL host and the object store
- Object-store bucket does not exist or the configured path is not writable

**Suggested fix:** Verify object-store credentials and connectivity:
```sql
```
Check the `last_error` column for the underlying transport error.

---

### DucklakeCatalogError

**Message:** `DuckLake catalog write error: <details>`

**Description:** Writing to the DuckLake catalog (e.g. registering a new
Parquet file or updating table metadata) failed.

**Common causes:**
- DuckLake catalog database is unreachable
- Insufficient privileges to write to the DuckLake catalog tables
- Schema version mismatch between pg_trickle and the DuckLake catalog

**Suggested fix:** Check DuckLake catalog connectivity and schema version.
Ensure the PostgreSQL user has write access to the DuckLake catalog tables.

---

### DucklakeSinkError

**Message:** `DuckLake sink error: <details>`

**Description:** Generic DuckLake sink error not covered by a more specific
variant. See the message detail for the underlying cause.

**Common causes:**
- DuckLake extension not installed in the target database
- Internal sink pipeline error

ensure the DuckLake extension is installed. Re-run
`pgtrickle.reinitialize_stream_table('<name>')` if the sink state is corrupt.

---

## Outbox / pg_tide Errors

### OutboxAlreadyEnabled

**Message:** `outbox already attached for stream table: <name>`

**Description:** `pgtrickle.attach_outbox()` was called for a stream
table that already has an outbox registered via the `pg_tide` integration.

**Common causes:**
- Re-running outbox attachment without checking for existing configuration
- Concurrent calls to `attach_outbox()` for the same stream table

**Suggested fix:** Check for existing outbox configuration with
`SELECT * FROM pgtrickle.pgt_stream_tables WHERE outbox_enabled = true`.
Use `pgtrickle.detach_outbox()` if you need to reconfigure.

---

### OutboxNotEnabled

**Message:** `outbox not attached for stream table: <name>`

**Description:** An outbox management operation was called on a stream
table that does not have a `pg_tide` outbox attached.

**Common causes:**
- Calling `detach_outbox()` or outbox-related functions on a stream table
  that never had an outbox configured

**Suggested fix:** Call `pgtrickle.attach_outbox()` first, or verify the
stream table name.

---

### PgTideMissing

**Message:** `attach_outbox() requires the pg_tide extension. Install it with: CREATE EXTENSION pg_tide;`

**Description:** The `pg_tide` extension is not installed in the
current database. The outbox/inbox functionality requires `pg_tide` to be
present.

**Common causes:**
- Calling `pgtrickle.attach_outbox()` before installing `pg_tide`
- The extension was dropped after outbox configuration

**Suggested fix:**
```sql
CREATE EXTENSION pg_tide;
```
See [pg_tide on GitHub](https://github.com/trickle-labs/pg-tide) for
installation instructions.

### PgTideUnsupportedVersion

**Message:** `pg_tide version <installed> is unsupported; supported range is <range>`

**Description:** The installed pg_tide version is outside pg_trickle's tested
outbox compatibility range: 0.47.0 through 0.53.0.

**Suggested fix:** Install a supported pg_tide version, then retry the
operation.

### PgTideUpgradeInProgress

**Message:** `pg_tide <version> is not ready for outbox integration; missing: <objects>`

**Description:** pg_tide reports a supported version, but its required outbox
API or catalog is incomplete. This normally means an extension upgrade is in
progress or did not finish.

**Suggested fix:** Complete the pg_tide upgrade and retry.

### PgTideOperationDenied

**Message:** `pg_tide denied <operation>: <detail>`

**Description:** pg_tide rejected the operation under the original caller's
identity. pg_trickle does not bypass or duplicate pg_tide authorization.

**Suggested fix:** Grant the caller the privileges required by pg_tide, or
run the operation as an authorized role.

### PgTideBindingMismatch

**Message:** `stale pg_tide outbox binding for <name>: <detail>`

**Description:** The live pg_tide outbox no longer matches the extension OID,
version, or creation timestamp recorded when it was attached.

**Suggested fix:** Detach the stale pg_trickle mapping after restoring access
to the original pg_tide catalog, then attach the intended outbox again.

---

## Placeholder Errors

### UnresolvedPlaceholder

**Message:** `unresolved placeholder '<token>' in SQL for <context>`

**Description:** A41-2: A delta SQL template still contains an unresolved
`__PGS_*__` or `__PGT_*__` placeholder token after all substitution passes
have completed. Executing SQL with a raw token would cause an obscure
PostgreSQL syntax error; this error is raised early to give a clear,
actionable message.

**Common causes:**
- A source table OID or stream table ID that is referenced in the query but
  not present in the current refresh frontier
- A bug in the delta SQL template generation where a new placeholder type
  was introduced but not registered in the substitution map
- An upstream stream table was dropped while the delta SQL was cached

**Suggested fix:** Reinitialize the affected stream table with
`pgtrickle.reinitialize_stream_table()` to force a fresh template generation.
If this persists, please [report the issue](https://github.com/trickle-labs/pg-trickle/issues).

---

## DVM Engine Errors

### DiffDepthExceeded

**Message:** `differential query depth exceeded limit of <N> levels; reduce query nesting or raise pg_trickle.max_parse_depth`

**Description:** C-7: The `diff_node()` recursion depth exceeded
the configured limit during differential query generation. This prevents stack
overflows on pathologically deeply-nested queries.

**Common causes:**
- Queries with more than `pg_trickle.max_parse_depth` levels of nested
  subqueries, CTEs, or operator trees
- Highly chained view references that expand into deep nesting

**Suggested fix:** Simplify the query by reducing nesting depth. If the query
is legitimately deep, raise `pg_trickle.max_parse_depth`:
```sql
SET pg_trickle.max_parse_depth = 128;
```
Alternatively, use `refresh_mode => 'FULL'` to bypass the differential engine.

---

### DiffCteCountExceeded

**Message:** `differential query CTE count exceeded limit of <N>; simplify the query or raise pg_trickle.max_diff_ctes`

**Description:** R-7: The number of CTEs generated during
differentiation exceeded the configured limit (`pg_trickle.max_diff_ctes`).
This prevents unbounded memory growth for queries that produce thousands of
intermediate CTEs.

**Common causes:**
- Multi-source queries with many join paths where each path generates
  independent delta CTEs
- Queries with many aggregation levels each requiring separate delta expressions

**Suggested fix:** Simplify the query or raise the CTE limit:
```sql
SET pg_trickle.max_diff_ctes = 500;
```
Alternatively, use `refresh_mode => 'FULL'`.

---

### StSourceFrontierMissing

**Message:** `upstream stream table (pgt_id=<id>) not found in refresh frontier; the source stream table may have been dropped — call pgtrickle.reinitialize_stream_table() to recover`

**Description:** C-4: A stream-table-to-stream-table source frontier
entry is missing from the refresh frontier, indicating the upstream stream
table was dropped while a downstream stream table still references it.

**Common causes:**
- An upstream stream table was dropped directly (bypassing the dependency
  check) while a downstream stream table's delta SQL still references it
- Database restored from backup at a point before the upstream ST was
  recreated

**Suggested fix:**
```sql
SELECT pgtrickle.reinitialize_stream_table('downstream_stream_table');
```
If the upstream stream table was intentionally removed, drop and recreate
the downstream one with an updated defining query.

---

## Internal Errors

### InternalError

**Message:** `internal error: <details>`

**Description:** An unexpected internal error that indicates a bug in
pg_trickle.

**Common causes:**
- This should not happen in normal operation

**Suggested fix:** Please [report the issue](https://github.com/trickle-labs/pg-trickle/issues)
with the full error message, your PostgreSQL version, and pg_trickle version.
Include the output of `pgtrickle.check_health()` and the relevant PostgreSQL
log entries.

---

## v0.23.0 — DVM Scaling Errors

### change_buffer_overflow Alert

**Alert:** `pg_trickle_alert change_buffer_overflow`

**Description:** A source table's change buffer exceeded the
`pg_trickle.max_change_buffer_alert_rows` threshold during refresh.

**Common causes:**
- High write rate on source tables
- Slow or blocked refresh cycles
- WAL accumulation during cross-query consistency checks

**Suggested fix:**
- Increase `pg_trickle.max_change_buffer_alert_rows` if the write rate is expected
- Check for long-running transactions blocking the refresh
- Consider increasing refresh frequency or using FULL mode for affected tables

### DIFF-Slower-Than-FULL Warning

**Warning:** `[pg_trickle] DIFF refresh for <table> took Xms vs last FULL Yms — DIFF is Nx slower`

> Note: `DIFF` is the abbreviated form of `DIFFERENTIAL` used in log output.

**Description:** Emitted when `pg_trickle.log_delta_sql = on` and a DIFFERENTIAL
refresh takes longer than the last recorded FULL refresh.

**Common causes:**
- Query complexity exceeds the DVM engine's O(Δ) capacity (see
  [PERFORMANCE_COOKBOOK.md §13](PERFORMANCE_COOKBOOK.md#13-dvm-query-complexity-limits))
- Stale planner statistics on change buffer tables
- work_mem too low for hash joins in the delta SQL

**Suggested fix:**
- Check the delta SQL via `pgtrickle.explain_diff_sql('<table>')`
- Increase `pg_trickle.delta_work_mem` for the affected database
- Switch to AUTO or FULL mode for queries with known threshold-collapse patterns

---

## See Also

- [FAQ — Troubleshooting](FAQ.md#troubleshooting)
- [SQL Reference](SQL_REFERENCE.md)
- [Configuration Reference](CONFIGURATION.md)
