//! SNAP-1/2/3 (v0.27.0): Stream-table snapshot & point-in-time restore API.
//!
//! Provides `snapshot_stream_table()`, `restore_from_snapshot()`,
//! `list_snapshots()`, and `drop_snapshot()` SQL functions.
//!
//! # Design
//!
//! A snapshot is an ordinary PostgreSQL table in the `pgtrickle` schema with
//! the naming convention `snapshot_<st_name>_<epoch_ms>`. Each snapshot row
//! matches the storage schema of the stream table plus three metadata columns:
//!   - `__pgt_snapshot_version TEXT` — extension version at snapshot time
//!   - `__pgt_frontier        JSONB` — frontier at snapshot time
//!   - `__pgt_snapshotted_at  TIMESTAMPTZ` — wall clock at snapshot time
//!
//! A catalog table `pgtrickle.pgt_snapshots` records each snapshot's metadata
//! so `list_snapshots()` can return size and row count information.

use super::helpers::{
    QualifiedIdentifier, RelationIdentity, check_stream_table_ownership, outer_user_id,
    parse_qualified_identifier_with_current_schema, resolve_relation_identity,
    transfer_output_table_ownership,
};
use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;
use pgrx::prelude::*;

// ── STAB-1 (v0.30.0): SubTransaction RAII helper ─────────────────────────
//
// Wraps the CREATE TABLE AS + catalog INSERT in snapshot_stream_table_impl and
// the TRUNCATE + INSERT in restore_from_snapshot_impl in a PostgreSQL internal
// sub-transaction.  On drop (without explicit commit), rolls back automatically
// so no orphan tables or truncated storage tables are left behind on crash.

struct SnapSubTransaction {
    old_cxt: pgrx::pg_sys::MemoryContext,
    old_owner: pgrx::pg_sys::ResourceOwner,
    finished: bool,
}

impl SnapSubTransaction {
    fn begin() -> Self {
        // SAFETY: Called within a PostgreSQL transaction (SQL function context).
        // CurrentMemoryContext and CurrentResourceOwner are always valid here.
        let old_cxt = unsafe { pgrx::pg_sys::CurrentMemoryContext };
        let old_owner = unsafe { pgrx::pg_sys::CurrentResourceOwner };
        // SAFETY: BeginInternalSubTransaction sets up a sub-transaction.
        unsafe { pgrx::pg_sys::BeginInternalSubTransaction(std::ptr::null()) };
        Self {
            old_cxt,
            old_owner,
            finished: false,
        }
    }

    fn commit(mut self) {
        // SAFETY: Commits the sub-transaction; restores the outer context.
        unsafe {
            pgrx::pg_sys::ReleaseCurrentSubTransaction();
            pgrx::pg_sys::MemoryContextSwitchTo(self.old_cxt);
            pgrx::pg_sys::CurrentResourceOwner = self.old_owner;
        }
        self.finished = true;
    }

    fn rollback(mut self) {
        // SAFETY: Rolls back the sub-transaction; restores the outer context.
        unsafe {
            pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pgrx::pg_sys::MemoryContextSwitchTo(self.old_cxt);
            pgrx::pg_sys::CurrentResourceOwner = self.old_owner;
        }
        self.finished = true;
    }
}

impl Drop for SnapSubTransaction {
    fn drop(&mut self) {
        if !self.finished {
            // Auto-rollback for panic safety.
            // SAFETY: Same invariants as rollback().
            unsafe {
                pgrx::pg_sys::RollbackAndReleaseCurrentSubTransaction();
                pgrx::pg_sys::MemoryContextSwitchTo(self.old_cxt);
                pgrx::pg_sys::CurrentResourceOwner = self.old_owner;
            }
        }
    }
}

// ── Internal helpers ───────────────────────────────────────────────────────

const SNAPSHOT_COMMENT_PREFIX: &str = "pgtrickle:snapshot:v1|";
const SNAPSHOT_CATALOG_MIGRATION_SQL: &str = "\
ALTER TABLE pgtrickle.pgt_snapshots\n\
    ADD COLUMN IF NOT EXISTS snapshot_relid oid,\n\
    ADD COLUMN IF NOT EXISTS snapshot_provenance_token text,\n\
    ADD COLUMN IF NOT EXISTS created_by_role_oid oid;\n\
\n\
\n\
CREATE UNIQUE INDEX IF NOT EXISTS idx_pgt_snapshots_snapshot_relid\n\
    ON pgtrickle.pgt_snapshots (snapshot_relid);";
#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotCatalogRow {
    pgt_id: i64,
    snapshot_schema: String,
    snapshot_table: String,
    snapshot_version: String,
    frontier_json: Option<String>,
    snapshot_relid: pg_sys::Oid,
    snapshot_provenance_token: String,
    created_by_role_oid: pg_sys::Oid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotProvenance {
    pgt_id: i64,
    pgt_relid: u32,
    snapshot_relid: u32,
    created_by_role_oid: u32,
    token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpectedStreamBinding {
    pgt_id: i64,
    pgt_relid: u32,
}

impl SnapshotProvenance {
    fn new(
        meta: &StreamTableMeta,
        snapshot_relid: pg_sys::Oid,
        created_by_role_oid: pg_sys::Oid,
        token: String,
    ) -> Self {
        Self {
            pgt_id: meta.pgt_id,
            pgt_relid: meta.pgt_relid.to_u32(),
            snapshot_relid: snapshot_relid.to_u32(),
            created_by_role_oid: created_by_role_oid.to_u32(),
            token,
        }
    }

    fn encode_comment(&self) -> String {
        format!(
            "{SNAPSHOT_COMMENT_PREFIX}pgt_id={}|pgt_relid={}|snapshot_relid={}|created_by_role_oid={}|token={}",
            self.pgt_id, self.pgt_relid, self.snapshot_relid, self.created_by_role_oid, self.token
        )
    }

    fn parse(comment: &str) -> Result<Self, PgTrickleError> {
        let payload = comment
            .strip_prefix(SNAPSHOT_COMMENT_PREFIX)
            .ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "relation is missing pg_trickle snapshot provenance".to_string(),
                )
            })?;

        let mut pgt_id = None;
        let mut pgt_relid = None;
        let mut snapshot_relid = None;
        let mut created_by_role_oid = None;
        let mut token = None;

        for entry in payload.split('|') {
            let (key, value) = entry.split_once('=').ok_or_else(|| {
                PgTrickleError::InvalidArgument(format!(
                    "invalid snapshot provenance entry: {entry}"
                ))
            })?;

            match key {
                "pgt_id" => {
                    pgt_id = Some(value.parse::<i64>().map_err(|_| {
                        PgTrickleError::InvalidArgument(format!(
                            "invalid snapshot provenance pgt_id: {value}"
                        ))
                    })?)
                }
                "pgt_relid" => {
                    pgt_relid = Some(value.parse::<u32>().map_err(|_| {
                        PgTrickleError::InvalidArgument(format!(
                            "invalid snapshot provenance pgt_relid: {value}"
                        ))
                    })?)
                }
                "snapshot_relid" => {
                    snapshot_relid = Some(value.parse::<u32>().map_err(|_| {
                        PgTrickleError::InvalidArgument(format!(
                            "invalid snapshot provenance snapshot_relid: {value}"
                        ))
                    })?)
                }
                "created_by_role_oid" => {
                    created_by_role_oid = Some(value.parse::<u32>().map_err(|_| {
                        PgTrickleError::InvalidArgument(format!(
                            "invalid snapshot provenance created_by_role_oid: {value}"
                        ))
                    })?)
                }
                "token" => {
                    if value.is_empty() {
                        return Err(PgTrickleError::InvalidArgument(
                            "snapshot provenance token must not be empty".to_string(),
                        ));
                    }
                    token = Some(value.to_string());
                }
                other => {
                    return Err(PgTrickleError::InvalidArgument(format!(
                        "unknown snapshot provenance key: {other}"
                    )));
                }
            }
        }

        Ok(Self {
            pgt_id: pgt_id.ok_or_else(|| {
                PgTrickleError::InvalidArgument("snapshot provenance is missing pgt_id".to_string())
            })?,
            pgt_relid: pgt_relid.ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "snapshot provenance is missing pgt_relid".to_string(),
                )
            })?,
            snapshot_relid: snapshot_relid.ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "snapshot provenance is missing snapshot_relid".to_string(),
                )
            })?,
            created_by_role_oid: created_by_role_oid.ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "snapshot provenance is missing created_by_role_oid".to_string(),
                )
            })?,
            token: token.ok_or_else(|| {
                PgTrickleError::InvalidArgument("snapshot provenance is missing token".to_string())
            })?,
        })
    }
}

fn resolve_stream_name(name: &str) -> Result<QualifiedIdentifier, PgTrickleError> {
    parse_qualified_identifier_with_current_schema(name)
}

fn resolve_snapshot_name(name: &str) -> Result<QualifiedIdentifier, PgTrickleError> {
    parse_qualified_identifier_with_current_schema(name)
}

/// Build a safe snapshot table name from the ST name and current timestamp (ms).
pub(super) fn auto_snapshot_table_name(st_name: &str) -> String {
    let epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let safe_name = st_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("pgtrickle.snapshot_{}_{}", safe_name, epoch_ms)
}

fn generate_snapshot_provenance_token() -> Result<String, PgTrickleError> {
    Spi::get_one::<String>(
        "SELECT md5(random()::text || clock_timestamp()::text || \
                txid_current()::text || pg_backend_pid()::text)",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InternalError(
            "PostgreSQL did not return a snapshot provenance token".to_string(),
        )
    })
}

/// CORR-3 (v0.30.0): Build a comma-separated list of user-visible column names from
/// the snapshot table, excluding pg_trickle metadata columns.
///
/// Uses `pg_attribute` catalog walk instead of `SELECT * EXCEPT (...)` so the
/// function works on all PG 18.x minor versions without PG-minor sensitivity.
fn build_user_column_list(
    relid: pg_sys::Oid,
    src_schema: &str,
    src_table: &str,
) -> Result<String, PgTrickleError> {
    let skip: &[&str] = &[
        "__pgt_snapshot_version",
        "__pgt_frontier",
        "__pgt_snapshotted_at",
    ];

    let cols: Vec<String> = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT a.attname::text \
                 FROM pg_catalog.pg_attribute a \
                 WHERE a.attrelid = $1 \
                   AND a.attnum   > 0 \
                   AND NOT a.attisdropped \
                 ORDER BY a.attnum",
                None,
                &[relid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows {
            let name: String = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_default();
            if !name.is_empty() && !skip.contains(&name.as_str()) {
                out.push(format!("\"{}\"", name.replace('"', "\"\"")));
            }
        }
        Ok::<_, PgTrickleError>(out)
    })?;

    if cols.is_empty() {
        return Err(PgTrickleError::SpiError(format!(
            "no user columns found in snapshot table {}.{}",
            src_schema, src_table
        )));
    }
    Ok(cols.join(", "))
}

fn ensure_snapshot_catalog_support() -> Result<(), PgTrickleError> {
    let present_columns: Vec<String> = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT a.attname::text \
                 FROM pg_catalog.pg_attribute a \
                 JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = 'pgtrickle' \
                   AND c.relname = 'pgt_snapshots' \
                   AND a.attnum > 0 \
                   AND NOT a.attisdropped \
                   AND a.attname IN ('snapshot_relid', 'snapshot_provenance_token', 'created_by_role_oid')",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut columns = Vec::new();
        for row in rows {
            if let Some(column) = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            {
                columns.push(column);
            }
        }
        Ok::<_, PgTrickleError>(columns)
    })?;

    let mut missing = Vec::new();
    for required in [
        "snapshot_relid",
        "snapshot_provenance_token",
        "created_by_role_oid",
    ] {
        if !present_columns.iter().any(|column| column == required) {
            missing.push(required);
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(PgTrickleError::InternalError(format!(
            "pgtrickle.pgt_snapshots is missing required WP3 columns [{}]. Apply this migration SQL before using snapshot APIs:\n{}",
            missing.join(", "),
            SNAPSHOT_CATALOG_MIGRATION_SQL
        )))
    }
}

fn load_snapshot_catalog_row(
    qualified: &QualifiedIdentifier,
) -> Result<SnapshotCatalogRow, PgTrickleError> {
    ensure_snapshot_catalog_support()?;
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT pgt_id, snapshot_schema, snapshot_table, snapshot_version, \
                        frontier::text, snapshot_relid, snapshot_provenance_token, created_by_role_oid \
                 FROM pgtrickle.pgt_snapshots \
                 WHERE snapshot_schema = $1 AND snapshot_table = $2",
                None,
                &[qualified.schema().into(), qualified.name().into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        if rows.is_empty() {
            return Err(PgTrickleError::SnapshotSourceNotFound(format!(
                "{}.{}",
                qualified.schema(),
                qualified.name()
            )));
        }

        let row = rows.first();
        Ok(SnapshotCatalogRow {
            pgt_id: row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("missing snapshot pgt_id".to_string())
                })?,
            snapshot_schema: row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("missing snapshot schema".to_string())
                })?,
            snapshot_table: row
                .get::<String>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("missing snapshot table".to_string())
                })?,
            snapshot_version: row
                .get::<String>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("missing snapshot version".to_string())
                })?,
            frontier_json: row
                .get::<String>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?,
            snapshot_relid: row
                .get::<pg_sys::Oid>(6)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument(
                        "snapshot catalog row has no provenance relation OID; recreate or repair it"
                            .to_string(),
                    )
                })?,
            snapshot_provenance_token: row
                .get::<String>(7)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument(
                        "snapshot catalog row has no provenance token; recreate or repair it"
                            .to_string(),
                    )
                })?,
            created_by_role_oid: row
                .get::<pg_sys::Oid>(8)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument(
                        "snapshot catalog row has no creator role; recreate or repair it"
                            .to_string(),
                    )
                })?,
        })
    })
}

fn read_snapshot_provenance(relid: pg_sys::Oid) -> Result<SnapshotProvenance, PgTrickleError> {
    let comment = Spi::get_one_with_args::<String>(
        "SELECT pg_catalog.obj_description($1, 'pg_class')::text",
        &[relid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InvalidArgument("snapshot table is missing provenance comment".to_string())
    })?;

    SnapshotProvenance::parse(&comment)
}

fn ensure_snapshot_metadata_columns(relid: pg_sys::Oid) -> Result<(), PgTrickleError> {
    let present_columns: Vec<String> = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT a.attname::text \
                 FROM pg_catalog.pg_attribute a \
                 WHERE a.attrelid = $1 \
                   AND a.attnum > 0 \
                   AND NOT a.attisdropped \
                   AND a.attname IN ('__pgt_snapshot_version', '__pgt_frontier', '__pgt_snapshotted_at')",
                None,
                &[relid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let mut columns = Vec::new();
        for row in rows {
            if let Some(column) = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            {
                columns.push(column);
            }
        }
        Ok::<_, PgTrickleError>(columns)
    })?;

    let mut missing = Vec::new();
    for required in [
        "__pgt_snapshot_version",
        "__pgt_frontier",
        "__pgt_snapshotted_at",
    ] {
        if !present_columns.iter().any(|column| column == required) {
            missing.push(required);
        }
    }

    if missing.is_empty() {
        Ok(())
    } else {
        Err(PgTrickleError::InvalidArgument(format!(
            "relation is not a valid pg_trickle snapshot: missing metadata columns [{}]",
            missing.join(", "),
        )))
    }
}

fn validate_snapshot_relation_binding(
    relation: &RelationIdentity,
    catalog: &SnapshotCatalogRow,
    provenance: &SnapshotProvenance,
    expected_stream: Option<ExpectedStreamBinding>,
) -> Result<(), PgTrickleError> {
    if relation.relkind != 'r' {
        return Err(PgTrickleError::InvalidArgument(format!(
            "relation {}.{} is not a heap table snapshot",
            relation.qualified.schema(),
            relation.qualified.name()
        )));
    }

    if relation.relid != catalog.snapshot_relid {
        return Err(PgTrickleError::InvalidArgument(format!(
            "snapshot relation identity mismatch for {}.{}: catalog relid {} != current relid {}",
            catalog.snapshot_schema,
            catalog.snapshot_table,
            catalog.snapshot_relid.to_u32(),
            relation.relid.to_u32(),
        )));
    }

    if relation.relowner != catalog.created_by_role_oid {
        return Err(PgTrickleError::InvalidArgument(format!(
            "snapshot owner mismatch for {}.{}: catalog owner {} != current owner {}",
            catalog.snapshot_schema,
            catalog.snapshot_table,
            catalog.created_by_role_oid.to_u32(),
            relation.relowner.to_u32(),
        )));
    }

    if let Some(expected_stream) = expected_stream
        && (catalog.pgt_id != expected_stream.pgt_id
            || provenance.pgt_id != expected_stream.pgt_id
            || provenance.pgt_relid != expected_stream.pgt_relid)
    {
        return Err(PgTrickleError::InvalidArgument(format!(
            "snapshot {}.{} does not belong to stream table pgt_id={} relid={}",
            catalog.snapshot_schema,
            catalog.snapshot_table,
            expected_stream.pgt_id,
            expected_stream.pgt_relid
        )));
    }

    if provenance.pgt_id != catalog.pgt_id
        || provenance.snapshot_relid != catalog.snapshot_relid.to_u32()
        || provenance.created_by_role_oid != catalog.created_by_role_oid.to_u32()
        || provenance.token != catalog.snapshot_provenance_token
    {
        return Err(PgTrickleError::InvalidArgument(format!(
            "snapshot provenance mismatch for {}.{}",
            catalog.snapshot_schema, catalog.snapshot_table
        )));
    }

    Ok(())
}

fn validate_snapshot_version(snapshot_version: &str) -> Result<(), PgTrickleError> {
    let current_version = env!("CARGO_PKG_VERSION");
    let snapshot_major = snapshot_version.split('.').next().unwrap_or("0");
    let current_major = current_version.split('.').next().unwrap_or("0");
    if snapshot_major != current_major {
        return Err(PgTrickleError::SnapshotSchemaVersionMismatch(format!(
            "snapshot version {snapshot_version} incompatible with current {current_version} (major version differs)"
        )));
    }
    Ok(())
}

// ── SNAP-1: snapshot_stream_table ─────────────────────────────────────────

/// SNAP-1 (v0.27.0): Export the current content of a stream table into an
/// archival snapshot table.
///
/// The snapshot table is created in the `pgtrickle` schema with the naming
/// convention `snapshot_<name>_<epoch_ms>` unless `p_target` is given.
/// Returns the fully-qualified name of the created snapshot table.
#[pg_extern(schema = "pgtrickle")]
pub fn snapshot_stream_table(p_name: &str, p_target: default!(Option<&str>, "NULL")) -> String {
    snapshot_stream_table_impl(p_name, p_target).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn snapshot_stream_table_impl(name: &str, target: Option<&str>) -> Result<String, PgTrickleError> {
    ensure_snapshot_catalog_support()?;

    let stream_name = resolve_stream_name(name)?;
    let meta = StreamTableMeta::get_by_name(stream_name.schema(), stream_name.name())?;
    check_stream_table_ownership(meta.pgt_relid, stream_name.schema(), stream_name.name())?;

    let snapshot_name = match target {
        Some(target_name) => resolve_snapshot_name(target_name)?,
        None => resolve_snapshot_name(&auto_snapshot_table_name(&meta.pgt_name))?,
    };

    if resolve_relation_identity(snapshot_name.clone())?.is_some() {
        return Err(PgTrickleError::SnapshotAlreadyExists(
            snapshot_name.quoted(),
        ));
    }

    let storage_fqn = stream_name.quoted();
    let snapshot_fqn = snapshot_name.quoted();

    let frontier_json = meta
        .frontier
        .as_ref()
        .map(|f| serde_json::to_string(f).unwrap_or_else(|_| "null".to_string()))
        .unwrap_or_else(|| "null".to_string());

    let ext_ver = env!("CARGO_PKG_VERSION");
    let snapshot_provenance_token = generate_snapshot_provenance_token()?;
    let created_by_role_oid = outer_user_id();

    // CREATE TABLE AS SELECT — copy all rows plus metadata columns
    let create_sql = format!(
        "CREATE TABLE {} AS \
         SELECT *, \
                $1::text        AS __pgt_snapshot_version, \
                $2::jsonb       AS __pgt_frontier, \
                now()           AS __pgt_snapshotted_at \
         FROM {}",
        snapshot_fqn, storage_fqn
    );

    // STAB-1 (v0.30.0): Wrap CREATE TABLE AS + catalog INSERT in a SubTransaction.
    // If the catalog INSERT fails, the subtransaction rolls back, cleaning up
    // the orphan snapshot table automatically.
    let subtxn = SnapSubTransaction::begin();
    let create_result = Spi::run_with_args(
        &create_sql,
        &[ext_ver.into(), frontier_json.as_str().into()],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("snapshot create failed: {e}")));

    if let Err(e) = create_result {
        subtxn.rollback();
        return Err(e);
    }

    let snapshot_relation = match resolve_relation_identity(snapshot_name.clone())? {
        Some(relation) => relation,
        None => {
            subtxn.rollback();
            return Err(PgTrickleError::InternalError(format!(
                "snapshot relation {} was created but could not be resolved from pg_class",
                snapshot_fqn
            )));
        }
    };

    if snapshot_relation.relkind != 'r' {
        subtxn.rollback();
        return Err(PgTrickleError::InternalError(format!(
            "snapshot relation {} has unexpected relkind '{}'",
            snapshot_fqn, snapshot_relation.relkind
        )));
    }

    let provenance = SnapshotProvenance::new(
        &meta,
        snapshot_relation.relid,
        created_by_role_oid,
        snapshot_provenance_token.clone(),
    );

    let comment_sql = format!(
        "COMMENT ON TABLE {} IS '{}'",
        snapshot_fqn,
        provenance.encode_comment().replace('\'', "''")
    );
    if let Err(e) = Spi::run(&comment_sql) {
        subtxn.rollback();
        return Err(PgTrickleError::SpiError(format!(
            "snapshot provenance comment failed: {e}"
        )));
    }

    if let Err(e) = transfer_output_table_ownership(snapshot_name.schema(), snapshot_name.name()) {
        subtxn.rollback();
        return Err(e);
    }

    let owned_snapshot_relation = match resolve_relation_identity(snapshot_name.clone())? {
        Some(relation) => relation,
        None => {
            subtxn.rollback();
            return Err(PgTrickleError::InternalError(format!(
                "snapshot relation {} disappeared before catalog registration",
                snapshot_fqn
            )));
        }
    };

    if owned_snapshot_relation.relowner != created_by_role_oid {
        subtxn.rollback();
        return Err(PgTrickleError::InternalError(format!(
            "snapshot relation {} owner {} does not match creator {} after transfer",
            snapshot_fqn,
            owned_snapshot_relation.relowner.to_u32(),
            created_by_role_oid.to_u32()
        )));
    }

    let insert_result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgtrickle.pgt_snapshots \
         (pgt_id, snapshot_schema, snapshot_table, snapshot_version, frontier, created_at, \
          snapshot_relid, snapshot_provenance_token, created_by_role_oid) \
         VALUES ($1, $2, $3, $4, $5::jsonb, now(), $6, $7, $8) \
         RETURNING snapshot_id",
        &[
            meta.pgt_id.into(),
            snapshot_name.schema().into(),
            snapshot_name.name().into(),
            ext_ver.into(),
            frontier_json.as_str().into(),
            owned_snapshot_relation.relid.into(),
            snapshot_provenance_token.as_str().into(),
            created_by_role_oid.into(),
        ],
    )
    .map_err(|e| {
        PgTrickleError::SpiError(format!(
            "[pg_trickle] SNAP-1: snapshot table created but catalog INSERT failed \
             (snapshot at {} was rolled back): {}",
            snapshot_fqn, e
        ))
    })?;

    if insert_result.is_none() {
        subtxn.rollback();
        return Err(PgTrickleError::InternalError(format!(
            "snapshot catalog insert for {} returned no snapshot_id",
            snapshot_fqn
        )));
    }

    subtxn.commit();

    pgrx::log!(
        "[pg_trickle] SNAP-1: snapshot created for '{}.{}' → {}",
        stream_name.schema(),
        stream_name.name(),
        snapshot_fqn
    );

    Ok(snapshot_fqn)
}

// ── SNAP-2: restore_from_snapshot ─────────────────────────────────────────

/// SNAP-2 (v0.27.0): Rehydrate a stream table from an archival snapshot.
///
/// The stream table must already be registered. After restore the frontier is
/// set to the snapshot's frontier so the next refresh cycle is DIFFERENTIAL
/// (skipping the initial FULL re-scan).
#[pg_extern(schema = "pgtrickle")]
pub fn restore_from_snapshot(p_name: &str, p_source: &str) {
    restore_from_snapshot_impl(p_name, p_source).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn restore_from_snapshot_impl(name: &str, source: &str) -> Result<(), PgTrickleError> {
    let stream_name = resolve_stream_name(name)?;
    let meta = StreamTableMeta::get_by_name(stream_name.schema(), stream_name.name())?;
    check_stream_table_ownership(meta.pgt_relid, stream_name.schema(), stream_name.name())?;

    let snapshot_name = resolve_snapshot_name(source)?;
    let snapshot_catalog = load_snapshot_catalog_row(&snapshot_name)?;
    validate_snapshot_version(&snapshot_catalog.snapshot_version)?;

    let snapshot_relation = resolve_relation_identity(snapshot_name.clone())?
        .ok_or_else(|| PgTrickleError::SnapshotSourceNotFound(snapshot_name.quoted()))?;
    let snapshot_provenance = read_snapshot_provenance(snapshot_relation.relid)?;
    validate_snapshot_relation_binding(
        &snapshot_relation,
        &snapshot_catalog,
        &snapshot_provenance,
        Some(ExpectedStreamBinding {
            pgt_id: meta.pgt_id,
            pgt_relid: meta.pgt_relid.to_u32(),
        }),
    )?;
    ensure_snapshot_metadata_columns(snapshot_relation.relid)?;

    let src_fqn = snapshot_name.quoted();
    let storage_fqn = stream_name.quoted();

    // STAB-1 (v0.30.0): Wrap TRUNCATE + INSERT in a SubTransaction with an
    // exclusive lock acquired before the TRUNCATE, so no orphan/truncated
    // storage table is left on crash and concurrent refreshes are blocked.
    let subtxn = SnapSubTransaction::begin();

    // nosemgrep: rust.spi.run.dynamic-format — DDL cannot be parameterized; storage_fqn is a double-quoted and escaped catalog identifier.
    let lock_result = Spi::run(&format!(
        "LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
        storage_fqn
    ))
    .map_err(|e| PgTrickleError::SpiError(format!("restore lock failed: {e}")));

    if let Err(e) = lock_result {
        subtxn.rollback();
        return Err(e);
    }

    // Truncate, then bulk-insert from snapshot (excluding metadata columns)
    let truncate_result =
        Spi::run(&format!("TRUNCATE {}", storage_fqn)) // nosemgrep: rust.spi.run.dynamic-format
            .map_err(|e| PgTrickleError::SpiError(format!("truncate failed: {e}")));

    if let Err(e) = truncate_result {
        subtxn.rollback();
        return Err(e);
    }

    // CORR-3 (v0.30.0): Build explicit column list from pg_attribute catalog walk,
    // eliminating PG-minor-version sensitivity of SELECT * EXCEPT (...).
    let user_cols = match build_user_column_list(
        snapshot_relation.relid,
        snapshot_name.schema(),
        snapshot_name.name(),
    ) {
        Ok(cols) => cols,
        Err(e) => {
            subtxn.rollback();
            return Err(e);
        }
    };
    let insert_sql = format!(
        "INSERT INTO {} ({}) \
         SELECT {} FROM {}",
        storage_fqn, user_cols, user_cols, src_fqn
    );
    let insert_result =
        Spi::run(&insert_sql) // nosemgrep: rust.spi.run.dynamic-format — DDL/DML cannot be parameterized for table names; storage_fqn and src_fqn are double-quoted and escaped catalog identifiers.
            .map_err(|e| PgTrickleError::SpiError(format!("restore insert failed: {e}")));

    if let Err(e) = insert_result {
        subtxn.rollback();
        return Err(e);
    }

    // Restore frontier so next refresh is DIFFERENTIAL (not FULL)
    if let Some(fj) = snapshot_catalog.frontier_json.as_deref() {
        let frontier_result = Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET frontier = $1::jsonb, is_populated = true \
             WHERE pgt_id = $2",
            &[fj.into(), meta.pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(format!("frontier restore failed: {e}")));

        if let Err(e) = frontier_result {
            subtxn.rollback();
            return Err(e);
        }
    }

    subtxn.commit();

    // Signal the DAG to pick up the frontier change
    crate::shmem::signal_dag_invalidation(meta.pgt_id);

    pgrx::log!(
        "[pg_trickle] SNAP-2: restored '{}.{}' from '{}'",
        stream_name.schema(),
        stream_name.name(),
        snapshot_name.quoted()
    );

    Ok(())
}

// ── SNAP-3a: list_snapshots ────────────────────────────────────────────────

/// SNAP-3 (v0.27.0): List all archival snapshot tables for a stream table.
///
/// Returns one row per snapshot ordered by creation time descending.
#[pg_extern(schema = "pgtrickle")]
#[allow(clippy::type_complexity)]
pub fn list_snapshots(
    p_name: &str,
) -> TableIterator<
    'static,
    (
        name!(snapshot_table, Option<String>),
        name!(created_at, Option<TimestampWithTimeZone>),
        name!(row_count, Option<i64>),
        name!(frontier, Option<pgrx::JsonB>),
        name!(size_bytes, Option<i64>),
    ),
> {
    let rows = list_snapshots_impl(p_name);
    TableIterator::new(rows)
}

#[allow(clippy::type_complexity)]
fn list_snapshots_impl(
    name: &str,
) -> Vec<(
    Option<String>,
    Option<TimestampWithTimeZone>,
    Option<i64>,
    Option<pgrx::JsonB>,
    Option<i64>,
)> {
    let stream_name = match resolve_stream_name(name) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let meta = match StreamTableMeta::get_by_name(stream_name.schema(), stream_name.name()) {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };

    Spi::connect(|client| {
        let rows_result = client.select(
            "SELECT \
               format('%I.%I', s.snapshot_schema, s.snapshot_table) AS snapshot_table, \
               s.created_at, \
               NULL::bigint AS row_count, \
               s.frontier, \
               pg_total_relation_size( \
                 (format('%I.%I', s.snapshot_schema, s.snapshot_table))::regclass \
               ) AS size_bytes \
             FROM pgtrickle.pgt_snapshots s \
             WHERE s.pgt_id = $1 \
             ORDER BY s.created_at DESC",
            None,
            &[meta.pgt_id.into()],
        );

        match rows_result {
            Ok(rows) => {
                let mut out = Vec::new();
                for row in rows {
                    let snap_table = row.get::<String>(1).unwrap_or(None);
                    let created_at = row.get::<TimestampWithTimeZone>(2).unwrap_or(None);
                    let row_count = row.get::<i64>(3).unwrap_or(None);
                    let frontier_json = row.get::<pgrx::JsonB>(4).unwrap_or(None);
                    let size_bytes = row.get::<i64>(5).unwrap_or(None);
                    out.push((snap_table, created_at, row_count, frontier_json, size_bytes));
                }
                out
            }
            Err(_) => Vec::new(),
        }
    })
}

// ── SNAP-3b: drop_snapshot ────────────────────────────────────────────────

/// SNAP-3 (v0.27.0): Drop an archival snapshot table.
///
/// Removes the snapshot table and its catalog row from `pgtrickle.pgt_snapshots`.
#[pg_extern(schema = "pgtrickle")]
pub fn drop_snapshot(p_snapshot_table: &str) {
    drop_snapshot_impl(p_snapshot_table).unwrap_or_else(|e| pgrx::error!("{}", e))
}

fn drop_snapshot_impl(snapshot_table: &str) -> Result<(), PgTrickleError> {
    let snapshot_name = resolve_snapshot_name(snapshot_table)?;
    let snapshot_catalog = load_snapshot_catalog_row(&snapshot_name)?;
    let stream_meta = StreamTableMeta::get_by_id(snapshot_catalog.pgt_id)?.ok_or_else(|| {
        PgTrickleError::InternalError(format!(
            "snapshot {} belongs to missing stream table pgt_id={}",
            snapshot_name.quoted(),
            snapshot_catalog.pgt_id
        ))
    })?;
    check_stream_table_ownership(
        stream_meta.pgt_relid,
        &stream_meta.pgt_schema,
        &stream_meta.pgt_name,
    )?;

    let snapshot_relation = resolve_relation_identity(snapshot_name.clone())?
        .ok_or_else(|| PgTrickleError::SnapshotSourceNotFound(snapshot_name.quoted()))?;
    let snapshot_provenance = read_snapshot_provenance(snapshot_relation.relid)?;
    validate_snapshot_relation_binding(
        &snapshot_relation,
        &snapshot_catalog,
        &snapshot_provenance,
        Some(ExpectedStreamBinding {
            pgt_id: stream_meta.pgt_id,
            pgt_relid: stream_meta.pgt_relid.to_u32(),
        }),
    )?;
    ensure_snapshot_metadata_columns(snapshot_relation.relid)?;

    let subtxn = SnapSubTransaction::begin();
    let fqn = snapshot_name.quoted();

    let drop_result =
        Spi::run(&format!("DROP TABLE {}", fqn)) // nosemgrep: rust.spi.run.dynamic-format — DDL cannot be parameterized; fqn is a PostgreSQL-quoted catalog identifier.
            .map_err(|e| PgTrickleError::SpiError(format!("drop snapshot failed: {e}")));

    if let Err(e) = drop_result {
        subtxn.rollback();
        return Err(e);
    }

    let delete_result = Spi::get_one_with_args::<i64>(
        "WITH deleted AS ( \
             DELETE FROM pgtrickle.pgt_snapshots \
             WHERE snapshot_schema = $1 AND snapshot_table = $2 AND snapshot_relid = $3 \
             RETURNING 1 \
         ) \
         SELECT count(*) FROM deleted",
        &[
            snapshot_catalog.snapshot_schema.as_str().into(),
            snapshot_catalog.snapshot_table.as_str().into(),
            snapshot_catalog.snapshot_relid.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(format!("drop snapshot catalog cleanup failed: {e}")));

    let deleted_rows = match delete_result {
        Ok(Some(count)) => count,
        Ok(None) => {
            subtxn.rollback();
            return Err(PgTrickleError::InternalError(format!(
                "snapshot catalog cleanup for {} returned no row count",
                fqn
            )));
        }
        Err(e) => {
            subtxn.rollback();
            return Err(e);
        }
    };

    if deleted_rows != 1 {
        subtxn.rollback();
        return Err(PgTrickleError::InternalError(format!(
            "snapshot catalog cleanup for {} deleted {} rows",
            fqn, deleted_rows
        )));
    }

    subtxn.commit();

    pgrx::log!("[pg_trickle] SNAP-3: dropped snapshot '{}'", fqn);

    Ok(())
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_snapshot_table_name_contains_st_name() {
        let name = auto_snapshot_table_name("orders");
        assert!(
            name.contains("orders"),
            "snapshot name should contain the ST name: {name}"
        );
        assert!(
            name.starts_with("pgtrickle.snapshot_"),
            "snapshot name should start with pgtrickle.snapshot_: {name}"
        );
    }

    #[test]
    fn test_auto_snapshot_table_name_sanitizes_special_chars() {
        let name = auto_snapshot_table_name("my-table");
        assert!(!name.contains('-'), "dashes should be sanitized: {name}");
    }

    #[test]
    fn test_snapshot_provenance_comment_round_trip() {
        let provenance = SnapshotProvenance {
            pgt_id: 42,
            pgt_relid: 101,
            snapshot_relid: 202,
            created_by_role_oid: 303,
            token: "abc123".to_string(),
        };

        let encoded = provenance.encode_comment();
        let decoded = SnapshotProvenance::parse(&encoded).expect("comment should parse");
        assert_eq!(decoded, provenance);
    }

    #[test]
    fn test_validate_snapshot_relation_binding_rejects_recreated_relation() {
        let relation = RelationIdentity {
            qualified: QualifiedIdentifier::parse_with_default(
                "pgtrickle.snapshot_orders",
                "public",
            )
            .expect("qualified name should parse"),
            relid: pg_sys::Oid::from(55u32),
            relkind: 'r',
            relowner: pg_sys::Oid::from(7u32),
        };
        let catalog = SnapshotCatalogRow {
            pgt_id: 9,
            snapshot_schema: "pgtrickle".to_string(),
            snapshot_table: "snapshot_orders".to_string(),
            snapshot_version: "0.84.0".to_string(),
            frontier_json: Some("null".to_string()),
            snapshot_relid: pg_sys::Oid::from(54u32),
            snapshot_provenance_token: "deadbeef".to_string(),
            created_by_role_oid: pg_sys::Oid::from(7u32),
        };
        let provenance = SnapshotProvenance {
            pgt_id: 9,
            pgt_relid: 77,
            snapshot_relid: 54,
            created_by_role_oid: 7,
            token: "deadbeef".to_string(),
        };

        let err = validate_snapshot_relation_binding(
            &relation,
            &catalog,
            &provenance,
            Some(ExpectedStreamBinding {
                pgt_id: 9,
                pgt_relid: 77,
            }),
        )
        .expect_err("recreated snapshot should be rejected");

        assert!(format!("{err}").contains("identity mismatch"));
    }

    #[test]
    fn test_validate_snapshot_relation_binding_rejects_wrong_stream_provenance() {
        let relation = RelationIdentity {
            qualified: QualifiedIdentifier::parse_with_default(
                "pgtrickle.snapshot_orders",
                "public",
            )
            .expect("qualified name should parse"),
            relid: pg_sys::Oid::from(55u32),
            relkind: 'r',
            relowner: pg_sys::Oid::from(7u32),
        };
        let catalog = SnapshotCatalogRow {
            pgt_id: 9,
            snapshot_schema: "pgtrickle".to_string(),
            snapshot_table: "snapshot_orders".to_string(),
            snapshot_version: "0.84.0".to_string(),
            frontier_json: Some("null".to_string()),
            snapshot_relid: pg_sys::Oid::from(55u32),
            snapshot_provenance_token: "deadbeef".to_string(),
            created_by_role_oid: pg_sys::Oid::from(7u32),
        };
        let provenance = SnapshotProvenance {
            pgt_id: 11,
            pgt_relid: 88,
            snapshot_relid: 55,
            created_by_role_oid: 7,
            token: "deadbeef".to_string(),
        };

        let err = validate_snapshot_relation_binding(
            &relation,
            &catalog,
            &provenance,
            Some(ExpectedStreamBinding {
                pgt_id: 9,
                pgt_relid: 77,
            }),
        )
        .expect_err("snapshot from another stream should be rejected");

        assert!(format!("{err}").contains("does not belong to stream table"));
    }
}
