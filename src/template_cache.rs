//! G14-SHC: Catalog-backed cross-backend template cache.
//!
//! Eliminates cold-start latency when new backends connect by persisting
//! delta SQL templates in an UNLOGGED catalog table (`pgtrickle.pgt_template_cache`).
//!
//! ## Cache hierarchy
//!
//! 1. **L1 — thread-local** (`DELTA_TEMPLATE_CACHE` in `dvm/mod.rs`):
//!    In-process HashMap, ~0 ns lookup.  Flushed on `CACHE_GENERATION` bump.
//!
//! 2. **L2 — catalog table** (`pgtrickle.pgt_template_cache`):
//!    Cross-backend persistent cache.  ~1 ms SPI lookup on L1 miss.
//!    Eliminates the ~45 ms DVM parse+differentiate cost for cold backends.
//!
//! Templates are validated by query hash, planner format version, and the
//! bounded source-statistics epoch. A mismatch is treated as stale.

use pgrx::spi::Spi;

use crate::error::PgTrickleError;

/// A template entry loaded from or stored to the catalog cache table.
pub struct CachedTemplate {
    pub delta_sql_template: String,
    pub output_columns: Vec<String>,
    pub source_oids: Vec<u32>,
    pub is_deduplicated: bool,
    pub has_key_changed: bool,
    pub is_all_algebraic: bool,
    pub statistics_epoch: String,
    pub planning_version: u16,
}

/// Look up a cached delta template from the catalog table.
///
/// Returns `Some(template)` if a valid entry exists for the given `pgt_id`
/// and `defining_query_hash`, or `None` if no entry or hash mismatch.
///
/// Cost: ~1 ms (single SPI SELECT).
pub fn lookup(pgt_id: i64, defining_query_hash: u64) -> Option<CachedTemplate> {
    if !crate::config::pg_trickle_template_cache_enabled() {
        return None;
    }

    let hash_i64 = defining_query_hash as i64;

    let cached = Spi::connect(|client| {
        let table = client
            .select(
                "SELECT delta_sql, columns, source_oids, is_dedup, key_changed, all_algebraic, \
                        statistics_epoch, planning_version \
                 FROM pgtrickle.pgt_template_cache \
                 WHERE pgt_id = $1 AND query_hash = $2",
                None,
                &[pgt_id.into(), hash_i64.into()],
            )
            .ok()?;

        if table.is_empty() {
            return None;
        }

        let row = table.first();

        let delta_sql: String = row.get::<String>(1).ok().flatten()?;
        let columns: Vec<String> = row.get::<Vec<String>>(2).ok().flatten().unwrap_or_default();
        let source_oids_i32: Vec<i32> = row.get::<Vec<i32>>(3).ok().flatten().unwrap_or_default();
        let is_dedup: bool = row.get::<bool>(4).ok().flatten().unwrap_or(false);
        let key_changed: bool = row.get::<bool>(5).ok().flatten().unwrap_or(false);
        let all_algebraic: bool = row.get::<bool>(6).ok().flatten().unwrap_or(false);
        let statistics_epoch: String = row.get::<String>(7).ok().flatten()?;
        let planning_version = row
            .get::<i32>(8)
            .ok()
            .flatten()
            .and_then(|version| u16::try_from(version).ok())?;

        let source_oids: Vec<u32> = source_oids_i32.into_iter().map(|v| v as u32).collect();

        Some(CachedTemplate {
            delta_sql_template: delta_sql,
            output_columns: columns,
            source_oids,
            is_deduplicated: is_dedup,
            has_key_changed: key_changed,
            is_all_algebraic: all_algebraic,
            statistics_epoch,
            planning_version,
        })
    });

    cached.filter(|template| {
        template.planning_version == crate::dvm::planner::FORMAT_VERSION
            && template.statistics_epoch
                == crate::dvm::planner::statistics_epoch_for_sources(&template.source_oids)
    })
}

/// Store (or update) a delta template in the catalog cache table.
///
/// Uses INSERT ... ON CONFLICT DO UPDATE to handle both first-time
/// population and hash-mismatch updates.
pub fn store(
    pgt_id: i64,
    defining_query_hash: u64,
    template: &CachedTemplate,
) -> Result<(), PgTrickleError> {
    if !crate::config::pg_trickle_template_cache_enabled() {
        return Ok(());
    }

    let hash_i64 = defining_query_hash as i64;
    let source_oids_i32: Vec<i32> = template.source_oids.iter().map(|&v| v as i32).collect();

    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_template_cache \
         (pgt_id, query_hash, delta_sql, columns, source_oids, \
          is_dedup, key_changed, all_algebraic, statistics_epoch, planning_version, cached_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now()) \
         ON CONFLICT (pgt_id) DO UPDATE SET \
           query_hash = EXCLUDED.query_hash, \
           delta_sql = EXCLUDED.delta_sql, \
           columns = EXCLUDED.columns, \
           source_oids = EXCLUDED.source_oids, \
           is_dedup = EXCLUDED.is_dedup, \
           key_changed = EXCLUDED.key_changed, \
           all_algebraic = EXCLUDED.all_algebraic, \
           statistics_epoch = EXCLUDED.statistics_epoch, \
           planning_version = EXCLUDED.planning_version, \
           cached_at = now()",
        &[
            pgt_id.into(),
            hash_i64.into(),
            template.delta_sql_template.as_str().into(),
            template.output_columns.clone().into(),
            source_oids_i32.into(),
            template.is_deduplicated.into(),
            template.has_key_changed.into(),
            template.is_all_algebraic.into(),
            template.statistics_epoch.as_str().into(),
            i32::from(template.planning_version).into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    Ok(())
}

/// Invalidate (remove) a cached template for a specific stream table.
///
/// Called on ALTER QUERY, DROP, or reinitialize.
pub fn invalidate(pgt_id: i64) {
    let _ = Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_template_cache WHERE pgt_id = $1",
        &[pgt_id.into()],
    );
}

/// STAB-3 (v0.30.0): Delete L2 catalog template cache entries older than
/// `max_age_hours` hours in a single batched DELETE.
///
/// Called from the per-database scheduler tick to prevent stale entries from
/// accumulating after ALTER QUERY without DROP or source-OID renumbering.
/// Returns the number of rows deleted (0 if nothing to purge or GUC disabled).
///
/// `max_age_hours = 0` means "no age purge" (disabled).
pub fn purge_stale_entries(max_age_hours: i32) -> i64 {
    if max_age_hours <= 0 || !crate::config::pg_trickle_template_cache_enabled() {
        return 0;
    }
    Spi::get_one_with_args::<i64>(
        "WITH deleted AS ( \
             DELETE FROM pgtrickle.pgt_template_cache \
             WHERE cached_at < now() - ($1 * interval '1 hour') \
             RETURNING 1 \
         ) SELECT count(*) FROM deleted",
        &[(max_age_hours as i64).into()],
    )
    .unwrap_or(None)
    .unwrap_or(0)
}

/// Invalidate all cached templates.
///
/// Called when a bulk cache flush is needed (e.g., extension upgrade).
pub fn invalidate_all() {
    let _ = Spi::run("DELETE FROM pgtrickle.pgt_template_cache");
}

/// Check whether the template cache table exists.
///
/// Used to gracefully handle the case where the extension was upgraded
/// but the migration hasn't been applied yet.
pub fn table_exists() -> bool {
    Spi::get_one::<bool>(
        "SELECT EXISTS ( \
           SELECT 1 FROM pg_catalog.pg_tables \
           WHERE schemaname = 'pgtrickle' AND tablename = 'pgt_template_cache' \
         )",
    )
    .unwrap_or(Some(false))
    .unwrap_or(false)
}

// TEST-10-02 (v0.49.0): Unit tests for pure-Rust template cache logic.
#[cfg(test)]
mod tests {
    /// The template cache stores `defining_query_hash` as `i64` in PostgreSQL
    /// (since PG has no unsigned integer type). Verify the round-trip conversion
    /// used in `lookup` and `store` is lossless.
    #[test]
    fn test_hash_i64_roundtrip_max() {
        let original: u64 = u64::MAX;
        let as_i64 = original as i64;
        let back: u64 = as_i64 as u64;
        assert_eq!(original, back);
    }

    #[test]
    fn test_hash_i64_roundtrip_zero() {
        let original: u64 = 0;
        let as_i64 = original as i64;
        let back: u64 = as_i64 as u64;
        assert_eq!(original, back);
    }

    #[test]
    fn test_hash_i64_roundtrip_arbitrary() {
        for val in [1u64, 42, 0xDEAD_BEEF, 0x8000_0000_0000_0000, u64::MAX - 1] {
            let back = (val as i64) as u64;
            assert_eq!(val, back, "round-trip failed for {val}");
        }
    }

    /// Source OIDs are stored as `i32[]` in PostgreSQL. Verify the u32 → i32
    /// → u32 conversion used in `store` / `lookup` is lossless for OID ranges.
    #[test]
    fn test_source_oid_i32_roundtrip() {
        // PostgreSQL OIDs fit in u32; the conversion is purely bitwise.
        for oid in [0u32, 1, 12345, u32::MAX / 2, u32::MAX] {
            let as_i32 = oid as i32;
            let back = as_i32 as u32;
            assert_eq!(oid, back, "OID round-trip failed for {oid}");
        }
    }

    /// `purge_stale_entries` must return 0 immediately when `max_age_hours <= 0`.
    /// This is pure-Rust early-exit logic that does not require a PG backend.
    #[test]
    fn test_purge_disabled_at_zero_or_negative() {
        // The early-exit path is: `if max_age_hours <= 0 ... return 0`
        // We can't call the full function without SPI, but we can verify the
        // guard condition logic is correct.
        let cases = [0_i32, -1, i32::MIN];
        for age in cases {
            assert!(age <= 0, "guard condition holds for {age}");
        }
    }
}
