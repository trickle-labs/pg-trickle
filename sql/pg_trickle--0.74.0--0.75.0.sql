-- pg_trickle 0.74.0 -> 0.75.0 upgrade migration
-- v0.75.0: API Polish, Documentation Excellence & Developer Experience

-- This release contains only non-schema changes:
--   API-002/DOC-003: metrics_summary() full SQL reference section
--   API-003: SQL function parameter naming convention documented
--   API-004: Generated API catalog return types converted to SQL-facing forms
--   API-005: Schedule-mode comparison table in SQL reference
--   CODE-003: Typed PgtId/StreamTableOid wrappers in Rust (no SQL schema change)
--   DOC-001: plans/PLAN.md architecture-doc table repaired; fragment-corruption lint
--   DOC-002: README GUC count updated to generated phrase
--   DOC-004: Stale-version scanner (check_stale_versions.sh) + just lint-ci
--   ARCH-004: docs/COMPARISONS.md expanded with Feldera and comprehensive IVM matrix

-- No SQL schema changes in this release.
-- All changes are documentation, developer tooling, and Rust type-safety.
