# Plan: Bounded incremental INTERSECT

**Status:** Bounded implementation candidate. Ordinary top-level `INTERSECT` is admitted for differential maintenance only when both branches are direct scans or name-preserving direct-column projections over distinct ordinary or partitioned base tables, with matching output types and deterministic collations. `INTERSECT ALL`, `EXCEPT`, nested/filtered forms, stream-table inputs, and IMMEDIATE refresh remain outside this slice.

## Audit baseline

The source audit was performed against main commit `9e07d049fa6d7ea8aa782cd53f761da5ee393800`. The separate #1293 candidate changes CDC compaction only and does not affect this set-operation trace. The local environment reported PostgreSQL 18.6, pgrx/cargo-pgrx 0.18.0, and rustc/cargo 1.99.1. No incremental INTERSECT runtime path was executed because production admission rejects it.

Relevant implementation:

- Admission: `src/dvm/parser/validation.rs::collect_admission_issues`
- Delta generator: `src/dvm/operators/intersect.rs::diff_intersect`
- Private FULL state: `src/setop_state.rs::rebuild_for_full_refresh`
- Full materialization and state rebuild: `src/refresh/merge/mod.rs`, `src/api/refresh_ops.rs`, `src/api/helpers.rs`
- Durable state registry: `src/lib.rs::pgt_set_operation_states`
- Existing state tests: `tests/e2e_set_operation_tests.rs`
- Prior architecture decision: [ADR-005](../adrs/ADR-005.md)

## Current reachability and state path

`collect_admission_issues` adds DVM-81-1-SET for every INTERSECT node. AUTO therefore resolves to FULL; explicit DIFFERENTIAL is rejected. Circular-dependency validation has a separate fail-closed INTERSECT rejection. The current E2E test that creates an AUTO INTERSECT and expects FULL verifies fallback only.

The existing `diff_intersect` groups signed branch changes by output row and tags the left and right contributions. It then reads prior `__pgt_count_l` and `__pgt_count_r` from the visible stream table. That is incompatible with the current FULL path: before materializing a set-operation result, `normalize_full_set_operation_storage` removes the old visible counter columns. The visible ST must remain the PostgreSQL query result and must never contain private counts or invisible rows. Thus the old generator is not a reachable implementation and cannot be enabled by deleting the admission guard.

A FULL refresh builds private state after materializing the public result. `rebuild_for_full_refresh` runs the two branch queries under the stream owner's role into a session-local temporary table, then copies the staged rows into a permanent extension-owned relation in `pgtrickle`. The relation contains the complete set of output values, a row identity, and left/right counts for each value present in either branch, including rows invisible in the INTERSECT result. It has a unique row-identity index, is revoked from PUBLIC, grants the stream owner SELECT, is registered in `pgt_set_operation_states`, and becomes an extension member. Create, manual FULL, scheduler FULL, and reinitialization paths rebuild state in the same transaction as the visible full result. Drop and query alteration remove the private state.

The current state module supports registry reads, drop, register, and FULL rebuild. It has no differential state reader or writer. The registry is extension config data; this audit found no per-stream state relation rebind/rebuild hook for logical restore before a future differential read. A restore or stale schema version must therefore be detected and repaired through FULL before differential execution may use state.

## Proposed first admitted subset

The first implementation should admit only a single top-level ordinary INTERSECT whose two branches are direct base-table projections of simple column references. Require:

- `INTERSECT` with set semantics (`all = false`), exactly two branches;
- each branch resolves to one supported base-table scan with a projection of direct source columns, no computed expressions, filters, joins, aggregates, CTEs, subqueries, or nested set operations;
- compatible output column types and collations under PostgreSQL's set-operation rules;
- two distinct source relation identities for the initial slice;
- explicit DIFFERENTIAL and AUTO both select differential maintenance only for this admitted subset; IMMEDIATE and unsupported AUTO shapes remain FULL or are rejected;
- no EXCEPT, EXCEPT ALL, or INTERSECT ALL admission.

Admission must be based on the parsed tree and catalog-resolved dependencies, not string matching alone. Reuse the existing FULL fallback and capability-manifest guard for every shape outside this exact subset. Relax the guard only after all gates below pass on the same candidate.

## Required state and delta contract

For each complete output tuple x, let L(x) and R(x) be nonnegative branch multiplicities. Private state must contain x whenever either multiplicity is positive, whether or not it is visible. The public INTERSECT contains exactly one row for x iff L(x) > 0 and R(x) > 0.

For each selected source cut, aggregate all captured signed changes independently by branch and complete output tuple. Combine each aggregate with the corresponding old private count. Reject an impossible negative or overflowing count as a state-integrity error; never clamp it. Then:

- write the new private counts, retaining rows while either count is positive and removing only rows whose two counts are zero;
- emit one public INSERT only for a transition from invisible to visible;
- emit one public DELETE only for a transition from visible to invisible;
- emit no public DML when the row remains visible, even if one or both private counts change.

Group and compare values with PostgreSQL set-operation equality, including NULL grouping and the relevant collation/type semantics. The row identity is a locator; correctness must not depend on an unchecked hash-only equality assumption. Reuse the existing canonical row-identity path and exact tuple checks where needed.

The old counters must come from the validated private state relation, never from the public ST. A private-state write/apply hook is needed in addition to the existing public MERGE. The generator must produce a public result delta without exposing counter columns or invisible rows.

## Role and transaction boundary

Source branch evaluation must run under the stream owner so RLS and stored search path rules match FULL evaluation. The private relation is extension-owned and currently grants the stream owner SELECT only. Stage computed source deltas under the owner, restore the extension-privileged execution context, then apply the private-state changes safely; do not grant broad write access to the private relation.

Private state changes, public ST changes, refresh-history completion, and frontier advancement must commit in the same PostgreSQL transaction and at the same selected source cut. Any error during count validation, private-state application, public MERGE, or finalization must roll back all four. Existing source locks and capture-frontier rules must be verified for both branches and any shared source dependencies; a transaction that cannot prove the required cut must not advance the state.

Before using state, resolve its deterministic relation name and validate catalog ownership, extension membership, operation, `is_all = false`, schema version, relation shape, unique identity index, and exact stream-table identity. If state is missing, stale, or incompatible after restore or upgrade, do not execute a differential delta against it. Require a FULL rebuild or fail closed according to the stream table's FULL policy, with a clear history/reason record.

## Recovery and lifecycle requirements

The state relation is derived data. FULL refresh must rebuild it from the same locked source cut as the public result and replace the relation/catalog binding atomically. Differential updates must be logged so crash recovery exposes either the pre-refresh state/public result/frontier or all post-refresh values. Validate rollback by injecting failures after private-state DML and after public MERGE.

DROP, ALTER QUERY, identity rebuild, extension upgrade, logical restore, and restart must never leave a differential refresh reading stale or misbound state. Exercise state deletion/recreation and missing/stale registry rows. Confirm extension dump behavior for the registry and dynamic state relations; after restore, require an explicit state-validity check and FULL rebootstrap before differential use when state contents or OIDs are not trustworthy.

## Qualification gates and evidence

1. Add admission unit tests for the exact accepted AST and for every excluded form. Assert explicit DIFFERENTIAL admission and AUTO behavior only for the accepted ordinary shape; keep all other set-operation fallback/rejection tests.
2. Add an independent PostgreSQL defining-query bag oracle and private-state oracle. Test asymmetric multiplicities, left-only/right-only rows, simultaneous changes, count transitions across zero, duplicate insertion/deletion, invisible-then-visible rows, NULL tuples, multi-column tuples, and no-change cycles.
3. Test FULL seed → differential cuts → FULL refresh → differential cuts. After every step compare public exact rows and schema, all private counts against independent branch counts, and the visibility iff invariant.
4. Verify actual completed DIFFERENTIAL work, no silent FULL fallback, and no counters/invisible rows in the public relation. Exercise the real installed extension and the same candidate binary.
5. Inject rollback/apply failures and restart/restore/lifecycle events. Assert that public rows, private state, and frontier move atomically or remain unchanged. Confirm a missing/stale state routes to FULL or fails closed.
6. Once the path is reachable, build separate isolated mutants for a wrong public DELETE sign and an incorrect zero-to-positive visibility threshold. Require the intended exact-result mismatch and verify the loaded candidate identity; setup/compilation/timeout failures are inconclusive.
7. Run `just fmt`, `just lint`, relevant unit/integration/E2E tiers, and CI. Only then update `docs/capability-manifest.json`, README/support matrix, and SQL reference to advertise this exact ordinary INTERSECT subset.

## Decision

The implementation candidate now connects the admitted differential path to the private multiplicity relation. It validates the state binding and layout, stages branch-count deltas, applies private state and visible rows in the same refresh transaction, and falls back to FULL when state is missing or invalid. It adds admission, exact-result, rollback, lifecycle, and upgrade E2E coverage.

### Local qualification record (2026-10-30)

- Candidate source is now based on current `main` at `943cc7cbed679fd95f9807896edf26afce62282e`; the implementation diff excluding this plan has SHA-256 `8dfd1251f451e537ddb71824e06a7aa516ce2c9d42c05b0aee45672efa35dfb6`.
- PostgreSQL 18.6, pgrx 0.18.0, rustc/cargo 1.99.1. `just fmt`, `just lint` (with Homebrew Bash 5.3 on this macOS host), and `just test-unit` passed; all 2,675 unit tests passed.
- Rebased candidate E2E image: `pg_trickle_e2e:1292-candidate-rebased`, image digest `sha256:644eb5d34127dfd1f1607a9351d4c88f05b74ff63f7702cb6183d4dd0a60b474`; packaged `pg_trickle.so` SHA-256 `ac7a46618b64bf54616d28c8253c71fdb4022ac9e34c22db44c6ea43fd51b541`.
- On the rebased candidate image, `PGS_E2E_IMAGE=pg_trickle_e2e:1292-candidate-rebased ./scripts/run_e2e_tests.sh --test e2e_set_operation_tests test_intersect` passed all 10 matching tests. The same command with filter `test_setop_private_state_` passed all 3 matching tests. The ignored `test_upgrade_v1083_preserves_intersect_private_state_for_differential` passed with `PGS_UPGRADE_FROM=0.108.3`, `PGS_UPGRADE_TO=0.108.4`, and `PGS_E2E_IMAGE=pg_trickle_upgrade_e2e:latest` built from the rebased candidate.
- Two separate images based on the rebased source injected semantic defects into `src/dvm/operators/intersect.rs`. Reversing the public delete action produced an exact-oracle extra `{\"val\":7}` row (mutant source SHA-256 `f22c4a26cb2228d341cad936a76297f92ab1fe9c6b28adb7b3f411d60915039a`, loaded library SHA-256 `106a7db3ebeeee0174e7d5b559a562bb0bcc74e6a47a7b3d139c2a1d5517d331`). Requiring both old counts to be zero for a visibility insert produced an exact-oracle missing `{\"val\":7}` row (mutant source SHA-256 `defc7b7ed0e685f942cfe90cd0acd5508a74e22158ab2621bae007bf34af55f8`, loaded library SHA-256 `42a54130f1fdeded26bca15dfa7390131a731b96264d56ab2627e426cc2c1c4d`). Both used `PGS_E2E_IMAGE=<mutant image> ./scripts/run_e2e_tests.sh --test e2e_set_operation_tests test_intersect_differential_presence_transitions_and_private_counts`; both failed only at the intended exact-result mismatch.

This is a bounded candidate, not completion of every proposed acceptance item above. Exact-head CI has not run. Keep the issue open until it passes CI and any remaining source-cut, recovery, and compatibility obligations are verified. Unsupported set-operation shapes remain fail-closed.
