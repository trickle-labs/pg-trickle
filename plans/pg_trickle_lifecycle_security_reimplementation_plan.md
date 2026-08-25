# Reimplementation Plan: Secure Stream-Table Lifecycle APIs on `origin/main`

## 1. Executive summary

Reimplement the work behind issue `#941` and PRs `#942`/`#943` from a fresh branch based on the current upstream `origin/main`. Do **not** cherry-pick either PR. Use them only as a catalogue of edge cases and useful regression tests.

The implementation must solve two separate problems:

1. **Private-infrastructure access:** a non-superuser stream-table owner should be able to call an explicitly granted lifecycle API without direct privileges on `pgtrickle` catalog tables or the `pgtrickle_changes` schema.
2. **Execution-identity safety:** SQL supplied by a stream-table author, including user-defined functions, operators, views, casts, and RLS policies, must not accidentally execute with the extension owner's elevated privileges merely because a lifecycle function is `SECURITY DEFINER`.

The recommended design is:

- Public owner-lifecycle entry points may use `SECURITY DEFINER` with a pinned path when they need private infrastructure.
- Every call captures the original caller's role and exact pre-function `search_path` in a typed `CallerContext`.
- Every operation that parses, resolves, rewrites, plans, or executes user-authored SQL runs under a restricted **stream-owner context**, not under the extension owner.
- Private catalog, CDC, trigger, and bookkeeping operations remain in the privileged context and use fully qualified, catalog-derived identifiers.
- Refresh execution is split into **prepare → execute-as-owner → finalize** phases so the same identity rule holds for initial, manual, scheduled, full, differential, and immediate refreshes.
- The work is delivered as several reviewable PRs, with the core issue fixed before the higher-risk snapshot/publication/outbox integrations.

This is deliberately more structured than mechanically adding `security_definer` annotations. The latter fixes the grant problem but can widen the authority of caller-controlled SQL and object creation.

---

## 2. Baseline and branch policy

At the time this plan was written:

- Upstream branch: `origin/main`
- Baseline commit: `e5fed2dc8c31ff8d1311f3f51db934ec58aff3ce`
- Package version: `0.87.2`
- PostgreSQL target: PostgreSQL 18
- pgrx version: `0.18.0`

Create the implementation branch from the latest upstream head at the time coding begins:

```bash
git fetch origin --prune
git switch --create fix/lifecycle-privilege-boundary origin/main
git rev-parse HEAD
git status --short
```

Record the actual base SHA in the first commit and PR description. Rebase on `origin/main` again immediately before final CI and merge.

### Branch rules

- Do not merge or cherry-pick PR `#942` or `#943` into the implementation branch.
- Port only well-understood ideas: exact caller-path handling, storage-owner preservation, all-target cascade authorization, and relevant behavioral tests.
- Keep generated SQL, upgrade SQL, API policy, docs, and test changes in the same PR as the behavior they describe.
- Avoid a single very large PR. Use the staged PR sequence in section 8.

---

## 3. Security model and non-negotiable invariants

The implementation is complete only when all of these invariants are true.

### 3.1 Caller identity

1. `GetOuterUserId()` identifies the original SQL caller at every public `SECURITY DEFINER` boundary.
2. Authorization is always evaluated against that caller, never against `current_user` while the definer is active.
3. A caller can operate only on stream tables they own, except for the existing superuser bypass.
4. Cascades and bulk operations authorize **every affected stream table before the first mutation**.

### 3.2 User-authored SQL

5. User-authored defining SQL never executes as the extension owner.
6. Query parsing, rewrite, view expansion, function/operator resolution, full refresh SQL, differential SQL, Top-K SQL, and immediate-mode expression execution use the current stream-table owner's role.
7. Source-table ACLs and RLS are therefore evaluated consistently as the stream-table owner for initial, manual, and scheduled refreshes.
8. A privilege error in user SQL fails the operation without advancing the frontier or leaving partially rebuilt storage.

### 3.3 Search path

9. Public definer functions use a fixed path containing only trusted schemas, normally:

   ```text
   pgtrickle, pg_catalog, pg_temp
   ```

10. The exact caller path that was active before entering the definer function is captured, normalized, and stored with the defining query.
11. The special `"$user"` path element is expanded to the quoted original caller before storage, so later ownership changes do not reinterpret it under the extension owner.
12. Unqualified target and source names resolve under the captured caller context, not under the pinned definer path.
13. The previous role, security flags, GUC nesting level, and search path are restored after success, PostgreSQL `ERROR`, and Rust error paths.

### 3.4 Privileged work

14. Privileged code accepts canonical OIDs and quoted/catalog-derived identifiers, not unresolved caller strings.
15. Caller SQL is never interpolated or executed inside a privileged-only section.
16. Calls into another extension, such as `pg_tide`, are not automatically made with pg_trickle's owner privileges. The called extension must enforce its own boundary.
17. Caller-selected output schemas and database-wide objects require caller-equivalent privilege checks.

### 3.5 Transactionality

18. Storage recreation, catalog replacement, trigger/CDC changes, and full repopulation remain atomic in one PostgreSQL transaction or an internal subtransaction.
19. Failed cascades, failed owner-context execution, or failed ownership transfer roll back all preceding mutations.
20. Refresh frontiers and success history are finalized only after owner-context SQL succeeds.

---

## 4. Scope

### 4.1 Core issue scope

The first functional delivery fixes the three functions named by issue `#941`:

- `pgtrickle.create_or_replace_stream_table`
- `pgtrickle.alter_stream_table`
- `pgtrickle.drop_stream_table`

It also refactors shared creation and refresh machinery used by:

- `create_stream_table`
- `create_stream_table_if_not_exists`
- creation presets and `bulk_create`
- full refreshes triggered by ALTER or reinitialization

This shared work is necessary because create-or-replace delegates to existing create/alter implementations.

### 4.2 Follow-up owner-lifecycle scope

After the core boundary is proven, extend it to:

- `set_stream_table_refresh_policy`
- `set_stream_table_storage_policy`
- `bulk_alter_stream_tables`
- `bulk_drop_stream_tables`
- `reset_fuse`
- `stat_reset`
- `pause_stream_table`
- `resume_stream_table`
- `repair_stream_table`
- `refresh_stream_table` only after all refresh modes use owner-context execution

### 4.3 Separate capability-specific scope

Handle these in independent PRs because stream ownership is not the only relevant permission:

- Snapshot APIs
- Publication APIs
- Outbox/pg_tide APIs

### 4.4 Functions intentionally left `SECURITY INVOKER`

Unless a separate design demonstrates a need, retain invoker execution for:

- `write_and_refresh`, because it executes arbitrary caller SQL
- `exec_stream_ddl`, because it is a dispatcher to checked APIs
- `refresh_if_stale`, if it only delegates to the hardened refresh API
- canary wrappers that delegate to checked lifecycle functions
- subscription APIs that do not resolve a canonical stream relation
- global cross-stream diagnostic reports

The API policy should explain each exception instead of relying on a name-based convention.

---

## 5. Architecture

## 5.1 New security-context module

Create `src/api/security_context.rs` and keep all identity-switching `unsafe` code in this module.

### Types

```rust
pub(crate) struct CallerContext {
    pub role_oid: pg_sys::Oid,
    pub role_name: String,
    pub search_path: String,
}

pub(crate) struct StreamExecutionContext {
    pub owner_oid: pg_sys::Oid,
    pub search_path: String,
}

pub(crate) enum EntryContext {
    SecurityDefiner,
    SecurityInvoker,
}
```

### Capture functions

Implement:

```rust
capture_caller_context(entry: EntryContext) -> Result<CallerContext, PgTrickleError>
stream_execution_context(meta: &StreamTableMeta) -> Result<StreamExecutionContext, PgTrickleError>
```

For a definer entry:

- Read `GetOuterUserId()`.
- Recover the exact pre-function `search_path` from PostgreSQL's function GUC stack.
- Fail closed if the `search_path` GUC record or expected `GUC_SAVE` stack entry is absent.
- Expand a standalone `"$user"` element while preserving quoted identifiers, commas inside quoted names, escaped quotes, and whitespace.

For an invoker entry:

- Read `current_setting('search_path')` directly.
- Still use `GetOuterUserId()` for consistent nested-wrapper behavior.

### Restricted owner execution

PostgreSQL 18 provides `SwitchToUntrustedUser` and `RestoreUserContext`. Add a small `#[repr(C)]` declaration for PostgreSQL's `UserContext` and local FFI declarations if pgrx does not expose them.

Implement one audited helper:

```rust
with_stream_owner_context<T>(
    ctx: &StreamExecutionContext,
    f: impl FnOnce() -> Result<T, PgTrickleError>,
) -> Result<T, PgTrickleError>
```

Required behavior:

1. Save the current definer identity and security flags.
2. Switch to the stream owner using PostgreSQL's untrusted-user helper.
3. Set the stored defining `search_path` transaction-locally.
4. Explicitly set `row_security = on` for owner-executed defining SQL.
5. Execute the closure through `PgTryBuilder`.
6. Restore GUC state and identity in `finally`, including PostgreSQL `ERROR` paths.
7. Support nested calls without leaking the inner path or role.
8. Add a debug assertion that the active user inside the closure equals the expected owner.

Do not expose a generic “run as arbitrary role” public API. The target role must come from the canonical stream relation or captured outer caller.

### Unsafe-code requirements

- Every unsafe block receives a precise `// SAFETY:` explanation.
- Update `.unsafe-baseline` only after review of the new blocks.
- Add unit tests for the pure search-path parser.
- Add PostgreSQL-backed tests for identity restoration after errors.

---

## 5.2 Persist the defining-query search path

Add a column to `pgtrickle.pgt_stream_tables`:

```sql
defining_search_path TEXT NOT NULL
```

Update `StreamTableMeta` and all row decoding/insert/update paths.

### Write policy

- On CREATE: store the captured caller path.
- On `ALTER ... query => ...`: store the caller path used to resolve the new query.
- On create-or-replace with a changed query: store the new caller path.
- On config-only ALTER or no-op create-or-replace: preserve the existing value.
- On scheduled/manual refresh: use the stored value.
- On storage ownership transfer: preserve the stored value; execution identity changes to the new current relation owner, but name resolution remains the path under which the definition was authored.

### Legacy-row backfill

For existing rows, backfill the behavior used by current main:

```text
quote_ident(current storage owner) + ', public'
```

Use `pg_class.relowner` and `pg_roles.rolname`. Fail the upgrade if a catalog row references a missing storage relation rather than silently inventing a path.

---

## 5.3 Canonical name resolution

Replace ad hoc `splitn('.')`, hard-coded `public`, and `current_schema()` calls made under a definer path.

Implement context-aware resolvers:

```rust
resolve_existing_stream_table(name, caller_ctx)
resolve_create_target(name, caller_ctx)
resolve_existing_relation(name, execution_ctx)
resolve_target_schema(name, caller_ctx)
```

Rules:

- Existing relations resolve via PostgreSQL under the caller role/path and return OID plus canonical schema/name.
- Nonexistent create targets use the caller's effective `current_schema()` under their captured path.
- Once resolved, all privileged phases use canonical OIDs or `QualifiedIdentifier` values.
- Identifier quoting must use a single shared helper; never concatenate unescaped caller strings into SQL.

Apply this to create, create-or-replace, alter, drop, bulk operations, snapshots, publications, and outbox APIs.

---

## 5.4 Split lifecycle implementations into phases

Refactor mixed functions into explicit phases. Use typed structs rather than threading raw strings through privileged code.

### Create

```text
prepare_create_as_caller
    → apply_private_create_as_definer
    → initialize_storage_as_owner
    → finalize_create_as_definer
```

`prepare_create_as_caller`:

- Resolve the output target.
- Validate caller `CREATE` on the target schema.
- Parse/rewrite/validate the defining query as caller.
- Resolve source OIDs as caller.
- Validate caller `SELECT` and schema `USAGE`.
- Produce a `PreparedCreatePlan` containing only canonical values.

`apply_private_create_as_definer`:

- Create storage and private catalog rows.
- Create CDC/IVM infrastructure.
- Transfer storage ownership to the caller before any defining-query execution.
- Store `defining_search_path`.

`initialize_storage_as_owner`:

- Run the generated full-population SQL as the storage owner.
- Use the stored path and `row_security = on`.

`finalize_create_as_definer`:

- Store frontier/history state.
- Invalidate scheduler/DAG caches.

All phases remain in one transaction.

### Alter

```text
load_and_authorize_target
    → prepare_alter_as_owner
    → apply_private_alter_as_definer
    → repopulate_as_owner when required
    → finalize_as_definer
```

Requirements:

- Capture the old storage `relowner` before destructive recreation.
- Transfer every recreated table back to that exact owner before repopulation.
- Query rewrite, dependency extraction, Top-K detection, incremental-admission parsing, and full repopulation run as owner.
- Private catalog/CDC mutation remains definer-only.
- Preserve atomic rollback across query migration, mode changes, and partition-key changes.

### Drop

Replace recursive “authorize root, mutate, then recurse” behavior with a precomputed `DropPlan`:

1. Resolve root under caller context.
2. Traverse all downstream dependencies without mutation.
3. Detect cycles/duplicates and build deterministic child-first order.
4. Check ownership of every target against the original caller.
5. Reject the whole plan before mutation if one target is unauthorized.
6. Execute the plan as definer using canonical IDs.

Reuse the same planner for single cascade and bulk drop.

---

## 5.5 Refresh execution: prepare, owner execution, finalize

This work is a prerequisite for making `refresh_stream_table` definer-backed and for claiming that defining SQL does not inherit extension-owner privileges.

### Full refresh

Split current full refresh into:

1. **Privileged prepare**
   - Load private metadata/dependencies.
   - Acquire locks.
   - Validate private change buffers.
   - Snapshot downstream state if needed.
   - Disable extension-managed bookkeeping where required.

2. **Owner execution**
   - Run the defining `SELECT` and its row-identity/auxiliary expressions as stream owner.
   - Write to the owner-owned storage table.
   - Use stored path and RLS-on semantics.

3. **Privileged finalize**
   - Capture downstream deltas.
   - Re-enable triggers.
   - Store frontier/history.
   - Clean private buffers and invalidate caches.

### Differential refresh

Current generated MERGE SQL reads private CDC buffers and may also evaluate expressions originating in the defining query. Separate those concerns.

Introduce a `DeltaStage` abstraction:

```rust
struct DeltaStage {
    source_oid: pg_sys::Oid,
    relation: QualifiedIdentifier, // pg_temp relation
    lower_bound: String,
    upper_bound: String,
}
```

Privileged prepare:

- Copy only the bounded CDC rows required for this refresh into uniquely named `pg_temp` staging tables.
- Preserve typed columns, operation markers, LSNs, changed-column masks, and row identity fields.
- Grant the stream owner `SELECT` on each staging relation; do not grant access to `pgtrickle_changes`.
- Return a source-OID → staged-relation map.

Owner execution:

- Extend DVM/codegen APIs to accept the staged-relation map instead of embedding private buffer names.
- Execute the generated delta/MERGE SQL as stream owner.
- Ensure this phase references only source relations, owner-owned stream storage, `pg_temp` stages, and trusted `pg_catalog` objects.

Privileged finalize:

- Advance frontier only after successful owner execution.
- Perform buffer cleanup using the committed bounds.
- Drop stages explicitly; retain `ON COMMIT DROP` as a safety net.

### Immediate mode

Audit `src/ivm.rs` and trigger apply functions. CDC capture may remain definer-owned, but expression-bearing SQL derived from a stream definition must run under the stream owner's stored execution context. Add the same identity tests for IMMEDIATE mode.

### Fused and parallel refresh

A fused execution unit may contain only stream tables with the same owner OID
and stored defining path. Split mixed-owner or mixed-path units before SQL
generation and preserve dependency order. Parallel workers establish and
restore their owner context independently.

### Failure rules

- Owner-context permission or RLS errors mark refresh failed.
- Never advance frontier on failure.
- Never clean source buffer rows beyond a failed refresh's previous frontier.
- Always restore the scheduler backend's original superuser context.

---

## 6. API-specific authorization rules

## 6.1 Core and simple lifecycle APIs

| API | Required checks | Execution notes |
|---|---|---|
| create/create-or-replace | caller source `SELECT`, source schema `USAGE`, target schema `CREATE` | defining SQL as caller/owner; private setup as definer |
| alter/policy/bulk alter | ownership of every target; source checks for changed query | prevalidate full batch before mutation |
| drop/bulk drop | ownership of every target in complete drop plan | child-first atomic execution |
| pause/resume | stream ownership | no defining SQL; private CDC/catalog work allowed as definer |
| repair | stream ownership; canonical dependency validation | no arbitrary caller SQL in privileged phase |
| reset_fuse/stat_reset | stream ownership | catalog-only mutation |
| refresh | stream ownership | enable only after all refresh modes use owner execution |

## 6.2 Snapshots

Define explicit ownership semantics rather than inheriting definer authority accidentally.

Recommended policy:

- Default internally named snapshots may be created in `pgtrickle` as definer, then transferred to the caller.
- A caller-supplied target schema requires caller `USAGE` and `CREATE` privileges.
- Every created snapshot is owned by its creator and records creator OID/provenance.
- Restore requires:
  - ownership of the destination stream table, and
  - `SELECT` on the snapshot relation.
- Drop requires ownership of the snapshot relation, or superuser.
- Transferring a stream table does not silently transfer historical snapshots.

Add tests for ownership transfer and custom target schemas.

## 6.3 Publications

- Require stream ownership.
- Require caller-equivalent `CREATE` privilege on the current database.
- Prefer running `CREATE PUBLICATION` under caller context; update the private pg_trickle catalog separately as definer.
- If privileged creation is unavoidable, transfer publication ownership to the caller and verify it in `pg_publication`.
- Drop must verify both the pg_trickle binding and the publication's expected identity/owner.

## 6.4 pg_tide outbox integration

- Check stream ownership first.
- Invoke `tide.outbox_create`/related pg_tide APIs as the caller, allowing pg_tide to enforce its own security model.
- Never lend pg_trickle's extension-owner identity to a different extension.
- Register or remove pg_trickle's private mapping in a separate definer phase.
- Add integration tests for pg_tide absent, present-but-denied, and present-and-authorized states.

---

## 7. SQL attributes and API policy

### Public function attributes

For each converted public lifecycle function:

```rust
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
```

Do not add `public` or caller-writable schemas to a definer path.

Functions that execute arbitrary SQL, such as `write_and_refresh`, remain invoker.

### API policy

Update `scripts/sql_api_policy.json` with exact overload identities. Do not rely solely on prefix classification.

Enhance policy validation so every `owner_lifecycle` entry declares one of:

- `definer_owner_checked`
- `invoker_delegating`
- `capability_specific`

This may be an additional field or a companion checked file. CI should reject an owner-lifecycle function with no declared execution policy.

### Static boundary check

Add `scripts/check_privilege_boundaries.py` and/or Semgrep rules that flag:

- `SECURITY DEFINER` without the pinned path.
- Calls that execute `query`, `defining_query`, `original_query`, or generated DVM SQL outside an owner-context helper.
- Dynamic caller-controlled identifiers in privileged SPI SQL.
- External-extension calls inside an extension-owner-only section.
- Newly exported lifecycle functions absent from the policy matrix.

The check should be conservative and allow narrowly documented suppressions.

---

## 8. Delivery plan, roadmap versions, and PR sequence

Each release is one reviewable PR with a six-person-week budget. The sequence
starts after v0.87.6 and must finish before v0.88.0. A release cannot absorb
work from a later boundary unless its own merge gate remains unchanged.

## v0.87.7 / PR 1: Security-context foundation and catalog migration

Roadmap: [v0.87.7](../roadmap/v0.87.7.md)

Files expected:

- `src/api/security_context.rs` (new)
- `src/api/mod.rs`
- `src/api/helpers.rs`
- `src/catalog.rs` or the current catalog module
- `src/lib.rs`
- `.unsafe-baseline`
- unit/pg tests
- upgrade SQL and archive SQL

Deliverables:

- `CallerContext` and `StreamExecutionContext`.
- Exact caller-path capture and `$user` expansion.
- Restricted owner-context execution with guaranteed restoration.
- `defining_search_path` catalog column and legacy backfill.
- No public API attribute changes yet, except any necessary shared correction to existing creation paths.

Merge gate:

- Unit tests and PostgreSQL-backed context restoration tests green.
- Upgrade from current upstream release succeeds.
- No behavior change outside the documented execution-context foundation.

## v0.87.8 / PR 2: Refresh engine owner-context execution

Roadmap: [v0.87.8](../roadmap/v0.87.8.md)

Files expected:

- `src/api/refresh_ops.rs`
- `src/refresh/mod.rs`
- `src/refresh/merge/mod.rs`
- `src/refresh/codegen.rs`
- merge submodules as needed
- `src/ivm.rs`
- CDC staging helpers
- E2E security and correctness tests

Deliverables:

- Full refresh split into prepare/execute/finalize.
- Differential `DeltaStage` implementation.
- Scheduled, manual, initial, reinitialize, Top-K, fused, parallel, and immediate paths use stream-owner execution for definition-derived SQL.
- RLS and privilege semantics documented and tested.

Merge gate:

- Existing DVM correctness corpus unchanged.
- TPC-H/correctness gates pass.
- Security exploit tests prove no extension-owner execution of caller UDFs.
- Parallel/manual/scheduler refresh tests pass.

## v0.87.9 / PR 3: Core issue `#941`

Roadmap: [v0.87.9](../roadmap/v0.87.9.md)

Files expected:

- `src/api/create.rs`
- `src/api/alter.rs`
- `src/api/helpers.rs`
- `tests/e2e_ownership_tests.rs`
- docs/policy/generated SQL

Deliverables:

- Harden `create_or_replace_stream_table`, `alter_stream_table`, and `drop_stream_table`.
- Preserve exact storage owner across all recreation paths.
- Preauthorize complete cascade plans.
- Non-superusers need no private catalog/change-schema grants.

Merge gate:

- Both create and replace paths work with exact public API grants only.
- Mixed-owner cascade is denied and fully rolled back.
- Query-changing and partition-changing ALTER preserve owner and data.

## v0.87.10 / PR 4: Remaining lifecycle APIs and policy enforcement

Roadmap: [v0.87.10](../roadmap/v0.87.10.md)

Deliverables:

- Policy wrappers and bulk APIs.
- Pause/resume/repair.
- Fuse/stat reset.
- Manual refresh conversion now that PR 2 is present.
- Exact API-policy declarations and static privilege-boundary checks.
- Upgrade preflight diagnostics and least-privilege grant documentation.

Keep this PR free of snapshot/publication/outbox changes.

## v0.87.11 / PR 5: Snapshots

Roadmap: [v0.87.11](../roadmap/v0.87.11.md)

Implement the target-schema and snapshot-owner policy in section 6.2 with dedicated E2E tests.

## v0.87.12 / PR 6: Publications

Roadmap: [v0.87.12](../roadmap/v0.87.12.md)

Implement the capability-specific publication checks in section 6.3 with
dedicated E2E, upgrade, rollback, and concurrency tests.

## v0.87.13 / PR 7: pg_tide outbox integration

Roadmap: [v0.87.13](../roadmap/v0.87.13.md)

Implement the pg_tide boundary in section 6.4 with absent, denied, authorized,
rollback, upgrade, and supported-version tests.

After the replacements are open, close PRs `#942` and `#943` as superseded, linking the new PR sequence.

---

## 9. Database migration and packaging

The sequence uses v0.87.6 as its implementation baseline and ships as v0.87.7
through v0.87.13. Each release adds a direct upgrade from its predecessor and
keeps chained upgrades green. If another release claims one of these versions,
renumber the complete sequence before implementation instead of merging two
security boundaries into one release.

### Migration contents

Create:

```text
sql/pg_trickle--<current>--<next>.sql
sql/archive/pg_trickle--<next>.sql
```

The upgrade script must:

1. Add `defining_search_path` nullable.
2. Backfill from each storage relation's current owner.
3. Assert no unresolved rows remain.
4. Set the column `NOT NULL`.
5. Apply `ALTER FUNCTION ... SECURITY DEFINER` and `SET search_path` for converted exact signatures.
6. Explicitly preserve or set `SECURITY INVOKER` for functions intentionally left invoker where generated SQL could otherwise drift.
7. Preserve existing function ACLs.
8. Update the schema-version ledger if required by repository release conventions.

### Upgrade verification

Add upgrade E2E assertions for:

- Backfilled path values.
- `prosecdef` and `proconfig` on every converted overload.
- Existing ACLs retained.
- Pre-upgrade stream tables can refresh under their current owner.
- A non-superuser can use converted lifecycle APIs without private grants after upgrade.
- Rollback of a failing owner-context query leaves the catalog/frontier unchanged.

Run:

```bash
just check-upgrade <current> <next>
just check-upgrade-all
just test-upgrade <current> <next>
```

---

## 10. Test strategy

## 10.1 Pure unit tests

Add tests for:

- `$user` expansion.
- Quoted role names.
- Schema names containing commas.
- Escaped double quotes.
- Empty path elements and whitespace preservation.
- Canonical identifier parsing.
- Drop-plan ordering and duplicate elimination.
- Authorization matrix helpers.

Fuzz the search-path segment parser and qualified-identifier parser.

## 10.2 PostgreSQL-backed context tests

Create a focused test file, for example `tests/e2e_security_context_tests.rs`.

Required cases:

1. **Identity probe:** a caller-owned function returning `current_user` is used in a stream definition. Initial, full, differential, manual, scheduled, reinitialize, Top-K, fused, parallel, and immediate refreshes must materialize the stream owner, never the extension owner.
2. **Privilege probe:** a caller-owned function tries to read a table readable only by the extension owner. Creation/refresh must fail with insufficient privilege.
3. **DDL probe:** a caller-owned function tries to create or alter an object in `pgtrickle`; it must fail.
4. **RLS probe:** source RLS results are identical for initial and later refreshes and reflect the stream-owner policy.
5. **Error restoration:** after a PostgreSQL `ERROR`, `current_user`, search path, GUC nest state, and backend usability are restored.
6. **Nested context:** create-or-replace delegating to alter does not lose the outer caller or path.
7. **Custom path:** a role whose source schema differs from its role name resolves an unqualified source correctly.
8. **Quoted path:** quoted schema names and `$user` work.
9. **Ownership transfer:** after `ALTER TABLE ... OWNER TO`, refresh executes as the new owner but uses the stored defining path.
10. **Revoked source privilege:** revoking source `SELECT` causes refresh failure without frontier advancement.

## 10.3 Lifecycle E2E tests

Rewrite the ownership fixture to grant only:

- `USAGE` on `pgtrickle`
- exact `EXECUTE` on the function under test
- required source/output privileges

Do **not** grant:

- `USAGE` on `pgtrickle_changes`
- `SELECT` on private pgtrickle catalog tables
- `EXECUTE ON ALL FUNCTIONS`

Core cases:

- Owner succeeds, non-owner fails, superuser succeeds.
- Create-or-replace create path and replace path both succeed.
- Query-incompatible ALTER recreates storage and preserves owner.
- Partition-key ALTER recreates storage and preserves owner.
- Mode change that triggers full refresh preserves identity and data.
- Cross-owner cascade fails before mutation.
- Same-owner cascade succeeds.
- Bulk operations prevalidate all targets and are atomic.

## 10.4 Snapshot tests

- Default internal snapshot succeeds and is transferred to caller.
- Custom schema without `CREATE` fails.
- Custom schema with `CREATE` succeeds.
- Restore requires destination ownership and snapshot `SELECT`.
- Drop requires snapshot ownership.
- Stream ownership transfer does not silently transfer old snapshots.
- Provenance mismatch/recreated relation is rejected.

## 10.5 Publication tests

- Caller without database `CREATE` is denied.
- Caller with database `CREATE` and stream ownership succeeds.
- Resulting publication owner matches the documented policy.
- Non-owner cannot create/drop.
- Recreated or renamed publication cannot be confused with the recorded binding.

## 10.6 Outbox tests

- pg_tide absent returns the documented actionable error.
- pg_tide present but caller unauthorized is denied by pg_tide.
- Authorized caller succeeds without pg_trickle private grants.
- Mapping registration rolls back if pg_tide creation fails.
- pg_trickle does not invoke pg_tide as the extension owner.

## 10.7 Concurrency and rollback tests

- Scheduler and manual refresh contend without leaking identity.
- Owner-context failure under an advisory/row lock releases state correctly.
- Parallel refresh mode uses the correct owner independently in each backend.
- Cascaded drop and storage recreation roll back on injected failures.
- Temp delta-stage names cannot collide across concurrent refreshes.

## 10.8 Performance and correctness gates

The differential staging refactor changes the hot path, so record before/after measurements.

Required gates:

- Existing DVM corpus: zero result changes.
- SQLancer/correctness oracle: zero new mismatches.
- TPC-H correctness: all supported queries pass.
- Median differential-refresh overhead stays within an agreed bound; use 10% as the initial review threshold for representative small/medium deltas unless maintainers set another value.
- No unbounded temp-table growth or leaked temp relations after errors.

---

## 11. Local and CI validation matrix

Run targeted checks while developing, then the full matrix before each merge.

### Fast development loop

```bash
cargo fmt -- --check
cargo clippy --all-targets --features pg18 -- -D warnings
bash scripts/check_security_definer.sh
python3 scripts/check_sql_api_policy.py self-test
just test-unit
```

### Targeted E2E

```bash
just build-e2e-image
./scripts/run_e2e_tests.sh --test e2e_ownership_tests --no-capture
./scripts/run_e2e_tests.sh --test e2e_security_context_tests --no-capture
./scripts/run_e2e_tests.sh --test e2e_upgrade_tests --run-ignored all --no-capture
```

### Full required matrix

```bash
just lint-ci
just unsafe-inventory
just test-unit
just test-integration
just test-light-e2e
just test-e2e
just test-e2e-parallel
just test-pgrx
just dvm-corpus
just test-correctness-gate
just sqlancer-rust-only
just check-upgrade-all
just test-upgrade <current> <next>
just test-dbt
```

CI must also pass Semgrep, secret scanning, docs drift, unsafe inventory, generated SQL/API drift, and all repository-required workflows.

No PR in this sequence is mergeable merely because GitHub reports `mergeable: true`; it requires green tests and an approving security-aware review.

---

## 12. Documentation updates

Update together, not after implementation:

- `docs/SECURITY_MODEL.md`
- `docs/SECURITY_GUIDE.md`
- `docs/SQL_REFERENCE.md`
- generated `docs/SQL_API_CATALOG.md`
- release notes/changelog
- operator grant examples

The documents must agree on:

- who executes defining queries;
- how source ACLs and RLS apply;
- which APIs are definer vs invoker;
- what exact grants a stream author needs;
- snapshot/publication/outbox ownership semantics;
- the pre-upgrade privilege diagnostic and the hard stream-owner cutover.

Remove examples that grant `EXECUTE ON ALL FUNCTIONS` or private catalog/change-schema access to routine stream authors. Show exact grants generated by the API policy tool.

---

## 13. Compatibility and rollout

Changing defining-query execution from extension owner to stream owner can
expose deployments that relied on implicit superuser access. Treat this as an
intentional security-hardening compatibility change.

Do not provide a `legacy_extension_owner` mode. Such a mode would violate the
non-negotiable execution-identity invariant and leave a privileged SQL path in
production. Provide a superuser-only pre-upgrade diagnostic that lists stream
tables whose owners lack source `SELECT` or schema `USAGE`. The diagnostic must
name the missing grant and make no changes. Upgrade fails before catalog
mutation when unresolved privilege gaps remain.

Fresh installations and upgrades both use stream-owner execution. The upgrade
notes must describe the diagnostic, required grants, hard cutover, and rollback
procedure.

---

## 14. Observability

Add structured diagnostics for security-context failures without logging query text or secrets:

- stream `pgt_id` and canonical name;
- requested operation;
- expected execution owner OID/name;
- stage: capture, prepare, owner execution, or finalize;
- SQLSTATE/category;
- whether frontier advancement was suppressed.

Expose a read-only diagnostic function that reports:

- current storage owner;
- stored defining search path;
- whether owner has source privileges;
- configured execution-identity mode;
- last refresh identity failure.

Do not include raw defining SQL in routine error logs.

---

## 15. Review checklist

Each PR reviewer should explicitly verify:

- [ ] No caller-controlled SQL executes in extension-owner context.
- [ ] Every new definer function has a pinned trusted path.
- [ ] Original caller identity is used for authorization.
- [ ] Every bulk/cascade target is authorized before mutation.
- [ ] Recreated relations retain the intended owner.
- [ ] Unqualified names resolve using captured/stored caller context.
- [ ] Error paths restore role, path, GUC state, locks, and temp resources.
- [ ] Upgrade SQL changes existing functions, not only fresh installs.
- [ ] Minimal-grant E2E tests prove private schemas remain inaccessible.
- [ ] Docs, API policy, generated SQL, and implementation agree.
- [ ] Full CI and correctness gates are green.

---

## 16. Definition of done

The reimplementation is complete when:

1. A non-superuser owner can create-or-replace, alter, drop, pause, resume, repair, and refresh their own stream table with exact function grants and normal source/output privileges only.
2. The same role has no direct access to pg_trickle private catalogs or `pgtrickle_changes`.
3. A non-owner cannot operate on the table, including through bulk or cascade paths.
4. Caller-defined executable objects never observe the extension owner as `current_user` during any refresh mode.
5. Custom and quoted search paths behave consistently across initial, manual, and scheduled refreshes.
6. Storage and auxiliary objects retain documented ownership after recreation.
7. Snapshot, publication, and outbox operations enforce their additional capability-specific checks.
8. Fresh install and upgrade paths produce identical function attributes, ACL policy, and catalog shape.
9. All required CI, E2E, upgrade, DVM corpus, and correctness tests pass.
10. PRs `#942` and `#943` are closed as superseded with links to the replacement series.

---

## 17. Source references used for this plan

- Current upstream baseline: <https://github.com/trickle-labs/pg-trickle/tree/e5fed2dc8c31ff8d1311f3f51db934ec58aff3ce>
- Issue `#941`: <https://github.com/trickle-labs/pg-trickle/issues/941>
- Superseded PR `#942`: <https://github.com/trickle-labs/pg-trickle/pull/942>
- Superseded PR `#943`: <https://github.com/trickle-labs/pg-trickle/pull/943>
- PostgreSQL 18 `UserContext`: <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/utils/usercontext.h>
- PostgreSQL 18 user-context implementation: <https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/init/usercontext.c>
