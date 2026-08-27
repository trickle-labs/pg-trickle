# v0.87.12 implementation plan: Publication security

> **Status:** Planned
> **Target:** v0.87.12
> **Baseline:** v0.87.11
> **Roadmap:** [roadmap/v0.87.12.md](../roadmap/v0.87.12.md)
> **Program:** [Lifecycle security reimplementation](pg_trickle_lifecycle_security_reimplementation_plan.md)
> **Last updated:** 2026-08-27

## 1. Outcome

v0.87.12 makes downstream publication management obey the original caller's
PostgreSQL privileges. Owning a stream table remains necessary, but it no
longer gives the caller the extension owner's database capabilities.

The release must:

- run caller-selected publication DDL with the original caller's identity;
- let PostgreSQL enforce database `CREATE`, table ownership, and publication
  ownership;
- store an immutable binding between the stream table and the live publication;
- reject missing, renamed, recreated, re-owned, or retargeted publications
  before pg_trickle mutates either object;
- keep PostgreSQL DDL and private catalog bookkeeping in one transaction;
- route explicit drop, stream-table drop, and repair through one validator; and
- preserve least-privilege operation without granting access to private
  pg_trickle catalogs.

This release covers downstream publications created by
`pgtrickle.stream_table_to_publication()`. It does not change the internal WAL
CDC publications managed by `src/wal_decoder.rs`.

### 1.1 Release invariants

- `stream_table_to_publication()` requires both stream-table ownership and the
  caller's native ability to run `CREATE PUBLICATION` in the current database.
- PostgreSQL performs the database `CREATE` and table-ownership checks while
  the original caller is the effective role. A definer-side privilege query is
  not a substitute for native DDL authorization.
- A newly created publication is owned by the original caller. The stored
  owner OID must equal `pg_publication.pubowner` before private registration.
- The binding records the publication name, publication OID, publication owner
  OID, bound stream relation OID, and exact explicit relation OID set.
- A publication name is display and DDL data. It is never sufficient proof of
  object identity.
- All explicit relation OID arrays are sorted, duplicate-free, and compared as
  sets. The v0.87.12 API creates a one-relation publication, so the expected set
  is exactly the bound stream relation OID.
- A valid downstream publication is not `FOR ALL TABLES` and has no
  `pg_publication_namespace` membership. Schema-wide expansion counts as
  relation-scope drift even if the explicit `pg_publication_rel` row remains.
- Drop and repair acquire PostgreSQL object locks, then reread and validate the
  live catalogs. Validation is not a time-of-check/time-of-use query.
- Missing OID, same-name OID reuse, rename, owner transfer, stream-relation
  replacement, and relation-set drift return a specific binding error before
  pg_trickle changes public or private state.
- Explicit publication drop runs with the original caller's identity. The
  caller must retain PostgreSQL's native right to drop that publication.
- Publication DDL and binding writes use one top-level PostgreSQL transaction.
  An error in any phase rolls back every earlier phase.
- No publication path catches a PostgreSQL error and continues with partial
  cleanup. `DROP PUBLICATION IF EXISTS` is not used after a binding exists.
- Stream-table drop does not ignore publication errors and does not drop a
  publication found only by name.
- Concurrent pg_trickle publication operations serialize per `pgt_id`.
  Concurrent native publication DDL either runs before validation, waits for
  the pg_trickle transaction, or leaves a detectable stale binding after it
  commits. It cannot redirect a pg_trickle mutation to another publication.
- Ordinary callers need only the documented public function grants and native
  PostgreSQL privileges. They do not receive `SELECT`, `INSERT`, `UPDATE`, or
  `DELETE` on private pg_trickle tables.
- No new GUC, background worker, public repair API, dependency, or privileged
  owner-transfer path is added unless an executable PostgreSQL 18 probe proves
  that caller-context `CREATE PUBLICATION` cannot work.

### 1.2 Baseline facts that narrow the work

| Area | v0.87.11 baseline | v0.87.12 gap |
|---|---|---|
| Public API | `src/api/publication.rs` exports invoker `stream_table_to_publication(text)` and `drop_stream_table_publication(text)`. | Invoker code cannot reach private metadata under minimal grants. Making the complete function definer would lend extension-owner authority to DDL. |
| Name resolution | Both functions call `parse_qualified_name_pub()` and then read `StreamTableMeta`. | They do not use `resolve_owned_stream_table()` or the caller's pre-definer `search_path`. |
| Creation | One SPI closure runs `CREATE PUBLICATION` and updates `pgt_stream_tables`. | The code does not separate caller DDL from definer bookkeeping or verify the resulting owner and relation set. |
| Binding | `pgt_stream_tables.downstream_publication_name` stores one text value. | There is no publication OID, owner OID, bound stream OID, or expected relation set. |
| Explicit drop | Drop trusts the stored name and uses `DROP PUBLICATION IF EXISTS`. | Rename silently leaves an orphan. Same-name recreation can drop an unrelated publication. |
| Stream-table drop | `execute_drop_stream_table()` repeats the name-only drop and discards its error. | This sibling path bypasses any fix confined to the public publication function. |
| Repair | `repair_stream_table_impl()` locks the stream table but does not inspect a downstream publication. | Repair can mutate stream state while its publication binding is already stale. |
| Diagnostics | `health_check()` joins `pg_publication` by name and reports only owner mismatch. | Missing and same-name-reused publications can disappear from the diagnostic or look valid. |
| Security context | v0.87.7 through v0.87.11 provide `CallerContext`, exact pre-definer path capture, canonical stream resolution, and safe owner-context execution. | The identity-switch implementation needs one caller-specific entry point. It does not need a second generic role-switch system. |
| Policy | `scripts/sql_api_policy.json` marks both functions as `owner_lifecycle` and `definer_owner_checked`, although the Rust functions remain invoker. | Publication APIs need pinned definer entry points plus a `capability_specific` policy because native publication rights are part of authorization. |
| Tests | `tests/e2e_publication_crash_recovery_tests.rs` covers basic operation and disruption, and tolerates publication creation failure in one path. | There is no blocking security, provenance, rollback, deterministic concurrency, or direct-upgrade gate. |

### 1.3 PostgreSQL 18 facts used by the design

PostgreSQL 18 already supplies the capability checks that this release needs:

- `CREATE PUBLICATION` requires `CREATE` on the current database.
- Adding a table requires ownership rights on that table.
- The role that executes `CREATE PUBLICATION` becomes the publication owner.
- Only the publication owner or a superuser can run `DROP PUBLICATION`.
- `pg_publication.oid` and `pg_publication.pubowner` identify the publication
  and its owner.
- `pg_publication_rel` records explicit publication-to-relation membership.
- PostgreSQL's publication code uses `LockDatabaseObject()` with an
  `AccessExclusiveLock` before relation-set changes and rereads the catalog
  after it acquires the lock.

The implementation should follow those native rules rather than duplicate ACL
logic. The relevant upstream references are:

- [PostgreSQL 18 `CREATE PUBLICATION`](https://www.postgresql.org/docs/18/sql-createpublication.html)
- [PostgreSQL 18 `DROP PUBLICATION`](https://www.postgresql.org/docs/18/sql-droppublication.html)
- [PostgreSQL publication command source](https://doxygen.postgresql.org/publicationcmds_8c_source.html)

## 2. Authority and ownership contract

### 2.1 Create

The original caller must satisfy all of these conditions:

1. The caller has `EXECUTE` on
   `pgtrickle.stream_table_to_publication(text)`.
2. The caller resolves the supplied stream-table name under the caller's saved
   `search_path`.
3. The caller owns the resolved stream table under the existing lifecycle
   ownership rule, or the caller is a superuser.
4. The caller has native `CREATE` on the current database.
5. The caller has native ownership rights on the stream storage relation.

Conditions 2 and 3 run before public DDL. PostgreSQL enforces conditions 4 and
5 by executing `CREATE PUBLICATION` as the captured caller.

The publication owner policy is simple: `pubowner` equals the original caller
OID that entered the public function. A caller who acts through membership in
the stream owner's role still owns the new publication as the active caller
role. A superuser caller owns the publication as that superuser role.

### 2.2 Explicit drop

The original caller must satisfy both pg_trickle and PostgreSQL:

1. The caller owns the stream table under the lifecycle ownership rule.
2. The live publication exactly matches the private binding.
3. The caller is allowed by PostgreSQL to drop the bound publication.

The third condition matters after ownership changes. Transferring the stream
table does not transfer its existing publication. A new stream owner who does
not also own the publication cannot use pg_trickle to borrow the extension
owner's drop authority.

### 2.3 Automatic drop with a stream table

`drop_stream_table()` and `bulk_drop_stream_tables()` retain their existing
complete-plan stream ownership checks. Before they mutate the first target,
they must also:

- acquire every planned stream advisory lock in ascending `pgt_id` order;
- load every attached publication binding;
- acquire all publication object locks in ascending publication OID order,
  then all stream relation locks in ascending relation OID order;
- reread and validate every binding against the locked live catalogs; and
- confirm the caller can act as every live publication owner.

The execution phase drops each validated publication as the caller, then drops
the stream storage and private rows as the definer. A failure rolls back the
whole outer transaction, including earlier cascade members.

### 2.4 Repair and diagnosis

`repair_stream_table()` remains the only public stream repair API. If the
stream has a downstream publication, repair validates the binding before it
changes storage, CDC state, frontiers, or status. Repair does not silently
adopt a publication, rewrite provenance, or repair owner/relation drift.

`lifecycle_preflight()` and `health_check()` report the exact mismatch and a
safe remediation. Reversible drift can be fixed by restoring the recorded
name, owner, or relation set. Missing or recreated objects require explicit
operator review. v0.87.12 does not add a force-detach API that could erase the
only record tying a publication to a stream table.

### 2.5 Resulting operation matrix

| Operation | Stream owner | Database `CREATE` | Publication owner | Expected result |
|---|---:|---:|---:|---|
| Create | yes | yes | n/a | Succeeds; caller becomes publication owner. |
| Create | yes | no | n/a | PostgreSQL returns `42501`; no publication or binding remains. |
| Create | no | yes | n/a | pg_trickle returns `42501` before DDL. |
| Create | superuser | bypassed | n/a | Succeeds; superuser caller owns the publication. |
| Explicit drop | yes | irrelevant | yes or caller can act as owner | Succeeds after exact binding validation. |
| Explicit drop | yes | irrelevant | no | PostgreSQL returns `42501`; publication and binding remain. |
| Explicit drop | no | irrelevant | yes | pg_trickle returns `42501` before DDL. |
| Repair | yes | irrelevant | unchanged and binding valid | Continues with normal repair. |
| Repair | yes | irrelevant | binding mismatch | Returns `55000` before repair mutation. |

## 3. Target design

### 3.1 Keep one narrow caller-context switch

Convert both public publication functions to pinned definer entry points:

```rust
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
```

At entry, call `capture_caller_context(EntryContext::SecurityDefiner)` and
resolve the target through the existing owned-stream logic. Refactor
`resolve_owned_stream_table()` around a small
`resolve_owned_stream_table_with_caller(name, &caller)` helper so the function
captures the caller once and every lifecycle API keeps the same canonical
resolution and ownership check. Do not call
`current_schema()` or parse an unqualified stream name under the pinned
definer path.

Add `with_caller_context()` to `src/api/security_context.rs`. It accepts only a
captured `CallerContext`, runs the closure as `caller.role_oid` under
`caller.search_path` with `row_security = on`, and restores role, security
flags, and all GUC state on success, PostgreSQL `ERROR`, or Rust unwind.

Reuse the private identity-switch and GUC checkpoint code already used by
`with_stream_owner_context()`. Do not expose a general
`with_arbitrary_role(oid, path)` function. The two public wrappers must make it
impossible for an untrusted OID or path to enter the primitive.

Before writing fallback code, add an executable PostgreSQL 18 probe that runs
`CREATE PUBLICATION ... FOR TABLE ...` inside `with_caller_context()`. The
expected result is that PostgreSQL performs its ordinary native checks. If the
probe passes, do not implement privileged creation or ownership transfer.

Only if PostgreSQL rejects the safe caller-context path for a documented
backend restriction may implementation use the roadmap fallback. That fallback
must create, transfer ownership, verify `pubowner`, and register the binding in
one transaction. It must still prove the caller's database `CREATE` right and
must never commit an extension-owned publication.

### 3.2 Store provenance in a focused private binding table

Add one private table instead of four more columns to every hot
`StreamTableMeta` load:

```sql
CREATE TABLE pgtrickle.pgt_publication_bindings (
    pgt_id                  BIGINT PRIMARY KEY
                            REFERENCES pgtrickle.pgt_stream_tables(pgt_id)
                            ON DELETE CASCADE,
    stream_relid            OID NOT NULL,
    publication_oid         OID NOT NULL UNIQUE,
    publication_name        TEXT NOT NULL UNIQUE,
    publication_owner_oid   OID NOT NULL,
    expected_relation_oids  OID[] NOT NULL,
    CONSTRAINT pgt_publication_binding_relations_check
        CHECK (expected_relation_oids = ARRAY[stream_relid])
);
```

The one-row-per-stream and unique-publication constraints reject duplicate
bindings. The exact array check records the current API contract without
adding speculative multi-table support. If a later release publishes multiple
explicit relations, that release can widen the check and its validator with a
new migration.

Do not register this table with `pg_extension_config_dump()` in v0.87.12.
Publication, role, and relation OIDs are cluster-local and cannot be restored
as raw numbers without risking a binding to an unrelated object. Keep all
default privileges private. The existing dump of
`pgt_stream_tables.downstream_publication_name` remains only a compatibility
record; after a dump/restore with an attached publication, the missing
canonical row is a fail-closed `private_binding_incomplete` state. Document the
supported v0.87.12 procedure as detaching downstream publications before dump
and recreating them through the API after restore. Stable-identifier remapping
and an explicit reconciliation workflow are separate future work, not an
automatic name-based adoption path in this release.

Retain `pgt_stream_tables.downstream_publication_name` in v0.87.12 to preserve
the catalog shape and monitoring output. Treat it as a compatibility
projection, not identity; its existing dump is diagnostic state, not a
restorable OID binding. The canonical write helper changes the binding row and
this legacy field in the same transaction. The validator reports a
private-state mismatch if the two names differ.

This design avoids widening every repeated catalog SELECT and positional
decoder in `src/catalog.rs`. It also keeps publication-only provenance out of
the refresh hot path.

### 3.3 Use one binding value and one pure mismatch classifier

Add a private `PublicationBinding` value in `src/api/publication.rs` with the
six table fields. Keep loading and mutation SPI blocks short.

Separate catalog reads from decision logic:

- `load_publication_binding(pgt_id)` returns `Option<PublicationBinding>`;
- `load_live_publication_by_oid(oid)` returns the live name and owner, if any;
- `load_live_publication_oid_by_name(name)` distinguishes missing from name
  reuse;
- `load_explicit_publication_relids(oid)` returns a sorted, duplicate-free OID
  vector;
- `load_publication_namespace_oids(oid)` proves that no schema-wide membership
  was added; and
- a pure `classify_publication_binding()` compares the binding, the current
  stream OID, the legacy name, `puballtables`, namespace membership, and the
  live catalog values.

The classifier applies this priority so compound drift has one deterministic
result:

1. Classify private completeness first. No canonical row and no legacy name
   means no binding; exactly one representation or disagreeing names means
   `private_binding_incomplete`.
2. If the bound OID is absent, use the stored-name lookup only to distinguish
   `publication_name_reused` from `missing_publication`; never use it as a
   mutation target.
3. If the bound OID exists, compare its live name first. A changed live name is
   `publication_renamed` even if another object now occupies the old name.
4. Compare owner, current stream OID, publication scope, and explicit relation
   set in that order.

The stable mismatch reasons are:

| Reason | Detection |
|---|---|
| `missing_publication` | Stored publication OID and stored name resolve to no live object. |
| `publication_name_reused` | Stored OID is gone, but the stored name resolves to a different OID. |
| `publication_renamed` | Stored OID exists with a different live name. |
| `publication_owner_changed` | Live `pubowner` differs from the stored owner OID. |
| `stream_relation_changed` | Current `pgt_relid` differs from the bound `stream_relid`. |
| `publication_relations_changed` | Sorted `pg_publication_rel.prrelid` values differ from `expected_relation_oids`. |
| `publication_scope_changed` | `puballtables` is true or `pg_publication_namespace` has any row. |
| `private_binding_incomplete` | Exactly one private representation exists, or the canonical and legacy names disagree. |

Run pure unit tests for every classifier branch, compound-drift priority, and
reordered or duplicate relation input. Catalog query tests must cast `name`
columns such as `pubname` and `rolname` to `text` before fetching Rust
`String` values.

### 3.4 Lock before trusting a binding

Use two kinds of locks for different races:

1. A transaction-scoped advisory lock keyed by `pgt_id` serializes pg_trickle
   create, explicit drop, repair, and stream-table drop.
2. PostgreSQL object locks serialize validation with native publication and
   relation DDL.

Wrap `pg_sys::LockDatabaseObject()` in one publication-specific safe helper.
Use `PublicationRelationId`, the stored publication OID, and:

- `AccessExclusiveLock` for explicit or automatic drop; and
- `AccessShareLock` for read-only repair validation.

After acquiring the object lock, reread `pg_publication` and
`pg_publication_rel` by OID, then classify the binding. Lock the bound stream
relation with at least `AccessShareLock` before comparing the current stream
OID and explicit relation set. Every new `unsafe` block requires a precise
`// SAFETY:` comment.

For a multi-stream drop plan, acquire locks before the first mutation:

1. advisory stream locks in ascending `pgt_id` order;
2. publication object locks in ascending publication OID order; and
3. stream relation locks in ascending relation OID order.

Do not acquire these sets in topological execution order. Topological order is
for child-first deletion; sorted lock order prevents two overlapping bulk
drops from deadlocking. Publication-before-relation matches PostgreSQL's native
publication ALTER lock order and prevents an inverse-order deadlock with it.
WP0 must confirm the exact PostgreSQL 18 order in executable probes before the
helper is frozen.

Health reporting is observational and need not hold object locks across the
whole health query. It must still join by stored OID and label the result as a
point-in-time diagnosis.

### 3.5 Creation phases

`stream_table_to_publication_impl()` follows these phases inside the caller's
outer transaction:

1. Capture the original caller and canonical caller `search_path`.
2. Resolve and authorize the stream through `resolve_owned_stream_table()`.
3. Acquire the blocking transaction advisory lock for `meta.pgt_id`.
4. Reload the private row under `FOR UPDATE` and reject either an existing
   canonical binding or a non-null legacy name.
5. Derive `pgt_pub_<pgt_name>` from the canonical stream metadata. If that name
   exceeds PostgreSQL's 63-byte identifier limit, keep the longest valid UTF-8
   prefix that fits and append `_` plus a 16-hex-character xxh64 suffix. Use
   the installed `xxhash-rust` dependency with an explicit seed and framed
   schema/name components. Add or reuse only a small deterministic identifier
   helper; the current outbox-local truncation is a precedent, not a reusable
   publication helper. Unit-test UTF-8 byte boundaries and deterministic
   distinction between qualified names. Quote the publication and qualified
   relation identifiers with the existing helpers.
6. Run `CREATE PUBLICATION ... FOR TABLE ...` inside
   `with_caller_context()`. Do not preflight database ACLs as the definer.
7. Read the new publication by exact name. Record its OID, `pubowner`, and
   sorted explicit relation set.
8. Verify that the owner equals `caller.role_oid`, the explicit relation set
   equals `[meta.pgt_relid]`, `puballtables` is false, no schema membership
   exists, and the current stream OID still equals the resolved OID.
9. Back in definer context, insert the canonical binding and update
   `downstream_publication_name`.
10. Log success only after both private writes succeed.

The function must not open one SPI connection across the identity switch.
Caller DDL, live verification, and private registration use separate short SPI
blocks while remaining in the same PostgreSQL transaction.

Name collision is not reconciliation. If an unrelated publication already
uses `pgt_pub_<pgt_name>`, PostgreSQL returns duplicate object and the private
binding remains absent. pg_trickle never adopts the conflicting publication.

### 3.6 Explicit drop phases

`drop_stream_table_publication_impl()` follows these phases:

1. Capture the original caller and resolve the owned stream canonically.
2. Acquire the `pgt_id` advisory lock and lock the private stream row.
3. Load both private representations. If neither exists, return
   `PublicationNotFound` even if a publication with the generated name exists.
   If exactly one exists or the names disagree, return
   `private_binding_incomplete`.
4. Acquire the publication object lock, then the stream relation lock.
5. Reread and validate every binding field.
6. After locking and validating the live owner, use
   `pg_has_role(caller_oid, live_pubowner, 'USAGE') OR caller.rolsuper` for the
   early bulk-plan capability check. This models inherited role authority;
   native DDL remains the authoritative check.
7. Run `DROP PUBLICATION <validated-live-name>` without `IF EXISTS` inside
   `with_caller_context()`.
8. Back in definer context, delete the binding row and clear the legacy name.
9. Log success only after private cleanup succeeds.

If native drop fails, the binding remains. If private cleanup fails after the
DDL statement, PostgreSQL rolls back the DDL and the binding remains. Do not
use a subtransaction to keep either half.

### 3.7 Route lifecycle cleanup through the same primitive

Remove the direct publication SQL from `execute_drop_stream_table()` in
`src/api/alter.rs`. Split the shared publication work into two internal steps:

- prepare and lock a validated binding with no mutation; and
- execute caller-context drop for an already validated, locked binding.

`build_drop_plan()` continues to authorize every stream-table target before
mutation. The execution wrapper then locks and prevalidates all publication
bindings in the complete plan before dropping the first publication or storage
relation.

This path must propagate every publication error. Delete the current
best-effort `let _ = Spi::run(...)` behavior. A stale binding blocks stream
drop rather than risking deletion of an unrelated publication or silently
leaving an orphan.

`StreamTableMeta::delete()` and the binding table's `ON DELETE CASCADE` remain
the last private cleanup. Explicitly deleting the binding before deleting the
stream row is optional and should be skipped unless it improves the returned
error. One cascade is enough.

### 3.8 Validate before stream repair

In `repair_stream_table_impl()`:

1. keep the existing stream ownership check, but replace the current
   `pg_try_advisory_xact_lock()`/`RefreshSkipped` behavior with the same
   blocking `pgt_id` advisory lock used by publication lifecycle operations;
2. load the optional publication binding;
3. if present, take the read locks and validate it; and
4. only then begin storage, CDC, frontier, or status repair.

Repair does not add a table to a publication, change a publication owner,
rename a publication, or replace provenance. Those actions could convert an
unrelated live object into a trusted one.

The replacement paths already exist. Add a binding-attached prerequisite gate
to `alter_stream_table_query()`'s `SchemaChange::Incompatible` branch and
`alter_stream_table_partition_key()` before either sets `pgt_relid = 0` or
drops storage. Audit `create_or_replace_stream_table_impl()`, which delegates
query replacement into `alter_stream_table_impl()`, to prove it reaches the
same gate. v0.87.12 does not migrate publication membership to a replacement
relation. Repair must use the same gate before any missing-storage
reinitialization that would create a new OID.

### 3.9 Make diagnostics use object identity

Extend `lifecycle_preflight()` with a `publication_bindings` object. The check
must work both before and after the v0.87.12 table exists:

- Before upgrade, inspect every non-null legacy name and report whether it
  resolves to one publication whose explicit relation set is exactly the
  stream `pgt_relid`, with neither all-table nor schema-wide scope. Mark a
  passing row as observed current state, not proven history.
- After upgrade, validate the canonical binding fields and legacy projection.
- Report the stream, stored publication name/OID, live name/OID, mismatch
  reason, and one concrete remediation per row.
- Keep the function read-only and superuser-only.

Update `health_check()` to join publication state by stored OID. Report
missing OIDs, reused names, renames, owner changes, stream OID changes, and
relation-set drift. The existing owner mismatch message should distinguish a
valid historical publication owner from a stored/live owner mismatch. Stream
ownership transfer alone is not provenance corruption.

Keep `pg_stat_stream_tables.downstream_publication` and existing user-visible
name output unchanged in v0.87.12. A later release may replace the legacy
column with a view over the binding table after dump compatibility is reviewed.

### 3.10 Errors and SQLSTATEs

Add one typed `PgTrickleError::PublicationBindingMismatch` variant with the
publication name and stable mismatch reason. Map it at the SQL boundary to
`OBJECT_NOT_IN_PREREQUISITE_STATE` (`55000`) with:

- `MESSAGE`: the affected publication binding is stale;
- `DETAIL`: stored and live identity fields relevant to the mismatch; and
- `HINT`: restore the recorded state, inspect `lifecycle_preflight()`, or use
  the documented superuser recovery procedure after confirming the live
  object is unrelated.

Retain:

- `PublicationAlreadyExists` (`42710`) for an existing private binding;
- `PublicationNotFound` (`42704`) for no private binding; and
- native PostgreSQL `42501`, `42710`, and `42704` errors from caller-context
  DDL when those errors describe the failing operation.

Do not reuse `PublicationRebuildFailed`. That variant belongs to internal WAL
CDC publication rebuilds and does not describe downstream provenance.

Update `docs/ERRORS.md` with every mismatch reason and recovery rule.

## 4. Transaction and failure contract

### 4.1 One transaction, three contexts

The public function call has three sequential contexts, not three
transactions:

```text
pinned definer entry
    -> capture and authorize original caller
    -> caller-context PostgreSQL publication DDL
    -> definer-context private binding write
    -> return and let the caller commit or roll back
```

PostgreSQL DDL is transactional. Do not add autonomous transactions, worker
handoffs, durable job rows, or manual compensating DDL.

### 4.2 Failure matrix

The roadmap's failure phases are the mutation boundaries below. Advisory-lock,
object-lock, and caller-context setup are non-mutating preconditions, but they
still need bounded failure tests: hold each lock from another connection and
assert `lock_timeout` leaves all state unchanged; force caller-context setup
and body errors and assert role, path, GUC nest level, and backend usability are
restored.

| Phase | Injected failure | Required final state |
|---|---|---|
| Canonical resolution | Missing or shadowed stream name | No publication and no binding change. |
| Stream authorization | Non-owner caller | No publication and no binding change. |
| Advisory lock acquisition | Hold the exact `pgt_id` transaction lock from another session and use bounded `lock_timeout` | Timeout leaves public and private state unchanged. |
| Publication/relation lock acquisition | Hold the native object or relation lock from another session and use bounded `lock_timeout` | Timeout leaves public and private state unchanged. |
| Caller-context execution | Force the wrapped body to return Rust and PostgreSQL errors | Role, path, GUC nest state, and backend usability are restored; no mutation commits. |
| Caller DDL privilege | Revoke database `CREATE` | Native `42501`; no publication and no binding. |
| Caller DDL name allocation | Pre-create conflicting publication name | Conflicting publication remains untouched; no binding. |
| Post-create verification | An E2E `ddl_command_end` event trigger, owned by the test superuser and guarded against recursion, changes the new publication owner or relation set before control returns to the API | Verification fails and the outer transaction rolls back both the sabotage and the created publication. |
| Canonical binding insert | A temporary E2E trigger raises on `pgt_publication_bindings` insert | Created publication rolls back; no binding or legacy name survives. |
| Legacy projection update | A temporary E2E trigger raises on the stream row update | Binding insert and publication both roll back. |
| Pre-drop validation | Rename, recreate, re-own, retarget, or remove the publication | No public or private mutation. |
| Caller drop DDL | Native ownership denial or a temporary DDL event-trigger failure | Publication and binding remain. |
| Canonical binding cleanup | A temporary E2E trigger raises on binding delete | `DROP PUBLICATION` rolls back; publication and binding remain. |
| Legacy projection cleanup | A temporary E2E trigger raises on legacy-name clear | Public drop and canonical cleanup roll back. |

Use temporary PostgreSQL test fixtures for failure injection. Do not add a
production GUC solely to fail a publication phase. If a post-DDL boundary
cannot be reached with a table trigger, event trigger, or pure validator test,
add the smallest test-only hook supported by the E2E build and document why.

### 4.3 Never compensate by name

Do not handle a failed binding insert with a second best-effort
`DROP PUBLICATION <generated-name>`. Transaction rollback is the compensation.
A name-based cleanup statement can race name reuse and repeat the original
security bug.

## 5. Concurrency contract

### 5.1 pg_trickle operations on one stream

The transaction advisory lock makes create, explicit drop, repair, and stream
drop linearizable for one `pgt_id`:

- concurrent create/create yields one committed binding and one
  `PublicationAlreadyExists` result;
- concurrent drop/drop yields one committed drop and one
  `PublicationNotFound` result;
- create/drop commits in lock acquisition order and leaves either one valid
  binding or no binding; and
- repair cannot cross an explicit or automatic publication drop.

Use a blocking advisory lock for these user-initiated lifecycle operations.
Returning `RefreshSkipped` would turn a deterministic lifecycle call into a
retry protocol that the public API does not document.

### 5.2 Native publication DDL

Native `ALTER PUBLICATION`, `DROP PUBLICATION`, and owner transfer do not take
pg_trickle advisory locks. The PostgreSQL publication object lock closes the
validation-to-mutation window within a pg_trickle transaction.

Native DDL can still commit before a pg_trickle call acquires the lock or after
the pg_trickle transaction commits. That state is allowed only because the
binding keeps the old immutable identity and the next drop, repair, preflight,
or health check reports drift. pg_trickle never silently updates the binding
to match later native DDL.

### 5.3 Deterministic concurrency tests

Do not use sleeps as ordering. Use separate connections, explicit
transactions, transaction advisory locks, `pg_locks`, and bounded
`lock_timeout` values as barriers.

Fetch the target `pgt_id` from `pgt_stream_tables`. The barrier session calls
`SELECT pg_advisory_xact_lock($1::bigint)` with that exact value, matching the
existing lifecycle lock key; there is no hidden key derivation. Confirm each
waiter in `pg_locks` before queuing the next operation or releasing the
barrier.

Required cases:

- two create calls queued behind a held `pgt_id` lock;
- two drop calls queued behind a held `pgt_id` lock;
- create then drop from an initially unbound stream: hold the advisory lock,
  queue create and observe it waiting, queue drop and observe it waiting,
  release, and require create to commit before drop, leaving no publication or
  binding;
- drop then create from an initially valid binding: hold the advisory lock,
  queue drop and observe it waiting, queue create and observe it waiting,
  release, and require drop to commit before create, leaving a new valid
  binding with a new publication OID;
- rename commits while drop is blocked before validation, then drop reports
  `publication_renamed`;
- same-name recreation commits while drop is blocked, then drop reports
  `publication_name_reused` and leaves the replacement untouched;
- owner transfer commits while repair is blocked, then repair reports
  `publication_owner_changed`;
- relation-set change commits while drop is blocked, then drop reports
  `publication_relations_changed`;
- schema membership is added while drop is blocked, then drop reports
  `publication_scope_changed`;
- native ALTER started while repair holds the publication object lock waits or
  reaches `lock_timeout`, then succeeds only after repair commits; and
- overlapping bulk stream drops acquire sorted lock sets without deadlock or
  partial committed cleanup.

Every case asserts both sides of the binding: `pg_publication` and
`pgtrickle.pgt_publication_bindings`, plus the legacy projection.

## 6. Upgrade and compatibility

### 6.1 Direct v0.87.11 to v0.87.12 migration

Add `sql/pg_trickle--0.87.11--0.87.12.sql`. It must run in this order:

1. Create `pgtrickle.pgt_publication_bindings` with its constraints and
   indexes.
2. Inspect every v0.87.11 row whose `downstream_publication_name` is non-null.
3. Resolve the live publication by exact name.
4. Read its OID, owner OID, and sorted explicit relation OIDs.
5. Require the relation set to equal `[pgt_relid]`, `puballtables` to be false,
   and schema membership to be empty.
6. Insert the validated provenance row.
7. Assert that the number of inserted bindings equals the number of non-null
   legacy names.
8. Leave the raw-OID binding table out of `pg_extension_config_dump()`.
9. Change both exact publication function signatures to `SECURITY DEFINER` and
   set `search_path = pgtrickle, pg_catalog, pg_temp`.
10. Preserve all existing function ACLs.

The migration adopts the currently named, currently valid publication as an
observed-state trust cutover. It may store the live owner even if the stream
table has since changed owner. v0.87.11 has no historical publication OID, so
the migration cannot distinguish a missing object from a renamed one or prove
that an observationally identical same-name object was not recreated.
Immutable OID provenance begins when the v0.87.12 backfill commits; document
this limit in the preflight and upgrade guide.

The migration must fail before commit if a legacy name resolves to no current
publication or the current publication has the wrong explicit relation set or
scope. It must name the affected stream and publication and direct the
operator to `lifecycle_preflight()`. Never skip an invalid row or infer a
different object from `pgt_pub_<pgt_name>`.

### 6.2 Pre-upgrade diagnosis

After installing the v0.87.12 shared library but before `ALTER EXTENSION`, the
existing v0.87.11 SQL function definition can call the new
`lifecycle_preflight()` Rust body. Make the new publication check detect that
the binding table does not yet exist and validate legacy rows without writing.

Document this sequence:

```sql
SELECT pgtrickle.lifecycle_preflight();
ALTER EXTENSION pg_trickle UPDATE TO '0.87.12';
```

The result must include exact rows that block provenance backfill and label a
currently valid legacy row as an observed-state adoption whose history cannot
be proven. A failed upgrade transaction leaves the v0.87.11 schema, function
attributes, ACLs, publications, and legacy names unchanged.

### 6.3 Fresh install and generated artifacts

Regenerate `sql/archive/pg_trickle--0.87.12.sql` from the same source that
defines the fresh-install table and function attributes. Fresh install and
direct upgrade must agree on:

- table columns, types, constraints, indexes, and exclusion from config dump;
- function owner, `prosecdef`, `proconfig`, volatility, strictness, and ACLs;
- exact API identities and `capability_specific` policy;
- private schema/table privileges; and
- user-visible monitoring output.

Update version references in `Cargo.toml`, `Cargo.lock`, `META.json`, Docker
files, `Justfile`, `ROADMAP.md`, `CHANGELOG.md`, and the implemented roadmap
status as part of the implementation release, not as part of this planning
commit.

### 6.4 API policy

Keep both exact signatures classified as `owner_lifecycle`, but change their
execution policy to `capability_specific` in
`scripts/sql_api_policy.json`. The policy means:

- the pinned definer phase reaches private pg_trickle state;
- the original caller's stream ownership is checked; and
- PostgreSQL publication DDL runs with the original caller's native
  capabilities.

The generated deny-first ACL remains unchanged: `PUBLIC` does not regain
`EXECUTE`. Upgrade preserves explicit grants made before v0.87.12.

## 7. Work packages and implementation order

The roadmap allocates six person-weeks. Keep each package correctness-complete
and do not split behavior from its rollback tests.

### WP0: Freeze the PostgreSQL contract, 0.25 person-week

- Add an executable caller-context `CREATE PUBLICATION` probe on PostgreSQL 18.
- Confirm the publication OID, owner OID, and explicit relation-set queries.
- Confirm the `LockDatabaseObject()` modes and publication-before-relation
  order used by native ALTER/DROP behavior.
- Record that no privileged owner-transfer fallback is needed when the probe
  passes.

Exit: a failing security test proves the current invoker/private-catalog gap,
and the caller-context probe proves the primary design.

### WP1: Add provenance and upgrade diagnostics, 1.00 person-week

- Add the private binding table and explicit no-config-dump policy to
  `src/lib.rs`.
- Add `PublicationBinding` reads, writes, and pure mismatch classification.
- Extend `lifecycle_preflight()` and `health_check()`.
- Add the strict v0.87.11 backfill and failed-upgrade diagnostics.
- Update catalog compatibility and fail-closed dump/restore documentation.

Exit: fresh rows have immutable identity, valid legacy rows establish an
explicit observed-state cutover, and observably invalid legacy rows block
upgrade atomically.

### WP2: Implement caller-equivalent creation, 1.00 person-week

- Add the narrow `with_caller_context()` wrapper.
- Convert create to a pinned definer entry point.
- Resolve and authorize before DDL.
- Run native creation as the caller.
- Verify owner and relations before binding registration.
- Cover quoted names, conflicts, grants, revocations, and exact owner.

Exit: a role with only public API grants and native database/table rights can
create, while removing database `CREATE` denies it without residue.

### WP3: Implement provenance-bound drop and repair, 1.25 person-weeks

- Add advisory, relation, and publication object locking.
- Convert explicit drop to caller-context DDL without `IF EXISTS`.
- Route stream-table drop and bulk drop through the shared validator.
- Change repair to the blocking lifecycle lock and validate bindings before
  repair.
- Gate incompatible query ALTER, partition-key ALTER, and the
  create-or-replace route before storage replacement while a binding exists.
- Remove ignored publication cleanup errors.
- Add stable mismatch errors and documentation.

Exit: every mutation path rejects stale identity and cannot drop a same-name
replacement.

### WP4: Security, rollback, concurrency, and upgrade gates, 2.50 person-weeks

- Add strict fresh-install security E2E coverage.
- Add every mismatch and unrelated-publication case.
- Add phase failure injection without a production test GUC.
- Add deterministic create/drop/native-DDL concurrency coverage.
- Add direct upgrade success and rollback tests.
- Run the full release matrix and manual branch CI.

Exit: all roadmap-required tests pass and no test tolerates publication setup
failure.

## 8. File-level change map

### Core Rust

| File | Planned change |
|---|---|
| `src/api/publication.rs` | Add binding values/catalog access, pure mismatch classification, object-lock wrapper, phased create/drop, and shared lifecycle cleanup primitives. Keep subscriber-lag code separate. |
| `src/api/security_context.rs` | Add the captured-caller execution wrapper by reusing the existing guarded identity/GUC implementation. |
| `src/api/alter.rs` | Remove name-only best-effort publication drop, prelock/prevalidate cascade plans, make repair use the blocking lifecycle lock, and gate incompatible query and partition-key storage replacement while a binding exists. |
| `src/api/create.rs` | Prove `create_or_replace_stream_table_impl()` delegates binding-attached query replacement through the same storage-replacement gate. |
| `src/api/diagnostics.rs` | Extend `lifecycle_preflight()` with legacy and canonical publication checks. |
| `src/monitor/health.rs` | Diagnose downstream publication state by bound OID and expected relation set. |
| `src/error.rs` | Add the typed publication-binding mismatch error. |
| `src/api/mod.rs` | Map the mismatch to SQLSTATE `55000` with detail and hint. |
| `src/lib.rs` | Define the private binding table for fresh installs and deliberately exclude its raw OIDs from config dump. |

Do not modify `src/catalog.rs` merely to carry publication provenance through
every `StreamTableMeta` read. The focused binding table avoids that broad hot
path change.

### SQL, policy, and release artifacts

| File | Planned change |
|---|---|
| `sql/pg_trickle--0.87.11--0.87.12.sql` | Create/backfill the binding table from validated observed state, assert completeness, leave it out of config dump, and alter exact function attributes. |
| `sql/archive/pg_trickle--0.87.12.sql` | Regenerated fresh-install SQL. |
| `scripts/sql_api_policy.json` | Point at the v0.87.12 archive and mark both publication APIs `capability_specific`. |
| `scripts/run_light_e2e_tests.sh` | Include the focused v0.87.12 security test binary; stock PostgreSQL 18 supports the required publication cases. |
| Version/package files | Bump all normal v0.87.12 release references together. |

### Tests

| File | Planned change |
|---|---|
| `tests/e2e_v08712_publication_security_tests.rs` | New strict authorization, ownership, provenance, rollback, minimal-grant, lifecycle, and concurrency suite. |
| `tests/e2e_upgrade_tests.rs` | Add exact v0.87.11 to v0.87.12 success and rollback cases. |
| `tests/catalog_compat_tests.rs` | Add the binding table, constraints, and explicit no-config-dump contract if this suite owns those checks. |
| `tests/e2e_publication_crash_recovery_tests.rs` | Remove tolerant setup behavior where the v0.87.12 image guarantees publication support; retain disruption coverage. |

### Documentation

| File | Planned change |
|---|---|
| `docs/PUBLICATIONS.md` | Correct publisher privileges and ownership, explain immutable binding behavior, native owner transfer, and safe recovery. |
| `docs/SQL_REFERENCE.md` | Document create/drop authority, SQLSTATEs, atomicity, and minimal grants. |
| `docs/UPGRADING.md` | Add preflight, observed-state legacy backfill, failure recovery, ACL retention, and detach-before-dump/recreate-after-restore guidance. |
| `docs/ERRORS.md` | Add publication-binding mismatch reasons and hints. |
| `docs/SECURITY_GUIDE.md` and `docs/SECURITY_MODEL.md` | State the caller-DDL/definer-bookkeeping boundary and distinguish downstream from internal WAL publications. |
| `CHANGELOG.md`, `ROADMAP.md`, `roadmap/v0.87.12.md` | Record implemented behavior and close exit criteria at release time. |

## 9. Verification plan

### 9.1 Unit and static checks

- Every mismatch reason from section 3.3 has a pure unit test.
- Relation OID comparison sorts and deduplicates input and rejects NULL or
  incomplete catalog state. `FOR ALL TABLES` and schema membership fail the
  scope classifier.
- Caller-context tests prove role, saved `search_path`, `row_security`, GUC,
  PostgreSQL error, and Rust unwind restoration.
- Object-lock helpers contain the required `// SAFETY:` comments.
- No non-test publication path uses `unwrap()` or `panic!()`.
- Every catalog `name` value fetched as Rust `String` is cast to `text`.
- The privilege-boundary check accepts the two pinned definer entry points and
  finds no caller-derived SQL in definer context.
- Exact API policy generation retains deny-first ACLs.

### 9.2 Fresh-install security matrix

Create roles with only these grants where applicable:

- `USAGE` on schema `pgtrickle`;
- exact `EXECUTE` on the create/drop publication functions;
- exact stream-table creation grants needed by the fixture;
- source `SELECT` and source schema `USAGE`;
- target schema `CREATE` for stream creation; and
- current database `CREATE` only in the allowed publication cases.

Do not grant private catalog access or `EXECUTE ON ALL FUNCTIONS`.

Required assertions:

- stream owner plus database `CREATE` creates successfully;
- revoking database `CREATE` immediately denies creation;
- restoring database `CREATE` restores creation without private grants;
- non-owner cannot create or drop even with database `CREATE`;
- the exact `pubowner` is the original caller OID;
- inherited-role and superuser cases follow the documented owner policy;
- dropping requires the caller's native publication ownership;
- stream ownership transfer does not transfer publication ownership;
- `PUBLIC` lacks function `EXECUTE`;
- both functions have `prosecdef = true` and the exact pinned `proconfig`; and
- private binding table privileges remain absent for the test roles.

### 9.3 Identifier and conflict cases

- Unqualified stream names resolve under the original caller's saved path.
- Quoted schema and stream names preserve case and embedded quotes.
- The generated publication name is quoted as one identifier.
- A stream name near PostgreSQL's identifier limit produces a valid,
  deterministic, collision-resistant publication name without splitting a
  UTF-8 code point.
- A hostile-looking identifier cannot add a second SQL statement.
- A preexisting unrelated publication with the generated name remains
  unchanged after create fails.
- A publication with a similar prefix or a different internal WAL CDC name is
  never read, altered, or dropped.

### 9.4 Binding mismatch matrix

For each case, call both explicit drop and `repair_stream_table()`. Where
applicable, also call `drop_stream_table()` and bulk drop. Assert the stable
reason and unchanged live/private state.

| Fixture | Expected reason | Object that must survive |
|---|---|---|
| Drop the live publication | `missing_publication` | Private binding remains for diagnosis. |
| Drop and recreate the same name | `publication_name_reused` | Replacement publication. |
| Rename the bound publication | `publication_renamed` | Renamed publication. |
| Transfer publication ownership | `publication_owner_changed` | Re-owned publication. |
| Add or remove a relation | `publication_relations_changed` | Altered publication and all member tables. |
| Add a schema to the publication | `publication_scope_changed` | Altered publication and schema tables. |
| Replace the stream storage OID | `stream_relation_changed` | Replacement stream relation. |
| Change only the legacy name field as superuser | `private_binding_incomplete` | Canonical publication. |
| Delete only the canonical binding as superuser | `private_binding_incomplete` | Live publication and legacy name. |
| Create an unrelated publication with no private binding | `PublicationNotFound` | Unrelated publication. |

### 9.5 Rollback and concurrency

- Run every failure row from section 4.2 and assert all three states: live
  publication, canonical binding, and legacy name.
- Use deterministic barriers for every case from section 5.3.
- Put a subscriber or replication slot on a publication where practical and
  confirm a rejected binding operation does not disrupt it.
- Kill/restart PostgreSQL after a committed binding and confirm object and
  provenance remain consistent.
- Abort an explicit transaction after successful create and after successful
  drop; both operations must restore their pre-transaction state.

### 9.6 Upgrade tests

For a real v0.87.11 to v0.87.12 image:

- create a valid legacy name-only binding, upgrade, and verify every provenance
  field from live catalogs;
- preserve the existing publication OID, owner, relation set, and subscriber
  across the in-place extension upgrade;
- preserve explicit function grants and keep `PUBLIC` revoked;
- verify exact `prosecdef` and pinned `search_path`;
- verify the binding table is not registered for config dump and that
  fresh/upgrade schemas otherwise match;
- use a separate `E2eDb` fixture for each failed-upgrade case so one aborted
  extension-update transaction cannot contaminate another;
- create a missing-name legacy row and a currently named publication with a
  wrong relation set or scope, then prove each upgrade fails with no partial
  table, binding, or function-attribute change;
- verify a currently valid legacy name is adopted while preflight documents
  that pre-v0.87.12 rename or same-name-reuse history is unknowable; and
- rerun `lifecycle_preflight()` after a successful upgrade and require an OK
  publication section.

Guard the suite with the exact upgrade harness environment:
`PGS_UPGRADE_FROM=0.87.11` and `PGS_UPGRADE_TO=<current version>`. Skip with an
explicit reason when either value does not match; never pass against a
different source version.

### 9.7 Required commands

During implementation, after every code change:

```bash
just fmt
just lint
```

Run the focused and generated-contract checks:

```bash
just test-unit
just privilege-boundary-check
bash scripts/check_security_definer.sh
python3 scripts/check_sql_api_policy.py self-test
python3 scripts/check_sql_api_policy.py check
python3 scripts/gen_catalogs.py --check
python3 scripts/gen_test_schema.py --check
python3 scripts/gen_plans_index.py --check
just check-version-sync
just check-meta-version
just check-upgrade 0.87.11 0.87.12
just check-upgrade-all
```

Run database tiers in increasing cost order:

```bash
just test-integration
./scripts/run_e2e_tests.sh --test e2e_v08712_publication_security_tests --no-capture
just test-light-e2e
just test-e2e
just test-upgrade 0.87.11 0.87.12
just test-all
```

Before merge, dispatch the complete branch CI because this change touches a
security boundary, transactional DDL, upgrade behavior, and concurrency:

```bash
gh workflow run ci.yml --ref <implementation-branch>
```

## 10. Documentation contract

The release documentation must say:

- A publisher needs stream ownership and `CREATE` on the current database.
- The role that calls the API owns the new publication.
- A subscriber's `REPLICATION` and table `SELECT` grants are separate from the
  publisher's creation rights.
- Explicit drop also requires PostgreSQL publication ownership or superuser.
- Stream ownership transfer does not transfer an existing publication.
- pg_trickle records OID provenance and refuses to act on a stale name.
- Rename, owner change, relation-set change, storage replacement, and same-name
  recreation require operator reconciliation before pg_trickle drop or repair.
- DDL and private registration are atomic.
- Ordinary users must not receive private catalog or `pgtrickle_changes`
  privileges.
- Internal WAL CDC publications are outside this downstream API and are not
  adopted by its binding logic.
- Raw publication bindings are not portable across dump/restore in v0.87.12;
  detach them before dump and recreate them through the API after restore.

Remove or correct the current claim in `docs/PUBLICATIONS.md` that a
publication is always owned by the role that originally created the stream
table. The owner is the role that successfully calls the publication API.

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Converting the entry point to definer lends database authority. | Run only canonical lookup and binding writes as definer. Run publication DDL inside the captured caller context and verify owner afterward. |
| A definer-side ACL query differs from PostgreSQL DDL semantics. | Let native `CREATE PUBLICATION` and `DROP PUBLICATION` enforce privileges. Use prechecks only to make multi-target failures early, never as authorization replacement. |
| Same-name recreation targets an unrelated object. | Store OID, lock by OID, reread, compare name, and never fall back from missing OID to name-based mutation. |
| Rename or owner transfer races validation. | Acquire a PostgreSQL publication object lock, then reread catalogs before mutation. |
| Table replacement silently changes publication membership. | Store the stream relation OID separately, lock the relation, and compare current stream and explicit relation OIDs. |
| Stream drop keeps the old unsafe sibling path. | Delete direct SQL from `execute_drop_stream_table()` and route all cleanup through the shared validator. |
| Bulk drops deadlock on overlapping target sets. | Acquire sorted advisory locks, then sorted publication locks, then sorted relation locks before child-first execution, matching native publication lock order. |
| Private cleanup fails after public DDL. | Keep both phases in one transaction and propagate the error so PostgreSQL rolls back DDL. |
| Legacy rows cannot prove historical provenance. | Treat upgrade as a documented observed-state trust cutover. Reject missing or observably invalid current objects; do not claim to detect indistinguishable pre-v0.87.12 name reuse. |
| A dedicated binding table duplicates the legacy name field. | Make the new table authoritative, update both values atomically, diagnose mismatch, and defer legacy-column removal to a compatibility release. |
| Raw binding OIDs become unsafe after dump/restore. | Do not config-dump the binding table. Fail closed when only the restored legacy name remains, document detach-before-dump/recreate-after-restore, and defer stable-identifier remapping. |
| Health checks miss stale publications. | Join by stored OID, then compare the stored name and relation set. Do not use an inner name join. |
| Failure injection adds a production-only test knob. | Prefer temporary table triggers, event triggers, transaction aborts, and pure classifier tests. Add no GUC unless no existing PostgreSQL fixture can reach a required boundary. |
| Existing crash test skips the security assertion after setup failure. | Make v0.87.12 security fixtures strict. A missing publication is a failed test, not a skip. |
| Publication work accidentally changes internal WAL CDC. | Keep `src/wal_decoder.rs` out of the implementation unless a shared helper change requires a narrow compile fix. Add an unrelated-internal-publication regression test. |

## 12. Pull-request sequence

Use one implementation PR for this six-week release. The migration, security
boundary, dual representation writes, shared drop fix, and rollback tests are
one correctness unit. If review size requires slices, make them a stacked
review series and merge them together only when the final slice is green:

1. **Contracts and provenance.** Add the probes, strict tests, binding table,
   classifier, preflight, and upgrade backfill.
2. **Caller-equivalent create.** Add the caller wrapper, pinned entry point,
   verified caller DDL, atomic canonical/legacy writes, and rollback tests.
3. **Validated drop and repair.** Add locks, explicit drop, cascade/bulk routing,
   repair and storage-replacement gates, errors, and concurrency tests.
4. **Release artifacts.** Regenerate SQL/policy/docs, run direct upgrade and the
   complete CI matrix, then mark the roadmap implemented.

No intermediate slice may ship a canonical table that current create/drop
paths do not maintain. If stacked review is unavailable, keep the work in the
single PR rather than merge a partially authoritative representation.

Do not merge a definer publication entry point before caller-context DDL tests
pass. Do not merge provenance columns without routing the stream-drop sibling
path through them.

## 13. Definition of done

- [ ] A stream owner without database `CREATE` cannot create a publication.
- [ ] A stream owner with database `CREATE` and exact function `EXECUTE` can
      create without private catalog grants.
- [ ] The resulting `pg_publication.pubowner` equals the original caller OID.
- [ ] Non-owners cannot create, drop, repair, or remove a stream with an
      attached publication through borrowed definer authority.
- [ ] Bindings record publication name/OID, owner OID, bound stream OID, and
      exact relation OIDs.
- [ ] Missing, renamed, recreated, re-owned, retargeted, and stream-replaced
      bindings return stable errors before mutation.
- [ ] Explicit drop and automatic stream cleanup use the same validator and
      never trust a name alone.
- [ ] `repair_stream_table()` validates publication provenance before any
      repair mutation.
- [ ] Caller DDL and private bookkeeping commit or roll back together at every
      injected failure phase.
- [ ] Create/drop/native-DDL races use deterministic barriers and finish with a
      valid binding, no binding, or a diagnosable stale binding. No unrelated
      publication is changed.
- [ ] Fresh install and v0.87.11 upgrade agree on schema, constraints,
      no-config-dump policy, attributes, ACLs, and API policy.
- [ ] Missing or observably invalid legacy bindings fail upgrade without
      partial catalog or function changes; valid rows establish a documented
      observed-state trust cutover.
- [ ] `PUBLIC` lacks execute rights, explicit grants survive upgrade, and
      ordinary callers need no private table grants.
- [ ] `health_check()` and `lifecycle_preflight()` diagnose every binding
      mismatch by OID.
- [ ] Documentation separates publisher rights, subscriber rights, downstream
      publications, and internal WAL CDC publications.
- [ ] No new production GUC, dependency, background worker, or public force
      API is added.
- [ ] Formatting, lint, unit, integration, light/full E2E, direct upgrade,
      full repository, and manual branch CI gates pass with zero warnings.
