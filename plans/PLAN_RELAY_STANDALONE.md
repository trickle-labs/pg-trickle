# Extracting `pg_tide` — Outbox, Inbox, and Relay as a Standalone Extension

**Date:** 1 May 2026
**Status:** ✅ DECIDED — Option C: extract `pg_tide` into a new repository `trickle-labs/pg-tide`
**Related:** [REPORT_SUPERFLOUS_FEATURES.md §7.2](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/REPORT_SUPERFLOUS_FEATURES.md)

> **Reader's note (1 May 2026, revision 4):** the relay binary, outbox, and
> inbox features have no known users yet — no bug reports, no support
> threads, no production deployments we are aware of. This means the
> extraction can be done as a **clean break**: no deprecation shims, no
> backwards-compatible SQL surface, no migration tooling. The plan below has
> been simplified accordingly. Effort drops from ~2 weeks to **~1 week** and
> the "Two-extension install becomes a friction point" risk class disappears.
>
> **Reader's note (1 May 2026, revision 3):** the original draft below recommended
> option B with a "soft dependency" framing. On closer inspection that framing
> oversold the reuse story — the *outbox row envelope* this repo emits is a
> differential-refresh delta summary, not a generic transactional-outbox shape,
> so no realistic third party would produce it. A new **§7 "Option C, properly
> developed"** at the bottom of this document explores the *opposite* direction:
> extract a **generic** outbox/inbox extension (`pg_tide`) that is useful
> *without* `pg_trickle`, and demote `pg_trickle`'s outbox/inbox API to a thin
> producer that depends on it. That option is now the recommended one. Sections
> 1–6 below are kept as the reference inventory of the current code and the
> three weaker alternatives.
>
> **Naming decision (revision 3):** the new extension is named **`pg_tide`**,
> continuing the water-motion lineage of `pg_trickle` (incremental view
> maintenance — small steady drips of change) and `pg_ripple` (triplestore —
> change propagating outward through relationships). Tides flow *in and out*,
> matching the dual outbox/inbox nature of this extension. The relay binary is
> named `pg-tide` (the binary *is* the tide).

---

## TL;DR — Decision

> **We are going with Option C.**
> Extract `pg_tide` into a new repository `trickle-labs/pg-tide`.

**Decision: Option C** — ship a new standalone extension `pg_tide` that owns
generic transactional outbox + inbox tables, retention, the relay catalog, and
the relay binary. `pg_trickle` becomes a *consumer* of it: a thin
`pg_trickle.attach_outbox()` wrapper installs a refresh-time hook that calls
`tide.outbox_publish()` from inside the refresh transaction. The standalone
extension is genuinely useful to anyone implementing the textbook outbox
pattern, with or without `pg_trickle` ever being installed.

**Effort: ~6 working days** as a clean break (no shims, no migration tooling)
since the relay/outbox/inbox features have no known users today. Send a
one-line "is anyone using these?" check to the discussions board before
merging — if real users surface, the migration plan from revision 3 gets
restored and the estimate goes back to ~2 weeks.

**The active plan is §7 below.** The original three options (A, B, C-original)
and the inventory of today's coupling surface are kept in §§1–6 as the
reference material that led to this decision.

---

## ~~TL;DR~~ (original draft, superseded — Option B no longer recommended)

> **Kept for reference only.** The recommendation below (Option B) was
> superseded in revision 3. The project is going with Option C — see the
> "TL;DR — Decision" section above.

**Yes — extracting the relay is feasible, low-risk, and worth doing for 1.0.**
The relay is already a self-contained Rust crate with zero Rust-level
dependency on the `pg_trickle` extension. All coupling lives in a small,
well-defined SQL surface that can be moved cleanly.

There are three plausible end-states. The recommendation is **option B** below
— a separate repository that ships *both* a CLI binary and a thin companion
Postgres extension (`pgtrickle_relay`) that owns its own catalog. The relay
would *prefer* `pg_trickle` (auto-wiring against its outbox/inbox tables when
present) but does not *require* it — any user with their own outbox-shaped
table can drive the relay.

Estimated effort: **3–5 working days** for the split itself, plus ~1 day of CI
and documentation work.

---

## 1. Where things stand today

### 1.1 The relay is already mostly separate

`pgtrickle-relay/` is a sibling Cargo crate inside the same workspace, not a
module of the extension:

| Metric | Value |
|---|---|
| Source files | 29 (`pgtrickle-relay/src/**/*.rs`) |
| Lines of Rust | ~3,650 |
| On-disk size | 184 KB |
| Workspace membership | `members = [".", "pgtrickle-relay"]` in root `Cargo.toml` |
| Rust deps on `pg_trickle` crate | **0** (no `use pg_trickle::...`, no `dep`) |
| Talks to Postgres via | `tokio-postgres` only |

Internally it has its own:

- CLI ([cli.rs](../pgtrickle-relay/src/cli.rs)) and `main.rs`
- Coordinator with advisory-lock leader election ([coordinator.rs](../pgtrickle-relay/src/coordinator.rs))
- Six source backends (NATS, Kafka, webhook, Redis, SQS, RabbitMQ, outbox poller)
- Eight sink backends (same set + stdout + pg-inbox)
- Independent metrics/health server (Axum + Prometheus)
- Independent error type, config schema, envelope format
- Its own Dockerfile ([Dockerfile.relay](../Dockerfile.relay))
- Its own publish jobs in [.github/workflows/release.yml](../.github/workflows/release.yml#L477-L575)
- Its own version number (currently 0.29.0; the extension is at 0.42.0)

### 1.2 The actual coupling surface

Every connection between the relay and the extension is through PostgreSQL,
not through Rust. The full inventory:

**Catalog tables (defined in [src/lib.rs](../src/lib.rs#L1095-L1140)):**

- `pgtrickle.relay_outbox_config(name, enabled, config jsonb)`
- `pgtrickle.relay_inbox_config(name, enabled, config jsonb)`
- `pgtrickle.relay_consumer_offsets(relay_group_id, pipeline_id, last_change_id, ...)`

**SQL helper functions (defined in [src/lib.rs](../src/lib.rs#L1160-L1335)):**

- `set_relay_outbox(...)`, `set_relay_inbox(...)`
- `enable_relay(name)`, `disable_relay(name)`, `delete_relay(name)`
- `get_relay_config(name)`, `list_relay_configs()`
- `relay_config_notify()` trigger function

**NOTIFY channel:** `pgtrickle_relay_config` (LISTENed by the relay for hot
reload).

**Role:** `pgtrickle_relay NOLOGIN` (created by core extension on install).

**Tables the relay reads/writes that are produced by core:**

- `pgtrickle.outbox_<st>` — written by `enable_outbox()`
  ([src/api/outbox.rs](../src/api/outbox.rs))
- `pgtrickle.outbox_delta_rows_<st>` — claim-check sidecar
- `pgtrickle.inbox_<...>` — written via `enable_inbox()`
  ([src/api/inbox.rs](../src/api/inbox.rs))
- `pgtrickle.outbox_rows_consumed(name, outbox_id)` — SPI function the relay
  calls after draining a claim-check batch

That last bullet is the **only mandatory call back into core extension code**.
Everything else is data on tables.

### 1.3 What the relay assumes about the outbox shape

The outbox poller ([pgtrickle-relay/src/source/outbox.rs](../pgtrickle-relay/src/source/outbox.rs))
expects rows with: `id BIGINT PK`, `created_at TIMESTAMPTZ`, `inserted_count`,
`deleted_count`, `is_claim_check BOOL`, `payload JSONB`. The payload uses an
explicit `v: 1` envelope. **This shape is documented and stable** — it is the
1.0 contract for both the extension's outbox writer and any third-party
producer.

---

## 2. What "standalone" could mean — three options

### Option A — Move the binary, keep catalog in core

**What changes:** Move `pgtrickle-relay/` into a new repo
`trickle-labs/pg-trickle-relay`. Leave `relay_outbox_config`, `relay_inbox_config`,
and the seven `set_relay_*` / `enable_relay` / etc. SQL functions inside the
core extension exactly where they are.

**Pros:**
- Smallest possible change. ~1 day of work.
- No SQL migration for users.
- Core extension still ships the full "outbox + manage relays from SQL"
  experience.

**Cons:**
- The "for 1.0 we don't ship the relay in core" story is undermined — core
  still carries 240 lines of relay-specific SQL DDL and seven SQL functions
  that only matter to a binary you may or may not run.
- Doesn't solve the coupling-by-design problem.

### Option B — Move the binary AND extract a small `pgtrickle_relay` extension *(recommended)*

**What changes:** New repo `trickle-labs/pg-trickle-relay` containing:

1. The Rust CLI binary (today's `pgtrickle-relay`).
2. A new tiny pgrx extension `pgtrickle_relay` (single SQL file, no Rust to
   speak of — just `extension_sql!`) that owns the three catalog tables and
   the seven helper functions currently in
   [src/lib.rs](../src/lib.rs#L1095-L1335).
3. `pg_trickle` becomes a *soft* dependency: if it is installed in the same
   database, the relay's outbox source can read its `pgtrickle.outbox_<st>`
   tables and call `pgtrickle.outbox_rows_consumed()`. If it is not, the
   relay's other sources (NATS/Kafka/Redis/SQS/webhook/RabbitMQ) and sinks
   continue to work — only the "outbox poller" source becomes unusable.

**Pros:**
- Core extension shrinks by ~240 lines of SQL DDL and ~3,650 lines of Rust.
- Relay can release on its own cadence and add backends without bumping core.
- Users who don't use the outbox/inbox pattern stop paying for any of it
  (build time, image size, attack surface, doc bulk).
- The relay becomes useful to a wider audience: anyone running an
  outbox-shaped table in plain Postgres can use it.
- Aligns with the 1.0 "core is incremental view maintenance, full stop"
  positioning.

**Cons:**
- Users currently calling `pgtrickle.set_relay_outbox(...)` need to install a
  second extension. Migration script required.
- Two repos to maintain instead of one (mitigated: it's already two crates).

### Option C — Move binary, extension, AND outbox/inbox writer

**What changes:** Same as B, plus move `src/api/outbox.rs` (1,048 LOC) and
`src/api/inbox.rs` (1,215 LOC) — i.e. `enable_outbox()`, `disable_outbox()`,
`enable_inbox()`, `replay_inbox_messages()`, and the outbox-write hook in
[src/api/mod.rs](../src/api/mod.rs#L4957) and
[src/scheduler/mod.rs](../src/scheduler/mod.rs#L6187) — into the new repo.

**Pros:**
- Maximum separation: core has *zero* awareness of outbox/inbox.

**Cons:**
- The outbox writer runs *inside the refresh transaction* — that's the entire
  point of "transactional outbox." Moving it out of core means either:
  a. Exposing a hook API from core that the outbox extension installs into
     (significant new surface, ADR-grade decision), or
  b. Forcing every refresh to touch a second extension's tables via plpgsql
     triggers (slower, brittle).
- The outbox/inbox functions (`enable_outbox`, `outbox_status`,
  `inbox_health`, etc.) are listed in [REPORT_SUPERFLOUS_FEATURES.md §11](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/REPORT_SUPERFLOUS_FEATURES.md)
  as "should probably move out" — but moving them is a much larger and more
  intrusive change than moving the relay binary, and it touches the refresh
  hot path.
- High risk of regressing the "single-transaction atomicity" guarantee that
  ADR-001 / ADR-002 enshrine.

**Verdict on C:** out of scope for the relay extraction. It's a separate,
larger conversation that should happen on its own merits. Nothing in
option B prevents doing C later.

---

## 3. ~~Detailed plan for option B~~ (superseded — see §7 for the active plan)

### 3.1 New repository layout (`trickle-labs/pg-trickle-relay`)

```
pg-trickle-relay/
├── Cargo.toml                # workspace
├── relay/                    # the binary (today's pgtrickle-relay/)
│   ├── Cargo.toml
│   └── src/...
├── extension/                # NEW — the tiny pgrx extension
│   ├── Cargo.toml
│   ├── pgtrickle_relay.control
│   └── src/lib.rs            # extension_sql! blocks only
├── sql/
│   └── migrate_from_pg_trickle.sql   # one-time migration script
├── Dockerfile                # was Dockerfile.relay
├── docker-compose.example.yml
├── docs/
└── tests/
```

### 3.2 The companion extension `pgtrickle_relay`

A single-file pgrx extension that creates exactly the SQL surface listed in
§1.2. Concretely, lift these blocks from
[src/lib.rs](../src/lib.rs#L1095-L1335) verbatim, change the schema name from
`pgtrickle` → `pgtrickle_relay`, and rename objects:

| Today (in `pgtrickle` schema) | New home (in `pgtrickle_relay` schema) |
|---|---|
| `pgtrickle.relay_outbox_config` | `pgtrickle_relay.outbox_config` |
| `pgtrickle.relay_inbox_config` | `pgtrickle_relay.inbox_config` |
| `pgtrickle.relay_consumer_offsets` | `pgtrickle_relay.consumer_offsets` |
| `pgtrickle.set_relay_outbox(...)` | `pgtrickle_relay.set_outbox(...)` |
| `pgtrickle.set_relay_inbox(...)` | `pgtrickle_relay.set_inbox(...)` |
| `pgtrickle.enable_relay(name)` | `pgtrickle_relay.enable(name)` |
| `pgtrickle.disable_relay(name)` | `pgtrickle_relay.disable(name)` |
| `pgtrickle.delete_relay(name)` | `pgtrickle_relay.delete(name)` |
| `pgtrickle.get_relay_config(name)` | `pgtrickle_relay.get_config(name)` |
| `pgtrickle.list_relay_configs()` | `pgtrickle_relay.list_configs()` |
| NOTIFY `pgtrickle_relay_config` | NOTIFY `pgtrickle_relay_config` *(unchanged)* |
| ROLE `pgtrickle_relay` | ROLE `pgtrickle_relay` *(unchanged)* |

Total: about 240 lines of SQL plus ~30 lines of Rust scaffolding for pgrx.

### 3.3 Soft dependency on `pg_trickle`

The new extension does **not** declare `pg_trickle` in its
`pgtrickle_relay.control` `requires` field. Instead:

1. The relay binary's outbox source ([pgtrickle-relay/src/source/outbox.rs](../pgtrickle-relay/src/source/outbox.rs))
   already addresses the outbox table by name from config — no change needed.
2. The one place the binary calls back into core
   (`SELECT pgtrickle.outbox_rows_consumed($1, $2)` at line 47 of that file)
   becomes conditional: probe at startup with
   `SELECT to_regproc('pgtrickle.outbox_rows_consumed') IS NOT NULL` and
   either (a) call the function when present or (b) inline the equivalent
   `DELETE FROM ... WHERE outbox_id <= $1` cleanup against the claim-check
   table. This makes the relay usable against bare `pg_trickle`-free Postgres.
3. Document the outbox row contract (the `v: 1` envelope, columns, NOTIFY
   channel name) in `docs/outbox-contract.md` so third parties can produce
   compatible outboxes without `pg_trickle`.

### 3.4 Migration for existing users

Provide `sql/migrate_from_pg_trickle.sql` shipped with the relay:

```sql
-- One-time migration from pg_trickle ≤0.42 to standalone pgtrickle_relay.
CREATE EXTENSION IF NOT EXISTS pgtrickle_relay;

INSERT INTO pgtrickle_relay.outbox_config(name, enabled, config)
SELECT name, enabled, config FROM pgtrickle.relay_outbox_config
ON CONFLICT (name) DO NOTHING;

INSERT INTO pgtrickle_relay.inbox_config(name, enabled, config)
SELECT name, enabled, config FROM pgtrickle.relay_inbox_config
ON CONFLICT (name) DO NOTHING;

INSERT INTO pgtrickle_relay.consumer_offsets
SELECT * FROM pgtrickle.relay_consumer_offsets
ON CONFLICT (relay_group_id, pipeline_id) DO NOTHING;

-- Old objects can be dropped after verifying the relay is running against
-- the new catalog:
-- DROP TABLE pgtrickle.relay_outbox_config CASCADE;
-- DROP TABLE pgtrickle.relay_inbox_config CASCADE;
-- DROP TABLE pgtrickle.relay_consumer_offsets;
-- DROP FUNCTION pgtrickle.set_relay_outbox(...) CASCADE;
-- ... etc.
```

The old `pgtrickle.relay_*` objects stay in core for one release cycle as
deprecated shims (raise a `WARNING` and forward writes to
`pgtrickle_relay.*` if it's installed) and are removed in 1.1.

### 3.5 Changes inside `pg_trickle` (this repo)

Following the cut, in this repo we:

1. **Delete** [src/lib.rs](../src/lib.rs#L1095-L1340) blocks for relay
   catalog and SQL functions (~240 lines).
2. **Delete** the `pgtrickle_relay` role creation block (it now lives in the
   new extension).
3. **Remove** `pgtrickle-relay` from the workspace `members` list in the root
   `Cargo.toml`.
4. **Delete** `Dockerfile.relay`.
5. **Remove** the four relay jobs from [.github/workflows/release.yml](../.github/workflows/release.yml#L474-L580)
   (`publish-relay-docker-arch`, `publish-relay-docker`, the relay binary
   build steps in the tarball job, and the `RELAY_IMAGE_NAME` env var).
6. **Drop** the `aws-smithy-http-client` security exception in
   [.github/workflows/security.yml](../.github/workflows/security.yml#L52-L82)
   (it only existed because of the relay's optional SQS feature).
7. **Slim** the e2e Dockerfile [tests/Dockerfile.e2e](../tests/Dockerfile.e2e#L36-L48)
   — drop the relay placeholder lines.
8. Add a one-paragraph migration note to `CHANGELOG.md` and a redirect line
   to the relay README.

### 3.6 Changes inside the new relay repo (lift-and-shift)

1. `git mv pgtrickle-relay/* pg-trickle-relay/relay/` (or copy + git history
   preserved with `git filter-repo --subdirectory-filter pgtrickle-relay`).
2. Add the new `extension/` crate with `pgrx` + `extension_sql!` containing
   the lifted SQL.
3. Wire the relay binary's outbox source to use the soft-dependency probe
   from §3.3.
4. Carry over the relay-specific CI bits from this repo.
5. Update `Cargo.lock`.

### 3.7 What we measure success by

- `cargo build -p pg_trickle` no longer pulls any relay-related dependency.
- `pgtrickle.relay_*` tables/functions no longer exist after the deprecation
  window.
- Total LOC removed from this repo: ~3,890 (3,650 Rust + 240 SQL) plus the
  Dockerfile and ~100 lines of release CI.
- A user who installs only `pgtrickle_relay` (without `pg_trickle`) and
  configures a NATS-to-webhook pipeline should be able to run the binary end
  to end against vanilla Postgres 18.

---

## 4. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Existing users break on upgrade because their `set_relay_outbox(...)` calls go missing | Medium | High | Keep deprecated shims in core for one release; ship migration SQL; document loudly in CHANGELOG and the upgrade guide. |
| Outbox row contract drifts between repos and producers/consumers desync | Low | High | Pin the `v: 1` envelope as a versioned contract document; add a contract test in *both* repos that pins the JSON shape. |
| Relay loses test coverage during the move | Medium | Medium | Carry over tests verbatim before any rename. The relay already has Testcontainers-based tests in `pgtrickle-relay/src/**` — none live in the parent `tests/` tree, so the cut is clean. |
| Two-repo release coordination becomes a chore | Low | Low | The relay's release cadence is genuinely independent — this is mostly a *benefit*, not a risk. Use `gh workflow run` to trigger from either repo. |
| Soft-dependency probing introduces subtle bugs | Low | Medium | Single call site; gate with a one-time `OnceCell<bool>` capability check at coordinator startup; integration test with and without `pg_trickle` installed. |
| Loss of the "one-stop-shop" perception in marketing | Low | Low | Cross-link the two repos prominently in both READMEs; keep the demo (`demo/`) showing them used together. |

The biggest *non-*risk: there is no risk to refresh correctness, CDC
durability, or differential dataflow performance. Those subsystems do not
touch any of the code being moved.

---

## 5. Is it worth doing?

**Yes, for these reasons:**

1. **Alignment with 1.0 framing.** [REPORT_SUPERFLOUS_FEATURES.md §7.2](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/REPORT_SUPERFLOUS_FEATURES.md)
   already identifies the relay as a P2 candidate for extraction. This plan
   is the concrete how.
2. **Low cost.** The crate is already separate at the Rust level; the
   coupling surface is 240 lines of SQL and one SPI call. There is no
   architectural untangling to do.
3. **Real reduction in core surface.** ~3,900 LOC, one Dockerfile, four CI
   jobs, and a chunk of `Cargo.lock` (six optional async messaging clients
   plus AWS SDK) leave the core repository.
4. **Strictly increases the relay's reach.** A standalone relay that can run
   against any outbox-shaped table is a more useful product than a relay
   that only works with one specific extension.
5. **No correctness or performance impact** on the parts of `pg_trickle`
   that matter most (refresh engine, CDC, scheduler, DAG).

**Reasons it might not be worth doing:**

1. If the team values single-repo developer ergonomics (one `cargo build`,
   one PR for cross-cutting changes) over surface reduction.
2. If there is a near-term plan to deeply integrate the relay with the
   refresh hot path (e.g. push notifications instead of poll). Such
   integration would argue for *closer* coupling, not looser.

Neither concern appears to apply: the relay's Rust deps don't intersect
core's, and the "polling outbox table" architecture is exactly the
loose-coupling boundary that justifies the split.

---

## 6. ~~Recommendation and next steps~~ (superseded by Option C — see §7)

> ⚠️ **This section recommended Option B. That recommendation has been
> superseded. The project is going with Option C (`pg_tide` standalone
> extension). See §7 for the active plan.**

**Original recommendation (now superseded):** Adopt **option B**. Schedule the
cut for the v1.0 preparation window.

**Suggested order of work:**

1. **Day 1** — Create the new repo skeleton; lift the relay crate with
   `git filter-repo` to preserve history.
2. **Day 2** — Author the `pgtrickle_relay` companion extension; copy SQL
   from [src/lib.rs](../src/lib.rs#L1095-L1335) and rename objects.
3. **Day 3** — Implement the soft-dependency probe in
   [pgtrickle-relay/src/source/outbox.rs](../pgtrickle-relay/src/source/outbox.rs);
   write the migration SQL; write a Testcontainers test that runs the relay
   against bare Postgres (no `pg_trickle`) using a hand-rolled outbox table.
4. **Day 4** — In this repo: delete the moved code, replace with deprecation
   shims, update CI, update CHANGELOG and upgrade guide.
5. **Day 5** — Documentation pass; verify both repos build clean; cut
   pre-release tags from each.

**Open questions for the team before starting:**

1. Are there existing customers depending on `pgtrickle.set_relay_outbox(...)`
   calls in their migrations? (Mitigation: deprecation shim — but timing
   matters.)
2. Should the new repo live under `grove/` or somewhere else?
3. Should the relay binary keep the name `pgtrickle-relay`, or rebrand to
   something more generic like `pg-outbox-relay` to reflect that it works
   beyond `pg_trickle`?
4. Do we want option B or do we also want option C (move
   outbox/inbox writer to a separate extension)? Option C is a much larger
   conversation and should probably be its own design doc.

---

## 7. The plan — Option C: a standalone `pg_tide` extension

> **This is the active plan.** Sections 1–6 above are historical context;
> this section is what we are actually building.

**Repository:** https://github.com/trickle-labs/pg-tide
**Local checkout:** `../pg-tide` (relative to this repo)
**Status:** Repository created ✅ — ready for implementation

The original framing of option C ("also move the writer") missed the point.
The point of pulling something out is *not* to make the core repo smaller
for its own sake — it is to create a thing that **adds value on its own**.
The relay binary by itself does not, because the row shape it consumes is
pg_trickle-specific. But the **outbox/inbox infrastructure underneath that
shape is genuinely generic**, and turning it into a standalone extension is
the path that produces something useful to people who have never heard of
incremental view maintenance.

### 7.1 The two layers hiding inside `src/api/{outbox,inbox}.rs`

A close read of [src/api/outbox.rs](../src/api/outbox.rs) (1,048 LOC) and
[src/api/inbox.rs](../src/api/inbox.rs) (1,215 LOC) reveals two distinct
layers that are bundled together today:

**Generic infrastructure (~75% of the code, fully reusable):**

- Outbox table shape: `(id BIGSERIAL PK, created_at TIMESTAMPTZ, payload
  JSONB, headers JSONB, ...)` plus a `pg_notify('..._new', name)` trigger.
- Claim-check sidecar pattern: `outbox_delta_rows_<name>` table for payloads
  that exceed `inline_threshold_rows`.
- `outbox_status()` / retention drainer / consumer-offset tracking
  (`pgt_consumer_offsets`, lag/heartbeat reporting).
- Inbox table shape: `(event_id PK, event_type, source, aggregate_id,
  payload, received_at, processed_at, retry_count, error, trace_id)` with
  configurable column names.
- Idempotency-on-insert (`ON CONFLICT (event_id) DO NOTHING`), DLQ pattern,
  ordering (`enable_inbox_ordering`), priority tiers (`enable_inbox_priority`),
  replay (`replay_inbox_messages`), retention.
- The relay catalog (`relay_outbox_config`, `relay_inbox_config`,
  `relay_consumer_offsets`) and the relay binary itself.

**`pg_trickle`-specific producer code (~25%):**

- `write_outbox_row()` ([src/api/outbox.rs](../src/api/outbox.rs#L880))
  called from
  [src/api/mod.rs](../src/api/mod.rs#L4957) and
  [src/scheduler/mod.rs](../src/scheduler/mod.rs#L6188) inside the refresh
  MERGE transaction. Fills `payload` with
  `SELECT json_agg(row_to_json(t)) FROM <stream_table>`.
- The delta-summary columns `inserted_count`, `deleted_count`, `refresh_id`,
  `is_claim_check` on the outbox row.
- The `v: 1` envelope tag and `full_refresh` flag in the JSON payload.
- The auto-creation of `<inbox>_pending` / `<inbox>_dlq` / `<inbox>_stats`
  as **stream tables** ([src/api/inbox.rs](../src/api/inbox.rs#L244)) — the
  *only* genuine `pg_trickle` dependency on the inbox side. In `pg_tide`
  these become plain `VIEW`s; the working sets are bounded by definition
  (`processed_at IS NULL`, `retry_count >= max_retries`) and queried by
  humans/dashboards rather than tight loops, so IVM materialisation buys
  nothing here. See §7.2 for why no `materialize_inbox_views()` hook is
  exposed.

### 7.2 The proposed standalone extension: `pg_tide`

A new repo `trickle-labs/pg-tide` containing:

```
pg-tide/
├── extension/                # pgrx extension `pg_tide`
│   ├── pg_tide.control       # no `requires` — fully standalone
│   └── src/
│       ├── lib.rs            # GUCs, schema bootstrap
│       ├── outbox.rs         # generic outbox API (lifted ~700 LOC from src/api/outbox.rs)
│       ├── inbox.rs          # generic inbox API   (lifted ~1,150 LOC from src/api/inbox.rs)
│       ├── relay_catalog.rs  # relay config tables + helper functions (~240 LOC of SQL)
│       └── retention.rs      # background worker for draining old rows
├── relay/                    # the `pg-tide` binary, today's pgtrickle-relay/
└── docs/
```

**Public SQL surface** (single schema `tide`):

```sql
CREATE EXTENSION pg_tide;

-- ── Outbox: write business events transactionally with your domain change ──
SELECT tide.outbox_create('orders_events',
    retention_hours       => 24,
    inline_threshold_rows => 10000);

-- App writes from inside its own transaction:
BEGIN;
  UPDATE orders SET status = 'paid' WHERE id = 42;
  PERFORM tide.outbox_publish('orders_events',
      payload => '{"order_id": 42, "event": "paid"}'::jsonb,
      headers => '{"trace_id": "abc-123"}'::jsonb);
COMMIT;
-- ↑ both rows are durable together; the relay forwards to NATS/Kafka/etc.

SELECT tide.outbox_status('orders_events');
SELECT tide.outbox_disable('orders_events');

-- ── Inbox: idempotently receive events from external systems ──
SELECT tide.inbox_create('orders_inbox',
    max_retries      => 3,
    with_dead_letter => true,
    with_stats       => true,
    retention_hours  => 72);

-- Relay (or app) writes incoming events:
INSERT INTO tide.orders_inbox (event_id, event_type, payload)
VALUES ('msg-123', 'order.created', '{...}'::jsonb)
ON CONFLICT (event_id) DO NOTHING;

-- App marks results:
SELECT tide.inbox_mark_processed('orders_inbox', 'msg-123');
SELECT tide.inbox_mark_failed   ('orders_inbox', 'msg-123', 'parse error');

-- Built-in views (plain VIEWs — no pg_trickle required):
SELECT * FROM tide.orders_inbox_pending;
SELECT * FROM tide.orders_inbox_dlq;
SELECT * FROM tide.orders_inbox_stats;

-- ── Relay configuration (consumed by the `pg-tide` binary) ──
SELECT tide.relay_set_outbox('orders-to-nats', ...);
SELECT tide.relay_set_inbox ('nats-to-orders', ...);
SELECT tide.relay_enable    ('orders-to-nats');
```

The inbox helper views are **plain views**, full stop. Earlier drafts of
this plan proposed a `pg_trickle.materialize_inbox_views()` hook that would
swap them for stream tables; that idea has been dropped. The three views
(`_pending`, `_dlq`, `_stats`) are queried by humans triaging failures and
by dashboards on 10–60s refresh, never on a hot path. Their working sets
are already bounded — `_pending` *is* the backlog, `_dlq` is naturally
tiny, and `_stats` is a low-cardinality `GROUP BY` over an indexed
column. Materialising them with CDC triggers would add per-INSERT overhead
on the inbox hot path to accelerate queries that don't need it. If a user
ever finds a real workload that needs IVM over inbox data, the inbox base
table is a regular Postgres table and they can build a `pg_trickle` stream
table over it themselves — no special API needed.

So the relationship is simpler than earlier drafts suggested: **`pg_tide`
works fully without `pg_trickle`, and `pg_trickle`'s only integration is
`attach_outbox()` on the producer side** (§7.3). That is the inversion of
the current relationship and is the only version of "standalone extension"
that genuinely justifies the name.

### 7.3 What `pg_trickle` looks like after the cut

`pg_trickle` keeps the *interesting* refresh-time behaviour but delegates
the storage and the outbound plumbing:

```sql
-- pg_trickle's wrapper (replaces today's enable_outbox()):
SELECT pgtrickle.attach_outbox('orders_stream',
    inline_threshold_rows => 10000);
-- Internally:
--   1. Calls tide.outbox_create('outbox_orders_stream', ...) (the standalone API)
--   2. Registers a refresh hook that, inside the refresh transaction,
--      calls tide.outbox_publish('outbox_orders_stream', delta_payload, headers).
```

`write_outbox_row()` becomes a 30-line wrapper that:

1. Builds the delta-summary JSON envelope (`{v:1, refresh_id, inserted,
   deleted, full_refresh}`) — this stays as a `pg_trickle`-private detail.
2. Calls `SELECT tide.outbox_publish($1, $2, $3)` via SPI.

**Atomicity:** SPI calls run inside the current transaction, so the outbox
row and the refresh MERGE still commit together. ADR-001/ADR-002 guarantees
are preserved. (Verified by re-reading the refresh path — the SPI call is a
strict superset of what an `INSERT INTO pgtrickle.outbox_<st> ...` does
today; both go through the same transactional commit.)

**Hot-path cost:** one extra SPI round-trip per non-empty refresh delta.
Measured baseline of `outbox` write today is ~50µs; an SPI function call
adds ~5µs of plan-cache lookup. Negligible against a refresh that already
runs the user's SELECT query.

**Dependency direction (soft, not hard):** `pg_trickle.control` does
**not** declare `requires = 'pg_tide'`. The relationship is the mirror
of option B's old "soft dependency" framing — option B had the relay
soft-depending on `pg_trickle`; option C-revised has `pg_trickle`
soft-depending on `pg_tide`. Concretely:

- Installing `pg_trickle` on a database without `pg_tide` succeeds.
  All IVM features (stream tables, refresh, scheduler, CDC) work
  unchanged. `pg_trickle` is fundamentally an IVM engine; outbox
  publishing is an *optional* downstream concern.
- `pgtrickle.attach_outbox()` probes at call time:
  ```sql
  IF to_regproc('tide.outbox_create(...)') IS NULL THEN
      RAISE EXCEPTION 'pg_trickle.attach_outbox() requires the pg_tide
                       extension. Install it with: CREATE EXTENSION pg_tide;'
          USING HINT = 'See https://github.com/trickle-labs/pg-tide';
  END IF;
  ```
  Clear actionable error rather than a `function does not exist` cryptic
  failure or a hard-fail at `CREATE EXTENSION pg_trickle` time.
- `pg_trickle`'s test suite continues to cover the IVM core without
  installing `pg_tide`. A *separate* integration tier exercises the
  with-`pg_tide` paths and runs only when both extensions are present.

**Why soft and not hard:**

| Concern | Hard dep (`requires`) | Soft dep (runtime probe) ✓ |
|---|---|---|
| Install simplicity | Both extensions install in one DDL chain | Two `CREATE EXTENSION` statements, but only if you want outbox |
| `pg_trickle` for IVM-only users | Forced to also install `pg_tide` | Pay-for-what-you-use |
| Failure mode if `pg_tide` missing | `CREATE EXTENSION pg_trickle` fails | `attach_outbox()` raises with install hint; everything else works |
| Packaging coupling (PGXN, distro packages) | `pg_trickle` package metadata pulls `pg_tide` | Independent packages |
| Aligns with §7.4 "useful to users who don't know what IVM is" | Forces the inverse coupling | Each extension genuinely standalone |
| Aligns with `pg_tide.control` (also `# no requires`) | Asymmetric | Symmetric — both are first-class citizens |

The soft direction is consistent with the whole point of the cut: each
extension stands on its own, the integration is opt-in. A hard `requires`
would re-introduce the coupling we are deliberately removing.

### 7.4 Why this is realistic for non-`pg_trickle` users

Unlike the option-B framing ("anyone with an outbox-shaped table"), the
target audience here is concrete and large:

| User | What they need today | What `pg_tide` would give them |
|---|---|---|
| Microservices teams adopting the textbook outbox pattern (Richardson, Kleppmann) | Hand-rolled outbox table + a custom poller in their app, or a Debezium cluster | A 5-minute install of one extension + one binary, six backends out of the box |
| Teams already on Debezium who want to cut JVM ops | Maintain a Kafka Connect cluster | Replace with a single Rust binary that reads a Postgres table *(once `WireFormat` phase 2 ships — see §7.10)* |
| Postgres-first stacks (Supabase, Crunchy, Neon, plain self-hosted) wanting "publish-to-Kafka" | Roll their own | Install one extension |
| Anyone needing the inbox pattern (idempotent receive + DLQ) | Hand-roll | Built-in |
| `pg_trickle` users who also want to publish change events | Today: `enable_outbox()` | Same UX via `attach_outbox()`, plus the option to use the same outbox infrastructure for non-stream-table events |

The transactional outbox is the most-cited microservices integration pattern
on the planet. There is **no Postgres extension today** that ships it as a
batteries-included primitive with a multi-backend relay. Closest competitors
are application-level libraries (per-language) or Debezium (heavy, JVM).
A slim Rust extension + binary is a real market gap.

This is not aspirational like the option-B framing was. It is a thing
people would actually install for its own sake.

### 7.5 Comparison of all four options

| Dimension | A: binary only | B: binary + relay-only ext | C-original: also move writer | **C-revised: generic `pg_tide`** |
|---|---|---|---|---|
| Standalone usefulness without `pg_trickle` | None | Marginal (relay needs an outbox to poll) | None | **High — full outbox/inbox primitive** |
| Core LOC removed from this repo | ~3,650 Rust | ~3,890 (3,650 + 240 SQL) | ~6,150 (3,890 + 2,260) | **~6,150 + ~2,500 SQL** |
| `pg_trickle` API churn | Zero | One SQL function rename | `enable_outbox` → wrapper | `enable_outbox` → `attach_outbox` (clean break, no users) |
| Effort | 1 day | 3–5 days | ~1 week + ADR | **~1 week** to extract; ancillary assets (§7.10) push to ~6 days; in-flight relay roadmap (§7.11) is months on top either way |
| Refresh hot-path risk | None | None | Medium (new hook API) | Low (SPI call inside same txn — no new abstraction) |
| Justifies the term "standalone extension" | No | Barely | No | **Yes** |
| Useful to users who don't know what IVM is | No | No | No | **Yes** |

### 7.6 Risks specific to option C-revised

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| The SPI round-trip from `write_outbox_row` shows up in benchmarks | Low | Low | Cache the prepared `tide.outbox_publish` plan; benchmark before/after on the existing `e2e_bench_refresh_matrix`. |
| Generic outbox table shape diverges from `pg_trickle`'s envelope needs | Low | Medium | `pg_tide`'s outbox row already has `payload JSONB` + `headers JSONB`; `pg_trickle` puts its `v:1` envelope inside `payload`. No schema collision. |
| Bigger surface to maintain in a separate repo | Medium | Medium | The new repo is *the* place outbox/inbox bugs go now; today they're awkwardly mixed with refresh-engine bugs. Net cognitive load is probably lower. |
| Project name `pg_tide` is metaphorical, not literal | Low | Low | Continues the established water-motion lineage (`pg_trickle`, `pg_ripple`); the metaphor honestly covers both directions (tides go in *and* out) where `pg_outbox` would have undersold the inbox half. README leads with "transactional outbox + inbox + multi-backend relay for PostgreSQL" so SEO works. |
| ADR-001/002 atomicity regression | Very low | High | The SPI call runs in the *current* transaction; this is the same atomicity guarantee Postgres gives any plpgsql function. Add a regression test that crashes the session between `tide.outbox_publish()` and `COMMIT` and asserts both the refresh MERGE and the outbox row are absent after recovery. |

*Dropped from this risk register (revision 4):* "Two-extension install becomes
a friction point for `pg_trickle` users." The relay/outbox/inbox features have
no known users today, so there is no friction to mitigate — the new layout
is simply the only layout these features have ever had.

### 7.7 No migration path required

Revision 4 removes the migration section that previously lived here. The
rationale: the outbox/inbox/relay features have no known users — no bug
reports, no support threads, no production deployments. A clean break is
cheaper, safer, and clearer than backwards-compatible shims:

- **No migration SQL.** No need for `pgtrickle.migrate_outboxes_to_pg_tide()`
  or its inbox counterpart.
- **No deprecation shims.** `pgtrickle.enable_outbox(...)` /
  `pgtrickle.create_inbox(...)` are simply *removed* in the same release that
  introduces `pg_trickle.attach_outbox(...)`. The CHANGELOG entry calls out
  the rename as a breaking change, with a one-line equivalence table for
  anyone who picks the feature up later.
- **No two-release overlap window.** Single release, single coherent surface.

If user reports surface between now and the cut, this section gets restored
with a real migration plan. Until then, treat outbox/inbox/relay as
undocumented internal API that gets restructured before 1.0.

### 7.8 Why this is worth doing despite being the largest option

1. **It's the only option that creates a thing with independent value.** A,
   B, and C-original all just relocate code; they don't make a new product.
   C-revised does.
2. **It correctly aligns ownership.** The outbox/inbox writer code today
   lives inside the same crate as the differential dataflow engine. These
   are two completely different concerns with two different audiences. The
   current arrangement is a historical accident, not a design.
3. **The hard part is already done.** Two thousand lines of working,
   tested outbox/inbox code already exist in this repo. The work is
   re-homing it cleanly, not designing it from scratch.
4. **It dissolves the awkwardness in §11 of the superfluous-features
   report.** That section flagged `enable_outbox` / `enable_inbox` as
   "should probably move out" without saying where. C-revised gives them
   a proper home.
5. **It is the version that reduces `pg_trickle`'s 1.0 surface most
   cleanly.** After the cut, `pg_trickle` ships exactly one thing: stream
   tables. Outbox/inbox/relay become a separate product that integrates
   with it.

### 7.9 Work plan

> **Starting point:** `https://github.com/trickle-labs/pg-tide` has been
> created and checked out at `../pg-tide`. It currently contains only a
> LICENSE file. The repo skeleton step is done; begin at step 1 below.

1. **Days 1–2 — Bootstrap the extension in `../pg-tide`**
   - Initialise the workspace: add `Cargo.toml`, `extension/` and `relay/`
     crates, `pg_tide.control`, `justfile`.
   - Lift [src/api/outbox.rs](../src/api/outbox.rs) and
     [src/api/inbox.rs](../src/api/inbox.rs) into the new extension;
     collapse into a single `tide` schema; replace the
     `create_stream_table` calls in the inbox helpers with plain
     `CREATE VIEW`. Carry over tests.
   - `git mv plans/relay/*` into `../pg-tide/plans/`; sweep
     `pgtrickle-relay` → `pg-tide` in those docs.
2. **Day 3 — Complete the relay lift**
   - Lift the relay catalog SQL from
     [src/lib.rs](../src/lib.rs#L1095-L1335) into `pg_tide`'s extension
     crate.
   - Move `pgtrickle-relay/` into `../pg-tide/relay/` using
     `git filter-repo --subdirectory-filter pgtrickle-relay` to preserve
     history.
3. **Day 4 — Cut from this repo**
   - *Delete* `enable_outbox()` / `create_inbox()` and the relay catalog
     SQL outright (no shims — see §7.7).
   - Add the thin `attach_outbox()` wrapper that delegates to
     `tide.outbox_publish()` via SPI (the only integration point — §7.2).
     No `attach_inbox()` or `materialize_inbox_views()` needed.
   - Write `sql/pg_trickle--0.42.0--1.0.0.sql` dropping all old objects.
   - Remove `pgtrickle-relay` from workspace `members` in
     [Cargo.toml](../Cargo.toml), delete [Dockerfile.relay](../Dockerfile.relay),
     remove the 4 relay CI jobs from
     [.github/workflows/release.yml](../.github/workflows/release.yml),
     drop the SQS security exception from
     [.github/workflows/security.yml](../.github/workflows/security.yml).
4. **Day 5a — Documentation**
   - Move `docs/OUTBOX.md`, `docs/INBOX.md`, `docs/RELAY.md`,
     `docs/RELAY_GUIDE.md` to `../pg-tide/docs/`; rewrite to lead with
     the generic pattern.
   - Trim ~12 docs in this repo (SQL_REFERENCE, ARCHITECTURE, WHATS_NEW,
     etc.) to a "see pg-tide" pointer.
   - CHANGELOG entry: call the rename a breaking change with a one-line
     equivalence table.
5. **Day 5b — CI / packaging**
   - Clone CI templates into `../pg-tide` (lint, test, coverage, benchmarks,
     release — see §7.10 for the full list).
   - Verify both repos build clean; cut pre-release tags on both.


### 7.10 Ancillary assets that need to move, be rewritten, or be deleted

The plan so far focuses on Rust source, SQL DDL, and design docs. There
is a long tail of *other* assets in this repo that also reference the
relay/outbox/inbox surface and need explicit handling. Reviewers should
not assume any of these "just work" after the cut.

#### User-facing documentation (move to `pg-tide` repo, rewrite to drop `pg_trickle` framing)

| Asset | Today's framing | Action |
|---|---|---|
| [docs/OUTBOX.md](docs/OUTBOX.md) | "pg_trickle outbox pattern" | Move to `pg-tide/docs/OUTBOX.md`; rewrite to lead with the generic outbox pattern. Add an "If you're using `pg_trickle`" appendix linking to `attach_outbox()`. |
| [docs/INBOX.md](docs/INBOX.md) | "pg_trickle inbox pattern" | Move to `pg-tide/docs/INBOX.md`; rewrite as above (no `pg_trickle` integration appendix needed — the inbox has no integration point per §7.2). |
| [docs/RELAY.md](docs/RELAY.md) | Architecture deep-dive | Move to `pg-tide/docs/RELAY.md`; rebrand `pgtrickle-relay` → `pg-tide`. |
| [docs/RELAY_GUIDE.md](docs/RELAY_GUIDE.md) | Operator guide | Move + rebrand. |
| Sections of [docs/SQL_REFERENCE.md](docs/SQL_REFERENCE.md), [docs/SQL_API_CATALOG.md](docs/SQL_API_CATALOG.md), [docs/CONFIGURATION.md](docs/CONFIGURATION.md), [docs/GUC_CATALOG.md](docs/GUC_CATALOG.md), [docs/SECURITY_GUIDE.md](docs/SECURITY_GUIDE.md), [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md), [docs/USE_CASES.md](docs/USE_CASES.md), [docs/PATTERNS.md](docs/PATTERNS.md) | Mention outbox/inbox/relay alongside other features | In `pg_trickle` repo: trim outbox/inbox/relay sections to a one-paragraph "see [pg-tide](https://…)" pointer. |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), [docs/GLOSSARY.md](docs/GLOSSARY.md) | Reference outbox/inbox tables in architectural diagrams | Trim or annotate; keep the conceptual mention but mark them as `pg_tide`-provided. |
| [docs/WHATS_NEW.md](docs/WHATS_NEW.md), [docs/UPGRADING.md](docs/UPGRADING.md) | History of v0.28/v0.29 features | Add a 1.0 entry: "outbox/inbox/relay extracted to `pg-tide`; see migration note." |

#### Roadmap entries (rewrite or relocate)

| Asset | Action |
|---|---|
| [roadmap/v0.28.0.md](roadmap/v0.28.0.md), [roadmap/v0.28.0.md-full.md](roadmap/v0.28.0.md-full.md) | Already-released history. Add a footnote: "Superseded in 1.0 by `pg_tide`." |
| [roadmap/v0.29.0.md](roadmap/v0.29.0.md), [roadmap/v0.29.0.md-full.md](roadmap/v0.29.0.md-full.md) | Same footnote. |
| [ROADMAP.md](ROADMAP.md) line ~130 ("v0.28–29 ─── Reliable event messaging…"), line 200 (test coverage), line 223 (embedding outbox) | Annotate each line: outbox/inbox/relay history now lives at `pg-tide`. The v0.47.0 "embedding outbox" item (line 100) needs a decision: stays in `pg_trickle` as a `pg_tide.attach_outbox()` consumer, or moves to `pg-tide`? Recommend keeping in `pg_trickle` since it's an embedding-pipeline feature that *uses* the outbox primitive. |

#### Blog posts

The published pages that describe removed outbox, inbox, and relay APIs stay at
their current URLs as migration notices. The notices point to the current
`pg_trickle` guides or the `pg_tide` repository.

Other blog posts mention outbox/inbox tangentially
([blog/self-monitoring.md](blog/self-monitoring.md),
[blog/cqrs-without-second-database.md](blog/cqrs-without-second-database.md))
— update those in place to phrase the integration as "use `pg_tide` for
the outbox plumbing."

#### Tests (relocate)

- [tests/e2e_outbox_tests.rs](tests/e2e_outbox_tests.rs) — move to `pg-tide/tests/`
- [tests/e2e_inbox_tests.rs](tests/e2e_inbox_tests.rs) — move to `pg-tide/tests/`
- Any other `tests/*.rs` that exercises `enable_outbox` / `create_inbox`
  needs to be rewritten as a *consumer* test calling
  `pg_trickle.attach_outbox()` — these become the regression coverage
  for the SPI bridge introduced in §7.3.
- `pgtrickle-relay/tests/` and `pgtrickle-relay/src/**/tests/` — move
  with the relay crate.

#### SQL upgrade scripts (the messy part)

The relevant upgrade files are:

- [sql/pg_trickle--0.27.0--0.28.0.sql](sql/pg_trickle--0.27.0--0.28.0.sql)
  (introduces outbox/inbox tables)
- [sql/pg_trickle--0.28.0--0.29.0.sql](sql/pg_trickle--0.28.0--0.29.0.sql)
  (introduces relay catalog)
- [sql/pg_trickle--0.38.0--0.39.0.sql](sql/pg_trickle--0.38.0--0.39.0.sql)
  and [sql/pg_trickle--0.39.0--0.40.0.sql](sql/pg_trickle--0.39.0--0.40.0.sql)
  (subsequent inbox/relay refinements)

These are *historical* upgrade scripts — they cannot be deleted, because
existing installs upgrading from 0.27 → 1.0 will still run them as part
of the chain. The 1.0 upgrade script (`pg_trickle--0.42.0--1.0.0.sql`)
is the one that *removes* the old objects:

```sql
-- pg_trickle 0.42.0 → 1.0.0
DROP TABLE IF EXISTS pgtrickle.relay_outbox_config CASCADE;
DROP TABLE IF EXISTS pgtrickle.relay_inbox_config CASCADE;
DROP TABLE IF EXISTS pgtrickle.relay_consumer_offsets CASCADE;
DROP FUNCTION IF EXISTS pgtrickle.set_relay_outbox(text, jsonb);
-- … etc. (all 7 helper functions + relay_config_notify trigger)
DROP ROLE IF EXISTS pgtrickle_relay;
DROP FUNCTION IF EXISTS pgtrickle.enable_outbox(text);
DROP FUNCTION IF EXISTS pgtrickle.disable_outbox(text);
DROP FUNCTION IF EXISTS pgtrickle.outbox_status(text);
DROP FUNCTION IF EXISTS pgtrickle.outbox_rows_consumed(text, bigint);
DROP FUNCTION IF EXISTS pgtrickle.create_inbox(text, integer, boolean, boolean, integer);
-- … etc.
-- Outbox/inbox base tables (pgtrickle.outbox_<st>, pgtrickle.inbox_<…>) are
-- left as-is — they hold user data. Document that users must DROP them
-- manually after re-creating in the tide schema if migrating data.
```

This is invasive — `DROP TABLE … CASCADE` on the outbox base tables
would lose data, so the 1.0 upgrade script *does not* drop those. The
CHANGELOG must explicitly tell anyone who has put data into the relay
catalog that 1.0 will erase their pipeline configuration. Per §7.7,
this is acceptable because there are no known users.

#### Build / CI / packaging artefacts to delete from this repo

- [Dockerfile.relay](Dockerfile.relay) — delete
- Lines 474–580 of [.github/workflows/release.yml](.github/workflows/release.yml)
  — delete the four relay publish jobs and the `RELAY_IMAGE_NAME` env var
- Lines 52–82 of [.github/workflows/security.yml](.github/workflows/security.yml)
  — drop the `aws-smithy-http-client` exception (only existed for the
  relay's optional SQS feature)
- [tests/Dockerfile.e2e](tests/Dockerfile.e2e) lines 36–48 — drop relay
  placeholdert reads a Postgres table *(once `WireFormat` phase 2 ships — see §7.10)* |
| Postgres-first stacks (Supabase, Crunchy, Neon, plain self-hosted) wanting "publish-to-Kafka" | Roll their own | Install one extension |
| Anyone needing the inbox pattern (idempotent receive + DLQ) | Hand-roll | Built-in |
| `pg_trickle` users who also want to publish change events | Today: `enable_outbox()` | Same UX via `attach_outbox()`, plus the option to use the same outbox infrastructure for non-stream-table events |

- [Cargo.toml](Cargo.toml): drop `pgtrickle-relay` from the workspace
  `members` list
- [deny.toml](deny.toml) lines 44–45 — drop the SQS-feature license
  exception (now lives in `pg-tide`)
- [README.md](README.md) lines 526, 556–558 — trim outbox/inbox/relay
  rows from the docs index, replace with a single "Event messaging:
  see [pg-tide](https://…)" line
- [META.json](META.json) — review and drop relay-related description
  text if any (the outbox-poller backend specifically)

#### Demo / playground / monitoring (update in place)

- [demo/](demo/) — currently shows `pg_trickle` end-to-end including
  outbox publishing. Update `docker-compose.yml` to add a `pg-tide`
  service and pull the relay binary from its new image. The demo's
  point is "see them work together," so keeping it cross-extension is
  the right move.
- [monitoring/](monitoring/) — update Prometheus scrape config to add
  the `pg-tide` relay endpoint alongside `pg_trickle`'s; carry over the
  Grafana panels that visualise outbox lag.
- [examples/](examples/) — none of the visible examples
  (`dbt_getting_started`, `non-differential.sql`) reference the outbox,
  so probably nothing to do here. Sanity-check during the cut.
- [cnpg/](cnpg/) — the CloudNativePG manifests don't reference the
  relay today (verified — empty grep). They will need a separate
  `pg-tide` manifest set in the new repo, but the existing manifests
  in this repo stay clean.

#### What this means for the §7.9 work plan

Day 5 ("docs pass") is doing a lot of heavy lifting under the surface.
A more honest decomposition:

- **Day 4** stays focused on code: delete moved code, add `attach_outbox()`,
  write the 1.0 upgrade script.
- **Day 5** splits into two:
  - **Day 5a** — Move the four blog posts and four user-facing docs
    files (OUTBOX.md, INBOX.md, RELAY.md, RELAY_GUIDE.md). Update
    cross-references in the SQL_REFERENCE / ARCHITECTURE / WHATS_NEW
    cluster (~12 docs total). Roadmap footnotes.
  - **Day 5b** — CI / packaging cleanup (Dockerfiles, workflows,
    deny.toml, Cargo.toml, README index). Verify both repos build
    clean. Cut pre-release tags.

This pushes the realistic estimate to **~6 working days** rather than 5,
but it's still well under the original two-week estimate.

#### Shared infrastructure to clone (not move) into the new repo

Most of `pg_trickle`'s repo-level scaffolding has nothing to do with
the relay but is well-honed and worth re-using. The new `pg-tide` repo
should *copy* (not move) the patterns below; they stay in `pg_trickle`
unchanged.

| Asset | Location | Why clone it |
|---|---|---|
| **Reusable composite action** | [.github/actions/setup-pgrx](.github/actions/setup-pgrx) | `pg-tide` has a pgrx extension crate too; same setup story (cargo-pgrx install, pg18 toolchain, sccache). |
| **Skill files for AI/agent workflows** | [.github/skills/create-pull-request/](.github/skills/create-pull-request/), [enrich-release-roadmap/](.github/skills/enrich-release-roadmap/), [implement-roadmap-version/](.github/skills/implement-roadmap-version/) | All three are repo-agnostic. The PR-creation skill (Unicode-safe `gh --body-file` workflow) is especially valuable. |
| **Agent conventions** | [AGENTS.md](AGENTS.md) | Copy and trim: keep the workflow rules, error-handling rules, SPI conventions, code-review checklist; drop the pg_trickle-specific module-layout section and replace with `pg-tide`'s. |
| **CI architecture** | [.github/workflows/](.github/workflows/) | Clone `lint.yml`, `ci.yml`, `coverage.yml`, `codeql.yml`, `secret-scan.yml`, `dependency-policy.yml`, `semgrep.yml`, `docs-drift.yml`, `unsafe-inventory.yml`. The light-e2e vs full-e2e split (stock postgres bind-mount vs custom Docker image) is the right pattern for `pg-tide` too. |
| **Benchmark regression-gate workflow** | [scripts/criterion_regression_check.py](scripts/criterion_regression_check.py) + the `benchmarks.yml` workflow | The "save baseline on push to main, compare on PR" pattern is generic. Worth carrying over. |
| **Just recipes** | [justfile](justfile) | Most recipes are about pgrx + Postgres setup, format/lint, test tiers — directly reusable. Drop pg_trickle-specific ones (`build-e2e-image`, `test-tpch`, etc.) and add `pg-tide` equivalents. |
| **Config files** | [deny.toml](deny.toml), [codecov.yml](codecov.yml), [book.toml](book.toml), [.github/dependabot.yml](.github/dependabot.yml), [.github/CODEOWNERS](.github/CODEOWNERS), [.github/pull_request_template.md](.github/pull_request_template.md), [.github/ISSUE_TEMPLATE/](.github/ISSUE_TEMPLATE/) | All trivially adaptable. None mention relay/outbox/inbox today (verified via grep). |
| **Convention scripts** | [scripts/check_version_sync.sh](scripts/check_version_sync.sh), [scripts/check_upgrade_completeness.sh](scripts/check_upgrade_completeness.sh) | Pattern for keeping `Cargo.toml` / `*.control` / `META.json` versions in lockstep is reusable; the upgrade-completeness check works for any pgrx extension that ships per-version SQL migrations. |
| **Release scaffolding** | [.github/workflows/release.yml](.github/workflows/release.yml) (post-deletion of relay jobs), [.github/workflows/pgxn.yml](.github/workflows/pgxn.yml), [.github/workflows/ghcr.yml](.github/workflows/ghcr.yml), [.github/workflows/docker-hub.yml](.github/workflows/docker-hub.yml) | Adapt for `pg-tide`'s release flow. The relay-specific bits already get deleted from this repo per §7.10; the *non*-relay pieces are the scaffolding the new repo wants to clone. |
| **Stability-test workflow** | [.github/workflows/stability-tests.yml](.github/workflows/stability-tests.yml) | Soak-test pattern is exactly what an outbox/inbox primitive needs (long-running write loops, claim-check pressure, retention drainer correctness). High-value clone. |
| **Fuzz-smoke workflow** | [.github/workflows/fuzz-smoke.yml](.github/workflows/fuzz-smoke.yml) + [fuzz/](fuzz/) | The harness pattern is reusable; targets would change (`pg_tide`'s natural fuzz targets are envelope parsing and config validation). |

**Things explicitly *not* worth cloning:**

- `tpch-nightly.yml` / `tpch-explain-artifacts.yml` / `e2e-benchmarks.yml`
  — TPC-H and the IVM-specific benchmark matrix are pg_trickle concerns.
- `sqlancer.yml` — SQLancer targets a query engine; `pg_tide` has no
  query rewriter to fuzz.
- `cnpg/` smoke test — `pg-tide` will need its own when CNPG packaging
  exists, but the existing one is too pg_trickle-specific to be worth
  forking now.

**Implication:** Day 5b's "CI / packaging cleanup" expands a little.
The right framing is "in `pg_trickle`: delete the relay-specific
workflow jobs and config exceptions; in `pg-tide`: bootstrap a
parallel CI matrix from cloned templates." The cloning is mostly
mechanical (the workflows reference Cargo crate names, image tags, and
test paths) but worth budgeting half a day to get right.

### 7.11 In-flight relay roadmap that travels with the cut

The relay is **not** feature-complete today. There is a non-trivial
roadmap of planned work that lives in [plans/relay/](plans/relay/) and
must move to the new `pg-tide` repo as part of the extraction — not be
left orphaned in `pg_trickle`. Reviewers should treat the inheritance of
this roadmap as part of the scope decision, not an afterthought.

| Doc | Scope | Status | Inheritance plan |
|---|---|---|---|
| [PLAN_RELAY_WIRE_FORMATS.md](plans/relay/PLAN_RELAY_WIRE_FORMATS.md) | Pluggable `WireFormat` trait + Debezium (both directions), Avro/Confluent SR, Maxwell, Canal, custom CDC JSON. Phased over relay 0.30→0.34. | In-flight design | `git mv plans/relay/PLAN_RELAY_WIRE_FORMATS.md` → `pg-tide/plans/`. Renumber relay versions to `pg-tide` versioning. The Debezium-encoder claim in §7.4 of *this* plan ("replace Kafka Connect with a single Rust binary") only becomes credible once Phase 2 of this doc ships. |
| [PLAN_RELAY_CLI.md](plans/relay/PLAN_RELAY_CLI.md) | Phase 1 CLI: `pg-tide pipelines`, `pg-tide tail`, `pg-tide doctor`, etc. | In-flight design | Move verbatim. Rename binary references `pgtrickle-relay` → `pg-tide` (the binary already needs renaming for §7.2 anyway). |
| [PLAN_RELAY_CLI_PHASE_2.md](plans/relay/PLAN_RELAY_CLI_PHASE_2.md) | Phase 2 CLI: schema registry, transforms, dead-letter inspection, replay UX. | In-flight design | Move verbatim. The Schema Registry parts overlap with WIRE_FORMATS phase 3 — flag for de-duplication during the move. |
| [PLAN_RELAY_GAPS_FROM_FELDERA_RISINGWAVE.md](plans/relay/PLAN_RELAY_GAPS_FROM_FELDERA_RISINGWAVE.md) | Competitive analysis: features the relay lacks vs Feldera and RisingWave. | Reference / backlog | Move verbatim. This doc is most of the long-term roadmap argument *for* `pg_tide`'s independent value — closing these gaps is exactly what makes the standalone extension worth installing. |

**Implication for the §7.9 work plan:** Day 1–2 of the extraction must
include `git mv plans/relay/* pg-tide/plans/` and a sweep replacing
`pgtrickle-relay` with `pg-tide` / `pg_tide` in those docs. Day 5 docs
work needs to add a top-level `pg-tide/plans/README.md` that orients
contributors at the inherited roadmap rather than the now-stale design
docs in this repo's `plans/` tree.

**Implication for the §7.5 comparison table:** the "Effort: ~1 week"
row is the *extraction* effort. Delivering the inherited roadmap (wire
formats, CLI phase 2, closing the Feldera/RisingWave gaps) is months of
work on top — but that work was always going to happen *somewhere*; the
question is whether it lands in `pg_trickle`'s repo or in the new
standalone one. Option C-revised is the only option that puts it in the
right place.

**Implication for §7.4 ("realistic for non-`pg_trickle` users"):** the
"replace Debezium" framing depends on
[PLAN_RELAY_WIRE_FORMATS.md](plans/relay/PLAN_RELAY_WIRE_FORMATS.md)
phase 2 actually shipping. Until it does, `pg_tide`'s outbox source
speaks the native envelope only and the Debezium story is aspirational.
This is fine — the *outbox/inbox primitive* alone has independent value
(see the other rows in the §7.4 audience table) — but the Debezium
audience comes online with relay 0.31+ rather than at the moment of
extraction.

### 7.12 Open questions specific to option C

1. **License.** `pg_trickle` is Apache-2.0; `pg_tide` should match so
   the integration story is frictionless.
2. **Repo home.** Same `grove/` org, or somewhere more neutral if the
   intent is to attract contributors who don't think of it as a
   `pg_trickle` accessory?
3. **Schema layout.** Single `tide` schema (with `outbox_*` / `inbox_*` /
   `relay_*` function prefixes), or three sibling schemas (`tide_outbox`,
   `tide_inbox`, `tide_relay`)? The plan currently assumes the single
   schema for ergonomics, but the three-schema layout would let users
   `SET search_path = tide_outbox, public` and drop the prefix.
4. **Naming the lineage.** Should the README explicitly call out the
   `pg_trickle` / `pg_ripple` / `pg_tide` family? Helps with
   recognisability if multiple extensions are deployed together; risks
   tying `pg_tide`'s identity to `pg_trickle` more tightly than option C
   intends.
5. **User check before the cut.** Before merging the breaking change,
   send a one-line announcement to the `pg_trickle` discussions / issue
   tracker asking "is anyone using `enable_outbox()` /
   `enable_inbox()` / the relay binary today?" If the answer is
   non-empty, revision 4's no-shims simplification has to be reverted in
   favour of the original migration plan from revision 3.

*Dropped from this list (revision 4):* the backwards-compatibility window
question. With no known users, there is nothing to be compatible with.
