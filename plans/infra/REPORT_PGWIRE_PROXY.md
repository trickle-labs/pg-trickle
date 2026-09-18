# REPORT_PGWIRE_PROXY.md — pgwire Proxy / Intercept Analysis

> **Status:** Research / Analysis  
> **Relates to:** [REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md) (§11, Open Question #4)  
> **Author:** pg_trickle project

---

## Table of Contents

- [1. Context and Question](#1-context-and-question)
- [2. What Is a pgwire Proxy?](#2-what-is-a-pgwire-proxy)
- [3. Prior Art and Competitive Landscape](#3-prior-art-and-competitive-landscape)
- [4. What Could a Proxy Enable for pg_trickle?](#4-what-could-a-proxy-enable-for-pg_trickle)
- [5. Architecture Options](#5-architecture-options)
- [6. Detailed Capability Analysis](#6-detailed-capability-analysis)
- [7. Implementation Cost and Complexity](#7-implementation-cost-and-complexity)
- [8. Risk Assessment](#8-risk-assessment)
- [9. Comparison: Proxy vs. Direct Sidecar](#9-comparison-proxy-vs-direct-sidecar)
- [10. Recommendation](#10-recommendation)
- [11. If We Build It — Phased Approach](#11-if-we-build-it--phased-approach)
- [12. Open Questions](#12-open-questions)

---

## 1. Context and Question

The [External Sidecar Process report](REPORT_EXTERNAL_PROCESS.md) outlines
pg_trickle running as a standalone process that connects to PostgreSQL over
standard pgwire client connections. Open Question #4 asks:

> Should we support pgwire as a **proxy**? The sidecar could intercept SQL
> traffic and transparently add CDC triggers — no user action needed. This
> is how Epsio works. Adds significant complexity.

This document researches the question in depth: **would intercepting or
proxying the PostgreSQL wire protocol benefit pg_trickle, and if so, how?**

**Clarification:** Epsio does _not_ actually use a pgwire proxy. It uses a
direct sidecar with logical replication CDC and a "Commander" that receives
instructions via `pg_logical_emit_message`. The product that _does_ use a
transparent pgwire proxy for IVM is **ReadySet**. This distinction matters
for the analysis.

---

## 2. What Is a pgwire Proxy?

A pgwire proxy sits between the application and PostgreSQL, speaking the
PostgreSQL wire protocol on both sides:

```
┌──────────────────────────────────────────────────────────────────┐
│                          Application                             │
│  (uses standard Postgres driver: libpq, JDBC, asyncpg, etc.)    │
└──────────────────────┬───────────────────────────────────────────┘
                       │ pgwire (port 6432)
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                        pgwire Proxy                              │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐    │
│  │ Frontend     │  │ SQL Parser  │  │ Decision Logic       │    │
│  │ (server side)│  │ (optional)  │  │ (route/intercept/    │    │
│  │              │  │             │  │  modify/passthrough) │    │
│  └──────┬───────┘  └──────┬──────┘  └──────────┬───────────┘    │
│         │                 │                     │               │
│  ┌──────▼─────────────────▼─────────────────────▼───────────┐   │
│  │ Backend (client side) — upstream PG connection pool       │   │
│  └──────────────────────────┬────────────────────────────────┘   │
└─────────────────────────────┼────────────────────────────────────┘
                              │ pgwire (port 5432)
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                                │
└──────────────────────────────────────────────────────────────────┘
```

The proxy can operate at different levels of SQL awareness:

| Level | What the proxy understands | Complexity |
|-------|---------------------------|------------|
| **L0: Byte-level relay** | Nothing — just forwards TCP bytes. Like HAProxy. | Trivial |
| **L1: Message-level** | pgwire message framing (Query, Parse, Bind, Execute, etc.). Can inspect message types. | Low |
| **L2: Simple query parsing** | Parses SQL text from `Query` messages. Can classify SELECT/INSERT/UPDATE/DELETE. | Medium |
| **L3: Full SQL parsing** | Full AST parsing of every query. Can rewrite queries, detect DDL, extract table references. | High |
| **L4: Semantic understanding** | Understands schema, query plans, data types. Can make routing decisions based on query semantics. | Very high |

Existing products operate at different levels:

- **PgBouncer:** L1 (message-level pooling)
- **PgCat:** L2-L3 (query routing with `sqlparser` crate)
- **Supavisor:** L1 (message-level, Elixir-based)
- **ReadySet:** L4 (full semantic understanding, dataflow graph construction)

---

## 3. Prior Art and Competitive Landscape

### 3.1 ReadySet — The Proxy IVM Model

ReadySet is the most relevant comparison. It is a **transparent pgwire proxy**
that:

1. Sits between the app and Postgres (wire-compatible with both MySQL and PG).
2. Intercepts `SELECT` queries and categorizes them as cacheable or not.
3. For cacheable queries, builds an internal **streaming dataflow graph**
   (similar to differential dataflow / Noria) that incrementally maintains
   query results.
4. Consumes the PostgreSQL **logical replication stream** to receive changes.
5. Returns cached results for cache-hit queries; proxies everything else to
   upstream PG.
6. Users control caching via `CREATE CACHE FROM <query>` custom commands.

**Key architectural choices:**
- ReadySet **does not write results back to PG** — it maintains an in-memory
  materialization. This means the cached data lives only in the ReadySet
  process.
- ReadySet is eventually consistent — there's a small delay between writes
  and cache updates.
- ReadySet's value proposition is **read performance** (precomputed results,
  zero SQL execution cost on reads), not in-database materialization.
- License: BSL 1.1 (converts to Apache 2.0 after 4 years).
- Written in Rust, ~250K+ LoC.

**What pg_trickle could learn from ReadySet:**
- The proxy intercept model makes adoption **effortless** — just change the
  connection string.
- Custom SQL commands (`CREATE CACHE`, `SHOW CACHES`) work because the proxy
  can intercept them before they reach PG.
- The logical replication CDC approach avoids trigger overhead on write path.

**What pg_trickle should NOT copy from ReadySet:**
- ReadySet's in-memory-only materialization is a **different product category**.
  pg_trickle writes results back to PG tables, making them queryable by any
  tool, persistent across restarts, and indexable. This is fundamentally more
  useful for many use cases.
- ReadySet is a ~250K LoC project with 35+ contributors. The proxy layer
  alone is enormous. pg_trickle should not attempt to replicate this scope.

### 3.2 Epsio — The Non-Proxy Sidecar Model

Epsio is NOT a proxy. It is a sidecar that:

1. Creates a **logical replication slot** to consume WAL changes.
2. Runs a **CDC Forwarder** (Rust, native PG replication protocol) that
   streams changes to an internal execution engine.
3. Receives commands via `pg_logical_emit_message()` — the user calls
   `CALL epsio.create_view(...)` which publishes a message that the
   sidecar's "Commander" picks up.
4. Writes results back to standard PG tables in the user's database.
5. The execution engine maintains internal state in RocksDB for stateful
   operators (JOINs, aggregates).

**Key insight:** Epsio achieves zero-install deployment **without** a proxy.
The user SQL functions (`epsio.create_view`, `epsio.list_views`) are
installed as a thin PL/pgSQL layer that communicates with the sidecar via
`pg_logical_emit_message()` and a response table.

### 3.3 PgCat — Connection Pooler with Query Routing

PgCat (Rust, MIT, 3.9K stars) is a PgBouncer replacement that:

- Supports transaction and session pooling.
- Parses queries (via `sqlparser` crate) for read/write routing.
- Routes queries to shards based on comments, SET commands, or automatic
  parsing.
- Supports mirroring, failover, and load balancing.

**Relevant lesson:** PgCat demonstrates that a Rust pgwire proxy with SQL
parsing is feasible and performant. Their codebase is ~12K LoC.

### 3.4 pgwire Crate — The Building Block

The `pgwire` Rust crate (v0.38, 730 stars, MIT/Apache-2.0) provides:

- **Server (frontend):** Accept connections from PG clients. Handle startup,
  auth (cleartext, MD5, SCRAM-SHA-256), simple query, extended query, COPY,
  cancel, notifications.
- **Client (backend):** Connect to upstream PG. Same protocol coverage.
  Designed specifically for building proxy components.
- **Protocol v3.0 and v3.2** (Postgres 18) support.
- **Logical replication** server and client APIs.

Used by: GreptimeDB, PeerDB, Dozer, SpacetimeDB, risinglight (452 dependents).

The crate is mature enough for production use and provides both sides of the
proxy equation (accept client connections, forward to upstream PG).

### 3.5 Landscape Summary

| Product | Architecture | Proxy? | CDC Method | Result Storage |
|---------|-------------|--------|-----------|---------------|
| **ReadySet** | Transparent proxy + dataflow engine | **Yes** | Logical replication | In-memory only |
| **Epsio** | Sidecar (no proxy) | **No** | Logical replication | Writeback to PG tables |
| **pg_ivm** | Extension (C) | N/A | Triggers | PG tables |
| **PgCat** | Connection pooler/proxy | **Yes** (routing only) | N/A | N/A |
| **Supavisor** | Connection pooler/proxy | **Yes** (routing only) | N/A | N/A |
| **pg_trickle (current)** | Extension (Rust) | N/A | Triggers | PG tables |
| **pg_trickle (proposed sidecar)** | Sidecar | **No** | Triggers or WAL | PG tables |

---

## 4. What Could a Proxy Enable for pg_trickle?

There are several distinct capabilities a pgwire proxy could provide. Each
has different value, complexity, and overlap with the existing plan:

### 4.1 Transparent DDL Interception

**Capability:** Intercept `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE` and
automatically install/update/remove CDC triggers without any explicit user
action.

**Current approach (sidecar without proxy):** The sidecar polls
`pg_catalog` for schema changes, or uses event triggers where available
(the user must call management functions explicitly).

**Value:** Medium-high. Reduces friction — users don't need to learn
pg_trickle management APIs. Source tables get CDC triggers automatically
when they're referenced by a stream table definition.

**Complexity:** Medium. Requires L2-L3 parsing to detect DDL statements.
Must forward DDL to PG first, wait for success, then install triggers.
Must handle `IF NOT EXISTS`, rollbacks, and transactional DDL.

### 4.2 Custom Command Interception

**Capability:** Support custom SQL syntax like:

```sql
CREATE STREAM TABLE revenue_by_region AS
  SELECT region, SUM(amount) FROM orders GROUP BY region;
```

The proxy intercepts this before it reaches PG, processes it as a
pg_trickle management operation, and returns a success response.

**Current approach (sidecar without proxy):**
`SELECT pgtrickle.create_stream_table(...)` function calls, which the
sidecar installs as PL/pgSQL stubs that write to command/response tables
(Epsio-style Commander pattern).

**Value:** High for developer experience. The custom DDL syntax feels
native and matches the extension's `ProcessUtility_hook`-based syntax
plan (see the rejected native-syntax proposal).

**Complexity:** Medium-high. Must parse the custom syntax, distinguish it
from regular PG DDL, and synthesize appropriate pgwire response messages
(CommandComplete, error handling, etc.). The proxy becomes a partial SQL
parser.

### 4.3 Transparent Query Routing (ReadySet-style)

**Capability:** Automatically detect `SELECT` queries that match stream
table definitions, and route them to the materialized storage tables
instead of executing the defining query.

**Current approach:** Stream tables ARE storage tables — the user queries
them directly. No routing needed.

**Value:** Low. pg_trickle already writes results to queryable PG tables.
There's no benefit to having the proxy redirect queries because the data
is already in PG. A user can simply `SELECT * FROM st_revenue_by_region`.

**Complexity:** Very high (L4: semantic understanding required). This would
essentially be rebuilding ReadySet's query matching engine. Not worth it.

### 4.4 Write Path Monitoring (Automatic CDC)

**Capability:** Observe `INSERT/UPDATE/DELETE` statements flowing through
the proxy. Use this to replace or augment trigger-based CDC.

**Current approach:** Row-level AFTER triggers write to change buffer
tables, or WAL-based CDC via logical replication.

**Value:** Low-Medium. The proxy sees SQL statements, not row-level
changes. It would need to:
1. Parse the DML statement to identify affected tables.
2. Either install triggers on those tables (redundant with current approach)
   or somehow extract the changed rows from the statement.

Extracting row-level changes from SQL statements is fundamentally less
reliable than triggers or WAL. Consider: `UPDATE orders SET status = 'shipped'
WHERE created_at < '2025-01-01'` — the proxy can't know which rows were
affected without executing the query first.

**Complexity:** Very high for marginal benefit. Triggers and WAL are
strictly superior CDC mechanisms.

### 4.5 Connection Pooling / Multiplexing

**Capability:** Pool upstream connections, similar to PgBouncer/PgCat.

**Value:** Low for pg_trickle's core mission. Connection pooling is a
solved problem. Users already have PgBouncer, PgCat, Supavisor, or RDS
Proxy in their stack. Adding another pooler creates confusion.

**Complexity:** High. Correct transaction-mode pooling is notoriously
hard (prepared statements, SET, advisory locks, notifications).

### 4.6 Observability and Query Profiling

**Capability:** Monitor all SQL traffic flowing through the proxy. Build
a dashboard showing query frequencies, latencies, and which queries could
benefit from stream table materialization.

**Value:** Medium. Profiling is useful for initial onboarding ("which of
my queries should be stream tables?") but is a one-time rather than
ongoing need. Tools like `pg_stat_statements` already provide this.

**Complexity:** Low-Medium (L1-L2 parsing). Most of this is just timing
query round-trips and categorizing statement types.

### 4.7 NOTIFY-Free Signaling

**Capability:** Since the proxy sees all DML, it can signal the sidecar's
scheduler to refresh relevant stream tables without relying on
`LISTEN/NOTIFY` or polling.

**Value:** Low-Medium. Eliminates one integration point but adds proxy
complexity. `LISTEN/NOTIFY` is simple and reliable.

**Complexity:** Low if the proxy is already built. Free side-benefit.

---

## 5. Architecture Options

There are three main architecture options, ranging from no proxy to full proxy:

### Option A: No Proxy (Current Sidecar Plan)

```
App ──pgwire──▶ PostgreSQL ◀──pgwire── pg_trickle sidecar
```

- The sidecar connects directly to PG as a client.
- Management via SQL functions (PL/pgSQL stubs), HTTP API, or CLI.
- CDC via triggers or logical replication.
- No interception of user SQL traffic.

**Pros:**
- Simplest architecture. No additional hop in the data path.
- Zero latency impact on application queries.
- No single point of failure added.
- Compatible with existing connection poolers.
- Matches Epsio's proven architecture.

**Cons:**
- User must explicitly manage stream tables (API calls).
- DDL changes on source tables need polling or event triggers to detect.
- No custom SQL syntax in sidecar mode.

### Option B: Optional Proxy Sidecar ("Lite Proxy")

```
App ──pgwire──▶ pg_trickle proxy (port 6432) ──pgwire──▶ PostgreSQL
                       │
                       └── internally signals sidecar scheduler
```

The proxy is **optional** — users can also connect directly to PG and
manage stream tables via API/CLI. The proxy adds:
- Custom DDL interception (`CREATE STREAM TABLE ...`)
- Transparent DDL monitoring (detect schema changes on source tables)
- Query profiling / observability

**Pros:**
- Zero-friction onboarding: change connection string, use familiar SQL.
- Custom syntax support without extension or `ProcessUtility_hook`.
- Schema change detection without polling.
- Optional — users who don't want the proxy skip it.

**Cons:**
- Added latency on every query (even passthrough).
- New single point of failure in the data path.
- Must handle auth, SSL/TLS, extended query protocol, COPY, etc.
- Increased operational complexity (another port to manage/monitor).
- Incompatible with some connection pooler topologies.
- Partial SQL parsing is a maintenance burden.

### Option C: Full Transparent Proxy ("ReadySet-style")

```
App ──pgwire──▶ pg_trickle full proxy ──pgwire──▶ PostgreSQL
                       │
                       ├── query matching + routing
                       ├── CDC via logical replication
                       ├── in-memory or PG-stored materialization
                       └── connection pooling
```

**Pros:**
- Maximum transparency — users change nothing except connection string.
- Could intercept and cache read queries automatically.
- Full control over the SQL traffic path.

**Cons:**
- Enormous complexity (ReadySet is ~250K LoC).
- Must be bug-for-bug compatible with PG wire protocol.
- Becomes a hard dependency in the application's data path.
- Query routing logic is unnecessary for pg_trickle (results are already
  in PG tables).
- Connection pooling duplicates existing infrastructure.
- Years of engineering investment.

---

## 6. Detailed Capability Analysis

### 6.1 Latency Impact Assessment

Every query passing through the proxy incurs additional latency:

| Operation | Overhead |
|-----------|----------|
| TCP accept + TLS handshake | ~1-5ms (amortized by connection pooling) |
| pgwire message parsing | ~0.01-0.1ms per message |
| SQL text parsing (if L2+) | ~0.1-1ms per query |
| Upstream connection acquisition | ~0.01ms (pooled) |
| Message relay (both directions) | ~0.05-0.2ms |
| **Total per-query overhead** | **~0.2-1.5ms** |

For most OLTP workloads with 5-50ms query latencies, this adds 1-30%
overhead. For sub-millisecond lookups (e.g., key-value access patterns),
the 0.2-1.5ms overhead is **significant** and potentially unacceptable.

### 6.2 Protocol Compatibility Challenges

A pgwire proxy must handle:

| Protocol Feature | Difficulty | Notes |
|-----------------|-----------|-------|
| Simple query protocol | Easy | Single `Query` message → forward |
| Extended query protocol | Hard | `Parse` → `Bind` → `Describe` → `Execute` → `Sync` flow; must handle pipelining |
| Prepared statements | Hard | Client may reference server-side prepared statements across transactions |
| COPY IN/OUT | Medium | Streaming data; must relay without corruption |
| LISTEN/NOTIFY | Medium | Async notifications from server; must relay to correct client |
| SSL/TLS | Medium | Must terminate client-side TLS and optionally initiate upstream TLS |
| SCRAM-SHA-256 auth | Hard | Multi-round-trip auth; proxy must either pass through or re-authenticate |
| Cancel requests | Hard | Separate TCP connection with backend PID/secret key |
| Streaming replication | Very hard | If proxy must also consume WAL stream |
| Transaction state tracking | Medium | Must track `idle`, `in transaction`, `failed transaction` |
| Multiple result sets | Medium | Some drivers expect specific result set shapes |
| Error message relay | Easy | Forward `ErrorResponse` messages as-is |

The `pgwire` crate (v0.38) handles most of these, but integrating them
into a correct proxy is substantial engineering work. PgCat (12K LoC) is
a useful reference for what "correct proxy" looks like.

### 6.3 Custom DDL — Deep Dive

The most compelling proxy capability for pg_trickle is custom DDL syntax.
Here's how it would work:

```
Client sends: Q("CREATE STREAM TABLE st_revenue AS SELECT ...")
  │
  ▼
Proxy parses SQL text
  ├── Matches "CREATE STREAM TABLE" pattern?
  │     └── Yes → extract name, defining query, options
  │           ├── Parse the defining query with pg_query.rs
  │           ├── Call sidecar management API internally
  │           ├── Wait for stream table creation to complete
  │           └── Return CommandComplete("CREATE STREAM TABLE")
  └── No match → forward to PostgreSQL as-is
```

**Challenges:**
1. The proxy only sees `Query("CREATE STREAM TABLE st_revenue AS ...")` as
   a raw string. It must distinguish custom syntax from valid PG SQL. This
   requires either regex matching (fragile) or full SQL parsing.
2. Extended query protocol: `Parse("CREATE STREAM TABLE ...")` must also be
   intercepted. This is harder because the SQL text arrives in a `Parse`
   message and may have parameter placeholders.
3. Transaction handling: If the custom DDL is inside a `BEGIN`/`COMMIT`
   block, the proxy must handle it correctly — probably by erroring (stream
   table DDL is not transactional) or by implicitly committing first.
4. Error handling: If the sidecar fails to create the stream table, the
   proxy must synthesize a proper `ErrorResponse` message.
5. `psql` tab completion will not know about `CREATE STREAM TABLE` — the
   proxy can't fix this.

**Mitigation:** For pg_trickle specifically, a simpler approach exists.
Instead of full custom DDL, the proxy could intercept
`SELECT pgtrickle.create_stream_table(...)` function calls. This is
standard PG syntax — no custom DDL parsing needed. The proxy recognizes
the function call, routes it to the sidecar, and returns the result. But
this provides no UX benefit over the direct sidecar approach.

---

## 7. Implementation Cost and Complexity

### 7.1 Effort Estimates

| Component | Effort | Description |
|-----------|--------|-------------|
| **pgwire server (frontend)** | 2-3 weeks | Accept client connections, TLS, auth passthrough |
| **pgwire client (backend)** | 1-2 weeks | Upstream PG connection pool, health checks |
| **Message relay (L1)** | 1-2 weeks | Forward messages between client and server, handle all protocol states |
| **SQL parsing (L2-L3)** | 2-3 weeks | Integrate `pg_query.rs` for statement classification, DDL detection |
| **Custom DDL interception** | 2-3 weeks | Pattern matching, sidecar API integration, response synthesis |
| **Extended query protocol** | 2-4 weeks | `Parse`/`Bind`/`Execute` tracking, prepared statement management |
| **Connection pooling** | 2-3 weeks | Transaction/session mode pooling (or integrate existing pooler) |
| **TLS / Auth** | 1-2 weeks | SCRAM-SHA-256 passthrough, client cert support |
| **Observability** | 1-2 weeks | Prometheus metrics, query profiling, latency histograms |
| **Testing** | 3-5 weeks | Protocol compliance, regression, load testing, driver compat |
| **Total (Lite Proxy — Option B)** | **16-29 weeks** | 4-7 months of focused work |

For comparison, the entire sidecar (Option A, no proxy) is estimated at
15-22 weeks in [REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md). Adding
a proxy roughly **doubles** the sidecar development timeline.

### 7.2 Ongoing Maintenance

| Concern | Estimate |
|---------|----------|
| **PG version compat** | Each PG release may change wire protocol behavior. Must test with every major version. |
| **Driver compat** | Different client drivers (libpq, JDBC, psycopg, node-pg, asyncpg, pgx-go) exercise different protocol paths. Ongoing regression testing needed. |
| **Security patches** | Proxy is on the network path — any vulnerability is high-severity. |
| **Performance regression** | Continuous benchmarking to prevent proxy from becoming a bottleneck. |

---

## 8. Risk Assessment

| Risk | Severity | Likelihood | Mitigation |
|------|----------|-----------|-----------|
| **Added latency degrades OLTP performance** | High | Medium | Make proxy optional; provide bypass mode for latency-sensitive connections |
| **Protocol incompatibility with specific drivers** | High | Medium | Extensive driver test matrix; PgCat has solved this for most drivers, reference their work |
| **Proxy becomes single point of failure** | High | Low | HA deployment (multiple proxy instances + load balancer) |
| **Operational complexity deters adoption** | Medium | High | Many users already have PgBouncer/PgCat — adding another proxy is confusing |
| **Custom DDL parsing is fragile** | Medium | Medium | Use function-call interception instead of true custom syntax |
| **Proxy conflicts with existing connection pooler** | Medium | High | Users must choose: pooler → proxy → PG, or proxy → PG. Double-proxy is problematic. |
| **Maintenance burden exceeds team capacity** | High | Medium | Proxy is a full product in itself; consider leveraging PgCat as a base instead of building from scratch |
| **Scope creep** (users expect the proxy to do pooling, HA, etc.) | Medium | High | Clear documentation that the proxy is pg_trickle-specific, not a general-purpose pooler |

---

## 9. Comparison: Proxy vs. Direct Sidecar

| Dimension | Direct Sidecar (Option A) | Lite Proxy (Option B) |
|-----------|--------------------------|----------------------|
| **User onboarding** | Call API / CLI to create stream tables | Change connection string; use custom SQL |
| **Latency impact** | Zero (off the query path) | +0.2-1.5ms per query |
| **Single point of failure** | No (sidecar crash doesn't affect queries) | Yes (proxy down = app down) |
| **DDL detection** | Poll `pg_catalog` or event triggers | Transparent interception |
| **Custom DDL syntax** | Not possible (function calls only) | `CREATE STREAM TABLE ...` works |
| **Pooler compatibility** | Works alongside any pooler | Must be carefully positioned in the connection topology |
| **Development effort** | 15-22 weeks | 31-51 weeks (sidecar + proxy) |
| **Ongoing maintenance** | Low | High (protocol compat, driver testing, security) |
| **Architecture precedent** | Epsio (proven, shipping) | ReadySet (proven, but much larger scope) |
| **Failure blast radius** | Low (sidecar failure = stale stream tables, app unaffected) | High (proxy failure = app cannot connect to DB) |

### 9.1 The Epsio Data Point

Epsio is the closest competitor to pg_trickle's sidecar vision. They
deliberately chose **not** to build a proxy, despite having the engineering
resources (funded company, Rust CDC forwarder). Instead:

- Management commands flow via `pg_logical_emit_message()` + response table.
- CDC uses logical replication (not proxy observation).
- Results are written back to PG tables.
- The sidecar is completely off the query hot path.

This validates that the non-proxy sidecar architecture is sufficient for a
successful IVM product. Epsio is deployed in production at scale without a
proxy.

### 9.2 The ReadySet Data Point

ReadySet built a full proxy because their **product is the proxy** — it's
a transparent caching layer. Query routing (serve cached results vs. proxy
to upstream PG) is the core feature and requires being in the query path.

pg_trickle's model is different: results are written back to PG tables.
The user queries PG directly. There's no query routing decision to make.
The proxy adds operational overhead without a core product reason.

---

## 10. Recommendation

### Verdict: **Do NOT build a pgwire proxy. Use the direct sidecar model (Option A).**

**Rationale:**

1. **The core value proposition doesn't require a proxy.** pg_trickle writes
   results to PG tables. Users query PG directly. There's no read-path
   routing that benefits from interception.

2. **The highest-value proxy feature (custom DDL) has a non-proxy alternative.**
   The Epsio-style Commander pattern (PL/pgSQL stubs + `pg_logical_emit_message`
   or command/response tables) provides a SQL-native management interface
   without proxy complexity. Users call `CALL pgtrickle.create_stream_table(...)`
   — familiar, no connection string changes, works with any middleware.

3. **Proxy adds latency with no compensating benefit.** Unlike ReadySet (which
   eliminates query execution cost), a pg_trickle proxy would **add** latency
   to every query while providing no read-path improvement.

4. **Proxy is a high-severity reliability risk.** The proxy becomes the only
   path to the database. If it crashes, the entire application is down. The
   sidecar model has a much smaller blast radius — sidecar crash means stale
   stream tables, not application outage.

5. **Development cost is disproportionate to benefit.** The proxy roughly
   doubles the sidecar development timeline (16-29 weeks additional) for
   features that are nice-to-have, not must-have.

6. **Operational complexity deters adoption.** Managed PG users (the primary
   sidecar audience) often already have connection poolers, proxies, and
   middleware. Adding another component in the data path creates confusion.

7. **Epsio validates the non-proxy approach.** A funded, production-deployed
   competitor ships without a proxy. The market has spoken.

### What To Build Instead

For the capabilities that a proxy _would_ provide, use these alternatives:

| Proxy Capability | Non-Proxy Alternative |
|-----------------|----------------------|
| Custom DDL syntax | Commander pattern: PL/pgSQL stubs + command/response tables, or HTTP API |
| DDL change detection | Schema fingerprinting + polling (every scheduler interval) |
| CDC | Trigger-based (installed by sidecar) or WAL logical replication |
| Observability | Prometheus `/metrics` endpoint on sidecar HTTP server |
| Connection pooling | Use existing pooler (PgBouncer, PgCat, Supavisor) |

---

## 11. If We Build It — Phased Approach

If, despite the recommendation above, there is a strong product reason to
build a proxy in the future (e.g., customer demand, competitive pressure),
here is a phased approach:

### Phase P0: Proof of Concept (3-4 weeks)

- Use `pgwire` crate for server + client.
- Simple query protocol passthrough only.
- Intercept `CREATE STREAM TABLE` as a string pattern match.
- No extended query protocol, no TLS, no auth.
- Goal: validate latency overhead and developer experience.

### Phase P1: Production Lite Proxy (8-12 weeks)

- Extended query protocol support.
- TLS / SCRAM-SHA-256 auth passthrough.
- DDL interception (CREATE/ALTER/DROP on source tables).
- Custom pg_trickle DDL commands.
- Prometheus metrics.
- Connection health checking (not pooling).
- Goal: usable by early adopters willing to add the proxy.

### Phase P2: Hardening (4-6 weeks)

- Driver compatibility test matrix (libpq, JDBC, psycopg3, asyncpg,
  node-pg, pgx-go, rust-postgres).
- Load testing (pgbench through proxy).
- HA deployment documentation (multiple instances + LB).
- COPY protocol support.
- Cancel request passthrough.
- Goal: production-ready for general use.

### Phase P3: Optional Connection Pooling (4-6 weeks)

- Transaction-mode pooling.
- Prepared statement remapping.
- Read/write routing to replicas.
- Goal: replace PgBouncer for users who want a single component.

**Total: 19-28 weeks** — significant investment. Only pursue after the
direct sidecar is proven and there is clear demand.

---

## 12. Open Questions

1. **Could we instead contribute pg_trickle proxy features to PgCat?**
   PgCat is Rust, MIT-licensed, and already handles the hard proxy work.
   A pg_trickle plugin for PgCat (query interception + DDL detection)
   could leverage their protocol implementation. This would be dramatically
   less effort than building from scratch.

2. **Is there a market segment where the proxy UX is the deciding factor?**
   If surveys show that "change connection string" onboarding triples
   adoption compared to "install PL/pgSQL stubs + call functions", the
   proxy investment may be justified. But this is speculative until
   validated.

3. **Could a pgwire proxy enable pg_trickle on databases where even
   PL/pgSQL function creation is restricted?** Some ultra-locked-down
   managed PG instances don't allow `CREATE FUNCTION`. A proxy that
   intercepts management commands could work without ANY server-side
   installation. This is a narrow but potentially high-value use case.

4. **Would a WebSocket-based management API be a better UX investment than
   a proxy?** A web dashboard where users visually create stream tables,
   monitor refresh status, and see the DAG — combined with an HTTP/WS API —
   could be more impactful than proxy-based SQL interception.

5. **Is `pg_logical_emit_message()` available on all target managed PG
   services?** If not, the Commander pattern may need a fallback (polling
   a commands table), which the proxy could bypass. Research needed per
   managed PG service.

---

## References

- [pgwire crate (v0.38)](https://crates.io/crates/pgwire) — Rust
  implementation of PostgreSQL wire protocol
- [ReadySet](https://github.com/readysettech/readyset) — Transparent
  pgwire proxy with incremental view maintenance (BSL 1.1)
- [Epsio architecture](https://docs.epsio.io/about/integrating-epsio/) —
  Sidecar IVM engine without proxy
- [PgCat](https://github.com/postgresml/pgcat) — Rust PostgreSQL
  connection pooler and proxy (MIT)
- [Supavisor](https://github.com/supabase/supavisor) — Cloud-native
  PostgreSQL connection pooler (Apache-2.0)
- [REPORT_EXTERNAL_PROCESS.md](REPORT_EXTERNAL_PROCESS.md) — External sidecar
  process feasibility study
- Rejected native-syntax proposal — Native DDL
  syntax plan for the extension
