# pg_trickle × pgvector — Synergy Report

**Date:** 2026-04-27
**Author:** Internal research
**Status:** PROPOSED
**Related roadmap items:** v0.37.0 (pgVectorMV — already planned), this plan
extends scope to v0.38–v1.x

---

## 1. Executive Summary

**pg_trickle** incrementally maintains materialized views inside PostgreSQL
using differential dataflow over trigger-based CDC.
**pgvector** adds a `vector` type (plus `halfvec`, `sparsevec`, `bit`),
distance operators (`<->`, `<=>`, `<#>`, `<+>`), and approximate-nearest-
neighbour indexes (HNSW, IVFFlat) to PostgreSQL.

The two extensions are **highly complementary**, and the combination is
strategically important: pgvector dominates the AI/RAG-on-Postgres niche
(50k+ stars, shipped on every managed-PG service), while pg_trickle is the
fastest path from "raw events / source rows" to "low-latency, derived state
that an AI agent can query." Embeddings are derived state. Embeddings need
recomputation on source change. ANN indexes need fresh data. Hybrid search
needs a denormalised, scored source table. **All four problems are exactly
the problems pg_trickle was built to solve** — just for vectors instead of
relational deltas.

The v0.37.0 roadmap already commits to `pgVectorMV` (a `vector_avg`
algebraic aggregate). This document scopes that work and adds eleven further
synergy items spanning v0.37 through v1.x: incremental embedding pipelines,
per-cluster centroid maintenance, hybrid search materializations, ANN
freshness monitoring, IVFFlat re-clustering scheduling, sparse-vector
support, half-precision passthrough, and an `embedding_stream_table`
ergonomic API.

The strategic upside is large: pgvector's 50k-star user base is exactly the
AI/RAG audience that suffers most from "embeddings go stale, ANN index drift,
nightly re-cluster cron job" pain. pg_trickle solves all three, and an
official integration story would be one of the strongest v1.x marketing
narratives available.

---

## 2. pgvector Overview

| Attribute | Details |
|---|---|
| **Repository** | [pgvector/pgvector](https://github.com/pgvector/pgvector) |
| **Language** | C (extension), some SQL |
| **License** | PostgreSQL License (very permissive — strictly better than AGPL) |
| **Stars / Contributors** | ~50k ★ / 100+ contributors |
| **Latest stable** | 0.8.x line (as of writing) |
| **PG versions** | 13–18 |
| **Deployment** | First-class on RDS, Aurora, Cloud SQL, Supabase, Neon, Crunchy, Timescale Cloud, Azure, CNPG |
| **Maintainer** | Andrew Kane (`ankane`) — ships fast, conservative releases, ABI-stable |

### 2.1 Type System

| Type | Width | Notes |
|---|---|---|
| `vector(d)` | 4·d bytes (float32) + header | The original type. Up to 16 000 dimensions (2 000 for indexes). |
| `halfvec(d)` | 2·d bytes (float16) | Added in 0.7. ~50% storage saving with negligible quality loss for embeddings. Indexable. |
| `sparsevec(d)` | variable (index/value pairs) | Added in 0.7. For BM25-style or learned sparse representations (SPLADE, etc.). Indexable with HNSW. |
| `bit(d)` (built-in) | d bits | Used with `<~>` Hamming distance for binary-quantised embeddings. |

### 2.2 Operators

| Operator | Distance | Index AM support | Notes |
|---|---|---|---|
| `<->` | L2 (Euclidean) | HNSW, IVFFlat | Most common ANN operator. |
| `<=>` | Cosine | HNSW, IVFFlat | Default for normalised embeddings. |
| `<#>` | Negative inner product | HNSW, IVFFlat | Use when vectors are already normalised; faster than cosine. |
| `<+>` | L1 (Manhattan) | HNSW (0.7+) | |
| `<~>` | Hamming | HNSW (0.7+, on `bit`) | Binary quantisation. |
| `<%>` | Jaccard | HNSW (0.7+, on `bit`) | |

All operators are **immutable, side-effect-free, and deterministic** — but
they are *custom operators* registered by pgvector, not built-in PostgreSQL
operators.

### 2.3 Index Access Methods

| AM | Build cost | Query cost | Update model | Recall tuning |
|---|---|---|---|---|
| **HNSW** | High (memory- and CPU-heavy) | O(log N) typical | Per-row insert OK; deletes mark tombstones | `m`, `ef_construction`, `ef_search` |
| **IVFFlat** | Medium (requires sample for k-means) | O(N/lists) | Inserts append to nearest list; quality degrades as data drifts from initial centroids | `lists` (build-time), `probes` (query-time) |

Crucial for this analysis:

- **HNSW handles incremental writes well** but degrades on heavy DELETE
  workloads (tombstone bloat); periodic `REINDEX` is the standard remedy.
- **IVFFlat *requires* periodic re-clustering** when the data distribution
  shifts. The recommended pattern in pgvector docs is "build the index
  after the table is mostly populated; rebuild on a schedule."

### 2.4 Functions Worth Knowing

- `cosine_distance`, `l2_distance`, `inner_product`, `l1_distance`,
  `hamming_distance`, `jaccard_distance` — function forms of the operators.
- `vector_dims`, `vector_norm`, `l2_normalize` — utilities.
- `avg(vector)`, `sum(vector)` — built-in aggregates (added in 0.5+).
  Critically: **`avg(vector)` is just `sum / count` element-wise**, which
  means it is algebraically maintainable using the same Welford-style
  scheme pg_trickle already uses for `AVG(numeric)`.
- `binary_quantize(vector) → bit(d)` — for binary-index workflows.
- `subvector(vector, start, len)` — Matryoshka-style truncation.

### 2.5 What pgvector Deliberately Does Not Do

- No incremental aggregation / IVM (delegated to apps or other extensions).
- No CDC, no triggers, no scheduling (delegated to apps or other extensions).
- No automatic embedding generation (delegated to pgai / pg_ml / app code).
- No hybrid-search ranking primitives (delegated to apps).
- No re-clustering scheduler (manual `REINDEX`).

**Every "no" in that list is a "yes" in pg_trickle's roadmap.** This is the
core thesis of this report.

---

## 3. Synergy Analysis

### 3.1 The Core Value Proposition

**Without pg_trickle:** Anyone running pgvector at production scale faces
four recurring operational problems:

1. **Embedding freshness.** When source rows change, embeddings must be
   recomputed. Most teams hand-roll a queue + worker + cron job, with
   eventual-consistency windows measured in minutes to hours.
2. **Hybrid search staleness.** Production RAG combines BM25/keyword with
   vector similarity, then re-ranks. The denormalised "search corpus" view
   (joining documents + metadata + tags + permissions) is usually a
   nightly-rebuilt materialised view.
3. **IVFFlat drift.** As data distribution changes, IVFFlat recall drops.
   Teams either rebuild on a schedule (under-fresh) or never rebuild
   (recall degrades silently).
4. **Centroid / cluster maintenance.** Many ML pipelines need *aggregate*
   vectors (per-user, per-tenant, per-cluster). These are derived data
   that change with every source insert and are almost always recomputed
   from scratch.

**With pg_trickle:** Each of these is reframed as "a stream table with a
`vector`-aware refresh strategy." Differential refresh means the vector
ANN index sees only true deltas (good for HNSW, neutral for IVFFlat).
A `vector_avg` algebraic aggregate maintains centroids in O(Δ). A
schedule-driven full refresh of the IVFFlat-backing table is just another
stream-table cadence. And the entire orchestration runs in-database, with
no Python worker, no Celery, no Airflow.

### 3.2 Synergy Matrix

| ID | pg_trickle Provides | pgvector Provides | Combined Value |
|---|---|---|---|
| **VS-1: Embedding-derived stream tables** | Differential refresh of any SELECT | `vector` storage + ANN index | Embeddings recomputed only on source-row change, indexed automatically |
| **VS-2: Incremental centroid maintenance** | `vector_avg` algebraic aggregate (v0.37) | `avg(vector)` semantics + HNSW indexable centroids | Per-cluster / per-user / per-tenant centroids maintained in O(Δ) |
| **VS-3: Hybrid search corpus** | Multi-way joins, GROUP BY, full SQL into a flat ST | Vector column + ANN index on the same flat table | Hybrid (BM25 ∪ vector ∪ metadata-filter) search over a single, always-fresh flat table |
| **VS-4: ANN freshness monitoring** | Self-monitoring views (v0.20+), watermarks (v0.7+) | — | Operators see "last refresh", "rows behind", "embedding lag" per ST |
| **VS-5: Scheduled IVFFlat rebuild** | SLA scheduler (v0.22), tiered scheduling (v0.14) | IVFFlat needs periodic `REINDEX` | Re-clustering becomes a managed cadence (`reindex_schedule`) |
| **VS-6: Sparse + dense fusion** | Stream table assembles `(dense_vec, sparse_vec, text)` triple | Both `vector` and `sparsevec` indexable | One ST = one fused retrieval index for SPLADE-style hybrid |
| **VS-7: Vector passthrough in DIFFERENTIAL** | Already works (vector is opaque to delta engine) | `vector` columns survive CDC unchanged | Existing pgvector-shaped data benefits from differential refresh today |
| **VS-8: Half-precision / quantisation pipelines** | ST defines `cast / binary_quantize` once | `halfvec`, `bit`, `binary_quantize` | Storage-tiered ST (raw float32 → halfvec → bit) maintained automatically |
| **VS-9: Permission-scoped vector views** | RLS-aware stream tables (v0.5) | Vector + RLS works natively | Per-tenant ANN indexes on RLS-filtered STs |
| **VS-10: Outbox of embedding events** | Outbox/Inbox (v0.28–29) | — | Downstream systems get notified on embedding arrival/change |
| **VS-11: Multi-level RAG pipelines** | Stream tables on stream tables (DAG) | ANN index at any layer | Raw → chunks → enriched → ranked, each layer maintained incrementally |
| **VS-12: Reactive subscriptions for AI agents** | Reactive subscriptions (v0.35) | — | Agents subscribe to "new neighbours appear within ε of query vector" |

### 3.3 Where pg_trickle's Existing Engine Already Fits

- **Vectors as opaque payload.** pg_trickle's CDC and MERGE paths copy
  values byte-for-byte. `vector(1536)` works today without engine changes
  (verified — see [docs/FAQ.md](../../docs/FAQ.md#does-pg_trickle-work-with-pgvector)).
- **Algebraic aggregates.** pg_trickle already maintains `SUM`, `COUNT`,
  `AVG`, `STDDEV` algebraically (v0.9, v0.16). Adding `vector_avg` is a
  rule-set extension, not a new engine.
- **TopK with ORDER BY ... LIMIT.** Stream tables already support TopK
  (v0.2) with ordering operators. The blocker for `ORDER BY embedding <-> q`
  is that `q` is per-query, not per-ST — see §5.4.
- **Schedule + cost model.** `lazy / scheduled / immediate` already exists.
  IVFFlat re-clustering can be modelled as a third refresh mode
  (`reindex_after_refresh = 'always' | 'on_drift' | 'never'`).

### 3.4 Where pg_trickle Needs New Engine Work

- **Differentiation rules for `<->`, `<=>`, `<#>`** — only matter inside
  delta predicates (e.g. `WHERE a.embedding <-> b.embedding < 0.5`).
  Distance is *non-linear* and *non-monotone in joins*; the right answer
  is "fall back to FULL for distance-predicated stream tables, document
  this clearly." (Already the current behaviour; just needs to be a
  first-class supported fallback rather than an emergent one.)
- **`vector_avg` algebraic aggregate.** Needs a new reducer for the
  algebraic-aggregate framework. Implementation is straightforward: maintain
  `(sum_vec, count)`, divide on read. Welford is *not* required for
  numerical stability of vector means in practice (embeddings live in a
  bounded range), but element-wise running mean works.
- **Reindex scheduling primitive.** A new ST option
  `post_refresh_action = 'reindex' | 'analyze' | 'none'` that runs
  inside the refresh transaction (or as a follow-up).

---

## 4. Concrete Use Cases

### 4.1 Always-Fresh Document Embeddings (RAG)

**Problem.** A documentation site stores Markdown chunks in `doc_chunks`.
Each chunk has an `embedding vector(1536)` derived from `chunk_text`.
Today, when `chunk_text` changes, an external worker must notice, recompute
the embedding via OpenAI/Voyage/local model, and `UPDATE` the row.

**Solution sketch (with pgai or callable embedding function in DB):**

```sql
SELECT pgtrickle.create_stream_table(
  'doc_chunks_embedded',
  $$
    SELECT c.id, c.doc_id, c.chunk_text,
           pgai.openai_embed('text-embedding-3-small', c.chunk_text)
             AS embedding,
           d.title, d.tags, d.last_updated
    FROM doc_chunks c
    JOIN documents  d ON d.id = c.doc_id
  $$,
  refresh_mode => 'DIFFERENTIAL',
  schedule     => '5 seconds'
);

CREATE INDEX ON doc_chunks_embedded
  USING hnsw (embedding vector_cosine_ops);
```

**Why this is unique to pg_trickle.** Only the *changed chunks* re-embed
(differential delta = the only set of `chunk_text` values that changed).
HNSW receives clean inserts/deletes. No external worker. No queue.

### 4.2 Per-User / Per-Tenant Centroids (Recommendation)

**Problem.** A recommender computes `user_taste = avg(item.embedding)
WHERE user_actions.user_id = U`. Today this is recomputed nightly per user.

**Solution (with pgVectorMV from v0.37):**

```sql
SELECT pgtrickle.create_stream_table(
  'user_taste',
  $$
    SELECT a.user_id,
           vector_avg(i.embedding) AS taste_vec,
           COUNT(*) AS action_count
    FROM user_actions a
    JOIN items i ON i.id = a.item_id
    GROUP BY a.user_id
  $$,
  refresh_mode => 'DIFFERENTIAL'
);

CREATE INDEX ON user_taste USING hnsw (taste_vec vector_cosine_ops);

-- "Find users whose taste is similar to the query item":
SELECT user_id FROM user_taste
ORDER BY taste_vec <=> $1 LIMIT 50;
```

**Why this is unique.** Each new `user_action` updates exactly one row's
centroid in O(1). No rebuild. The HNSW index over taste vectors stays
fresh by definition.

### 4.3 Hybrid Search Corpus (BM25 + Vector + Metadata)

**Problem.** Production RAG retrieval is almost never pure vector — it
fuses BM25, vector similarity, recency, and ACL filters. The retrieval
corpus is a denormalised join across 4–8 tables.

**Solution.**

```sql
SELECT pgtrickle.create_stream_table(
  'search_corpus',
  $$
    SELECT d.id, d.title, d.body, d.embedding,
           array_agg(t.tag) AS tags,
           p.tenant_id, p.acl_groups,
           d.created_at, d.updated_at
    FROM documents d
    JOIN doc_perms p ON p.doc_id = d.id
    LEFT JOIN doc_tags t ON t.doc_id = d.id
    GROUP BY d.id, p.tenant_id, p.acl_groups
  $$,
  refresh_mode => 'DIFFERENTIAL',
  schedule     => '10 seconds'
);

CREATE INDEX ON search_corpus USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ON search_corpus USING gin  (to_tsvector('english', body));
CREATE INDEX ON search_corpus USING gin  (tags);
CREATE INDEX ON search_corpus            (tenant_id);
```

The application's hybrid-search query then runs against a single flat,
fresh, properly indexed table — no Elasticsearch, no Weaviate, no Qdrant.

### 4.4 IVFFlat Re-Cluster on Drift

**Problem.** IVFFlat recall degrades as the embedding distribution drifts.
Teams need a re-cluster cadence.

**Solution.**

```sql
SELECT pgtrickle.create_stream_table(
  'product_embeddings',
  $$ SELECT id, embedding FROM products WHERE active $$,
  refresh_mode         => 'DIFFERENTIAL',
  schedule             => '1 minute',
  post_refresh_action  => 'reindex_if_drift',  -- new in proposal §5.5
  reindex_drift_threshold => 0.15              -- 15% rows changed since last reindex
);
```

pg_trickle tracks rows-changed-since-last-rebuild against a threshold;
when crossed, it issues `REINDEX INDEX CONCURRENTLY` on the IVFFlat
index after the refresh transaction commits.

### 4.5 Reactive Neighbour Alerts

**Problem.** Anomaly detection: "alert me whenever a new transaction
embedding lands within ε of any known-fraud embedding."

**Solution (combines v0.35 reactive subscriptions with pgvector):**

```sql
LISTEN fraud_neighbour;

SELECT pgtrickle.create_reactive_subscription(
  'fraud_neighbour',
  $$
    SELECT t.id
    FROM transactions_embedded t, known_fraud k
    WHERE t.embedding <=> k.embedding < 0.05
  $$
);
```

Every refresh that adds a row to the join produces a `NOTIFY` carrying
the new transaction ID. The subscription is differential: only *new*
neighbour pairs fire (no spurious republish on every refresh).

### 4.6 Storage-Tiered Embeddings

**Problem.** Storing 100M × `vector(1536)` is 600 GB. `halfvec` cuts it
to 300 GB; binary quantisation cuts it to ~19 GB but with recall loss.
A tiered pipeline (binary for first-pass recall, halfvec for re-rank,
float32 only for top-K) is the standard pattern.

**Solution.**

```sql
-- Tier 1: float32 source
-- Tier 2: halfvec stream table (passthrough, automatic)
SELECT pgtrickle.create_stream_table(
  'docs_half',
  $$ SELECT id, embedding::halfvec(1536) AS h FROM docs $$
);

-- Tier 3: bit-quantised stream table on top of tier 2
SELECT pgtrickle.create_stream_table(
  'docs_bin',
  $$ SELECT id, binary_quantize(h)::bit(1536) AS b FROM docs_half $$
);
```

Each tier is incrementally maintained. Each can be independently indexed
(`bit_hamming_ops`, `halfvec_cosine_ops`, `vector_cosine_ops`).

---

## 5. Technical Integration Considerations

### 5.1 Vector Type as Opaque Payload (Already Works)

The CDC trigger generator uses `format_type()`, which returns `vector(1536)`,
`halfvec(1536)`, `sparsevec(1536)`, etc. correctly. Change buffers are
type-faithful. MERGE on the storage side is binary-equality based, which
is correct for vectors (NaN edge case noted in §8). **No engine work
needed for VS-7.**

### 5.2 Custom Operator Differentiation Rules

The DVM engine's parser (`src/dvm/parser/`) classifies operators as
"known monotone", "known with rule", or "unsupported → fallback to FULL."
pgvector operators (`<->`, `<=>`, `<#>`, `<+>`, `<~>`, `<%>`) are
*non-monotone non-linear* on the join input — there is no general
differential rule. The principled position:

1. **Detect them by Oid lookup** (not by name — names can be re-aliased).
2. **Whitelist them as IVM-incompatible-but-FULL-safe**: a stream table
   containing them in `WHERE` or `ORDER BY` falls back to FULL refresh
   automatically with an `INFO` log explaining why.
3. **Document that vector passthrough in projection is fine** for DIFFERENTIAL.

This is mostly already the behaviour but should be explicitly tested and
documented as a stable contract, not an emergent fallback.

### 5.3 `vector_avg` Algebraic Aggregate (v0.37 — pgVectorMV)

The implementation is straightforward:

- **State:** `(sum_vec: vector(d), count: int8)`.
- **Add row:** `sum_vec += new.embedding; count += 1`.
- **Remove row:** `sum_vec -= old.embedding; count -= 1`.
- **Read:** `sum_vec / count` (element-wise).
- **Edge case `count == 0`:** delete the group row (matches existing
  `AVG` group-disappear semantics).

Numerical stability concerns (Welford for floating-point drift) are
real over millions of updates but mitigable: re-baseline the group
state on every Nth full refresh, or scrub via watermark-driven
recomputation. Empirically, embeddings live in a bounded subspace
(typically `[-1, 1]^d` after normalisation), so the running sum stays
well below `f32` overflow for sub-billion-row groups.

**Open question:** `halfvec` and `sparsevec` arithmetic — do we provide
`halfvec_avg` and `sparsevec_avg` separately, or always upcast the
running state to `vector(d)`? Recommendation: upcast state to `vector`,
return as the input type.

### 5.4 ORDER BY With Per-Query Vectors

`ORDER BY embedding <=> $1 LIMIT k` requires a *per-query* parameter,
which is by design not part of an ST's defining query. The right
integration story is:

- **STs do not bake in the query vector.** They produce the *corpus*.
- **Application queries** then run `ORDER BY embedding <=> q LIMIT k`
  against the ST as a normal SELECT, hitting the HNSW/IVFFlat index.

Where pg_trickle helps is the corpus *side*: keep it dense, indexed,
fresh, denormalised, RLS-filtered, schema-stable. The Top-K query
itself is plain pgvector.

(There is a separate, more advanced pattern — "materialised neighbour
graph" — where for a fixed set of pivots you maintain k-nearest
neighbours as an ST. That is feasible but out of scope for v0.37 and
likely a v1.x research item.)

### 5.5 ANN Index Maintenance Cadence

Two new per-ST options proposed:

| Option | Values | Effect |
|---|---|---|
| `post_refresh_action` | `none` (default), `analyze`, `reindex`, `reindex_if_drift` | Action to run after a refresh commits |
| `reindex_drift_threshold` | `float in [0, 1]` (default `0.10`) | Fraction of rows changed since last reindex that triggers `reindex_if_drift` |

Implementation notes:
- Run inside a separate transaction *after* the refresh commits so that
  `REINDEX CONCURRENTLY` does not lengthen the refresh window.
- Track "rows changed since last reindex" in `pgtrickle.pgt_stream_tables`
  alongside existing watermark counters.
- Per the SLA scheduler (v0.22), reindex jobs go in a separate tier so
  they never block refresh latency budgets.

### 5.6 Memory & Resource Planning

| Resource | pgvector Impact | pg_trickle Interaction | Recommendation |
|---|---|---|---|
| `maintenance_work_mem` | HNSW build needs lots | `REINDEX` triggered post-refresh | ≥ 4 GB for any non-trivial corpus |
| `shared_buffers` | HNSW pages benefit from cache | Stream-table buffers also compete | Increase by 25–50% over a pure-pgvector budget |
| `work_mem` | Vector `ORDER BY ... LIMIT` if no index | DVM merge stage | 64–256 MB |
| Disk | `vector(1536)` × N rows | Change buffers double-store deltas | Plan 2× steady-state for differential mode |
| `max_parallel_workers_per_gather` | HNSW build can parallelise | Refresh worker pool (v0.22) | ≥ 8 |

### 5.7 Volatility & IMMEDIATE Mode

The pgvector distance operators are `IMMUTABLE`. They *can* appear in
IMMEDIATE-mode stream tables provided they are in a projection or a
filter that doesn't require differentiation across joined deltas.
The validator (`src/dvm/parser/validation.rs`) already classifies
volatility correctly via `pg_proc.provolatile`; nothing new needed.

Embedding *generation* functions (e.g. `pgai.openai_embed`) are
typically `VOLATILE` and call out to network. These currently force
FULL mode; that is correct behaviour. Users should pre-compute
embeddings into a column rather than embedding-on-read.

### 5.8 NULL and Dimension Mismatches

- `vector` allows NULL values. CDC handles NULL correctly today.
- Dimension mismatches (`vector(768)` joined with `vector(1536)`) raise
  at query time, not refresh time, since pg_trickle does not introspect
  vector dimensions. This is the right behaviour: errors propagate as
  refresh failures, observable through the v0.20+ self-monitoring.

### 5.9 NaN / Inf in Vectors

pgvector rejects NaN/Inf at insert time (since 0.7). Older versions
allowed them; if a stream table sees a NaN-bearing vector, MERGE
equality semantics may behave unexpectedly. Recommendation:
documentation note + a CI test that asserts pg_trickle behaves
correctly under both.

---

## 6. Roadmap Fit

The v0.37.0 roadmap already commits to **pgVectorMV** (the
`vector_avg` algebraic aggregate). This plan extends that into a
multi-release programme.

### 6.1 v0.37.0 (planned) — pgVectorMV core

| Item | Status | Effort |
|---|---|---|
| `vector_avg` algebraic aggregate (sum + count, element-wise mean) | Planned | M |
| `vector_sum` algebraic aggregate (just sum) | Add to scope | S |
| Validator: explicitly classify pgvector distance operators as "FULL-fallback safe" with documented `INFO` log | Add to scope | S |
| Integration test: `user_taste` example end-to-end with HNSW index | Add to scope | M |
| FAQ + cookbook entry: "Maintaining centroids with pgVectorMV" | Add to scope | S |

### 6.2 v0.38.0 — Embedding Pipelines & ANN Maintenance

| Item | Description | Effort |
|---|---|---|
| **VP-1** | `post_refresh_action` ST option (`none`/`analyze`/`reindex`/`reindex_if_drift`) | M |
| **VP-2** | `reindex_drift_threshold` ST option + drift counter in catalog | M |
| **VP-3** | `pgtrickle.vector_status()` view: per-ST embedding lag, ANN index age, drift % | S |
| **VP-4** | Cookbook: "Always-fresh RAG with pg_trickle + pgvector + pgai" | S |
| **VP-5** | Docker image variant: `pg_trickle + pgvector + pgai` for one-command RAG | S |

### 6.3 v0.39.0 — Hybrid Search & Sparse Vectors

| Item | Description | Effort |
|---|---|---|
| **VH-1** | `sparsevec_avg`, `halfvec_avg` aggregates (auto-upcast state) | M |
| **VH-2** | Reactive subscription example: "alert on new neighbour within ε" | S |
| **VH-3** | Hybrid-search cookbook: BM25 + vector + metadata-filter ST pattern | S |
| **VH-4** | Benchmark suite: differential refresh + HNSW insert throughput vs. baseline (pure pgvector) | M |

### 6.4 v0.40.0+ — Advanced Patterns

| Item | Description | Effort |
|---|---|---|
| **VA-1** | `embedding_stream_table()` ergonomic API: `(name, source, text_column, embedding_function)` → ST + ANN index in one call | L |
| **VA-2** | Materialised k-NN graph for fixed pivot set (research) | XL |
| **VA-3** | Outbox-emitted embedding events: downstream sinks see "embedding for doc X is now V" | M |
| **VA-4** | Per-tenant ANN index pattern with RLS-aware ST partitioning | M |
| **VA-5** | Joint case study / blog with pgvector or pgai team | — |

### 6.5 Why This Belongs in the Pre-1.0 Window

Three reasons to keep VP-* in v0.38 and not push it past v1.0:

1. **Audience overlap is enormous.** pgvector is the single most-installed
   PostgreSQL extension in the AI niche. Anything that solves IVFFlat
   re-clustering and embedding freshness automatically ships to a
   ready-made user base.
2. **Engine cost is low.** VP-1/VP-2/VP-3 are catalog options + a post-
   commit hook. They reuse the existing scheduler and do not touch the
   DVM hot path.
3. **Risk surface is small.** None of the VS-* items change the engine
   semantics for non-vector workloads. Vector users opt in; everyone
   else is unaffected.

---

## 7. Competitive Landscape

### 7.1 What This Replaces

| Traditional Stack | pg_trickle + pgvector |
|---|---|
| Postgres → Debezium → Kafka → Python worker → embed → write back | Postgres + pg_trickle stream table calling embed function |
| Postgres + nightly cron `REINDEX` of IVFFlat | `post_refresh_action = 'reindex_if_drift'` |
| Postgres + Airflow DAG to recompute centroids | `vector_avg` algebraic aggregate, refreshed every N seconds |
| Postgres + Pinecone/Weaviate/Qdrant for hybrid search | Single stream table indexed with HNSW + GIN + B-tree |
| Postgres + Redis cache of "user taste vector" | `user_taste` stream table with HNSW |

### 7.2 Comparison With Alternatives

| Approach | Embedding freshness | Hybrid search | Centroid maintenance | Ops complexity |
|---|---|---|---|---|
| **pg_trickle + pgvector** | Seconds (differential) | Yes (single ST) | O(Δ) algebraic | Low (1 PG, 2 extensions) |
| pgvector alone + cron worker | Minutes–hours | Hand-rolled | Full rebuild | Medium |
| pgvector + Debezium + Python | Seconds (with infra) | Hand-rolled | Custom code | High |
| Pinecone / Weaviate / Qdrant | Seconds | Native | Native | High (separate cluster) |
| pg_ivm + pgvector | Immediate | Limited (no aggregates) | None | Low but limited |
| Timescale + pgai + pgvector | Seconds | Manual | Manual | Medium |

The closest commercial overlap is **Timescale's pgai + pgvectorscale**
combination. pg_trickle differs by being aggregate-aware (centroids,
hybrid-search corpora as first-class STs) and by having a generalised
incremental engine rather than embedding-pipeline-specific tooling.
The two stories are complementary, not competitive — pgai handles the
embedding *generation* call, pg_trickle handles the *propagation* of
source changes through to the embedded artefact.

---

## 8. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **`vector_avg` numerical drift over millions of updates** | Medium | Slow accuracy degradation | Periodic full-refresh re-baseline (every N hours per ST); document the threshold; add CI assertion that `vector_avg` ≈ `avg(vector)` over a synthetic 10 M-row scenario |
| **HNSW tombstone bloat under heavy DELETE deltas** | Medium | Recall and latency degrade | Surface via `pgtrickle.vector_status()`; recommend `reindex_if_drift` |
| **IVFFlat recall silently degrades** | High (without VP-1) → Low (with VP-1) | Bad search quality | `reindex_if_drift` + drift dashboard |
| **NaN/Inf in vectors break MERGE equality** | Low (rejected at insert by pgvector ≥ 0.7) | Refresh stuck | Reject NaN/Inf in pre-flight check; CI test |
| **pgvector ABI changes** | Low (Andrew Kane is conservative) | Compile failure | Pin minor version range; CI matrix across pgvector 0.7, 0.8 |
| **pgai is volatile** | Medium | Embedding functions can fail / be slow / cost money | Document pattern: precompute into a column, then ST is fast; never put `VOLATILE` external calls inside DIFFERENTIAL paths |
| **Per-row HNSW insert dominates refresh time** | Medium | Refresh latency rises with corpus size | Document `(N rows changed) × O(log N)` cost; recommend tiered scheduling; expose timing in self-monitoring |
| **Sparse-vector aggregation semantics ambiguous** | Low | Confusion | `sparsevec_avg` defined as element-wise mean over the union of dimensions, treating absent entries as 0 — document explicitly |
| **NULL vector handling** | Low | Group counted but no vector to add | Skip NULL rows in `vector_avg` add/remove (matches `AVG`) |

---

## 9. Open Questions

1. **`pgai` vs. user-supplied embedding function.** The cookbook should
   show both: the pgai path (zero infra, vendor lock-in) and the
   "pre-compute in your app, write the column" path (no vendor, more
   work). Which do we lead with?
2. **Should `vector_avg` short-circuit to upstream `avg(vector)` when
   group size > some threshold?** pgvector ships a non-incremental
   `avg(vector)`; the algebraic version trades correctness floor for
   memory state. Investigate breakeven row counts.
3. **Reactive subscriptions over distance predicates.** Today reactive
   subscriptions detect insert/update/delete on the materialised result
   set. For "neighbour appeared within ε," is the *result set* the join
   `(query_vec, t)` rows? Needs a small design pass.
4. **Materialised k-NN graph (VA-2).** Promising but research-grade.
   Worth a separate `docs/research/MATERIALIZED_KNN.md` before commitment.
5. **`pgvectorscale` and `vchord`.** Should we test against these higher-
   performance pgvector variants too? Recommendation: yes for
   pgvectorscale (Timescale-maintained, broad usage); defer vchord.
6. **Distributed (Citus) vector aggregation.** With v0.32–v0.34
   delivering Citus support, can `vector_avg` shard cleanly?
   (Spoiler: yes — it's a sum + count, both shard-additive — but
   needs explicit testing.)

---

## 10. Conclusion

pgvector and pg_trickle solve different halves of the same problem.
pgvector stores and indexes vectors; pg_trickle keeps derived data
fresh. AI/RAG workloads are *the* use case where derived data needs
to be both vector-shaped and continuously fresh, and they are also
the use case where the operational pain (cron rebuilds, stale ANN
indexes, centroid recomputation, hand-rolled freshness pipelines)
is most acute.

The v0.37.0 roadmap already commits to the engine primitive
(`vector_avg`). This plan extends that into a coherent two-release
programme:

- **v0.37.0 — pgVectorMV core.** Centroid maintenance, distance-
  operator FULL-fallback documented as a contract, integration test,
  cookbook.
- **v0.38.0 — Embedding pipeline ergonomics.** `post_refresh_action`,
  drift-driven reindex, vector status view, RAG cookbook, combined
  Docker image.
- **v0.39.0 — Hybrid search, sparse vectors, benchmarks.**
- **v0.40+ — Ergonomic API, materialised k-NN graphs, joint
  case study.**

Strategic upside: pgvector's user base is the single largest available
target audience for pg_trickle, and the integration story
("incremental ANN-index maintenance, automatic embedding freshness,
zero new infrastructure") is one of the strongest narratives the
project can ship before v1.0. None of the proposed work changes
engine semantics for non-vector users; it is purely additive.

---

## 11. References

- pgvector repository: <https://github.com/pgvector/pgvector>
- pgvector 0.7 release notes (halfvec, sparsevec, L1, Hamming, Jaccard)
- pgai by Timescale: <https://github.com/timescale/pgai>
- pgvectorscale: <https://github.com/timescale/pgvectorscale>
- [docs/FAQ.md — Does pg_trickle work with pgvector?](../../docs/FAQ.md)
- [roadmap/v0.37.0.md — pgVectorMV](../../roadmap/v0.37.0.md)
- [plans/PLAN_OVERALL_ASSESSMENT_7.md — F4 pgVectorMV brief](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/PLAN_OVERALL_ASSESSMENT_7.md)
- [plans/patterns/PLAN_CQRS.md — pgvector as semantic read model](../patterns/PLAN_CQRS.md)
- [plans/ecosystem/PLAN_PG_SEARCH.md — companion ParadeDB synergy report](PLAN_PG_SEARCH.md)
