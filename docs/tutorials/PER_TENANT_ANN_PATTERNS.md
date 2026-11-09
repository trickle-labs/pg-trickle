# Per-Tenant ANN Indexing Patterns

> VA-3 — Production patterns for multi-tenant RAG using RLS-scoped
> embedding corpora.

## Security Model

**Trust boundaries** for multi-tenant ANN stream tables:

1. **Row-level security can enforce tenant isolation** for restricted readers
   of the stream table. pg_trickle respects PostgreSQL's normal RLS rules;
   the stream table itself is a regular table.
2. **Defining SQL runs as the stream-table owner** with `row_security = on`.
   PostgreSQL still exempts superusers and roles with `BYPASSRLS`; table owners
   normally bypass their own policies unless `FORCE ROW LEVEL SECURITY` is set.
   Use a dedicated corpus owner and check its source-table access and policies.
   When that owner is subject to source RLS, explicit `DIFFERENTIAL` is rejected
   and `AUTO` uses `FULL`; see the [RLS contract](../SQL_REFERENCE.md#row-level-security-rls).
3. **Never grant direct INSERT/UPDATE to application users** — only the
   pg_trickle background worker should write to stream tables.
4. **Audit the defining query** — if the defining query joins tenant data
   across boundaries (e.g. `FROM all_tenants_table`), the stream table will
   contain cross-tenant data.  Verify RLS on source tables applies during the
   defining query execution if you rely on source-level isolation.

Source policies are not inherited by the storage table: configure and test its
own RLS policies and reader grants. A custom GUC is not authentication: any
client with arbitrary SQL access can set `app.tenant_id`. The examples below
assume a trusted application authenticates the tenant and runs queries through
a restricted reader that cannot bypass RLS. Do not grant readers direct access
to partitions unless those relations have equivalent protection.

---

## Pattern 1: Tenant Column + RLS Policy

```sql
CREATE EXTENSION IF NOT EXISTS vector;

-- Source table; add source RLS separately if the owner must be restricted
CREATE TABLE tenant_embeddings (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'standard',
    content     TEXT NOT NULL,
    embedding   vector(3) NOT NULL
);

-- Three dimensions keep this example runnable; use your model's dimension.
INSERT INTO tenant_embeddings (tenant_id, tier, content, embedding) VALUES
    ('00000000-0000-0000-0000-000000000001', 'premium', 'red apple', '[1,0,0]'),
    ('00000000-0000-0000-0000-000000000002', 'standard', 'blue berry', '[0,1,0]');

-- Stream table (inherits tenant_id column)
SELECT pgtrickle.create_stream_table(
    'tenant_corpus',
    $$
        SELECT id, tenant_id, content, embedding
        FROM tenant_embeddings
    $$,
    '1m',
    'DIFFERENTIAL'
);

-- Enable RLS
ALTER TABLE tenant_corpus ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_corpus FORCE ROW LEVEL SECURITY;

-- Policy: each user sees only their tenant's rows
CREATE POLICY tenant_isolation ON tenant_corpus
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

-- ANN index (spans all tenants — filtered at query time by RLS)
CREATE INDEX ON tenant_corpus USING hnsw(embedding vector_cosine_ops);
```

**Query pattern:**

```sql
-- A trusted application binds the authenticated tenant as $1 in a transaction.
BEGIN;
SELECT set_config('app.tenant_id', $1::text, true);

-- This query automatically applies the RLS policy
SELECT id, content, embedding <=> '[1,0,0]'::vector AS distance
FROM tenant_corpus
WHERE tenant_id = $1::text::uuid
ORDER BY embedding <=> '[1,0,0]'::vector
LIMIT 10;
COMMIT;
```

Approximate index scans can return fewer than the requested number of rows when
tenant filtering discards candidates. pgvector 0.8 and later support iterative
scans with configurable limits; tune them for the workload and measure recall
against an exact query with the same tenant filter. Exact filtered search can be
preferable for highly selective tenants.

---

## Pattern 2: Partitioned by Tenant (High-Volume)

For large corpora, hash partitioning can distribute indexes and maintenance
across buckets. A bucket can contain multiple tenants; it is not per-tenant
index isolation. Evaluate its cost and recall for your workload.

### `partition_by` syntax

The `partition_by` parameter accepts a string in one of two forms:

| Form | Example | Description |
|------|---------|-------------|
| `HASH:<column>:<buckets>` | `'HASH:tenant_id:16'` | HASH-partition on a single column into N equal-sized buckets.  `<buckets>` must be a power of 2 between 2 and 256. |
| `RANGE:<column>` | `'RANGE:created_at'` | RANGE-partition on a date/timestamp column.  Boundaries must be added manually via `CREATE TABLE … PARTITION OF`. |

```sql
-- Create a HASH-partitioned stream table with 16 buckets
SELECT pgtrickle.create_stream_table(
    'tenant_corpus_partitioned',
    $$
        SELECT id, tenant_id, content, embedding
        FROM tenant_embeddings
    $$,
    '1m',
    'DIFFERENTIAL',
    partition_by => 'HASH:tenant_id:16'
);
```

pg_trickle creates 16 child tables named
`tenant_corpus_partitioned_p0` … `tenant_corpus_partitioned_p15`.

```sql
-- Create an HNSW index on each partition
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 0..15 LOOP
        EXECUTE format(
            'CREATE INDEX ON tenant_corpus_partitioned_p%s '
            'USING hnsw(embedding vector_cosine_ops)',
            i
        );
    END LOOP;
END $$;

-- Enable RLS for queries made through the partitioned parent
ALTER TABLE tenant_corpus_partitioned ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_corpus_partitioned FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant_corpus_partitioned
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
```

The parent's policy applies when the parent is queried. It does not protect a
reader granted direct access to a child: deny that access or configure and test
the child's own policies and privileges. The
[partitioned-corpus E2E test](../../tests/e2e_pgvector_hybrid_tests.rs) checks
parent filtering for a restricted reader and permission denial on a leaf.

**Query pattern — unchanged from Pattern 1:**

```sql
BEGIN;
SELECT set_config('app.tenant_id', $1::text, true);

-- PostgreSQL automatically routes to the correct partition
SELECT id, content, embedding <=> '[1,0,0]'::vector AS distance
FROM tenant_corpus_partitioned
WHERE tenant_id = $1::text::uuid
ORDER BY embedding <=> '[1,0,0]'::vector
LIMIT 10;
COMMIT;
```

**When to use HASH partitioning:**

- You have 10+ large tenants (> 500K embeddings each)
- You want per-partition HNSW rebuilds to keep index quality high
- Your queries always include an equality predicate on `tenant_id`

**Choosing `<buckets>`:**

| Tenant count | Recommended buckets |
|--------------|---------------------|
| 2–4          | 4                   |
| 5–16         | 16                  |
| 17–64        | 32 or 64            |
| 65+          | 64 or 128           |

---

## Pattern 3: Separate Stream Tables per Tier

For SLA isolation between tenant tiers, create one stream table per tier.
This gives each tier an independent refresh schedule and index configuration.

```sql
-- Premium tenants: 10-second refresh, full-precision HNSW
SELECT pgtrickle.create_stream_table(
    'premium_corpus',
    $$
        SELECT id, tenant_id, content, embedding
        FROM tenant_embeddings
        WHERE tier = 'premium'
    $$,
    '10s',
    'DIFFERENTIAL'
);
CREATE INDEX ON premium_corpus USING hnsw(embedding vector_cosine_ops)
    WITH (m = 32, ef_construction = 128);

ALTER TABLE premium_corpus ENABLE ROW LEVEL SECURITY;
ALTER TABLE premium_corpus FORCE ROW LEVEL SECURITY;
CREATE POLICY premium_tenant ON premium_corpus
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);

-- Standard tenants: 5-minute refresh, half-precision to save storage
SELECT pgtrickle.create_stream_table(
    'standard_corpus',
    $$
        SELECT id, tenant_id, content,
               embedding::halfvec(3) AS embedding
        FROM tenant_embeddings
        WHERE tier = 'standard'
    $$,
    '5m',
    'DIFFERENTIAL'
);
CREATE INDEX ON standard_corpus USING hnsw(embedding halfvec_cosine_ops);

ALTER TABLE standard_corpus ENABLE ROW LEVEL SECURITY;
ALTER TABLE standard_corpus FORCE ROW LEVEL SECURITY;
CREATE POLICY standard_tenant ON standard_corpus
    USING (tenant_id = NULLIF(current_setting('app.tenant_id', true), '')::uuid);
```

**Application routing:**

```sql
-- The trusted application chooses a tier and binds its authenticated tenant.
BEGIN;
SELECT set_config('app.tenant_id', $1::text, true);

-- The application queries the corpus for the authenticated tenant's tier.
COMMIT;
```

**When to use tier separation:**

- SLAs differ materially between tiers (e.g., sub-second vs sub-minute freshness)
- You want different index parameters (HNSW `m`, `ef_construction`) per tier
- Tier boundaries are stable and known at deployment time

**Monitoring per-tier freshness:**

```sql
SELECT pgt_name, status, refresh_mode, last_refresh_at
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name IN ('premium_corpus', 'standard_corpus')
ORDER BY pgt_name;
```

---

## Security Checklist

- [ ] RLS enabled (`ALTER TABLE ... ENABLE ROW LEVEL SECURITY`)
- [ ] RLS forced where table-owner access must be subject to policy; readers
      are neither superusers nor roles with `BYPASSRLS`
- [ ] Defining query audited: no unintentional cross-tenant joins
- [ ] Source tables have RLS if they contain cross-tenant data
- [ ] Trusted application binds the authenticated tenant using transaction-local
      `set_config`, and pooled transactions reset their context
- [ ] Readers cannot bypass parent policies through direct partition access
- [ ] Stream table not directly writable by application users

---

## Monitoring

```sql
-- Opt-in per-tenant row count; this scans the corpus and does not measure lag.
SELECT
    tenant_id,
    COUNT(*) AS embedding_count
FROM tenant_corpus
GROUP BY tenant_id;

-- Overall vector stream table health
SELECT * FROM pgtrickle.vector_status();
```
