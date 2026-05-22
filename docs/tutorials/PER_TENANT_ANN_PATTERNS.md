# Per-Tenant ANN Indexing Patterns

> VA-3 — Production patterns for multi-tenant RAG using RLS-scoped
> embedding corpora.

## Security Model

**Trust boundaries** for multi-tenant ANN stream tables:

1. **Row-level security enforces tenant isolation** — every SELECT against the
   stream table must pass through RLS policies.  pg_trickle respects PostgreSQL
   RLS; the stream table itself is a regular table.
2. **The refresh runs as a superuser background worker** — the background
   worker bypasses RLS when writing to the stream table.  This is intentional
   and correct: the worker writes denormalised data from authorised source
   tables.  Tenant isolation is enforced on reads, not writes.
3. **Never grant direct INSERT/UPDATE to application users** — only the
   pg_trickle background worker should write to stream tables.
4. **Audit the defining query** — if the defining query joins tenant data
   across boundaries (e.g. `FROM all_tenants_table`), the stream table will
   contain cross-tenant data.  Verify RLS on source tables applies during the
   defining query execution if you rely on source-level isolation.

---

## Pattern 1: Tenant Column + RLS Policy

```sql
-- Source table with tenant isolation
CREATE TABLE tenant_embeddings (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   UUID NOT NULL,
    content     TEXT NOT NULL,
    embedding   vector(1536) NOT NULL
);

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
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid);

-- ANN index (spans all tenants — filtered at query time by RLS)
CREATE INDEX ON tenant_corpus USING hnsw(embedding vector_cosine_ops);
```

**Query pattern:**

```sql
-- Set tenant context before querying
SET app.tenant_id = 'your-tenant-uuid';

-- This query automatically applies the RLS policy
SELECT id, content, embedding <=> '[...]'::vector AS distance
FROM tenant_corpus
ORDER BY embedding <=> '[...]'::vector
LIMIT 10;
```

---

## Pattern 2: Partitioned by Tenant (High-Volume)

For tenants with millions of embeddings each, partition by `tenant_id` and
create per-partition indexes.  This avoids a single large HNSW index and
allows index maintenance to proceed per-partition.

### `partition_key` syntax

The `partition_key` parameter accepts a string in one of two forms:

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
    partition_key => 'HASH:tenant_id:16'
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

-- Enable RLS on the partitioned parent (propagates to children)
ALTER TABLE tenant_corpus_partitioned ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_corpus_partitioned FORCE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON tenant_corpus_partitioned
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid);
```

**Query pattern — unchanged from Pattern 1:**

```sql
SET app.tenant_id = 'your-tenant-uuid';

-- PostgreSQL automatically routes to the correct partition
SELECT id, content, embedding <=> '[...]'::vector AS distance
FROM tenant_corpus_partitioned
ORDER BY embedding <=> '[...]'::vector
LIMIT 10;
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
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid);

-- Standard tenants: 5-minute refresh, half-precision to save storage
SELECT pgtrickle.create_stream_table(
    'standard_corpus',
    $$
        SELECT id, tenant_id, content,
               embedding::halfvec(1536) AS embedding
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
    USING (tenant_id = current_setting('app.tenant_id', true)::uuid);
```

**Application routing:**

```sql
-- Route to the correct tier based on a session variable
SET app.tenant_id = 'your-tenant-uuid';
SET app.tenant_tier = 'premium';  -- or 'standard'

-- Your application selects the corpus table dynamically:
-- IF app.tenant_tier = 'premium' THEN query premium_corpus
-- ELSE query standard_corpus
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
- [ ] RLS forced (`ALTER TABLE ... FORCE ROW LEVEL SECURITY`) to prevent
      superuser bypass during app queries
- [ ] Defining query audited: no unintentional cross-tenant joins
- [ ] Source tables have RLS if they contain cross-tenant data
- [ ] Application uses parameterised `SET app.tenant_id = $1` not string
      interpolation
- [ ] Stream table not directly writable by application users

---

## Monitoring

```sql
-- Per-tenant embedding lag (requires tenant_id in defining query)
SELECT
    tenant_id,
    COUNT(*) AS embedding_count,
    MAX(updated_at) AS newest_embedding
FROM tenant_corpus
GROUP BY tenant_id;

-- Overall vector stream table health
SELECT * FROM pgtrickle.vector_status();
```
