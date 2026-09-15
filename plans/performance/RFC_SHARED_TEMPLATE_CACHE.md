# RFC: Shared-Memory MERGE Template Cache (G14-SHC)

**Status:** Draft  
**Author:** pg_trickle team  
**Date:** 2025-07-15  
**Target:** v0.16.0  
**Historical source:** [Performance assessment at the v0.106.0 review](https://github.com/trickle-labs/pg-trickle/blob/5ed05167/plans/performance/REPORT_OVERALL_STATUS.md)

---

## 1. Problem Statement

pg_trickle caches compiled MERGE SQL templates in a **thread-local**
`HashMap<i64, CachedMergeTemplate>` per PostgreSQL backend process. This
design has two measurable costs:

1. **Cold-start penalty:** Every new backend connection must regenerate all
   MERGE templates on first refresh — measured at ~45 ms per stream table on
   typical hardware. In connection-pool-heavy environments (PgBouncer
   transaction mode, CNPG pod recycling), this penalty is paid frequently.

2. **Memory duplication:** With *N* concurrent backends each caching *M*
   stream table templates, total memory is O(N × M). For 100 backends × 500
   stream tables, this can reach hundreds of MB of duplicated SQL strings.

The existing cross-session invalidation mechanism (`CACHE_GENERATION` atomic
counter + per-ST `defining_query_hash`) correctly handles staleness but cannot
share the actual template data across processes.

### Current Architecture

```
Backend A                          Backend B
┌──────────────────────┐          ┌──────────────────────┐
│ thread_local! {      │          │ thread_local! {      │
│   MERGE_TEMPLATE_CACHE│          │   MERGE_TEMPLATE_CACHE│
│   HashMap<pgt_id,    │          │   HashMap<pgt_id,    │
│     CachedMerge>     │          │     CachedMerge>     │
│ }                    │          │ }                    │
│                      │          │                      │
│ LOCAL_MERGE_CACHE_GEN│          │ LOCAL_MERGE_CACHE_GEN│
└──────────────────────┘          └──────────────────────┘
              │                               │
              ▼                               ▼
      ┌───────────────────────────────────────────────┐
      │  Shared Memory (shmem.rs)                     │
      │  CACHE_GENERATION: PgAtomic<AtomicU64>        │
      │  (invalidation signal only — no data sharing) │
      └───────────────────────────────────────────────┘
```

### Proposed Architecture

```
Backend A                          Backend B
┌──────────────────────┐          ┌──────────────────────┐
│ thread_local! {      │          │ thread_local! {      │
│   LOCAL index:       │          │   LOCAL index:       │
│   HashMap<pgt_id,    │          │   HashMap<pgt_id,    │
│     (gen, dsm_slot)> │          │     (gen, dsm_slot)> │
│ }                    │          │ }                    │
└──────────┬───────────┘          └──────────┬───────────┘
           │       shared read                │
           ▼                                  ▼
   ┌──────────────────────────────────────────────────┐
   │  DSM Segment: pg_trickle_template_cache          │
   │  ┌────────────────────────────────────────────┐  │
   │  │ Header: magic, version, entry_count, gen   │  │
   │  ├────────────────────────────────────────────┤  │
   │  │ Slot[0]: pgt_id=1, hash=..., len=2048      │  │
   │  │          merge_sql: "MERGE INTO ..."        │  │
   │  │          trigger_using: "SELECT ..."        │  │
   │  │          delta_sql: "SELECT ..."            │  │
   │  ├────────────────────────────────────────────┤  │
   │  │ Slot[1]: pgt_id=2, hash=..., len=4096      │  │
   │  │          ...                                │  │
   │  ├────────────────────────────────────────────┤  │
   │  │ ...                                        │  │
   │  └────────────────────────────────────────────┘  │
   │  Protected by: LWLock (shared read / exclusive   │
   │  write)                                          │
   └──────────────────────────────────────────────────┘
```

---

## 2. Design

### 2.1 Data Structures

#### DSM Segment Layout

The shared cache lives in a single DSM (Dynamic Shared Memory) segment
allocated at extension load time via `pg_shmem_init!()`.

```
┌─────────────────────────────────────────────────────┐
│ CacheHeader (64 bytes)                              │
│   magic: u32          = 0x50475443 ("PGTC")         │
│   version: u32        = 1                           │
│   max_entries: u32    = configurable via GUC         │
│   live_entries: u32   = current count               │
│   generation: u64     = bumped on any mutation       │
│   total_payload_bytes: u64                          │
│   _reserved: [u8; 24]                               │
├─────────────────────────────────────────────────────┤
│ SlotIndex[max_entries] (32 bytes each)              │
│   pgt_id: i64                                       │
│   defining_query_hash: i64                          │
│   payload_offset: u32   (offset into payload area)  │
│   payload_len: u32      (total bytes for this entry)│
│   merge_sql_len: u32                                │
│   trigger_using_len: u32                            │
│   delta_sql_len: u32                                │
│   _pad: u32                                         │
├─────────────────────────────────────────────────────┤
│ Payload Area (variable size)                        │
│   Concatenated UTF-8 strings for each slot:         │
│   [merge_sql][trigger_using_sql][delta_sql]         │
└─────────────────────────────────────────────────────┘
```

#### Rust Structures

```rust
/// Shared-memory cache header. Stored at offset 0 of the DSM segment.
#[repr(C)]
struct CacheHeader {
    magic: u32,
    version: u32,
    max_entries: u32,
    live_entries: u32,
    generation: u64,
    total_payload_bytes: u64,
    _reserved: [u8; 24],
}

/// Fixed-size index entry for one cached template.
#[repr(C)]
struct SlotIndex {
    pgt_id: i64,
    defining_query_hash: i64,
    payload_offset: u32,
    payload_len: u32,
    merge_sql_len: u32,
    trigger_using_len: u32,
    delta_sql_len: u32,
    _pad: u32,
}
```

#### Per-Backend Local Index

Each backend maintains a lightweight thread-local index that maps
`pgt_id → (generation, slot_index)`. This avoids scanning the shared
slot array on every cache hit:

```rust
thread_local! {
    static LOCAL_SHM_INDEX: RefCell<HashMap<i64, (u64, u32)>> =
        RefCell::new(HashMap::new());
    static LOCAL_SHM_GEN: Cell<u64> = const { Cell::new(0) };
}
```

When `LOCAL_SHM_GEN` diverges from the shared `CacheHeader.generation`, the
local index is invalidated and rebuilt lazily (entries looked up on next
access).

### 2.2 Size Budget

**GUC:** `pg_trickle.template_cache_max_entries` (default: 1024)

| Component | Size Formula | Example (1024 entries, avg 4 KB SQL) |
|-----------|-------------|--------------------------------------|
| Header | 64 B | 64 B |
| SlotIndex array | 32 × max_entries | 32 KB |
| Payload area | avg_sql_size × max_entries | 4 MB |
| **Total** | | **~4.03 MB** |

**GUC:** `pg_trickle.template_cache_payload_bytes` (default: 8 MB)

The payload area has a hard cap. When full, new entries evict the
least-recently-used slot (LRU based on a `last_access_gen` field added to
SlotIndex).

For most deployments (< 500 stream tables), the default 8 MB is sufficient.
Large deployments with complex queries may increase to 32 MB.

**Comparison:** With 100 backends, thread-local caching of 500 templates
at avg 4 KB each = 100 × 500 × 4 KB = **200 MB**. Shared cache: **~4 MB**.

### 2.3 Locking Strategy

| Operation | Lock Mode | Held Duration |
|-----------|-----------|---------------|
| Read (cache hit) | LWLock SHARED | ~microseconds (memcpy of SQL string) |
| Write (populate slot) | LWLock EXCLUSIVE | ~microseconds (memcpy + index update) |
| Eviction (LRU) | LWLock EXCLUSIVE | ~microseconds |
| Full invalidation | LWLock EXCLUSIVE | ~microseconds (reset header counters) |

**Lock identity:** A single `LWLock` named `pg_trickle_template_cache_lock`,
registered via pgrx's `PgLwLock` mechanism.

**Contention analysis:** Write operations (cache population) happen at most
once per stream table per cache generation — after the first backend populates
a slot, all subsequent backends read it with a shared lock. Under steady state,
all operations are shared reads. The worst case is a cold start after
`bump_cache_generation()` where N backends simultaneously attempt to populate
— the LWLock serializes writes but each write is <10 μs, so even with 100
backends the total serialization window is <1 ms.

### 2.4 Invalidation Strategy

The shared cache integrates with the existing `CACHE_GENERATION` counter:

1. **Global invalidation** (`bump_cache_generation()`): The cache population
   path checks `defining_query_hash` — if the cached hash doesn't match the
   catalog, the slot is evicted and repopulated. No bulk flush needed.

2. **Per-ST invalidation** (ALTER QUERY, DROP): The DDL path evicts the
   specific `pgt_id` slot under an exclusive lock, then bumps
   `CACHE_GENERATION` as today.

3. **Hash-based validation:** On every cache read, the caller compares the
   stored `defining_query_hash` with the catalog's current hash. If they
   differ, the entry is treated as a miss and repopulated.

This is consistent with the existing invalidation model — the shared cache
simply moves the data plane from per-backend to shared memory while keeping
the same invalidation signals.

### 2.5 Read Path (Cache Hit)

```
refresh_stream_table(pgt_id, defining_query_hash)
  │
  ├─► Check LOCAL_SHM_GEN vs CacheHeader.generation
  │   If stale → clear LOCAL_SHM_INDEX
  │
  ├─► Lookup pgt_id in LOCAL_SHM_INDEX
  │   Hit → get slot_index
  │   Miss → scan SlotIndex array under SHARED lock
  │          → populate LOCAL_SHM_INDEX
  │
  ├─► Read SlotIndex[slot_index] under SHARED lock
  │   Verify defining_query_hash matches
  │   Copy merge_sql, trigger_using_sql, delta_sql from payload
  │
  └─► Return CachedMergeTemplate (owned Strings)
```

**Cost on cache hit:** 1 atomic load (generation check) + 1 HashMap lookup +
1 LWLock shared acquire/release + memcpy of ~4 KB SQL. Total: ~2–5 μs.

**Cost on first access per session:** Same as above plus one scan of the
SlotIndex array (< 1 μs for 1024 entries).

### 2.6 Write Path (Cache Miss → Populate)

```
generate_merge_template(pgt_id, ...)
  │
  ├─► Generate MERGE SQL (existing code path, ~45 ms)
  │
  ├─► Acquire LWLock EXCLUSIVE
  │   ├─► Double-check: another backend may have populated while we waited
  │   │   If populated → release lock, return cached entry
  │   ├─► Find free slot or evict LRU
  │   ├─► Write SlotIndex + payload
  │   ├─► Bump CacheHeader.generation
  │   └─► Release lock
  │
  └─► Update LOCAL_SHM_INDEX, LOCAL_SHM_GEN
```

### 2.7 Fallback Behavior

If the shared cache is unavailable (extension not in
`shared_preload_libraries`, or DSM allocation fails), the system falls back
to the existing thread-local cache with zero behavior change. The shared cache
is purely an optimization — the thread-local cache remains as a warm fallback.

---

## 3. Implementation Plan

### Phase A: Infrastructure (v0.16.0-alpha)

1. Add `PgLwLock` for template cache to `shmem.rs`
2. Allocate fixed DSM segment in `init_shared_memory()`
3. Implement `CacheHeader` + `SlotIndex` read/write with `// SAFETY:` docs
4. Add GUCs: `template_cache_max_entries`, `template_cache_payload_bytes`
5. Unit tests for slot allocation, eviction, hash validation

### Phase B: Integration (v0.16.0-beta)

1. Add `shared_template_lookup()` and `shared_template_populate()` to
   `refresh.rs`
2. Wire into `get_or_build_merge_template()` as the first-tier cache
   (shared → thread-local fallback → generate)
3. Integration test: verify cross-session cache sharing
4. Integration test: verify invalidation on ALTER QUERY

### Phase C: Benchmarking (v0.16.0-rc)

1. Benchmark cold-start latency with/without shared cache (target: <1 ms vs
   ~45 ms per ST)
2. Benchmark steady-state refresh latency (target: no regression)
3. Benchmark PgBouncer transaction-mode workload with connection churn
4. Measure shared memory footprint across 100/500/1000 stream tables
5. LWLock contention profiling under high concurrency (100 backends)

---

## 4. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| LWLock contention under extreme concurrency | Low | Medium | Shared reads dominate; write serialization <10 μs per slot |
| DSM segment too small for large deployments | Medium | Low | Configurable via GUC; LRU eviction handles overflow |
| Memory corruption in `unsafe` DSM access | Low | High | Extensive `// SAFETY:` docs; bounds checks on all offsets; magic + version validation |
| PostgreSQL version incompatibility (DSM API changes) | Low | Medium | Use pgrx abstractions (`PgLwLock`, `pg_shmem_init!`); test on PG 17/18/19 |
| Overhead on non-pooled deployments (long-lived backends) | Low | Low | Shared cache is write-once-read-many; thread-local index eliminates repeated shm lookups |

---

## 5. Alternatives Considered

### A. Larger Thread-Local Cache with Prewarming

**Idea:** On backend startup, bulk-read all templates from a catalog table and
populate the thread-local cache.

**Rejected because:** Still O(N × M) memory. The prewarm query adds latency to
every new connection. Does not solve the fundamental duplication problem.

### B. `pg_shmem` Fixed-Size Hash Table

**Idea:** Use PostgreSQL's built-in `ShmemInitHash()` for a fixed-size hash
table in shared memory.

**Rejected because:** `ShmemInitHash` requires fixed-size entries. MERGE SQL
templates are variable-length strings (100 B to 50 KB). Would require
either truncation or a separate string pool — the SlotIndex + Payload design
achieves this more cleanly.

### C. Memory-Mapped File (mmap)

**Idea:** Write templates to a file and `mmap()` it across backends.

**Rejected because:** Adds filesystem dependency, complicates crash recovery,
and is slower than DSM for small reads. PostgreSQL's DSM infrastructure is
purpose-built for this use case.

### D. Redis/External Cache

**Idea:** Store templates in an external cache service.

**Rejected because:** Adds operational dependency and network latency that
exceeds the cold-start cost we're trying to eliminate. The optimization must
be self-contained within PostgreSQL.

---

## 6. Go/No-Go Criteria

Proceed to full implementation (v0.16.0) if the prototype benchmark confirms:

- [ ] Cold-start latency reduced by ≥90% (target: <5 ms for 100 STs, down
      from ~4.5 s)
- [ ] Steady-state refresh latency regression <1% (within measurement noise)
- [ ] Shared memory footprint <16 MB for 500 stream tables
- [ ] No LWLock contention visible in `pg_stat_activity` under 100-backend
      PgBouncer workload
- [ ] Graceful fallback to thread-local cache when DSM is unavailable

If cold-start reduction is <50% or steady-state regression is >5%, the
approach should be reconsidered or scoped to only the most expensive templates.

---

## 7. Open Questions

1. **Should the payload area use a compactor/defragmenter?** After many
   evictions, the payload area may become fragmented. A simple bump allocator
   with periodic compaction (on generation boundary) may suffice.

2. **Should we cache delta SQL separately?** The `delta_sql_template` is only
   used for partitioned tables. Excluding it from the shared cache would
   reduce payload size by ~30% but add complexity.

3. **Should prepared statement handles be shared?** Currently
   `PREPARED_MERGE_STMTS` tracks per-session prepared statements. These
   cannot be shared across backends (PostgreSQL limitation), but the shared
   cache could include a flag indicating "this template is prepare-friendly"
   to guide per-backend prepared statement decisions.

---

## Appendix: Relevant Code Locations

| Component | File | Lines |
|-----------|------|-------|
| Thread-local cache definition | [src/refresh.rs](../../src/refresh.rs) | ~105–110 |
| `CachedMergeTemplate` struct | [src/refresh.rs](../../src/refresh.rs) | ~78–100 |
| Cache generation counter | [src/shmem.rs](../../src/shmem.rs) | ~81–83 |
| `bump_cache_generation()` | [src/shmem.rs](../../src/shmem.rs) | ~278–286 |
| Cache hit/miss logic | [src/refresh.rs](../../src/refresh.rs) | ~3910–3950 |
| Template generation | [src/refresh.rs](../../src/refresh.rs) | ~1599+ |
| `init_shared_memory()` | [src/shmem.rs](../../src/shmem.rs) | ~120–128 |
