# Production audit, 22 September 2026

This audit starts at commit `3506533036d92a0e5388bb755fdec5c4b1d50a96`,
version 0.108.0. It covers CDC cleanup and compaction, scheduled refresh,
shipping image packaging, and operator instructions. It is a targeted audit,
not certification of every DVM operator, CDC mode, or deployment platform.

The highest-impact findings concern changes that cleanup or compaction could
permanently remove before a consumer reads them. The patch preserves those
changes and retains differential refresh. It does not change frontier commit
semantics or introduce a FULL fallback.

## Implemented changes

| Priority | Finding and trigger | Change | Evidence and regression |
|---|---|---|---|
| P1 | Deferred cleanup can revisit an earlier source while refreshing an unrelated table. It checked visibility before obtaining the TRUNCATE lock, allowing an invisible writer to commit and then lose its change. | Both deferred and frontier cleanup use bounded DELETE. Consumed source-TRUNCATE markers are reclaimed; the sentinel remains intact. `cleanup_use_truncate` remains accepted but has no effect. | [cleanup code](../src/refresh/codegen.rs), `test_cleanup_concurrent_writer_preserves_pending_change` in [concurrency tests](../tests/e2e_concurrent_tests.rs). |
| P1 | Shared-buffer compaction used one consumer's frontier interval. An insert consumed by A, followed by a delete, could cancel when slower consumer B refreshed, removing the delete A still needed. | Base and stream-table buffer compaction require exactly one registered consumer. | [compaction entry points](../src/cdc/mod.rs), `test_compaction_shared_source_preserves_consumer_boundaries` and `test_compaction_shared_stream_preserves_consumer_boundaries` in [shared-buffer tests](../tests/e2e_shared_buffer_tests.rs). |
| P1 | Compaction treated identical keyless rows as a single entity. Three inserts could become two because only the first and last events survived. | Base-buffer compaction requires a nondeferrable primary key. Stream-buffer compaction requires an immediate valid unique row-identity index. Duplicate-capable histories remain intact. | [compaction entry points](../src/cdc/mod.rs), `test_compaction_keyless_source_and_stream_preserve_duplicates` in [shared-buffer tests](../tests/e2e_shared_buffer_tests.rs). |
| P1 | The default scheduler fanout cache checked buffers without first polling foreign-table and materialized-view sources. Empty buffers could remain empty indefinitely. | Poll each distinct snapshot source before computing the change cache. Roll back failed polls and defer affected consistency groups while healthy groups continue. | [batched detection](../src/scheduler/mod.rs), [scheduler loop](../src/scheduler/scheduler_loop.rs), `test_scheduler_fanout_shared_matview_changes_refresh_both_consumers` in [scheduler tests](../tests/e2e_scheduler_tests.rs). |
| P2 | The serial scheduler parsed and semantically validated incremental queries before checking whether they were due or already refreshing. | Check schedule and refresh-lock availability first. Validation still precedes refresh execution. | `refresh_single_st` in [scheduler code](../src/scheduler/mod.rs). No measured throughput claim. |
| P1 | Shipping source-built images omitted migration scripts, so an existing database could lack its ALTER EXTENSION UPDATE path after an image change. | Copy migration SQL into the production and CNPG runtime images. | [production Dockerfile](../Dockerfile.ghcr), [CNPG Dockerfile](../cnpg/Dockerfile.ext-build). |
| P2 | The CNPG source build generated a new dependency lockfile instead of using the tested one. | Copy Cargo.lock and fetch with `--locked`. | [CNPG Dockerfile](../cnpg/Dockerfile.ext-build). |
| P1 | Installation instructions claimed repair could reconcile changed source OIDs after logical restore. It reuses persisted OIDs. Documentation also advertised nonexistent hold behavior and an invalid two-argument refresh call. | Explain restore adoption separately from recreation, correct the reinitialization call, and document that CDC pause currently discards changes. Update cleanup guidance, typed-buffer inspection examples, and manual-refresh lock-contention semantics across the operator docs. | [installation](../INSTALL.md), [backup and restore](BACKUP_AND_RESTORE.md), [configuration](CONFIGURATION.md), [security model](SECURITY_MODEL.md). |

PostgreSQL documents that TRUNCATE takes an ACCESS EXCLUSIVE lock and is not
MVCC-safe. The race above follows from applying that operation after a separate
visibility check. See [PostgreSQL 18 TRUNCATE](https://www.postgresql.org/docs/18/sql-truncate.html)
and [MVCC caveats](https://www.postgresql.org/docs/18/mvcc-caveats.html).

## Cost and compatibility

DELETE produces dead tuples and does work per consumed row. Busy installations
must monitor buffer size and autovacuum. Removing TRUNCATE also removes its
exclusive buffer lock and its wait behind unrelated source writers. Manual
differential refresh still takes SHARE locks on its own managed sources to
establish a safe visibility bound; this patch does not remove those locks.

Shared and keyless buffers retain more pending history because destructive
compaction is skipped. This may increase delta scan cost. Differential refresh
continues to read those histories. Single-consumer keyed buffers retain
compaction. The eligibility checks run only when the compaction threshold is
exceeded.

Snapshot polling still scans source data. Deduplication avoids repeating the
same poll for each consumer in the fanout pass. It does not make polling
incremental or impose a remote-query timeout.

## Remaining work, in priority order

| Priority | Action | Completion evidence |
|---|---|---|
| P1 | Replace destructive CDC pause semantics with durable buffering or reject source writes while capture is paused. Until then, leave `cdc_paused = off`; pause scheduling instead. The generated triggers in [cdc/mod.rs](../src/cdc/mod.rs) return without recording DML, and `hold` falls back to discard. | Commit insert, update, delete, and truncate operations across pause/resume and restart boundaries. Every accepted operation must be reflected after differential catch-up, or the source transaction must fail. Include upgrade-time trigger regeneration. |
| P1 | Qualify snapshot polling under concurrent source updates and overlapping manual/scheduled polls. [polling.rs](../src/cdc/polling.rs) reads the source separately for deletes, inserts, and snapshot replacement. This audit has not proven that all supported FDWs supply a consistent view across those reads. | Deterministically change the source between reads. Compare a staged single-snapshot design with existing behavior; require exact multiset agreement and no skipped changes. Serialize overlapping polls per source if the test demonstrates a race. |
| P1 | Add a supported logical-restore recreation command or an exact dependency-ordered runbook that exports definitions before backup and recreates catalog/CDC state against new OIDs. Current adoption and repair cannot remap dependencies. | Restore into a database with deliberately different OIDs, adopt it, recreate a multi-level DAG, perform new writes, and verify differential results and recovery status. Do not resume from old frontiers by guessing object identity. |
| P1 | Run the CDC fixes through crash recovery, long open transactions, shared consumers, partitioned buffers, and WAL capture before release. Partition detach/drop and WAL receipt handling were not changed or fully qualified here. | Fresh-image E2E and manual CI results on the final revision; include rollback after delta application and before frontier commit. Source data and stream data must match after recovery. |
| P2 | Measure write blocking from manual differential refresh. [refresh_ops.rs](../src/api/refresh_ops.rs) calls `lock_source_relations`, which takes SHARE locks on managed sources. A long writer can delay refresh, and refresh holds back new writers until its transaction ends. Keep this correctness barrier until there is a proven replacement. | Benchmark concurrent source writers and long transactions at the target refresh schedule. Replace the lock only with a commit-visible frontier/snapshot protocol that passes late-commit, rollback, join, and crash tests. |
| P2 | Bound cleanup work per transaction if DELETE increases refresh tail latency. Measure first; use small batches below the persisted minimum consumer frontier, with continued cleanup on later ticks. | Record p50/p95/p99 refresh and source-write latency, dead tuples, WAL bytes, and buffer age at sustained load. Catch-up must remain exact with a lagging consumer. |
| P2 | Avoid exact pending-row counts that cannot affect a disabled decision. [merge/mod.rs](../src/refresh/merge/mod.rs) counts pending rows before `maybe_auto_promote_buffer` even when automatic partition promotion is disabled. | Profile the default path, move the mode check ahead of the count, and show fewer buffer scans with unchanged promotion behavior for `auto`. |
| P2 | Bound snapshot polling time and cadence. Fanout currently polls before per-stream schedule gates, and the legacy non-fanout path still needs equivalent failure isolation. | Test an unreachable and a slow remote source alongside a due local stream. Assert a bounded delay for the local stream in both fanout modes. Poll a shared source once per required interval. |
| P2 | Decide whether `write_and_refresh` needs an optional strict completion contract. The SQL reference now documents its existing lock-contention behavior. [refresh_ops.rs](../src/api/refresh_ops.rs) can commit the user write after a skipped refresh with only a NOTICE. | Add a contention example and regression. If the API promises read-your-writes on the stream table, fail or wait rather than returning success after a skip. Preserve the existing manual refresh contract separately. |
| P2 | Exercise the actual shipping image upgrade paths in CI, including CNPG. Include Dockerfiles and installation docs in relevant workflow path filters. [ci.yml](../.github/workflows/ci.yml) currently filters the main workflow to code/test/build-manifest paths. | Build both runtime images, enumerate installed update paths, restore a supported previous-version database, and execute ALTER EXTENSION UPDATE. Checking that migration files exist alone does not prove upgrade SQL works. |

Do not tune away correctness checks or select UNLOGGED buffers to claim a
throughput improvement. Keep the logged default for deployments requiring
durable capture. FULL refresh remains recovery or unsupported-query behavior;
it is not the remedy for cleanup losing committed CDC rows.

## Validation

The following checks cover this patch. Tests
use Testcontainers and a freshly rebuilt extension image, not a local
PostgreSQL server.

- `just fmt`, then `just lint`: passed with zero warnings.
- `just test-unit`: 2,583 passed after allowing the local sockets used by the
  metrics-server tests. The initial sandbox restriction was not a code failure.
- `just test-integration`: 161 passed.
- Fresh-image CDC, concurrent refresh, shared-buffer, and GUC E2E suites:
  69 passed with retries disabled.
- Final rebuilt-image scheduler E2E suite: 8 passed with retries disabled,
  including failed remote polling, healthy-stream progress, and recovery.
  Total focused E2E coverage: 77 passing tests.
- Shipping-image migration inclusion and locked CNPG dependency checks:
  source-level checks passed. Runtime shipping-image upgrades remain untested.

No release, deployment, commit, or remote CI run is part of this patch.

Reproduce the focused E2E checks after building the image:

```bash
just build-e2e-image
./scripts/run_e2e_tests.sh --test e2e_concurrent_tests --test e2e_shared_buffer_tests --test e2e_cdc_tests --test e2e_guc_variation_tests --test-threads 4 --retries 0 --no-fail-fast
./scripts/run_e2e_tests.sh --test e2e_scheduler_tests --test-threads 2 --retries 0 --no-fail-fast
```

Before merging, push the reviewed revision and run the manual `ci.yml` workflow
on that branch. The ignored TPC-H, stability, WAL recovery, and shipping-image
upgrade qualifications remain separate release work.
