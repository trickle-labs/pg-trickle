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
| P1 | Installation instructions claimed repair could reconcile changed source OIDs after logical restore. It reuses persisted OIDs. Documentation also advertised nonexistent hold behavior and an invalid two-argument refresh call. | Separate restore adoption from recreation, resolve lifecycle ownership by current relation name after OID remapping, rebuild only base-table CDC triggers, normalize reused-buffer sentinels, correct the reinitialization call, document lossless CDC hold mode, and make `write_and_refresh` fail on a skipped refresh. | [backup and restore](BACKUP_AND_RESTORE.md), [configuration](CONFIGURATION.md), [SQL reference](SQL_REFERENCE.md), [upgrading](UPGRADING.md), [restore qualification](../tests/e2e_pg_dump_tests.rs). |

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

Snapshot polling still scans source data. Each poll now materializes one source
snapshot and serializes concurrent polls for that source; fanout and schedule
gates avoid polling a source before a consumer is due. It does not make
polling incremental or impose a remote-query timeout.

## Remaining work, in priority order

| Priority | Action | Completion evidence |
|---|---|---|
| P1 | Run the new CDC pause and polling paths through the final-image qualification matrix. | `test_cdc_hold_pause_preserves_dml_until_resume`, the existing shared-source/fanout tests, and the scheduled/manual `production-recovery-qualification` CI job. |
| P2 | Measure write blocking from manual differential refresh. Keep `lock_source_relations` until a commit-visible frontier/snapshot replacement passes late-commit, rollback, join, and crash tests. | Benchmark concurrent source writers and long transactions at the target refresh schedule. |
| P2 | Bound cleanup work per transaction if DELETE increases refresh tail latency. | Record p50/p95/p99 refresh and source-write latency, dead tuples, WAL bytes, and buffer age at sustained load before introducing batching. |
| P2 | Bound remote snapshot polling time. Cadence and overlap are now bounded by due gates and per-source advisory serialization; no remote-query timeout was added. | Add an explicit timeout only after measuring the supported FDW timeout contract and verifying rollback of partial polling state. |

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
  70 passed with retries disabled.
- Final rebuilt-image scheduler E2E suite: 9 passed with retries disabled,
  including lossless hold pause, failed remote polling, healthy-stream progress,
  and recovery.
- Final rebuilt-image logical-restore qualification: 4 passed with retries
  disabled, including the OID-shifted multi-level DAG recreation.
  Total focused E2E coverage: 83 passing tests.
- Shipping-image migration inclusion and locked CNPG dependency checks:
  source-level checks passed. Runtime upgrade execution is now in the scheduled
  and manual CI jobs, but has not run locally.

No deployment or remote CI run is part of this audit update.

Reproduce the focused E2E checks after building the image:

```bash
just build-e2e-image
./scripts/run_e2e_tests.sh --test e2e_concurrent_tests --test e2e_shared_buffer_tests --test e2e_cdc_tests --test e2e_guc_variation_tests --test-threads 4 --retries 0 --no-fail-fast
./scripts/run_e2e_tests.sh --test e2e_scheduler_tests --test-threads 2 --retries 0 --no-fail-fast
```

Before merging, run the manual `ci.yml` workflow on the pushed branch and
complete the source-lock, cleanup, and remote-polling measurements above.
