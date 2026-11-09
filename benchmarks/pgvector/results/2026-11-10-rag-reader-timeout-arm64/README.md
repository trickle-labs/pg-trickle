# Reader timeout cleanup under a lock

On 2026-11-10, PostgreSQL 18.6 ran in a disposable Linux ARM64 Docker
container on an Apple M1 Pro. The [check](timeout_check.py) held an exclusive
table lock for 35 seconds while one harness reader repeatedly queried the
table. At 32 seconds it counted reader backends and readers whose statement
had run for over 30 seconds, then checked again after the lock was released.

The previous harness [failed the negative control](old-reader-negative.log):
two reader backends existed while the lock was held, one over 30 seconds old.
The corrected harness [passed](timeout-check.log): one reader was active,
none was overdue, one server statement timeout was recorded, and no reader
backend remained after release. Both checks used the same test and database;
only the imported reader helper changed.

To repeat against an empty disposable PostgreSQL database, set the standard
libpq connection environment and run from the repository root:

```sh
PYTHONPATH=benchmarks/pgvector python3 \
  benchmarks/pgvector/results/2026-11-10-rag-reader-timeout-arm64/timeout_check.py
```

The check creates `public.rag_timeout_probe` and requires permission to
inspect its own sessions in `pg_stat_activity`. It exercises one lock and one
reader; it does not replace workload-scale or final-package qualification.
