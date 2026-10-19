# S1 graph-refresh qualification

Run the frozen R53 joint-stack workload from [issue #1168](https://github.com/trickle-labs/pg-trickle/issues/1168)
against exact control and candidate Docker images. The candidate contains the
graph-refresh optimizations merged in #1169 and the keyless-delta fix in #1171.
The follow-up avoids JIT compilation within owner-executed graph applies below 100 source changes when
`planner_aggressive` is enabled. The caller's JIT setting is restored on exit.
The LEFT JOIN-only diagnostic reduced row expansion but compiled more JIT
functions and ran slower; that change was dropped and its measurement is retained,
and does not qualify the throughput gate.

The follow-up also reuses parsed query trees and resolved CDC buffer OIDs.
For immutable pointwise inner joins with at most two table inputs, a completed reconciled baseline permits
ordinary differential maintenance without another complete comparison.
Eligible large graph joins use the required multiset comparison to produce the
apply delta directly, retaining duplicate occurrences and scalar COUNT state.
Constraint triggers, rules and row security keep the existing UPDATE path.
Graph statistics cover the bounded stages and materialized delta actually read.
The focused regressions check complete typed output deltas, NULL duplicates,
multi-cycle changes, rollback, foreign-key cascades and owner privileges.

The unchanged workload requires 300 admissions at >=60 cases/minute, complete
case/source vectors, zero future receipts, and a 1,000-case due burst with
p95 <=150 seconds and maximum <=180 seconds. Whole-run storage must remain
below 10 GiB. The supervisor stops at 9 GiB sampled storage, 4 GiB sampled
container memory, 45 minutes, or less than 10 GiB host free space. These are
sampled limits, not a per-operation memory guarantee.
The supervisor uses the bounded PostgreSQL restart handling described below
because the frozen worker-health checks restart the server. Persistent
failures, timeouts and other errors stop the run.

The workflow runs both images sequentially on the same native Linux amd64
runner. This environment differs from the earlier amd64 runs emulated on an
ARM64 Docker host; historical rates cannot establish a speedup on this host.
The workload and acceptance thresholds are unchanged.

On 2026-10-19 the user explicitly approved accepting a slower **complete
control** as the comparison baseline. `check_result.py --baseline` accepts only
the frozen harness's sole below-60 rate failure after all 300 admissions,
complete vectors and every other gate pass. It retains the raw failure and
exit status and labels the summary `BASELINE_COMPLETE`, with the failed rate
gate visible. Other control failures remain comparison blockers. The default
candidate checker still requires at least 60/minute and no failed gates.

The builder cleans the pg-trickle package before compiling either source tree,
retaining dependency caches. Run 39255297739 exposed a stale Cargo artifact:
the candidate reused the control library despite a different source checkout.
Those native candidate observations do not measure the JIT optimization.
Qualification now requires different pg-trickle library hashes, identical
MDM/React hashes, and a runtime JIT-scope regression on the actual candidate.
The supervisor retains full timestamped container logs and includes their bytes
in the storage budget; it replays Docker's retained logs when an intentional
restart ends a follow stream. A failed log capture fails supervision.
Run 39268153170 passed the library and actual-binary guards, then both images
were stopped by `du` racing with deleted PostgreSQL temporary relations. These
remain explicit supervision failures. Disk sampling now retries only that exact
temporary-relation ENOENT condition, at most four times 200 ms apart; permission,
other missing-file, timeout and persistent errors still stop the run.

Run 39275675630 then expired the five-attempt SQL metric retry while PostgreSQL
was intentionally starting or stopping. The supervisor now continues sampling
physical PGDATA and cgroup memory during recognized connection restart errors,
retains prior SQL counters, and records every unavailable SQL sample. A gap of
60 seconds fails supervision. Other SQL errors still fail immediately. This
does not change the frozen harness's readiness checks, workload or limits.

Run 39294113404 completed its corrected control at 23.3414/minute with only
the approved rate failure. The candidate then failed the frozen fresh-server
60-second readiness check while the Docker initialization server was shutting
down; it admitted no churn cases. Supervision itself recorded no errors.
This is retained as a startup failure, with no candidate throughput score.

The user approved a separate native Linux ARM64 Docker profile on 2026-10-19.
It uses the same source pins, MDM correction, workload and limits. Its image
identities, compiler manifest, host and observations must be retained separately
from amd64; a passing ARM64 result is specific to that declared platform.
The GitHub workflow continues to measure the native amd64 pair.

The user also approved `mdm-grants.patch` for **both** images after native run
39255297739 deadlocked on admission 236. MDM had reissued GRANT on every graph
table before each refresh; its catalog writes can deadlock with ANALYZE while
graph refresh waits for a SHARE storage lock. The patch checks existing SELECT
access and locks storage before granting missing access. `test_grant_analyze.py`
reproduces the old lock cycle in PostgreSQL 18.4 and checks both corrected paths.
`test_mdm_grants.py` runs the actual MDM binary after bootstrap, checks that
repeat refresh leaves graph-storage catalog rows unchanged, and verifies that
revoked SELECT is restored. Only this isolated catalog-write regression disables
autovacuum to exclude unrelated statistics writes; S1 keeps its frozen settings.
The original MDM source remains pinned; the exact patch is retained with the
qualification source and applied explicitly by the Dockerfile. The failed run
remains a failure, with no complete throughput or paired speedup score.

## Fixed inputs

| Component | Source |
| --- | --- |
| Corrected control | pg-trickle `1c0345791979907e2c1418b0008ea15d58311cb9` |
| Candidate | Exact PR head (or manually selected workflow ref) |
| MDM | pg-mdm `eef7937180d504e62ce1076074bbeed8cef5d5a7` |
| React | pg-react `f19d9b2c5f3d947705ea26aa1619a989f08ae1dc` |
| PostgreSQL | 18.4, pinned image digest in `Dockerfile` |
| Harness | Retained R53 files listed with original SHA-256 values in `frozen-harness.json` |

The old runnable joint-stack images were removed during host cleanup. Their
React package had admitted pg-trickle 0.108.3; the pinned React source still
admits only 0.108.0. The Dockerfile explicitly replaces that exact patch-version
pin in the 0.46.0/0.46.1 install SQL. Isolation, trigger CDC, scheduler settings,
change-ratio and Graph/Delta capability checks remain intact. Both images use
the same reconstructed React package; its installed SQL hashes are retained.

The user approved the separately identified corrected control on 2026-10-19.
The original exact control `sha256:1fdfeb8e1c08b049491583612f7e34cd4c0e27c7d789084a627ce3089e04d981`
completed two admissions and deadlocked on admission 3. Its retained
investigation and complete copied server log are in `historical-control/`.
The corrected control does not turn that failed attempt into a successful
comparison. Each new pair must complete on its own images and environment.

## Running and retaining evidence

The `S1 exact-image qualification` workflow runs for changes to this directory
or its workflow and can also be dispatched manually after the workflow is on
the default branch. Its artifact contains exact source snapshots (including
lockfiles), build logs, host configuration, image/library identities,
complete observations and vectors, resource samples, failures, and hashes.
Download artifacts before their 90-day expiration for long-term retention.

Local supervision checks need only Python's standard library:

```sh
python3 benchmarks/s1/check_frozen_harness.py
python3 benchmarks/s1/test_bootstrap_compat.py
python3 benchmarks/s1/test_check_result.py
python3 benchmarks/s1/qualification/run_bounded.py --self-check
python3 benchmarks/s1/qualification/test_supervisor.py
```

For a locally built exact joint-stack image:

```sh
PGREACT_S1_BATCH_SIZE=32 python3 benchmarks/s1/qualification/run_bounded.py \
  sha256:<full-image-id> candidate-300
```

For the approved native ARM64 profile, prepare the same source checkouts as
the workflow, build `Dockerfile.arm64-sdk` with `--platform linux/arm64`, then
build both images with the shared `Dockerfile`, `--platform linux/arm64` and
`--build-arg S1_BUILDER=<that-local-sdk-tag>`. Set `TRICKLE_SOURCE=_s1/control`
only for the control build. Retain `/sdk-manifest.txt` from the SDK and its image
identity. The SDK uses Rust 1.98.0 and pgrx 0.18.0; its installed PostgreSQL 18
header/package versions are recorded in that manifest. The runtime remains the
pinned PostgreSQL 18.4 image. Cargo build caches are separated by architecture.
Run the full supervisor and complete-result checker sequentially for both
exact image IDs on the same idle Docker host.
Set `DOCKER_DEFAULT_PLATFORM=linux/arm64` for the ARM64 supervisor invocation;
the frozen harness inherits this explicit platform selection for Docker runs.

Run names must be new; existing observations are never overwritten. The
supervisor directly invokes Docker instead of requiring the original host's
`rtk proxy` wrapper. S1 workload, SQL fixtures, and thresholds retain their saved
values. The bootstrap adapter retains the original MDM fixture and its hash,
then changes only two setup assertions: a one-record insert/delete may use the
exact affected-record resolver added by #1171 as well as the historical full
invalidation path. The helper-mutation checks remain mandatory. Both images
use the same adapter, and its input patterns must match exactly once.
`frozen-harness.json` records the original and adapted file hashes.
Diagnostic case-count overrides do not qualify the throughput gate.

`check_result.py <run-directory>` independently requires all S1 gates and
complete source/case vectors. An input-blocked harness that exits zero is
rejected, as are diagnostics, incomplete runs, future receipts, or resource
limit failures.

A passing S1 run covers this upstream throughput issue. React owner-outcome,
outage/recovery, and signed operating-envelope release gates remain separate;
the frozen harness reports those broader inputs as unresolved even after an
S1 performance pass.
