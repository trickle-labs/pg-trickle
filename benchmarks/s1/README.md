# S1 graph-refresh qualification

Run the frozen R53 joint-stack workload from [issue #1168](https://github.com/trickle-labs/pg-trickle/issues/1168)
against exact control and candidate Docker images. The candidate contains the
graph-refresh optimizations merged in #1169 and the keyless-delta fix in #1171.
The follow-up avoids JIT compilation within owner-executed graph applies below 100 source changes when
`planner_aggressive` is enabled. The caller's JIT setting is restored on exit.
The LEFT JOIN-only diagnostic reduced row expansion but compiled more JIT
functions and ran slower; that change was dropped and its measurement is retained,
and does not qualify the throughput gate.

## Current parser-corrected native ARM64 qualification

On 2026-10-20, the exact candidate built from source `84e15eabebcb84af49fd528d8c30c5e2e8d3348a`
passed the full native ARM64 Docker qualification. Its image was
`sha256:087e7188049c1ba79ec0f4b5c85e8eddaae7500d934cdc079c7691ad4f685fc0`;
the separately identified control was built from the approved source
`1c0345791979907e2c1418b0008ea15d58311cb9` as
`sha256:671cd2c7f21a3973cfe554b0a9c460b603c32bbcfec599b7b028784e871cefc7`.
Both images use the same corrected MDM grant patch and pinned React/MDM sources.

| Measurement | Candidate | Corrected control |
| --- | --- | --- |
| Admissions/minute | **60.036187212083334** | 27.209718099565716 |
| Complete admissions | 300 | 300 |
| Source/case vectors | 1,200 / 300, exact | 1,200 / 300, exact |
| Future receipts | 0 | 0 |
| Due-burst p95 / maximum | 109.177932 / 114.117059 s | 109.627996 / 114.512779 s |
| Conservative storage | 3,422,608,052 bytes | 3,418,885,308 bytes |
| Peak sampled memory | 2,044,936,192 bytes | 1,948,774,400 bytes |
| Strict result | **PASS, raw exit 0** | BASELINE_COMPLETE; rate failed, raw exit 1 |

Every candidate correctness, permission, worker-health, latency and resource
gate passed; supervision recorded no measurement errors or stop reason. The
full local vectors, observations, timestamped logs and resource samples remain
under `benchmarks/s1/qualification/native-arm64-84e-{control,candidate}-300/`.
The workflow below now runs the same strict profile on a native ARM64 Linux
runner. The earlier native amd64 failure remains retained separately and is
not replaced by this ARM64 result.

## Archived earlier native ARM64 qualification

The recorded result below applies to its exact archived images. A subsequent
bootstrap correction disables background workers only in Docker's temporary
initialization server, avoiding its shutdown race. Both control and candidate
receive the same entrypoint correction; the final server receives the original
arguments and worker settings. CI verifies temporary-server isolation and the
unchanged runtime settings on both images before running qualification. This
changes image identities and does not transfer the archived throughput score
to the rebuilt images. The workload, readiness bound and rate gates are unchanged.

The subsequent parser correction preserves `JOIN ... USING` predicates in
differential plans and SQL rewrites. It fixes the compiler-v9 graph's extra
rows after a composite-key insert. This changes compiled extension inputs;
the archived result below does not qualify this new candidate.

Native amd64 run 39375497790 completed both images after the bootstrap fix.
The control completed at 23.057896203519824/minute under the approved baseline
policy. Candidate `sha256:cb37bdd19af6251d78c82d2e354e21faba4c9befefc8ab8fad0734f469822490`
failed throughput at 49.79070394611743/minute, with the other recorded gates
passing. Its raw exit 1 and all 102 verified artifact hashes remain retained
under `target/issue1168-completion/native-d221d05/`. This failure is not
accepted by the candidate checker or replaced by the archived ARM64 score.

The exact candidate from source `a89347261896efa26162e707c782d1c2a2e9d28b`, image
`sha256:3a84aac71ae8b0f1ed73a7480dcb85205d14f7e948b864200da52e58ba04f316`,
passes the unchanged strict candidate checker with raw exit 0. The explicitly
approved corrected control is complete; its raw below-60 failure and exit 1
remain preserved under the control-only baseline policy.

| Measurement | Candidate | Corrected control |
| --- | --- | --- |
| Admissions/minute | **60.02924744994256** | 27.092425905801132 |
| Complete admissions | 300 | 300 |
| Source/case vectors | 1,200 / 300, exact | 1,200 / 300, exact |
| Future receipts | 0 | 0 |
| Due-burst p95 / maximum | 76.069447 / 80.866423 s | 109.314041 / 114.155216 s |
| Conservative storage | 3,317,364,733 bytes | 3,415,481,585 bytes |
| Peak sampled memory | 1,965,625,344 bytes | 1,764,052,992 bytes |
| Strict result | PASS | BASELINE_COMPLETE; rate failed |

All correctness, permission, worker-health and resource gates pass. Candidate
supervision recorded no measurement errors or stop reason. The initial Docker
inventory was empty; host-start receipts contain only its two exact-image
containers. Local tests, builds and compression were stopped during timing.

The [qualification record](arm64-qualification.json) retains source/image pins,
commands, explicit control/profile approvals, archive hashes and the manifest
of full local observations, vectors, logs and resource measurements under
`target/issue1168-completion/`. Both runnable images and the exact candidate
source are archived there. Earlier failures remain unchanged below and in
their retained run directories; unchanged images were not rerun to replace
an unfavorable rate.

This result qualifies the approved Linux ARM64 Docker profile. Native amd64
performance has not qualified. The harness paces admissions at 60/minute, so
this result does not establish capacity above that offered rate. Formatting,
zero-warning lint, 2,618 Docker units, 49 focused graph/ownership/atomicity/
conformance checks and actual MDM/JIT guards pass for the exact candidate.
Full local integration/E2E, TPC-H and dbt suites were not run; one known
standalone PostgreSQL error-stub unit is excluded.

The follow-up also reuses parsed query trees and resolved CDC buffer OIDs.
Incremental refresh admission uses fresh resolved volatility analysis without
creating an output-schema probe view whose column information is discarded.
CREATE/ALTER still inspect exact output types and typmods where needed.
Pure string cost classification does not switch execution roles. Source-commit
buffer discovery uses direct joins for base and stream-table dependencies,
retaining identical buffers, timestamp calculations and safe-bound filters.
The retained 73-table catalog diagnostic found identical complete vectors and
reduced aggregate lookup execution from 43.182 ms to 11.354 ms; this is not a
qualification score. The regression checks oldest timestamps from both kinds
of dependency.
For immutable pointwise inner joins with at most three table inputs, a completed reconciled baseline permits
ordinary differential maintenance without another complete comparison.
Eligible large graph joins use the required multiset comparison to produce the
apply delta directly, retaining duplicate occurrences and bigint scalar-aggregate state.
Constraint triggers, rules and row security keep the existing UPDATE path.
Graph statistics cover the bounded stages and materialized delta actually read.
Graph refresh also reuses the metadata already validated in each contract snapshot
and loads manual-refresh upstream metadata once. Eligible exact replacement deltas
without hidden counts can feed split downstream capture directly.
For supported boolean, bytea, integer, text, varchar, UUID and temporal outputs
with deterministic collations, native comparison operates on complete multisets
before encoding changed row identities. Plain replacements group signed weights;
counted intermediates retain `EXCEPT ALL`. Other types retain the existing comparison.
The focused regressions check complete typed output deltas, NULL duplicates,
multi-cycle changes, rollback, foreign-key cascades and owner privileges.

Normalized pointwise deltas can also feed split downstream capture, with joins
requiring the guarded exact baseline, avoiding another pre/post snapshot. This requires
the existing weight-normalization wrapper and excludes hidden count state.
The regression checks duplicate occurrences, NULLs, cancellation and absence
of the redundant pre-snapshot within the refresh transaction.

Small immutable computed projections with a reconciled baseline can reuse their
mandatory comparison too. Native full-value matching retains the actual stored
row identity and physical deletion target, including duplicate NULL occurrences.
A record hash narrows incomplete-identity joins; full NULL-safe equality verifies
every match. Complete identities retain direct identity matching.
The regression changes the caller's timezone and checks complete temporal deltas.

The unchanged workload requires 300 admissions at >=60 cases/minute, complete
case/source vectors, zero future receipts, and a 1,000-case due burst with
p95 <=150 seconds and maximum <=180 seconds. Whole-run storage must remain
below 10 GiB. The supervisor stops at 9 GiB sampled storage, 4 GiB sampled
container memory, 45 minutes, or less than 10 GiB host free space. These are
sampled limits, not a per-operation memory guarantee.
The supervisor uses the bounded PostgreSQL restart handling described below
because the frozen worker-health checks restart the server. Persistent
failures, timeouts and other errors stop the run.

The GitHub workflow runs both images sequentially on a native Linux ARM64
runner with the Docker platform pinned to `linux/arm64`. This is the separately
approved ARM64 profile; it does not imply a native amd64 throughput result. The
workload, candidate floor and all other acceptance thresholds are unchanged.

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

The complete ARM64 candidate from source `17c49e7e52c0e15ef155adbf4c1608725a5c55f1`,
image `sha256:29d448459fa81c35c5d85558061f984b023345b5f89320da93a747932faf8df4`,
failed the rate gate at 59.369956913836965/minute. Its complete 1,200 source and
300 case vectors matched, with zero future receipts. A cgroup read raced with
container teardown and was also retained as a measurement error; the default
complete-result checker rejected the run. Earlier complete candidates at
57.6127, 59.1360 and 59.0177/minute remain failures as well.

A metadata fillfactor-50 prototype completed at 59.4198/minute, with all vectors
and other gates passing. Despite fewer new-page updates in a temporary-table
diagnostic, it did not materially improve the full workload and was discarded.
Its source `ca4c5faba9c0c5198c5442ba25b20946e747a6c4`, exact image
`sha256:36ee69d30e83a17a08314cad2b99b8bb2a633679eea1db05e9da4d905a9bf7f1`,
raw failure, complete observations and portable image remain retained.

The subsequent pointwise/projection candidate, source
`6d12294d4079ff9cb5f30db9ee8b7fd5c29fe09e`, image
`sha256:2e1439bcd726e7c50ef2a852ed1b782ebfb0cb88305fae7077361948cca3103b`,
completed at 59.97136027709061/minute and remains a failure. All 1,200 source
and 300 case vectors matched, with zero future receipts and all other gates
passing. Due-burst p95/max were 77.284419/82.261 seconds; conservative storage
was 3,408,026,079 bytes and peak sampled memory 2,076,282,880 bytes. The raw
exit, official checker rejection, full vectors, logs and runnable archive are
retained. An admission-to-admission partial rate omitted the initial timing
offset; only the unchanged official SQL window determines the final rate.

Direct identity matching, source `f1c7b373f347ff6bd8ff76bde7b4de893fd0bd0c`,
image `sha256:19963a62f001cca4be118f770b73d6c3e2580c39b1bb7cb6aafbaf15448a9bb6`,
then completed at 59.90644968878966/minute and also remains a failure. Its
complete vectors and all other gates passed: due p95/max 76.085647/80.941095
seconds, conservative storage 3,428,191,327 bytes, peak memory 1,890,263,040
bytes. The exact image, full observations and official rejection are retained.

Memory sampling now handles a confirmed stopped/removed container consistently
with the existing SQL/disk sampling paths: it records an explicit
`exited_container_samples` receipt and retains previous memory/storage peaks.
A failed memory read from a running container still stops supervision. Fault
injection verifies both paths and preservation of the preceding resource sample.
The failed run above remains unchanged. Workload, candidate rate, readiness,
sampling interval, resource limits and complete-vector requirements are unchanged.

Run 39294113404 completed its corrected control at 23.3414/minute with only
the approved rate failure. The candidate then failed the frozen fresh-server
60-second readiness check while the Docker initialization server was shutting
down; it admitted no churn cases. Supervision itself recorded no errors.
This is retained as a startup failure, with no candidate throughput score.

Run 39304244497 likewise completed its corrected native amd64 control at
21.0940/minute, then failed candidate fresh-server readiness before churn.
Its complete artifact has 66 verified file hashes; no candidate rate is inferred.

The complete ARM64 candidate from source `a75c10ff71642f6fa7483610ed6f5ed5863b7f43`,
image `sha256:b3d768acb3e2ae962543d68b363b7948256daf1890be1d6dd88989e957955d3a`,
failed only the throughput gate at 45.0327/minute. All 300 admissions, 1,200
source-row vectors and 300 case vectors passed, with zero future receipts.
Due-burst p95/max were 76.631159/81.643393 seconds, conservative storage was
3,549,250,675 bytes and peak sampled memory was 2,009,587,712 bytes.
Raw exit 1, full observations, logs, resource accounting, exact compiled-source
proof and the runnable image archive remain under `target/issue1168-completion/`.
This failed candidate is never accepted using the control-only baseline policy.

The user approved a separate native Linux ARM64 Docker profile on 2026-10-19.
It uses the same source pins, MDM correction, workload and limits. Its image
identities, compiler manifest, host and observations are retained separately
from amd64; a passing ARM64 result is specific to that declared platform.

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
